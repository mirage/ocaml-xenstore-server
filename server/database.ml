(*
 * Copyright (C) Citrix Systems Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation; version 2.1 only. with the special
 * exception on linking described in file LICENSE.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *)
open Sexplib
open Lwt
open Xenstore
open Protocol

let ( |> ) a b = b a
let ( ++ ) f g x = f (g x)

let debug fmt = Logging.debug "database" fmt
let error fmt = Logging.error "database" fmt

let store, store_wakener = Lwt.task ()
let persister, persister_wakener = Lwt.task ()

let persist ?origin side_effects =
  persister >>= fun p ->
  p ?origin (List.rev side_effects.Transaction.updates)

let immediate f =
  f >>= fun (t, e) ->
  persist e >>= fun () ->
  return t

let initialise = function
| S.NoPersistence ->
  let s = Store.create () in
  Lwt.wakeup persister_wakener (fun ?origin us ->
    List.iter (fun u ->
      debug "%sNot persisting %s" (match origin with None -> "" | Some x -> x ^ ": ") (Sexp.to_string (Store.sexp_of_update u));
    ) us;
    return ()
  );
  Lwt.wakeup store_wakener s;
  return ()
| S.Git filename ->
  let open Irmin_unix in
  let module Git = IrminGit.FS(struct
    let root = Some filename
    let bare = true
  end) in
  let module DB = Git.Make(IrminKey.SHA1)(IrminContents.String)(IrminTag.String) in
  DB.create () >>= fun db ->

  let dir_suffix = ".dir" in
  let value_suffix = ".value" in

  let value_of_filename path = match List.rev (Protocol.Path.to_string_list path) with
  | [] -> []
  | file :: dirs -> List.rev ((file ^ value_suffix) :: (List.map (fun x -> x ^ dir_suffix) dirs)) in

  let dir_of_filename path =
    List.rev (List.map (fun x -> x ^ dir_suffix) (List.rev (Protocol.Path.to_string_list path))) in

  let remove_suffix suffix x =
    let suffix' = String.length suffix and x' = String.length x in
    String.sub x 0 (x' - suffix') in
  let endswith suffix x =
    let suffix' = String.length suffix and x' = String.length x in
    suffix' <= x' && (String.sub x (x' - suffix') suffix' = suffix) in

  (* These must all be idempotent *)
  let p ?origin us =
    DB.View.create () >>= fun v ->
    Lwt_list.iter_s
      (function
        | Store.Write(path, contents) ->
          debug "+ %s %s" (match origin with None -> "" | Some x -> x) (Protocol.Path.to_string path);
          (try_lwt
            DB.View.update v (value_of_filename path) (Sexp.to_string (Node.sexp_of_contents contents))
          with e -> (error "%s" (Printexc.to_string e)); return ())
        | Store.Rm path ->
          debug "- %s %s" (match origin with None -> "" | Some x -> x) (Protocol.Path.to_string path);
          (try_lwt
            DB.View.remove v (dir_of_filename path) >>= fun () ->
            DB.View.remove v (value_of_filename path)
          with e -> (error "%s" (Printexc.to_string e)); return ())
      ) us >>= fun () ->
    let origin = match origin with
    | None -> None
    | Some o -> Some (IrminOrigin.create "%s" o) in
    DB.View.merge_path ?origin db [] v >>= function
    | `Ok () -> return ()
    | `Conflict msg ->
      error "Conflict while merging database view: %s (this shouldn't happen, all backend transactions are serialised)" msg;
      return () in
  let store = Store.create () in
  let t = Transaction.make Transaction.none store in
  DB.dump db >>= fun contents ->
  (* Sort into order of path length, so we create directories before we need them *)
  let contents = List.sort (fun (path, _) (path', _) -> compare (List.length path) (List.length path')) contents in
  List.iter
    (fun (path, value) ->
     debug "%s <- %s" (String.concat "/" path) value;
     (* The keys are always of the form foo.dir/bar.dir/baz.value
        The List.sort guarantees that foo.value will have been processed before foo.dir/bar.value *)
     let path = List.map (fun element ->
        if endswith dir_suffix element then remove_suffix dir_suffix element
        else if endswith value_suffix element then remove_suffix value_suffix element
        else element (* should never happen *)
     ) path in
     let contents = Node.contents_of_sexp (Sexp.of_string value) in
     let path = Protocol.Path.of_string_list path in
     Transaction.write t None contents.Node.creator (Perms.of_domain 0) path contents.Node.value;
     Transaction.setperms t (Perms.of_domain 0) path contents.Node.perms
    ) contents;
  Lwt.wakeup persister_wakener p;
  Lwt.wakeup store_wakener store;
  return ()
