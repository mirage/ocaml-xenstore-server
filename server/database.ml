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

module type VIEW = sig
  type t

  val create: unit -> t Lwt.t

  val read: t -> Protocol.Path.t -> Node.contents Lwt.t

  val write: t -> Protocol.Path.t -> Node.contents -> unit Lwt.t

  val rm: t -> Protocol.Path.t -> unit Lwt.t

  val merge: t -> string -> unit Lwt.t
end

let view, view_wakener = Lwt.task ()

let make_view = function
  | S.NoPersistence ->
    let module M = struct
      type t = unit
      let create () = return ()
      let read () _ = fail (Failure "not implemented")
      let write () _ _ = return ()
      let rm () _ = fail (Failure "not implemented")
      let merge () _ = fail (Failure "not implemented")
    end in
    return (module M: VIEW)
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

    let module M = struct
      type t = DB.View.t
      let create = DB.View.create
      let write t path contents =
        debug "+ %s" (Protocol.Path.to_string path);
        (try_lwt
          DB.View.update t (value_of_filename path) (Sexp.to_string (Node.sexp_of_contents contents))
        with e -> (error "%s" (Printexc.to_string e)); return ())
      let rm t path =
        debug "- %s" (Protocol.Path.to_string path);
        (try_lwt
          DB.View.remove t (dir_of_filename path) >>= fun () ->
          DB.View.remove t (value_of_filename path)
        with e -> (error "%s" (Printexc.to_string e)); return ())
      let read t _ = fail (Failure "not implemented")
      let merge t origin =
        let origin = IrminOrigin.create "%s" origin in
        DB.View.merge_path ~origin db [] t >>= function
        | `Ok () -> return ()
        | `Conflict msg ->
          error "Conflict while merging database view: %s (this shouldn't happen, all backend transactions are serialised)" msg;
          return ()
    end in
    return (module M: VIEW)

let store, store_wakener = Lwt.task ()
let persister, persister_wakener = Lwt.task ()

let persist ?origin side_effects =
  persister >>= fun p ->
  p ?origin (List.rev side_effects.Transaction.updates)

let immediate f =
  f >>= fun (t, e) ->
  persist e >>= fun () ->
  return t

let initialise kind =
  make_view kind >>= fun m ->
  Lwt.wakeup view_wakener m;
  let module View = (val m: VIEW) in

  let s = Store.create () in
  Lwt.wakeup store_wakener s;

  (* These must all be idempotent *)
  let p ?origin us =
    View.create () >>= fun v ->
    Lwt_list.iter_s
      (function
        | Store.Write(path, contents) ->
          View.write v path contents
        | Store.Rm path ->
          View.rm v path
      ) us >>= fun () ->
    let origin = match origin with None -> "unknown" | Some x -> x in
    View.merge v origin in
  Lwt.wakeup persister_wakener p;

  return ()
