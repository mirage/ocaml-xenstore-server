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
open Sexplib.Std
open Sexplib
open Lwt
open Xenstore
open Error

let debug fmt = Logging.debug "xenstored" fmt
let info  fmt = Logging.info  "xenstored" fmt
let error fmt = Logging.error "xenstored" fmt

module type DB_S = Irmin.S
  with type key = Irmin.Contents.String.Path.t
   and type value = string

let make ?(prefer_merge=true) config db_m =
  let module DB = (val db_m: DB_S) in
  DB.create config Irmin_unix.task >>= fun db ->
  (* view is no longer embedded in S_MAKER *)
  let module DB_View = Irmin.View(DB) in
  let module V = struct
    type t = {
      v: DB_View.t;
    }

    let remove_suffix suffix x =
      let suffix' = String.length suffix and x' = String.length x in
      String.sub x 0 (x' - suffix')
    let endswith suffix x =
      let suffix' = String.length suffix and x' = String.length x in
      suffix' <= x' && (String.sub x (x' - suffix') suffix' = suffix)

    let dir_suffix = ".dir"
    let value_suffix = ".value"

    let root = "/"

    let value_of_filename path = match List.rev (Protocol.Path.to_string_list path) with
    | [] -> [ root ]
    | file :: dirs -> root :: (List.rev ((file ^ value_suffix) :: (List.map (fun x -> x ^ dir_suffix) dirs)))

    let dir_of_filename path =
      root :: (List.rev (List.map (fun x -> x ^ dir_suffix) (List.rev (Protocol.Path.to_string_list path))))

    let to_filename = List.map (fun x ->
      if endswith dir_suffix x
      then remove_suffix dir_suffix x
      else if endswith value_suffix x
           then remove_suffix value_suffix x
           else x
    )

    let create () =
      DB_View.of_path (db "") [] >>= fun v ->
      return { v }
    let mem t path =
      (try_lwt
        DB_View.mem t.v (value_of_filename path)
       with e -> (error "%s" (Printexc.to_string e); return false))
    let write t path contents =
      (try_lwt
        DB_View.update t.v (value_of_filename path) (Sexp.to_string (Node.sexp_of_contents contents)) >>= fun () ->
        return (`Ok ())
      with e -> (error "%s" (Printexc.to_string e)); return (`Ok ()))
    let list t path =
      (try_lwt
        (* TODO: differentiate a directory which doesn't exist from an empty directory
        DB.View.read (value_of_filename path) >>= function
        | None -> return (`Enoent path)
        | Some _ ->
        *)
          DB_View.list t.v (dir_of_filename path) >>= fun keys ->
          let union x xs = if not(List.mem x xs) then x :: xs else xs in
          return (`Ok (List.fold_left (fun acc x -> match (List.rev x) with
            | basename :: _ ->
              if endswith dir_suffix basename
              then union (remove_suffix dir_suffix basename) acc
              else
                if endswith value_suffix basename
                then union (remove_suffix value_suffix basename) acc
                else acc
            | [] -> acc
          ) [] keys))
      with e -> (error "%s" (Printexc.to_string e)); return (`Enoent path))

    let rm t path =
      (try_lwt
        DB_View.remove t.v (dir_of_filename path) >>= fun () ->
        DB_View.remove t.v (value_of_filename path) >>= fun () ->
        return (`Ok ())
      with e -> (error "%s" (Printexc.to_string e)); return (`Ok ()))
    let read t perms path =
      (try_lwt
        DB_View.read t.v (value_of_filename path) >>= function
        | None -> return (`Enoent path)
        | Some x ->
          let contents = Node.contents_of_sexp (Sexp.of_string x) in
          if Perms.check perms Perms.READ contents.Node.perms
          then return (`Ok contents)
          else return (`Eacces path)
       with e -> (error "%s" (Printexc.to_string e)); return (`Enoent path))
    let merge t msg =
      ( if prefer_merge then begin
            DB_View.merge_path (db msg) [] t.v >>= function
          | `Ok () -> return true
          | `Conflict msg ->
            info "Conflict while merging database view: %s. Attempting a rebase." msg;
            return false
        end else return false )
      >>= function
      | true -> return true
      | false ->
        DB_View.rebase_path (db msg) [] t.v >>= function
        | `Ok () -> return true
        | `Conflict msg ->
          info "Conflict while rebasing database view: %s. Asking client to retry" msg;
          return false

    type watch = unit -> unit Lwt.t

    let rec ls_lR view key =
      DB_View.list view key >>= fun keys' ->
      Lwt_list.map_s (ls_lR view) keys' >>= fun path_list_list ->
      return (key :: (List.concat path_list_list))

    module KeySet = Set.Make(struct
      type t = string list
      let compare = Pervasives.compare
    end)

    let ls_lR view key =
      ls_lR view key
      >>= fun all ->
      let keys = List.map to_filename all in
      return (List.fold_left (fun set x -> KeySet.add x set) KeySet.empty keys)

    let watch path callback_fn =
      DB_View.watch_path (db "") (dir_of_filename path) (function
        | `Updated ((_, a), (_, b)) ->
          ls_lR b []
          >>= fun all ->
          Lwt_list.iter_s
            (fun key ->
              info "Watchevent (updated): %s/%s" (Protocol.Path.to_string path) (String.concat "/" key);
              callback_fn (Protocol.Path.of_string_list key)
            ) (KeySet.fold (fun elt acc -> elt :: acc) all [])
        | `Removed ((_, a)) ->
          ls_lR a []
          >>= fun all ->
          Lwt_list.iter_s
            (fun key ->
              info "Watchevent (removed): %s/%s" (Protocol.Path.to_string path) (String.concat "/" key);
              callback_fn (Protocol.Path.of_string_list key);
            ) (KeySet.fold (fun elt acc -> elt :: acc) all [])
        | `Added ((_, a)) ->
          ls_lR a []
          >>= fun all ->
          Lwt_list.iter_s
            (fun key ->
              info "Watchevent (added): %s/%s" (Protocol.Path.to_string path) (String.concat "/" key);
              callback_fn (Protocol.Path.of_string_list key); 
            ) (KeySet.fold (fun elt acc -> elt :: acc) all [])
      ) >>= fun unwatch_dir ->
      DB.watch_key (db "") (value_of_filename path) (fun _ ->
        info "Watchevent (changed): %s" (Protocol.Path.to_string path);
        callback_fn path
      ) >>= fun unwatch_value ->
      return (fun () -> unwatch_value () >>= fun () -> unwatch_dir ())
    
    let unwatch watch = watch ()
  end in

  (* Create the root node *)
  V.create () >>= fun v ->

  fail_on_error (V.write v Protocol.Path.empty Node.({ creator = 0;
                                                       perms = Protocol.ACL.({ owner = 0; other = NONE; acl = []});
                                                       value = "" })) >>= fun () ->
  V.merge v "Adding root node\n\nA xenstore tree always has a root node, owned
  by domain 0." >>= fun ok ->
  ( if not ok then fail (Failure "Failed to merge transaction writing the root node") else return () ) >>= fun () ->
  return (module V: Persistence.PERSISTENCE)

