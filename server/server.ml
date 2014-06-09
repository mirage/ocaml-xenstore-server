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
open Protocol

let ( |> ) a b = b a
let ( ++ ) f g x = f (g x)

let debug fmt = Logging.debug "server" fmt
let info fmt  = Logging.info "server" fmt
let error fmt = Logging.error "server" fmt

let fail_on_error = function
| `Ok x -> return x
| `Error x -> fail (Failure x)

module Make = functor(T: S.SERVER) -> struct

  include T

  (* Side effects associated with a request *)
  type side_effects = {
    side_effects: Transaction.side_effects; (* a set of idempotent side-effects *)
    next_read_ofs: offset;                  (* the next byte to read *)
    next_write_ofs: offset;                 (* the next byte to write *)
  } with sexp

  let introspect channel =
    let module Interface = struct
      include Tree.Unsupported
      let read t (perms: Perms.t) (path: Protocol.Path.t) =
        Perms.has perms Perms.CONFIGURE;
        match T.Introspect.read channel (Protocol.Path.to_string_list path) with
        | Some x -> x
        | None -> raise (Node.Doesnt_exist path)
      let exists t perms path = try ignore(read t perms path); true with Node.Doesnt_exist _ -> false
      let ls t perms path =
        Perms.has perms Perms.CONFIGURE;
        T.Introspect.ls channel (Protocol.Path.to_string_list path)
      let write t _ _ perms path v =
        Perms.has perms Perms.CONFIGURE;
        if not(T.Introspect.write channel (Protocol.Path.to_string_list path) v)
        then raise Perms.Permission_denied
    end in
    (module Interface: Tree.S)

  let rec ls_lR channel path =
    let keys = T.Introspect.ls channel path in
    List.concat (List.map (fun k ->
      let path' = path @ [ k ] in
      match T.Introspect.read channel path' with
      | None -> ls_lR channel path'
      | Some v -> (path', v) :: (ls_lR channel path')
    ) keys)

  module PEffects = PRef.Make(struct type t = side_effects with sexp end)

  let handle_connection t =
    lwt address = T.address_of t in
    let dom = T.domain_of t in
    let interface = introspect t in
    Connection.create (address, dom) >>= fun (c, e1) ->
    let special_path name = [ "tool"; "xenstored"; name; (match Uri.scheme address with Some x -> x | None -> "unknown"); string_of_int (Connection.index c) ] in

    (* If this is a restart, there will be an existing side_effects entry.
       If this is a new connection, we set an initial state *)
    T.get_read_offset t >>= fun next_read_ofs ->
    T.get_write_offset t >>= fun next_write_ofs ->

    let initial_state = {
      side_effects = Transaction.no_side_effects();
      next_read_ofs;
      next_write_ofs;
    } in

    PEffects.create (special_path "effects") initial_state >>= fun (peffects, e2) ->

    let connection_path = Protocol.Path.of_string_list (special_path "transport") in
    Mount.mount connection_path interface >>= fun e3 ->

    let origin = Printf.sprintf "Accepted connection %d from domain %d over %s\n\nInitial parameters:\n%s"
      (Connection.index c) dom (match Uri.scheme address with None -> "unknown protocol" | Some x -> x)
      (String.concat "\n" (List.map (fun (path, v) -> Printf.sprintf "  %s: %s" (String.concat "/" path) v) (ls_lR t []))) in

    Database.persist ~origin Transaction.(e1 ++ e2 ++ e3) >>= fun () ->

    (* Hold this mutex when writing to the output channel: *)
    let write_m = Lwt_mutex.create () in
    Lwt_mutex.lock write_m >>= fun () ->

		let (background_watch_event_flusher: unit Lwt.t) =
			while_lwt true do
        Connection.pop_watch_events c >>= fun w ->
        (* XXX: it's possible to lose watch events if we crash here *)
        Lwt_mutex.lock write_m >>= fun () ->
        Lwt_list.iter_s
          (fun (p, tok) ->
            T.enqueue t (Protocol.Response.Watchevent(p, tok)) >>= fun next_write_ofs ->
            flush t next_write_ofs
            (* XXX: now safe to remove event from the queue *)
          ) w >>= fun () ->
        Lwt_mutex.unlock write_m;
        return ()
			done in

    try_lwt
      let rec loop () =
        (* (Re-)complete any outstanding request. In the event of a crash
           these steps will be re-executed. Each step must therefore be
           idempotent. *)

        (* First execute the idempotent side_effects *)
        PEffects.get peffects >>= fun (r, e1) ->
        Quota.limits_of_domain dom >>= fun (limits, e2) ->
        let side_effects = Transaction.(e1 ++ e2) in
        Lwt_list.fold_left_s (fun side_effects w ->
          Connection.watch c (Some limits) w >>= fun effects ->
          return Transaction.(side_effects ++ effects)
        ) side_effects (Transaction.get_watch r.side_effects) >>= fun side_effects ->
        Lwt_list.fold_left_s (fun side_effects w ->
          Connection.unwatch c w >>= fun effects ->
          return Transaction.(side_effects ++ effects)
        ) side_effects (Transaction.get_unwatch r.side_effects) >>= fun side_effects ->
        Lwt_list.fold_left_s (fun side_effects w ->
          Connection.fire (Some limits) w >>= fun effects ->
          return Transaction.(side_effects ++ effects)
        ) side_effects (Transaction.get_watches r.side_effects) >>= fun side_effects ->
        Quota.limits_of_domain dom >>= fun (limits, e) ->
        let side_effects = Transaction.(side_effects ++ e) in
        Connection.PPerms.get (Connection.perm c) >>= fun (perm, e) ->
        let side_effects = Transaction.(side_effects ++ e) in
        Lwt_list.iter_s Introduce.introduce (Transaction.get_domains r.side_effects) >>= fun () ->

        let origin =
          Printf.sprintf "Resynchronising state for connection %d domain %d"
          (Connection.index c) dom in
        Database.persist ~origin side_effects >>= fun () ->

        (* Second transmit the response packet *)
        flush t r.next_write_ofs >>= fun () ->
        Lwt_mutex.unlock write_m;

        (* Read the next request, parse, and compute the response actions.
           The transient in-memory store is updated. Other side-effects are
           computed but not executed. *)
        ( T.recv t r.next_read_ofs >>= function
          | read_ofs, `Ok (hdr, request) ->
	          Connection.pop_watch_events_nowait c >>= fun events ->
            Database.store >>= fun store ->

            Lwt_mutex.lock write_m >>= fun () ->
            (* This will 'commit' updates to the in-memory store: *)
            Call.reply store (Some limits) perm c hdr request >>= fun (response, side_effects) ->
            return (response, side_effects, read_ofs)
          | read_ofs, `Error msg ->
					  (* quirk: if this is a NULL-termination error then it should be EINVAL *)
            let response = Protocol.Response.Error "EINVAL" in
            let side_effects = Transaction.no_side_effects () in

            Lwt_mutex.lock write_m >>= fun () ->
            return (response, side_effects, read_ofs )
        ) >>= fun (response, side_effects, next_read_ofs) ->

          (* If we crash here then future iterations of the loop will read
             the same request packet. However since every connection is processed
             concurrently we don't expect to compute the same response each time.
             Therefore the choice of which transaction to commit may be made
             differently each time, however the client should be unaware of this. *)

          T.enqueue t response >>= fun next_write_ofs ->
          (* If we crash here the packet will be dropped because we've not persisted
             the side-effects, including the write_ofs value: *)

          PEffects.set { side_effects; next_read_ofs; next_write_ofs } peffects >>= fun e ->
          let origin =
            Printf.sprintf "Journalling state for connection %d domain %d"
            (Connection.index c) dom in
          Database.persist ~origin e >>= fun () ->
          let updates = Transaction.get_updates side_effects in
          begin match updates with
          | [] -> return ()
          | _ ->
            let origin = Printf.sprintf "Transaction from connection %d domain %d\n\n%s"
              (Connection.index c) dom
              (String.concat "\n" (List.map Store.string_of_update updates)) in
            Database.persist ~origin side_effects
          end >>= fun () ->
          loop () in
			loop ()
		with e ->
			Lwt.cancel background_watch_event_flusher;
      Mount.unmount connection_path >>= fun e1 ->
			Connection.destroy address >>= fun e2 ->
      PEffects.destroy peffects >>= fun e3 ->
      Quota.remove dom >>= fun e4 ->
      let origin =
        Printf.sprintf "Closing connection %d domain %d\n\nException was: %s"
          (Connection.index c) dom (Printexc.to_string e) in
      Database.persist ~origin Transaction.(e1 ++ e2 ++ e3 ++ e4) >>= fun () ->
      T.destroy t

	let serve_forever persistence =
    Database.initialise persistence >>= fun () ->
		lwt server = T.listen () in
		T.accept_forever server handle_connection
end
