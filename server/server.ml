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

module Make(T: S.SERVER)(V: Persistence.VIEW) = struct

  type channel_state = {
    next_read_ofs: T.offset;                  (* the next byte to read *)
    next_write_ofs: T.offset;                 (* the next byte to write *)
  }

  module C = Connection.Make(V)
  module E = Effects.Make(V)

  let handle_connection t =
    T.address_of t >>= fun address ->
    let domid = T.domain_of t in

    V.create () >>= fun v ->
    C.create v (address, domid) >>= fun c ->
    let special_path name = [ "tool"; "xenstored"; name; (match Uri.scheme address with Some x -> x | None -> "unknown"); string_of_int (C.index c) ] in

    (* If this is a restart, there will be an existing side_effects entry.
       If this is a new connection, we set an initial state *)
    T.get_read_offset t >>= fun next_read_ofs ->
    T.get_write_offset t >>= fun next_write_ofs ->

    let initial_state = {
      next_read_ofs;
      next_write_ofs;
    } in

    let effects = ref initial_state in

    let origin = Printf.sprintf "Accepted connection %d from domain %d over %s"
      (C.index c) domid (match Uri.scheme address with None -> "unknown protocol" | Some x -> x) in

    V.merge v origin >>= fun ok ->
    if not ok then error "Failed to commit the connection transaction";

    (* Hold this mutex when writing to the output channel: *)
    let write_m = Lwt_mutex.create () in
    Lwt_mutex.lock write_m >>= fun () ->

(*
		let (background_watch_event_flusher: unit Lwt.t) =
			while_lwt true do
        C.pop_watch_events c >>= fun w ->
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
*)
    try_lwt
      let rec loop () =
        (* (Re-)complete any outstanding request. In the event of a crash
           these steps will be re-executed. Each step must therefore be
           idempotent. *)

        (* First execute the idempotent side_effects *)
        let r = !effects in
(*
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
*)
        let perms = C.perms c in
(*
        let origin =
          Printf.sprintf "Resynchronising state for connection %d domain %d"
          (Connection.index c) dom in
        Database.persist ~origin side_effects >>= fun () ->
*)
        (* Second transmit the response packet *)
        T.flush t r.next_write_ofs >>= fun () ->
        Lwt_mutex.unlock write_m;

        (* Read the next request, parse, and compute the response actions.
           The transient in-memory store is updated. Other side-effects are
           computed but not executed. *)
        ( T.recv t r.next_read_ofs >>= function
          | read_ofs, `Ok (hdr, request) ->
          (*
	          C.pop_watch_events_nowait c >>= fun events ->
*)
            Lwt_mutex.lock write_m >>= fun () ->
            (* This will 'commit' updates to the in-memory store: *)
(*
            E.reply v (Some limits) perm c hdr request >>= fun (response, side_effects) ->
*)
            E.reply domid perms hdr request >>= fun (response, side_effects) ->
            let hdr = Protocol.({ hdr with Header.ty = Response.get_ty response}) in
            return (hdr, response, side_effects, read_ofs)
          | read_ofs, `Error msg ->
					  (* quirk: if this is a NULL-termination error then it should be EINVAL *)
            let response = Protocol.Response.Error "EINVAL" in

            Lwt_mutex.lock write_m >>= fun () ->
            let hdr = Header.({ tid = -1l; rid = -1l; ty = Op.Error; len = 0 }) in
            return (hdr, response, Effects.nothing, read_ofs )
        ) >>= fun (hdr, response, side_effects, next_read_ofs) ->

          (* If we crash here then future iterations of the loop will read
             the same request packet. However since every connection is processed
             concurrently we don't expect to compute the same response each time.
             Therefore the choice of which transaction to commit may be made
             differently each time, however the client should be unaware of this. *)

          T.enqueue t hdr response >>= fun next_write_ofs ->
          (* If we crash here the packet will be dropped because we've not persisted
             the side-effects, including the write_ofs value: *)

          effects := { next_read_ofs; next_write_ofs };

          let origin = Printf.sprintf "Transaction from connection %d domain %d"
              (C.index c) domid in
        loop () in
			loop ()
		with e ->
      info "Closing connection %d to domain %d: %s"
        (C.index c) domid (Printexc.to_string e);
    (*
			Lwt.cancel background_watch_event_flusher;
      Mount.unmount connection_path >>= fun e1 ->
      *)
      V.create () >>= fun v ->
			C.destroy v c >>= fun () ->
      V.merge v (Printf.sprintf "Closing connection %d to domain %d\n\nException was: %s"
        (C.index c) domid (Printexc.to_string e)) >>= fun ok ->
      if not ok then error "Failed to commit closing connection transaction";
      (*
      PEffects.destroy peffects >>= fun e3 ->
      Quota.remove dom >>= fun e4 ->
      let origin =
        Printf.sprintf "Closing connection %d domain %d\n\nException was: %s"
          (Connection.index c) dom (Printexc.to_string e) in
      Database.persist ~origin Transaction.(e1 ++ e2 ++ e3 ++ e4) >>= fun () ->
      *)
      T.destroy t

	let serve_forever () =
		T.listen () >>= fun server ->
		T.accept_forever server handle_connection
end
