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

(* Side effects associated with a request *)
type side_effects = {
  side_effects: Transaction.side_effects; (* a set of idempotent side-effects *)
  (* These 2 fields should be abstract tokens from shared-memory-ring *)
  next_read_ofs: int64;                   (* the next byte to read *)
  next_write_ofs: int64;                  (* the next byte to write *)
} with sexp

module Make = functor(T: S.TRANSPORT) -> struct

  include T

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

  (* We store the next whole packet to transmit in this persistent buffer.
     The contents are only valid iff valid <> 0 *)
  cstruct buffer {
    uint16_t length; (* total number of payload bytes in buffer. *)
    uint64_t offset; (* offset in the output stream to write first byte *)
    uint8_t buffer[4112];
  } as little_endian
  let _ = assert(4112 = Protocol.xenstore_payload_max + Protocol.Header.sizeof)

  let handle_connection t =
    lwt address = T.address_of t in
    let dom = T.domain_of t in
    let interface = introspect t in
    Connection.create (address, dom) >>= fun (c, e1) ->
    let special_path name = [ "tool"; "xenstored"; name; (match Uri.scheme address with Some x -> x | None -> "unknown"); string_of_int (Connection.index c) ] in

    (* If this is a restart, there will be an existing side_effects entry.
       If this is a new connection, we set an initial state *)
    T.Reader.next t >>= fun (next_read_ofs, _) ->
    T.Writer.next t >>= fun (next_write_ofs, _) ->
    let initial_state = {
      side_effects = Transaction.no_side_effects();
      next_read_ofs;
      next_write_ofs;
    } in

    PEffects.create (special_path "effects") initial_state >>= fun (peffects, e2) ->

    let connection_path = Protocol.Path.of_string_list (special_path "transport") in
    Mount.mount connection_path interface >>= fun e3 ->

    PBuffer.create sizeof_buffer >>= fun poutput ->
    let output = PBuffer.get_cstruct poutput in
    set_buffer_offset output 0L;
    set_buffer_length output 0;
    (* This output buffer is safe to flush *)
    PBuffer.create sizeof_buffer >>= fun pinput ->
    let input = PBuffer.get_cstruct pinput in
    set_buffer_offset input 0L;
    set_buffer_length input 0;

    (* XXX: write the input, output handles to the store *)

    let origin = Printf.sprintf "Accepted connection %d from domain %d over %s\n\nInitial parameters:\n%s"
      (Connection.index c) dom (match Uri.scheme address with None -> "unknown protocol" | Some x -> x)
      (String.concat "\n" (List.map (fun (path, v) -> Printf.sprintf "  %s: %s" (String.concat "/" path) v) (ls_lR t []))) in

    Database.persist ~origin Transaction.(e1 ++ e2 ++ e3) >>= fun () ->

    (* Flush any pending output to the channel. This function can suffer a crash and
       restart at any point. On exit, the output buffer is invalid and the whole
       packet has been transmitted. *)
    let rec flush next_write_ofs =

        let offset = get_buffer_offset output in
        if next_write_ofs <> offset then begin
          Printf.fprintf stderr "flush: packet is from the future\n%!";
          return ()
        end else begin
          let length = get_buffer_length output in
          T.Writer.next t >>= fun (offset', space') ->
          let space = Cstruct.sub (get_buffer_buffer output) 0 length in
          (* write as much of (offset, space) into (offset', space') as possible *)
          (* 1. skip any already-written data, check we haven't lost any *)
          ( if offset < offset'
            then
              let to_skip = Int64.sub offset' offset in
              return (offset', Cstruct.shift space (Int64.to_int to_skip))
            else
              if offset > offset'
              then fail (Failure (Printf.sprintf "Some portion of the output stream has been skipped. Our data starts at %Ld, the stream starts at %Ld" offset offset'))
              else return (offset, space)
          ) >>= fun (offset, space) ->
          (* 2. write as much as there is space for *)
          let to_write = min (Cstruct.len space) (Cstruct.len space') in
          Cstruct.blit space 0 space' 0 to_write;
          let next_offset = Int64.(add offset' (of_int to_write)) in
          Printf.fprintf stderr "write acking %Ld\n%!" next_offset;
          T.Writer.ack t next_offset >>= fun () ->
          let remaining = to_write - (Cstruct.len space) in
          if remaining = 0
          then return ()
          else flush next_write_ofs
      end in

    (* Enqueue an output packet. This assumes that the output buffer is empty. *)
    let enqueue response =
        let reply_buf = get_buffer_buffer output in
        let payload_buf = Cstruct.shift reply_buf Protocol.Header.sizeof in

        ( Logging_interface.response response >>= function
          | true ->
            debug "-> out  %s %ld %s" (Uri.to_string address) 0l (Sexp.to_string (Response.sexp_of_t response));
            return ()
          | false ->
            return () ) >>= fun () ->

        Printf.fprintf stderr "<- %s\n%!" (Sexp.to_string (Response.sexp_of_t response));
        let next = Protocol.Response.marshal response payload_buf in
        let length = next.Cstruct.off - payload_buf.Cstruct.off in
        let hdr = { Header.tid = 0l; rid = 0l; ty = Protocol.Response.get_ty response; len = length} in
        ignore (Protocol.Header.marshal hdr reply_buf);
        T.Writer.next t >>= fun (offset, _) ->
        set_buffer_length output (length + Protocol.Header.sizeof);
        set_buffer_offset output offset;
        return offset in

    (* [fill ()] fills up input with the next request. This function can crash and
       be restarted at any point. On exit, a whole packet is available for unmarshalling
       and the next packet is still on the ring. *)
    let rec fill () =
      (* compute the maximum number of bytes we definitely need *)
      let length = get_buffer_length input in
      let offset = get_buffer_offset input in
      let buffer = get_buffer_buffer input in
      ( if length < Protocol.Header.sizeof
        then return (Protocol.Header.sizeof - length)
        else begin
          (* if we have the header then we know how long the payload is *)
          fail_on_error (Protocol.Header.unmarshal buffer) >>= fun hdr ->
          return (Protocol.Header.sizeof + hdr.Protocol.Header.len - length)
        end ) >>= fun bytes_needed ->
      Printf.fprintf stderr "bytes_needed = %d\n%!" bytes_needed;
      if bytes_needed = 0
      then return () (* packet ready for reading, stream positioned at next packet *)
      else begin
        let offset = Int64.(add offset (of_int length)) in
        let space = Cstruct.sub buffer length bytes_needed in
        T.Reader.next t >>= fun (offset', space') ->
        (* 1. skip any already-read data, check we haven't lost any *)
        ( if offset < offset'
          then fail (Failure (Printf.sprintf "Some portion of the input stream has been skipped. We need data from %Ld, the stream starts at %Ld" offset offset'))
          else
            if offset > offset'
            then
              let to_skip = Int64.sub offset offset' in
              return (offset, Cstruct.shift space' (Int64.to_int to_skip))
            else return (offset, space') ) >>= fun (offset', space') ->
        (* 2. read as much as there is space for *)
        let to_copy = min (Cstruct.len space) (Cstruct.len space') in
        Printf.fprintf stderr "copying %d bytes\n%!" to_copy;
        Cstruct.blit space' 0 space 0 to_copy;
        set_buffer_length input (length + to_copy);

        let next_offset = Int64.(add offset' (of_int to_copy)) in
        T.Reader.ack t next_offset >>= fun () ->
        Printf.fprintf stderr "acking %Ld\n%!" next_offset;
        fill ()
      end in

    let rec read read_ofs =
      Printf.fprintf stderr "read read_ofs=%Ld\n%!" read_ofs;
      Printf.fprintf stderr "fill()\n%!";
      if get_buffer_offset input <> read_ofs then begin
        Printf.fprintf stderr "fill dropping prevously buffered packet\n%!";
        set_buffer_length input 0;
        set_buffer_offset input read_ofs;
      end;
      fill () >>= fun () ->
      Printf.fprintf stderr "fill complete\n%!";

        let buffer = get_buffer_buffer input in
        fail_on_error (Protocol.Header.unmarshal buffer) >>= fun hdr ->
        let payload = Cstruct.sub buffer Protocol.Header.sizeof hdr.Protocol.Header.len in
        (* return the read_ofs value a future caller would need to get the next packet *)
        let read_ofs' = Int64.(add read_ofs (of_int (get_buffer_length input))) in
        match Protocol.Request.unmarshal hdr payload with
        | `Ok r ->
          return (read_ofs', `Ok (hdr, r))
        | `Error e ->
          return (read_ofs', `Error e)
      in

    (* Hold this mutex when writing to the output channel: *)
    let write_m = Lwt_mutex.create () in
    Lwt_mutex.lock write_m >>= fun () ->

		let (background_watch_event_flusher: unit Lwt.t) =
			while_lwt true do
        Connection.pop_watch_events c >>= fun w ->
        (* XXX: it's possible to lose watch events if we crash here *)
        Lwt_mutex.lock write_m >>= fun () ->
        Lwt_list.iter_s
          (fun (p, t) ->
            enqueue (Protocol.Response.Watchevent(p, t)) >>= fun next_write_ofs ->
            flush next_write_ofs
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
        flush r.next_write_ofs >>= fun () ->
        Lwt_mutex.unlock write_m;

        (* Read the next request, parse, and compute the response actions.
           The transient in-memory store is updated. Other side-effects are
           computed but not executed. *)
        ( read r.next_read_ofs >>= function
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

          enqueue response >>= fun next_write_ofs ->
          (* If we crash here the packet will be dropped because we've not persisted
             the side-effects, including the write_ofs value: *)

          PEffects.set { side_effects; next_read_ofs; next_write_ofs } peffects >>= fun side_effects ->
          Database.persist side_effects >>= fun () ->
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
