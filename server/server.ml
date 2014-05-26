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

(* A complete response to some action: *)
type response = {
  (* response: Protocol.Response.t option;   (* a packet to write to the channel *) *)
  side_effects: Transaction.side_effects; (* a set of idempotent side-effects to effect *)
  read_ofs: int64;                        (* in the past I read this byte *)
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

  module PResponse = PRef.Make(struct type t = response with sexp end)

  (* We store the next whole packet to transmit in this persistent buffer.
     The contents are only valid iff valid <> 0 *)
  cstruct buffer {
    uint16_t length; (* total number of payload bytes in buffer. *)
    uint64_t offset; (* offset in the output stream to write first byte *)
    uint8_t buffer[4112];
    uint8_t valid;   (* 1 means buffer contains valid data *)
  } as little_endian
  let _ = assert(4112 = Protocol.xenstore_payload_max + Protocol.Header.sizeof)

  let handle_connection t =
    lwt address = T.address_of t in
    let dom = T.domain_of t in
    let interface = introspect t in
    Connection.create (address, dom) >>= fun c ->
    let special_path name = [ "tool"; "xenstored"; name; (match Uri.scheme address with Some x -> x | None -> "unknown"); string_of_int (Connection.index c) ] in

    PCstruct.create sizeof_buffer >>= fun poutput ->
    let output = PCstruct.get_cstruct poutput in
    set_buffer_offset output 0L;
    set_buffer_valid output 0;
    PCstruct.create sizeof_buffer >>= fun pinput ->
    let input = PCstruct.get_cstruct pinput in
    set_buffer_valid input 0;

    (* XXX: write the input, output handles to the store *)

    (* Initialise the persistent 'current response' *)
    T.Reader.next t >>= fun (next_read_ofs, _) ->
    let initial_state = {
      side_effects = Transaction.no_side_effects(); (* no side effects to execute *)
      read_ofs = Int64.pred next_read_ofs; (* the last acknowledged read offset *)
    } in

    PResponse.create (special_path "response") initial_state >>= fun presponse ->

    (* Flush any pending output to the channel. This function can suffer a crash and
       restart at any point. On exit, the output buffer is invalid and the whole
       packet has been transmitted. *)
    let rec flush () =
      let valid = get_buffer_valid output in
      if valid = 0 then begin
      Printf.fprintf stderr "flush: no data\n%!";
        return () (* no outstanding data to flush *)
      end else begin
        let offset = get_buffer_offset output in
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
        then set_buffer_valid output 0; (* packet has been sent; invalidate buffer *)
        flush ()
      end in

    (* Enqueue an output packet. This assumes that the output buffer is empty. *)
    let enqueue (* write_ofs *) response =
      if get_buffer_valid output = 1
      then fail (Failure "We cannot marshal a response to the output buffer because it is already full")
      else begin
        let reply_buf = get_buffer_buffer output in
        let payload_buf = Cstruct.shift reply_buf Protocol.Header.sizeof in

        Printf.fprintf stderr "<- %s\n%!" (Sexp.to_string (Response.sexp_of_t response));
        let next = Protocol.Response.marshal response payload_buf in
        let length = next.Cstruct.off - payload_buf.Cstruct.off in
        let hdr = { Header.tid = 0l; rid = 0l; ty = Protocol.Response.get_ty response; len = length} in
        ignore (Protocol.Header.marshal hdr reply_buf);
        T.Writer.next t >>= fun (offset, _) ->
        (*
        if offset <> write_ofs then begin
          Printf.fprintf stderr "marshal dropping data offset=%Ld write_ofs=%Ld" offset write_ofs;
          info "Dropping packet because output stream has advanced. This should only happen over a restart."
        end else begin
        *)
          set_buffer_offset output offset;
          set_buffer_length output (length + Protocol.Header.sizeof);
          set_buffer_valid  output 1;
          (*
        end;
        return Int64.(add offset (of_int (length + Protocol.Header.sizeof)))
        *)
        return ()
      end in

(*
    (* [write response] marshals [response] in the output stream. *)
    let write write_ofs =
      Printf.fprintf stderr "write write_ofs=%Ld\n%!" write_ofs;
		  let m = Lwt_mutex.create () in
      fun response ->
        Lwt_mutex.with_lock m
          (fun () ->
            flush () >>= fun () ->
            ( Logging_interface.response response >>= function
              | true ->
                debug "-> out  %s %ld %s" (Uri.to_string address) 0l (Sexp.to_string (Response.sexp_of_t response));
                return ()
              | false ->
                return () ) >>= fun () ->
            marshal write_ofs response >>= fun write_ofs ->
            Printf.fprintf stderr "calling flush()\n%!";
            flush () >>= fun () ->
            Printf.fprintf stderr "write returning %Ld\n%!" write_ofs;
            return write_ofs
          ) in
*)
    (* [fill ()] fills up input with the next request. This function can crash and
       be restarted at any point. On exit, a whole packet is available for unmarshalling
       and the next packet is still on the ring. *)
    let rec fill () =
      (* if the buffer is invalid then initialise it *)
      ( if get_buffer_valid input = 0 then begin
          T.Reader.next t >>= fun (offset', _) ->
          set_buffer_offset input offset';
          set_buffer_length input 0;
          set_buffer_valid  input 1;
          Printf.fprintf stderr "offset = %Ld\n%!" offset';
          return ()
        end else return () ) >>= fun () ->
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
      fill () >>= fun () ->
      Printf.fprintf stderr "fill complete\n%!";
      let read_ofs' = get_buffer_offset input in
      if read_ofs' <= read_ofs then begin
      Printf.fprintf stderr "dropping packet read_ofs = %Ld read_ofs' = %Ld\n%!" read_ofs read_ofs';
          (* This packet was seen before and we persisted a response. *)
          set_buffer_valid input 0;
          read read_ofs
      end else begin
        let buffer = get_buffer_buffer input in
        fail_on_error (Protocol.Header.unmarshal buffer) >>= fun hdr ->
        let payload = Cstruct.sub buffer Protocol.Header.sizeof hdr.Protocol.Header.len in
        (* return the read_ofs value a future caller would need to get the next packet *)
        let read_ofs'' = Int64.(add read_ofs' (of_int (get_buffer_length input))) in
        match Protocol.Request.unmarshal hdr payload with
        | `Ok r ->
          return (read_ofs', `Ok (hdr, r))
        | `Error e ->
          return (read_ofs, `Error e)
      end in

    let write_m = Lwt_mutex.create () in
    Lwt_mutex.lock write_m >>= fun () ->

		let (background_watch_event_flusher: unit Lwt.t) =
			while_lwt true do
        Connection.pop_watch_events c >>= fun w ->
        (* XXX: it's possible to lose watch events if we crash here *)
        Lwt_mutex.lock write_m >>= fun () ->
        flush () >>= fun () ->
        Lwt_list.iter_s
          (fun (p, t) ->
            enqueue (Protocol.Response.Watchevent(p, t)) >>= fun () ->
            (* XXX: now safe to remove event from the queue *)
            flush ()
          ) w >>= fun () ->
        Lwt_mutex.unlock write_m;
        return ()
			done in
    let connection_path = Protocol.Path.of_string_list (special_path "transport") in
    Mount.mount connection_path interface >>= fun () ->
    try_lwt
      let rec loop () =
        (* (Re-)complete any outstanding request. In the event of a crash
           these steps will be re-executed. Each step must therefore be
           idempotent. *)
        PResponse.get presponse >>= fun r ->
        Quota.limits_of_domain dom >>= fun limits ->
        Transaction.get_watch r.side_effects   |> Lwt_list.iter_s (Connection.watch c (Some limits)) >>= fun () ->
        Transaction.get_unwatch r.side_effects |> Lwt_list.iter_s (Connection.unwatch c)             >>= fun () ->
        Transaction.get_watches r.side_effects |> Lwt_list.iter_s (Connection.fire (Some limits))    >>= fun () ->
        Lwt_list.iter_s Introduce.introduce (Transaction.get_domains r.side_effects) >>= fun () ->
        Database.persist r.side_effects >>= fun () ->
        (* If there is a response to write then write it. We must not send the
           same reply twice because the client may re-use the request id (a small
           integer) *)
        flush () >>= fun () ->
        Lwt_mutex.unlock write_m;

        (* Read the next request, parse, and compute the response actions.
           The transient in-memory store is updated. Other side-effects are
           computed but not executed. *)
        ( read r.read_ofs >>= function
          | read_ofs, `Ok (hdr, request) ->
	          Connection.pop_watch_events_nowait c >>= fun events ->
            Database.store >>= fun store ->
            Quota.limits_of_domain dom >>= fun limits ->
            Connection.PPerms.get (Connection.perm c) >>= fun perm ->

            Lwt_mutex.lock write_m >>= fun () ->
            (* This will 'commit' updates to the in-memory store: *)
            Call.reply store (Some limits) perm c hdr request >>= fun (response, side_effects) ->
            return (response, { side_effects; read_ofs })
          | read_ofs, `Error msg ->
					  (* quirk: if this is a NULL-termination error then it should be EINVAL *)
            let response = Protocol.Response.Error "EINVAL" in
            let side_effects = Transaction.no_side_effects () in

            Lwt_mutex.lock write_m >>= fun () ->
            return (response, { (* response = Some response; *) side_effects; read_ofs })
        ) >>= fun (response, effects) ->

          (* If we crash here then future iterations of the loop will read
             the same request packet. However since every connection is processed
             concurrently we don't expect to compute the same response each time.
             Therefore the choice of which transaction to commit may be made
             differently each time, however the client should be unaware of this. *)

          enqueue response >>= fun () ->

          PResponse.set effects presponse >>= fun () ->
          (* Record the full set of response actions. They'll be executed
             (possibly multiple times) on future loop iterations. *)

          loop () in
			loop ()
		with e ->
			Lwt.cancel background_watch_event_flusher;
			Connection.destroy address >>= fun () ->
      Mount.unmount connection_path >>= fun () ->
      PResponse.destroy presponse >>= fun () ->
      Quota.remove dom >>= fun () ->
      T.destroy t

	let serve_forever persistence =
    Database.initialise persistence >>= fun () ->
		lwt server = T.listen () in
		T.accept_forever server handle_connection
end
