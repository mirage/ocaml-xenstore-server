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
open Xenstore

(** A byte-level transport over the xenstore Unix domain socket *)

(* The unix domain socket may not exist, or we may get a connection
   refused error if the server is in the middle of restarting. *)
let initial_retry_interval = 0.1 (* seconds *)
let max_retry_interval = 5.0 (* seconds *)
let retry_max = 100 (* attempts *)

let xenstored_socket = ref "/var/run/xenstored/socket"

open Lwt

type 'a t = 'a Lwt.t
let return x = return x
let ( >>= ) m f = m >>= f

(*
module Input = struct
  type t = {
    fd: Lwt_unix.file_descr;
    full_buffer: Cstruct.t; (* the maximum amount of space used for buffering *)
    mutable available: Cstruct.t; (* remaining un-acknowledged data *)
    mutable seq: int64; (* seq of next, un-acknowledged byte. See  *)
  }

  let make fd size =
    let full_buffer = Cstruct.create size in
    let available = Cstruct.sub full_buffer 0 0 in
    let seq = 0L in
    { fd; full_buffer; available; seq }

  let next t =
    if Cstruct.len t.available > 0
    then return (t.seq, t.available)
    else begin
      Lwt_bytes.read t.fd t.full_buffer.Cstruct.buffer t.full_buffer.Cstruct.off t.full_buffer.Cstruct.len >>= fun n ->
      t.available <- Cstruct.sub t.full_buffer 0 n;
      if n = 0
      then fail End_of_file
      else return (t.seq, t.available)
    end

  let ack t seq =
    let bytes_consumed = Int64.(to_int (sub seq t.seq)) in
    t.available <- Cstruct.shift t.available bytes_consumed;
    t.seq <- seq;
    return ()
end
*)

let complete op fd buf =
  let ofs = buf.Cstruct.off in
  let len = buf.Cstruct.len in
  let buf = buf.Cstruct.buffer in
  let rec loop acc fd buf ofs len =
    op fd buf ofs len >>= fun n ->
    let len' = len - n in
    let acc' = acc + n in
    if len' = 0 || n = 0
    then return acc'
    else loop acc' fd buf (ofs + n) len' in
  loop 0 fd buf ofs len >>= fun n ->
  if n = 0 && len <> 0
  then fail End_of_file
  else return ()

(*
module Output = struct
  type t = {
    fd: Lwt_unix.file_descr;
    full_buffer: Cstruct.t;
    mutable available: Cstruct.t;
    mutable seq: int64; (* seq of next byte to write *)
  }

  let make fd size =
    let full_buffer = Cstruct.create size in
    let available = full_buffer in
    let seq = 0L in
    { fd; full_buffer; available; seq }

  let next t =
    if Cstruct.len t.available = 0 then t.available <- t.full_buffer;
    return (t.seq, t.available)

  let ack t seq =
    let bytes_to_send = Int64.(to_int (sub seq t.seq)) in
    complete Lwt_bytes.write t.fd (Cstruct.sub t.available 0 bytes_to_send) >>= fun () ->
    t.available <- Cstruct.shift t.available bytes_to_send;
    t.seq <- seq;
    return ()
end
*)

(* Individual connections *)
type connection = {
  fd: Lwt_unix.file_descr;
  sockaddr: Lwt_unix.sockaddr;
  read_buffer: Cstruct.t;
  write_buffer: Cstruct.t;
  (*
  input: Input.t;
  output: Output.t;
*)
}

let alloc (fd, sockaddr) =
  let read_buffer = Cstruct.create (Protocol.Header.sizeof + Protocol.xenstore_payload_max) in
  let write_buffer = Cstruct.create (Protocol.Header.sizeof + Protocol.xenstore_payload_max) in
  return { fd; sockaddr; read_buffer; write_buffer }

let create () =
  let sockaddr = Lwt_unix.ADDR_UNIX(!xenstored_socket) in
  let fd = Lwt_unix.socket Lwt_unix.PF_UNIX Lwt_unix.SOCK_STREAM 0 in
  let start = Unix.gettimeofday () in
  let rec retry n interval =
    if n > retry_max
    then fail (Failure (Printf.sprintf "Failed to connect to xenstore after %.0f seconds" (Unix.gettimeofday () -. start)))
    else
      Lwt.catch
        (fun () ->
           Lwt_unix.connect fd sockaddr
        ) (fun _ ->
            Lwt_unix.sleep interval
            >>= fun () ->
            retry (n + 1) (interval +. 0.1)
          ) in
  retry 0 initial_retry_interval >>= fun () ->
  alloc (fd, sockaddr)

let destroy { fd } = Lwt_unix.close fd

(*
module Reader = struct
  type t = connection
  let next t = Input.next t.input
  let ack t seq = Input.ack t.input seq
end

module Writer = struct
  type t = connection
  let next t = Output.next t.output
  let ack t seq = Output.ack t.output seq
end

module PBuffer = struct
  type handle = int64

  type t = {
    handle: handle;
    buffer: Cstruct.t;
  }

  let table : (int64, t) Hashtbl.t = Hashtbl.create 10

  open Lwt

  let fresh_handle =
    let next = ref 0L in
    fun () ->
      let this = !next in
      next := Int64.succ !next;
      this

  let create size =
    let handle = fresh_handle () in
    let buffer = Cstruct.create size in
    let t = { handle; buffer } in
    Hashtbl.replace table handle t;
    return t

  let destroy t =
    Hashtbl.remove table t.handle;
    return ()

  let get_cstruct t = t.buffer
  let handle t = t.handle

  let lookup handle =
    if Hashtbl.mem table handle
    then return (Some (Hashtbl.find table handle))
    else return None
end
*)

let read { fd } = complete Lwt_bytes.read fd
let write { fd } = complete Lwt_bytes.write fd

let int_of_file_descr fd =
  let fd = Lwt_unix.unix_file_descr fd in
  let (fd: int) = Obj.magic fd in
  fd

let address_of { fd } =
  let creds = Lwt_unix.get_credentials fd in
  let pid = creds.Lwt_unix.cred_pid in
  Lwt.catch
    (fun () ->
       Lwt_io.with_file ~mode:Lwt_io.input
         (Printf.sprintf "/proc/%d/cmdline" pid)
         (fun ic ->
            Lwt_io.read_line_opt ic
            >>= fun cmdline ->
            match cmdline with
            | Some x -> return x
            | None -> return "unknown")
       >>= fun cmdline ->
       (* Take only the binary name, stripped of directories *)
       let filename =
         try
           let i = String.index cmdline '\000' in
           String.sub cmdline 0 i
         with Not_found -> cmdline in
       let basename = Filename.basename filename in
       let name = Printf.sprintf "%d:%s:%d" pid basename (int_of_file_descr fd) in
       return (Uri.make ~scheme:"unix" ~path:name ())
    ) (fun _ ->
        return (Uri.make ~scheme:"unix" ~path:(string_of_int pid) ())
      )

let domain_of _ = 0

(* Servers which accept connections *)
type server = Lwt_unix.file_descr

let _ =
  (* Make sure a write to a closed fd doesn't cause us to quit
     	   with SIGPIPE *)
  Sys.set_signal Sys.sigpipe Sys.Signal_ignore

let listen () =
  let sockaddr = Lwt_unix.ADDR_UNIX(!xenstored_socket) in
  let fd = Lwt_unix.socket Lwt_unix.PF_UNIX Lwt_unix.SOCK_STREAM 0 in
  Lwt.catch (fun () -> Lwt_unix.unlink !xenstored_socket) (fun _ -> return ())
  >>= fun () ->
  Lwt_unix.bind fd sockaddr;
  Lwt_unix.listen fd 5;
  return fd

let rec accept_forever fd process =
  Lwt_unix.accept_n fd 16
  >>= fun (conns, _exn_option) ->
  let (_: unit Lwt.t list) = List.map (fun x -> alloc x >>= process) conns in
  accept_forever fd process

type offset = unit [@@deriving sexp]

let get_read_offset _ = return ()
let get_write_offset _ = return ()

let flush _ _ = return ()

let enqueue t hdr response =
  let reply_buf = t.write_buffer in
  let payload_buf = Cstruct.shift reply_buf Protocol.Header.sizeof in
  let next = Protocol.Response.marshal response payload_buf in
  let length = next.Cstruct.off - payload_buf.Cstruct.off in
  let hdr = Protocol.Header.({ hdr with len = length }) in
  ignore (Protocol.Header.marshal hdr reply_buf);
  write t (Cstruct.sub t.write_buffer 0 (Protocol.Header.sizeof + length))

let recv t _ =
  let hdr = Cstruct.sub t.read_buffer 0 Protocol.Header.sizeof in
  read t hdr >>= fun () ->
  match Protocol.Header.unmarshal hdr with
  | `Error x -> return ((), `Error x)
  | `Ok x ->
    let payload = Cstruct.sub t.read_buffer Protocol.Header.sizeof x.Protocol.Header.len in
    read t payload >>= fun () ->
    begin match Protocol.Request.unmarshal x payload with
      | `Error y -> return ((), `BadRequest (x, y))
      | `Ok y -> return ((), `Ok (x, y))
    end

module Introspect = struct
  let read { fd } = function
    | [ "readable" ] -> Some (string_of_bool (Lwt_unix.readable fd))
    | [ "writable" ] -> Some (string_of_bool (Lwt_unix.writable fd))
    | _ -> None

  let ls t = function
    | [] -> [ "readable"; "writable" ]
    | _ -> []

  let write _ _ _ = false
end
