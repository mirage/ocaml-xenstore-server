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
open Lwt
open Xenstore

let debug fmt = Logging.debug "connection" fmt
let info  fmt = Logging.info  "connection" fmt
let error fmt = Logging.debug "connection" fmt

module Watch = struct
  type t = Protocol.Name.t * string [@@deriving sexp]
end

type t = {
  uri: Uri.t;
  domid: int;
  index: int;
  mutable perms: Perms.t;
}

let all = Hashtbl.create 7

let next_index =
  let next = ref 0 in
  fun () ->
    let this = !next in
    incr next;
    this

let create (uri, domid) =
  let index = next_index () in
  let perms = Perms.of_domain domid in
  let c = { index; uri; domid; perms } in
  Hashtbl.replace all uri c;
  return c

let index t = t.index

let perms t = t.perms

let set_perms t perms = t.perms <- perms

let find_by_domid domid =
  Hashtbl.fold (fun _ t acc -> if t.domid = domid then t :: acc else acc) all []

let destroy t =
  Hashtbl.remove all t.uri;
  return ()
