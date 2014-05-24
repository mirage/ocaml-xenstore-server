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
