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

type t = {
  name: string;
  buffer: Cstruct.t;
}

let table : (string, t) Hashtbl.t = Hashtbl.create 10

open Lwt

let create name size =
  if Hashtbl.mem table name
  then return (Hashtbl.find table name)
  else begin
    let buffer = Cstruct.create size in
    let t = { name; buffer } in
    Hashtbl.replace table name t;
    return t
  end

let destroy t =
  Hashtbl.remove table t.name;
  return ()

let to_cstruct t = t.buffer
