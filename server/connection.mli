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
open Xenstore

module Watch : sig
  type t = Protocol.Name.t * string [@@deriving sexp]
end

type t

val create: (Uri.t * int) -> t Lwt.t

val index: t -> int
(** a unique id associated with a connection *)

val find_by_domid: int -> t list

val perms: t -> Perms.t
(** The premissions associated with a connection *)

val set_perms: t -> Perms.t -> unit

val destroy: t -> unit Lwt.t
(** [destroy v t] destroys any connection associated with [t] *)
