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

type t
(** A persistent buffer *)

val create: string -> int -> t Lwt.t
(** [create name size]: returns the persistent buffer of length [size]
    associated with [name], creating it if it doesn't alraedy exist.
    The buffer is guaranteed to persist across restarts. *)

val destroy: t -> unit Lwt.t
(** [destroy t]: permanently deallocates the persistent buffer [t] *)

val to_cstruct: t -> Cstruct.t
(** [to_cstruct t] returns the Cstruct.t associated with [t] *)
