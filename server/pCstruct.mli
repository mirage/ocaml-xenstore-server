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

val create: int -> t Lwt.t
(** [create name size]: creates a fresh persistent buffer of length [size].
    The buffer is guaranteed to persist across restarts. *)

val destroy: t -> unit Lwt.t
(** [destroy t]: permanently deallocates the persistent buffer [t] *)

val cstruct: t -> Cstruct.t
(** [cstruct t] returns the Cstruct.t associated with [t] *)

type handle = int64
(** A handle which can be persisted in a store, and then used to lookup the
    persistent buffer after a restart. *)

val handle: t -> handle
(** [handle t] returns a unique handle associated with this buffer. The handle
    can be used to retrieve the same buffer in future. *)

val lookup: handle -> t option Lwt.t
(** [lookup handle] returns [Some t] if [handle] refers to an existing [t]
    or [None] if [handle] cannot be found. *)
