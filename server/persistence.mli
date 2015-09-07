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

module type PERSISTENCE = sig
  type t

  val create: unit -> t Lwt.t

  val read: t -> Perms.t -> Protocol.Path.t -> [ `Ok of Node.contents | `Enoent of Protocol.Path.t | `Eacces of Protocol.Path.t ] Lwt.t

  val list: t -> Protocol.Path.t -> [ `Ok of string list | `Enoent of Protocol.Path.t ] Lwt.t

  val write: t -> Perms.t ->  Protocol.Path.t -> Node.contents -> [ `Ok of unit | `Eacces of Protocol.Path.t ] Lwt.t

  val mem: t -> Protocol.Path.t -> bool Lwt.t

  val rm: t -> Protocol.Path.t -> [ `Ok of unit | `Einval | `Enoent of Protocol.Path.t ] Lwt.t

  val merge: t -> string -> bool Lwt.t

  type watch

  val watch: Protocol.Path.t -> (Protocol.Path.t -> unit Lwt.t) -> watch Lwt.t

  val unwatch: watch -> unit Lwt.t
end

module type EFFECTS = sig
  val reply: int -> Perms.t -> Protocol.Header.t -> (string -> Protocol.Name.t -> unit Lwt.t) -> Protocol.Request.t -> (Protocol.Response.t * unit) Lwt.t

end
