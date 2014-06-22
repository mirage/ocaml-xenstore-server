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

val remove: int -> 'view Transaction.side_effects Lwt.t
(** [remove domid] removes all domain-specific limit overrides *)

val limits_of_domain: int -> (Limits.t * 'view Transaction.side_effects) Lwt.t
(** [limits_of_domain domid] returns the limits currently in force
    for domain [domid] *)
