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

let debug fmt = Logging.debug "quota" fmt
let info  fmt = Logging.info  "quota" fmt
let warn  fmt = Logging.warn  "quota" fmt

module PerDomain = PMap.Make(struct
  type t = int
  let of_string = int_of_string
  let to_string = string_of_int
end)(struct type t = int with sexp end)

let prefix = [ "tool"; "xenstored"; "quota" ]

let maxent, maxent_wakener = Lwt.task ()
let maxsize, maxsize_wakener = Lwt.task ()
let maxwatch, maxwatch_wakener = Lwt.task ()
let maxtransaction, maxtransaction_wakener = Lwt.task ()
let maxwatchevent, maxwatchevent_wakener = Lwt.task ()
let maxent_overrides, maxent_overrides_wakener = Lwt.task ()
let maxwatch_overrides, maxwatch_overrides_wakener = Lwt.task ()
let maxtransaction_overrides, maxtransaction_overrides_wakener = Lwt.task ()

let _ =
  PRef.Int.create (prefix @ [ "default"; "number-of-entries" ]) 10000 >>= fun (maxent, e1) ->
  PRef.Int.create (prefix @ [ "default"; "entry-length" ]) 4096 >>= fun (maxsize, e2) ->
  PRef.Int.create (prefix @ [ "default"; "number-of-registered-watches" ]) 50 >>= fun (maxwatch, e3) ->
  PRef.Int.create (prefix @ [ "default"; "number-of-active-transactions" ]) 20 >>= fun (maxtransaction, e4) ->
  PRef.Int.create (prefix @ [ "default"; "number-of-queued-watch-events" ]) 256 >>= fun (maxwatchevent, e5) ->
  PerDomain.create (prefix @ [ "number-of-entries" ]) >>= fun (maxent_overrides, e6) ->
  PerDomain.create (prefix @ [ "number-of-registered-watches" ]) >>= fun (maxwatch_overrides, e7) ->
  PerDomain.create (prefix @ [ "number-of-active-transactions" ]) >>= fun (maxtransaction_overrides, e8) ->
  Database.persist ~origin:"Initialise the global quota settings." Transaction.(e1 ++ e2 ++ e3 ++ e4 ++ e5 ++ e6 ++ e7 ++ e8) >>= fun () ->
  Lwt.wakeup maxent_wakener maxent;
  Lwt.wakeup maxsize_wakener maxsize;
  Lwt.wakeup maxwatch_wakener maxwatch;
  Lwt.wakeup maxtransaction_wakener maxtransaction;
  Lwt.wakeup maxwatchevent_wakener maxwatchevent;
  Lwt.wakeup maxent_overrides_wakener maxent_overrides;
  Lwt.wakeup maxwatch_overrides_wakener maxwatch_overrides;
  Lwt.wakeup maxtransaction_overrides_wakener maxtransaction_overrides;
  return ()

let remove domid =
  maxent_overrides >>= fun maxent_overrides ->
  maxwatch_overrides >>= fun maxwatch_overrides ->
  maxtransaction_overrides >>= fun maxtransaction_overrides ->
  PerDomain.remove domid maxent_overrides >>= fun effects1 ->
  PerDomain.remove domid maxwatch_overrides >>= fun effects2 ->
  PerDomain.remove domid maxtransaction_overrides >>= fun effects3 ->
  return Transaction.(effects1 ++ effects2 ++ effects3)

(* A snapshot of the current state for a given domid, needed to check
   for quota violations during a transaction. *)
let limits_of_domain domid =
  maxent >>= fun maxent ->
  maxsize >>= fun maxsize ->
  maxwatch >>= fun maxwatch ->
  maxwatchevent >>= fun maxwatchevent ->
  maxtransaction >>= fun maxtransaction ->
  maxent_overrides >>= fun maxent_overrides ->
  maxwatch_overrides >>= fun maxwatch_overrides ->
  maxtransaction_overrides >>= fun maxtransaction_overrides ->
  PerDomain.mem domid maxent_overrides >>= fun b ->
  (if b then
          PerDomain.find domid maxent_overrides
  else PRef.Int.get maxent) >>= fun number_of_entries ->
  PRef.Int.get maxsize >>= fun entry_length ->
  PerDomain.mem domid maxwatch_overrides >>= fun b ->
  (if b then PerDomain.find domid maxwatch_overrides else PRef.Int.get maxwatch) >>= fun number_of_registered_watches ->
  PerDomain.mem domid maxtransaction_overrides >>= fun b ->
  (if b then PerDomain.find domid maxtransaction_overrides else PRef.Int.get maxtransaction) >>= fun number_of_active_transactions ->
  PRef.Int.get maxwatchevent >>= fun number_of_queued_watch_events ->
  return { Limits.number_of_entries; entry_length; number_of_registered_watches; number_of_active_transactions; number_of_queued_watch_events }
