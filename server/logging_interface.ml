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
open Lwt
open Logging

module POpBoolMap = PMap.Make(Protocol.Op)(struct type t = bool with sexp end)

let requests =
  POpBoolMap.create [ "tool"; "xenstored"; "log"; "request" ] >>= fun (t, effects) ->
  Database.persist effects >>= fun () ->
  return t

let responses =
  POpBoolMap.create [ "tool"; "xenstored"; "log"; "response" ] >>= fun (t, effects) ->
  Database.persist effects >>= fun () ->
  return t

let request r =
  requests >>= fun requests ->
  POpBoolMap.mem (Protocol.Request.get_ty r) requests >>= function
  | false -> return false
  | true -> POpBoolMap.find (Protocol.Request.get_ty r) requests

let response r =
  responses >>= fun responses ->
  POpBoolMap.mem (Protocol.Response.get_ty r) responses >>= function
  | false -> return false
  | true -> POpBoolMap.find (Protocol.Response.get_ty r) responses

let _ =
  (* Populate every missing key with an explicit 'false' so that we can
     see what the keys are supposed to be *)
  let missing_becomes_false map =
    Lwt_list.fold_left_s (fun side_effects x ->
      POpBoolMap.mem x map >>= function
      | false ->
        POpBoolMap.add x false map >>= fun effects ->
        return Transaction.(side_effects ++ effects)
      | true -> return side_effects
    ) (Transaction.no_side_effects ()) Protocol.Op.all in
  requests >>= fun requests ->
  missing_becomes_false requests >>= fun effects1 ->
  responses >>= fun responses ->
  missing_becomes_false responses >>= fun effects2 ->
  Database.persist Transaction.(effects1 ++ effects2)
