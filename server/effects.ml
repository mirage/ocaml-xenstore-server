open Lwt
open Sexplib
open Xenstore
open Persistence


let debug fmt = Logging.debug "effects" fmt
let info  fmt = Logging.info "effects" fmt
let error fmt = Logging.error "effects" fmt

type t = unit

let nothing = ()

module Make(V: VIEW) = struct

  let reply_or_fail v hdr req = fail Not_found

  let reply v hdr req =
    Lwt.catch
      (fun () ->
        reply_or_fail v hdr req >>= fun x ->
        return (x, None))
      (fun e ->
        let errorwith ?(error_info = Some (Printexc.to_string e)) code =
          return ((Protocol.Response.Error code, nothing), error_info) in
        match e with
        (*
        | Transaction_again                     -> errorwith "EAGAIN"
        | Limits.Limit_reached                  -> errorwith "EQUOTA"
        | Store.Already_exists p                -> errorwith ~error_info:p "EEXIST"
        | Node.Doesnt_exist p                   -> errorwith ~error_info:(Protocol.Path.to_string p) "ENOENT"
        | Perms.Permission_denied               -> errorwith "EACCES"
        *)
        | Protocol.Path.Invalid_path(p, reason) -> errorwith ~error_info:(Some (Printf.sprintf "%s: %s" p reason)) "EINVAL"
        | Not_found                             -> errorwith "ENOENT"
        | Invalid_argument i                    -> errorwith ~error_info:(Some i) "EINVAL"
        (*
        | Limits.Data_too_big                   -> errorwith "E2BIG"
        | Limits.Transaction_opened             -> errorwith "EQUOTA" *)
        | (Failure "int_of_string")             -> errorwith "EINVAL"
        (*
        | Tree.Unsupported                      -> errorwith "ENOTSUP"
        *)
        | _ ->
          (* quirk: Write <string> (no value) is one of several parse
             failures where EINVAL is expected instead of EIO *)
          errorwith "EINVAL"
      ) >>= fun ((response_payload, side_effects), info) ->
    debug "-> out  %ld %s%s"
      hdr.Protocol.Header.tid
      (Sexp.to_string (Protocol.Response.sexp_of_t response_payload))
      (match info with None -> "" | Some x -> " " ^ x);
    return (response_payload, side_effects)
end
