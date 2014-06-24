open Lwt
open Sexplib
open Xenstore
open Protocol
open Persistence
open Error

let debug fmt = Logging.debug "effects" fmt
let info  fmt = Logging.info  "effects" fmt
let error fmt = Logging.error "effects" fmt

type t = unit

let nothing = ()

exception Not_implemented of string

module Make(V: VIEW) = struct

  (* Every write or mkdir will recursively create parent nodes if they
     don't already exist. *)
  let rec mkdir v path creator =
    V.mem v path >>= function
    | true ->
      return (`Ok ())
    | false ->
      (* The root node has been created in Main, otherwise we'd blow the
         stack here. *)
      let dirname = Path.dirname path in
      mkdir v dirname creator >>|= fun () ->
      V.read v dirname >>|= fun node ->
      V.write v path Node.({ node with creator; value = "" }) >>|= fun () ->
      return (`Ok ())

  (* Rm is recursive *)
  let rec rm v path =
    V.list v path >>|= fun names ->
    V.rm v path >>|= fun () ->
    iter_s (rm v) (List.map (fun name -> Path.concat path (Path.of_string name)) names)

  let transactions = Hashtbl.create 16

  let next_transaction_id =
    let next = ref 1l in
    fun () ->
      let this = !next in
      next := Int32.succ !next;
      this

  let with_transaction domid hdr req f =
    if hdr.Header.tid = 0l then begin
      let origin = Printf.sprintf "Domain %d: merging %s" domid (Protocol.Request.to_string req) in
      let rec retry counter =
        V.create () >>= fun v ->
        f v >>|= fun (response, side_effects) ->
        (* No locks are held so this merge might conflict with a parallel
           transaction. We retry forever assuming this is rare. *)
        V.merge v origin >>= function
        | true ->
          return (`Ok (response, side_effects))
        | false ->
          info "rid %ld tid %ld failed to merge after %d attempts"
            hdr.Header.rid hdr.Header.tid counter;
          retry (counter + 1) in
      retry 0
    end else begin
      let v = Hashtbl.find transactions hdr.Header.tid in
      f v
    end

  (* The 'path operations' are the ones which can be done in transactions.
     The other operations are always done outside any current transaction. *)
  let pathop domid perms path op v = match op with
  | Request.Read ->
    V.read v path >>|= fun node ->
    return (`Ok (Response.Read node.Node.value, nothing))
  | Request.Getperms ->
    V.read v path >>|= fun node ->
    return (`Ok (Response.Getperms node.Node.perms, nothing))
  | Request.Setperms perms ->
    V.read v path >>|= fun node ->
    V.write v path { node with Node.perms } >>|= fun () ->
    return (`Ok (Response.Setperms, nothing))
  | Request.Directory ->
    V.list v path >>|= fun names ->
    return (`Ok (Response.Directory names, nothing))
  | Request.Write value ->
    let dirname = Path.dirname path in
    mkdir v dirname domid >>|= fun () ->
    V.read v dirname >>|= fun node ->
    V.write v path Node.({ node with creator = domid; value }) >>|= fun () ->
    return (`Ok (Response.Write, nothing))
  | Request.Mkdir ->
    mkdir v path domid >>|= fun () ->
    return (`Ok (Response.Mkdir, nothing))
  | Request.Rm ->
    rm v path >>|= fun () ->
    return (`Ok (Response.Rm, nothing))

  let reply_or_fail domid perms hdr req = match req with
  | Request.PathOp (path, op) ->
    let path = Path.of_string path in
    with_transaction domid hdr req (pathop domid perms path op)
  | Request.Getdomainpath domid ->
    return (`Ok (Response.Getdomainpath (Printf.sprintf "/local/domain/%d" domid), nothing))
  | Request.Transaction_start ->
    V.create () >>= fun v ->
    let tid = next_transaction_id () in
    Hashtbl.replace transactions tid v;
    return (`Ok (Response.Transaction_start tid, nothing))
  | Request.Transaction_end commit ->
    let v = Hashtbl.find transactions hdr.Header.tid in
    Hashtbl.remove transactions hdr.Header.tid;
    if commit then begin
      let origin = Printf.sprintf "Domain %d: merging transaction %ld" domid hdr.Header.tid in
      V.merge v origin >>= function
      | true ->
        return (`Ok (Response.Transaction_end, nothing))
      | false ->
        return `Conflict
    end else begin
      return (`Ok (Response.Transaction_end, nothing))
    end
  | _ ->
    return (`Not_implemented (Op.to_string hdr.Header.ty))

  let reply domid perms hdr req =
    debug "<-  rid %ld tid %ld %s"
      hdr.Header.rid hdr.Header.tid
      (Sexp.to_string (Request.sexp_of_t req));

    let errorwith ?(error_info = None) code =
      return ((Response.Error code, nothing), error_info) in

    Lwt.catch
      (fun () ->
        reply_or_fail domid perms hdr req >>= function
        | `Ok x -> return (x, None)
        | `Enoent path -> errorwith ~error_info:(Some (Path.to_string path)) "ENOENT"
        | `Not_implemented fn -> errorwith ~error_info:(Some fn) "EINVAL"
        | `Conflict -> errorwith "EAGAIN"
      )
      (fun e ->
        match e with
        (*
        | Transaction_again                     -> errorwith "EAGAIN"
        | Limits.Limit_reached                  -> errorwith "EQUOTA"
        | Store.Already_exists p                -> errorwith ~error_info:p "EEXIST"
        | Node.Doesnt_exist p                   -> errorwith ~error_info:(Protocol.Path.to_string p) "ENOENT"
        | Perms.Permission_denied               -> errorwith "EACCES"
        *)
        | Path.Invalid_path(p, reason)          -> errorwith ~error_info:(Some (Printf.sprintf "%s: %s" p reason)) "EINVAL"
        | Not_found                             -> errorwith "ENOENT"
        | Invalid_argument i                    -> errorwith ~error_info:(Some i) "EINVAL"
        | Not_implemented x                     -> errorwith ~error_info:(Some x) "EINVAL"
        (*
        | Limits.Data_too_big                   -> errorwith "E2BIG"
        | Limits.Transaction_opened             -> errorwith "EQUOTA" *)
        | (Failure "int_of_string")             -> errorwith ~error_info:(Some "int_of_string") "EINVAL"
        (*
        | Tree.Unsupported                      -> errorwith "ENOTSUP"
        *)
        | e ->
          (* quirk: Write <string> (no value) is one of several parse
             failures where EINVAL is expected instead of EIO *)
          errorwith ~error_info:(Some (Printexc.to_string e)) "EINVAL"
      ) >>= fun ((response_payload, side_effects), info) ->
    debug "->  rid %ld tid %ld %s%s"
      hdr.Header.rid hdr.Header.tid
      (Sexp.to_string (Response.sexp_of_t response_payload))
      (match info with None -> "" | Some x -> " " ^ x);
    return (response_payload, side_effects)
end
