open Lwt
open Xenstore
open Persistence

type t = unit

let nothing = ()

module Make(V: VIEW) = struct

  let reply v hdr req =
    return (Protocol.Response.Error "EINVAL", nothing)
end
