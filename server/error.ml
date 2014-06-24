open Lwt

let (>>|=) m f = m >>= function
  | `Ok x -> f x
  | `Enoent _
  | `Not_implemented _
  | `Conflict as e -> return e

let fail_on_error m = m >>= function
| `Ok x -> return x
| `Enoent x -> fail (Failure (Printf.sprintf "The path %s does not exist" x))
| `Not_implemented x -> fail (Failure (Printf.sprintf "The operation %s is not implemented" x))
| `Conflict -> fail (Failure "A transaction conflict happened")
