open Lwt

let (>>|=) m f = m >>= function
  | `Ok x -> f x
  | `Enoent _
  | `Einval
  | `Eacces _
  | `Not_implemented _
  | `Conflict as e -> return e

let rec iter_s f = function
  | [] -> return (`Ok ())
  | x :: xs ->
    f x >>|= fun () ->
    iter_s f xs

let fail_on_error m = m >>= function
| `Ok x -> return x
| `Eacces x -> fail (Failure (Printf.sprintf "The path %s cannot be read" x))
| `Enoent x -> fail (Failure (Printf.sprintf "The path %s does not exist" x))
| `Einval -> fail (Failure "The argument was invalid")
| `Not_implemented x -> fail (Failure (Printf.sprintf "The operation %s is not implemented" x))
| `Conflict -> fail (Failure "A transaction conflict happened")
