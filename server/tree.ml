open Xenstore

module type S = sig
	val exists: 'view Transaction.t -> Perms.t -> Protocol.Path.t -> bool
	val mkdir: 'view Transaction.t -> Limits.t option -> int -> Perms.t -> Protocol.Path.t -> unit
	val read: 'view Transaction.t -> Perms.t -> Protocol.Path.t -> string
	val write: 'view Transaction.t -> Limits.t option -> int -> Perms.t -> Protocol.Path.t -> string -> unit
	val ls: 'view Transaction.t -> Perms.t -> Protocol.Path.t -> string list
	val rm: 'view Transaction.t -> Perms.t -> Protocol.Path.t -> unit
	val getperms: 'view Transaction.t -> Perms.t -> Protocol.Path.t -> Protocol.ACL.t
	val setperms: 'view Transaction.t -> Perms.t -> Protocol.Path.t -> Protocol.ACL.t -> unit
end

exception Unsupported

module Unsupported = struct
	let exists _ _ _ = raise Unsupported
	let mkdir _ _ _ _ _ = raise Unsupported
	let read _ _ _ = raise Unsupported
	let write _ _ _ _ _ _ = raise Unsupported
	let ls _ _ _ = raise Unsupported
	let rm _ _ _ = raise Unsupported
	let getperms _ _ _ = raise Unsupported
	let setperms _ _ _ _ = raise Unsupported
end
