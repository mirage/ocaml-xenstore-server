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

let debug fmt = Logging.debug "connection" fmt
let info  fmt = Logging.info  "connection" fmt
let error fmt = Logging.debug "connection" fmt

exception End_of_file

type watch = {
	con: t;
	token: string;
	name: Protocol.Name.t;
	mutable count: int;
}

and t = {
	address: Uri.t;
	domid: int;
	domstr: string;
	idx: int; (* unique counter *)
	transactions: (int32, Transaction.t) Hashtbl.t;
	mutable next_tid: int32;
	watches: (Protocol.Name.t, watch list) Hashtbl.t;
	mutable nb_watches: int;
	mutable nb_dropped_watches: int;
	mutable stat_nb_ops: int;
	mutable perm: Perms.t;
	watch_events: (string * string) Queue.t;
	cvar: unit Lwt_condition.t;
	domainpath: Protocol.Name.t;
}

let by_address : (Uri.t, t) Hashtbl.t = Hashtbl.create 128
let by_index   : (int,   t) Hashtbl.t = Hashtbl.create 128

let watches : (string, watch list) Trie.t ref = ref (Trie.create ())

let list_of_watches () =
	Trie.fold (fun path v_opt acc ->
		match v_opt with
		| None -> Printf.sprintf "%s <- None" path :: acc
		| Some vs -> Printf.sprintf "%s <- %s" path (String.concat ", " (List.map (fun v -> v.con.domstr) vs)) :: acc
	) !watches []

let watch_create ~con ~name ~token = { 
	con = con; 
	token = token; 
	name = name;
	count = 0;
}

let get_con w = w.con
 
let number_of_transactions con =
	Hashtbl.length con.transactions

let anon_id_next = ref 1

let destroy address =
	try
		let c = Hashtbl.find by_address address in
		Logging.end_connection ~tid:Transaction.none ~con:c.domstr;
		watches := Trie.map
			(fun watches ->
				match List.filter (fun w -> w.con != c) watches with
				| [] -> None
				| ws -> Some ws
			) !watches;
		Hashtbl.remove by_address address;
		Hashtbl.remove by_index c.idx;
	with Not_found ->
		error "Failed to remove connection for: %s" (Uri.to_string address)

let counter = ref 0

let create (address, dom) =
	if Hashtbl.mem by_address address then begin
		info "Connection.create: found existing connection for %s: closing" (Uri.to_string address);
		destroy address
	end;
	let con = 
	{
		address = address;
		domid = dom;
		idx = !counter;
		domstr = Uri.to_string address;
		transactions = Hashtbl.create 5;
		next_tid = 1l;
		watches = Hashtbl.create 8;
		nb_watches = 0;
		nb_dropped_watches = 0;
		stat_nb_ops = 0;
		perm = Perms.of_domain dom;
		watch_events = Queue.create ();
		cvar = Lwt_condition.create ();
		domainpath = Store.getdomainpath dom;
	}
	in
	incr counter;
	Logging.new_connection ~tid:Transaction.none ~con:con.domstr;
	Hashtbl.replace by_address address con;
	Hashtbl.replace by_index con.idx con;
	con

let restrict con domid =
	con.perm <- Perms.restrict con.perm domid

let get_watches (con: t) name =
	if Hashtbl.mem con.watches name
	then Hashtbl.find con.watches name
	else []

let key_of_name x =
  let open Protocol.Name in match x with
  | Predefined IntroduceDomain -> [ "@introduceDomain" ]
  | Predefined ReleaseDomain   -> [ "@releaseDomain" ]
  | Absolute p -> "" :: (List.map Protocol.Path.Element.to_string (Protocol.Path.to_list p))
  | Relative p -> "" :: (List.map Protocol.Path.Element.to_string (Protocol.Path.to_list p))

let add_watch con name token =
	if con.nb_watches >= (Quota.maxwatch_of_domain con.domid)
	then raise Quota.Limit_reached;

	let l = get_watches con name in
	if List.exists (fun w -> w.token = token) l
	then raise (Store.Already_exists (Printf.sprintf "%s:%s" (Protocol.Name.to_string name) token));
	let watch = watch_create ~con ~token ~name in
	Hashtbl.replace con.watches name (watch :: l);
	con.nb_watches <- con.nb_watches + 1;

	watches :=
		(let key = key_of_name (Protocol.Name.(resolve name con.domainpath)) in
		let ws =
            if Trie.mem !watches key
            then Trie.find !watches key
            else []
        in
        Trie.set !watches key (watch :: ws));
	watch

let del_watch con name token =
	let ws = Hashtbl.find con.watches name in
	let w = List.find (fun w -> w.token = token) ws in
	let filtered = List.filter (fun e -> e != w) ws in
	if List.length filtered > 0 then
		Hashtbl.replace con.watches name filtered
	else
		Hashtbl.remove con.watches name;
	con.nb_watches <- con.nb_watches - 1;

	watches :=
		(let key = key_of_name (Protocol.Name.(resolve name con.domainpath)) in
		let ws = List.filter (fun x -> x != w) (Trie.find !watches key) in
        if ws = [] then
                Trie.unset !watches key
        else
                Trie.set !watches key ws)


let fire_one name watch =
	let name = match name with
		| None ->
			(* If no specific path was modified then we fire the generic watch *)
			watch.name
		| Some name ->
			(* If the watch was registered as a relative path, then we make
			   all the watch events relative too *)
			if Protocol.Name.is_relative watch.name
			then Protocol.Name.(relative name watch.con.domainpath)
			else name in
	let name = Protocol.Name.to_string name in
	let open Xenstore.Protocol in
	Logging.response ~tid:0l ~con:watch.con.domstr (Response.Watchevent(name, watch.token));
	watch.count <- watch.count + 1;
	if Queue.length watch.con.watch_events >= (Quota.maxwatchevent_of_domain watch.con.domid) then begin
		error "domid %d reached watch event quota (%d >= %d): dropping watch %s:%s" watch.con.domid (Queue.length watch.con.watch_events) (Quota.maxwatchevent_of_domain watch.con.domid) name watch.token;
		watch.con.nb_dropped_watches <- watch.con.nb_dropped_watches + 1
	end else begin
		Queue.add (name, watch.token) watch.con.watch_events;
		Lwt_condition.signal watch.con.cvar ()
	end

let fire (op, name) =
	let key = key_of_name name in
	Trie.iter_path
		(fun _ w -> match w with
		| None -> ()
		| Some ws -> List.iter (fire_one (Some name)) ws
		) !watches key;
	
	if op = Protocol.Op.Rm
	then Trie.iter
		(fun _ w -> match w with
		| None -> ()
		| Some ws -> List.iter (fire_one None) ws
		) (Trie.sub !watches key)

let find_next_tid con =
	let ret = con.next_tid in con.next_tid <- Int32.add con.next_tid 1l; ret

let register_transaction con store =
	if Hashtbl.length con.transactions >= (Quota.maxtransaction_of_domain con.domid)
	then raise Quota.Limit_reached;	

	let id = find_next_tid con in
	let ntrans = Transaction.make id store in
	Hashtbl.add con.transactions id ntrans;
	Logging.start_transaction ~tid:id ~con:con.domstr;
	id

let unregister_transaction con tid =
	Hashtbl.remove con.transactions tid

let get_transaction con tid =
	try
		Hashtbl.find con.transactions tid
	with Not_found as e ->
		error "Failed to find transaction %lu on %s" tid con.domstr;
		raise e

let mark_symbols con =
	Hashtbl.iter (fun _ t -> Store.mark_symbols (Transaction.get_store t)) con.transactions

let stats con =
	Hashtbl.length con.watches, con.stat_nb_ops

let debug con =
	let list_watches con =
		let ll = Hashtbl.fold 
			(fun _ watches acc -> List.map (fun watch -> watch.name, watch.token) watches :: acc)
			con.watches [] in
		List.concat ll in

	let watches = List.map (fun (name, token) -> Printf.sprintf "watch %s: %s %s\n" con.domstr (Protocol.Name.to_string name) token) (list_watches con) in
	String.concat "" watches

module Introspect = struct
	include Tree.Unsupported

	let read_connection t perms path c = function
		| [] ->
			""
		| "address" :: [] ->
			Uri.to_string c.address
		| "current-transactions" :: [] ->
			string_of_int (Hashtbl.length c.transactions)
		| "total-operations" :: [] ->
			string_of_int c.stat_nb_ops
		| "current-watch-queue-length" :: [] ->
			string_of_int (Queue.length c.watch_events)
		| "total-dropped-watches" :: [] ->
			string_of_int c.nb_dropped_watches
		| "watch" :: [] ->
			""
		| "watch" :: n :: [] ->
			let n = int_of_string n in
			let all = Hashtbl.fold (fun _ w acc -> w @ acc) c.watches [] in
			if n > (List.length all) then raise (Node.Doesnt_exist path);
			""
		| "watch" :: n :: "name" :: [] ->
			let n = int_of_string n in
			let all = Hashtbl.fold (fun _ w acc -> w @ acc) c.watches [] in
			if n > (List.length all) then raise (Node.Doesnt_exist path);
			Protocol.Name.to_string (List.nth all n).name
		| "watch" :: n :: "token" :: [] ->
			let n = int_of_string n in
			let all = Hashtbl.fold (fun _ w acc -> w @ acc) c.watches [] in
			if n > (List.length all) then raise (Node.Doesnt_exist path);
			(List.nth all n).token
		| "watch" :: n :: "total-events" :: [] ->
			let n = int_of_string n in
			let all = Hashtbl.fold (fun _ w acc -> w @ acc) c.watches [] in
			if n > (List.length all) then raise (Node.Doesnt_exist path);
			string_of_int (List.nth all n).count
		| _ -> raise (Node.Doesnt_exist path)

	let read t (perms: Perms.t) (path: Protocol.Path.t) =
		Perms.has perms Perms.CONFIGURE;
		match Protocol.Path.to_string_list path with
		| [] -> ""
		| "socket" :: [] -> ""
		| "socket" :: idx :: rest ->
			let idx = int_of_string idx in
			if not(Hashtbl.mem by_index idx) then raise (Node.Doesnt_exist path);
			let c = Hashtbl.find by_index idx in
			read_connection t perms path c rest
		| "domain" :: [] -> ""
		| "domain" :: domid :: rest ->
			let address = Uri.make ~scheme:"domain" ~path:domid () in
			if not(Hashtbl.mem by_address address) then raise (Node.Doesnt_exist path);
			let c = Hashtbl.find by_address address in
			read_connection t perms path c rest
		| _ -> raise (Node.Doesnt_exist path)

	let exists t perms path = try ignore(read t perms path); true with Node.Doesnt_exist _ -> false

	let rec between start finish = if start > finish then [] else start :: (between (start + 1) finish)

	let list_connection t perms c = function
		| [] ->
			[ "address"; "current-transactions"; "total-operations"; "watch"; "current-watch-queue-length"; "total-dropped-watches" ]
		| [ "watch" ] ->
			let all = Hashtbl.fold (fun _ w acc -> w @ acc) c.watches [] in
			List.map string_of_int (between 0 (List.length all - 1))
		| [ "watch"; n ] -> [ "name"; "token"; "total-events" ]
		| _ -> []

	let ls t perms path =
		Perms.has perms Perms.CONFIGURE;
		match Protocol.Path.to_string_list path with
		| [] -> [ "socket"; "domain" ]
		| [ "socket" ] ->
			Hashtbl.fold (fun x c acc -> match Uri.scheme x with
                        | Some "unix" -> string_of_int c.idx :: acc
			| _ -> acc) by_address []
		| [ "domain" ] ->
			Hashtbl.fold (fun x _ acc -> match Uri.scheme x with
			| Some "domain" -> Uri.path x :: acc
			| _ -> acc) by_address []
		| "domain" :: domid :: rest ->
			let address = Uri.make ~scheme:"domain" ~path:domid () in
			if not(Hashtbl.mem by_address address) then raise (Node.Doesnt_exist path);
			let c = Hashtbl.find by_address address in
			list_connection t perms c rest
		| "socket" :: idx :: rest ->
			let idx = int_of_string idx in
			if not(Hashtbl.mem by_index idx) then raise (Node.Doesnt_exist path);
			let c = Hashtbl.find by_index idx in
			list_connection t perms c rest
		| _ -> []
end
let _ = Mount.mount (Protocol.Path.of_string "/tool/xenstored/connection") (module Introspect: Tree.S)
