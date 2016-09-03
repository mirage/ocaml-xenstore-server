open Lwt
open Xenstore
open Xenstored
open OUnit

let debug_to_stdout = ref false

let debug fmt = Logging.debug "xenstored" fmt
let info  fmt = Logging.info  "xenstored" fmt
let error fmt = Logging.error "xenstored" fmt

let socket =
  let tmp = Filename.temp_file "xenstore-test" (string_of_int (Unix.getpid ())) in
  Unix.unlink tmp;
  tmp

let _ =
  Sockets.xenstored_socket := socket

module Server = Server.Make(Sockets)
let server =
  let open Irmin_unix in
  let module DB =
    Irmin_mem.Make(Irmin.Contents.String)(Irmin.Ref.String)(Irmin.Hash.SHA1) in
  let config = Irmin_mem.config () in
  Xirmin.make config (module DB)
  >>= fun store ->
  let module V = (val store: Persistence.PERSISTENCE) in
  let module S = Xenstored.Server.Make(Sockets)(V) in
  return (module S: Persistence.SERVER)

let rec logging_thread logger =
  Logging.get logger
  >>= fun lines ->
  Lwt_list.iter_s
    (fun x ->
       if !debug_to_stdout
       then Lwt_io.write_line Lwt_io.stdout x
       else return ()
    ) lines
  >>= fun () ->
  logging_thread logger

let server_thread =
  let (_: 'a) = logging_thread Logging.logger in
  info "Starting test";
  server >>= fun server ->
  let module S = (val server: Xenstored.Persistence.SERVER) in
  S.serve_forever ()

let fail_on_error = function
  | `Ok x -> return x
  | `Error x -> fail (Failure x)

let test (request, response) () =
  let open Sockets in
  create ()
  >>= fun c ->
  let request' = Cstruct.create (String.length request) in
  Cstruct.blit_from_string request 0 request' 0 (String.length request);
  debug "writing request \"%s\"" (String.escaped request);
  write c request'
  >>= fun () ->

  let response' = Cstruct.create 1024 in
  let header = Cstruct.sub response' 0 Protocol.Header.sizeof in
  debug "Reading header";
  read c header
  >>= fun () ->
  debug "Unmarshalling header";
  let payload = Cstruct.shift response' Protocol.Header.sizeof in
  fail_on_error (Protocol.Header.unmarshal header) >>= fun hdr ->
  let payload' = Cstruct.sub payload 0 (min hdr.Protocol.Header.len (Cstruct.len payload)) in
  debug "Reading payload (%d bytes)" (Cstruct.len payload');
  read c payload'
  >>= fun () ->
  let response' = Cstruct.sub response' 0 (payload'.Cstruct.off + payload'.Cstruct.len) in
  debug "read response \"%s\"" (String.escaped (Cstruct.to_string response'));
  assert_equal ~printer:String.escaped response (Cstruct.to_string response');
  return ()

let _ =
  let verbose = ref false in
  Arg.parse [
    "-verbose", Arg.Unit (fun _ -> verbose := true), "Run in verbose mode";
    "-connect", Arg.Set_string Sockets.xenstored_socket, "Connect to a specified xenstored (otherwise use an internal server)";
    "-debug",   Arg.Set debug_to_stdout, "Print debug logging";
  ] (fun x -> Printf.fprintf stderr "Ignoring argument: %s" x)
    "Test xenstore server code";

  let suite = "xenstore" >::: (List.map (fun (x', x, y', y) ->
      Printf.sprintf "%s -> %s" (String.escaped x') (String.escaped y')
      >:: (fun () -> Lwt_main.run (test (x, y) ()))
    ) Messages.all) in
  run_test_tt ~verbose:!verbose suite


