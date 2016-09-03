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
open Sexplib
open Lwt
open Xenstore
open Xenstored
open Error

let debug fmt = Logging.debug "xenstored" fmt
let info  fmt = Logging.info  "xenstored" fmt
let error fmt = Logging.error "xenstored" fmt

let syslog = Lwt_log.syslog ~facility:`Daemon ()

let shutting_down_logger = ref false
let shutdown_logger () =
  shutting_down_logger := true;
  info "Shutting down the logger"

let rec logging_thread daemon logger =
  let log_batch () =
    Logging.get logger
    >>= fun lines ->
    Lwt_list.iter_s
      (fun x ->
        if daemon
        then Lwt_log.log ~logger:syslog ~level:Lwt_log.Notice x
        else Lwt_io.write_line Lwt_io.stdout x
      ) lines in
  log_batch () >>= fun () ->
  if not(!shutting_down_logger)
  then logging_thread daemon logger
  else begin
    (* Grab the last few lines after the shutdown was triggered *)
    log_batch () >>= fun () ->
    Lwt_io.flush_all ()
  end

let default_pidfile = "/var/run/xenstored.pid"

open Cmdliner

let pidfile =
  let doc = "The path to the pidfile, if running as a daemon" in
  Arg.(value & opt string default_pidfile & info [ "pidfile" ] ~docv:"PIDFILE" ~doc)

let daemon =
  let doc = "Run as a daemon" in
  Arg.(value & flag & info [ "daemon" ] ~docv:"DAEMON" ~doc)

let path =
  let doc = "The path to the Unix domain socket" in
  Arg.(value & opt string !Sockets.xenstored_socket & info [ "path" ] ~docv:"PATH" ~doc)

let enable_xen =
  let doc = "Provide service to VMs over shared memory" in
  Arg.(value & flag & info [ "enable-xen" ] ~docv:"XEN" ~doc)

let enable_unix =
  let doc = "Provide service locally over a Unix domain socket" in
  Arg.(value & flag & info [ "enable-unix" ] ~docv:"UNIX" ~doc)

let irmin_path =
  let doc = "Persist xenstore database writes to the specified Irminsule database path" in
  Arg.(value & opt (some string) None & info [ "database" ] ~docv:"DATABASE" ~doc)

let prefer_merge =
  let doc = "Prefer to generate merge commits (default is to always rebase transactions)" in
  Arg.(value & flag & info [ "prefer-merge"] ~docv:"PREFER-MERGE" ~doc)

let ensure_directory_exists dir_needed =
    if not(Sys.file_exists dir_needed && (Sys.is_directory dir_needed)) then begin
      error "The directory (%s) doesn't exist.\n" dir_needed;
      fail (Failure "directory does not exist")
    end else return ()

let program_thread daemon path pidfile enable_xen enable_unix irmin_path prefer_merge () =
  let open Irmin_unix in
  ( match irmin_path with
  | None ->
    info "No database provided: will use an in-memory database";
    let module DB =
      Irmin_mem.Make(Irmin.Contents.String)(Irmin.Tag.String)(Irmin.Hash.SHA1) in
    let config = Irmin_mem.config () in
    return (config, (module DB: Xirmin.DB_S))
  | Some x ->
    let module DB =
      Irmin_git.FS(Irmin.Contents.String)(Irmin.Tag.String)(Irmin.Hash.SHA1) in
    let config = Irmin_git.config ~root:x ~bare:true () in
    return (config, (module DB: Xirmin.DB_S))
  ) >>= fun (config, db_m) ->

  Xirmin.make ~prefer_merge config db_m
  >>= fun v_m ->
  let module V = (val v_m : Persistence.PERSISTENCE) in

  let module UnixServer = Server.Make(Sockets)(V) in
  let module DomainServer = Server.Make(Interdomain)(V) in
  ( if not enable_xen && (not enable_unix) then begin
      error "You must specify at least one transport (--enable-unix and/or --enable-xen)";
      fail (Failure "no transports specified")
    end else return () )
  >>= fun () ->

  ( if enable_unix
    then ensure_directory_exists (Filename.dirname path)
    else return () )
  >>= fun () ->

  ( if daemon
    then ensure_directory_exists (Filename.dirname pidfile)
    else return () )
  >>= fun () ->

  ( if daemon then begin
    Lwt.catch
      (fun () ->
      debug "Writing pidfile %s" pidfile;
      (try Unix.unlink pidfile with _ -> ());
      let pid = Unix.getpid () in
      Lwt_io.with_file pidfile ~mode:Lwt_io.output (fun chan -> Lwt_io.fprintlf chan "%d" pid)
      >>= fun () ->
      return ()
    ) (function Unix.Unix_error(Unix.EACCES, _, _) ->
      error "Permission denied (EACCES) writing pidfile %s" pidfile;
      error "Try a new --pidfile path or running this program with more privileges";
      fail (Failure "EACCES writing pidfile")
      | e -> Lwt.fail e)
  end else begin
    debug "We are not daemonising so no need for a pidfile.";
    return ()
  end)
  >>= fun () ->
  let (a: unit Lwt.t) =
    if enable_unix then begin
      info "Starting server on unix domain socket %s" !Sockets.xenstored_socket;
      Lwt.catch UnixServer.serve_forever
        (function
          | Unix.Unix_error(Unix.EACCES, _, _) as e ->
            error "Permission denied (EACCES) binding to %s" !Sockets.xenstored_socket;
            error "To resolve this problem either run this program with more privileges or change the path.";
            fail e
          | Unix.Unix_error(Unix.EADDRINUSE, _, _) as e ->
            error "The unix domain socket %s is already in use (EADDRINUSE)" !Sockets.xenstored_socket;
            error "To resolve this program either run this program with more privileges (so that it may delete the current socket) or change the path.";
            fail e
          | e ->
            error "Failed to start the unix domain socket server: %s" (Printexc.to_string e);
            fail e)
    end else return () in
  let (b: unit Lwt.t) =
    (*
    if enable_xen then begin
      info "Starting server on xen inter-domain transport";
      DomainServer.serve_forever ()
    end else *) return () in
  Introduce.(introduce { Domain.domid = 0; mfn = 0n; remote_port = 0 }) >>= fun () ->
  debug "Introduced domain 0";
  a
  >>= fun () ->
  debug "Unix domain socket server has shutdown.";
  b
  >>= fun () ->
  debug "Xen interdomain server has shutdown.";
  debug "No servers remaining, shutting down.";
  return ()

let with_logging daemon program_thread =
  info "User-space xenstored version %s starting" Version.version;
  let l_t = logging_thread daemon Logging.logger in
  Lwt.catch program_thread (fun e ->
    error "Main thread threw %s" (Printexc.to_string e);
    return ()) >>= fun () ->
  shutdown_logger ();
  l_t

let program pidfile daemon path enable_xen enable_unix irmin_path prefer_merge=
  Sockets.xenstored_socket := path;
  if daemon then Lwt_daemon.daemonize ();
  try
    Lwt_main.run (with_logging daemon (program_thread daemon path pidfile enable_xen enable_unix irmin_path prefer_merge))
  with e ->
    exit 1

let program_t = Term.(pure program $ pidfile $ daemon $ path $ enable_xen $ enable_unix $ irmin_path $ prefer_merge)

let info =
  let doc = "User-space xenstore server" in
  let man = [
    `S "DESCRIPTION";
    `P "The xenstore service allows Virtual Machines running on top of the Xen hypervisor to share configuration information and setup high-bandwidth shared-memory communication channels for disk and network IO.";
    `P "The xenstore service provides a tree of key=value pairs which may be transactionally updated over a simple wire protocol. Traditionally the service exposes the protocol both over a Unix domain socket (for convenience in domain zero) and over shared memory rings. Note that it is also possible to run xenstore as a xen kernel, for enhanced isolation: see the ocaml-xenstore-xen/xen frontend.";
    `S "EXAMPLES";
    `P "To run as the main xenstore service on a xen host:";
    `P "  $(tname) --daemon --enable-xen --enable-unix";
    `P "To run in userspace only in the foreground for testing on an arbitrary host:";
    `P "  $(tname) --enable-unix --path ./mysocket";
    `S "BUGS";
    `P "Please report bugs at https://github.com/xapi-project/ocaml-xenstore-xen"
  ] in
  Term.info "xenstored" ~version:Version.version ~doc ~man

let () = match Term.eval (program_t, info) with
  | `Ok () -> exit 0
  | _ -> exit 1
