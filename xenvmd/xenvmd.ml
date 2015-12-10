open Sexplib.Std
open Lwt
open Log
open Errors

module Impl = struct
  type 'a t = 'a Lwt.t
  let bind = Lwt.bind
  let return = Lwt.return
  let fail = Lwt.fail
  let handle_failure = Lwt.catch
  let ignore_result _ = Lwt.return ()
  
  type context = {
    stoppers : (unit Lwt.u) list;
    config : Config.Xenvmd.t
  }

  let get context () =
    fatal_error "get" (Vg_io.read (fun x -> return (`Ok x)))

  let create context ~name ~size ~creation_host ~creation_time ~tags =
    Vg_io.write (fun vg ->
      Lvm.Vg.create vg name ~creation_host ~creation_time ~tags size
    ) >>= ignore_result

  let rename context ~oldname ~newname =
    Vg_io.write (fun vg ->
      Lvm.Vg.rename vg oldname newname
    ) >>= ignore_result

  let remove context ~name =
    Vg_io.write (fun vg ->
      Lvm.Vg.remove vg name
    ) >>= ignore_result

  let resize context ~name ~size =
    Vg_io.write (fun vg ->
      Lvm.Vg.resize vg name size
    ) >>= ignore_result

  let set_status context ~name ~readonly =
    Vg_io.write (fun vg ->
      Lvm.Vg.set_status vg name Lvm.Lv.Status.(if readonly then [Read] else [Read; Write])
    ) >>= ignore_result

  let add_tag context ~name ~tag =
    Vg_io.write (fun vg ->
      Lvm.Vg.add_tag vg name tag
    ) >>= ignore_result

  let remove_tag context ~name ~tag =
    Vg_io.write (fun vg ->
      Lvm.Vg.remove_tag vg name tag
    ) >>= ignore_result

  let get_lv context ~name =
    let open Lvm in
    fatal_error "get_lv"
      (Vg_io.read (fun vg ->
        let lv = Lvm.Vg.LVs.find_by_name name vg.Vg.lvs in
        return (`Ok ({ vg with Vg.lvs = Vg.LVs.empty }, lv))
      ))

  let flush context ~name =
    (* We don't know where [name] is attached so we have to flush everything *)
    Host.flush_all () >>=
    Vg_io.sync

  let shutdown context () =
    List.iter (fun u -> Lwt.wakeup u ()) context.stoppers;
    Xenvmd_stats.stop ()
    >>= fun () ->
    Host.shutdown ()
    >>= fun () ->
    Freepool.shutdown ()
    >>= fun () ->
    let (_: unit Lwt.t) =
      Lwt_unix.sleep 1.
      >>= fun () ->
      exit 0 in
    return (Unix.getpid ())

  module Host = struct
    let create context ~name = Host.create name
    let connect context ~name = Host.connect context.config name
    let disconnect context ~cooperative ~name = Host.disconnect ~cooperative name
    let destroy context ~name = Host.destroy name
    let all context () = Host.all ()
  end

  module Fist = struct
    let set context point value = return (Fist.set point value)
    let list context () = return (Fist.list ())
  end
end

module XenvmServer = Xenvm_interface.ServerM(Impl)

open Cohttp_lwt_unix

let handler ~info stoppers config (ch,conn) req body =
  Cohttp_lwt_body.to_string body >>= fun bodystr ->
  XenvmServer.process {Impl.stoppers; config} (Jsonrpc.call_of_string bodystr) >>= fun result ->
  Server.respond_string ~status:`OK ~body:(Jsonrpc.string_of_response result) ()

let maybe_write_pid config =
  match config.Config.Xenvmd.listenPath with
  | None ->
      (* don't need a lock file because we'll fail to bind to the port *)
    Lwt.return ()
  | Some path ->
    let pidfile = path ^ ".lock" in
    info "Writing pidfile to %s" pidfile
    >>= fun () ->
    begin
      match Pidfile.write_pid pidfile with
      | `Ok _ ->
        Lwt.return ()
      | `Error (`Msg msg) ->
        Log.error "Caught exception while writing pidfile: %s" msg >>= fun () ->
        Log.error "The pidfile %s is locked: you cannot start the program twice!" pidfile >>= fun () ->
        Log.error "If the process was shutdown cleanly then verify and remove the pidfile." >>= fun () ->
        exit 1
    end

let get_sanlock path =
  debug "GETTING THE SANLOCK..." >>= fun () ->
  let _start = Unix.gettimeofday () in
  let offset = Sanlock.get_alignment path in
  let lockspace = Sanlock.read_lockspace path in
  let host_id = Random.int 2000 in
  let _ = Sanlock.add_lockspace lockspace host_id in
  let resource = Sanlock.read_resource ~offset path in
  let handle = Sanlock.register () in
  Sanlock.acquire handle resource;
  debug "I GOT THE SANLOCK!!!!" >>= return

let run sanlock_path port sock_path config log_filename =
  let t =
    (match log_filename with
     | Some f ->
       Lwt_log.file ~mode:`Append ~file_name:f () >>= fun logger ->
       Lwt_log.default := logger;
       Lwt.return ()
     | None -> Lwt.return ())
    >>= fun () ->
    (match sanlock_path with
     | Some path ->
       get_sanlock path
     | None -> return ()
    )
    >>= fun () ->
    maybe_write_pid config
    >>= fun () ->
    info "Started with configuration: %s" (Sexplib.Sexp.to_string_hum (Config.Xenvmd.sexp_of_t config))
    >>= fun () ->
    Vg_io.vgopen ~devices:config.Config.Xenvmd.devices
    >>= fun () ->
    Freepool.start Xenvm_interface._journal_name
    >>= fun () ->
    Host.reconnect_all config
    >>= fun () ->
    (* Create a snapshot cache of the metadata for the stats thread *)
    Vg_io.read return >>= fun vg ->
    let stats_vg_cache = ref vg in

    let rec service_stats () =
      Vg_io.read return >>= fun vg -> stats_vg_cache := vg;
      Lwt_unix.sleep 5.
      >>= fun () ->
      service_stats () in

    (* See below for a description of 'stoppers' and 'stop' *)
    let service_http stoppers mode stop =
      let ty = match mode with
        | `TCP (`Port x) -> Printf.sprintf "TCP port %d" x
        | `Unix_domain_socket (`File p) -> Printf.sprintf "Unix domain socket '%s'" p
        | _ -> "<unknown>"
      in
      Lwt.ignore_result (debug "Listening for HTTP request on: %s\n" ty);
      let info = Printf.sprintf "Served by Cohttp/Lwt listening on %s" ty in
      let conn_closed (ch,conn) = () in
      let callback = handler ~info stoppers config in
      let c = Server.make ~callback ~conn_closed () in
      (* Listen for regular API calls *)
      Server.create ~mode ~stop c in

    
    let tcp_mode =
      match config.Config.Xenvmd.listenPort with
      | Some port -> [`TCP (`Port port)]
      | None -> []
    in
    
    begin
      match config.Config.Xenvmd.listenPath with
      | Some p ->
        (* Remove the socket first, if it already exists *)
        Lwt.catch (fun () -> Lwt_unix.unlink p) (fun _ -> Lwt.return ()) >>= fun () -> 
        Lwt.return [ `Unix_domain_socket (`File p) ]            
      | None ->
        Lwt.return []
    end >>= fun unix_mode ->

    let services = tcp_mode @ unix_mode in

    (* stoppers here is a list of type (unit Lwt.u) list, and 'stops'
       is a list of type (unit Lwt.t). Each of the listening Cohttp
       servers is given one of the 'stop' threads, and the whole
       'stoppers' list is passed to every handler. When a 'shutdown'
       is issued, whichever server received the call to shutdown can
       use the 'stoppers' list to shutdown each of the listeners so
       they no longer react to API calls. *)
    let stops,stoppers = List.map (fun _ -> Lwt.wait ()) services |> List.split in
    let threads = List.map2 (service_http stoppers) (tcp_mode @ unix_mode) stops in

    (* start reporting stats to rrdd if we have the config option *)
    begin match config.Config.Xenvmd.rrd_ds_owner with
    | Some owner -> Xenvmd_stats.start owner stats_vg_cache
    | None -> ()
    end;

    Lwt.join ((service_stats ())::threads) in

  Lwt_main.run t

let daemonize config =
  (* Ideally we would bind our sockets before daemonizing to avoid racing
     with the next command, but the conduit API doesn't let us pass a socket
     in. Instead we daemonize in a fork()ed child, and in the parent we wait
     for a connect() to succeed. *)
  if Unix.fork () <> 0 then begin
    let started = ref false in
    let rec wait remaining =
      if remaining = 0 then begin
        Printf.fprintf stderr "Failed to communicate with xenvmd: check the configuration and try again.\n%!";
        exit 1;
      end;
      begin match config.Config.Xenvmd.listenPort with
      | Some port ->
        let s = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
        (try
          Unix.connect s (Unix.ADDR_INET(Unix.inet_addr_of_string "127.0.0.1", port));
          Unix.close s;
          started := true
        with _ ->
          Unix.close s)
      | None -> ()
      end;
      begin match config.Config.Xenvmd.listenPath with
      | Some path ->
        let s = Unix.socket Unix.PF_UNIX Unix.SOCK_STREAM 0 in
        (try
          Unix.connect s (Unix.ADDR_UNIX path);
          Unix.close s;
          started := true
        with e ->
          Unix.close s)
      | None -> ()
      end;
      if not !started then begin
        Unix.sleep 1;
        wait (remaining - 1)
      end in
    wait 30;
    exit 0
  end;
  Lwt_daemon.daemonize ()

let main port sock_path config_path daemon log watchdog sanlock_path =
  let open Config.Xenvmd in
  Sys.(set_signal sigterm (Signal_handle (fun _ -> exit (128+sigterm))));
  let config = t_of_sexp (Sexplib.Sexp.load_sexp config_path) in
  let config = { config with listenPort = match port with None -> config.listenPort | Some x -> Some x } in let config = { config with listenPath = match sock_path with None -> config.listenPath | Some x -> Some x } in

  Lwt_log.add_rule "*" Lwt_log.Debug;

  if daemon && watchdog
  then begin
    (* Sanity check: daemon and watchdog implies that paths all have to be absolute *)
    let fail_if_not_absolute (which,path) =
      if Filename.is_relative path
      then begin
        Printf.fprintf stderr "When using watchdog and daemon mode, all paths must be absolute.\nThe '%s' path is relative: '%s'\n" which path;
        exit 1
      end
    in
    let paths = ["config",config_path] @
                (match config.listenPath with Some x -> [ "listenPath", x] | None -> []) @
                (match log with Some x -> [ "log", x] | None -> []) @
                (List.map (fun dev -> ("config device", dev)) config.devices) in
    List.iter fail_if_not_absolute paths
  end;

  if daemon then daemonize config;

  if watchdog
  then Lwt_main.run (Watchdog.watchdog ())
  else run sanlock_path port sock_path config log
    
open Cmdliner

let info =
  let doc =
    "XenVM LVM daemon" in
  let man = [
    `S "EXAMPLES";
    `P "TODO";
  ] in
  Term.info "xenvm" ~version:"0.1-alpha" ~doc ~man

let watchdog =
  let doc = "Use a watchdog process to look after the daemon" in
  Arg.(value & flag & info ["watchdog"] ~docv:"DAEMON" ~doc)

let sanlock_path =
  let doc = "Block device or file to use as lock to acquire" in
  Arg.(value & opt (some string) None & info [ "sanlock" ] ~docv:"SANLOCK" ~doc)

let port =
  let doc = "TCP port of xenvmd server" in
  Arg.(value & opt (some int) None & info [ "port" ] ~docv:"PORT" ~doc)

let sock_path =
  let doc = "Path to create unix-domain socket for server" in
  Arg.(value & opt (some string) None & info [ "path" ] ~docv:"PATH" ~doc)

let config =
  let doc = "Path to the config file" in
  Arg.(value & opt file "remoteConfig" & info [ "config" ] ~docv:"CONFIG" ~doc)

let daemon =
  let doc = "Detach from the terminal and run as a daemon" in
  Arg.(value & flag & info ["daemon"] ~docv:"DAEMON" ~doc)

let log =
  let doc = "Log to a file rather than syslog/stdout" in
  Arg.(value & opt (some string) None & info [ "log" ] ~docv:"LOGFILE" ~doc)

let cmd = 
  let doc = "Start a XenVM daemon" in
  let man = [
    `S "EXAMPLES";
    `P "TODO";
  ] in
  Term.(pure main $ port $ sock_path $ config $ daemon $ log $ watchdog $ sanlock_path),
  Term.info "xenvmd" ~version:"0.1" ~doc ~man

let _ =
   Random.self_init ();
   match Term.eval cmd with | `Error _ -> exit 1 | _ -> exit 0


