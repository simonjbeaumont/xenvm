(* Watchdog *)

open Lwt.Infix

(* Logic is largely copied from xapi's *)

let restart_return_code = 123

let fork_and_exec cmd args =
  match Lwt_unix.fork () with
  | 0 ->
    (* I'm the child *)
    Unix.execv cmd (Array.of_list args)
  | n ->
    n

type action =
  | Exit of int (* Exit with this exit code *)
  | Restart (* Restart the subprocess *)
  | Monitor of int (* The subprocess is still running, keep monitoring it *)

let watchdog () =
  let cmd = Unix.readlink "/proc/self/exe" in

  ignore(Unix.sigprocmask Unix.SIG_BLOCK [Sys.sigint]);
  Sys.catch_break false;

  let last_badexit = ref 0. in
  let last_badsig = ref 0. in
  let no_retry_interval = 60. in

  let rec loop current_pid =
    let pid =
      match current_pid with
      | Some pid -> pid
      | None ->
        let overridden_args = [ "--daemon"; "--watchdog" ] in
        let core_args =
          List.filter (fun x -> not (List.mem x overridden_args)) (Array.to_list Sys.argv)
        in
        let pid = fork_and_exec cmd core_args in
        pid
    in
    Lwt.catch
      (fun () ->
         Lwt_unix.waitpid [] pid >>= fun (_, state) ->
         match state with
         | Unix.WEXITED i when i = restart_return_code ->
           Log.info "watchdog: Restarting xenvm process"
           >>= fun () -> Lwt.return Restart
         | Unix.WEXITED i when i = 0 ->
           Log.info "watchdog: Subprocess exited 0: Not restarting"
           >>= fun () -> Lwt.return (Exit 0)
         | Unix.WEXITED i ->
           Log.warn "watchdog: Received bad exit code %d" i
           >>= fun () ->
           let ctime = Unix.gettimeofday () in
           if ctime < (!last_badexit +. no_retry_interval)
           then begin
             Log.error "watchdog: Received 2 bad exits within no-retry-interval. Giving up."
             >>= fun () ->
             Lwt.return (Exit i)
           end else begin
             last_badexit := ctime;
             Lwt.return Restart
           end
         | Unix.WSIGNALED i ->
           Log.info "watchdog: Received signal %d" i
           >>= fun () ->
           if i = Sys.sigsegv || i = Sys.sigpipe then begin
             let ctime = Unix.gettimeofday () in
             if ctime < (!last_badsig +. no_retry_interval) then begin
               Log.error "watchdog: Received 2 bad signals within no-retry-interval. Giving up."
               >>= fun () ->
               Lwt.return (Exit 13)
             end else begin
               last_badsig := ctime;
               Lwt.return Restart
             end
           end else begin
             Log.error "watchdog: not restarting due to signal"
             >>= fun () ->
             Lwt.return (Exit 12)
           end
         | Unix.WSTOPPED i ->
           Log.info "watchdog: received stop code %d" i
           >>= fun () ->
           Lwt_unix.sleep 1.0
           >>= fun () ->
           Unix.kill pid Sys.sigcont;
           Lwt.return (Monitor pid))
      (fun e ->
         match e with
         | Unix.Unix_error(Unix.EINTR,_,_) ->
           Log.info "Caught eintr: retrying"
           >>= fun () ->
           Lwt.return (Monitor pid)
         | _ ->               
           Log.info "Caught exception while waitpiding: %s. Delaying 30 seconds" (Printexc.to_string e)
           >>= fun () ->
           Lwt_unix.sleep 30.0
           >>= fun () ->
           Lwt.return (Monitor pid))
    >>= fun result ->
    match result with
    | Monitor pid -> loop (Some pid)
    | Restart -> loop None
    | Exit n -> exit n
  in               
  loop None
           
          
