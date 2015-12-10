open Cohttp_lwt_unix
open Lwt
open Lvm
open Xenvm_common
open Errors

let add_prefix x xs = List.map (function
  | [] -> []
  | y :: ys -> (x ^ "/" ^ y) :: ys
) xs

let table_of_pv_header prefix pvh = add_prefix prefix [
  [ "id"; Uuid.to_string pvh.Label.Pv_header.id; ];
  [ "device_size"; Int64.to_string pvh.Label.Pv_header.device_size; ];
  [ "extents"; string_of_int (List.length pvh.Label.Pv_header.extents) ];
  [ "metadata_areas"; string_of_int (List.length pvh.Label.Pv_header.metadata_areas) ];
]

let table_of_pv pv = add_prefix (Pv.Name.to_string pv.Pv.name) [
  [ "name"; Pv.Name.to_string pv.Pv.name; ];
  [ "id"; Uuid.to_string pv.Pv.id; ];
  [ "status"; String.concat ", " (List.map Pv.Status.to_string pv.Pv.status) ];
  [ "size_in_sectors"; Int64.to_string pv.Pv.size_in_sectors ];
  [ "pe_start"; Int64.to_string pv.Pv.pe_start ];
  [ "pe_count"; Int64.to_string pv.Pv.pe_count; ]
] @ (table_of_pv_header (Pv.Name.to_string pv.Pv.name ^ "/label") pv.Pv.label.Label.pv_header)

let table_of_lv lv = add_prefix lv.Lv.name [
  [ "name"; lv.Lv.name; ];
  [ "id"; Uuid.to_string lv.Lv.id; ];
  [ "tags"; String.concat ", " (List.map Name.Tag.to_string lv.Lv.tags) ];
  [ "status"; String.concat ", " (List.map Lv.Status.to_string lv.Lv.status) ];
  [ "segments"; string_of_int (List.length lv.Lv.segments) ];
]

let table_of_vg vg =
  let pvs = List.flatten (List.map table_of_pv vg.Vg.pvs) in
  let lvs = Vg.LVs.fold (fun _ lv acc -> lv :: acc) vg.Vg.lvs [] in
  let lvs = List.flatten (List.map table_of_lv lvs) in [
  [ "name"; vg.Vg.name ];
  [ "id"; Uuid.to_string vg.Vg.id ];
  [ "status"; String.concat ", " (List.map Vg.Status.to_string vg.Vg.status) ];
  [ "extent_size"; Int64.to_string vg.Vg.extent_size ];
  [ "max_lv"; string_of_int vg.Vg.max_lv ];
  [ "max_pv"; string_of_int vg.Vg.max_pv ];
] @ pvs @ lvs @ [
  [ "free_space"; Int64.to_string (Pv.Allocator.size vg.Vg.free_space) ];
]

let format config name filenames =
    let t =
      let module Vg_IO = Vg.Make(Log)(Block)(Time)(Clock) in
      let open Xenvm_interface in
      (* 4 MiB per volume *)
      let size = Int64.(mul 4L (mul 1024L 1024L)) in
      Lwt_list.map_s
        (fun filename ->
          Block.connect filename
          >>= function
          | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" filename))
          | `Ok x -> return x
        ) filenames
      >>= fun blocks ->
      let pvs = List.mapi (fun i block ->
        let name = match Pv.Name.of_string (Printf.sprintf "pv%d" i) with
        | `Ok x -> x
        | `Error (`Msg x) -> failwith x in
        (name,block)
      ) blocks in
      let creation_host = Unix.gethostname () in
      let creation_time = Unix.gettimeofday () |> Int64.of_float in
      Vg_IO.format name ~creation_host ~creation_time ~magic:`Journalled pvs >>|= fun () ->
      Vg_IO.connect (List.map snd pvs) `RW
      >>|= fun vg ->
      (return (Vg.create (Vg_IO.metadata_of vg) _journal_name size ~creation_host ~creation_time))
      >>|= fun (_, op) ->
      (return (Vg.create (Vg_IO.metadata_of vg) _sanlock_lease_vg_name size ~creation_host ~creation_time))
      >>|= fun (_, op') ->
      Vg_IO.update vg [ op; op' ]
      >>|= fun () ->
      return () in
  Lwt_main.run t

(* LVM locking code can be seen here:
 * https://git.fedorahosted.org/cgit/lvm2.git/tree/lib/misc/lvm-flock.c#n141 *)
let with_lvm_lock config vg_name f =
  if config.mock_dm then f () else
    let lock_path = Filename.concat "/run/lock/lvm" ("V_" ^ vg_name ^ ":aux") in
    with_flock lock_path f

(* Change the label on the PV and revert it if the function [f] fails *)
let with_label_change block magic f =
  let module Label_IO = Label.Make(Block) in
  let (>>:=) m f = m >>= function `Ok x -> f x | `Error (`Msg e) -> fail (Failure e) in
  Label_IO.read block >>:= fun orig_label ->
  let label_header = Label.Label_header.create magic in
  let new_label = { orig_label with Label.label_header } in
  Label_IO.write block new_label >>:= fun () ->
  Lwt.catch f (fun exn -> Label_IO.write block orig_label >>:= fun () -> fail exn)


let upgrade config physical_device (vg_name, _) =
  let module Vg_IO = Vg.Make(Log)(Block)(Time)(Clock) in
  let t =
    with_lvm_lock config vg_name (fun () ->
      with_block physical_device (fun block ->
        (* Change the label magic to be Journalled to hide from LVM *)
        with_label_change block `Journalled (fun () ->
          (* Create the redo log, first reconnect to pick up the label changes *)
          Vg_IO.connect ~flush_interval:0. [ block ] `RW >>|= fun vg ->
          let creation_host = Unix.gethostname () in
          let creation_time = Unix.gettimeofday () |> Int64.of_float in
          let size = Int64.mul 4L mib in
          begin match Vg.create (Vg_IO.metadata_of vg) Xenvm_interface._journal_name
            size ~creation_host ~creation_time with
          | `Ok (_, op) -> `Ok [ op ]
          | `Error (`DuplicateLV _) -> `Ok []
          | `Error e -> `Error e
          end >>*= fun ops ->
          Vg_IO.update vg ops >>|= fun () ->
          return ()
        )
      )
    ) in
  Lwt_main.run t

let downgrade config physical_device (vg_name, _) =
  let module Vg_IO = Vg.Make(Log)(Block)(Time)(Clock) in
  let t =
    with_lvm_lock config vg_name (fun () ->
      with_block physical_device (fun block ->
        (* Change the label magic to be LVM to hide from XenVM *)
        with_label_change block `Lvm (fun () ->
          (* Destroy the redo log, first reconnect to pick up the label changes *)
          Vg_IO.connect ~flush_interval:0. [ block ] `RW >>|= fun vg ->
          begin match Vg.remove (Vg_IO.metadata_of vg) Xenvm_interface._journal_name with
          | `Ok (_, op) -> `Ok [ op ]
          | `Error (`UnknownLV _) -> `Ok []
          | `Error e -> `Error e
          end >>*= fun ops ->
          Vg_IO.update vg ops >>|= fun () ->
          return ()
        )
      )
    ) in
  Lwt_main.run t

let dump config filenames =
    let t =
      let module Vg_IO = Vg.Make(Log)(Block)(Time)(Clock) in
      let open Xenvm_interface in
      Lwt_list.map_s
        (fun filename ->
          Block.connect filename
          >>= function
          | `Error _ -> fail (Failure (Printf.sprintf "Failed to open %s" filename))
          | `Ok x -> return x
        ) filenames
      >>= fun blocks ->
      Vg_IO.connect blocks `RO
      >>|= fun vg ->
      let md = Vg_IO.metadata_of vg in
      let buf = Cstruct.create (64 * 1024 * 1024) in
      let next = Vg.marshal md buf in
      let buf = Cstruct.(sub buf 0 ((len buf) - (len next))) in
      let txt = Cstruct.to_string buf in
      output_string Pervasives.stdout txt;
      output_string Pervasives.stdout "\n";
      return () in
  Lwt_main.run t

let host_create copts (vg_name,_) host =
  let t =
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    Client.Host.create host in
  Lwt_main.run t
let host_connect copts (vg_name,_) host =
  let t =
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    Client.Host.connect host in
  Lwt_main.run t
let host_disconnect copts (vg_name,_) uncooperative host =
  let t =
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    let rec retry n =
      Lwt.catch
        (fun () ->
           Client.Host.disconnect ~cooperative:(not uncooperative) ~name:host)
        (fun e ->
           Printf.printf "Caught exception: %s\n%!" (Printexc.to_string e);
           match e with
           | Xenvm_interface.HostStillConnecting _ ->
             if n = 0 then Lwt.fail e else begin
               Printf.printf "Exception is non-fatal: retrying\n%!";
               Lwt_unix.sleep 5.0
               >>= fun () ->
               retry (n-1)
             end
           | _ ->
             Printf.printf "Giving up\n%!";
             Lwt.return ()
        )
    in
    retry 12 in
  Lwt_main.run t 
let host_dump copts (vg_name,_) physical_device host =
  let t =
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    let local_device : string = match (info,physical_device) with
      | _, Some d -> d (* cmdline overrides default for the VG *)
      | Some info, None -> info.local_device (* If we've got a default, use that *)
      | None, None -> failwith "Need to know the local device!" in
    Client.Host.all ()
    >>= fun all ->
    let open Xenvm_interface in
    let h = List.find (fun h -> h.name = host) all in
    Lwt_io.with_file Lwt_io.Output h.toLVM.lv (Lvdump.to_file (vg_name, h.toLVM.lv) local_device)
    >>= fun () ->
    Lwt_io.with_file Lwt_io.Output h.fromLVM.lv (Lvdump.to_file (vg_name, h.fromLVM.lv) local_device) in
  Lwt_main.run t 

let host_destroy copts (vg_name,_) host =
  let t =
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    Client.Host.destroy host in
  Lwt_main.run t 
let host_list copts (vg_name,_) =
  let t =
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    Client.Host.all () >>= fun hosts ->
    let open Xenvm_interface in
    let table_of_queue q = [
      [ "lv"; q.lv ];
      [ "suspended"; string_of_bool q.suspended ]
    ] @ (List.map (fun (k, v) -> [ k; v ]) q.debug_info) in
    let table_of_host h =
      let connection_state = [ "state"; match h.connection_state with Some x -> Jsonrpc.to_string (rpc_of_connection_state x) | None -> "None" ] in
      let fromLVM = add_prefix "fromLVM" (table_of_queue h.fromLVM) in
      let toLVM = add_prefix "toLVM" (table_of_queue h.toLVM) in
      [ connection_state ] @ fromLVM @ toLVM @ [ [ "freeExtents"; Int64.to_string h.freeExtents ] ] in
    List.map (fun h -> add_prefix h.name (table_of_host h)) hosts
    |> List.concat
    |> print_table true [ "key"; "value" ]
    |> Lwt_list.iter_s (fun x -> stdout "%s" x) in
  Lwt_main.run t

let shutdown copts (vg_name,_) =
  let is_alive pid =
    try Unix.kill pid 0; true with _ -> false
  in
  let lwt_while guard body =
    let rec inner () =
      if guard () then body () >>= inner else Lwt.return ()
    in inner ()
  in
  let t =
    get_vg_info_t copts vg_name >>= fun info ->
    set_uri copts info;
    Client.shutdown () >>= fun pid ->
    lwt_while (fun () -> is_alive pid) (fun () -> Lwt_unix.sleep 1.0)
  in Lwt_main.run t

let flush copts (vg_name, lv_name_opt) =
  match lv_name_opt with
  | None -> failwith "Need LV name"
  | Some lv_name ->
    let t =
      get_vg_info_t copts vg_name >>= fun info ->
      set_uri copts info;
      Client.flush ~name:lv_name in
    Lwt_main.run t

let help config =
  Lwt_main.run (
    stdout "help - %s %s" config.config (match config.uri_override with | Some u -> u | None -> "URI unset")
  )

open Cmdliner
let info =
  let doc =
    "XenVM LVM utility" in
  let man = [
    `S "EXAMPLES";
    `P "TODO";
  ] in
  Term.info "xenvm" ~version:"0.1-alpha" ~doc ~man

let hostname =
  let doc = "Unique name of client host" in
  Arg.(required & pos 1 (some string) None & info [] ~docv:"HOSTNAME" ~doc)

let filenames =
  let doc = "Path to the files" in
  Arg.(non_empty & pos_all file [] & info [] ~docv:"FILENAMES" ~doc)

let filename =
  let doc = "Path to the file" in
  Arg.(required & pos 0 (some file) None & info [] ~docv:"FILENAME" ~doc)

let vgname =
  let doc = "Name of the volume group" in
  Arg.(value & opt string "vg" & info ["vg"] ~docv:"VGNAME" ~doc)

let lvname =
  let doc = "Name of the logical volume" in
  Arg.(value & opt string "lv" & info ["lv"] ~docv:"LVNAME" ~doc)

let size =
  let doc = "Size of the LV in megs" in
  Arg.(value & opt int64 4L & info ["size"] ~docv:"SIZE" ~doc)

let dump_cmd =
  let doc = "Dump the metadata in LVM format to stdout" in
  let man = [
    `S "DESCRIPTION";
    `P "Prints the volume group metadata to stdout in LVM format. Note this will not include any updates which are still pending in the redo-log."
  ] in
  Term.(pure dump $ copts_t $ filenames),
  Term.info "dump" ~sdocs:copts_sect ~doc ~man

let format_cmd =
  let doc = "Format the specified file as a VG" in
  let man = [
    `S "DESCRIPTION";
    `P "Places volume group metadata into the file specified"
  ] in
  Term.(pure format $ copts_t $ vgname $ filenames),
  Term.info "format" ~sdocs:copts_sect ~doc ~man

let upgrade_cmd =
  let doc = "Upgrade the specified VG from LVM2 to XenVM" in
  let man = [
    `S "DESCRIPTION";
    `P "Upgrades the volume group metadata so that it will be usable by xenvmd and hidden from lvm."
  ] in
  Term.(pure upgrade $ copts_t $ physical_device_arg_required $ name_arg),
  Term.info "upgrade" ~sdocs:copts_sect ~doc ~man

let downgrade_cmd =
  let doc = "Downgrade the specified VG from XenVM to LVM2" in
  let man = [
    `S "DESCRIPTION";
    `P "Downgrades the volume group metadata so that it will be usable by lvm and hidden from xenvm."
  ] in
  Term.(pure downgrade $ copts_t $ physical_device_arg_required $ name_arg),
  Term.info "downgrade" ~sdocs:copts_sect ~doc ~man

let host_connect_cmd =
  let doc = "Connect to a host" in
  let man = [
    `S "DESCRIPTION";
    `P "Register a host with the daemon. The daemon will start servicing block updates from the shared queues.";
  ] in
  Term.(pure host_connect $ copts_t $ name_arg $ hostname),
  Term.info "host-connect" ~sdocs:copts_sect ~doc ~man

let host_disconnect_cmd =
  let uncooperative_flag =
    let doc = "Perform an uncooperative disconnect (if the host is dead)" in
    Arg.(value & flag & info ["uncooperative"] ~doc) in
  let doc = "Disconnect from a host" in
  let man = [
    `S "DESCRIPTION";
    `P "Dergister a host with the daemon. The daemon will suspend the block queues (unless the --uncooperative flag is used) and stop listening to requests.";
  ] in
  Term.(pure host_disconnect $ copts_t $ name_arg $ uncooperative_flag $ hostname),
  Term.info "host-disconnect" ~sdocs:copts_sect ~doc ~man

let host_dump_cmd =
  let doc = "Dump a host's metadata volumes" in
  let man = [
    `S "DESCRIPTION";
    `P "Copy the contents of a host's metadata volumes to local files in the current directory for forensic analysis.";
  ] in
  Term.(pure host_dump $ copts_t $ name_arg $ physical_device_arg $ hostname),
  Term.info "host-dump" ~sdocs:copts_sect ~doc ~man

let host_create_cmd =
  let doc = "Initialise a host's metadata volumes" in
  let man = [
    `S "DESCRIPTION";
    `P "Creates the metadata volumes needed for a host to connect and make disk updates.";
  ] in
  Term.(pure host_create $ copts_t $ name_arg $ hostname),
  Term.info "host-create" ~sdocs:copts_sect ~doc ~man

let host_destroy_cmd =
  let doc = "Disconnects and destroy a host's metadata volumes" in
  let man = [
    `S "DESCRIPTION";
    `P "Disconnects the metadata volumes cleanly and destroys them.";
  ] in
  Term.(pure host_destroy $ copts_t $ name_arg $ hostname),
  Term.info "host-destroy" ~sdocs:copts_sect ~doc ~man

let host_list_cmd =
  let doc = "Lists all known hosts" in
  let man = [
    `S "DESCRIPTION";
    `P "Lists all the hosts known to Xenvmd and displays the state of the metadata volumes.";
  ] in
  Term.(pure host_list $ copts_t $ name_arg),
  Term.info "host-list" ~sdocs:copts_sect ~doc ~man

let shutdown_cmd =
  let doc = "Shut the daemon down cleanly" in
  let man = [
    `S "DESCRIPTION";
    `P "Flushes the redo log and shuts down, leaving the system in a consistent state.";
  ] in
  Term.(pure shutdown $ copts_t $ name_arg),
  Term.info "shutdown" ~sdocs:copts_sect ~doc ~man

let flush_cmd =
  let doc = "Flush all pending operations for the LV to the on-disk metadata" in
  let man = [
    `S "DESCRIPTION";
    `P "Flushes all pending operations for the LV to the on-disk metadata, this
        is sometimes necessary if you wish to use commands with the --offline
        flag, e.g. xenvm-lvs.";
  ] in
  Term.(pure flush $ copts_t $ name_arg),
  Term.info "flush" ~sdocs:copts_sect ~doc ~man

let default_cmd =
  Term.(pure help $ copts_t), info
      
let cmds = [
  Lvresize.lvresize_cmd;
  Lvresize.lvextend_cmd;
  format_cmd;
  upgrade_cmd;
  downgrade_cmd;
  dump_cmd;
  shutdown_cmd; host_create_cmd; host_destroy_cmd;
  flush_cmd;
  host_list_cmd;
  host_connect_cmd;
  host_disconnect_cmd;
  host_dump_cmd;
  Benchmark.benchmark_cmd;
  Lvcreate.lvcreate_cmd;
  Lvchange.lvchange_cmd;
  Vgchange.vgchange_cmd;
  Lvs.lvs_cmd;
  set_vg_info_cmd;
  Vgcreate.vgcreate_cmd;
  Vgs.vgs_cmd;
  Pvs.pvs_cmd;
  Vgremove.vgremove_cmd;
  Pvcreate.pvcreate_cmd;
  Pvremove.pvremove_cmd;
  Lvremove.lvremove_cmd;
  Lvrename.lvrename_cmd;
  Lvdisplay.lvdisplay_cmd;
  Lvdump.lvdump_cmd;
]

let () =
  Random.self_init ();
  Lwt_main.run (
    Lwt_log.log ~logger:syslog ~level:Lwt_log.Notice (String.concat " " (Array.to_list Sys.argv))
  );
  match Term.eval_choice default_cmd cmds with
  | `Error _ -> exit 1 | _ -> exit 0
