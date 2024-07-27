module H = Hdr_histogram
module Ts = Runtime_events.Timestamp

type ts = { mutable start_time : int64; 
            mutable end_time : int64 }
let wall_time = { start_time = Int64.zero; 
                  end_time = Int64.zero }

let domain_start_ts = Array.make 128 Int64.zero
let domain_wall_time = Array.make 128 0
let domain_gc_time = Array.make 128 0

let lifecycle domain_id ts lifecycle_event _data =
  let ts = Runtime_events.Timestamp.to_int64 ts in
  let do_at_domain_start () =
      assert (domain_start_ts.(domain_id) = Int64.zero);
      domain_start_ts.(domain_id) <- ts
  in
  let do_at_domain_stop () =
      let open Int64 in
      assert (domain_start_ts.(domain_id) != zero);
      domain_wall_time.(domain_id) <- 
        domain_wall_time.(domain_id) + to_int (sub ts domain_start_ts.(domain_id));
      domain_start_ts.(domain_id) <- zero
  in
  match lifecycle_event with
  | Runtime_events.EV_RING_START ->
      wall_time.start_time <- ts;
      do_at_domain_start ()
  | Runtime_events.EV_RING_STOP ->
      wall_time.end_time <- ts;
      do_at_domain_stop ()
  | Runtime_events.EV_DOMAIN_SPAWN ->
      do_at_domain_start ()
  | Runtime_events.EV_DOMAIN_TERMINATE ->
      do_at_domain_stop ()
  | _ -> ()

let print_percentiles json output hist =
  let ms_of_ns ns = ns /. 1_000_000. in
  let s_of_ns ns = float_of_int ns /. 1_000_000_000. in
  let mean_latency = H.mean hist |> ms_of_ns
  and max_latency = float_of_int (H.max hist) |> ms_of_ns in
  let percentiles =
    [| 25.0; 50.0; 60.0; 70.0; 75.0; 80.0; 85.0; 90.0; 95.0; 96.0; 97.0; 
       98.0; 99.0; 99.9; 99.99; 99.999; 99.9999; 100.0; |]
  in
  let oc = match output with Some s -> open_out s | None -> stderr in
  let total_gc_time = Array.fold_left ( + ) 0 domain_gc_time in

  if json then
    let distribs =
      List.init (Array.length percentiles) (fun i ->
          let percentile = percentiles.(i) in
          let value =
            H.value_at_percentile hist percentiles.(i)
            |> float_of_int |> ms_of_ns |> string_of_float
          in
          Printf.sprintf "\"%.4f\": %s" percentile value)
      |> String.concat ","
    in
    Printf.fprintf oc
      {|{"mean_latency": %f, "max_latency": %f, "distr_latency": {%s}}|}
      mean_latency max_latency distribs
  else (
    Printf.fprintf oc "\n";
    Printf.fprintf oc "Execution times:\n";

    let real_time = Int64.(sub wall_time.end_time wall_time.start_time |> to_int) in
    Printf.fprintf oc "Wall time (s):\t%.2f\n" (s_of_ns real_time);
    let total_wall_time = ref 0 in
    let ap = Array.combine domain_wall_time domain_gc_time in
    Array.iteri (fun i (wall_time, gc_time) ->
      if gc_time > 0 && wall_time = 0 then begin
        Printf.fprintf stderr
          "[Olly] Warning: Domain %d has GC time but no CPU time\n" i
      end else
        total_wall_time := !total_wall_time + wall_time) ap;


    Printf.fprintf oc "CPU time (s):\t%.2f\n" (s_of_ns !total_wall_time);
    Printf.fprintf oc "GC time (s):\t%.2f\n" (s_of_ns total_gc_time);
    Printf.fprintf oc "GC overhead (%% of CPU time):\t%.2f%%\n"
      (float_of_int total_gc_time /. (float_of_int !total_wall_time) *. 100.);
    Printf.fprintf oc "\n";
    Printf.fprintf oc "Per domain stats:\n";
    Printf.fprintf oc "Domain\t Wall(s)\t GC(s)\t GC(%%)\n";
    Array.iteri (fun i (c,g) ->
      if c > 0 then
        Printf.fprintf oc "%d\t %.2f\t %.2f\t %.2f\n" i (s_of_ns c) (s_of_ns g)
          ((float_of_int g) *. 100. /. (float_of_int c)))
      (Array.combine (domain_wall_time) (domain_gc_time));
    Printf.fprintf oc "\n";
    Printf.fprintf oc "GC latency profile:\n";
    Printf.fprintf oc "#[Mean (ms):\t%.2f,\t Stddev (ms):\t%.2f]\n" mean_latency
      (H.stddev hist |> ms_of_ns);
    Printf.fprintf oc "#[Min (ms):\t%.2f,\t max (ms):\t%.2f]\n"
      (float_of_int (H.min hist) |> ms_of_ns)
      max_latency;
    Printf.fprintf oc "\n";
    Printf.fprintf oc "Percentile \t Latency (ms)\n";
    Fun.flip Array.iter percentiles (fun p ->
        Printf.fprintf oc "%.4f \t %.2f\n" p
          (float_of_int (H.value_at_percentile hist p) |> ms_of_ns)))

let gc_stats json output exec_args =
  let current_event = Hashtbl.create 13 in
  let hist =
    H.init ~lowest_discernible_value:10 ~highest_trackable_value:10_000_000_000
      ~significant_figures:3
  in
  let is_gc_phase phase =
    match phase with
    | Runtime_events.EV_MAJOR | Runtime_events.EV_STW_LEADER
    | Runtime_events.EV_INTERRUPT_REMOTE ->
        true
    | _ -> false
  in
  let runtime_begin ring_id ts phase =
    if is_gc_phase phase then
      match Hashtbl.find_opt current_event ring_id with
      | None -> Hashtbl.add current_event ring_id (phase, Ts.to_int64 ts)
      | _ -> ()
  in
  let runtime_end ring_id ts phase =
    match Hashtbl.find_opt current_event ring_id with
    | Some (saved_phase, saved_ts) when saved_phase = phase ->
        Hashtbl.remove current_event ring_id;
        let open Int64 in
        let latency = to_int (sub (Ts.to_int64 ts) saved_ts) in
        assert (H.record_value hist latency);
        domain_gc_time.(ring_id) <- domain_gc_time.(ring_id) + latency
    | _ -> ()
  in
  let init = Fun.id in
  let cleanup () = print_percentiles json output hist in
  let open Olly_common.Launch in
  olly
    { empty_config with runtime_begin; runtime_end; lifecycle; init; cleanup }
    exec_args

let gc_stats_cmd =
  let open Cmdliner in
  let open Olly_common.Cli in
  let json_option =
    let doc = "Print the output in json instead of human-readable format." in
    Arg.(value & flag & info [ "json" ] ~docv:"json" ~doc)
  in

  let output_option =
    let doc =
      "Redirect the output of `olly` to specified file. The output of the \
       command is not redirected."
    in
    Arg.(
      value
      & opt (some string) None
      & info [ "o"; "output" ] ~docv:"output" ~doc)
  in

  let man =
    [
      `S Manpage.s_description;
      `P "Report the GC latency profile.";
      `I ("Wall time", "Real execution time of the program");
      `I ("CPU time", "Total CPU time across all domains");
      `I
        ( "GC time",
          "Total time spent by the program performing garbage collection \
           (major and minor)" );
      `I
        ( "GC overhead",
          "Percentage of time taken up by GC against the total execution time"
        );
      `I
        ( "GC time per domain",
          "Time spent by every domain performing garbage collection (major and \
           minor cycles). Domains are reported with their domain ID   (e.g. \
           `Domain0`)" );
      `I
        ( "GC latency profile",
          "Mean, standard deviation and percentile latency profile of GC \
           events." );
      `Blocks help_secs;
    ]
  in
  let doc = "Report the GC latency profile and stats." in
  let info = Cmd.info "gc-stats" ~doc ~sdocs ~man in
  Cmd.v info Term.(const gc_stats $ json_option $ output_option $ exec_args 0)
