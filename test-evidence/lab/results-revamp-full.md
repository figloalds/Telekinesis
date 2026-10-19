# tkfs two-machine lab: revamp-full

- Desktop exe: `C:\tkfs-build\lab\target-revamp\release\tkfs.exe` (tkfs 0.1.0, sha256 424e37612df4)
- Laptop exe: `C:\tkfs-lab\target-revamp\release\tkfs.exe` (tkfs 0.1.0, sha256 cd6687e09cfe)
- Build commits: {'desktop': 'e8c2979', 'laptop': 'e8c2979'}; harness 1.4
- Path: desktop daemon -> `ws://127.0.0.1:43281` -> ssh -L -> laptop 127.0.0.1:43280 (Wi-Fi laptop 192.168.100.3)

- Idle before run: desktop CPU {'cpu_mean': 23.8, 'cpu_p50': 25.0, 'cpu_p95': 38.9, 'cpu_max': 38.9, 'samples': 8, 'build_procs_mean': 1, 'build_procs_max': 1}; laptop [{'load_pct': 4, 'top_cpu_time_processes': ['System', 'MsMpEng', 'Taskmgr', 'perfmon', 'svchost', 'svchost', 'ChatGPT', 'svchost']}, {'load_pct': 12, 'top_cpu_time_processes': ['System', 'MsMpEng', 'Taskmgr', 'perfmon', 'svchost', 'svchost', 'ChatGPT', 'svchost']}, {'load_pct': 4, 'top_cpu_time_processes': ['System', 'MsMpEng', 'Taskmgr', 'perfmon', 'svchost', 'svchost', 'ChatGPT', 'svchost']}]
- Tunnel starts: [{'tag': 'tunnel-probe', 'pid': 90868, 'seconds': 0.26}, {'tag': 'tunnel', 'pid': 62388, 'seconds': 0.32}, {'tag': 'tunnel-restored', 'pid': 37084, 'seconds': 0.28}]

## Link ceiling

| Probe | MB/s | seconds |
|---|---|---|
| scp_up (256 MiB) | 7.05 | 38.08 |
| scp_down (256 MiB) | 7.68 | 34.96 |
| tunnel_up (256 MiB) | 8.79 | 30.55 |
| tunnel_down (256 MiB) | 6.97 | 38.49 |

Ping RTT ms: min 2 / avg 2.5 / max 3

## S1: S1 initial receive (laptop -> desktop)

**Status:** complete

| Milestone | t (s) |
|---|---|
| paired | 0.43 |
| remote_list_visible | 0.58 |
| receive_returned | 0.83 |
| first_metadata | 3.01 |
| worker_running | 5.01 |
| worker_status_ok | 5.01 |
| all_paths_and_hashes | 7.66 |
| first_object_bytes | 7.66 |
| content_complete | 85.06 |

- Dataset 575 MiB; receive->complete 84.23 s; effective 7.16 MB/s; transfer phase 7.79 MB/s; peak 10 s window 8.95 MB/s
- Laptop ingest into WinFsp mount: 163.6 s (3.7 MB/s), projection ready +1.5 s
- Source settle before S1: settled True after 15.1 s (threshold 0.25 cores; last {'t_s': 15.1, 'procs': 2, 'cores': 0.001})
- Verification: {'backing_files': {'checked': 2001, 'ok': 2001, 'absent': 0, 'bad': [], 'method': 'STATE/objects/<sha256> size+sha256', 'layout_known': True}, 'sampled_small': 50, 'small_ok': 50, 'small_bad': 0, 'bad_paths': [], 'large': {'big.bin': {'method': 'backing-object-file', 'ok': True}}}
- Desktop load: CPU mean 11.7% (p95 22.9%, max 30.6%), build processes mean 1 max 1
- Laptop load: CPU mean 16.2% max 30.4%; laptop worker status p50 21 ms max 27 ms
- Control-lane metadata: last {'t': 1792170753282, 'control_exchanges': 164, 'control_received_wire_bytes': 5081088, 'local_events': 4102, 'remote_events': 4102, 'local_only_events': 0, 'remote_only_events': 0, 'local_pending': 0, 'remote_pending': 0}; local events/s 35.59, exchanges/s 1.83, events/exchange 19.41, local vs remote events 100.0%
- Desktop snapshot calls: {'snapshots': 136, 'peers': {'count': 46, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 28, 'latency_ms_p95': 37, 'latency_ms_max': 43}, 'inspect': {'count': 45, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 29, 'latency_ms_p95': 37, 'latency_ms_max': 59}, 'status': {'count': 45, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 29, 'latency_ms_p95': 88, 'latency_ms_max': 112}}

## S2: S2 concurrent two-way incremental

**Status:** complete

| Milestone | t (s) |
|---|---|
| laptop_writes_done | 28.39 |
| desktop_writes_done | 46.68 |
| desktop_has_laptop_mods | 48.46 |
| laptop_has_desktop_new | 120.81 |
| metadata_converged | 120.81 |
| fully_converged | 120.81 |

- Writes: {'desktop_new_files': 200, 'desktop_errors': [], 'laptop_modified_files': 200, 'laptop_write_ms': 26985}
- Latency after the writer finished: {'laptop_mods_on_desktop_after_laptop_writes': 20.07, 'desktop_new_on_laptop_after_desktop_writes': 74.13}
- State reads: {'desktop_state_failed': 0, 'laptop_state_failed': 0, 'polls': 20}; max seen {'desktop_mods_seen': 200, 'laptop_new_seen': 200}
- Verification: {'backing_files': {'checked': 25, 'ok': 25, 'absent': 0, 'bad': [], 'method': 'STATE/objects/<sha256> size+sha256', 'layout_known': True}, 'sampled_small': 25, 'small_ok': 25, 'small_bad': 0, 'bad_paths': [], 'large': {}}
- Conflicts: {'desktop': 0, 'laptop': 0}
- Desktop load: CPU mean 17.4% (p95 23.3%, max 29.7%), build processes mean 1 max 1
- Laptop load: CPU mean 25.1% max 32.2%; laptop worker status p50 28 ms max 554 ms
- Control-lane metadata: last {'t': 1792170873727, 'control_exchanges': 477, 'control_received_wire_bytes': 5608223, 'local_events': 4713, 'remote_events': 4713, 'local_only_events': 0, 'remote_only_events': 0, 'local_pending': 0, 'remote_pending': 0}; local events/s 5.09, exchanges/s 2.58, events/exchange 1.98, local vs remote events 100.0%
- Desktop snapshot calls: {'snapshots': 180, 'peers': {'count': 60, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 30, 'latency_ms_p95': 34, 'latency_ms_max': 40}, 'inspect': {'count': 60, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 30, 'latency_ms_p95': 38, 'latency_ms_max': 51}, 'status': {'count': 60, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 29, 'latency_ms_p95': 148, 'latency_ms_max': 167}}

## S3: S3 tunnel outage during a 512 MiB transfer

**Status:** complete

| Milestone | t (s) |
|---|---|
| laptop_copy_done | 20.85 |
| transfer_started | 23.07 |
| tunnel_killed | 28.44 |
| tunnel_restore_started | 73.47 |
| tunnel_restored | 73.75 |
| lanes_reconnected | 109.76 |
| transfer_resumed | 162.21 |
| transfer_complete | 230.13 |

- Resume: {'partial_at_kill': 47448064, 'partial_before_restore': 57933824, 'min_partial_after_restore': 57933824, 'resumed_from_offset': True, 'restore_to_resume_s': 88.46, 'restore_to_lanes_connected_s': 36.01, 'restore_to_complete_s': 156.38}
- During outage: {'lane_connected_fraction': 0.0, 'lane_errors': ['CONNECTION_LOST: CONNECTION_LOST: IO error: Foi forçado o cancelamento de uma conexão existente pelo host remoto. (os error 10054)', 'CONNECTION_LOST: IO error: Foi forçado o cancelamento de uma conexão existente pelo host remoto. (os error 10054)', 'WS_CONNECT_FAILED: 127.0.0.1:43281: Nenhuma conexão pôde ser feita porque a máquina de destino as recusou ativamente. (os error 10061)'], 'inspect_sync_errors': ['CONNECTION_LOST: IO error: Foi forçado o cancelamento de uma conexão existente pelo host remoto. (os error 10054)', 'WS_CONNECT_FAILED: 127.0.0.1:43281: Nenhuma conexão pôde ser feita porque a máquina de destino as recusou ativamente. (os error 10061)'], 'worker_health': [], 'metadata_caught_up_values': ['False']}
- Verification: {'backing_files': {'checked': 1, 'ok': 1, 'absent': 0, 'bad': [], 'method': 'STATE/objects/<sha256> size+sha256', 'layout_known': True}, 'sampled_small': 0, 'small_ok': 0, 'small_bad': 0, 'bad_paths': [], 'large': {'big2.bin': {'method': 'backing-object-file', 'ok': True}}}
- Desktop load: CPU mean 13.6% (p95 38.6%, max 61.4%), build processes mean 1.7 max 12
- Laptop load: CPU mean 14.6% max 28.7%; laptop worker status p50 21 ms max 31 ms
- Control-lane metadata: last {'t': 1792171118755, 'control_exchanges': 790, 'control_received_wire_bytes': 5873005, 'local_events': 4715, 'remote_events': 4715, 'local_only_events': 0, 'remote_only_events': 0, 'local_pending': 0, 'remote_pending': 0}; local events/s 0.01, exchanges/s 1.23, events/exchange 0.01, local vs remote events 100.0%
- Desktop snapshot calls: {'snapshots': 345, 'peers': {'count': 115, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 28, 'latency_ms_p95': 37, 'latency_ms_max': 55}, 'inspect': {'count': 115, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 27, 'latency_ms_p95': 35, 'latency_ms_max': 73}, 'status': {'count': 115, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 27, 'latency_ms_p95': 36, 'latency_ms_max': 72}}

## Caveats

- Desktop is shared with concurrent heavy builds/tests; see desktop_load (CPU %, build process counts) per scenario.
- Peer traffic crosses an SSH -L tunnel (encryption + single TCP stream over Wi-Fi); compare against link.tunnel_* not raw Wi-Fi.
- Milestones are observed by polling (2 s snapshots, 0.5-1 s in S3); resolution is about +/-2 s, except S2 whose loop reads both projections each pass (about 4.5 s per pass on an idle pair, far more when a `state` read times out; S2 window / reads.polls gives the period).
- S3 fault = ssh client terminated: desktop-side sockets close immediately (RST/EOF), not a silent black-hole.
- Laptop timestamps are corrected by clock_offset_ms (uncertainty listed there).
- Status polling itself takes worker locks on both sides; identical polling is used for every build (manage status only with --capture-manage-status). The `state` projection dump polled every ~2 s (desktop in S1, both machines in S2) holds the worker's engine lock: see progress state_ms.
- Control-lane metadata_summary counters (local_events, exchanges, wire bytes) are diagnostics whose meaning changes with the protocol (v3 reports feed tips); compare builds on projection/content milestones.
- cat-object can fetch a missing object on demand; verification.backing_files hashes installed objects directly and is the replication check.
