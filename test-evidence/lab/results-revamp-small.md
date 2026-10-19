# tkfs two-machine lab: revamp-small

- Desktop exe: `C:\tkfs-build\lab\target-revamp\release\tkfs.exe` (tkfs 0.1.0, sha256 424e37612df4)
- Laptop exe: `C:\tkfs-lab\target-revamp\release\tkfs.exe` (tkfs 0.1.0, sha256 cd6687e09cfe)
- Build commits: {'desktop': 'e8c2979', 'laptop': 'e8c2979'}; harness 1.4
- Path: desktop daemon -> `ws://127.0.0.1:43281` -> ssh -L -> laptop 127.0.0.1:43280 (Wi-Fi laptop 192.168.100.3)

- Idle before run: desktop CPU {'cpu_mean': 18.9, 'cpu_p50': 22.7, 'cpu_p95': 25.6, 'cpu_max': 25.6, 'samples': 8, 'build_procs_mean': 1, 'build_procs_max': 1}; laptop [{'load_pct': 1, 'top_cpu_time_processes': ['System', 'MsMpEng', 'Taskmgr', 'perfmon', 'svchost', 'svchost', 'ChatGPT', 'svchost']}, {'load_pct': 6, 'top_cpu_time_processes': ['System', 'MsMpEng', 'Taskmgr', 'perfmon', 'svchost', 'svchost', 'ChatGPT', 'svchost']}, {'load_pct': 2, 'top_cpu_time_processes': ['System', 'MsMpEng', 'Taskmgr', 'perfmon', 'svchost', 'svchost', 'ChatGPT', 'svchost']}]
- Tunnel starts: [{'tag': 'tunnel-probe', 'pid': 78324, 'seconds': 0.26}, {'tag': 'tunnel', 'pid': 52320, 'seconds': 0.28}, {'tag': 'tunnel-restored', 'pid': 36044, 'seconds': 0.34}]

## Link ceiling

| Probe | MB/s | seconds |
|---|---|---|
| scp_up (256 MiB) | 8.78 | 30.57 |
| scp_down (256 MiB) | 7.77 | 34.54 |
| tunnel_up (256 MiB) | 8.84 | 30.37 |
| tunnel_down (256 MiB) | 7.32 | 36.68 |

Ping RTT ms: min 2 / avg 2.1 / max 3

## S1: S1 initial receive (laptop -> desktop)

**Status:** complete

| Milestone | t (s) |
|---|---|
| paired | 0.46 |
| remote_list_visible | 0.76 |
| receive_returned | 1.06 |
| worker_running | 3.06 |
| worker_status_ok | 3.06 |
| first_metadata | 3.11 |
| all_paths_and_hashes | 3.11 |
| first_object_bytes | 3.11 |
| content_complete | 76.32 |

- Dataset 518 MiB; receive->complete 75.26 s; effective 7.22 MB/s; transfer phase 7.42 MB/s; peak 10 s window 7.45 MB/s
- Laptop ingest into WinFsp mount: 31.1 s (17.5 MB/s), projection ready +0.5 s
- Source settle before S1: settled True after 14.5 s (threshold 0.25 cores; last {'t_s': 14.5, 'procs': 2, 'cores': 0.001})
- Verification: {'backing_files': {'checked': 201, 'ok': 201, 'absent': 0, 'bad': [], 'method': 'STATE/objects/<sha256> size+sha256', 'layout_known': True}, 'sampled_small': 50, 'small_ok': 50, 'small_bad': 0, 'bad_paths': [], 'large': {'big.bin': {'method': 'backing-object-file', 'ok': True}}}
- Desktop load: CPU mean 15.4% (p95 25.7%, max 43.8%), build processes mean 1 max 1
- Laptop load: CPU mean 11.9% max 16.6%; laptop worker status p50 20 ms max 22 ms
- Control-lane metadata: last {'t': 1792165207682, 'control_exchanges': 141, 'control_received_wire_bytes': 703978, 'local_events': 502, 'remote_events': 502, 'local_only_events': 0, 'remote_only_events': 0, 'local_pending': 0, 'remote_pending': 0}; local events/s 0.0, exchanges/s 1.8, events/exchange 0.0, local vs remote events 100.0%
- Desktop snapshot calls: {'snapshots': 115, 'peers': {'count': 39, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 29, 'latency_ms_p95': 41, 'latency_ms_max': 47}, 'inspect': {'count': 38, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 29, 'latency_ms_p95': 42, 'latency_ms_max': 72}, 'status': {'count': 38, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 28, 'latency_ms_p95': 40, 'latency_ms_max': 63}}

## S2: S2 concurrent two-way incremental

**Status:** complete

| Milestone | t (s) |
|---|---|
| laptop_writes_done | 13.71 |
| desktop_writes_done | 14.03 |
| desktop_has_laptop_mods | 14.63 |
| laptop_has_desktop_new | 52.46 |
| metadata_converged | 52.46 |
| fully_converged | 52.46 |

- Writes: {'desktop_new_files': 200, 'desktop_errors': [], 'laptop_modified_files': 200, 'laptop_write_ms': 12385}
- Latency after the writer finished: {'laptop_mods_on_desktop_after_laptop_writes': 0.92, 'desktop_new_on_laptop_after_desktop_writes': 38.43}
- State reads: {'desktop_state_failed': 0, 'laptop_state_failed': 0, 'polls': 15}; max seen {'desktop_mods_seen': 200, 'laptop_new_seen': 200}
- Verification: {'backing_files': {'checked': 25, 'ok': 25, 'absent': 0, 'bad': [], 'method': 'STATE/objects/<sha256> size+sha256', 'layout_known': True}, 'sampled_small': 25, 'small_ok': 25, 'small_bad': 0, 'bad_paths': [], 'large': {}}
- Conflicts: {'desktop': 0, 'laptop': 0}
- Desktop load: CPU mean 10.1% (p95 18.4%, max 28.7%), build processes mean 1 max 1
- Laptop load: CPU mean 17.2% max 25.7%; laptop worker status p50 20 ms max 79 ms
- Control-lane metadata: last {'t': 1792165261871, 'control_exchanges': 305, 'control_received_wire_bytes': 1104544, 'local_events': 1113, 'remote_events': 1113, 'local_only_events': 0, 'remote_only_events': 0, 'local_pending': 0, 'remote_pending': 0}; local events/s 11.71, exchanges/s 3.07, events/exchange 3.82, local vs remote events 100.0%
- Desktop snapshot calls: {'snapshots': 81, 'peers': {'count': 27, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 27, 'latency_ms_p95': 34, 'latency_ms_max': 35}, 'inspect': {'count': 27, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 27, 'latency_ms_p95': 35, 'latency_ms_max': 36}, 'status': {'count': 27, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 26, 'latency_ms_p95': 38, 'latency_ms_max': 38}}

## S3: S3 tunnel outage during a 512 MiB transfer

**Status:** complete

| Milestone | t (s) |
|---|---|
| laptop_copy_done | 22.7 |
| transfer_started | 24.52 |
| tunnel_killed | 29.94 |
| tunnel_restore_started | 74.98 |
| tunnel_restored | 75.32 |
| lanes_reconnected | 112.76 |
| transfer_resumed | 161.1 |
| transfer_complete | 227.33 |

- Resume: {'partial_at_kill': 39845888, 'partial_before_restore': 45350912, 'min_partial_after_restore': 0, 'resumed_from_offset': False, 'restore_to_resume_s': 85.78, 'restore_to_lanes_connected_s': 37.44, 'restore_to_complete_s': 152.01}
- During outage: {'lane_connected_fraction': 0.0, 'lane_errors': ['CONNECTION_LOST: CONNECTION_LOST: IO error: Foi forçado o cancelamento de uma conexão existente pelo host remoto. (os error 10054)', 'CONNECTION_LOST: IO error: Foi forçado o cancelamento de uma conexão existente pelo host remoto. (os error 10054)', 'WS_CONNECT_FAILED: 127.0.0.1:43281: Nenhuma conexão pôde ser feita porque a máquina de destino as recusou ativamente. (os error 10061)'], 'inspect_sync_errors': ['CONNECTION_LOST: IO error: Foi forçado o cancelamento de uma conexão existente pelo host remoto. (os error 10054)', 'WS_CONNECT_FAILED: 127.0.0.1:43281: Nenhuma conexão pôde ser feita porque a máquina de destino as recusou ativamente. (os error 10061)'], 'worker_health': [], 'metadata_caught_up_values': ['False']}
- Verification: {'backing_files': {'checked': 1, 'ok': 1, 'absent': 0, 'bad': [], 'method': 'STATE/objects/<sha256> size+sha256', 'layout_known': True}, 'sampled_small': 0, 'small_ok': 0, 'small_bad': 0, 'bad_paths': [], 'large': {'big2.bin': {'method': 'backing-object-file', 'ok': True}}}
- Desktop load: CPU mean 35.3% (p95 59.7%, max 69.5%), build processes mean 6.0 max 13
- Laptop load: CPU mean 9.3% max 20.7%; laptop worker status p50 20 ms max 25 ms
- Control-lane metadata: last {'t': 1792165508570, 'control_exchanges': 638, 'control_received_wire_bytes': 1387026, 'local_events': 1115, 'remote_events': 1115, 'local_only_events': 0, 'remote_only_events': 0, 'local_pending': 0, 'remote_pending': 0}; local events/s 0.01, exchanges/s 1.3, events/exchange 0.01, local vs remote events 100.0%
- Desktop snapshot calls: {'snapshots': 342, 'peers': {'count': 114, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 34, 'latency_ms_p95': 51, 'latency_ms_max': 635}, 'inspect': {'count': 114, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 35, 'latency_ms_p95': 50, 'latency_ms_max': 65}, 'status': {'count': 114, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 32, 'latency_ms_p95': 45, 'latency_ms_max': 50}}

## Caveats

- Desktop is shared with concurrent heavy builds/tests; see desktop_load (CPU %, build process counts) per scenario.
- Peer traffic crosses an SSH -L tunnel (encryption + single TCP stream over Wi-Fi); compare against link.tunnel_* not raw Wi-Fi.
- Milestones are observed by polling (2 s snapshots, 0.5-1 s in S3); resolution is about +/-2 s, except S2 whose loop reads both projections each pass (about 4.5 s per pass on an idle pair, far more when a `state` read times out; S2 window / reads.polls gives the period).
- S3 fault = ssh client terminated: desktop-side sockets close immediately (RST/EOF), not a silent black-hole.
- Laptop timestamps are corrected by clock_offset_ms (uncertainty listed there).
- Status polling itself takes worker locks on both sides; identical polling is used for every build (manage status only with --capture-manage-status). The `state` projection dump polled every ~2 s (desktop in S1, both machines in S2) holds the worker's engine lock: see progress state_ms.
- Control-lane metadata_summary counters (local_events, exchanges, wire bytes) are diagnostics whose meaning changes with the protocol (v3 reports feed tips); compare builds on projection/content milestones.
- cat-object can fetch a missing object on demand; verification.backing_files hashes installed objects directly and is the replication check.
