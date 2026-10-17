# tkfs two-machine lab: baseline-small

- Desktop exe: `C:\tkfs-build\lab\target-baseline\release\tkfs.exe` (tkfs 0.1.0, sha256 69c6e8f8414f)
- Laptop exe: `C:\tkfs-lab\target-baseline\release\tkfs.exe` (tkfs 0.1.0, sha256 274b7de7e12e)
- Build commits: {'desktop': '6298d0b', 'laptop': '6298d0b'}; harness 1.4
- Path: desktop daemon -> `ws://127.0.0.1:43281` -> ssh -L -> laptop 127.0.0.1:43280 (Wi-Fi laptop 192.168.100.3)

- Idle before run: desktop CPU {'cpu_mean': 19.9, 'cpu_p50': 20.9, 'cpu_p95': 25.3, 'cpu_max': 25.3, 'samples': 8, 'build_procs_mean': 1, 'build_procs_max': 1}; laptop [{'load_pct': 4, 'top_cpu_time_processes': ['System', 'MsMpEng', 'Taskmgr', 'perfmon', 'svchost', 'svchost', 'svchost', 'RtkAudUService64']}, {'load_pct': 6, 'top_cpu_time_processes': ['System', 'MsMpEng', 'Taskmgr', 'perfmon', 'svchost', 'svchost', 'svchost', 'RtkAudUService64']}, {'load_pct': 5, 'top_cpu_time_processes': ['System', 'MsMpEng', 'Taskmgr', 'perfmon', 'svchost', 'svchost', 'svchost', 'RtkAudUService64']}]
- Tunnel starts: [{'tag': 'tunnel-probe', 'pid': 40272, 'seconds': 0.28}, {'tag': 'tunnel', 'pid': 32312, 'seconds': 0.27}, {'tag': 'tunnel-restored', 'pid': 97608, 'seconds': 0.28}]

## Link ceiling

| Probe | MB/s | seconds |
|---|---|---|
| scp_up (256 MiB) | 8.73 | 30.75 |
| scp_down (256 MiB) | 7.81 | 34.36 |
| tunnel_up (256 MiB) | 8.78 | 30.56 |
| tunnel_down (256 MiB) | 7.22 | 37.16 |

Ping RTT ms: min 2 / avg 2.2 / max 4

## S1: S1 initial receive (laptop -> desktop)

**Status:** complete

| Milestone | t (s) |
|---|---|
| paired | 0.41 |
| remote_list_visible | 0.53 |
| receive_returned | 0.8 |
| worker_status_ok | 2.8 |
| worker_running | 4.84 |
| first_metadata | 20.55 |
| first_object_bytes | 22.63 |
| all_paths_and_hashes | 162.42 |
| worker_metadata_caught_up | 164.5 |
| content_complete | 189.27 |

- Dataset 518 MiB; receive->complete 188.47 s; effective 2.88 MB/s; transfer phase 3.26 MB/s; peak 10 s window 3.95 MB/s
- Laptop ingest into WinFsp mount: 37.4 s (14.5 MB/s), projection ready +0.5 s
- Source settle before S1: settled True after 14.3 s (threshold 0.25 cores; last {'t_s': 14.3, 'procs': 2, 'cores': 0.043})
- Verification: {'backing_files': {'checked': 201, 'ok': 201, 'absent': 0, 'bad': [], 'method': 'STATE/objects/<sha256> size+sha256', 'layout_known': True}, 'sampled_small': 50, 'small_ok': 50, 'small_bad': 0, 'bad_paths': [], 'large': {'big.bin': {'method': 'backing-object-file', 'ok': True}}}
- Desktop load: CPU mean 24.7% (p95 52.2%, max 82.0%), build processes mean 1 max 1
- Laptop load: CPU mean 12.2% max 20.7%; laptop worker status p50 68 ms max 83 ms
- Control-lane metadata: last {'t': 1792163514432, 'control_exchanges': 289, 'control_received_wire_bytes': 12136599, 'local_events': 502, 'remote_events': 502, 'local_only_events': 0, 'remote_only_events': 0, 'local_pending': 0, 'remote_pending': 0}; local events/s 2.64, exchanges/s 1.55, events/exchange 1.71, local vs remote events 100.0%
- Desktop snapshot calls: {'snapshots': 286, 'peers': {'count': 96, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 37, 'latency_ms_p95': 47, 'latency_ms_max': 57}, 'inspect': {'count': 95, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 32, 'latency_ms_p95': 45, 'latency_ms_max': 73}, 'status': {'count': 95, 'failures': 0, 'busy_or_stale': 63, 'latency_ms_p50': 36, 'latency_ms_p95': 83, 'latency_ms_max': 111}}

## S2: S2 concurrent two-way incremental

**Status:** complete

| Milestone | t (s) |
|---|---|
| laptop_writes_done | 10.43 |
| desktop_writes_done | 15.64 |
| desktop_has_laptop_mods | 112.28 |
| laptop_has_desktop_new | 1031.64 |
| metadata_converged | 1031.64 |
| fully_converged | 1031.64 |

- Writes: {'desktop_new_files': 200, 'desktop_errors': [], 'laptop_modified_files': 200, 'laptop_write_ms': 9103}
- Latency after the writer finished: {'laptop_mods_on_desktop_after_laptop_writes': 101.85, 'desktop_new_on_laptop_after_desktop_writes': 1016.0}
- State reads: {'desktop_state_failed': 0, 'laptop_state_failed': 0, 'polls': 205}; max seen {'desktop_mods_seen': 200, 'laptop_new_seen': 200}
- Verification: {'backing_files': {'checked': 25, 'ok': 25, 'absent': 0, 'bad': [], 'method': 'STATE/objects/<sha256> size+sha256', 'layout_known': True}, 'sampled_small': 25, 'small_ok': 25, 'small_bad': 0, 'bad_paths': [], 'large': {}}
- Conflicts: {'desktop': 0, 'laptop': 0}
- Desktop load: CPU mean 22.4% (p95 44.1%, max 86.8%), build processes mean 1.6 max 14
- Laptop load: CPU mean 18.1% max 39.7%; laptop worker status p50 27 ms max 295 ms
- Control-lane metadata: last {'t': 1792164547327, 'control_exchanges': 543, 'control_received_wire_bytes': 18408563, 'local_events': 1113, 'remote_events': 1113, 'local_only_events': 0, 'remote_only_events': 0, 'local_pending': 0, 'remote_pending': 0}; local events/s 0.59, exchanges/s 0.24, events/exchange 2.43, local vs remote events 100.0%
- Desktop snapshot calls: {'snapshots': 1542, 'peers': {'count': 514, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 36, 'latency_ms_p95': 50, 'latency_ms_max': 874}, 'inspect': {'count': 514, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 32, 'latency_ms_p95': 50, 'latency_ms_max': 136}, 'status': {'count': 514, 'failures': 0, 'busy_or_stale': 44, 'latency_ms_p50': 104, 'latency_ms_p95': 142, 'latency_ms_max': 271}}

## S3: S3 tunnel outage during a 512 MiB transfer

**Status:** complete

| Milestone | t (s) |
|---|---|
| laptop_copy_done | 22.67 |
| transfer_started | 31.72 |
| tunnel_killed | 37.35 |
| tunnel_restore_started | 82.47 |
| tunnel_restored | 82.75 |
| lanes_reconnected | 110.75 |
| transfer_resumed | 110.75 |
| transfer_complete | 298.04 |

- Resume: {'partial_at_kill': 0, 'partial_before_restore': 18868221, 'min_partial_after_restore': 18868221, 'resumed_from_offset': False, 'restore_to_resume_s': 28.0, 'restore_to_lanes_connected_s': 28.0, 'restore_to_complete_s': 215.29}
- During outage: {'lane_connected_fraction': 0.01, 'lane_errors': ['IO error: Foi forçado o cancelamento de uma conexão existente pelo host remoto. (os error 10054): Foi forçado o cancelamento de uma conexão existente pelo host remoto. (os error 10054)', 'REMOTE_BULK_EXCHANGE_FAILED: IO error: Foi forçado o cancelamento de uma conexão existente pelo host remoto. (os error 10054): Foi forçado o cancelamento de uma conexão existente pelo host remoto. (os', 'WS_CONNECT_FAILED: Nenhuma conexão pôde ser feita porque a máquina de destino as recusou ativamente. (os error 10061)'], 'inspect_sync_errors': ['IO error: Foi forçado o cancelamento de uma conexão existente pelo host remoto. (os error 10054): Foi forçado o cancelamento de uma conexão existente pelo host remoto. (os error 10054)', 'WS_CONNECT_FAILED: Nenhuma conexão pôde ser feita porque a máquina de destino as recusou ativamente. (os error 10061)'], 'worker_health': [], 'metadata_caught_up_values': ['False', 'None', 'True']}
- Verification: {'backing_files': {'checked': 1, 'ok': 1, 'absent': 0, 'bad': [], 'method': 'STATE/objects/<sha256> size+sha256', 'layout_known': True}, 'sampled_small': 0, 'small_ok': 0, 'small_bad': 0, 'bad_paths': [], 'large': {'big2.bin': {'method': 'backing-object-file', 'ok': True}}}
- Desktop load: CPU mean 19.7% (p95 37.9%, max 97.1%), build processes mean 1.6 max 3
- Laptop load: CPU mean 11.9% max 32.2%; laptop worker status p50 126 ms max 161 ms
- Control-lane metadata: last {'t': 1792164862664, 'control_exchanges': 743, 'control_received_wire_bytes': 23300658, 'local_events': 1115, 'remote_events': 1115, 'local_only_events': 0, 'remote_only_events': 0, 'local_pending': 0, 'remote_pending': 0}; local events/s 0.01, exchanges/s 0.61, events/exchange 0.01, local vs remote events 100.0%
- Desktop snapshot calls: {'snapshots': 447, 'peers': {'count': 149, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 36, 'latency_ms_p95': 46, 'latency_ms_max': 226}, 'inspect': {'count': 149, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 31, 'latency_ms_p95': 43, 'latency_ms_max': 55}, 'status': {'count': 149, 'failures': 0, 'busy_or_stale': 26, 'latency_ms_p50': 109, 'latency_ms_p95': 140, 'latency_ms_max': 578}}

## Caveats

- Desktop is shared with concurrent heavy builds/tests; see desktop_load (CPU %, build process counts) per scenario.
- Peer traffic crosses an SSH -L tunnel (encryption + single TCP stream over Wi-Fi); compare against link.tunnel_* not raw Wi-Fi.
- Milestones are observed by polling (2 s snapshots, 0.5-1 s in S3); resolution is about +/-2 s, except S2 whose loop reads both projections each pass (about 4.5 s per pass on an idle pair, far more when a `state` read times out; S2 window / reads.polls gives the period).
- S3 fault = ssh client terminated: desktop-side sockets close immediately (RST/EOF), not a silent black-hole.
- Laptop timestamps are corrected by clock_offset_ms (uncertainty listed there).
- Status polling itself takes worker locks on both sides; identical polling is used for every build (manage status only with --capture-manage-status). The `state` projection dump polled every ~2 s (desktop in S1, both machines in S2) holds the worker's engine lock: see progress state_ms.
- Control-lane metadata_summary counters (local_events, exchanges, wire bytes) are diagnostics whose meaning changes with the protocol (v3 reports feed tips); compare builds on projection/content milestones.
- cat-object can fetch a missing object on demand; verification.backing_files hashes installed objects directly and is the replication check.
