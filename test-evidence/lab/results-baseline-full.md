# tkfs two-machine lab: baseline-full

- Desktop exe: `C:\tkfs-build\lab\target-baseline\release\tkfs.exe` (tkfs 0.1.0, sha256 69c6e8f8414f)
- Laptop exe: `C:\tkfs-lab\target-baseline\release\tkfs.exe` (tkfs 0.1.0, sha256 274b7de7e12e)
- Build commits: {'desktop': '6298d0b', 'laptop': '6298d0b'}; harness 1.4
- Path: desktop daemon -> `ws://127.0.0.1:43281` -> ssh -L -> laptop 127.0.0.1:43280 (Wi-Fi laptop 192.168.100.3)

- Idle before run: desktop CPU {'cpu_mean': 40.3, 'cpu_p50': 34.0, 'cpu_p95': 85.4, 'cpu_max': 85.4, 'samples': 8, 'build_procs_mean': 2, 'build_procs_max': 2}; laptop [{'load_pct': 7, 'top_cpu_time_processes': ['System', 'MsMpEng', 'Taskmgr', 'perfmon', 'svchost', 'svchost', 'ChatGPT', 'svchost']}, {'load_pct': 1, 'top_cpu_time_processes': ['System', 'MsMpEng', 'Taskmgr', 'perfmon', 'svchost', 'svchost', 'ChatGPT', 'svchost']}, {'load_pct': 5, 'top_cpu_time_processes': ['System', 'MsMpEng', 'Taskmgr', 'perfmon', 'svchost', 'svchost', 'ChatGPT', 'svchost']}]
- Tunnel starts: [{'tag': 'tunnel-probe', 'pid': 94480, 'seconds': 0.38}, {'tag': 'tunnel', 'pid': 32672, 'seconds': 0.3}]

## Link ceiling

| Probe | MB/s | seconds |
|---|---|---|
| scp_up (256 MiB) | 8.6 | 31.2 |
| scp_down (256 MiB) | 7.6 | 35.32 |
| tunnel_up (256 MiB) | 8.56 | 31.36 |
| tunnel_down (256 MiB) | 7.06 | 38.04 |

Ping RTT ms: min 2 / avg 2.7 / max 3

## S1: S1 initial receive (laptop -> desktop)

**Status:** incomplete (cap reached)

| Milestone | t (s) |
|---|---|
| paired | 0.47 |
| remote_list_visible | 0.58 |
| receive_returned | 0.86 |
| worker_status_ok | 2.86 |
| worker_running | 4.9 |
| first_metadata | 85.88 |
| first_object_bytes | 87.93 |

- Dataset 575 MiB; receive->complete n/a; effective n/a; transfer phase n/a; peak 10 s window 2.65 MB/s
- Laptop ingest into WinFsp mount: 102.6 s (5.9 MB/s), projection ready +1.6 s
- Source settle before S1: settled True after 14.6 s (threshold 0.25 cores; last {'t_s': 14.6, 'procs': 2, 'cores': 0.225})
- Progress reached: {'t': 1803.86, 'files_visible': 479, 'files_matching': 478, 'current_available': None, 'current_missing': None, 'partial_bytes': None, 'received_object_bytes': 553051739, 'state_ms': 3812}
- Desktop load: CPU mean 30.6% (p95 62.3%, max 98.1%), build processes mean 2.7 max 14
- Laptop load: CPU mean 17.9% max 36.3%; laptop worker status p50 507 ms max 709 ms
- Control-lane metadata: last {'t': 1792167666773, 'control_exchanges': 778, 'control_received_wire_bytes': 106812931, 'local_events': 1056, 'remote_events': 4102, 'local_only_events': 0, 'remote_only_events': 3046, 'local_pending': 310, 'remote_pending': 0}; local events/s 0.58, exchanges/s 0.43, events/exchange 1.35, local vs remote events 25.7%
- Desktop snapshot calls: {'snapshots': 2676, 'peers': {'count': 892, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 38, 'latency_ms_p95': 108, 'latency_ms_max': 3061}, 'inspect': {'count': 892, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 32, 'latency_ms_p95': 63, 'latency_ms_max': 1935}, 'status': {'count': 892, 'failures': 0, 'busy_or_stale': 454, 'latency_ms_p50': 45, 'latency_ms_p95': 118, 'latency_ms_max': 1064}}

## S2: S2 concurrent two-way incremental

**Status:** incomplete (cap reached)

| Milestone | t (s) |
|---|---|
| desktop_writes_done | 30.18 |
| laptop_writes_done | 643.49 |

- Writes: {'desktop_new_files': 200, 'desktop_errors': [], 'laptop_modified_files': 200, 'laptop_write_ms': 642065}
- State reads: {'desktop_state_failed': 0, 'laptop_state_failed': 58, 'polls': 74}; max seen {'desktop_mods_seen': 0, 'laptop_new_seen': 7}
- Progress reached: {'t': 1802.02, 'desktop_mods_seen': 0, 'laptop_new_seen': None, 'desktop_files': 683, 'laptop_files': None, 'desktop_content_complete': False, 'laptop_content_complete': False, 'desktop_state_ms': 138, 'desktop_conflicts': None, 'laptop_conflicts': None}
- Conflicts: {'desktop': None, 'laptop': None}
- Desktop load: CPU mean 29.7% (p95 52.7%, max 100.0%), build processes mean 1.8 max 41
- Laptop load: CPU mean 22.7% max 44.6%; laptop worker status p50 27 ms max 640 ms
- Control-lane metadata: last {'t': 1792169486576, 'control_exchanges': 787, 'control_received_wire_bytes': 108081273, 'local_events': 1476, 'remote_events': 4327, 'local_only_events': 386, 'remote_only_events': 3237, 'local_pending': 310, 'remote_pending': 386}; local events/s 0.23, exchanges/s 0.0, events/exchange 58.71, local vs remote events 34.1%
- Desktop snapshot calls: {'snapshots': 2718, 'peers': {'count': 906, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 37, 'latency_ms_p95': 65, 'latency_ms_max': 1741}, 'inspect': {'count': 906, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 32, 'latency_ms_p95': 54, 'latency_ms_max': 391}, 'status': {'count': 906, 'failures': 0, 'busy_or_stale': 52, 'latency_ms_p50': 148, 'latency_ms_p95': 211, 'latency_ms_max': 655}}

## S3: S3 tunnel outage during a 512 MiB transfer

**Status:** transfer never started

| Milestone | t (s) |
|---|---|
| laptop_copy_done | 124.89 |

- Desktop load: CPU mean 25.3% (p95 38.6%, max 65.9%), build processes mean 0.7 max 1
- Laptop load: CPU mean 18.8% max 28.2%; laptop worker status p50 27 ms max 673 ms
- Control-lane metadata: last {'t': 1792170105656, 'control_exchanges': 792, 'control_received_wire_bytes': 108793079, 'local_events': 1482, 'remote_events': 4343, 'local_only_events': 372, 'remote_only_events': 3233, 'local_pending': 310, 'remote_pending': 372}; local events/s 0.01, exchanges/s 0.01, events/exchange 1.0, local vs remote events 34.1%
- Desktop snapshot calls: {'snapshots': 897, 'peers': {'count': 299, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 36, 'latency_ms_p95': 45, 'latency_ms_max': 241}, 'inspect': {'count': 299, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 31, 'latency_ms_p95': 42, 'latency_ms_max': 54}, 'status': {'count': 299, 'failures': 0, 'busy_or_stale': 52, 'latency_ms_p50': 136, 'latency_ms_p95': 169, 'latency_ms_max': 269}}

## Caveats

- Desktop is shared with concurrent heavy builds/tests; see desktop_load (CPU %, build process counts) per scenario.
- Peer traffic crosses an SSH -L tunnel (encryption + single TCP stream over Wi-Fi); compare against link.tunnel_* not raw Wi-Fi.
- Milestones are observed by polling (2 s snapshots, 0.5-1 s in S3); resolution is about +/-2 s, except S2 whose loop reads both projections each pass (about 4.5 s per pass on an idle pair, far more when a `state` read times out; S2 window / reads.polls gives the period).
- S3 fault = ssh client terminated: desktop-side sockets close immediately (RST/EOF), not a silent black-hole.
- Laptop timestamps are corrected by clock_offset_ms (uncertainty listed there).
- Status polling itself takes worker locks on both sides; identical polling is used for every build (manage status only with --capture-manage-status). The `state` projection dump polled every ~2 s (desktop in S1, both machines in S2) holds the worker's engine lock: see progress state_ms.
- Control-lane metadata_summary counters (local_events, exchanges, wire bytes) are diagnostics whose meaning changes with the protocol (v3 reports feed tips); compare builds on projection/content milestones.
- cat-object can fetch a missing object on demand; verification.backing_files hashes installed objects directly and is the replication check.
