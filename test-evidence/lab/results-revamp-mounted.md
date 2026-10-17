# tkfs two-machine lab: revamp-mounted

- Desktop exe: `C:\tkfs-build\lab\target-revamp\release\tkfs.exe` (tkfs 0.1.0, sha256 424e37612df4)
- Laptop exe: `C:\tkfs-lab\target-revamp\release\tkfs.exe` (tkfs 0.1.0, sha256 cd6687e09cfe)
- Build commits: {'desktop': 'e8c2979', 'laptop': 'e8c2979'}; harness 1.4
- Path: desktop daemon -> `ws://127.0.0.1:43281` -> ssh -L -> laptop 127.0.0.1:43280 (Wi-Fi laptop 192.168.100.3)

- Idle before run: desktop CPU {'cpu_mean': 14.2, 'cpu_p50': 10.1, 'cpu_p95': 43.8, 'cpu_max': 43.8, 'samples': 9, 'build_procs_mean': 0, 'build_procs_max': 0}; laptop [{'load_pct': 1, 'top_cpu_time_processes': ['System', 'MsMpEng', 'perfmon', 'Taskmgr', 'svchost', 'svchost', 'ChatGPT', 'svchost']}, {'load_pct': 2, 'top_cpu_time_processes': ['System', 'MsMpEng', 'perfmon', 'Taskmgr', 'svchost', 'svchost', 'ChatGPT', 'svchost']}, {'load_pct': 8, 'top_cpu_time_processes': ['System', 'MsMpEng', 'perfmon', 'Taskmgr', 'svchost', 'svchost', 'ChatGPT', 'svchost']}]
- Tunnel starts: [{'tag': 'tunnel', 'pid': 29496, 'seconds': 0.26}]

## M1: M1 mounted receive (laptop -> desktop WinFsp mount)

**Status:** incomplete or mismatched

| Milestone | t (s) |
|---|---|
| paired | 0.39 |
| remote_list_visible | 0.7 |
| receive_returned | 0.98 |
| gate_open_seen | 1.04 |
| gate_cleared | 3.13 |
| mount_attached | 3.13 |
| inspect_running | 3.13 |
| mount_usable | 3.15 |
| listing_done | 3.37 |
| hash_done | 34.98 |
| new_file_written | 35.0 |
| new_file_on_laptop | 38.34 |

- Laptop ingest into WinFsp mount: 25.1 s (21.7 MB/s), projection ready +0.5 s
- Source settle before S1: settled True after 14.7 s (threshold 0.25 cores; last {'t_s': 14.7, 'procs': 2, 'cores': 0.003})
- Desktop load: CPU mean 9.0% (p95 16.6%, max 22.1%), build processes mean 0 max 0
- Laptop load: CPU mean 13.4% max 23.8%; laptop worker status p50 20 ms max 21 ms
- Control-lane metadata: last {'t': 1792171296577, 'control_exchanges': 71, 'control_received_wire_bytes': 645710, 'local_events': 505, 'remote_events': 505, 'local_only_events': 0, 'remote_only_events': 0, 'local_pending': 0, 'remote_pending': 0}; local events/s 13.23, exchanges/s 1.83, events/exchange 7.21, local vs remote events 100.0%
- Desktop snapshot calls: {'snapshots': 80, 'peers': {'count': 20, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 27, 'latency_ms_p95': 33, 'latency_ms_max': 35}, 'inspect': {'count': 20, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 27, 'latency_ms_p95': 35, 'latency_ms_max': 36}, 'status': {'count': 20, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 26, 'latency_ms_p95': 34, 'latency_ms_max': 38}, 'manage-status': {'count': 20, 'failures': 0, 'busy_or_stale': 0, 'latency_ms_p50': 25, 'latency_ms_p95': 34, 'latency_ms_max': 51}}

## Caveats

- Desktop is shared with concurrent heavy builds/tests; see desktop_load (CPU %, build process counts) per scenario.
- Peer traffic crosses an SSH -L tunnel (encryption + single TCP stream over Wi-Fi); compare against link.tunnel_* not raw Wi-Fi.
- Milestones are observed by polling (2 s snapshots, 0.5-1 s in S3); resolution is about +/-2 s, except S2 whose loop reads both projections each pass (about 4.5 s per pass on an idle pair, far more when a `state` read times out; S2 window / reads.polls gives the period).
- S3 fault = ssh client terminated: desktop-side sockets close immediately (RST/EOF), not a silent black-hole.
- Laptop timestamps are corrected by clock_offset_ms (uncertainty listed there).
- Status polling itself takes worker locks on both sides; identical polling is used for every build (manage status only with --capture-manage-status). The `state` projection dump polled every ~2 s (desktop in S1, both machines in S2) holds the worker's engine lock: see progress state_ms.
- Control-lane metadata_summary counters (local_events, exchanges, wire bytes) are diagnostics whose meaning changes with the protocol (v3 reports feed tips); compare builds on projection/content milestones.
- cat-object can fetch a missing object on demand; verification.backing_files hashes installed objects directly and is the replication check.

## Mounted check details

- mount_point: C:\tkfs-build\lab\mnt\revamp-mounted
- gate_cleared_as: none
- gate_transitions: [(1.04, 'awaiting'), (3.13, 'none')]
- mount_transitions: [(1.04, 'detached'), (3.13, 'attached')]
- content_complete_at_mount: False
- content_at_mount: {'complete': False, 'current_available': 200, 'current_missing': 1, 'current_objects': 201, 'downloading': 1, 'items': [{'downloaded': 6123461, 'error': None, 'object': 'eb47383a505e8996a9bed0c31d1202518225722bb81ae3ee52ab05cd83998631', 'path': 'big.bin', 'size': 536870912, 'size_known': True, 'state': 'downloading'}], 'partial_bytes': 6123461, 'requested': 0, 'shared_available': 201, 'shared_objects': 202}
- backing_at_mount: {'layout_known': True, 'present': 200, 'total': 201, 'missing': ['eb47383a505e8996a9bed0c31d1202518225722bb81ae3ee52ab05cd83998631']}
- listing: {'seconds': 0.18, 'files_listed': 201, 'expected': 201, 'missing': [], 'extra': [], 'tkfs_entries': [], 'size_mismatch': [], 'all_listed': True}
- hash_via_mount: {'files': 201, 'ok': 200, 'bad': ['big.bin: [Errno 22] Invalid argument'], 'bytes': 6637430, 'seconds': 31.6, 'MBps': 0.21, 'large_file_seconds': None, 'all_match': False}
- new_file: {'path': 'mounted-check/desktop-new.bin', 'size': 1048576, 'sha256': '4bf69f22c10ce559368ebf649ed178ffb10d16ec78d2bef5ecf446e40346683c', 'write_s': 0.022, 'desktop_projection_s': 2.73, 'laptop_projection_s': 0.67, 'laptop_content_complete_s': 3.34, 'laptop_mount_sha256_matches': True, 'laptop_mount_hash_s': 1.88}
- stop: {'rc': 0, 'seconds': 0.05, 'output': ',\n              "offer_ms": null,\n              "received_object_bytes": 8388608,\n              "received_wire_bytes": 8388608,\n              "remote_exchange_ms": null,\n              "sent_object_bytes": 0,\n              "sent_wire_bytes": 0,\n              "total_ms": 1501,\n              "transport": "tkfs-peer/3"\n            }\n          },\n          "role": "bulk",\n          "transport": "tkfs-peer/3"\n        }\n      ],\n      "metadata": {\n        "caught_up": true,\n        "consecutive_failures": 0,\n        "error": null,\n        "last_progress_ms": 1792171292366,\n        "last_success_ms": 1792171296608,\n        "metadata_caught_up": true,\n        "metadata_progress": {\n          "local_events": 505,\n          "local_only_events": 0,\n          "local_pending": 0,\n          "remote_events": 505,\n          "remote_only_events": 0,\n          "remote_pending": 0\n        },\n        "object_inventory": null,\n        "retry_at_ms": null,\n        "total_received_object_bytes": 0,\n        "total_received_wire_bytes": 646546,\n        "total_sent_object_bytes": 0,\n        "total_sent_wire_bytes": 100230,\n        "transfer": {\n          "apply_ms": null,\n          "offer_ms": null,\n          "received_object_bytes": 0,\n          "received_wire_bytes": 836,\n          "remote_exchange_ms": null,\n          "sent_object_bytes": 0,\n          "sent_wire_bytes": 765,\n          "total_ms": 62,\n          "transport": "tkfs-peer/3"\n        }\n      },\n      "remote_available": true\n    }\n  ]\n}\n', 'mount_point_gone': True, 'inspect_after': {'running': False, 'starting': False, 'mounted': False, 'initial_sync_pending': False, 'startup_phase': None, 'error': None}}
- manage status / inspect during the gate: 1 samples, 0 status failures, status latency {'p50': 26, 'p95': 26, 'max': 26}, inspect latency {'p50': 37, 'p95': 37, 'max': 37}

Example at t=1.04 s:

```json
{
 "t": 1.04,
 "status_folder": {
  "phase": "starting",
  "startup": {
   "phase": "opening state",
   "seq": 1
  },
  "gate": {
   "since_ms": 1792171257698,
   "source": "9cdc7fcf-582b-4bd4-a25a-ff2bd0629863",
   "state": "awaiting",
   "waiting_on": []
  },
  "mount_state": "detached",
  "mount_intent": "attach",
  "activation": {
   "counters": {
    "activated": 0,
    "fast": 0,
    "last_activation_at_ms": null,
    "last_slice_ms": 0,
    "quarantined": 0,
    "retried": 0,
    "running": false,
    "slices": 0,
    "slow": 0,
    "stale": 0,
    "starved_since_ms": null
   },
   "inbox": {
    "blocked": 0,
    "quarantined": 0,
    "ready": 0,
    "retry": 0,
    "unclassified": 0,
    "waiting": 0
   }
  },
  "content": {
   "complete": false,
   "current_available": 0,
   "current_missing": 0,
   "current_missing_bytes": 0,
   "current_objects": 0,
   "downloading": 0,
   "partial_bytes": 0,
   "requested": 0,
   "shared_available": 0,
   "shared_missing_bytes": 0,
   "shared_objects": 0
  },
  "sync": {
   "peers": [
    {
     "backoff_until_ms": null,
     "direction": "outbound",
     "endpoint": "ws://127.0.0.1:43281",
     "error": null,
     "installation": "9cdc7fcf-582b-4bd4-a25a-ff2bd0629863",
     "label": null,
     "last_progress_ms": null,
     "metadata": {
      "blocked": 0,
      "caught_up": false,
      "cursor_seq": 0,
      "last_exchange_ms": null,
      "local_pending": 0,
      "quarantined": 0,
      "rejected": 0,
      "remote_pending": 0,
      "remote_tip": 502,
      "to_receive": 0,
      "to_send": 0
     },
     "objects_down": {
      "bytes_remaining": 0,
      "eta_s": null,
      "in_flight": 0,
      "last_progress_ms": null,
      "objects_remaining": 0,
      "rate_bps": 0,
      "total_bytes": 0
     },
     "objects_up": {
      "bytes_remaining": 0,
      "eta_s": null,
      "in_flight": 0,
      "last_progress_ms": null,
      "objects_remaining": 0,
      "rate_bps": 0,
      "total_bytes": 0
     },
     "stalled_since_ms": null,
     "state": "waiting_for_worker"
    }
   ],
   "state": "stopped",
   "summary": "Not syncing: worker starting (opening state)"
  },
  "errors": []
 },
 "inspect": {
  "running": false,
  "starting": true,
  "mounted": false,
  "initial_sync_pending": true,
  "startup_phase": "synchronizing remote metadata",
  "error": null
 }
}
```
- `manage status` text at t=1.04 s (gate): runs/revamp-mounted/manage-status-00-gate.txt
- `manage status` text at t=3.13 s (gate-cleared): runs/revamp-mounted/manage-status-01-gate-cleared.txt
- `manage status` text at t=3.13 s (gate-cleared): runs/revamp-mounted/manage-status-01-gate-cleared.txt
