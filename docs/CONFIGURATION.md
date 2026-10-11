# Configuration

Create a file with `tkfs init -f PATH`. Existing files are never overwritten.
Config paths resolve relative to the TOML file. Prefer absolute mount paths
when supplying paths through the CLI or desktop.

```toml
format_version = 2
data_directory = "data"
default_mount_directory = "mounts"
maximum_running = 8

[network]
listen = "127.0.0.1:43180"
connections = 4
sync_interval_ms = 500
```

| Field | Meaning |
|---|---|
| `format_version` | Must be 2. Unknown fields are rejected. |
| `data_directory` | Ordinary writable backing state and catalog directory. |
| `default_mount_directory` | Parent for automatically assigned mount paths. |
| `maximum_running` | Maximum supervised workers, 1–64; default 8. |
| `network.listen` | Plain WS bind address. Default `127.0.0.1:43180`. Use `""` to disable inbound networking, or `"0.0.0.0:43180"` to bind all IPv4 interfaces. |
| `network.connections` | Default persistent connections per paired peer, 1–16; default 4. |
| `network.sync_interval_ms` | Delay between sync passes, 50–60000 ms; default 500. |

Restart the daemon after editing TOML. Folder mount changes and pairing changes
use the management API and persist immediately. A stopped folder can be assigned
a new mount or made headless. Mount paths must not overlap backing data or other
managed mounts. A mount target must not already exist. These are filesystem
correctness constraints, not ownership or ACL policy.

## Managed files

```text
data/
  folders.sqlite             v2 folder catalog, pairing verifier, outbound peers
  orchestrator.lock           single-writer lock, no ownership policy
  orchestrator.json           current local WS endpoint and network address
  states/<state UUID>/
    ...                      TKFS SQLite/object/staging state
    owner.lock               excludes duplicate workers
    runtime.json             local WS discovery, no credential/token
    worker.log               startup/runtime diagnostics
mounts/<folder label>/           real filesystem mount
```

The receiving code is 32 random bytes rendered as 64 hex characters. Its verifier
has an independent random salt and SHA-256 digest with a format-specific prefix.
The code itself is returned once by `manage reset-pairing-code`. A dialing peer
stores the remote code as ordinary catalog data for restart/reconnect. There is
no encrypted secret file, OS keyring, DPAPI or systemd credential dependency.
The daemon never chmods/chowns backing directories or rewrites Windows ACLs.

Back up filesystem states and `folders.sqlite` while workers/daemon are stopped.
Config version 1 is rejected. The v2 catalog has a different filename, so the
old `orchestrator.sqlite` is never silently reinterpreted or replaced. Old worker
states can still be opened by the standalone `daemon` command. No automated
catalog migration is supplied.

## Reverse proxy

Both peer and local management protocols are WebSockets. Management and worker
control bind ephemeral loopback ports; the configured listener handles only
pairing, folder discovery and replication, not management actions.

For example, an operator-managed nginx TLS endpoint can forward upgrades to the
orchestrator's `127.0.0.1:43180` listener:

```nginx
location / {
    proxy_pass http://127.0.0.1:43180;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_read_timeout 120s;
}
```

Configure the HTTPS listener/certificates separately. TKFS itself dials only
`ws://` URLs, including DNS hostnames and IPv6 addresses. If the remote route is
TLS-only, supply a local WS-to-WSS proxy or tunnel on the dialing host and pair
to that proxy's `ws://` address. No automatic TLS negotiation or custom certificate
handling is built into TKFS. Listener exposure and TLS are wholly operator choices.
