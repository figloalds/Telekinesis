# Linux

Build with `cargo build --locked`; outbound WSS requires the OpenSSL development
libraries and pkg-config (for example, `libssl-dev` and `pkg-config` on Debian/
Ubuntu). Install the distribution's FUSE 3 runtime and
make `/dev/fuse` available for mounting. Headless workers need no FUSE device.

The executable needs the matching OpenSSL runtime libraries. WSS uses the
distribution's CA trust bundle (commonly supplied by `ca-certificates`). These
are platform dependencies; TKFS does not install certificates or configure trust.

```sh
tkfs init -f ./orchestrator.toml
tkfs -f ./orchestrator.toml orchestrator
# In another terminal:
tkfs -f ./orchestrator.toml create Project
```

Use `--headless` on create/receive to run without a mount. Ordinary filesystem
access determines whether a directory is usable. TKFS does not require a matching
OS owner, private directory modes, systemd credentials or a keyring. It does not
change permissions on backing state/config directories. FUSE's normal kernel
permission checks and mount-helper policy still apply as configured by the OS.

The daemon, management clients and workers use the same WebSocket protocol on
both platforms. Remote dialing supports WS/WSS; local control and the daemon's
listener use plain WS. For service startup, adapt `deploy/tkfs-orchestrator.service.example`;
service identity, mount namespaces, paths and network exposure are OS configuration.

`manage shutdown` gracefully stops managed mounts. Standalone workers also handle
SIGINT/SIGTERM and refuse shutdown while busy. The daemon is normally stopped
through its management API; a forced daemon exit leaves workers until the next
daemon stops and restarts them under supervision.

Portable POSIX permission bits and basic timestamps remain filesystem metadata.
Symlinks, hardlinks, devices, native ownership and privileged mode bits are not
implemented. Case-insensitive portable names apply on Linux as on Windows.
