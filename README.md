# Telekinesis

Versioned folders backed by immutable objects and a host-filesystem working
layer. WinFsp on Windows and FUSE on Linux expose the folder; paired sessions
replicate entity events and hydrate missing content by digest.

**Windows mount lifecycle blocker (OPEN, P0):** resolve
[W6-P0-MOUNT-LIFECYCLE](docs/revamp/WAVE_6_MOUNT_LIFECYCLE_BLOCKER.md) before
other Wave 6 work or mounted qualification. The daemon no longer holds a handle
into its own mount (see `docs/revamp/notes/mount-lifecycle-fix.md`), but the
exit checks have not been run on a mount yet. Run mounted
repro/quickstart/benchmarks in an isolated Windows VM until they pass. A folder
mount does not remove this dependency.

This checkout implements the greenfield [Wave 6 replacement](docs/revamp/WAVE_6_REPLACEMENT.md).
Mounted qualification is blocked by the lifecycle demand above. Historical
stores and the previous write journal are not supported by this format.

```powershell
cargo build --release
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check

# Create and mount one folder.
target/release/tkfs --state C:/tkfs/example init
target/release/tkfs --state C:/tkfs/example daemon --mount X: --listen 127.0.0.1:43180

# Use another terminal while the daemon serves the mount.
target/release/tkfs --state C:/tkfs/example status
target/release/tkfs --state C:/tkfs/example conflicts
target/release/tkfs --state C:/tkfs/example gui
target/release/tkfs --state C:/tkfs/example stop
```

[Architecture](docs/ARCHITECTURE.md), [configuration](docs/CONFIGURATION.md),
[validation](docs/VALIDATION.md), and [authority scope](docs/revamp/AUTHORITY_SCOPE.md).

GPL-3.0-or-later. See the repository license and Slint notices.
