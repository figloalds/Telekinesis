# Telekinesis

Versioned folders backed by immutable objects and a host-filesystem working
layer. WinFsp on Windows and FUSE on Linux expose the folder; paired sessions
replicate entity events and hydrate missing content by digest.

**Windows mount lifecycle blocker (OPEN, P0):** the no-self-handle fix and
repeated isolated lifecycle matrices pass; full acceptance remains open. See
[W6-P0-MOUNT-LIFECYCLE](docs/revamp/WAVE_6_MOUNT_LIFECYCLE_BLOCKER.md) for the
qualified WinFsp dependency and evidence. Per the user's 2026-10-28 decision,
finish Wave 6 correctness and implementation before the final performance gates.
Keep mounted repro/quickstart/benchmarks in an isolated Windows VM until full
acceptance. Directory mounts require the same isolation as drive-letter mounts.

This checkout implements the greenfield [Wave 6 replacement](docs/revamp/WAVE_6_REPLACEMENT.md).
Host mounted qualification and cutover remain blocked by the demand above. Historical
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
