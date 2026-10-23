# Telekinesis

Versioned folders backed by immutable objects and a host-filesystem working
layer. WinFsp on Windows and FUSE on Linux expose the folder; paired sessions
replicate entity events and hydrate missing content by digest.

This checkout implements the greenfield [Wave 6 replacement](docs/revamp/WAVE_6_REPLACEMENT.md).
Performance qualification is still in progress. Historical stores and the
previous write journal are not supported by this format.

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
