# Validation

The folder-first rewrite was checked on Windows and Ubuntu/WSL on 2026-10-11.
The reboot results below describe commit `26ab12d`; the outbound WSS follow-up
has separate results at the end.

```sh
cargo check --locked --lib --bin tkfs
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
python scripts/orchestrator_e2e.py --exe target/debug/tkfs.exe
```

The full Windows suite passes all 74 tests, with no failures or ignored tests.
On Linux, select the Linux executable for the Python harness. The targeted Linux
run covers the library, `orchestrator` and `paired_sync` test targets (12 tests).
Strict Clippy checks pass on both platforms, including the older Rust 1.88 Linux
toolchain. The public CLI harness passes on Windows and Linux using four persistent
WS lanes and bidirectional sync between two headless orchestrators.

The six process integration tests exercise:

- Independent orchestrator/worker processes, two repositories, three persistent
  WS lanes, bidirectional sync with only one side dialing, and restart.
- Server-persisted salted verifier, code-only pairing, wrong-code refusal, code
  rotation invalidating existing sessions, re-pairing and local pool removal.
- A/B/C relay with events from three origins and private branches remaining local.
- Real WinFsp and FUSE mounts: filesystem write/read, cooperative stop, changed
  mount path, and remount with the original bytes intact. The FUSE test skips
  hosts without `/dev/fuse`; it ran with a real FUSE device in this validation.
- Version 2 configuration defaults and rejection of removed credential fields.
- A 6 MiB file transferred through bounded pages after a daemon crash, recovery
  of a surviving worker, and receiving-daemon restart with the same verifier.

Linux integration fixtures use ordinary group/world-writable parent directories,
confirming that the daemon does not impose private storage modes.

Retained tests cover causal conflicts, branches, snapshots, basic/POSIX metadata,
atomic save/recovery boundaries, acknowledgement correctness, object segmentation,
large files, bounded replication pages and 10,000 durable saves. Removed tests
were specific to encrypted transport, OPAQUE, credential stores and the previous
supervisor/desktop contracts. The new process and public CLI suites exercise the
replacement architecture.

The Windows Slint desktop compiles against the new management API. Physical UI
interaction, real separate-machine networking, operator reverse proxies and
unattended installed services are not qualified by these local tests. Test data
is disposable; no user data/configuration is migrated or deployed by validation.

## Outbound WSS follow-up

On 2026-10-11, `cargo test --locked` passed all 80 Windows tests with no failures
or ignored tests. Formatting and strict Clippy checks passed, and the public CLI
harness passed its four-lane bidirectional WS scenario.

The new transport tests cover WS/WSS endpoint validation, persistent WSS messages,
preserved paths/query strings, rejection of an untrusted server certificate and
rejection of a hostname mismatch with a trusted test CA. The test connector trusts
the fixture CA in memory only. These tests do not install certificates or restore
the retired enrollment, credential storage or ACL mechanisms.

The follow-up Ubuntu/WSL run (`cargo test --locked --lib --test orchestrator
--test paired_sync --target-dir target/linux-review`) stopped during the OpenSSL
dependency build because `pkg-config` was unavailable. It did not run the Linux
tests; install the build prerequisites in the [Linux guide](LINUX.md) before
repeating it. The earlier reboot's Linux results do not qualify the WSS change.
