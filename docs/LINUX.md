# TKFS Linux headless/FUSE

The Linux CLI and daemon reuse the causal core, SQLite/CAS durability, staging,
branch privacy and encrypted peer transport. Desktop UI and the O1 Windows
supervisor remain Windows-only. Linux builds do not compile Slint.

## Build and run

Use a native Linux filesystem for the checkout and state, with Rust/Cargo, Python
3 and a C compiler installed. Mounts require `/dev/fuse` access and `fusermount3`.
The adapter speaks kernel FUSE protocol 7.31 and uses the installed owner-only
`fusermount3` helper. It does not link libfuse or require its development headers.
The FUSE adapter adds `libc`; the separate paired-data service adds standard
Rustls/ring, rcgen and zeroize dependencies. Bundled SQLite needs a C compiler.

```bash
# Run from the repository root, with cargo and rustc available on PATH.
cargo build --locked
./target/debug/tkfs init --state /native/path/new-state
# State must exist; mount folder must not exist, and its parent must exist.
./target/debug/tkfs daemon --state /native/path/new-state --mount /native/path/new-mount
```

From another shell outside the mount:

```bash
./target/debug/tkfs --runtime /native/path/new-state/runtime.json status
./target/debug/tkfs --runtime /native/path/new-state/runtime.json branch private-work
./target/debug/tkfs --runtime /native/path/new-state/runtime.json checkout private-work
./target/debug/tkfs --runtime /native/path/new-state/runtime.json stop
```

`/native/path/new-state` and `/native/path/new-mount` are placeholders for paths
you choose on a native Linux filesystem. State must be outside the mount.

See [PAIRING.md](PAIRING.md) for the separate portable TLS data service,
owner-scoped repository grants and systemd credential/unit templates. The
legacy standalone `--listen` PSK test transport is not the paired service and
must not be treated as approved device authentication.

`-C /native/path/new-mount` discovers the same daemon through its read-only
`.tkfs-runtime.json`. Local control uses a mode-0600 Unix socket and checks both
endpoint UIDs with SO_PEERCRED, plus the capability token. State is owner-only
(0700). Nonblocking flock prevents competing state owners and releases on crash.
`stop` unmounts and exits only when the view is quiet. SIGINT/SIGTERM request the
same quiet shutdown; busy shutdown is refused with a diagnostic. Close open
files, directory iterators and shells whose cwd is inside the mount before
checkout or shutdown. A forced kill can leave a disconnected mount requiring
`fusermount3 -u -- /the/owned/mount`; do not detach someone else's mount.

## Filesystem and cross-platform contract

- Explicit fsync/flush publishes a complete durable revision; close also attempts
  publication. Failed publication retains staged bytes and original revision
  bases in pending saves. Buffered writes alone are not durable acknowledgments.
- Files use direct I/O, without writeback caching; lookup/attribute TTLs are zero.
  Private mmap (including ordinary ELF execution) worked in qualification;
  arbitrary mapped writes and shared mmap are not qualified. Notifications and
  inotify semantics are not implemented/qualified by this initial adapter.
- Checkout refuses live contexts, fences opens, unmounts before committing the
  selection and remounts at the same path. Kernel cache state is discarded.
  Unmount refusal preserves the original view. Remount failure reports unhealthy
  unavailable state; restart from durable selection. Fault-boundary injection
  for every remount stage remains later work.
- POSIX permission bits 0000..0777, including execute, are persisted in the causal
  `basic` metadata register. They survive content saves, rename, private forks,
  checkpoints/restores, restart and Windows metadata edits. Concurrent metadata
  changes retain conflict alternatives; permissions and timestamps are one atomic
  register, not independently merged fields. Windows retains POSIX bits without
  interpreting execute. Windows READONLY masks Linux write permissions.
- UID/GID are the daemon user/group; ownership changes, privileged mode bits,
  ACLs/xattrs, symlinks, hardlinks, device nodes and special files return explicit
  unsupported errors. Repositories requiring these cannot be fully checked out;
  they are never silently converted. POSIX open-unlink and replace-open-target
  semantics are deliberately stricter: operations are refused while those entries
  have open contexts. File sizes retain the core's bounded content policy.
- Names retain TKFS's version-1 NFC/lowercase case-insensitive matching and Windows
  name restrictions. `File` and `file` address one entry; exclusive case-only
  creation collides. This is not a native case-sensitive Linux namespace.
  Cross-platform repositories containing case-distinct paths are unsupported.
- The peer format is now **5** to include portable permissions. Update both
  endpoints before pairing; older peers are rejected rather than acknowledging
  metadata they cannot project. Local old metadata without POSIX bits retains
  default file 0644/directory 0755 behavior.

## Qualification and remaining scope

Run `python3 scripts/linux_fuse_e2e.py` on Linux with `/dev/fuse` access. It creates
only its own state, mount and Git fixtures under ignored `test-runs/linux-evidence`
(or `--evidence PATH`), retains logs
and JSON, and unmounts/stops its children. It checks disk staging, fsync, truncate,
rename/replace, enumeration, timestamps, executable bits, case collision,
explicit unsupported metadata, owner socket/lock, busy file and cwd checkout,
private branch isolation, restart, Git clone/status/edit/commit, and compiling and
running a C executable on the real mount. Rust tests cover causal/privacy/recovery
regressions and POSIX metadata preservation/conflicts.

This qualification does not establish production readiness, inotify/IDE behavior,
multi-user security, power-loss durability, or WAN behavior. Next network scope:
pair disposable Windows WinFsp and Linux FUSE states using ephemeral keys, qualify
shared saves and executable-bit propagation/private isolation, reconnect and
concurrent conflicts, then use an independent host/VPS for LAN/WAN conditions.
No GUI port, chunk experiments or branch-mount experiments are included.

## Development and verification

Use the repository checkout and Cargo.lock on each platform. No particular
username, checkout directory, Git branch or private Cargo cache is required.
Run from the repository root:

```bash
cargo test --locked
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
python3 scripts/linux_fuse_e2e.py
```

Add `--offline` to Cargo commands only after dependencies are cached. The mounted
harness needs a built `target/debug/tkfs`; `--binary PATH` selects another build
and `--evidence PATH` selects its disposable output directory. To assemble a
Linux runnable with build metadata and license notices, use
`python3 scripts/build.py --platform linux` (Python 3.12+).

Historical qualification used Ubuntu 24.04.4 x86_64 under WSL2, Rust/Cargo 1.88,
GCC 13.3 and FUSE 3.14. These are recorded test conditions, not required checkout
paths or a guarantee for every distribution/kernel combination.
