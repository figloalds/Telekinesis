# Validation

```powershell
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
python scripts/replacement_smoke.py --binary target/release/tkfs.exe
```

The trimmed suite retains transport framing/authentication/TLS tests and exact
filesystem invariants: receipt-based exactly-once sealing, recovery of unsealed
scratch files, changed-chunk reuse, snapshot fork isolation, concurrent entity
heads and explicit resolution, pinned pack GC, range hydration with missing
objects, and remote invalidation. Additional regressions cover local metadata
changes during an edit and temporary-file replacement of a stable target entity.

The real-mount smoke checks both mounted bytes and committed bytes, then pairs
two peers and updates an already-cached file. It runs on Windows and Linux;
use `--binary` and `--home` to select the executable and disposable test directory.

## Performance gate

Only `scripts/bench.py` defines the performance gate. It runs one warm-up and
five samples and reports medians for small-file copy, .NET clean/no-op builds,
1 GiB sequential write/read, 10,000 random 4 KiB overwrites, git status on a
50,000-file tree, and cached remote visibility after local apply.

```powershell
python scripts/bench.py --label native
python scripts/bench.py --label mounted --root X:/bench --compare test-evidence/bench/native.json
python scripts/bench.py --label wave6 --matrix-tkfs target/release/tkfs.exe
```

The matrix uses disposable stores on X: and Y: by default. `--matrix-mount` and
`--matrix-peer-mount` choose other free letters. It measures native, mounted,
and mounted with one paired peer, using identical workload dimensions.
Reports are under `test-evidence/bench/`. An exceeded target, failed workload or
incompatible baseline fails the command. Partial `--only` runs are diagnostic;
they do not establish completion of the full gate.

Qualification remains open until the full mounted matrix passes. Earlier
capsules and CCR qualification scripts are not part of the replacement gate.
