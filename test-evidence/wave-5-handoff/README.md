# Preserved Wave 5 exploratory work

`catchup_during_writes_probe.rs` is the integration worktree's uncommitted
`tests/zz_probe_wp52.rs`, preserved as research source. It explores metadata
catch-up while mounted writes continue and prints intermediate progress.
It is not part of the compiled test suite, not a new passing regression, and
was not run during closure. To use it later, review its assumptions and adapt
it to the current `tests/support` harness rather than invoking it as a
standalone Rust program.

`wp5-4-uncommitted-notes.md` preserves the package worktree's final uncommitted
notes verbatim, including its unfinished `SUITE_RESULT` field. This is a raw
historical artifact, not an additional test result. Its comment-only source
fix was already integrated as `05796d8`; the archived notes preserve the
additional review and verification context without inventing the missing
suite outcome.

The original worktree and its raw test logs were left intact. The main
[handoff](../../docs/revamp/notes/wave-5-handoff.md) lists completed work,
evidence and unresolved decisions.
