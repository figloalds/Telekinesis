# Keep file creation out of pack durability

Date: 2026-10-24. Part of W6-P0 performance acceptance.

Every regular-file create imported the canonical empty object and flushed the
shared pack store. That flush could commit unrelated objects being prepared by
the background sealer, breaking its intended pack batching and adding content
I/O to a metadata operation.

The store now installs the empty object durably once during startup and holds
a pin for it. Creates reuse its immutable reference without touching the pack
store. GC cannot remove it while the store can create another empty file.
Explicit file fsync, seal durability, caching and cleanup flushing are unchanged.

The regression first observed a pending unrelated object being committed by
create. It now verifies that the object remains pending and that the empty
reference survives GC. All 145 Rust tests, format and Clippy pass. Release
`e38f887a...` awaits its own 34-case VM matrix and copy diagnostic, queued only
after the current Git/build diagnostic finishes with clean bounded shutdowns.
Any cleanup timeout refuses further mounts in that guest.

The preceding recovery release `cf54ca85...` passes all 34 lifecycle cases,
including deterministic equal-timestamp recovery. Its copy median is still
11.59x native, so performance acceptance remains OPEN. The queued candidate is
not qualified by that earlier lifecycle result.

A subsequent [open/close lock correction](open-close-locks.md) passes host
regressions and awaits separate VM qualification; it is not part of this
candidate's pending measurements.
