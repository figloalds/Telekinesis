# Architecture

The event journal is authoritative. Each entity has a stable ID and versions
with explicit parents. Rename, deletion and timestamp changes update the
metadata database and its indexed in-memory namespace. Directory lookup and
listing never replay event cones.

## Mutable files and durability

A host scratch file backs each active byte version. Local metadata successors
share the draft; a remote content version selects a different scratch file.
Read, write, append and truncate use host file handles. Writes update a dirty
flag and 4 KiB bitmap, with no hashing or database transaction.

Last writable cleanup schedules a seal after one second, or sooner under the
64 MiB pending-size budget. Flush/fsync seals immediately. Changed 64 KiB chunks
are hashed and compared with the base; unchanged chunks retain their digests.
A pack flush precedes the atomic content-event and idempotency-receipt commit.
Metadata operations batch over a few milliseconds. Flush checkpoints namespace
changes too. Unsealed scratch records are examined on startup; receipts and
content comparison prevent duplicate seals when timestamps change at close.

The host cache remains enabled. Remote apply invalidates cached paths through
the native notify API. Already-open handles retain their byte version.

## Objects, snapshots and conflicts

Append-only packs target 64 MiB. A SQLite digest index locates objects, while
a bounded 256 MiB verified LRU avoids repeat hashing. Corrupt objects become
missing and can be fetched again. Manifests are flat ordered chunk lists.

A snapshot is a sorted tree root plus a frontier of entity versions. Forking
stores another reference to the same snapshot. Branch state is loaded from
cached trees and its current-entry deltas. Conflicting entity successors remain
heads in the conflicts view. Resolution parents all heads; discarded alternatives
stay reachable through history and retention roots, without sibling filenames.

GC marks snapshot history, branch bases, current heads, conflict alternatives
and handle pins, then rewrites packs below the live-fraction threshold. Pack
index changes commit before old segments are removed.

## Sessions and ownership

The orchestrator (one per data directory) owns the catalog (`folders.sqlite`:
folders, intents, pairing verifier, peer credentials, clock offsets), runs one
worker per folder and talks to paired peers; workers never listen for peers
or dial. Each worker owns one folder's metadata database and pack index and
serves a loopback multiplexed session (control lane 0, bulk lanes for
objects) to its orchestrator and to the folder commands. Between
orchestrators the same frame protocol runs over a pool of lanes per peer:
lane 0 carries metadata and control, the other lanes object bytes. Existing
authenticated session framing, TLS clients, pairing-code verification and
enrolled credentials are retained. Events apply before objects arrive;
foreground hydration precedes background prefetch, with digest requests in
bounded batches and per-peer backoff. See
[the restoration design](superpowers/specs/2026-10-28-orchestration-restoration-design.md).

There is no private write database, overlay map, save pipeline, object admission
outbox, per-reference grant, lease generation or pool-fencing engine.
