# Step 5: entity conflicts

Concurrent successors remain entity version heads; the existing visible head
stays selected until resolution. There are no generated conflict filenames.
Resolution is an event parenting every current head and selecting one state.
Reviewed alternatives remain queryable through the conflict history and remain
object-retention roots after resolution.

The conflicts query exposes current and resolved entity heads for the desktop
and CLI. Snapshot roots, branch bases, all entity heads and reviewed alternatives
form the GC closure; active scratch/read handles add pins in the pack layer.

Tests cover concurrent edits, local-head visibility, resolution convergence,
retention of the discarded alternative, and remote invalidation while an
existing handle continues to read its opened version.

The 2026-10-28 correctness fix seals pending local edits before resolving under
the publication lock. Sealing after resolution reopened the conflict and omitted
the latest local bytes from the reviewed alternatives. A failed pre-resolution
flush leaves the choice unapplied. Resolution notifies both the previous and
selected names; incoming delete conflicts notify the actual visible head.
Host regressions cover retained bytes/restart, rename/delete cache actions and
the failed-flush retry. The real-mount smoke exercises offline peer edits and
cached rename resolution on both drive-letter and directory mounts.

Conflict responses also distinguish `current_heads` from retained history.
When an entity conflicts again after a resolution, its earlier alternatives
remain visible as history but the desktop enables selection only for current
heads. A regression covers both conflict generations and rejects stale choices.
