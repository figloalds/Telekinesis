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
