# Repository instructions

Read [W6-P0-MOUNT-LIFECYCLE](docs/revamp/WAVE_6_MOUNT_LIFECYCLE_BLOCKER.md)
before modifying or running a filesystem mount. The user's 2026-10-28 instruction
is to proceed with Wave 6 correctness/implementation and leave performance gates
for last. This supersedes the earlier performance prerequisite for other work;
it does not close P0 or authorize host mounts or cutover before full acceptance.

- The user-mode filesystem server must not have a crash/termination dependency
  on I/O or retained handles to its own served namespace. This includes root
  handles used for cache flushing. Cover forced termination as well as orderly
  stop; a Drop handler or graceful-path reorder alone is insufficient.
- Until the blocker is closed, run mounted hang repro/qualification in an
  isolated Windows VM. Drive-letter and directory mounts use the same driver
  and callbacks; a directory mount is not a workaround for this lifecycle bug.
- Bound test shutdown, dump collection and post-kill waits. Preserve evidence
  and report a stranded process as failure. Do not wait forever or launch more
  mounts after a cleanup timeout. Preserve cache, durability and access semantics
  while fixing this; do not mask the failure by disabling them.
