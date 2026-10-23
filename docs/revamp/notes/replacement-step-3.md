# Step 3: scratch-file working layer

Open files share a host scratch file for their opened entity version. Local
chunks are copied at open; missing remote ranges are materialized on demand.
Reads, writes, append and truncate use the host file. Writes mark a compact
4 KiB bitmap and never hash, open a database or append journal records.
Remote versions select independent scratch files, preserving existing handles.

Scratch records retain the base version and materialization map for recovery.
Demand hydration records its materialized ranges before allowing edits, so
recovery cannot mistake an unhydrated hole for user-written zero bytes.
Ordinary local byte writes have no recovery-metadata I/O.

Opening a file no longer forces a metadata commit: namespace events share the
five-millisecond transaction window, while explicit flush still commits them
before the durable content event. Initial scratch records use one create/write;
later changes remain atomic replacements. A partial initial record or a new
entity lost inside the unflushed namespace window is preserved under
`scratch/quarantine/` without preventing startup.

Tests cover shared handles, remote-version isolation, partial writes, append,
truncate and zero extension. Native adapters are wired at runtime cutover,
after the event journal and sealer are available.
