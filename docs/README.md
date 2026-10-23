# Documentation

- [Personal-use deployment and DVFS authority scope](revamp/AUTHORITY_SCOPE.md):
  deployment-owned protection, required filesystem/replication authority checks,
  and exclusion of Users, ACLs and public-service security from current plans.

- [README](../README.md): what Telekinesis is, quick start, **upgrading from
  the previous release**, what every status means, error classes, runbooks.
- [CLI](CLI.md): every command, including `manage status` and `manage watch`.
- [Configuration](CONFIGURATION.md): the TOML file, environment variables,
  files and logs, pairing codes, per-peer credentials and local tokens,
  remote access through a TLS proxy, backups.
- [Architecture](ARCHITECTURE.md): processes and failure domains, the worker
  lifecycle, mounts and the initial-sync gate, activation, the v3 protocol
  (sessions, metadata feed, objects), status and errors, invariants.
- [Linux](LINUX.md): building, FUSE mounts, signals, systemd and containers.
- [Validation](VALIDATION.md): the test suite, qualification floors and the
  latest measurements, the desktop checklist, the soak.
- `revamp/`: the production revamp's plan (`REVAMP_PLAN.md`), the root-cause
  analysis of the previous release (`ROOT_CAUSES.md`) and each work package's
  notes.
- [Wave 6 implementation plan](revamp/WAVE_6_PLAN.md):
  64 KiB content, independent engines, Git-ready seams, durability and build-performance gates.
- [Next-wave transfer planner direction](revamp/NEXT_WAVE_TRANSPORT.md):
  file-aware and peer-aware strategy experiments; initial strategies and gates remain open.
- [Wave 6 investigation: metadata/object engines](revamp/notes/wp6-build-sync-juicefs-study.md):
  mounted-build stall evidence, JuiceFS source study, per-reference admission,
  demand scheduling, error handling and the concurrency/Tokio decision study.
- [TigerBeetle metadata supplement](revamp/notes/wp6-tigerbeetle-metadata-study.md):
  offline authority, bounded metadata pipelines, batching, repair and simulation.
- [Wave 5 closure and continuation handoff](revamp/notes/wave-5-handoff.md):
  integrated fixes, retained evidence, current limitations and pending decisions.
- [Chunked content and future Git workspaces](revamp/notes/chunked-cow-and-git-workspaces.md):
  proposed 64 KiB foundation, COW and retention contracts, and deferred hybrid Git views.
