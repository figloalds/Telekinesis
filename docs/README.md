# Documentation

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
