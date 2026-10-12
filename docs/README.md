# Documentation

- [Architecture](ARCHITECTURE.md): mount workers, daemon ownership and WS replication.
- [Configuration](CONFIGURATION.md): version 2 TOML, ordinary storage and proxy setup.
- [CLI](CLI.md): folder lifecycle, pairing and filesystem operations.
- [Linux](LINUX.md): building and running headless or FUSE workers.
- [Validation](VALIDATION.md): reproducible checks and remaining limitations.

The reboot retired O1 catalogs, protected credential storage, application-managed
certificates, the separate pairing service and desktop enrollment. Their designs
are available through Git history rather than active guides. Current outbound
WSS uses platform TLS verification and the same code-only pairing as WS; it does
not restore those mechanisms.
