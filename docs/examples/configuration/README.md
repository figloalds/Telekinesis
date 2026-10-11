# Sanitized configuration examples

These eight TOML files explain the current parser schemas. Invented UUIDs are
public; no certificates, keys, verifier databases or identity envelopes are
included. **Do not replace an existing configuration with a sample.** A managed
pairing sample cannot run without its matching app-created setup/identity/registry.
The receiver variants map to their platform-specific O1 files; actual managed
setup uses `server.toml` and absolute paths. See [configuration anatomy](../../CONFIGURATION.md).

`validate.rs` calls only `orchestrator::Config::load` and
`pairing_service::Config::load`. It checks native-platform acceptance, opposite
control-transport rejection, path resolution, omitted pairing defaults and the
absence of created files/catalogs. It never calls `identity`, `registry`, setup,
supervisor startup or networking. A config can parse on an OS whose selected
credential backend is unavailable: backend availability is checked at use time.

To run it, use a **new temporary directory** outside this example tree:

1. Copy these TOML files with their directory structure into `<scratch>/inputs`.
2. Create a temporary Cargo package containing `validate.rs` as `src/main.rs`:

   ```toml
   [package]
   name = "tkfs-config-doc-validation"
   version = "0.0.0"
   edition = "2024"

   [dependencies]
   tkfs = { path = "/absolute/path/to/Telekinesis" }
   ```

3. Copy the repository `Cargo.lock` into that temporary package so dependency
   versions remain pinned. Run `cargo run --offline --manifest-path
   <scratch>/Cargo.toml -- <scratch>/inputs`. Cargo adds the temporary package to
   its own copied lockfile; it does not edit the repository lockfile. Use a separate
   output directory if a running app holds the repository executable open.
4. Remove only the scratch directory you created after the process exits.

Run on Windows and Linux to validate both O1 transports. No installed credential
provider or privilege elevation is required for these parser checks. Building
the linked library still uses the repository's normal platform build prerequisites.
On Windows this separate helper also needs the installed `winfsp-x64.dll` on its
process DLL search path: the application's delay-load linker setting does not
propagate to a dependent temporary package. Add the existing WinFsp `bin` directory
to **that shell's temporary `PATH` only**, or run in a build shell where it is
already available. Do not install a provider or change the machine/user PATH just
to validate these examples.
Do not substitute `pairing status`, `credential-init`, `orchestrator` or
`reset-pairing-code` for a validation command: those can open or change managed state.
