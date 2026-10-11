# Desktop WSS client pairing

Open **Connect a server** in the Windows desktop application. This screen uses
the existing published-data protocol; it never exposes remote management.

1. Create a protected client or load an existing **outbound-only WSS** client
   config. The default is `.tkfs-pairing/client.toml` beside the application.
   Choose a writable folder with trusted ancestry if the storage validator
   refuses that location. Existing ownership and permissions are never repaired.
2. Load the server's public bootstrap JSON and enter its enrollment endpoint.
   Verify the certificate SHA-256 through the server owner, explicitly confirm
   the fingerprint, and enter the 200-digit code privately. Changed metadata,
   endpoint or config invalidates confirmation. Legacy invitation enrollment
   remains available. Secret inputs are ephemeral and excluded from screenshots,
   logs and retry receipts.
3. Give the owner the client's installation ID and certificate fingerprint.
   Approval is an owner-local action on the server. **Check approval** succeeds
   only after authentication; an empty authorized list means there are no
   repository grants. Locally trusting the server key is not server approval.
4. Use the receive wizard to prepare a fresh headless replica for the source
   repository UUID. Give its new device UUID to the source owner, who must issue
   the existing published-data grant for that replica. Pairing alone never
   issues this grant. Discover the authorized repository and configure local
   bidirectional sync; the wizard supplies the managed runtime path.
5. Sync until caught up, select a shared branch, verify/materialize it, stop the
   headless worker, attach an unused local mount and start it. Attachment checks
   the selected branch and generation under the state ownership lock. Changed
   grants, worker identity, pending data or branch selection require fresh
   verification. A stopped, unmounted preparation can resume after restart.

Windows keys use CurrentUser DPAPI. Config and the creation recovery record
contain only public identity and credential references. Interrupted creation
after a durable key can resume with the same key. Existing, damaged or unsafe
credentials are never replaced automatically. No plaintext credential fallback,
invitation journal, secret CLI argument or shell command is used.
Client config publication flushes a temporary file before a create-only atomic
rename; interrupted writes do not leave a partial final config or replace a key.

Configured grants survive reopening. Their own **Pause**, **Resume** and **Sync**
controls work without reloading an online repository list. Local revocation
persists and cannot be undone by Resume; the server owner must revoke their side
separately. Cancel interrupts local network I/O, not committed data. A consumed
invitation is not replayed automatically after uncertain delivery: check approval
if enrollment succeeded, otherwise request a fresh invitation. Sync can resume
using its durable inventories and acknowledgements.

Enable supervised sync for the selected preprovisioned outbound-only WSS config
to continue after closing the GUI. Its immutable public config/credential binding
is persisted in the supervisor catalog. Restart reuses existing credentials;
drift is reported as unavailable rather than accepted silently. Service running
does not mean every grant is caught up. Disable waits for actual service drain;
an uncertain drain retains ownership and requires explicit process recovery.
This manages all configured enabled grants and does not resume paused grants.

Existing standalone stores are not adopted; automatic remote grant issuance,
general remount/delete and service installation remain outside this workflow.
A grant synchronizes
**all published branches**, with one active peer/two replica origins per
repository; it does not filter a chosen branch. Sync preserves the mounted
branch. Choose a local view through Branches & snapshots for managed projects,
or the CLI for standalone workers. Private branches/history remain local.

Disposable acceptance: `cargo test --offline --test desktop_pairing`. It uses
real WSS, DPAPI fixture keys, a WinFsp fixture mount and real GUI callbacks in
light and dark themes. The temporary root must itself have trusted ancestry;
the tests never fix an unsafe ACL. Set `TKFS_DESKTOP_PAIRING_EVIDENCE` to a local
output directory to retain only reports, screenshots and secret-free GUI logs.
