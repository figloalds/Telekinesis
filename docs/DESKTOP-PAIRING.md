# Desktop WSS client pairing

Open **Connect a server** in the Windows desktop application. This screen uses
the existing published-data protocol; it never exposes remote management.

1. Create a protected client or load an existing **outbound-only WSS** client
   config. The default is `.tkfs-pairing/client.toml` beside the application.
   Choose a writable folder with trusted ancestry if the storage validator
   refuses that location. Existing ownership and permissions are never repaired.
2. Enter the server's numeric `wss://address:port/tkfs/sync` endpoint and paste
   its invitation privately. Inspect it, verify the exact certificate SHA-256
   with the server owner, and explicitly confirm enrollment. The invitation
   pins both endpoints and the server key; the endpoint cannot be silently
   replaced. The secret input is cleared when inspected and is never saved.
3. Give the owner the client's installation ID and certificate fingerprint.
   Approval is an owner-local action on the server. **Check approval** succeeds
   only after authentication; an empty authorized list means there are no
   repository grants. Locally trusting the server key is not server approval.
4. Select an authorized published repository and the native `runtime.json` of
   an already initialized, running local replica. Repository IDs must match;
   replica IDs must differ. Confirm bidirectional published-data sync, then use
   **Sync now / next page** until the worker reports it is caught up.

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

Backend limits are explicit: the screen does not create/adopt replicas, create
mounts, start the pairing service or install background sync. O1 cannot import
an existing repository ID or supervise WSS. Use the supported standalone
initialization/daemon workflow for that replica and mount. A grant synchronizes
**all published branches**, with one active peer/two replica origins per
repository; it does not filter a chosen branch. Sync preserves the mounted
branch. Choose a local view through Branches & snapshots for managed projects,
or the CLI for standalone workers. Private branches/history remain local.

Disposable acceptance: `cargo test --offline --test desktop_pairing`. It uses
real WSS, DPAPI fixture keys, a WinFsp fixture mount and real GUI callbacks in
light and dark themes. The temporary root must itself have trusted ancestry;
the tests never fix an unsafe ACL. Set `TKFS_DESKTOP_PAIRING_EVIDENCE` to a local
output directory to retain only reports, screenshots and secret-free GUI logs.
