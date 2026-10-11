# Desktop WSS client pairing

Open **Connect remote** in the Windows desktop application. This screen uses
the existing published-data protocol; it never exposes remote management.

1. On the server, open **Allow connections**, enter this computer's numeric IP
   reachable by the client and the chosen port (the GUI starts with 7421), then select
   **Set up / enable receiving**. Wait for **Running**. The
   application creates and retains its protected identity and config, then shows
   the actual listener state. No firewall or installed service is changed.
   Select the reset acknowledgement and **Reset pairing code**, then transfer its
   200 digits privately. Use this same IP and port on the client; loopback addresses
   only connect processes on the same computer.
2. On the client, enter only **IP**, **Port** and **Pairing key**, then Connect.
   The application creates its protected client identity under the selected
   orchestrator data directory and authenticates the server through OPAQUE before
   trusting its certificate. No public JSON file or manual fingerprint step is
   needed. Secret inputs are ephemeral and excluded from screenshots, logs and
   retry receipts. Existing advanced pinned/legacy enrollment remains separate.
3. Review and approve the pending device locally on the server.
   **Check approval** succeeds
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

Windows keys use CurrentUser DPAPI. Config contains only public identity and
credential references; the durable setup record contains an encrypted copy of
the exact identity for crash recovery. Interrupted creation
after a durable key resumes with the same key. Existing, damaged or unsafe
credentials are never replaced automatically. No plaintext credential fallback,
invitation journal, secret CLI argument or shell command is used.
Client config publication flushes a temporary file before a create-only atomic
rename; interrupted writes do not leave a partial final config or replace a key.
The normal client lives at `<O1 data>/desktop-pairing/client.toml`; the receiver
uses `<O1 data>/receiving-pairing/server.toml`. Existing selected or
executable-adjacent orchestrator configs are never relocated. A fresh private
profile location is an explicit first-run choice. Unsafe ancestors are reported,
not repaired. Linux's automatic encrypted provider requires an already-root
process, systemd 256+ and an existing valid protected host key; it never elevates
or initializes that key. Other hosts report unavailable. Existing advanced
systemd credential configurations remain supported.

An old salted-hash enrollment key needs an explicit reset to enable the new
three-field protocol. This generates a new key; existing pairs and sync grants
survive. The normal flow never silently falls back to bearer-code enrollment.
The separately named CLI `manage reset-legacy-pairing-code` is available for
advanced pinned-v2 compatibility, including Linux hosts without the new encrypted
provider. It deliberately disables simplified v3 enrollment until a normal reset.
The receiving IP/port is fixed at initial setup in this slice; the application
supports disable/re-enable of that endpoint, not editing an existing endpoint.

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
