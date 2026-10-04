# Paired published-data service (first bounded slice)

`tkfs pairing -f pairing.toml ...` manages a separate portable data service.
The default is `./pairing.toml` in the current directory; there is no parent or
global config search. Relative paths in the file resolve beside that file.
Windows O1/GUI catalog management remains separate and Windows-only. This service
does not adopt/create states, mount filesystems, start workers or upgrade daemons.
Run existing workers under the same OS account first, then grant their runtimes.
Windows workers now expose owner-authenticated named pipes rather than localhost
TCP. Old running workers must be explicitly restarted with the new binary before
new CLI/desktop/pairing clients can use them; this task did not upgrade live workers.
Their authenticated pipe/socket discovery address can change after a restart;
the service rereads the configured runtime file each sync attempt.

## Identities and authorization

One local OS owner/service account owns each installation. This is not a
multi-tenant account/role system. Each installation has its own certificate and
private key; OS SIDs/UIDs, repo IDs and replica UUIDs are not network credentials.
TLS 1.3 uses standard Rustls/ring certificate verification, with the exact approved
leaf certificate checked again. Session resumption and 0-RTT are disabled.
No legacy PSK or unauthenticated fallback is negotiated by this service.

Paired devices initially have **no repository grants**. Local owners explicitly
grant a repository and pin its local and remote replica IDs. Each grant permits
listing that published state and two-way sync of its shared branches; private
branch names/history and private-only CAS objects are excluded. Publication is
still an explicit local `publish --current-state-only` operation. No generic
management dispatcher or runtime paths/tokens are sent over the network. Remote
create/adopt/start/stop/checkout/restore/publish/invite/approve/revoke/shell/object
fetch requests are rejected. Network membership, LAN addresses and Tailscale
ACLs do not grant application access.

The existing causal engine supports two replicas per repository in this slice.
A second peer grant for the same repo is refused. A peer may echo identical
already known local events, but cannot invent events under another replica's
origin. This does not provide independently signed event provenance for a future
multi-hop mesh; that extension needs a separate protocol design.

## Configuration and credentials

Generate a fresh **public** installation UUID with `tkfs pairing new-id`. Replace
the example ID below on every installation. Never copy a live state to create a
new replica: initialize another state with the same repo UUID, which generates a
fresh replica UUID (`tkfs init --state ... --repo REPO_UUID`).

Windows example:

```toml
format_version = 1
installation = "11111111-1111-4111-8111-111111111111" # replace with new-id
data_directory = "pairing-data"
listen = "127.0.0.1:43180"
enrollment_listen = "127.0.0.1:43181"
advertise = "127.0.0.1:43180"
enrollment_advertise = "127.0.0.1:43181"
[credential]
backend = "windows-dpapi"
file = "pairing-data/identity.dpapi"
```

```powershell
tkfs pairing credential-init
tkfs pairing status
tkfs pairing run
```

DPAPI uses CurrentUser protection with UI disabled and an owner-only directory,
never machine-wide protection. Losing that user's DPAPI keys or restoring on a
different machine can make the credential unavailable. This does not defend
against malware running as the same OS owner or an administrator/root.
Credential generation is explicit and refuses to overwrite an existing key.

For Linux, use a dedicated private persistent directory outside login-unlocked
home storage and replace the credential section:

```toml
data_directory = "/var/lib/tkfs-pairing" # must belong to service account, mode 0700
# Remaining public fields as above, with a fresh installation UUID.
[credential]
backend = "systemd"
name = "tkfs.identity"
```

The app reads that named credential only from systemd's `CREDENTIALS_DIRECTORY`.
It rejects traversal, final-file symlinks, nonregular/oversized files, wrong
ownership, and group/world-readable credentials. It does not fall back to a
plaintext file or a login-dependent desktop keyring. `status` reports
`CREDENTIAL_LOCKED`, `CREDENTIAL_MISSING`, `CREDENTIAL_INVALID` or access refusal.
The service refuses to start when its credential cannot be loaded. It retains
the loaded key in memory for its lifetime; changing its own credential requires
a service restart. Peer revocation is independent and checked for every operation.

Operator provisioning is a separate explicit action. With a suitably configured
systemd host, `credential-provision --encrypted-output ...` pipes a freshly
generated key directly to `systemd-creds encrypt`; no plaintext intermediate file
is created. Encryption policy/host key/TPM provisioning belongs to the operator.
The output name must match `LoadCredentialEncrypted` and must not already exist.
The command reports only the public installation ID/fingerprint.

```bash
systemd --version
systemd-creds --version
# Operator example; review host key/TPM policy and destination before using sudo.
sudo /usr/local/bin/tkfs pairing -f /etc/tkfs/pairing.toml credential-provision \
  --encrypted-output /etc/credstore.encrypted/tkfs.identity
```

Systemd 250+ is required for `LoadCredentialEncrypted`; Ubuntu 24.04 systemd 255
was observed. The supplied unit is a template only. No account, credential,
system service, linger setting, network rule or reboot was provisioned during
implementation. Service accounts cannot use a desktop login keyring as their
sole unattended credential backend. To perform key-dependent local commands
(`invite`, `join`, `remote-list`, `sync`) on Linux, run them under the same service
account in an operator-created systemd unit/transient unit with the same
`LoadCredentialEncrypted`; ordinary interactive shells will report locked.
`status`, `approve`, `revoke` and `grant` use the owner-protected local registry.

## Pair, approve and grant

Set explicit reachable `listen` and `advertise` endpoints before running services
on two computers. Endpoints are numeric IPv4/IPv6 socket addresses; this bounded
slice does not perform DNS/MagicDNS resolution. The examples bind loopback for local testing. An operator can
select a Tailscale address and ACLs later; this task does not configure it.

1. Owner A runs `tkfs pairing invite --lifetime-seconds 300`. The one-line output
   is a secret bearer invitation containing A's public certificate. Transfer it
   privately and never save it in logs, argv, shell history or source control.
2. Owner B runs `tkfs pairing join` and pastes the invitation on masked stdin.
   Pipelines are supported; an invitation is never accepted as an argv option.
   B pins A's exact certificate from the invitation and submits B's public key.
3. A runs `tkfs pairing status`, verifies B's public installation/fingerprint
   and advertised numeric endpoint
   through the intended transfer channel, then runs
   `tkfs pairing approve INVITATION_ID --installation B_ID --fingerprint B_HASH`.
   Until approval, B cannot use A's data service. Approval grants no repositories.
4. Both owners explicitly configure their own side:

```text
tkfs pairing grant PEER_INSTALLATION_ID --repo REPO_UUID \
  --runtime /owned/state/runtime.json --remote-replica PEER_REPLICA_UUID
tkfs pairing remote-list PEER_INSTALLATION_ID
tkfs pairing sync PEER_INSTALLATION_ID --repo REPO_UUID
```

`remote-list` exposes only states locally granted to the authenticated peer that
have shared branches. The replica UUID shown there is authenticated as data from
that installation; owners select and pin the intended UUID. A copied/changed
worker identity fails the local binding check. `grant ... --disable` withdraws
that repo grant. It does not delete user data or stop/mount/modify a worker.

The service loads persisted grants after every restart and resumes enabled syncs
with bounded exponential retry (1..60 seconds). Workers must be independently
started/restarted by O1 or explicitly configured system units. Network-online
ordering is not ongoing connectivity: temporary network/peer/worker failures
remain unavailable and retry. Transfer pages are bounded and large objects use
the core's resumable sequential parts. An event enters ACK/known inventory only
after verified object bytes and durable causal activation; pending manifests
are not ACKs. Repeated transfers are idempotent, and responses bind to the current
request. A lost enrollment response consumes the invitation: issue a fresh
invitation and approve the intended exact key rather than replaying it.

## Revocation, rotation and limits

Network operations use absolute budgets: five seconds for TLS, then ten seconds
for an enrollment frame/response or 45 seconds for a sync frame/response. Byte
progress does not reset these deadlines. Enrollment has two connection slots,
separate from eight paired-service slots. Network shutdown cancellation is checked
at most every 100 milliseconds; draining service jobs has a 35-second ceiling and
reports `PAIRING_SHUTDOWN_DEADLINE` if exceeded. Local worker calls retain their
own bounded RPC timeout. These bounds do not guarantee storage/OS scheduling
latency or availability under a sustained network flood.

Pairing storage fails closed on unsafe existing paths; startup never repairs an
existing owner or ACL. Windows checks actual handle owners and DACLs before
opening the registry/credentials, including existing SQLite WAL/SHM/journal,
lock and other files. Private files/directories must belong only to the current
SID. Reparse points and multiply linked files are rejected. Retained ancestor,
directory and primary-file handles prevent namespace replacement; ancestors
must be owned by the current SID, SYSTEM, Administrators or TrustedInstaller,
with no foreign write/delete/security-control grants. Administrator/OS compromise
is outside this single-owner boundary. Linux requires current-UID private
directories/files (0700/0600), rejects symlinks/hardlinks and unsafe ancestors,
and permits root-owned sticky temporary directories.

An existing ancestor with a foreign write grant also fails validation even if
the final directory is private. For example, this development machine's
`AppData` contains a sandbox capability SID with full control. Use a separately
reviewed safe storage path; do not bypass the check or automatically change ACLs.
Windows regression fixtures used a disposable sibling path with safe ancestors.

`tkfs pairing revoke PEER_ID` persists revocation and stops that peer's data access
on fresh and already-established sessions. Each request reloads registry state
after TLS; registry transactions serialize data operations with local revocation
and grant changes. An operation already authorized/in progress may finish before
revoke commits; after revoke returns, later operations are denied. Revocation
cannot erase data the peer previously received. Re-granting or reusing an invite
does not clear revocation.

Initial rotation/recovery is explicit re-enrollment: revoke the old installation,
create a new installation UUID/key/config and a new pairing data directory,
enroll and approve it, then configure fresh explicit grants. For credential-only
rotation, retain the existing owned worker state/replica UUID and pin it again.
Replacing the actual data replica with another UUID when shared history already
contains both old authors is not qualified by this two-replica protocol; do not
claim transparent hardware/state migration or multi-origin history relay.
There is no implicit key replacement or inherited
privilege. Lost/locked credentials keep networking unavailable until this recovery
is performed; there is no downgrade to LAN trust or a global shared password.

Qualification includes disposable Windows/Linux loopback TLS peers and actual
owner pipe/Unix worker bridges, durable grant reload and process/service restart
paths. No-login boot, installed system units, public networks, cross-host
Tailscale/VPS connectivity, certificate expiry renewal and arbitrary multi-peer
topologies remain unqualified. Do not claim a Linux O1 supervisor or a deployed
production service from this slice.

References: [DPAPI](https://learn.microsoft.com/en-us/windows/win32/api/dpapi/nf-dpapi-cryptprotectdata),
[TLS 1.3](https://www.rfc-editor.org/rfc/rfc8446),
[systemd credentials](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html#Credentials),
[Rustls](https://docs.rs/rustls/0.23.45/rustls/).
