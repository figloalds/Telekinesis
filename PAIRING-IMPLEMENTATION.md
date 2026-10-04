# Approved bounded pairing implementation contract

2026-10-04 scope: paired installations may list and sync **published** states and
their published/shared branches. Pairing grants no remote catalog administration,
state creation/adoption, branch creation/checkout/restore, mount control, worker
start/stop, supervisor shutdown, shell access or local management API dispatch.
Private branch metadata and private-only CAS objects stay local.

The next code uses distinct persistent device credentials, TLS 1.3 with mutual
authentication for approved peers, single-use expiring enrollment invitations,
local owner approval and durable sync configuration/retry state. UUIDs remain
resource/origin IDs, not authentication credentials. TLS identity is explicitly
bound to installation/replica ownership. Revocation is checked during resumed and
active synchronization; no legacy/shared-secret or unauthenticated fallback.

Dependency plan: Rustls 0.23.45 with default features disabled and `ring,std`,
rcgen 0.14.10 with `ring,crypto` for device certificate generation, and zeroize
for in-memory secret buffers. Existing SHA-256, rand, rusqlite and serialization
support are reused. Dependencies come from official crates.io into a task-isolated
Cargo cache, with Rust 1.88 compatibility verified before implementation proceeds.
The ring provider avoids introducing AWS-LC/CMake or a toolchain upgrade. No OS
package or service installation is part of this work.

Windows secrets use CurrentUser DPAPI with owner-only storage. Linux unattended
credentials are explicitly provisioned through systemd's credential mechanism;
the app reads only its designated credential under CREDENTIALS_DIRECTORY and
never silently creates a plaintext secret file. Missing/locked/invalid credentials
produce explicit unavailable status. System-unit instructions/templates may use
an unprivileged service account and LoadCredentialEncrypted, but this task does
not install/enable services, modify keyrings, configure actual credentials or
reboot. No-login/reboot acceptance remains untested. Login-dependent Secret
Service alone is not the headless backend. Ubuntu systemd 255 and systemd-creds
were observed during read-only prerequisite checks.

Local management remains owner-authenticated named-pipe/SID on Windows or Unix
socket/UID on Linux. Localhost TCP alone is not OS user authentication. Network
enrollment/sync DTOs are separate from local management actions, not an allowlist
in front of a generic admin dispatcher. Tailscale/ACLs are optional routing and
exposure controls; neither a LAN address nor network membership grants data or
administrative rights.

O1 is still Windows-only at this starting point. Portable credential/catalog and
sync components must be reusable by the Linux headless service; a Linux worker or
sync service must not be described as a fully ported O1 supervisor. The runtime
bridge must maintain worker ownership and headless state locks, never auto-adopt
an existing user state from an invitation. Only explicitly configured syncs resume
after restart. Remote published catalog entries do not automatically create local
states or mounts.

Disposable tests must demonstrate accepted pairing and restart resume, plus
rejection of local-management operations, private catalog/object requests,
expired/replayed invitations, revoked keys and wrong installation/repository
bindings. Fixture credentials stay temporary and are never printed. Public
listeners, live daemon upgrades, real persistent credential provisioning,
firewall/Tailscale changes and service enablement are excluded.

Implemented slice: portable `pairing` CLI/service, DPAPI/systemd credential
sources, distinct TLS keys, owner-approved durable enrollment, explicit grants
with installation/replica binding, published-only worker DTOs, bounded background
retry and persistent restart resume. Windows worker control is now the same
owner-authenticated named-pipe transport used by management; Linux keeps UID
checked Unix sockets. TLS tickets, resumption and early data are disabled; fresh
registry authorization occurs after handshakes and before data use. Local grants,
revocation and data operations serialize through SQLite transactions. See
`PAIRING.md` and `PAIRING-VALIDATION.json` for instructions, qualification and the
two-replica/rotation boundaries. No installed user credential or system unit was
created during this work.
