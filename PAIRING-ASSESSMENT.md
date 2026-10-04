# Multi-computer identity and pairing assessment

Assessed 2026-10-04 after merging Linux commits de96171/fed668d into Windows
`master`. This document proposes the next implementation; it does not enable
networking, generate/store credentials or change running daemons.

## Current implementation and gaps

- `src/orchestrator.rs:23-89`: O1 configuration accepts only local named-pipe
  management and explicitly rejects `network.enabled=true`.
- `src/orchestrator.rs:393-453`: installation UUID/catalog is bound to the current
  Windows SID. The owner-only local pipe verifies both endpoint SIDs. This is
  useful local ownership, not an authenticated identity on another computer.
- `src/orchestrator.rs:745` strips `TKFS_PEER_KEY` from managed workers. Those
  workers call local `start_rpc` and do not configure peer replication. The
  private worker instance/token passed through stdin is a lifecycle credential,
  distinct from a persistent machine/network credential.
- `src/main.rs:363` and `src/runtime.rs:853-969`: standalone daemon replication
  requires manually supplied listen/address/peer UUID and a 32-byte hex secret in
  `TKFS_PEER_KEY`. The secret is held in memory; no network key storage/import,
  persistent pair catalog, enrollment, revocation or rotation exists.
- Peer frames use AES-256-GCM with random nonces and authenticated sender/receiver
  fields. A shared-secret holder can construct either side's identity claims;
  the UUID is not a public-key identity. There is no ephemeral authenticated
  handshake/forward secrecy or independent event signature.
- Responses bind `reply_to` to a fresh request, and semantic events/management
  operations have idempotency and payload checks. Incoming encrypted requests do
  not have a session sequence/replay window: encryption nonces are not a replay
  database. A recorded valid request can be resent; event idempotency limits some
  duplicate effects but is not general network replay protection.
- `src/core.rs:368` preserves the store's device UUID. Copying a state directory
  onto another computer copies this origin identity too; create a fresh replica
  through an explicit join/adoption operation instead. Keep old historical event
  origins intact rather than rewriting them under the new device ID.
- `src/core.rs:1899-1911` checks repository equality, shared-branch status and an
  allowed event-origin set. Runtime currently supplies `{local device, peer}`.
  `shared_page` exports all shared branches in that store. There is no durable
  grant tying an authenticated installation to allowed replicas/repositories,
  selected branches or read/write roles. An allowed origin string is not proof
  of authorship; a trusted writer can currently claim another allowed origin.
- Linux now has same-UID Unix socket control, owner-only state/socket modes and
  FUSE. The O1 supervisor itself remains Windows-only. Portable supervisor
  catalog/lifecycle/local management is a prerequisite for Linux orchestrator
  pairing, even though the credential and network modules should be portable.

## Separate identities and authorization

| Item | Meaning |
| --- | --- |
| Local SID/UID | OS account that can manage this installation and open local state. Matching names/UID numbers across computers do not prove the same person. |
| Installation/orchestrator UUID | Stable catalog identity; never derive trust from hostname, IP, mount path or friendly label. |
| Authentication public key | Proof that the remote endpoint controls a private credential. Persist an explicit binding to installation UUID and credential generation. |
| Replica/device UUID | Existing per-store causal event origin. One installation can represent several authorized replicas. Preserve it on normal restart; use a fresh ID for a new replica. |
| State UUID | Local registry resource/location. Different computers need distinct state IDs even for the same repository. |
| Repository/branch IDs | Logical data scope. Pairing alone grants no repository data. Private branches remain local and cannot be granted/exported. |
| Pair and share grant | Durable trust relationship and explicit repository/replica/branch/role permissions, with generation and revocation state. |

Start with one local OS owner per installation and two explicitly approved
computers belonging to that owner's trust domain. Multiple human owners and
portable user accounts require explicit roles/invitations; the existing local
SID/UID and POSIX file-owner projection do not implement that model. If later
administration must span several devices, define an owner/admin authority and
which credential may issue grants and revocations before allowing transitive
trust or automatic introductions.

## Recommended user flow

1. First identity setup creates a local private credential once, protects it in
   the platform store, and records its public fingerprint/installation binding.
   Normal startup unlocks it according to the selected backend. Do not copy a
   universal permanent secret to every computer.
2. On A, **Generate connection key** creates a short-lived, single-use invitation
   (for example a 10-minute lifetime). Internally it contains a random 256-bit
   enrollment secret, invitation ID, issuer public-key pin, endpoint hint and
   bounded purpose/scope. Store only its verifier plus expiry/state; show/export
   the invitation once and never log it. No illustrative live key is included
   here. An endpoint hint is routing information, not a trust decision.
3. On B, **Connect computer** accepts the invitation through a protected input or
   stdin, verifies A's pinned TLS identity, and submits B's public identity with
   proof of private-key possession. Unknown clients can use only the enrollment
   endpoint; they cannot issue management calls or replicate data.
4. A displays the pending computer and fingerprint and requires local owner
   approval. This protects against an invitation stolen before first use. A
   matching trusted out-of-band fingerprint is needed when transfer authenticity
   is uncertain. Labels alone do not authenticate a computer.
5. Commit the pair, bind the invitation to that joining public key, consume it
   atomically, and retain a retry receipt. Network failure/retry must not create
   a second pair or allow a different key to reuse the invitation. Pending/expired
   enrollment remains unable to receive repository contents.
6. The owner explicitly selects which repository to link and whether to create
   a fresh replica on B. Record matching logical repo ID but new local state and
   replica IDs; require deliberate validation for existing-state adoption. Do not
   create or overwrite local data merely because a token was pasted.
7. Persist the approved pair/grant and credential references. On restart,
   orchestrators reconnect without asking users to re-enter pairing secrets.
   Display trusted/offline/locked/revoked states and redact credentials throughout
   CLI, UI, logs, operation receipts, diagnostics and crash output.

This keeps the requested generate/paste experience while making the displayed
connection key an enrollment capability, not an unrestricted long-lived data key.

## Transport and storage

Use a maintained TLS 1.3 implementation with ephemeral key exchange and mutual
public-key authentication for normal paired sessions. Bind the verified key to
the stored installation and generation before accepting a claimed UUID. Pin
explicitly enrolled peers and validate handshake proof; do not disable certificate
verification globally. Keep enrollment on its limited, rate-limited endpoint.
Disable 0-RTT for management and replication mutations; it has weaker replay
properties ([TLS 1.3 specification](https://www.rfc-editor.org/rfc/rfc8446#section-2.3)).
Retain application operation IDs/receipts and event idempotency for legitimate
retries. Exact library/version/MSRV selection must be verified against existing
Rust 1.88 Linux and Windows targets before installing/downloading dependencies.

Windows: use CurrentUser DPAPI plus owner-only DACLs, storing only opaque encrypted
blobs/credential references in the catalog. Do not use machine-wide DPAPI for
single-user credentials: its scope permits other users on the same computer to
decrypt. Avoid the deprecated UI-prompt flow; the application controls input and
unlock UI ([Microsoft CryptProtectData](https://learn.microsoft.com/en-us/windows/win32/api/dpapi/nf-dpapi-cryptprotectdata)).
Credentials are normally bound to the account/computer, so another machine joins
with its own key rather than copying the blob. Local malware running as the owner
or privileged compromise remains outside this protection boundary.

Linux desktop: a Secret Service-compatible keyring can store credentials, but
availability, lock/unlock and session access must be detected rather than assumed
([Secret Service specification](https://specifications.freedesktop.org/secret-service/latest/)).
Linux headless/WSL: offer an explicit encrypted credential file unlocked by a
user-provided passphrase, stored with 0700 directory/0600 file, using reviewed KDF
and authenticated encryption. This requires unlock after restart unless an
operator explicitly chooses a service credential/TPM-backed provisioning route.
For unattended use a permission-protected plaintext file may be offered only as
an explicit weaker backend, with that risk stated; storing an encryption key beside
its ciphertext does not add meaningful protection. Never silently fall back to it.
Service identity and credential provisioning are separate authorized work; do not
assume an interactive user's keyring/DPAPI credential is accessible to a service.

Tailscale is a possible routing and network-access layer; a LAN can provide routes
as well. Do not infer application trust from either. A node/IP allowlist can reduce
exposure, but it does not bind TKFS installation UUIDs, distinguish local user
processes, preserve event-origin identity, choose shared repositories/branches,
or authorize administrative operations. Retain application enrollment and grants
with either transport. A Tailscale-only first network test is useful if already
configured by the owner; direct LAN and routed/VPS paths still require encryption
and authentication. No Tailscale installation/configuration was performed here.

## Rotation, revocation and recovery

- Revoke a lost computer's authenticated key/installation and its grants, cancel
  pending invitations and close its sessions. Enforce persisted revocation and
  generation on both new connections and active authorization checks. In a mesh,
  revocation must reach every granting participant; a disconnected participant
  cannot be promised immediate knowledge of a new revocation. Already downloaded
  data cannot be recalled.
- Routine rotation can use a bounded old-to-new transition approved by the old
  trusted key plus local policy. For a compromised old key, require owner approval
  from an independent trusted endpoint; an attacker with that key can authorize
  their own replacement. Expire overlap and invalidate session/resumption state.
- A lost private key should normally cause re-enrollment, not bypass trust from
  UUID alone. Backups/recovery of credentials must be deliberate and protected;
  restoring/copying data must not accidentally run duplicate replica identities.
- A permanent shared-secret MVP is simpler but symmetric: either holder can
  impersonate the other, historical captures remain exposed on key compromise,
  and compromise of a global key affects every holder. If used temporarily,
  choose an independent secret per pair, explicit repository grants and a standard
  authenticated handshake; protect storage, replay and rotation just as above.
  Prefer independent long-lived public credentials for durable multi-computer use.
- Mutual TLS authenticates the live connection; it does not independently sign
  historical event authors. The next two-peer direct flow must reject newly forged
  local-origin events and enforce installation-to-replica grants. Signed events
  and a defined authorization/history model are required before claiming verifiable
  authorship across relays, arbitrary meshes or compromised writers. Preserve
  legacy history as legacy rather than assigning invented signatures/provenance.

## Bounded next implementation

Implement a platform-neutral credential-store interface and registry tables for
identity bindings, invitations, pairs, grants and revocation generations. Add
versioned same-owner management actions: identity-init/status, invite-create,
join-from-protected-input, pending-approve, pair-list/revoke and repo-link. Keep
configuration as defaults and secret references; never grow a plaintext key TOML.

Then add the maintained authenticated transport and two-peer reconnect path for
one explicitly selected repository per grant, with both replicas writable and all
private branches excluded. Extend inventory/object/event paging to enforce those
grants before export and receive; do not claim read-only or per-branch grants until
they are actually enforced throughout mutation, inventory, acknowledgments and
object selection. Add Linux O1 catalog/local-management portability as a separate
prerequisite for Linux orchestrators, reusing the credential/pairing modules.

Acceptance must cover wrong OS owner, redaction, stolen/expired/reused invitations,
approval binding, restart/locked credential behavior, duplicate joins, MITM/wrong
key/UUID, replay, wrong repository/origin/grant, private-object exclusion, revoked
active sessions, key loss/rotation and protocol mismatch. Start on disposable
two-machine states with no public exposure; then qualify Windows/Linux and routed
network behavior. Credential persistence and auth protocol upgrades require their
own explicit migration/version design; today's format 5 upgrade is only the Linux
metadata integration and must not silently negotiate down to an unauthenticated
fallback. No authentication implementation was made by this assessment.
