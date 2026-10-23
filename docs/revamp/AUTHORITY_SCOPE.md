# DVFS deployment assumptions and authority scope

Decision: 2026-10-22. Telekinesis is currently a personal-use distributed virtual
filesystem for the owner's devices in an owner-controlled deployment. It is not
a public storage service for untrusted users. Users, accounts, roles, ACLs,
multi-tenant isolation and hostile local users are outside the current scope.
This decision governs current and next-wave planning and supersedes broader
security requirements in earlier plans and studies. It changes planning scope;
it does not claim that existing runtime mechanisms have already been removed.

## Pairing is the admission boundary

The server may be online and reachable by unwanted callers. DVFS must retain
the simple hashed pairing-code check as its application admission challenge:
reachability, a protected channel or an installation identifier alone does not
grant use. A missing or incorrect pairing code must not admit a peer or expose
filesystem/control operations. The environment's channel protection does not
replace this check.

After successful pairing, the peer is trusted. Do not design the filesystem as
a service for malicious authenticated users or add post-pairing user identities,
ACLs or tenant isolation. Continue checking repository/reference scope, routes,
generations, operation ownership and byte integrity for correct execution.
Trust does not make delayed work current or a hash a valid filesystem reference.
Unpairing ends that association; reconnect must not bypass the admission boundary.

## Deployment responsibilities

Use standard TLS/WSS or the enclosing VPN for channel confidentiality and
integrity. The deployment owns host access, OS account isolation, data-directory
access, network exposure and any proxy/VPN admission. DVFS must document which
of these guarantees its selected deployment supplies and rely on them rather
than implement a second version. A plain connection is appropriate only where
the enclosing deployment supplies the required protection; this is not a new
application cryptography requirement.

Do not add custom channel cryptography, additional challenge-response protocols, security
negotiation, certificate infrastructure, credential enrollment/rotation systems,
application ACLs or local-user isolation merely because earlier documents
prescribed them. No public-service abuse prevention, hostile-peer hardening or
security audit is a current release gate. Existing mechanisms may remain until
their callers are consolidated; retaining them temporarily does not make them
the required target design. Removing them must preserve the authority checks
below, without turning cleanup into a separate security workstream.

## DVFS responsibilities

| Boundary | Required behavior and reason |
|---|---|
| Pairing and routing | Enforce the selected simple hashed pairing-code admission challenge before accepting peer use, including on an online server. After pairing, treat the peer as trusted and associate each connection with the intended installation/repository. Refuse mistaken/self pairing and wrong routes. An installation identifier names a device; it is not a user or ACL principal. Environment-provided identity may supply route binding but does not waive pairing-code admission. |
| Sharing and reference scope | Serve or publish content only through the selected repository, branch and admitted reference. A known CAS digest or shared physical chunk does not itself grant access to every reference. Unshared repositories and private/internal roots must not accidentally enter replication. These are DVFS scope checks, not a multi-user permission system. |
| Withdrawal and stale work | Unpairing or withdrawing a reference stops new work under that association. Recheck generation/lease/owner identities at the relevant commit or delivery boundary so delayed work cannot regain authority. No globally instantaneous offline revocation or erasure of already delivered bytes is promised. |
| Filesystem operations | Bind operations to the correct workspace, view, entity, handle incarnation and operation owner. Validate namespace changes atomically; preserve open-handle, rename/replacement and delete-on-close semantics. Existing filesystem mode/readonly semantics do not imply a user/ACL design. |
| Content and persistence | Verify canonical descriptors, hashes, ranges, lengths and manifest edges; preserve causal ordering, exact receipts, durable saves and recovery. Hash verification establishes byte identity and structural integrity, not a new authentication protocol. |
| Ownership and lifetime | Pins, save owners, leases and GC roots prevent premature deletion or reuse. These are storage ownership, not user permissions. |
| Resource use | Bound parsing, decoded sizes, queues, memory, disk work and concurrency; honor cancellation and shutdown. Needed for malformed data, bugs, disconnects and ordinary load without assuming a hostile tenant. |

Use an application token or additional credential only if it enforces a concrete
DVFS control/worker authority boundary that the chosen OS/IPC/deployment does not
already provide. Loopback describes reachability, not proof of caller identity;
the deployment assumption and required owner binding must be explicit. This is
not a mandate to add tokens or remove existing ones indiscriminately. Do not
build a general identity system. Avoid exposing pairing codes in ordinary logs
or status; that needs no new redaction service or security infrastructure.

## Effect on plans and acceptance

- Wave 6 transport consolidation simplifies custom HMAC/enrollment wrapping
  toward the selected pairing model over the protected deployment channel.
  Validate intended-peer and repository routing, unpair/withdraw behavior and
  reconnect; no custom handshake or per-peer credential system is required for
  its own sake. Rebase applicable tests when implementation changes.
- Existing references to authorization or permissions mean the DVFS boundaries
  above unless a document explicitly describes an existing OS/filesystem field.
  They do not authorize adding Users, roles or ACLs. Each retained check should
  identify its DVFS boundary and the incorrect operation it prevents.
- Tests of stale grants, wrong routes, private/shared references, malformed
  content, ownership races and bounded load remain correctness coverage.
  Tests solely specifying retired HMAC/enrollment or hostile-user defenses are
  not new-design acceptance gates; retain their historical results and record
  replacement coverage when the implementation is simplified.
- Existing configuration/architecture sections describe shipped behavior where
  applicable, not a requirement to reproduce it. Historical plans, root-cause
  reports and evidence remain historical; their broader security prescriptions
  are superseded here. No past failure is relabeled as a pass.
- Finish the current directory/lifetime work and current-path consolidation.
  This scope review adds no security milestone ahead of build-performance and
  remaining filesystem qualification. It makes no runtime change and does not
  close an existing correctness failure.
