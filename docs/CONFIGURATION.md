# Configuration files and managed state

Telekinesis uses **TOML, not YAML**. The desktop normally needs one selected
`orchestrator.toml`; it creates its client and receiving configurations when you
explicitly connect or enable receiving. You do not create certificates, exchange
public JSON files, or edit these managed configurations for ordinary pairing.

This guide describes the implemented schemas and storage layout at `b42a76c`.
Examples use invented UUIDs and loopback addresses, contain no credentials, and
are for parser validation and explanation—not ready-to-run identities.

## Which file do I need?

| File | Purpose | Who changes it? |
|---|---|---|
| Selected `orchestrator.toml` | Local supervisor (O1), data root, local control endpoint, worker policy, default mount parent | Created by setup; owner may edit supported settings while stopped |
| `<O1 data>/desktop-pairing/client.toml` | App-managed outbound pairing identity and registry | App; ordinary **Connect remote** creates it once |
| `<O1 data>/receiving-pairing/server.toml` | App-managed receiving identity, listener and mapping to O1's enrollment verifier | App; **Set up / enable receiving** creates it once |
| Operator-selected `pairing.toml` | Advanced standalone pairing service using the same pairing schema | Operator, with separately provisioned credentials |
| SQLite, encrypted identity, setup and desktop receipt files | Durable identity, trust, grants, state and recovery | Managed; not configuration templates or files to delete/reset by hand |

TOML contains public identifiers, paths and credential references, not the private
key or pairing code. Public does **not** mean freely editable: identities, paths
and endpoint settings participate in persisted bindings. Keep configs private
enough to protect those bindings and their operational metadata.

## Find the selected configuration

| Entry point | Default | Explicit selection |
|---|---|---|
| Windows executable with no arguments, or `tkfs gui` | `orchestrator.toml` beside the canonical executable | `tkfs gui -f <path>` or desktop Configuration |
| `tkfs orchestrator`, `tkfs manage` | `orchestrator.toml` in the shell's current directory | `-f <path>` / `--defaults-file <path>` after the subcommand |
| `tkfs pairing` | `pairing.toml` in the shell's current directory | `tkfs pairing -f <path> <action>` |

There is no parent-directory search or automatic global/profile fallback. The
loaders require an existing file. CLI `init` and explicit desktop first-run setup
are separate creation actions. See [CLI modes](CLI.md#supervisor-desktop-and-pairing-modes).

Existing config files are canonicalized before loading. Relative fields resolve
beside the **config file's canonical target**, including when opened through a
symlink, not beside the current working directory. O1 resolves `data_directory`
and `default_mount_directory`; pairing resolves `data_directory`, file-based
credential `file`, and `orchestrator_config`. The latter must identify an existing,
valid O1 TOML even for a parse-only load. These loaders do not expand `~`, `%VAR%`
or `$VAR` within TOML path strings.

This rule does not apply to unrelated CLI path arguments: for example, advanced
`pairing grant --runtime <path>` resolves the supplied path from the shell's cwd
and stores a canonical runtime reference.

### Portable and profile first run

The default desktop configuration stays beside the executable. Its first-run
suggestions are `<config folder>/data` and `<config folder>/Projects`. CLI `init`
instead suggests `<config folder>/.tkfs/data` and `<config folder>/Projects`.
Desktop setup writes absolute selected paths; CLI `init` writes the relative
defaults `.tkfs/data` and `Projects`. Both write a fresh O1 UUID and a unique
control name.

The explicit Windows **Use a private location in my user profile** choice uses:

```text
%USERPROFILE%/.telekinesis/orchestrator.toml
%USERPROFILE%/.telekinesis/data/
%USERPROFILE%/Telekinesis/                    # mount parent
```

It creates a separate installation only after **Save and connect**. It never
relocates an existing portable configuration. On a later first-run launch,
**Open existing profile installation** is an explicit choice; it does not silently
override an adjacent or explicitly selected config. A convenient stable shortcut
can pass `gui -f <the existing profile config>`.

## O1 TOML reference

The complete schema is in [`orchestrator::Config`](../src/orchestrator.rs).
Unknown fields are rejected at the root and in all tables. Version is required.

```toml
format_version = 1
data_directory = 'data'
default_mount_directory = 'Projects' # optional; a parent, not a mount command

[control]
transport = 'named-pipe'             # use 'unix-socket' on Linux
name = 'tkfs-example'
```

Validated files: [Windows minimal](examples/configuration/orchestrator.windows.toml),
[Linux minimal](examples/configuration/orchestrator.linux.toml),
[expanded Windows](examples/configuration/orchestrator.expanded.toml).
TOML single quotes preserve Windows backslashes; double-quoted paths require
escaped backslashes or forward slashes.

| Field | Required/default | Meaning and limits |
|---|---|---|
| `format_version` | Required integer | Exactly `1` |
| `data_directory` | Required nonempty path | Native durable catalog/state root, separate from mounted views |
| `installation_id` | Optional UUID | Fresh-bootstrap identity choice; for an existing catalog it must match the actual O1 UUID. Not a rename or migration knob |
| `default_mount_directory` | Optional path | GUI new-project mount parent; does not remount existing states or replace a CLI `--mount` argument |
| `[control].transport` | Required | `named-pipe` on Windows, `unix-socket` on Linux |
| `[control].name` | Required | 1–100 ASCII letters/digits, `_` or `-`; an endpoint name, not a full pipe/socket path |
| `[network].enabled` | `false` | Must stay `false`; `true` is rejected by O1 |
| `[network].listen` | Omitted | Optional numeric IP:port, parsed only; does not bind a listener |
| `[workers].restart_policy` | `on-failure` when table omitted | Only `on-failure` accepted |
| `[workers].maximum_running` | `8` when table omitted | Integer 1–64, including headless workers |

If `[workers]` is present, **both fields are required**. `[network]` may be omitted
or empty. Networking services supervised by O1 have their own pairing configs;
do not turn on `[network].enabled` or paste pairing fields into O1 TOML.

Control is owner-local: the same Windows SID or Linux UID must manage the
installation. Linux derives its Unix control endpoint using the canonical data
root and control name; users do not put a socket pathname in `name`.

Edit worker policy or an unused default mount parent only with the supervisor
stopped, then reconnect/restart. A default-parent change affects future choices,
not existing mount reservations. There is no live O1 configuration reload.
Changing `installation_id`, data root or canonical config location is not a
supported relocation procedure. [O1 details](ORCHESTRATOR-CONFIG.md) explain its
catalog/startup checks.

## Pairing TOML reference

[`pairing_service::Config`](../src/pairing_service.rs) is shared by app-managed
and advanced standalone services. Root and credential tables reject unknown
fields. It does not contain a list of trusted peers or repository grants; those
are in the registry.

| Field | Required/default | Meaning |
|---|---|---|
| `format_version` | Required | Exactly `1` |
| `installation` | Required UUID | This pairing identity; distinct from O1's `installation_id` and a filesystem replica UUID |
| `data_directory` | Required path | Pairing registry directory, **not** O1's native state root or a mount |
| `credential` | Required table | Reference to this identity's protected certificate/private-key payload |
| `orchestrator_config` | Omitted | Exact O1 config used to verify numeric enrollment. Receiver sets it; app-managed outbound client omits it |
| `transport` | `tls` | `tls` compatibility mode or `wss`; app-managed roles explicitly use `wss` |
| `inbound` | `true` | For WSS, bind a receiving listener when true |
| `dial` | `true` | For WSS, perform automatic outbound sync for configured enabled grants; not a grant or trust toggle |
| `listen` | Empty string | WSS bind IP:port when inbound; empty when outbound-only |
| `enrollment_listen` | Empty string | Must be empty for WSS; raw TLS uses a separate enrollment address |
| `advertise` | Empty string | Public sync endpoint; outbound-only WSS uses the literal `outbound-only` |
| `enrollment_advertise` | Empty string | Public enrollment endpoint for inbound WSS; empty for outbound-only |

Defaults alone do not make a runnable config. With default `transport='tls'`,
all four address fields must parse as numeric socket addresses. The `inbound` and
`dial` role switches are WSS behavior; do not use them to infer raw-TLS listener
suppression. With `transport='wss'`:

- Inbound requires numeric `listen` and matching numeric public authorities for
  `wss://IP:PORT/tkfs/sync` and `wss://IP:PORT/tkfs/enroll`; IPv6 is bracketed.
  Public URL ports must be nonzero. No DNS, URL query, userinfo or redirect setup.
- Outbound-only requires `inbound=false`, `advertise='outbound-only'`, and empty
  `listen`, `enrollment_listen`, `enrollment_advertise`.
- App-managed receiver and outbound roles both require `dial=true`. A manual
  standalone listener with `dial=false` is valid but is a different deployment.
- Explicit numeric-loopback `ws://` is an advanced encrypted inner-TLS carrier;
  it is not plaintext trust or a fallback for the normal v3 flow.

The receiver's public compatibility field remains `/tkfs/enroll`. Normal IP/port/key
enrollment uses `/tkfs/enroll-v3` internally on that listener. Do not change TOML
paths to switch protocol versions.

### App-created examples

The [client](examples/configuration/data/desktop-pairing/client.toml) and
[Windows receiver](examples/configuration/data/receiving-pairing/server.windows.toml)
examples show the shape with readable relative paths and invented IDs. There is
also a [Linux receiver](examples/configuration/data/receiving-pairing/server.linux.toml).
Actual setup writes resolved absolute paths and independently generated UUIDs.
Do not install these examples over a managed config: their referenced credentials
and setup records intentionally do not exist.

The receiver has its own identity and references the selected O1 config. The
client has another identity and no `orchestrator_config` field; its `setup.sqlite`
still binds it to the originating O1/role. Separate registries prevent competing
service ownership. The initial receiver IP/port is immutable in this slice;
disable/re-enable preserves that endpoint. Editing the generated TOML is not an
endpoint-change workaround.

### Credential table

| `backend` | Required fields | Actual protection/loading |
|---|---|---|
| `platform-encrypted` | `file`, `name` | Current app-managed envelope; `file` accepts an absolute path or a path relative to this TOML. `name` is a cryptographic context, not a display label |
| `windows-dpapi` | `file` | Existing advanced Windows CurrentUser DPAPI identity file; not interchangeable with a platform-encrypted envelope |
| `systemd` | `name` | Existing Linux credential delivered as `$CREDENTIALS_DIRECTORY/<name>`; not a TOML path to an encrypted blob |

The protected payload contains the certificate **and private key** and must match
`installation`. Credential context names are bounded ASCII identifiers, not
paths. [Advanced TLS/systemd](examples/configuration/advanced/tls-systemd.toml)
and [outbound WSS/DPAPI](examples/configuration/advanced/wss-dpapi.toml) examples
illustrate references only; parser acceptance does not provision or decrypt them.

Windows app-managed secrets use CurrentUser DPAPI. A file copy to another machine
or OS account is not a working identity restore. Linux's new platform-encrypted
provider requires an **already-root** process, systemd 256+, and an existing
root-owned protected host secret. It does not elevate, initialize/replace that
secret, install a unit or fall back to plaintext. It holds a shared lock until
the direct helper is reaped to prevent stale-host-key replacement. Non-root
automatic v3 setup is unavailable under this provider. Existing `systemd`
credential delivery and established sync remain separate supported paths; they
do not imply that automatic v3 secret sealing is available. Linux provider tests
were synthetic; installed-provider and Linux GUI qualification are not claimed.

## Identities and directory anatomy

| Identifier | Authority |
|---|---|
| OS SID/UID | Owns local processes, private files and control access |
| O1 UUID | `orchestrator.sqlite` installation row; TOML optionally asserts it |
| Pairing UUID + certificate | Authenticated peer identity in client/receiver service, independently generated |
| Repository UUID | Shared logical filesystem/history |
| Replica/device UUID | Local author of a repository; grant pins both local and remote replicas |
| State UUID | O1 catalog entry for a managed local state; not a peer identity |

Illustrative portable layout below assumes `data_directory='data'`. Optional
files appear only after their corresponding action; names are not instructions
to precreate directories or manufacture database rows.

```text
app-or-config-folder/
  orchestrator.toml                     # selected owner settings
  orchestrator.tkfs-config.lock         # creation guard
  orchestrator.tkfs-bootstrap.json      # onboarding identity/data pin
  orchestrator.toml.pairing/
    verifier.sqlite                    # binding/rate/generation, salted v2 verifier,
                                       # encrypted OPAQUE registration/private setup
    verifier.sqlite-wal                 # transaction journal
    verifier.sqlite-shm                 # coordination / WAL index
  .tkfs-desktop/<canonical-config-hash>/
    identity/record.json                # O1 identity/data pin
    operation/record.json               # local lifecycle retry receipt
    branch-operation/record.json        # local branch retry receipt
    */record.legacy-sha256              # only after legacy metadata migration
  .tkfs-launch.lock                     # original lock names retained
  .tkfs-operation.lock
  .tkfs-branch-operation.lock
  .tkfs-supervisor-stdout.log
  .tkfs-supervisor-stderr.log
  data/
    orchestrator.sqlite                # O1 catalog, operations, local_services
    orchestrator.lock                   # one supervisor owns this root
    states/<state-uuid>/
      state.json                       # catalog/state/repo/replica marker
      metadata.sqlite                  # durable filesystem journal/branches
      objects/                         # immutable file content
      owner.lock                       # one worker owns this state
      worker.json                      # supervisor/worker lifecycle record
      runtime.json                     # current local endpoint AND control token
    staging/                           # interrupted state preparation
    logs/                              # worker logs
    desktop-pairing/
      client.toml
      identity.encrypted               # private identity envelope
      setup.sqlite                     # encrypted original identity + setup phase
      setup.lock
      registry/{pairs.sqlite,pairs.sqlite-wal,pairs.sqlite-shm,service.lock}
    receiving-pairing/
      server.toml                      # same managed file family as client
      identity.encrypted
      setup.sqlite
      setup.lock
      registry/{pairs.sqlite,pairs.sqlite-wal,pairs.sqlite-shm,service.lock}
  Projects/<chosen-mount>/              # view; may instead be elsewhere
    .tkfs-runtime.json                 # local discovery, including control token
```

`<config filename>.pairing` uses the **whole filename**: `custom.toml` gets
`custom.toml.pairing`, beside that config, not in O1 data. Verifier binding includes
the canonical config path, canonical data root and actual O1 UUID. Normal reset
persists no plaintext code: it atomically stores a salted v2 verifier plus a
protected OPAQUE record/server setup, generation and rate accounting. These are
sensitive cryptographic materials, not public bootstrap metadata.

`pairs.sqlite` retains local identity, invitations/pending candidates, trusted or
revoked peers and sync grants. `setup.sqlite` records immutable O1/role/endpoint
intent and an encrypted copy of the original identity. Prepared/publishing/completed
phases distinguish safe interrupted creation from missing completed files. An
ambiguous or damaged published setup fails recovery instead of generating a key
or empty trust registry.

`local_services` is an optional O1 catalog table for accepted outbound and receiver
bindings and desired enablement; it is not a TOML table. Bindings include canonical
paths, identity/certificate and configuration hashes. Drift stops the managed
service and reports unavailable; it does not adopt changed credentials. Enabled
supervised sync covers all configured enabled grants and does not resume paused
ones. Running service is not proof of catch-up. Closing the GUI does not stop O1.

Runtime files contain a **local control token**; do not publish them as server
bootstrap or copy their whole contents into support messages. Private keys,
encrypted credential blobs, verifier/setup databases, tokens and original pairing
codes are sensitive. UUIDs, advertised endpoints and public certificate
fingerprints can be shared intentionally; grants and logs still reveal metadata.

Desktop JSON receipts contain local requests/results, not enrollment secrets.
Both reset commands bypass these and the O1 operations journal. Legacy
`.tkfs-ui-*.json` files remain when migrated under the original locks; SHA-256
baselines detect conflicting writes from old clients. An unrelated installation's
branch receipt is not imported. Do not delete pins, receipts or baselines to clear
an error: preserve exact pending operation IDs/generations for reconciliation.

## Common tasks

### Pair two Windows desktops

1. Select/create the intended O1 installation on each computer. Keep an existing
   portable config selected; use profile setup only as an explicit new choice.
2. On the receiver, **Peers & sync → Allow connections**: enter its reachable
   numeric IP and port (GUI initial port 7421), then **Set up / enable receiving**.
   Wait for observed **Running**. Loopback addresses connect only the same host.
3. Acknowledge the reset and choose **Reset pairing code**. Transfer the 200 digits
   privately while visible; there is no recovery/display-again endpoint.
4. On the other desktop, **Connect remote** with the same IP, port and key. Identity
   setup and OPAQUE-authenticated public-key exchange are automatic. The receiver
   approves the pending device locally. No external bootstrap/fingerprint file is
   part of this normal flow.
5. Grant published repositories explicitly. Pairing/approval alone does not give
   folder access. The receive wizard prepares a replica, exposes its public IDs
   for the source owner's grant, discovers authorized content, syncs and verifies
   the selected shared branch before its first mount. See [desktop steps](DESKTOP-PAIRING.md).

There is no remote administration, automatic source-owner grant, firewall change,
service installation, general path reassignment or remove/adopt operation here.
Receiving IP/port changes are not supported by the current managed setup flow.

### Rotate enrollment or remove access

| Owner-local action | Effect | Does not do |
|---|---|---|
| Normal `manage reset-pairing-code` / UI reset | Fresh exactly-200-digit key; v3 registration and v2 verifier replace the old ones | Revoke trusted peers, grants, existing sync or already admitted pending approvals |
| Explicit `manage reset-legacy-pairing-code` | Advanced pinned-v2 rotation; removes v3 registration, marks upgrade required | Silently run as normal-flow fallback; migrate protected identity |
| Reject pending code candidate | Remove an unapproved numeric candidate | Revoke an already approved peer |
| Revoke peer locally | Persist local access denial | Revoke the other host's record or erase its received files |
| Pause grant / disable background sync / disable receiving | Stop selected sync scheduling/service role | Reset identities, revoke peers or erase grants |

Both reset commands are non-replayable and show the code once; lost delivery
requires another explicit reset. Old hash-only installations need a **normal**
explicit reset to enable three-field v3. Existing pair/sync credentials survive
that upgrade. V1 invitations retain their separate expiry/one-use rules; numeric
rotation does not invalidate them. Advanced pinned-v2 public-bootstrap steps are
documented separately in [PAIRING.md](PAIRING.md#numeric-pairing-codes).

### Diagnose without repairing identity implicitly

| Symptom | Safe next step |
|---|---|
| Wrong/missing selected TOML | Select the intended original path; do not initialize over an existing installation |
| Missing/corrupt catalog, registry, setup record or identity | Stop use, preserve all related files and restore a consistent known-good set; no automatic re-key or empty-registry repair |
| Protected credential unavailable | Check the original account/provider context; never change backend or add a plaintext key to bypass it |
| Unsafe ancestor, ACL, symlink or hardlink | Diagnose the path/ownership issue; application does not repair permissions or relax checks |
| Managed binding changed | Restore the exact accepted configuration/identity and use owner-local recovery; arbitrary TOML editing is not supported reconfiguration |
| Uncertain shutdown/drain | Keep ownership intact and inspect status; do not start overlapping service owners or delete their locks |
| Empty discovery after approval | Confirm explicit repository/replica grants; this is not evidence that pairing failed |

### Back up, restore and upgrade

Stop activity and cooperatively shut down the supervisor/workers and any standalone
pairing services before taking a consistent backup. Closing only the GUI is
insufficient. Preserve the selected config, complete native data (including
private branches/objects), config-adjacent enrollment store, managed pairing/setup
files and desktop/onboarding identity/operation records. Mounted file copies alone
are not a filesystem-history, trust or identity backup. Preserve ownership and
private permissions; protect backups as secrets.

Do not copy just a TOML or `pairs.sqlite` out of a live database. Include retained
WAL/SHM and related files in the stopped consistent set; they are not stale debris.
Windows pairing/verifier connections deliberately retain sidecar identity to
avoid last-close deletion racing security validation. Completed checkpoints can
truncate WAL, but this is not a hard cap while transactions are active.

There is no general relocate/import/clone recovery command. Same-environment
restoration must preserve IDs, canonical path bindings and the original protected
credential context. DPAPI backup portability depends on the original account's
Windows key material; Linux protected envelopes depend on host/user/context (and
possibly TPM) binding. Copying encrypted bytes to another host/account is **not**
a promised restore. If the original identity cannot be recovered, explicit new
identity enrollment/revocation is separate recovery work, not an edit of UUIDs.
Never clone an existing replica to manufacture a second independent author.

For upgrades, stop older processes and external SQLite tools using these stores,
close old GUI windows, then launch the new executable with `gui -f <same config>`.
No-argument launch selects the config beside the **new executable**, which may be
a different folder. A successful build does not replace a running supervisor.
Older/external connections can still delete WAL/SHM on last close, so do not mix
versions on the same registry. Leave managed files in place; no reset or credential
reprovision is required merely to run an updated binary.

## Example validation and implementation references

The [example README and validator](examples/configuration/README.md) load only
copied sample TOML through the current Rust parsers, check path/default/error
contracts, and assert that no files are created by loading. They never invoke a
credential backend, create an O1 catalog, mount or bind a listener. Parse-valid
does not mean provisioned or safe to deploy.

Source of truth: [O1](../src/orchestrator.rs), [pairing config](../src/pairing_service.rs),
[WSS validation](../src/wss_transport.rs), [credential references](../src/pairing.rs),
[automatic setup](../src/pairing_setup.rs), [service binding](../src/managed_pairing.rs),
[desktop paths/receipts](../src/desktop.rs), [enrollment store](../src/pairing_code.rs),
and [protected envelopes](../src/protected_secret.rs). For executable behavior and
qualification limits, see the [delivery ledger](DESKTOP-EVOLUTION.md).
