# WP6.G removal and replacement inventory

2026-10-21. Scope: [the greenfield interlude](../WAVE_6_PLAN.md#31-wp6g--greenfield-interlude-in-progress).
This is the integration inventory, not an assertion that all listed removals
have landed. The user removed prior-version support from the release. Preserve
the completed current-path behavior through CCR-WP6.4-30 and its historical
evidence. Do not reset existing development stores or mutate live mounts.

## Production routes and disposition

Scope review, 2026-10-22: the [personal-use authority
decision](../AUTHORITY_SCOPE.md) applies to every disposition below. Retain
pairing/route/reference and filesystem ownership checks; do not retain custom
HMAC, credential enrollment or local-user defenses solely because an earlier
row or historical test required them. Consolidate to simple hashed pairing over
the deployment-protected channel. Current Users/ACL work and a separate security
milestone are out of scope. Existing runtime mechanisms remain until their
callers and applicable authority coverage are replaced; no removal is claimed.

| Component and entry points | Disposition | Replacement, dependency and coverage owner | State |
|---|---|---|---|
| Private journal `WriteJournal::open`, `write/readers.rs`; header upgrades in `content/write.rs` | Remove v4/v5/v6 upgrade chain and prior-reader execution; retain selected application ID and v7 layout | Direct fresh initialization, exact current restart, unsupported-version refusal before writable open; `tests/content_write/formats.rs` plus existing Saved/adoption/reclamation/crash tests | Implemented in CCR-WP6.G-2; qualification recorded separately |
| `model/content.rs::ContentKind::Legacy`, binary kind 0/format 0, JSON legacy tags; `tests/fixtures/content_v1` | Remove legacy descriptors; retain direct kind 1 and manifest kind 2 with their existing digest domains and canonical empty identity | Reject unsupported descriptor tags without reinterpreting them; codec/golden tests in `content_format` | G14 removes the variant/codec branches. The old independent binary vector remains solely a negative fixture; scoped qualification recorded below |
| `content/local.rs` whole-file `read_legacy`, `convert_legacy`, `LegacyInput`; conversion caches in `content/write.rs` and `write/retention.rs` | Remove conversion and legacy verification branches; retain direct small-file digest verification and typed subtree reuse | All bases are typed from creation; `content_provider`, `content_write`, `content_retention`, internal retained-read tests. Rebase full replacement, cancellation and parallel-capacity scenarios rather than dropping them | G14 removes provider readers/conversion and both caches/provider overrides; current COW and retained recovery retain bounded chunk preparation. Other standalone/staging routes remain pending |
| Bare string content changes in Core projection/validation/admission; `core/content_saves.rs::begin_content_save_inner` fallback to `object_size` | Replace untyped content events and fallback capture with typed descriptor-only events | Update ordinary `Store::create`, write/import, checkpoint/restore and other producers before refusing their old values; retain causal parents, full captures, admission outbox/inbox, oracle and permission checks | Direct create/write/explicit-stream/import and standalone snapshot publication emit typed roots in G13. G14 removes fallback save capture and requires `content_ref`. G18 removes string acceptance in Core validation/projection and size fallbacks. The temporary Entry.content member only encodes/decodes null. G19 replaces status/folder-details with current descriptor/index/receipt summaries and off-lock snapshot reads; G20 removes preview/request hash controls in favor of registered-reference preview and bounded explicit range demand; old object catalogs, generic artifact consumers and standalone execution still require consolidation; full matrix/removal remain open |
| Core metadata `Store::{initialize,open_existing_with,open_selected,open_identity}`, `core/schema.rs`; branch UNIQUE migration, inbox ALTER chain, object/feed/snapshot index backfills | Replace old-store migrations with one directly initialized current schema and explicit store-format refusal | Preserve generic schema constraints, journal triggers, inbox classification, snapshot recovery/rebuild and acknowledgement correctness. Fresh startup, current restart and fault tests own coverage; rebuilding disposable data remains valid recovery | Implemented and qualified in CCR-WP6.G-3; staged source/image proof recorded separately |
| Orchestrator `catalog.rs::migrate`, `catalog_schema`, additive intent/gate/credential schemas; catalog-only legacy fixture consumers | Replace prior catalog migration with one selected fresh catalog layout; rebase retained lifecycle/pairing scenarios | Preserve installation identity, credentials/revocation, durable folder intent, mount gates, request ownership and startup reconciliation. Catalog actor and current daemon lifecycle tests own coverage; old datadir interoperability is removed scope | Implemented and qualified in CCR-WP6.G-4; staged test-only security rebase recorded separately |
| `Engine::new`/open paths, `NativeWrites::install`, `content_saves::SaveExecutor`, optional `Engine::native_writes` and `fixture:content_v1` in admission/RPC capabilities | Replace optional fixture selection with default current execution for fresh stores | Default installation must own stop/cancel/error handling. Preserve typed create, buffered writes/truncation, FULL flush/final close, rename/replacement, retired destination and startup recovery; actual WinFsp/FUSE tests | G12 qualifies RPC-default initialization and fatal retention; G15 covers current lifecycle obligations. G16 initializes the same service at every public WinFsp/FUSE attachment, including no-RPC library entry, and removes the C optional-mode argument. Other Rust Option/fallback/runtime/staging removal remains pending; no complete single-path claim |
| `staging.rs::{Staged,StageSnapshot}`, old `Engine` deferred create/save snapshot/pending-save persistence and restore | Remove whole-file staging/save route once native ownership covers its ordinary behavior | Preserve creation collision regression, failed-close recovery, branch/checkout/shutdown fences, deletion/open-handle lifetime and same-format retention through private journals; migrate applicable `pending_saves`/contracts scenarios to native tests | G15 moves current lifecycle obligations to Saved. G17 removes whole-file handle hydration/copy. G21 removes mounted mutation fallback selection. G22 removes deferred creation, first-save insertion and the parallel pending-name index; its 26-function ledger names current replacements. Memory/disk snapshots, raw save APIs, pending_saves persistence/control and shared-saver consolidation remain pending |
| WinFsp `mount.rs`, FUSE `fuse_linux.rs`: fallback after native coordinator returns `None`, old directory mutations, delete-on-close/unlink | Replace fallback selection and complete missing current operations before deleting their working route | Native create/open/read/write/fsync/close/rename/replace plus directory metadata/unlink/delete-on-close and OS handle semantics. Retain inode/parent/entity/view identity and existing repository permissions | G5–G10 qualify scoped file/directory mutation and retained observation behavior; G16 installs current execution at public attachment. G21 removes mounted mutation fallback selection. G22 retains source/destination operation handles across the last user close and passes the real concurrent workload. Complete remote/concurrent-parent lifetimes remain pending |
| `runtime/content_reads.rs`, `content_base.rs`, `staging::ContentSelection` versus reference-file reads | Replace bare-hash staging/hydration path; retain typed provider/range demand | Bounded immutable ranges, independent partial hydration, current pinned views, cancellation/deadlines, authorization and shared native owner identity; `content_mount_reads`/`content_transport` | G13/G18 consolidate typed producers; G17 removes whole-file handle hydration/copy and uses immutable current range selections. G20 replaces preview/request hash consumers. G22 reruns scoped current reads and real peer missing-range parents; complete transport/load qualification remains pending |
| `model/content_wire.rs::LEGACY_*` and legacy DTOs; `runtime/content_rpc.rs` legacy dispatch/POLL demand member; `objects_rpc.rs` old read/prepare/pull/push | Remove prior whole-file protocol and optional capability fallback; retain explicit current protocol validation | Typed demand/read/transfer completion, authenticated disclosure proofs, generation ownership, no ACK on unsupported input; same-version real-session/peer tests | Pending routing of every current object need |
| `orchestrator/{objects,inbound_objects,legacy_demand}.rs`, `content_dispatch.rs` legacy registration/poll/reservation scheduling and `content_transport.rs` compatibility selections | Remove duplicate whole-file scheduler, credits, spool/lease protocol and fallback choices after typed replacement | Keep single bounded typed scheduler, cross-repository fairness, source retry/backoff, exact leases, cancellation, current session reconnect and stalled-source progress. Rebase applicable race/capacity assertions in `content_transport` before retiring old-only tests | Pending |
| Core old object transfer/part/spool/demand/lease tables and helpers; object hash references for non-file internal artifacts | Remove legacy file transport/storage scaffolding; audit generic byte objects individually | Checkpoints/snapshots/control artifacts may still need immutable bytes. Give these a typed, authorized bounded route or document a current reason to retain their generic local CAS infrastructure; do not delete shared authorization or garbage-collection roots by name alone | Pending complete non-file consumer trace |
| `session/auth.rs` custom HMAC transcript, nonce/MAC enrollment secret wrapping and `peer-hmac-v1` negotiation in `session/handshake.rs` | Simplify under the user's 2026-10-22 transport-security decision: TLS/WSS or the enclosing VPN owns channel protection; remove custom channel-security and enrollment wrapping rather than extending them | Use ordinary application credential/permission checks over the selected transport. Keep pairing/unpairing behavior and current repository access rules; rebase handshake tests to the simplified current path | Removed in CCR-WP6.G-8 with scoped pairing/session/lifecycle coverage on both platforms. A broader Windows receipt/readiness diagnostic remains failed and unwaived; no separate security gate is added |
| Event causal feed, admission generations, retry classification, status publication/hub/catalog wrappers whose identifiers contain `legacy` | Retain current correctness/lifecycle mechanisms; remove actual older-version negotiation/translation | Causal ordering, durable acknowledgement, truthfully reported per-engine readiness and lifecycle fences remain requirements. A historical identifier alone does not identify an obsolete execution route | Audit each reachable branch during transport/default-path consolidation |
| Prior-executable readers, upgrade/downgrade/export-to-old-version fixtures and launcher fences | Remove executable compatibility gates as their component is consolidated; retain historical reports/source/image/failure archives | Unsupported input refusal is a current-format safety requirement. Wave 5 is an isolated performance reference, never a runtime fallback | Private-journal prior-reader tests removed; other fixtures pending |

## Qualification and coverage ledger

CCR-WP6.G-2 keeps the v7 journal's current on-disk bytes and descriptor semantics;
there is no new interpretation of an older encoding. The journal filename
`write-sessions-v1.sqlite` is a locator, not its accepted schema version. The
header is the authority. A file containing an unsupported header, including an
unmarked nonempty database, is refused with `UNSUPPORTED_WRITE_JOURNAL` and an
instruction to use a fresh store directory. Existing data is never reset.

Removed journal-only tests and their replacements:

| Removed test | Reason / retained correctness coverage |
|---|---|
| `v4_v5_and_v6_headless_journal_upgrade_preserves_exact_records_and_checkpoint_identities` | Upgrades are out of scope. `formats::fresh_current_journal_restarts_without_rewriting_saved_records_or_clocks` verifies exact current records, seals, identities, bytes and native/headless clocks. |
| `process_exit_during_v4_and_v5_headless_upgrade_preserves_saved_and_header_atomicity` | No upgrade transaction remains. `formats::process_exit_during_fresh_initialization_leaves_an_empty_or_complete_current_layout` covers three actual exit-86 initialization boundaries; existing checkpoint/publication/adoption crash tests remain. |
| Three `attested_v4_*`, `attested_v5_*`, `attested_v6_*` parents and shared helper | Prior executables and native-v5 matching-reader recovery are out of scope. Historical evidence remains unchanged. The current binary's nonmutating unsupported-header refusal is covered directly. |
| `the_experimental_v1_journal_is_refused_without_discarding_its_records` | Subsumed by the 104-case native/headless, DELETE/WAL, open/closed header matrix with exact database/WAL/rollback-journal comparisons. |
| `future_or_unrecognized_journal_formats_are_refused_without_rewriting_them` | Subsumed by that matrix and `formats::unmarked_nonempty_database_is_not_initialized_even_when_it_has_no_tables`, including a foreign table and a table-free view. |

The other inventory rows require their own coverage ledger when changed. No
still-relevant failed test has been erased. The Windows timestamp-policy driver
failure and structural demand reliability gap remain open with their original
bounds and evidence. Private-journal consolidation alone cannot close WP6.G,
remaining 6.1–6.4 obligations, WP6.5, WP6.6 or WP6.INT. In particular, current
two-peer fresh-store acceptance is still required after default-path changes.

Two development negatives are retained separately. The initial new restart
test incorrectly required the mutable Working owner/row seal to remain unchanged
after resume; only Saved is immutable there. Correcting that test changed no
runtime code. A subsequent independent actual-image probe found a real gap:
read-only SQLite created empty WAL/SHM sidecars when refusing a closed
unsupported WAL-mode database. The preceding green ordinary/kernel matrix did
not cover that case. The standalone-header guard and the expanded 104-case
open/closed matrix address it; malformed/previously-used-unmarked/empty locator
fixtures add coverage. A nonempty WAL remains authoritative through SQLite;
the guard does not parse or replace WAL recovery. Exact initial sources,
images' hashes, raw results and capture-time archive hashes remain preserved.

## Core metadata coverage ledger (CCR-WP6.G-3)

The qualified G3 checkpoint selected application ID `0x544b4d53`, version 1,
initialized from `core/schema.sql`, with 110 explicit schema objects. G5's
durable disposition correction advances the selected fresh layout to version 2
and 112 objects with the G5 scoped qualification below; it supplies no v1 reader
or upgrade.
Current repair remains bounded recovery, not support for a historical format.
The [qualification record](wp6-progress.md#current-core-metadata-ccr-wp6g-3)
covers this stage; it does not close the other inventory rows.

| Removed or rebased scenario | Reason / current coverage |
|---|---|
| `availability_index::legacy_state_upgrades_once_then_restarts_without_hashing` | Historical executables/upgrade are removed scope. `store_format` verifies direct fresh schema and current restart. |
| `availability_index::event_objects_backfill_is_chunked_and_resumable` | Rebased as `current_event_reference_repair_is_chunked_and_resumable`, using current repair cursors and retaining chunk/restart assertions. |
| `objects_v3_worker::legacy_spool_is_adopted_once` | Unrecorded older spools are no longer adopted. Existing current durable-offset, truncation, crash/retransfer and verified-install scenarios remain. |
| `activation_inbox::legacy_rows_are_classified_and_drained` old-store fixture half | Prior-schema upgrade is removed scope. Its current unclassified/transient retry half remains as `current_unclassified_rows_are_classified_and_drained`; `w1_integration` authors invalid/descendant current events in a fresh store and runs the real revalidation worker. |
| `harness_selftest::{legacy_datadir_fixture_opens_with_current_build,legacy_worker_fixture_opens_with_current_build}` | Executable interoperability is removed scope. Fixture-copy integrity remains; actual current worker startup is exercised by retained worker/peer scenarios. Catalog-only old-datadir consumers remain pending in the catalog row above. |
| Feed schema stamp and lost-table fixture | Assert the selected metadata version instead of a historical module stamp. Missing current feed structure is rebuilt without resetting acknowledgements. |
| Seal-path older-store journal-index test | Rebased and renamed to current derived-table loss. Journal sequence/epoch, snapshot replay, peer acknowledgement and oracle assertions remain. |
| Snapshot query-plan initialization | Uses the central current SQL. Chunked journal-index recovery/restart remains covered. |

Seven ordinary `store_format` parents cover schema/golden agreement,
concurrent identities with bounded busy retry, exact authority and typed/private
Saved restart, 32 header/mode/open-or-closed combinations through four Store APIs,
refusal without persistent changes, damaged current authority, three actual
fresh-initialization exits and three actual derived-repair exits. Its one ignored
child helper is invoked by those parents, not an extra unique passing case.

Initial negatives are preserved before correction. The new concurrent test
incorrectly required every simultaneous writer to succeed, then dropped its
temporary directory before all threads ended. Its correction preserves permitted
SQLite contention and requires identical committed identities after retry.
Separately, the broader retained worker suite exposed a real one-second claim
wait during prepared error projection. The existing 50 ms service bound remains.
The first nonblocking correction passed that focused case but failed the
retained both-phase admission/cancellation unit on both systems; its exact
intermediate source and results are preserved too. The revised family gate
marks durable error projection separately: it defers claims immediately for
that commit, while retaining cooperative admission waits, cancellation, stop
and run fencing. Both initial Windows/Linux failures and external diagnostics
have exact source/image/log capsules. Neither relevant test was removed.

The final follow-up changes only W2's CLI-status test precondition: after direct
controller readiness, wait up to one second for the public 4 Hz hub snapshot to
publish running, then keep the existing CLI/schema/watch assertions. Production
code and all other inputs/images are byte-identical. Earlier successful targets
retain their actual source/image provenance; W2 is rerun on both platforms and
the remaining Windows target/kernel cases run explicitly. The prior Windows
failure and complete prior Linux proof remain preserved. Totals are 761/769
ordinary cases and five real kernel parents per platform, not a claim of one
uninterrupted final-source matrix. Current catalog and other rows remain pending.

## Catalog coverage ledger (CCR-WP6.G-4)

The current catalog selects application ID `0x544b4353`, version 1 and seven
tables. The golden definitions come from the preceding attested fresh CLI
capture, excluding the obsolete mirror table. Current input is validated before
writer/lock/mount-directory creation or permission changes. There is no current
authoritative-data rebuild, old-format upgrade or credential reset on open.

| Removed or rebased coverage | Reason / current coverage owner |
|---|---|
| `catalog_actor::legacy_datadir_opens_equal_to_the_legacy_supervisor_view` | Prior-format adoption and old-view equivalence are removed scope. New exact-row catalog restart and current snapshot/identity/credential tests retain folder, gate, peer, verifier and key preservation. |
| `catalog_actor::downgrade_legacy_daemon_creates_pairs_and_rotates_on_a_migrated_catalog` | Downgraded writes are unsupported. Current command-generation coverage remains; current actor restart and the actual fresh-restart/two-peer scenario cover retained create/pair/rotate/restart behavior. |
| `catalog_actor::gate_changes_made_while_downgraded_are_reconciled_on_open` | Raw older-writer reconciliation is removed. Existing current Create/Retarget/Override/Clear gate assertions remain against the sole canonical row, with atomic rollback and a pending gate retained after SetMount(None)/restart. |
| `catalog_actor::legacy_oracle`, `legacy_view`, legacy mount helper | Test-only old catalog SQL/runtime is retired. Real catalog/directory-lock ownership assertions remain. Mount validation retains 15 explicit success/error/class cases and exact canonical targets, independent of a previous binary. |
| `security::a_rotation_by_a_build_without_credentials_revokes_them_at_the_next_open` | Older writers are unsupported. Rebased as `current_verifier_rotation_revokes_credentials_across_restarts`: the actual current command revokes old keys, stale-generation enrollment still fails, and newly enrolled keys survive restart. Existing revocation/session tests remain. |
| `security::code_paired_peers_enroll_on_their_next_connection` drops the credential table | That fixture requires the retired additive schema migration. Rebased as `current_code_paired_peers_reenroll_after_credential_loss_and_restart`: delete credential rows only in stopped isolated current fixtures, retain both installation identities, require matching new credentials distinct from the old key, authenticate with the new key and restore ready lanes. Missing current tables remain a fail-closed error covered by `catalog_format`. |
| `qual_wp3_int_e2e::legacy_fixture_upgrade_then_sync` | Rebased as `current_restart_then_sync`: create on a fresh current node, restart with installation unchanged, synchronize to a second current node and send content back. It is an explicit separate opt-in; no old-format fixture is opened. |
| Lease corruption's old catch-all `CATALOG_UNAVAILABLE` expectation | Selected current-format validation now reports malformed authority as `INVALID_CATALOG` before writable access; a valid exhausted counter retains `CATALOG_UNAVAILABLE`. The existing test keeps both causes, startup refusal, no-reuse and unchanged stored-value assertions. |

Seven new catalog-format parents cover golden initialization, exact current rows
and credentials, 32 nonmutating header/mode/open-or-closed combinations, damaged
authority, three actual initialization exits, an actual exit during verifier/key
rotation and counter exhaustion. Their ignored child helper is not an additional
unique case. Current directory-lock/panic/close, immutable snapshot/wake-up,
queued withdrawal/unknown outcome, authentication/revocation and lifecycle tests
remain required. `verifier_salt` is retained for current consistency validation;
invalid individual credential rows retain their scoped fail-closed handling.
Public `initial_sync_pending` is a current UI/API projection from gates, not an
on-disk mirror or previous-version negotiation.

The deliberately interrupted initial gate-validator checkpoint, old counter-error
expectation failure and dropped-table enrollment fixture failures are preserved
with source/image/log hashes. Both expectation/fixture failures were confirmed on
both platforms; their corrections are test-only, not a counter reset, schema
reconstruction or service-bound waiver. Final qualification records 1,143/1,151
ordinary Windows/Linux cases across 43 selected targets, five actual kernel
parents and one separate restart/two-peer scenario/platform. The 28-case security
target reruns on final source; the other 42 targets and explicit kernel/peer
proofs retain their preceding source SHA and unchanged invoked images. Only the
security test input differs. This staged provenance and exact capsules are in the
[catalog record](wp6-progress.md#current-catalog-ccr-wp6g-4). Other WP6.G rows remain
pending, including default typed execution.

## Native unlink coverage ledger (CCR-WP6.G-5, qualified file scope)

The initial 414-input `23ded7b8` candidate adds namespace-only unlink using the
current retirement machinery. Both platforms pass 31 namespace and 57 native-write
ordinary cases and one dedicated actual WinFsp/FUSE parent. Five new Core parents
include three actual exit-86 metadata boundaries; five new runtime parents cover
retained bytes, exact replies, failure retry and Engine availability. These are
focused checks, not the planned complete 11-target/six-kernel qualification.

Actual WinFsp injection of persistent `content.save.unlink_written` failure
returns from the void Cleanup path with a logged mount-op-9 error and the old
name still visible. This exposed an implementation gap. Durable byte
preservation does not preserve an unrecorded deletion request. Exact input/image
hashes, raw results and actual failed Windows harness/CLI bytes were captured
before any correction. The [progress record](wp6-progress.md#native-unlink-and-durable-dispositions-ccr-wp6g-5)
links that partial evidence and archive.

No test was removed at the initial checkpoint. The durable correction removes
the replacement parent's two old Windows assertions requiring error 50 for
delete/delete-on-close; their temporary unsupported-operation premise changed.
All replacement and retired-destination assertions remain. Dedicated positive
and failure/restart/cancellation kernel parents own removal coverage. The first
executable correction exposed
root traversal, post-publication inspection and repeated stale-ACK defects,
preserved with actual failing images before correction in the progress record.
Final 418-input source `b01a6d7c` passes 512 Windows/520 Linux ordinary cases
across 12 targets/platform and seven explicit actual kernel parents/platform.
The new authority/cancellation/restart regressions pass, including actual Windows
delete-on-close recovery and FUSE's fallible failure. Strict release Clippy and
format/diff checks pass; all 13 images/platform remain unchanged during proof.
The [final evidence](../../../test-evidence/wave-6/native-unlink-durable-checks.json)
retains the initial failure and all three correction development capsules.
Default native execution and complete directory/lifetime/remote operations remain
open inventory requirements.

## Native directory creation ledger (CCR-WP6.G-6, qualified creation scope)

The creation replacement uses one pooled FULL metadata writer outside Engine and
the existing bounded namespace reservations. Its `directory-create:v1` receipt
shares the selected current creation-authority table with `create:v1` file
receipts, without creating a byte owner or a new SQL/private-journal layout.
WinFsp directory Create and threaded FUSE MKDIR use it in native fixtures.
Initial mode belongs to the creation event; metadata-only handles keep enumeration
cuts. Exact historical replay and current handle-ACK authority are separate checks.

The existing actual native-file creation parent still checks four file receipts,
all bytes/flush/close/replacement assertions and its nested child. It now also
checks the directory receipt created by its existing `mkdir`, with total and
file/directory counts reported separately. No retained test scenario was removed.
Five new Core and five runtime parents, plus actual directory/progress and
parallel rename/open/name-reuse kernel parents, pass. The initial test-only private-method compile
failure and Linux driver CRLF failure are preserved in the progress record.
The first actual kernel run then fails the proposed same-parent named-stat timing
on both platforms. Linux wait sampling and handle controls separate kernel path
serialization from daemon progress. Windows handle metadata also exposes the SDK
FINE volume-wide namespace guard. Exact failed images/probes/header are archived.
The correction selects supported null SDK guards for explicit native mounts and
retains the 50 ms Engine/300 ms existing-handle checks; named-stat overlap remains
measured separately, without qualifying or waiving original W6 load/cache gates.
Final source `afe3e904` passes 522 Windows/530 Linux ordinary cases across 12
targets/platform and nine actual kernel parents/platform. There are 26/25 default
ignores; repeats and actual exit children are not extra unique passes. Strict
release Clippy and format/diff checks pass; all 13 images/platform and source
hashes stay unchanged through proof. The
[final evidence](../../../test-evidence/wave-6/native-directory-create-checks.json)
retains all source/driver/failure capsules. The separate actual current-image
Windows time-policy parent still fails and is not included among passing parents.
G6 did not qualify directory rename/delete/basic metadata or dispositions.
Their current scoped result follows; full lifetime/remote ordering and default
execution remain open.

## Native directory mutation/disposition ledger (CCR-WP6.G-7)

`content_saves/directory/{changes,deletion}.rs` owns exact metadata snapshots,
sealed command receipts and per-handle deletion requests. Runtime directory
coordinators acquire bounded entity/name ownership before pooled SQL, commit
outside Engine and retain metadata without byte-save owners. Windows routes
basic/rename/SetDelete/Cleanup through these commands; FUSE routes basic/rename/
rmdir and separates restored directory inode incarnations. Owned mutations
advance pending requests, replacement/removal consumes them and startup replays
requests before opening the private byte journal. Native child creation/moves
refuse a parent with pending deletion inside the writer.

Fresh metadata layout 3 adds only the directory request table/index (114 objects);
there is no old-layout reader or migration. The independent fixture retains the
preceding 112 definitions and explicitly adds the two new definitions.
The 429-input source `96ffcdb3` passes strict release Clippy on both platforms,
539 Windows/548 Linux ordinary cases across 12 targets/platform and eleven actual
kernel parents/platform, with 29/28 default ignores. All 13 images/platform and
the source remain unchanged through proof. Evidence:
[native-directory-metadata-checks.json](../../../test-evidence/wave-6/native-directory-metadata-checks.json).
Full remote/deadline/lost-observation lifetimes, default typed execution and
remaining greenfield removals stay open. Transport work must simplify the custom
handshake/enrollment wrapping under the user's TLS/WSS-or-VPN boundary.

## Next dependency trace: file-content producers and consumers

Trace refreshed during CCR-WP6.G-13. Default worker installation, removal of
production fixture gates and typed direct Core producers have landed. Remaining
codec/consumer, standalone Engine/staging and transport consolidation is incomplete. Removing descriptor kind 0 alone would leave current
operations without their working route.

| Current route | Required replacement before removal |
|---|---|
| `Store::create_with_attributes` and `core/content.rs` byte/explicit-stream/prepared publishers now emit current descriptors; `write_object_with_base_and_times` is removed | G13 verifies typed empty/direct/manifest output, parents/clocks, FULL-WAL, preparation outside an outer transaction and current CAS/metadata crashes. Remaining restore/recovery/codec consumers and all caller routes still need audit before single-path closure. |
| `Event::objects` remains an old file-object consumer; G18 makes `Entry::set_content_value`/`content_value`, commit, staging and sharing validation descriptor-only | G18 implements descriptor-only acceptance/projection with refusal before nonlocal byte work, bundle installation or feed acknowledgement. Event.objects/object_sizes and remaining consumers/tables still need removal; current replay and fixture/oracle checks pass in the scoped evidence. |
| `core/admission.rs::append_event` selects descriptor-valued content changes; production fixture gates are gone and direct Core producers now emit descriptors | G13 asserts initial empty-root admission, rollback/replay and actual cursor progress. Preserve this root handoff while removing string consumers and old whole-file reference/job tables. |
| `begin_content_save_inner` requires the typed `content_ref` after G14; G17 makes `Staged::from_entry` descriptor-only; G18 removes string/object-size fallback from `Engine::visible_size` and FUSE metadata attributes | Complete the remaining string/staging removal. Preserve retained handles, partial hydration, current SaveIntent ownership and bounded off-lock byte work. |
| `Engine::finish_save` and deferred-create publication consume pinned typed preparation; old staging/deferred APIs remain | Replace/remove standalone Engine/staging execution and its callers. G15 covers failed retain/commit, force stop, poison rollback/replacement, writer waits and gauges on the actual current path. The earlier six failing raw-API executions stay preserved; replacement coverage does not claim the remaining production routes have disappeared. |
| `Store::checkpoint` stores serialized projection bytes as a generic object; `restore` uses each entry's `content_value`; shared export enumerates `Event::objects` | Keep internal immutable checkpoint bytes distinct from file descriptors. Update restore and export traversal/authorization for typed roots rather than deleting generic local CAS by its historical name. |

The actual restart/two-peer catalog scenario exercises current executables and
fresh selected stores. Default worker installation is now implemented, but
remaining producer/consumer routes, standalone Engine APIs and transfer paths
still prevent claiming a complete single current path.

## G14 descriptor/provider coverage ledger

[Current evidence](../../../test-evidence/wave-6/current-content-codec-checks.json)
records 395 successful cases/platform across two identified source cohorts and
three actual final-CLI kernel parents/platform. The two failed receipt/read
fixtures are preserved with their actual harnesses before their test-only fixes;
neither those failures nor older transfer/lifecycle/timing failures are erased.

| Removed behavior/test expectation | Current replacement |
|---|---|
| Valid legacy binary/JSON descriptor and whole-file provider reads | Only direct/manifest descriptor goldens; negative old binary/JSON/hash-string decoding and metadata commit without admission on refusal. Independent current-range missing/corrupt tests remain. |
| Whole-file conversion, repeated conversion cache, parallel nested permits | Bounded repeated/parallel current chunk preparation, independent returned bytes and corrupt affected-chunk refusal. |
| Full replacement and stale buffer waits over current/legacy bases | Both current Direct and Manifest bases, absent bytes/structure, exact cancellation/stale selection and released capacity. |
| Retained legacy conversion cache | Current retained reconstruction with missing unchanged chunks, full replacement without any old bytes/structure, repeated restoration and unchanged retained-reader lifetime/cancel/capacity tests. |
| Receipt test expects no jobs without processing the admitted initial empty revision | Explicitly verify only canonical empty candidates, assert exactly the tested root remains, then preserve failed-index recovery/publication checks. |
| Empty legacy read bypasses a zero-second wait | Typed empty reads return CONTENT_DEADLINE at zero duration and Some(empty) with a valid deadline; stale-handle assertions remain. |

The transfer fixture no longer synthesizes retired descriptors. Existing
whole-file actor/race scenarios still need current-transfer replacements; no
full transfer-target pass is claimed. String `Entry.content`, Core ingress and
projection, status/preview/demand, standalone staging and old RPC/schedulers
remain separate pending entries above.

## G15 lifecycle replacement ledger

[Current evidence](../../../test-evidence/wave-6/native-saved-lifecycle-checks.json)
records 305 cases/platform in identified cohorts and one explicit final-CLI
WinFsp/FUSE parent/platform. It includes ten internal cases and 295 integration
cases. The prior G13 six-case raw-API failures remain historical evidence; the
following obligations now have current-path coverage.

| Former standalone coverage | Current owner and checks |
|---|---|
| `force_stop_persists_dirty` via `Engine::write` and the old table | Native coordinator write/retain, refusal of graceful stop, exact private Saved and heartbeat count, empty committed root, restart/read/flush and exact canonical bytes; old table stays empty. |
| `persist_after_poison` plus retained old rows | Method removed. Actual fatal-helper tests cover rollback and injected rollback failure/connection replacement, no half-done SQL publication, idempotent off-lock retention and exact current Saved recovery. Existing actual process poison-exit coverage stays. |
| Tracked locks, gauges and heartbeat | Native dirty file; lock/contended waiter observations and heartbeat with Engine/private writer blocked; durable Saved counted after force stop. |
| Retained-save writer wait and rollback | Native Saved and secondary publication writer contention; failed history insert preserves exact metadata/admission, old canonical content and Saved, then succeeds on retry. |
| Failed COMMIT followed by failed retain | Deferred-FK publication COMMIT failure rolls back; newer buffered bytes remain live after a private checkpoint refusal, the previous Saved remains identical/countable, and retry publishes the latest exact bytes. |

The journal counter reuses its pending index and publishes an atomic mirror only
after checkpoint/adoption commits. Executor tests cover committed 64-file groups,
group rollback, newer buffered versus Saved revisions and continuation/restart
counts. In-process fixtures own RPC shutdown/drain and fail normal teardown errors.
Remaining standalone Engine/staging/adapter/table and transfer routes above still
prevent a complete single-path claim.

## G16 direct-mount initialization ledger

[Current evidence](../../../test-evidence/wave-6/mount-default-current-checks.json)
records one source cohort with 209 Windows/215 Linux ordinary passes and two
explicit actual-kernel parents/platform. Public adapter attachment now installs
or reuses the current service before OS mount creation. The C native-content
argument and its alternate capability/guard selection are removed. The new
direct parent starts no RPC and verifies import/rewrite, create/rename/truncate/
fsync, same-owner reuse, exact private Saved recovery on another direct mount,
and fresh-metadata unsupported-journal/sidecar/mountpoint preservation. Existing
RPC-default and controller lifecycle cases pass too.

Convenience functions whose names/comments say `legacy` are not automatically
prior-version support: `mount::start`/`start_managed` now reach this same current
attachment. Remaining actual fallback code must still be removed. Startup
ordering also remains under audit because directory recovery precedes private-
journal opening; the new fresh-state refusal case does not qualify a pending
directory recovery beside unsupported private input.

## G17 handle/staging replacement ledger

[Current evidence](../../../test-evidence/wave-6/current-handle-content-checks.json)
records 235 Windows/241 Linux scoped ordinary passes and two explicit actual-
kernel parents/platform in one final source cohort. Three full broader targets
remain diagnostic at 34 pass/42 fail per platform; their exact failed images
and logs remain preserved and applicable obligations still need replacements.

| Removed expectation/route | Current coverage and remaining scope |
|---|---|
| Whole-file first-write copy and staging installation | A first 4 KiB COW edit, Engine availability during preparation, bounded old-chunk reads, one new data object, bounded manifests, no staging copy and zero-truncate without old data reads. |
| Corrupt whole-file copy refusal | Current affected-chunk write/truncate refusal before acknowledgement; unchanged metadata/length/Saved/CAS/staging, unaffected reads, repair and exact subsequent flush. The discovered premature-dirty marker is fixed, including zero-byte writes and preservation of earlier accepted edits. |
| Discarding a copy prepared for a superseded reference | No writable-copy API remains. Current selection/owner rechecks are covered by `selected_open_rechecks_metadata_before_it_can_supersede_a_save_owner` and existing stale-handle/range ownership cases; no old copy route is recreated. |
| Whole-file spool-growth read progress | Complete receipt-verified chunks advance only the requested current range. Stall, total deadline, fence cancellation and resource limits independently release demand/slots. |
| Raw part reception while a handle waits under a whole-file reference | Exact typed reference/entity/range demand; unrelated Engine directory creation remains live, receipt-based delivery returns exact bytes, and a later absent chunk ends at the total deadline. |
| A cached whole-file handle survives unrelated activation | The current immutable selection Arc stays identical without reload or byte change; a changed content head replaces it once and returns exact newly imported typed bytes. |
| Mounted missing/download reads backed by raw whole-file IDs | Actual WinFsp/FUSE reads use typed Core fixtures and verified chunk delivery, preserving missing-content OS errors and a complete 6 MiB progressing read. |

`fetch_verified[_waiting]` remains only for the separately audited generic
object-ID preview/fetch route. It is not handle hydration. Current `Staged`
regular-file construction requires a descriptor; directory/control memory and
the standalone deferred memory/disk route remain distinct. Remove those old
deferred-save APIs/tables and other adapter fallbacks after preserving their
applicable obligations on the native path. Test fixture poison cleanup merely
stops/wakes its actors; production fatal-poison handling is unchanged.

## G18 Core ingress/projection replacement ledger

[Current evidence](../../../test-evidence/wave-6/descriptor-only-metadata-checks.json)
records 282 scoped ordinary passes/platform: eleven complete targets (281),
including all 45 feed cases, and one focused current-fixture parent. Both
explicit RPC/direct actual-kernel parents also pass/platform. This does not
qualify the whole harness-selftest suite or remaining consumer/API targets.

| Removed behavior | Current coverage/owner |
|---|---|
| A hash string can become a file-content register | Validated Direct/Manifest JSON objects at public commit, commit checking, bundle reception, inbox staging, feed precheck and transactional admission. Seven unsupported input shapes, including a descriptor-shaped tuple, cannot commit or partially stage a batch. |
| Nonlocal commit/bundle performs old byte work before validation | Scalar commit returns INVALID_CONTENT before raw object expansion; bundle refusal leaves bytes, sizes, inbound provenance, metadata/admission and inbox unchanged. The original nonlocal CONTENT_SIZE_UNKNOWN failure remains preserved. |
| A digest-matching string payload can become feed knowledge | Whole feed path rejects it with INVALID_CONTENT, without staging a root/event, peer knowledge or sender outbox ACK. Current metadata/convergence, pagination and idle/private-tail targets pass. |
| Projection setters/getters or codec preserve a whole-file string | No setter/getter string branch; the old member only encodes/decodes null until consumer removal. Non-null strings are refused even when content_ref also exists. Valid current encoding and metadata header/schema 3 stay unchanged. |
| Metadata size falls back to whole-file object_sizes | Validated descriptor logical length for regular files; existing directory/control/session handling is separate. Missing current manifests/data do not block size lookup. |
| Large fixtures manually create old serialized projection/head/object rows | Public typed Core commits in a metadata batch own root handoff and save generations. Fixture data lives outside SQL. Full oracle, exact typed bytes, root/outbox counts, creation-precondition rollback, sealed restart and receiver convergence pass. Core's valid conflicting metadata is still admitted. |

Remaining explicit consumers include `Entry.content` in status/folder-details,
preview/demand, `Event::objects`/object-size payloads and old object/scheduling
tables. The null field is a temporary shape, not a second file representation.
The obsolete standalone `Engine::create_deferred` still constructs an in-memory
string-valued entry and is outside the default native path; remove it with the
memory/disk/table/adapter fallback route rather than adding a serialization
bridge. The new fixture proof is one focused parent, not qualification of every
fixture clone, old artifact helper or older runtime target. Earlier failures
retain their own sources/counts; do not treat G17's 42 as a new-source total.

## G19 status and folder-details replacement ledger

[Current evidence](../../../test-evidence/wave-6/current-status-checks.json)
records 319 scoped ordinary passes/platform (fourteen complete targets plus one
focused fixture) and both explicit actual-kernel parents/platform. All sixteen
selected images/platform keep their hashes through proof. The complete ten-case
status target covers current references, local indexing and actual RPC behavior.

| Removed consumer | Current behavior and retained obligation |
|---|---|
| Entry.content/status whole-file catalog scan | Distinct typed current/shared descriptors; verified direct chunks and canonical indexed manifest adjacency; no file-byte reads/hashes. |
| Hash-only missing items and physical-byte totals | Entity/revision/reference/descriptor/path and missing/indexing/corrupt state, with logical byte coverage for repeated chunks and absent pages. |
| Private dedup presence implies shared completion | Readability and public receipts are independent; pages and chunks need the existing public provenance for received shared roots. |
| Old WANT/spools imply current transfer progress | Worker samples active repository reservations and live unsatisfied range waiters. Atomic current frames expose no unverified partial-spool progress. |
| Local verified index depends on a connected peer | Maintenance verifies one current byte job per tick when no structural job is pending; the no-peer worker parent reaches complete. |
| Folder-details scans content/conflicts under Engine | Secondary SQLite snapshot, short identity capture/recheck, stale-view refusal, retained review token/pagination and worker status claims. |

Uncached full summary cost and hidden in-place external corruption remain
separate concerns: the former needs scale/load measurement, the latter is found
by verified reads/scrubbers. This report does not qualify all status/UI targets,
preview/demand APIs, Event.objects, old transfer/catalog/scheduling tables,
standalone saves or remaining platform/release gates. The temporary null
Entry.content shape remains until all its consumers and constructors are removed.

## G20 preview and explicit-demand replacement ledger

[Current evidence](../../../test-evidence/wave-6/reference-consumer-checks.json)
records 94 scoped ordinary passes/platform: five complete targets and four exact
real-peer/adapter cases. This is not the full transport suite or a new kernel
qualification. All seven selected images/platform retain their hashes.

| Removed consumer/behavior | Current replacement and scope |
|---|---|
| GUI sends a descriptor object to hash-only preview-object | preview-content receives the event/entity registered reference; historical alternatives and current empty/direct/manifest prefix reads pass. |
| Preview assumes whole-file CAS availability | Bounded verified 64 KiB range and current demand, independent of unrelated missing chunks, with view/admission checks before delivery. |
| request-object writes the old object_requests/WANT path | Explicit at-most-1MiB range demand with owned lease/pin, joining, cancel, expiry, completion/view/grant/stop cleanup and no old queue row. |
| Waiting control previews can fill completion handlers | Two control byte calls per worker, global read slots before allocation and separate process-wide 16-request prefetch budget. Other sessions/workers retain metadata/read capacity in the new cases. |
| Generic JSON routing loses preview session deadline/cancel | Direct session dispatch preserves its absolute deadline, cancel predicate and large response body framing; unrelated exchanges remain usable. |

Explicit request priority is volatile across worker restart; durable metadata
roots still own background replication. A queued acknowledgement is not a file
save or availability claim. Generic local cat-object/fetch_verified, Event.objects,
old catalog/transfer/scheduling tables and standalone saves remain separately
audited work. The temporary Entry.content null field is not removed yet.

## G21 mounted mutation dispatch replacement ledger

[Current evidence](../../../test-evidence/wave-6/current-adapter-checks.json)
qualifies both mounted adapters with five complete targets, focused FUSE/range
coverage and eight actual-kernel parents/platform.

| Removed adapter route | Current owner and remaining scope |
|---|---|
| WinFsp deferred create/Engine write/truncate/basic/rename/flush fallback | Required current service, private byte/namespace/metadata coordinators, existing current save/cleanup/close. |
| FUSE Engine-held mutable dispatch and old setattr-size/mode helper | Threaded current create/write/truncate/basic/namespace operations; no target-settle fallback. |
| Buffer reads for ordinary file fallback | Current descriptor range provider; only virtual discovery reads a buffer. |
| Optional service selects old mutation behavior | Mounted callback refuses missing service; attachment already initializes it. |
| Old Linux buffer-only metadata fixtures | Typed Core producers/current service, exact byte checks, atomic unsupported ownership refusal and retained directory-incarnation cases. |
| Rename only works while a private source is already open | Temporary current metadata owner for a closed/readonly source, existing namespace executor/receipt, no byte hydration or publication; two missing-chunk regressions. |

Root/discovery/directory metadata cases are explicit and preserve filesystem
semantics. Shared savers still serve current lifecycle and must not be deleted
with obsolete standalone behavior. Engine Memory/Disk/deferred methods,
pending_saves, temporary Entry.content and generic artifact/old object transport
remain pending; this ledger does not claim their removal or full release closure.

## G22 canonical creation replacement ledger

[Current evidence](../../../test-evidence/wave-6/deferred-creation-checks.json)
includes the complete 26-test-function retirement ledger with exact replacement
source/case owners. Eleven full targets and focused current cases plus sixteen
actual-kernel parents/platform qualify this scoped creation change.

| Removed component/mechanism | Current contract and owner |
|---|---|
| create_deferred, first-save create, string-valued empty file and optional environment mode | Current NativeWrites creation commits typed empty content before ACK; no serialization/compatibility bridge. |
| pending_creates map, late NameHeld restoration, lookup/list merging | One canonical Store namespace; private COW size; current child exists before bytes and blocks rmdir. |
| New-name insertion by standalone stage recovery/publication | Requires an existing committed typed file; cannot create an uncommitted name. Remaining old stage/snapshot/table implementation is still pending removal. |
| Legacy collision/rename tests use uncommitted fake destinations | Current file/directory reservations, retained private writers, exact receipts and unchanged 200ms conflict/private-I/O bound. FULL create during another writer must wait; the old in-memory shortcut is retired. |
| Path-based rename borrows an existing COW session without lifetime ownership | Readonly operation handles pin both selected sessions/workspace through gates and commit; deterministic last-user-close/deadline test and real concurrent Linux replay. Initial failures/executables are preserved separately. |
| Old local spool/snapshot fault/queue/create test mechanisms | Named current COW/SaveExecutor/private-journal/namespace/lifecycle owners in the JSON ledger; historical failures preserved, aggregate retry/load gates remain open. |
| Mounted local tests silently pass when unavailable or read whole-file hashes | Eight explicitly ignored real-kernel parents, required mount, typed provider byte oracles. All eight ran/platform; original 300-file and 8x150-file/5s bounds remain. |

No complete standalone/staging/table/shared-saver removal, full transport/library/
worker/repository, matched build, outage/storage/aggregate load/soak or release
closure is claimed. Existing earlier source-specific failures stay unwaived.

## CCR-WP6.G-23 startup ordering

The G16 audit's pending-directory/unsupported-private-journal gap is closed in
[the current scoped proof](../../../test-evidence/wave-6/journal-recovery-checks.json).
NativeWrites opens/validates private format before directory recovery commits
metadata. Both RPC/direct refusal preserve the exact pending intent, directory,
projection and events; a fresh current test journal recovers it afterward.
The existing directory-Cleanup recovery case remains, renamed to describe
recovery before accepting requests. Two complete targets pass 86 ordinary
cases/platform, plus both actual startup/mount parents/platform. This does not
claim complete corrupt-Saved recovery, standalone removal or release closure.


## Re-audit before the interlude exits

Current source audit, 2026-10-23: `Store::export_shared_objects` and the worker's
`runtime/export.rs` still enumerate `Event::objects`, which selects only string
content values. Current valid events carry descriptors, so that enumeration
cannot include their chunks or manifest pages. Replace both exports with pinned
typed reachability and verified bounded object reads before deleting that helper;
do not retain or recreate string file events as an export bridge. This is a
source finding, not a newly executed export test result. Checkpoint snapshot
blobs still use generic local CAS; distinguish those local artifacts from the
old file transport when removing its tables and schedulers.

Trace startup → worker → Core → adapter and authenticated peer → worker RPC →
admission/provider for every ordinary operation, recovery root and internal
artifact. Repeat searches for the removed tags/RPCs, untyped file-content
producers, old upgrade markers and optional fallback selection. Every retained
generic component must have its current caller and test owner recorded here.
Do not count historical evidence, a missing environment or an unimplemented
replacement as current-path qualification.
