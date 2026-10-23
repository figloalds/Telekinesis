# Wave 6 implementation record

2026-10-22 scope review: [personal-use deployment and DVFS authority
boundaries](../AUTHORITY_SCOPE.md) now govern current and next-wave planning.
Environment-provided protection must not be duplicated. Users/ACLs and
public-service hardening are out of scope; simple hashed pairing, route/reference
scope and filesystem correctness remain. Historical custom HMAC/enrollment and
security-suite entries below are implementation evidence, not obligations to
reproduce those mechanisms. Plans, contracts, architecture, validation and
configuration annotations reflect the decision. Documentation only; no runtime
removal, qualification or closure of directory/timestamp/demand failures claimed.

2026-10-22. Goal: implement `WAVE_6_PLAN.md`. Work is sequential on the existing
checkout; no merge, push, deployment, live-store migration or production activation
has been performed. The goal is **not complete**.

## Current phase — WP6.G greenfield interlude (2026-10-22)

User decision: this is greenfield; support for anything prior is unnecessary.
The authoritative [Wave 6 plan](../WAVE_6_PLAN.md#31-wp6g--greenfield-interlude-in-progress)
now inserts WP6.G immediately after the completed CCR-WP6.4-30 evidence and
before further implementation. Status: **in progress**. The initial scope
revision was documentation only. CCR-WP6.G-2 below starts implementation with
current-only private journals. CCR-WP6.G-3 consolidates and qualifies Core
metadata; CCR-WP6.G-4 consolidates and qualifies the separate current catalog.
Other runtime removals and default-path changes
remain pending in the [inventory](wp6-greenfield-inventory.md). No production
activation, live-store migration or deployment is claimed.

User steering, 2026-10-22: keep transport security to standard TLS/WSS or the
enclosing VPN. Do not expand this work into custom cryptography, additional
security negotiation or a second transport-security layer. The plan and current
contract record that boundary.

CCR-WP6.G-8 applies that simplification. Its scoped source
uses ordinary application credentials over TLS/WSS or the enclosing VPN, removes
the custom HMAC transcript/server proof/nonces/scheme negotiation and enrollment
wrapping, and replaces the HMAC utility with the existing credential comparison.
Pairing, catalog credentials, repository permissions, revocation and connection
bounds remain application behavior. Four selected current frame fixtures and
handshake/security cases are rebased; old transcript/replay/old-acceptor
expectations are removed as superseded scope. Standard WSS tests remain.
The 429-input source is
`2b404c6d11a8a13742df8ba588ac6bc7ab09e6c85de111915e87156bbea33908`.
Strict release Clippy passes on both platforms. Scoped qualification passes
272 ordinary cases across six Windows targets and 335 across seven Linux
targets, plus two actual peer/kernel parents per platform. The Windows broader
`content_transport` target separately passed 53 and failed one: verified bytes
and receipt recovery succeeded, but a readiness episode incremented a failure
counter. That failure is unwaived and its cause remains under investigation.
The six passing Windows targets were run in stages after the failed diagnostic;
the report retains each invocation and does not count those 53 cases as a
successful target. All eight selected images/platform were unchanged through
proof. Evidence: [basic-transport-auth-checks.json](../../../test-evidence/wave-6/basic-transport-auth-checks.json).
The final source/log archive is `target/wave6/basic-transport-auth-revocation-fixed.zip`
(2,694,091 bytes, SHA-256 `02ae40e72fa397a2760e783f2770dd1d2f602e6466998c591bb8b821ebbea640`).
The preceding
`ff41d160` candidate exposed the retained revocation-ordering regression on both
platforms: a handshake paused before unpair/re-enrollment received AUTH_OK but
missed the required PEER_REVOKED GOAWAY. Both tests exited 101. Exact source,
logs, both failed security harnesses and both CLI images are preserved in
`target/wave6/basic-transport-auth-revocation-before-fix.zip` (39,800,787 bytes,
SHA-256 `2dbecf397fcf127a5c8014ced44a4a2f40fc5826ce86bf6493b545d4fa28cd2e`).
The correction captures the existing revocation epoch when HELLO is admitted;
it adds no channel protection or authority system. Development
checkpoints preserve the removed enrollment-field compile error, the obsolete
transcript helper and the three unused test bindings in `target/wave6/basic-
transport-auth-{candidate,check,tests}.zip`. The completed G7 evidence below
belongs to its explicit earlier source. The Windows receipt failure's exact
source, failed harness and CLI are in `target/wave6/basic-auth-windows-receipt-readiness-failure.zip`
(24,061,808 bytes, SHA-256 `e6f9eb16316b0d039164a9de6905c5f3a77d73493f50a87d5aba1ee17ba8f0d7`).

CCR-WP6.G-9 reconciles directory removal after a lost observation.
An actual optimized-library probe on source `2b404c6d` returned CONTENT_DEADLINE
after committing removal, then STALE_CONTENT_DIRECTORY on a retained handle's
next basic update. Its exit 101, exact source, probe and linked library are
preserved in `target/wave6/directory-observation-before-fix.zip` (24,922,945 bytes,
SHA-256 `5b6479b4372a31601bcbc650969e439bbdade185f6126f7f4d5713c373301804`).
The 429-input production correction is
`8dcd56d27417bdc09af3757ad9ec49bf48544dfb06afa76f72cdd0cb3ac6980a`.
It reuses the existing exact removal/replacement receipt and observation path
to advance only matching retained incarnations; it refuses a later restoration.
New runtime regressions exercise remove, Cleanup and replacement after a
post-commit deadline. Strict release Clippy passes on both systems; 130 namespace
and native-write ordinary cases pass per platform on this source. The unchanged
standalone reproduction now exits 0 with `retained_handle_basic=Ok(true)`.
A bounded test diagnostic initially unwrapped the scheduler view before startup
publication. That fixture failure (53 transport passes, one failure) and its
actual Linux harness/CLI are preserved in
`target/wave6/directory-observation-diagnostic-before-fix.zip` (16,799,325 bytes,
SHA-256 `66b713c0224ebb7cb7409533d6781d73c1a49297ca92cc64f62303d5563231bc`).
The test-only startup guard selects final source
`ea7be9f172eb97572a990296761f8835ae3338fe95cbdf914d6852f4b8a08b17`;
only `tests/content_transport.rs` differs. Its 54 transfer cases pass on both
platforms. Total scoped coverage is **184 ordinary cases/platform**, in stages,
with 21 Windows/20 Linux default ignores and no explicit kernel parents in G9.
The two unchanged namespace/runtime harnesses and CLI per platform were checked
again after the transfer-only rebuild. Evidence:
[directory-observation-checks.json](../../../test-evidence/wave-6/directory-observation-checks.json).
Source/log archives are `target/wave6/directory-observation-current.zip`
(2,670,404 bytes, SHA-256 `c223012cccb650e214e786459d30bcdd9150aec4b0692b75a94ebf586909ff07`)
and `target/wave6/directory-observation-diagnostic-fixed.zip` (2,663,597 bytes,
SHA-256 `ef0082762c8f59a9f476f8bdf593cc6ea15711487c110cafee24898eb257c229`).
These passing reruns do not explain or waive G8's original Windows receipt
counter failure. Read-only directory enumeration after a lost observation,
full remote ordering and all remaining Wave 6 gates remain open.

Latest scoped qualification: CCR-WP6.G-10 extends the same receipt observation to retained
directory enumeration. An optimized-library probe on G9's final source returned
`retained_enumeration=Err(STALE_VIEW)` while its next basic mutation succeeded.
Exact source, failing probe, linked library and logs are preserved in
`target/wave6/directory-enumeration-before-fix.zip` (24,951,383 bytes, SHA-256
`79d476ca235437f96b10cab2ac383b342a8c4998a472f092dce68a97fd44c98a`).
The qualified correction has 429 inputs, SHA-256
`6c82fa25533e7fb1b76bf6cb9a57b5fa130263232a53ec05e28ff2389413bdd0`.
Enumeration checks the existing immutable removal receipt when the existence
head differs, reuses normal handle/view observation and checks the current head
again. This is bounded metadata work, with no new durability commit or byte
work. The three post-deadline scenarios now enumerate both retained handles
before any basic mutation or Cleanup retry; the restored-handle test still
requires refusal. Strict release Clippy passes on both systems. Each platform
passes **78 ordinary native-write cases and two actual directory kernel parents**;
13 Windows/12 Linux helpers/opt-ins remain ignored in the default target.
Both selected harness/CLI images per platform remain unchanged through proof.
The exact unchanged standalone reproduction now returns
`retained_enumeration=Ok(true)` and `retained_handle_basic=Ok(true)`, exit 0.
The five WinFsp SDK/driver file hashes and loaded driver identity were checked
unchanged before and after the actual mounts. No driver installation or reboot
occurred. Evidence:
[directory-enumeration-checks.json](../../../test-evidence/wave-6/directory-enumeration-checks.json).
The source/log archive is `target/wave6/directory-enumeration-current.zip`
(2,663,601 bytes, SHA-256 `419a741cccbc20fce2655140d8385e4ad47fdfecf334404f440fcb8738837f4c`).
This closes the scoped owned-removal enumeration defect, not full remote or
concurrent-parent lifetimes. The older receipt-counter, Windows timestamp,
demand reliability, build and remaining Wave 6 gates stay open.

CCR-WP6.G-11 introduced a failed default native worker candidate. The
430-input candidate is `cea8b6e5bc46f83f3d197576b609b00beaac41a8e503e6268078dc52590cb7a3`.
RPC startup installs/recovers the current write service before binding its
listener, reusing an already installed service rather than creating a second
pool. The production `fixture:content_v1` gates in metadata admission, native
saves, installation and worker capability reporting are removed. Stop cancels
save admissions after dirty-state retention; process teardown drains the same
bounded executor outside Engine and records a drain failure rather than claiming
it succeeded. New regressions exercise marker-free namespace/byte operations,
force-stop Saved recovery, unsupported-journal startup refusal and an explicit
actual WinFsp/FUSE default-worker mount. Strict release Clippy and all eight
selected release harnesses plus CLI build on both platforms. The three new
ordinary parents pass on both systems. The actual default-worker FUSE parent
passes; Windows reached graceful stop and the new fixture wrongly unwrapped
the API's permitted transient WORKER_BUSY. No Windows kernel pass is claimed.

The candidate is **failed/unqualified**. Diagnostics on both systems pass
metadata 8 and namespace 52. The native-write target passes 77/fails one obsolete
activation assertion; saves pass 59/fail one obsolete activation assertion.
Transfer passes 45/fails nine because its fixture explicitly installs a second
service after default startup. The Linux admission target passes 6/fails two
obsolete activation assertions; its direct retired-gate case also fails on
Windows. Required future-format refusal, FULL-WAL/outer-transaction checks,
ownership, recovery and routing coverage must be retained when rebasing these
fixtures. No relevant failing scenario is retired merely to pass the suite.

Both lifecycle targets pass 19/fail six. These expose incomplete integration,
not merely stale expectations: accept-fatal native retention reports
`CONTENT_SAVE_REQUIRES_OFFLOCK_RETAIN`, and poison/fatal paths still use the
old under-Engine retention. Startup with legacy pending sessions also fails
with CONTENT_RESOURCE_LIMIT. Current producers and runtime APIs must be
consolidated; do not add an old-store migration to work around that gap.
Typed-byte helpers and pending-state assertions need current Saved/descriptor
coverage. Those results describe the frozen G11 source above; they are not
corrected retroactively.

All failed source/logs and actual failed harnesses/CLI images (six Linux,
seven Windows) are preserved and read back in
`target/wave6/native-default-candidate-failures.zip` (76,575,574 bytes, SHA-256
`84be74cad8d8804a7040b20def2e4e6b4fbb5869dfafddac47b56893ff5bf24f`).
SDK/loaded-driver hashes remain unchanged. Structured candidate evidence:
[native-default-candidate-checks.json](../../../test-evidence/wave-6/native-default-candidate-checks.json).
G11 established the need for off-lock native fatal checkpoints, preserved
rollback/no-commit invariants, one fatal owner, service reuse and bounded
WORKER_BUSY retry. G12 below addresses those specific defects.

CCR-WP6.G-12 closes the scoped fatal-retention/default-worker integration
failures. The production correction is the 431-input source
`0852e93c2964b75d5ee54e313ab265f035b273a42aca76eb2589ce169a92ee70`.
Poison and accept-error exits share one fatal owner. Other normal Engine users
wait for that exit; the owner rolls back interrupted metadata, captures native
files and releases Engine before FULL private checkpoints. It then reacquires
Engine for retained-state bookkeeping and fatal reporting. Fatal checkpoints
serialize SaveFile state without taking an adapter gate that can belong to a
caller waiting for Engine. Existing rollback/replacement coverage is retained.
The new exact regression holds that gate, stalls a checkpoint and verifies
rollback, a free Engine and recovery of exact Saved bytes without publication.

NativeWrites installation reuses the same running service and refuses a stopped
executor. Marker-free admission/publication coverage retains future-format
refusal, inbox retry without quarantine, outer-transaction refusal and FULL-WAL
requirements. Worker restart fixtures now seed current Saved checkpoints and
read typed descriptors; the reachable control-import producer retains explicit
old-path coverage. Native recovery reports real progress before listener bind.

Strict all-test release Clippy, all nine selected harnesses plus CLI builds,
288 ordinary tests across eight targets and the new exact fatal-retention unit
pass on each platform. The ordinary lifecycle cases include real mounted
accept-fatal and poison exits, exact-byte restart recovery and rollback tests.
All three actual Linux kernel parents and both Windows peer kernel parents
pass at this source. The Windows default-kernel fixture still failed gracefully
with BUSY_VIEW: two delayed kernel Close callbacks had not drained its contexts.
Its exact source/logs and actual failed Windows harness/CLI are preserved and
read back in `target/wave6/native-exit-current-failures.zip` (22,888,117 bytes,
SHA-256 `071c38defc7043982c57e98cc5e45edd30ac4fb3ae1d5b7e6b2e8dd0feeec885`).
Structured intermediate evidence: [native-exit-candidate-checks.json](../../../test-evidence/wave-6/native-exit-candidate-checks.json).

The final 431-input source is
`b16fb231f0cecce6a576cc738223fa1cfae321533397586918450a99ae24ff90`.
Only `tests/native_default.rs` changed: the actual-kernel fixture observes
Engine handles becoming empty, with a five-second bound, before graceful stop.
It still retries only WORKER_BUSY; BUSY_VIEW and a real handle leak still fail.
The default-kernel parent now passes on both WinFsp and FUSE and reports drained
handles plus successful graceful stop. Strict all-test Clippy and all selected
builds pass again; the changed native_default target's three ordinary cases and
actual kernel parent were rerun on both platforms. The other nine images per
platform (eight harnesses including the exact unit, plus CLI) are byte-identical
to the preceding successful executions. The report records both source cohorts
and retains only identical-image results, without double counting repeated tests.
Coverage is 288 ordinary + one exact unit + three actual kernel parents per
platform; this is scoped regression evidence, not full Wave 6 qualification.

SDK and running-driver hashes are unchanged. Final source/proof capsule:
`target/wave6/native-close-observation.zip` (2,741,950 bytes, SHA-256
`cfebf273fb84b96fd26493f85b1461f57f3471b39fe8d286cf059a49cdae77e3`).
Final evidence: [native-exit-retention-checks.json](../../../test-evidence/wave-6/native-exit-retention-checks.json).
The initial 431-input lint candidate
`18371872b0cd70a68204ae797f583ad094ceea4ddf9a90c61093df47d42b3db5`
failed the collapsible-if lint on both systems. Its source/log capsule is
`target/wave6/native-exit-retention.zip` (2,646,819 bytes, SHA-256
`8ae5b5fbd1f935691cbe4e882cef622f770ed6ff9be7d08221efb787668f231f`).
No failed executable was produced by those lint checks.

G12 next step: consolidate direct Core create/write/control producers and
standalone Engine APIs onto typed descriptors/current saves. Preserve atomic control
receipts, explicit content/alive parents and mutation clocks. Do not put a
whole-file copy or manifest construction under the control transaction/Engine
lock, and do not add a legacy pending-save migration to bridge those callers.

The initial CCR-WP6.G-13 candidate consolidates Core file-content producers. Its
433-input candidate is
`b2e396866ff7ed24a2fe91199bcb7ebb4347a3e1da021606c2e258658379abb8`.
Core create now emits the canonical empty direct descriptor. Byte and explicit-
length stream writes construct current direct/manifest content, retain a pin
through publication and preserve content/alive parents and mutation clocks.
`write_object_with_base_and_times` and its bare-hash publisher are removed.
The remaining standalone save snapshot producer also prepares typed chunks;
its whole-file object installer, corrupt-copy bridge and duplicate asynchronous
availability-note actor are removed. Whole-file staging and standalone Engine
APIs themselves still require removal; no legacy promotion path was added.

The import RPC computes its receipt fingerprint and prepares immutable content
outside Engine and the control transaction. It captures the target/parent and
selected branch/generation, rechecks them before publication and retains the
existing atomic metadata/admission/receipt transaction. A direct unprepared
Engine import refuses rather than running the byte work under Engine. Tests
cover exact bounded stream consumption, typed empty/direct/manifest writes,
explicit causal parents/clocks, corrupt-chunk repair, prepared-store ownership,
metadata-only publication counters, off-lock RPC preparation, exact/reused-id
receipts, transaction rollback and stale incarnation refusal. The actual default
kernel parent now also imports through Core before mount, then opens, rewrites
and fsyncs that typed file through WinFsp/FUSE.

That candidate passes strict all-test Clippy and all selected builds. Its eleven
successful new/default cases and expanded actual default mount pass, as do the
two actual-peer kernel parents on each platform. The full selected ordinary
matrix is 296 pass / 57 fail per platform. Failed source/logs and fourteen actual
failed harness/CLI images are preserved and read back in
`target/wave6/core-producer-integration-failures.zip` (78,144,253 bytes, SHA-256
`b016ef7dd900fbd47bec3f8983d47e280ef10484505d2e2d5bffb59a513817fc`).
[core-producer-candidate-checks.json](../../../test-evidence/wave-6/core-producer-candidate-checks.json)
records each result and the required corrections; no full-matrix pass is claimed.

Preparatory sources/logs are preserved in `target/wave6/`:
`core-current-producers.zip` (2,649,330 bytes, SHA-256
`dac59225ad6a3b41271c78355796590d171241e173d7f78845fb4ce12a6bc07e`)
failed the newly unused old corrupt-copy helper lint;
`core-current-producers-clean.zip` (2,649,381 bytes, SHA-256
`2d4e12a87e25347ff724a240b80fe99c07f394fd916c106814fee57265571150`)
failed the new oracle fixture's mutable-borrow check. The corrected
`core-current-producers-checked.zip` (2,648,882 bytes, SHA-256
`39ae80bdc4152c9dae4355f055d752759714e7d4bc99365def86384532a3bdd3`)
and `core-current-publication.zip` (2,650,107 bytes, SHA-256
`dea3c47a241c602ece009dbc5955b1d796992b90040c06e9ac1389fa9d153cfc`)
pass strict Clippy and were superseded by off-lock receipt fingerprinting and
stronger RPC/actual-kernel coverage. No failed executable was produced by these
static-check attempts.

Current work: the G13 correction selects 433-input source
`2e154afd998b6e82acddeb8e790d6d33a6ae1f8a87bbe59b2079dacafbb44d3f`.
Prepared publication now requires FULL-or-stronger WAL and a usable connection,
while allowing the control receipt transaction and keeping its prepared pin
through outer commit. Byte/stream constructors refuse an existing metadata
transaction before reading. Current CAS installation and metadata crash hooks
replace the removed whole-file installer sites. Crash children arm them only
after startup/recovery so the observed exit belongs to the tested write.

Core integration fixtures now use typed events/reads throughout and current
native sessions for the long-lived writer. Metadata without bytes activates;
verified reads report missing/corrupt data. Private/public dedup coverage checks
the typed references and their distinct sharing scope, not inline file bytes in
a metadata bundle. The initial empty-root admission is explicitly checked and
publication counters use its saved cursor. Consumer replay/off-lock handoff
coverage also moves to actual cursors, including previously green tests that
would otherwise have tested the wrong initial row. The failed-rename/no-late-
retry case uses a genuine directory collision; typed replacement remains valid.

Strict all-test release Clippy, eleven selected harnesses and CLI build pass on
both platforms. Nine complete targets pass: Core-content 9, native-default 3,
Core 16, contracts 41, admission 8, metadata 8, mount writes 78, namespace 52 and
saves 60 (275 cases/platform). The broader ordinary matrix is 318 pass / 36 fail:
transfer passes 24/fails 30 and lifecycle passes 19/fails six on each platform.
Their still-required cancellation/credit/lease/reconnect and save/poison/gauge
scenarios must move to the current APIs before the old routes/tests are removed.
Test panics also show missing fixture-owned server teardown. These failures are
unwaived; no complete current-path qualification is claimed. Three actual kernel
parents pass per platform, including Core import followed by mounted native
rewrite/fsync, actual-peer range demand and offline COW fsync.

All source/logs and the six actual failed transfer/lifecycle/CLI images are
preserved and read back in `target/wave6/core-producer-durability-failures.zip`
(49,059,031 bytes, SHA-256
`6700f41ab468accd92698c67ca5519c01dbbf5e89ccef581781bddec83156335`).
[core-producer-durability-checks.json](../../../test-evidence/wave-6/core-producer-durability-checks.json)
contains the exact command/image/source cohort. SDK/loaded-driver hashes remain
unchanged; no deployment, live migration or driver change occurred.

The follow-up 433-input source is
`914c060d17e424bd0bb76cbb2b358e1283076dbfee96a87c48b2aae70f66c9da`.
Only `src/runtime/notify.rs` and `tests/core_content.rs` differ from that matrix.
The notification comparison now includes the typed descriptor: two different
same-length roots with identical clocks/basic info must emit a modification.
The new case exercises both full and incremental notifications from actual
committed metadata, and verifies no notification for an identical view. Strict
all-test release Clippy, both affected release harnesses and CLI builds pass on
both platforms. Current focused execution passes Core-content 10 and native-
default 3 ordinary cases/platform plus the actual default WinFsp/FUSE parent
with Core import, native rewrite/fsync, namespace operations, observed handle
drain and graceful stop. The broader matrix has not been rerun at this source;
its 36 failures remain unwaived and earlier producer successes are not relabelled
as current notification-source executions.

Final source/proof capsule: `target/wave6/core-current-notifications.zip`
(2,751,511 bytes, SHA-256
`e2ba231fb29a0543553d18f2ef755f431a962227f4b606c951872c9effa4d372`).
[core-current-producers-checks.json](../../../test-evidence/wave-6/core-current-producers-checks.json)
records the current 13-case/actual-kernel scope and the preceding producer cohort
explicitly. SDK/loaded-driver hashes remain unchanged. No release/deployment or
live migration is claimed.

Next: finish standalone Engine API/staging removal with current Saved/poison
rollback and failure/gauge coverage, remove remaining string decoding/consumers,
and consolidate transfer without retiring the applicable lifetime/load scenarios.
The selected producer/notification changes do not replace all remaining WP6.G,
cache/change-journal, build, retention, outage/durability and final release gates.

Default initialization and typed direct Core producers are parts of the
interlude. Standalone legacy Engine APIs, string descriptor consumers,
whole-file staging and duplicate transport still require removal
or consolidation with their current correctness coverage.
Their reachable paths are not treated as a completed single current path.
No release/deployment or live-store migration has occurred; the complete goal
and every unclosed original release gate remain open.

CCR-WP6.G-14 removes `ContentKind::Legacy` and its binary/JSON codec support,
the whole-file provider reader/converter, mutable and retained conversion caches,
and their provider-override branches. Current save capture now requires a typed
descriptor instead of synthesizing a whole-file reference from `object_size`.
Direct and manifest encodings, digest domains and current journal/schema headers
remain unchanged. The old independent binary vector is only a rejection fixture;
there is no migration or conversion on open.

The first source (`e6fa71a8`) failed Clippy because a nested creation module still
used its parent's removed import. Exact source/logs are preserved and read back in
`target/wave6/current-descriptor-removal.zip` (2,649,537 bytes, SHA-256
`840b7177c85580b2934ec9bc8ecb3f615fb93121e401f08e3c7cfd986d3e862e`).
The corrected 433-input source is
`48e3a632b299d25759983fb9bf7dfedcbeb9633b36efea872f51cba060d8a40c`.
Strict all-test release Clippy passes both platforms. Its complete initial
16-target runtime matrix passes 393 cases and fails two per platform; three
actual kernel parents pass separately per platform.

Replacement tests retain current Direct/Manifest full replacement with absent
bases, bounded repeated and parallel preparation, corrupt affected chunks,
obsolete preparations waiting behind occupied buffers and retained recovery with
unhydrated unchanged chunks. The old transfer fixture switch now uses ordinary
typed publication; the old whole-file protocol race tests still require transfer
consolidation and are not declared passing by this fixture change.

Initial runtime checks on both platforms exposed two current-producer fixture
assumptions: the receipt test counted the new initial empty root as a retry of
the tested root, and an old read test expected a legacy bypass at a zero-second
deadline. Their source, logs, both actual failed harnesses and CLI/platform were
preserved and read back before correction in
`target/wave6/current-descriptor-checked-failures.zip` (39,398,417 bytes, SHA-256
`d9bcf37f1e29d681650bfae086e8e5ee11678cf12a4335c305ef7de0c1082ed2`).

The final 433-input source is
`32e4cbdf5932b8411eb2959ce536e608be909f3cce63cdc70b0f322366fd54a4`.
Only `tests/content_objects.rs` and `tests/content_mount_reads.rs` differ. The
receipt fixture verifies canonical empty candidates and asserts the tested root
is the only remaining job before fault injection; its original recovery and
disclosure assertions remain. Typed empty reads now assert a zero deadline is
an error and an adequate deadline returns an empty result, retaining the stale
handle checks. Strict all-test release Clippy, affected harness/CLI builds and
36 corrected cases pass on each platform.

Combined qualification is 395 cases/platform across 16 targets, including eight
internal write/retention cases: 359 in fourteen complete successful initial-source
targets and 36 in the corrected final-source targets. The unaffected harnesses
were verified byte-identical after final proof; earlier ordinary executions are
not relabelled as executions of the final source. The Linux CLI hash changed
after the final build. All three actual kernel parents were therefore repeated
with final selected CLI images on both platforms and passed. The initial and
final kernel executions are recorded separately, not counted as six unique cases.
SDK/loaded-driver hashes remain unchanged.

Evidence: [current-content-codec-checks.json](../../../test-evidence/wave-6/current-content-codec-checks.json)
(233,122 bytes). Final source/proof archive
`target/wave6/current-content-codec.zip` (2,806,432 bytes, SHA-256
`db97b205540fb06698acbc4b8fa167fb9eb8a3374116dfa42b2ef94aacb3772b`).
Bare string metadata/projection/status/preview consumers, standalone staging and
old whole-file RPC/schedulers remain pending. This tranche does not close WP6.G
or the complete Wave 6 goal.

CCR-WP6.G-15 consolidates worker lifecycle regressions onto
current native saves. The heartbeat previously counted only the old metadata
`pending_saves` table, so durable native Saved checkpoints were missing from its
gauge. The current journal now keeps a commit-owned atomic count from its existing
pending index. Checkpoint/adoption transactions calculate the resulting count
before COMMIT and publish it only after success; heartbeat reads require neither
Engine nor a private-journal lock. The old counter remains only for the still-
pending standalone route.

The unused standalone `Engine::persist_after_poison` method is removed. Its
rollback/connection-replacement obligations move to the real off-lock fatal
helper's unit coverage, alongside the existing actual process poison-exit case.
Force stop/restart, gauges under blocked Engine/private writers, publication
writer wait, failed metadata publication and deferred COMMIT followed by failed
private retention now use current native create/read/write/flush and Saved
records. The in-process fixture owns RPC shutdown and waits for server release.
The executor's group/adoption tests also check durable count transitions.

The initial `f2c582a9` source failed the new test's borrowed-guard lifetime check,
not production compilation. It is preserved in
`target/wave6/native-saved-lifecycle.zip` (2,651,880 bytes, SHA-256
`530c8220aae10f2ac19bed7a1d792a39e878b20d8300f01fe2351606d92f1007`).
The corrected `2729ecac` source passed strict all-test release Clippy on both
platforms and was superseded by stronger group/adoption count assertions; its
source/log archive is `target/wave6/native-saved-current.zip` (2,651,092 bytes,
SHA-256 `7f6642766498b032f143d608c30f02f9ce0bba9b9ed64ce00d7a384caf795fdf`).
The 433-input runtime candidate is
`881e7b9861eaf0d9825c200af39cf425173844eb33fa898cc01e2afa334fb2ff`.
Strict all-test release Clippy and selected release builds pass on both platforms.
The initial runtime matrix passes 304 cases and fails one per platform. All 24
worker lifecycle cases and both actual fatal-helper tests pass. The failure is
an old executor assertion expecting no content descriptor after file creation;
current creation commits canonical empty content. Its source, logs and actual
failed executor/CLI images/platform are preserved and read back in
`target/wave6/native-saved-qualified-failures.zip` (33,478,687 bytes, SHA-256
`2c207e51788f900e1502591f4d899d0f23d6589cdc26f2c75385655b98c58e61`).

Final 433-input source:
`fd5fb4cf2f46fe9dfe81ad18cff78886db30f18e0436f49e5bf2061d76a517f4`.
Only `tests/content_executor.rs` and `tests/worker_lifecycle.rs` differ. The
blocked-publication case now compares the entire committed empty Entry while
the secondary writer stalls; independent byte work/queue admission, timing,
publication count, exact bytes, store revision and oracle checks remain. Normal
fixture teardown errors now fail the test after shutdown/drain is attempted.
Strict all-test Clippy, affected builds and 46 final-source executor/lifecycle
cases pass per platform.

Combined scoped qualification is **305 cases/platform**: 295 across eight
integration targets and ten internal write/exit cases. Earlier-source 259 cases
retain their exact source and unchanged harness hashes; they are not relabelled
as executions of the final source. The two affected harnesses supply 46 final
cases. One explicit actual default-worker WinFsp/FUSE kernel parent runs again
with each final selected CLI and passes. Existing process/mount lifecycle cases
remain inside the 24-case lifecycle target count, not extra unique passes.
All five SDK/runtime hashes and loaded driver identity remain unchanged.

Evidence: [native-saved-lifecycle-checks.json](../../../test-evidence/wave-6/native-saved-lifecycle-checks.json)
(163,878 bytes). Final source/proof archive:
`target/wave6/native-lifecycle-current.zip` (2,781,082 bytes, SHA-256
`ebf2378d9fd3468a12b01c869e5920bb211bb3179fad654a016bf921f8ce2891`).
The unused standalone poison API is removed and its obligations have current
coverage; old deferred-save/staging/pending-save tables and adapter selections
still require removal. This is not complete standalone/single-path consolidation
or full Wave 6 qualification. Old transfer, status/string consumer, cache/build,
retention/outage/durability/load/platform and release gates remain open, as do
the separately recorded receipt/readiness, timestamp and scheduler failures.

CCR-WP6.G-16 addresses an entry-point gap found before further staging removal:
RPC startup initialized the current service, but public WinFsp/FUSE attachment
could still start a fresh mount without it. Both adapter attachment functions
now initialize/reuse NativeWrites after the path checks and before the OS mount
or mountpoint creation. The WinFsp C bridge's optional native-content argument
and capability/guard selection are removed. Existing active services are reused;
there is no second executor or new configuration switch.

New isolated actual-kernel coverage exercises direct attachment with no RPC
startup: typed Core import/rewrite, create/rename/truncate/fsync, idempotent
service reuse, recovery of an exact uncommitted current Saved revision, and
nonmutating unsupported-journal refusal before mountpoint creation. Controller
force-detach/dispatch-loss fixtures now count current Saved via vitals and retry
through the off-lock current coordinator rather than the old table/inline API.

The initial `766a8bd5` source passed strict all-test Clippy on both systems and
was superseded by moving the isolated test's cleanup guard before its negative
attachment attempt. Its source/log archive is
`target/wave6/mount-current-initialization.zip` (2,652,997 bytes, SHA-256
`6391774b297a2c75fae770a1ce39f8af481194da9456802f904d4f1f877a1b7b`).
The 433-input runtime candidate is
`4adac4fba330a5fc9a0e199998cb22eaeca60050a528e1c954bdfd522d77d759`.
Strict all-test release Clippy and selected release builds pass on both platforms.
The single runtime source cohort passes **209 Windows /215 Linux** ordinary cases
across six targets: native-default 3, mount-lifecycle 11/17, worker-lifecycle 24,
native writes 78, namespace 52 and contracts 41. Both explicit actual-kernel
parents pass per platform: existing RPC-default coverage and the new direct
attachment without RPC. The latter exercises two successful attachment cycles,
an exact private Saved recovery and a prior unsupported-input refusal; these
cycles are not extra unique parent passes. No runtime failure occurred in this
G16 cohort.

All seven chosen harness/CLI images per platform retain their hashes throughout
the ordinary and kernel executions. All five SDK/runtime files and loaded driver
identity remain unchanged. Evidence:
[mount-default-current-checks.json](../../../test-evidence/wave-6/mount-default-current-checks.json)
(136,537 bytes). Source/proof archive:
`target/wave6/mount-default-current.zip` (2,719,085 bytes, SHA-256
`3387bc604e779c09bdfaf94d01dbd7af889bba2aac1137cbcedc37ce4c6b279e`).

This closes the missing direct-attachment initialization prerequisite before
removing old byte callbacks. It does not remove the remaining Rust adapter
fallbacks, Engine/staging/reference or transport routes. A further startup-order
audit is required: NativeWrites currently performs existing directory recovery
before opening the private journal. The new fresh-metadata unsupported-input
parent proves private-file/sidecar/mountpoint preservation, not refusal before
unrelated pending directory recovery. The full Wave 6 goal and all unclosed
release gates remain active. No release, deployment, live-store migration or
SDK/driver installation/reboot occurred.

CCR-WP6.G-17 removes the immutable whole-file handle path: `Staged::Reference`,
object snapshots, cached verified whole-file handles, writable-copy conversion,
`prepare_content`/`prepare_writable` and their WinFsp/FUSE calls. Regular-file
bases require typed descriptors. Direct small files use the current provider;
directories/control data and remaining standalone memory/disk deferred saves
are distinct. Generic object-ID fetch stays for `preview-object`/`cat-object`
while those consumers are audited; it no longer hydrates file handles.

Rebased tests use current COW and receipt-verified chunks: a 4 KiB first edit
reads/replaces only its chunk, zero truncation reads no old data, corrupt chunks
refuse writes/truncation before acknowledgement, requested-range progress resets
the stall clock, and deadline/fence/capacity outcomes release waiters. A waiting
read leaves Engine available. Unrelated activation preserves the immutable
selection; a new content head replaces it once and serves the new exact bytes.
Actual mounted missing/progress reads retain their OS behavior. Fixtures own
their handles/feeders and stop/wake the in-process worker on teardown.

The first runtime candidate, `32f08151`, passes seven Linux focused cases and
fails the new corruption case: a refused write left Engine's early dirty marker
set although no byte sequence changed. Before correction, its failed harness,
CLI, exact source and all eight logs were sealed in
`target/wave6/current-range-current-failures.zip` (14,660,385 bytes, SHA-256
`646b75df2a07ce4c49fa5e0b013e740b7f3544f1f06db5ce0d087a9ab84567e5`).
Native mutation completion now restores only that operation's prior dirty marker
when its byte sequence is unchanged, guarded by the same file and edit identity.
The counter advances safely before marking; partial accepted writes still keep
their clocks/dirty state. The regression also covers a zero-byte write, unchanged
metadata/length/Saved/CAS/staging on refusal, unaffected reads, repair, an earlier
accepted edit surviving a later refused write, and exact final flushed bytes.

That production correction passes all eight focused cases/platform plus the
seven complete Linux targets and both explicit Linux kernel parents. The broader
worker diagnostic then aborts during fixture cleanup after an old raw-API test
poisons Engine. Source `55624bc3`, logs and all three failed Linux diagnostic
harnesses/CLI are preserved in `target/wave6/current-range-clean-failures.zip`
(26,009,035 bytes, SHA-256
`aaefbc1fe502698b06ae690c679ae2f90725197c9025babbd999e4fac8fcb59f`).
The final test-only cleanup change sets stopping and wakes the listener after
poison instead of invoking production poison handling from an unwinding Drop.
The original test failures remain failures. Two preceding static-only candidates
are separately preserved and indexed in the report.

Final 434-input source:
`cbb1315f4761c9e338d6ba83541eea6fe144386b31dfb166b0ec234a865a0ac8`.
Strict all-test release Clippy and selected release builds pass on both systems.
One final source/image cohort passes **235 Windows/241 Linux** ordinary scoped
cases: 227/233 across seven complete targets plus eight focused cases/platform.
Both explicit actual-kernel parents pass/platform, including direct attachment
without RPC and current Saved recovery. The two local-throughput mounted read
cases are included in the eight focused passes, not counted again as explicit
parents. All eleven selected harness/CLI hashes/platform stay fixed through
proof; SDK/runtime files and the loaded driver remain unchanged.

The full three broader targets remain diagnostic on each platform:
`local_fs_throughput` 11 pass/28 fail, `partial_content` 1/7 and
`worker_concurrency` 22/7. All 29 worker cases now complete without cleanup abort.
These 34 passes/42 failures are not added to the scoped totals or labeled passing
targets. Exact final failed harnesses/CLIs and raw logs are in the read-back-
verified final capsule, `target/wave6/current-range-qualified.zip`
(55,044,779 bytes, SHA-256
`3519c9fb8f1755a75e32e0901c28a32bd2b39d782be560cc84a1ae38d2cbc07d`).
Evidence: [current-handle-content-checks.json](../../../test-evidence/wave-6/current-handle-content-checks.json)
(227,569 bytes). Applicable old test obligations still require current-path
replacements; discarded APIs/formats are not restored for compatibility.

Memory/disk deferred saves, old tables/scheduling and other Rust adapter
fallbacks, string metadata/status/preview/demand consumers, duplicate transfer
and remaining startup-order/cache/build/lifetime/reclamation/durability/load/
release gates stay open. The earlier receipt/readiness, timestamp and scheduler
failures remain unwaived. The goal stays active. Channel protection remains
TLS/WSS or the enclosing VPN; no custom security layer, deployment, migration,
driver installation or reboot was added.

CCR-WP6.G-18 closes Core metadata's string-valued content path. Content
registers must be validated descriptor objects at public commit, semantic
checking, bundle reception, inbox staging, feed precheck and transactional root
admission. Scalars/arrays, including a tuple a struct decoder could otherwise
accept, refuse before old byte/provenance work. Set/get projection paths no
longer fall back to strings; the temporary `Entry.content` field encodes/decodes
only null. Current projection encoding, selected metadata header/schema 3,
private journal 7 and catalog 1 remain unchanged. Regular metadata size now uses
the descriptor length, without a whole-file `object_sizes` fallback in Engine
or FUSE. Missing bytes/structure do not block that lookup.

The initial static candidate `e93b4d3a` passed strict checks; inspection found
that older bundle reception could install bytes/provenance before staging's
validation. The earlier gate and new negative test were added before runtime
qualification. Its archive is `target/wave6/descriptor-only-ingress.zip`
(2,656,018 bytes, SHA-256
`29a1ed58c3918c0db34d69a99b80cde78807c2ab692a66db704d1d2ce7876af3`).
The first runtime candidate `c6e4e99a` passes 11 admission cases and fails one:
public `commit(event, false)` still materializes old object bytes before
`check_commit`, returning CONTENT_SIZE_UNKNOWN for a scalar instead of refusing
its representation. Exact failed Linux harness/CLI and source/logs were sealed
before the correction in `target/wave6/descriptor-only-current-failures.zip`
(14,696,982 bytes, SHA-256
`ab86cf14ba8f3e53d0e86b2516f630bb8b8a3c2b883f64221445354773e0b9cb`).
The public entry now validates first. New admission tests cover local/nonlocal
refusal, all-or-nothing staging, bundle refusal without bytes/provenance, a
digest-matching invalid feed payload without knowledge/ACK, projection coding
and metadata-only size lookup.

Production source `d56af6f0` passes ten complete targets (236/platform) and both
explicit actual-kernel parents/platform. The whole feed target passes 41/fails
four per platform because its bulk fixture still manually emits string-valued
events/projections. Its exact failed feed harnesses/CLIs and source/logs are in
`target/wave6/descriptor-validated-current-failures.zip` (34,206,697 bytes,
SHA-256 `5650bac284636383750540272d0d4d8d777c3c69ebf51158c9d5d3916358e658`).
The bulk fixture now commits current descriptor-valued creates through public
Core in one metadata batch. It does not fabricate projection/head rows or old
file-object catalogs; Core writes roots/outbox/save generations. Direct fixture
bytes are installed outside SQL. A static borrow-check failure in the new
self-test is separately preserved in `target/wave6/descriptor-current-fixtures.zip`
(2,656,619 bytes, SHA-256
`53c73ed80cc6395808e54c6641be8e7df3e96327eb83443385a4145151fba6bb`).

The focused self-test then exposed a fixture precondition error: low-level
Core commit correctly admits valid conflicting metadata rather than rejecting
a name collision. Source `c7565874` and both actual failed self-test harnesses/
CLIs were preserved in `target/wave6/descriptor-fixtures-current-failures.zip`
(33,208,605 bytes, SHA-256
`bc40c3dbc650d4b06abc705ebd3fc32bf285a0a6a873e8e44400e6c28fceb7b9`).
The fixture now checks the live parent/name through a cached parent path and
indexed lookup before committing; production conflict semantics are unchanged.
Its proof covers full re-projection, exact typed bytes, complete root/outbox
counts, no event/root leakage after batch rollback, sealed restart and later
local/remote metadata convergence.

Final 434-input source:
`1da44d235abe5cb85434e3e5a59b3dcd01d6bcb23decef4cb33d4f4f2e23d3dc`.
One final source/image cohort passes **282 scoped ordinary cases/platform**:
281 across eleven complete targets and the one focused fixture parent. The
entire 45-case feed target retains its original pagination, 5K-event and idle/
private-tail bounds. Both strict RPC/direct actual-kernel parents pass/platform.
The full harness-selftest target and whole repository runtime suite are not
claimed. Strict all-test release Clippy, selected builds and format/diff checks
pass on both platforms. All thirteen selected image hashes/platform and all
five SDK/runtime files plus loaded driver identity stay fixed through proof.

Evidence: [descriptor-only-metadata-checks.json](../../../test-evidence/wave-6/descriptor-only-metadata-checks.json)
(168,246 bytes). Read-back-verified final source/proof archive:
`target/wave6/descriptor-current-qualified.zip` (2,743,904 bytes, SHA-256
`7d979c6f17d8c4ec229c1bbee9f44025d4d1091d942f59ebecce13280543ddfb`).
The remaining null-field status/folder-details/preview/demand consumers,
Event.objects/object_sizes, old standalone memory/disk saves/tables/fallbacks,
generic artifacts/transfer and all startup/lifetime/cache/build/reclamation/
durability/load/release gates remain open. The old standalone deferred-create
constructor is not a current supported projection producer and must be removed,
not given a string compatibility bridge. Earlier G17 42-failure counts belong
to that prior cohort; earlier receipt/readiness, timestamp and scheduler failures
remain unwaived. The full goal stays active. No transport-security layer,
deployment, live-store migration, driver installation or reboot was added.

CCR-WP6.G-19 replaces the status and folder-details whole-file consumers. The
current scan counts distinct active descriptors and granted shared historical
descriptors, follows verified typed jobs and canonical indexed adjacency, and
uses cached objects/manifests directory listings. It never reads/hashes file
bytes. Missing logical coverage includes repeated chunks and absent pages;
pending verification/expansion is indexing, and observed corruption stays
visible. Received shared completeness requires public receipts independently
of private dedup readability. The cache includes both listing generations,
foreign database changes and branch; transactional scans are never cached.

The worker samples current scheduler reservations and unsatisfied repository
range waiters, replacing old WANT/spool-derived activity. Actual folder-details
reads conflicts/paths/content on a secondary SQLite snapshot outside Engine,
then rechecks selected view identity. Review tokens, pagination and worker
status claims remain. Its deterministic delayed-snapshot race leaves Engine
available and refuses a checkout-stale reply.

The initial 6d86ef90 source fails strict lint on one unused test import on both
platforms; no runtime image was invoked. Source/log archive:
`target/wave6/typed-status-current.zip` (2,664,441 bytes, SHA-256
`7d9fdcee713599ff1a2b49d076939d7b5f649a0bb455d828da74508d332a0f95`).
The corrected 70ca207d candidate passes all-test strict lint and Linux selected
harness compilation. Inspection finds maintenance only expanded local manifests;
without a peer, present data could remain indexing indefinitely. Both ongoing
compile sessions were deliberately interrupted and terminal before editing.
No runtime image was invoked. Complete inspection source/log archive:
`target/wave6/typed-status-indexing-audit.zip` (2,669,323 bytes, SHA-256
`f4611f8f160fb62ad395f57dcddfcb024dbc9e7b87b2f9e1d177c838df1674b0`).
A preliminary archive of that source, captured before the interruption logs,
is historical only; it is not a final qualified cohort.

The 8afc21c8 runtime candidate passes nine status cases and fails one on both
systems because ordinary empty-file creation already initializes the provider's
manifest directory. Contracts pass 40/fail one on both: the new literal
CONTENT_DESCRIPTOR_MISSING was unclassified. The final source explicitly removes
the empty fixture directory before testing its absence and reuses the existing
Fatal MISSING_CONTENT_DESCRIPTOR contract. Linux's fourteen-target diagnostic is
316 pass/2 fail; Windows' four-target diagnostic is 76/2. Exact failed status and
contracts harnesses, CLIs and source/logs were sealed before either correction:
`target/wave6/typed-status-local-fixture-failures.zip` (41,647,458 bytes, SHA-256
`16e0a6755cd3f327cefc09c38fd2ad5e6fe7d6ffd9496406a73ee8a62782462f`).
Those counts belong to that candidate, not the final source.

Maintenance now verifies one bounded local data job per tick when no structural
page is pending, under the existing deadline and outside Engine. The no-peer
worker regression completes indexing without transfers. Ten status parents
also cover current direct/empty/metadata-only roots, nested repeated chunks and
missing tail page, derived adjacency/corruption repair, direct/manifest receipt
provenance, rollback/foreign verification and actual RPC identity/lock behavior.
The new scheduler parent verifies repository isolation, joined/stream counts,
satisfaction, cancellation and lease release. Normal status-fixture stop errors
fail rather than silently passing teardown.

Final 436-input source:
`60c97a231b4b83ac07bbb3b5432fc2d75b555dbc10412a58f62cd1cfef6df810`.
One final source/image cohort passes **319 scoped ordinary cases/platform**:
318 across fourteen complete targets, plus one focused current-fixture parent.
Both explicit actual-kernel parents pass/platform. All sixteen selected image
hashes/platform and five SDK/runtime files plus loaded driver identity remain
unchanged through proof. Strict all-test release Clippy, selected builds and
format/diff checks pass on Windows and Linux.

Evidence: [current-status-checks.json](../../../test-evidence/wave-6/current-status-checks.json).
Read-back-verified source/proof archive: `target/wave6/typed-status-final.zip`
(2,757,379 bytes, SHA-256 `07b7359e5b6d448f5a5832e169342730dcca466d8344b8140bbe79414ae886ff`).
This is a cached readiness observation, not a synchronous integrity scrub;
hidden same-length external rewrites are found by verified readers/scrubbers.
Uncached full summaries still walk current roots and reachable indexed structure;
large-store/build/load performance remains a gate. Preview/demand APIs,
Event.objects/old tables, standalone saves/adapter fallbacks, startup/lifetimes,
cache/change journal/build, retention/GC, outage/ENOSPC/durability/power-loss,
load/soak/platform and release qualification remain open. Earlier diagnostic
failures retain their own source scopes. No schema/header/version change,
security layer, release/deployment, live migration, driver install or reboot.

CCR-WP6.G-20 replaces the GUI/worker/Folder preview and explicit-request hash
consumers. The previous GUI sent descriptor JSON to preview-object's string
argument; preview-content now resolves the registered revision/entity reference.
It pins the immutable descriptor, verifies at most a 64 KiB prefix, and hydrates
only the requested shared range through current demand. Private locally present
bytes stay readable without publishing private missing data. Engine and SQL
transactions are not held across byte work/waiting; view and admission changes
are rechecked before delivery. Session cancellation/deadlines and large reply
body framing are preserved; JSON control retains the existing 15s bound.

Explicit request-content uses at-most-1MiB ranges, joins repeated scoped requests
and returns the owner's cancellation UUID. Current worker state owns its demand
lease/pin until completion, cancel, expiry, checkout, withdrawal or stop. It writes
no old object_requests/WANT/spool row and creates no per-request thread. Priority
is volatile in the worker incarnation with a fixed 24h lifetime; durable root
admission/background replication remains the restart owner. Queued replies are
not data-availability or file-save durability acknowledgements.

The 2deda9db static source passes Linux strict Clippy but fails the Windows GUI
bin's crate::core path; the callback must use the tkfs library namespace. Source
inspection also identifies a separate global prefetch reservation requirement.
Its source/log archive is target/wave6/reference-consumers.zip (2,673,910 bytes,
SHA-256 3c56938e7114949c7d551b54c332cf9609b07ecde839f2b60a3d2a0f0bb4dc83).
The 1476d792 static source passes strict all-test Clippy on both platforms; source
review then identifies long previews sharing the loopback pool with the byte
completion calls they await. Its source/log archive is
target/wave6/reference-consumers-current.zip (2,674,331 bytes, SHA-256
f223dec2a2614092adb58726dc4dee76b6fb59f37b8e4a44a1a2e277e567848e).
Neither static candidate invokes a runtime image.

The final implementation admits at most two control byte calls per worker
before Engine wait/provider work and takes global read slots before allocation.
Sixteen process-wide explicit prefetches have their own slots and release call
slots after acknowledgement. Expired, completed and superseded owners release
only their own demand/pin; stop clears the owned map. No channel security,
identity/ACL scheme, protocol negotiation or schema/header change was added.

Final 438-input source: 58ddc209a89bd1301f28f936d6c36a8bb5a5926b4e2c2caf297d952997af943e.
One final source/image cohort passes **94 scoped ordinary cases/platform**:
90 across complete content_inspection (12), content_status (10), content_objects
(18), content_scheduler (9) and contracts (41), plus four exact transport cases.
Two new actual paired-peer/loopback cases fetch preview and explicit prefetch
while an unrelated server chunk is absent. Existing bounded typed-frame and
adapter range cases pass; the adapter's original 250ms admission bound stays.
Tests cover historical alternatives, empty/direct/binary/manifest prefixes,
receipt delivery, Engine availability, independent cancel/withdrawal, body
framing/deadlines, queue joining/owners, cross-worker budgets, metadata status
under simultaneous previews, checkout, shutdown/pins, SQL contention and private
missing data. No runtime failure occurs in this final cohort. Strict all-test
release Clippy, selected builds/CLI and format/diff checks pass both platforms.
All seven chosen image hashes/platform remain fixed through proof.

Evidence: [reference-consumer-checks.json](../../../test-evidence/wave-6/reference-consumer-checks.json).
Source/proof archive: target/wave6/reference-consumers-owned.zip (2,737,680 bytes, SHA-256
e6fe2d8fca32079cb0593ef126901846fa8fc2a81c253311ebb5bf01fe48d20a). This is not a complete content_transport, repository, new
kernel mount, performance or release qualification. Earlier G19 kernel results
belong to their earlier source. Generic artifact fetching, Event.objects/old
tables, duplicate transfer/scheduling, standalone memory/disk execution and all
remaining Wave 6 startup/lifetime/cache/change-journal/build/retention/outage/
durability/load/platform/release gates remain open. Earlier diagnostic failures
retain their own sources and unwaived status. No deployment, live migration,
driver/SDK install or reboot occurred. TLS/WSS or the enclosing VPN remains the
transport-security boundary. The full goal remains active.

Preceding scoped qualification: CCR-WP6.G-5 native file unlink, durable
per-handle dispositions, cancellation and startup replay after persistent Windows
Cleanup failure. The corrected source passes 512 Windows/520 Linux ordinary cases
and seven actual kernel parents per platform. Complete directory/lifetime work,
default typed execution and the remaining inventory are still pending.

Preceding scoped qualification: CCR-WP6.G-6 directory creation outside Engine and
the native WinFsp operation-guard replacement. Its 422-input source passes
522 Windows/530 Linux ordinary cases and nine actual kernel parents per platform.
Directory rename/delete/basic metadata and durable directory dispositions remain
pending. There is no byte-format or SQL schema change in this stage. The separate
current-image Windows timestamp probe still fails and remains a release gate.

Preceding scoped qualification: CCR-WP6.G-7 directory metadata and durable
dispositions. Fresh metadata layout 3 adds only `directory_delete_intents` and
its entity/request index (63 tables, 34 triggers, 17 explicit indexes). It keeps
no layout-2 reader or migration. Exact directory captures and metadata receipts
do not create byte-save owners. The adapters now route native directory basic,
rename and removal through pooled FULL writers outside Engine; per-handle
existence heads fence restored incarnations. The current 429-input source passes
539 Windows/548 Linux ordinary cases across 12 targets/platform and eleven actual
kernel parents/platform; full Wave 6 qualification remains open. Evidence:
[native-directory-metadata-checks.json](../../../test-evidence/wave-6/native-directory-metadata-checks.json).
The development checkpoints below preserve failures before that scoped result.
The first Core candidate (425 inputs,
`ac8bda36637b1a720cb199bd0ed87ade376642eb42e320fc0d19f2eab7371787`)
stopped at strict Clippy's boolean simplification on both platforms; exact source
and logs are preserved in `target/wave6/directory-changes-core-candidate.zip`
(2,634,687 bytes, SHA-256
`14837ccb6c1e3126c60b831aee7a82f04d59c3e7e5504a8937ddf4c6c53dcb26`).
The integrated adapter candidate has 428 inputs, SHA-256
`cdcf81095a9eaf80ff127b10621171961b8d4313a44c406f4316a9e4cc24340a`.
Its compilation and qualification are ongoing. No preceding passing image is
substituted for this source, and the Windows time-policy failure remains open.

On that integrated candidate, strict Clippy passed on Windows and Linux. Linux
release harnesses and CLI built; the diagnostic namespace suite passed 48 cases
and runtime passed 76. Format tests passed five and failed two because their
fresh-store assertions still expected layout 2. A real Linux directory mount
parent failed with `ENOENT` after `retained_entry` read synthetic ROOT as a stored
entity. A separately linked current-rLib probe confirmed that a trigger changing
generation after receipt insertion was incorrectly acknowledged (`0 -> 1`, write
time 101 persisted); its assertion exited 101. That probe's earlier missing-
workspace preparation failure is retained separately and is not counted as
evidence of the scope defect. Exact source, logs, 13 Linux harness/CLI images,
linked probe and rLib are preserved in
`target/wave6/directory-changes-adapters-candidate-linux-failures.zip`
(87,564,969 bytes, SHA-256
`354de85d77a8c14ecaa001c8903218950c1bae421a49aa7e98c32b7d09c74fad`).
All members were read back. Windows compilation is still in progress; its live
build logs were explicitly excluded from that immutable Linux failure capsule.

That Windows run subsequently completed its release harness/CLI build and also
passed 48 namespace and 76 runtime cases. Its format suite reproduced the same
two stale-version assertions (five passed). Exact Windows source/logs and all
13 images are in
`target/wave6/directory-changes-adapters-candidate-windows-failures.zip`
(61,823,572 bytes, SHA-256
`3f3b22fe21efebb3df3b3c35cff1ab6a18f34440a1b54e5f71f6d11c6269ba1c`).

The corrections recheck complete scope after receipt insertion, preserve the
synthetic FUSE entries, use per-handle directory metadata in Windows responses
and separate FUSE directory inode incarnations. Added coverage includes shared
file/directory disposition capacity, three actual exit-86 removal boundaries,
scope-substitution rollback and handle-less setattr after directory restoration.
Two new actual kernel parents cover directory mutation/lifetime and persistent
Cleanup failure/recovery. These parents are not yet qualified.
Corrected candidate `cf7e0e6db6e9887e4cf0e4ce672f46149f48ceecb30951c9fd6f0677f2fc6955`
stopped at one unused `mut` in the new Linux test. Windows strict Clippy passed;
its following build was deliberately interrupted rather than continuing after
the Linux lint failure. The exact source/log archive is
`target/wave6/directory-changes-corrected-candidate.zip` (2,650,242 bytes), SHA-256
`04706816ca2bff71286f74fdb0a444544a680c2fb1a1c85a74c0141fb96cdacd`.
The current 429-input candidate removes that unused binding: SHA-256
`96ffcdb3cd2a713a01836bea438f54192dcc815810b1dad1ef80f5ed1fb15ea1`.
That source completes the scoped qualification recorded below; no full
directory/lifetime release gate is closed.

Next: finish the recorded removal/replacement inventory; consolidate fresh-store
storage, native COW saves/recovery and typed transfers; remove old format/readers,
fallbacks, promotion and upgrade chains; rebase applicable regression fixtures;
qualify one current path on Windows/FUSE and current-version peers. Required
ordinary behavior must have a current replacement before its legacy route is
removed. Complete the inventory and current-only contract/fixture updates as
part of the interlude, then resume remaining Wave 6 obligations.

Earlier entries are chronological evidence. Their requirements for legacy
promotion/migration, old workspace/launcher fences, mixed-version compatibility,
old-reader matrices and downgrade/export-to-old-version workflows are now
superseded, not blockers. Existing compatibility results and failure capsules
remain historical records. Wave 5 remains a performance reference only.
The Windows time-policy failure, structural-demand load reliability gap,
current namespace/lifetime and GC work, resource bounds, cache/change journal,
build comparisons and final platform/peer/outage/storage/soak gates remain open.
This decision does not authorize deleting existing stores or evidence.

## Private journal validation before directory recovery — CCR-WP6.G-23

NativeWrites installation now opens/validates the selected private journal
before directory recovery can commit a pending deletion. Previously an
unsupported journal could refuse startup after that independent namespace work
had already committed. The change moves the existing open call; it adds no
upgrade, previous-format reader, fallback or validation subsystem.

A new ordinary regression stages a real directory disposition and attempts
startup against an unsupported journal through both RPC and direct installation.
It requires exact journal bytes, no private sidecars/discovery files, identical
projection/events and directory snapshot, the same pending intent and a clean
derived-state oracle. The test then replaces only its unsupported fixture with
a fresh current journal and requires normal recovery of that retained intent.
The existing failed directory-Cleanup recovery case is retained and renamed to
state its contract: recovery before requests are accepted.

[journal-recovery-checks.json](../../../test-evidence/wave-6/journal-recovery-checks.json)
records the 438-input source `16d3645718b385c7aaa0a0c34886409d4e1c153f5efc37d4ffef695c4b3f63f1`.
Strict all-test release Clippy, selected builds/CLI, format/diff checks and two
complete targets pass on Windows/Linux: native_default 4 and content_mount_writes
82, giving 86 ordinary cases/platform. The existing actual RPC/default and
direct-mount/recovery parents also pass per platform. Three selected images per
platform keep their hashes through proof. G22's larger suite and load passes
remain evidence of its own earlier source, not new-source test counts.

The initial test source failed compilation because its two derived-state checks
needed mutable Engine guards. No runtime test executed on that source. Exact
source/logs remain in `target/wave6/journal-recovery.zip` (2,655,560 bytes), SHA-256
`2bd1242224a9a331d20e834406bcb5b8cc34313dc48a2fab2b8dda946422c48c`.
The next Linux execution passed three ordinary startup cases but the new parent
failed during setup: it called directory_snapshot before initializing the
current workspace identity. Its exact failed harness/CLI/source/logs are in
`target/wave6/journal-recovery-fixture-failure.zip` (15,766,254 bytes), SHA-256
`edc79bc91513b111c86d7e70b50f2ec965b6f2438920500db9752c016d32f96a`.
Both Clippy checks passed; the Windows build was deliberately terminated after
the Linux setup failure and did not execute runtime proof. The final fixture
uses public content_workspace_id before staging its disposition. Production
code is unchanged from that candidate; no runtime pass is attributed to it.
The final source/proof capsule is `target/wave6/journal-recovery-current.zip`
(2,704,107 bytes), SHA-256 `6f865b75f2e469e54e3c7968d4cdbca3c7f59a989324e3826dc49cf0128ba1c7`.

This qualifies the private-format refusal ordering, not every corrupt Saved
record, the full repository/transport/library, a build comparison, soak or release.
Remaining standalone staging/snapshot/pending-saves APIs/table/shared savers,
temporary Entry.content, generic artifacts/old transport scheduling, remote
lifetimes, metadata cache/change journal, retention/GC, storage/outage/load/build
and release gates remain open. TLS/WSS or VPN with simple pairing remains the
transport boundary. There is no format/layout change, deployment, merge or push.


## Canonical creation and deferred namespace removal — CCR-WP6.G-22

Engine no longer exposes deferred creation or commits a new file on its first
save. Its pending-name map, late namespace insertion/merge, NameHeld retry type,
uncommitted create serialization and TKFS_DEFERRED_CREATE selection are removed.
Lookup/enumeration use the canonical Store namespace; current COW still supplies
dirty size. Remaining standalone stage recovery requires an existing committed
typed file and cannot create a missing name. Current native empty creation,
private byte ownership and exact metadata/checkpoint receipts keep their contract.

The name/capacity tests now create current files/directories. A new parent proves
that an unflushed native child already has canonical empty content, appears once,
shows its private size and prevents directory removal. Replacement keeps both
private writers without premature publication. A stalled metadata writer retains
the 200ms bound for same-name file/directory rejection and unrelated private I/O.
The old instant in-memory create during another SQLite writer is an obsolete
deferred mechanism: current FULL creation must acquire the writer before ACK.

The retained concurrent Linux scenario exposed an existing lifetime gap: a
path-based rename could capture a clean COW session, then lose it to the last
asynchronous FUSE release before its metadata gates arrived. Windows completed
the same scenario; Linux returned EIO. Debug-level replay identified
STALE_CONTENT_VIEW on rename before the panic. Exact initial source7cd, failed
local harness/CLI and logs are in
`target/wave6/deferred-creation-concurrent-linux-failure.zip` (14,565,492 bytes),
SHA-256 `49899a7684b6662f273969fde70c3f94f57022ec21e7bdb2a4efb6377427f734`.
The logging-only diagnostic source33d, actual failed harness/CLI and decoded
error are in `target/wave6/deferred-creation-rename-view-failure.zip`
(14,485,856 bytes), SHA-256
`692cd21ad46dc67baa374e512dab9042246e90be97a8b0dda6ed1e2837925007`.
Those failed results remain source-specific evidence.

Rename now retains readonly operation handles for existing source/destination
sessions under Engine before dropping that short lock. They keep the workspace
and exact COW sessions alive while entity/adapter gates are awaited; success,
deadline and refusal release them. No byte save is introduced. A deterministic
parent closes both user handles in that window, requires quiet/checkout fencing,
then verifies replacement plus deadline cleanup/retry, exact handles, unchanged
bytes and zero premature publications. The real 8x150-file scenario also reruns
on the final source; earlier failures are not relabeled by the passing result.

The local_fs_throughput rebase retires 26 prior test functions, including their
old child helpers, with exact current replacement owners in the report's ledger.
These are old snapshot/spool/deferred mechanisms, not deleted current requirements.
Current executor, private checkpoint/publication/adoption exits, typed repair,
namespace ownership, heartbeat gauges and durable close own those obligations.
Historical raw-API failures stay in their earlier source evidence; no old failure
is relabeled as a pass. Global retry-saver/aggregate load and eventual standalone
execution removal remain open. This is not qualification of every remaining
raw Engine API or pending_saves recovery/control behavior.

The local target retains five ordinary COW/range parents and eight explicit
actual-mounted parents; unavailable mounting now fails explicit invocation rather
than returning a passing no-op. All eight ran/platform. Retained scales/bounds
include 300 copied files, eight writers x150 files with 5s operation/probe bounds,
30 editor replacements, absent/progressing ranges, graceful detach, live name/
unlink behavior and fsync during concurrent real native closes. Provider-based
byte oracles replace old whole-file hashes/catalog counts. These are functional
bounds, not matched W5/native/current build measurements or a 30-minute soak.

Evidence: [deferred-creation-checks.json](../../../test-evidence/wave-6/deferred-creation-checks.json).
The 438 inputs hash to `1002b1df35998011f57a506f2c948d3acf135d343caec0819988752df07f09ad`. Strict release
Clippy over all tests, selected harnesses/CLI, formatting and diff checks pass
Windows/Linux. One final source/image cohort passes 358 cases across eleven
complete targets/platform. Exact adapter range plus two current lifecycle gauge
cases add 3 Windows; Linux's nine module cases add nine more, giving
361 Windows/370 Linux scoped ordinary passes. Sixteen actual-kernel parents pass
per platform, including all eight rebased local cases and eight existing native/
range parents. Children, repeated calls and file-operation counts are not extra
unique tests. All fourteen Windows/fifteen Linux selected images keep their hashes
through proof. Builds were staged on one frozen source before runtime proof;
no Cargo uses a platform's release cache during its proof.

Source/proof capsule: `target/wave6/deferred-creation-owned.zip` (2,780,112 bytes), SHA-256
`b593502240d659a1124925a87beda1362a62fa9e225d497c758be41dab521766`. This is not the full transport/library/worker-lifecycle/repository
suite. There is no schema/header/layout change, upgrade, driver install, reboot,
deployment, merge or push. Standalone Memory/Disk/snapshot/shared-saver/pending
APIs/table, temporary Entry.content, generic artifacts/old transfer scheduling,
startup journal ordering, remote lifetimes, cache/change journal/build comparisons,
retention/GC, outage/ENOSPC/power-loss/aggregate load/soak and release gates stay
open. Transport remains standard TLS/WSS or VPN with simple pairing and DVFS
correctness; no new security workstream.


## Current mounted mutation dispatch — CCR-WP6.G-21

The WinFsp and FUSE callbacks no longer fall back to standalone Engine mutation
when a current helper is unavailable. Attachment already installs the service;
callbacks now require it. Current creation, byte writes/truncation, basic metadata,
rename/replacement, deletion, durability and close use their existing owners.
FUSE removes its Engine-held mutation branches, old mode/size helper and namespace
target-settle fallback. WinFsp removes deferred create and old write/truncate/
flush/basic/rename paths. Readonly immutable sessions have nothing to save, while
volume flush and native close keep the shared current saver/coordinator.

Root/discovery are explicit virtual cases. Root mode 0755 is a no-op; root
time/size changes and discovery mutations refuse. Directory metadata, directory
release/fsync and discovery buffer reads retain their ordinary behavior. FUSE
O_TRUNC releases a handle on failure. A source review after the first successful
build found fsync had lost its invalid-handle check with the removed dispatcher;
the final source validates before saving and keeps a regression for this.

Two old Linux buffer fixtures now use typed Core creation/write plus the installed
coordinator and exact provider byte checks. New module cases cover virtual
root/discovery behavior and refusal without the required current service. Mode/
ownership validation still occurs before any part applies; this is existing
filesystem semantics and adds no user/ACL model.

Evidence: [current-adapter-checks.json](../../../test-evidence/wave-6/current-adapter-checks.json).
The 438 inputs hash to `94d4efef6be4ee0cde13931b1cb5c93b088987ee3bae18f14baad499b025534d`. Strict all-test
release Clippy, selected builds/CLI and format/diff checks pass both platforms.
Five complete targets pass 194 cases/platform. The exact adapter-range case
adds one/platform; Linux's nine FUSE module cases add nine there. Eight actual
kernel parents/platform cover default worker/direct startup, create/close,
directory metadata/rename/replacement, open unlink/replacement, offline fsync and
missing-range hydration from a real peer. Children/repetitions are not extra
unique passes. All seven Windows/eight Linux chosen images retain their hashes throughout
proof; no Cargo uses a platform's release cache while its proof runs.

Source/proof capsule: `target/wave6/current-adapters-resolved.zip` (2,746,712 bytes), SHA-256
`ec7c8284d483630fa003d7d3d061152cc75a76136475255b2f5243a9bd8f3586`. Earlier static candidates preserve the Linux needless-return
failure and the later source-review finding. A later candidate passed 192 Windows
ordinary cases but failed its default-worker actual-kernel parent: a closed
rename source lacked the current metadata owner, previously hidden by fallback.
Its exact failed harness/CLI, source and logs are in
`target/wave6/current-adapters-closed-source-failure.zip` (22,938,041 bytes), SHA-256
`16a19bc651e3301d4231c127656c1936b91ce6bfc6607fc7a32eaedf10649e5c`.
Linux's remaining CLI compilation was deliberately stopped before the fix; it
supplies no runtime proof for that candidate. Final rename prepares a temporary
current source owner for closed/readonly files, validates the visible entity,
then uses the same executor/receipt and releases that temporary handle. Two new
parents remove a source chunk and require metadata-only rename, unchanged
descriptor/content head, zero byte publications and exact retained handles.
The first source-preparation correction then exposed incorrect virtual-root
resolution. Linux's kernel parent failed FILE_NOT_FOUND and the full native-write
diagnostic passed 78/failed the two new parents (12 ignored); nine module units
passed. Exact failed harnesses/CLI, source and logs are preserved in
`target/wave6/current-adapters-root-source-failure.zip` (20,159,381 bytes), SHA-256
`669897b9de269296b6150a35ef6b3b99f6b2e28fd9ac6709e7a4efc6330a8a1b`.
The final traversal resolves the virtual root through lookup("").
The final qualification does not relabel earlier outcomes.
This is neither the full transport/library/repository suite
nor a build/load/soak/release claim. Shared standalone APIs/memory/disk staging,
pending_saves, temporary Entry.content, old generic artifact/transport tables,
startup journal ordering, remote/concurrent lifetimes and remaining Wave 6 gates
stay open. Transport stays TLS/WSS or VPN with simple pairing and DVFS correctness;
no new security layer or driver/deployment changes.


## Current private journals (CCR-WP6.G-2)

2026-10-21. WP6.G is underway, not complete. The
[removal/replacement and coverage inventory](wp6-greenfield-inventory.md)
traces the current Core/startup/runtime/adapter/object entry points and records
which legacy components must be removed, replaced or retained for a current
reason. Remaining default-path, metadata schema, descriptor/producer, native
namespace/lifetime and transport consolidation stays mandatory before the
interlude exits.

`WriteJournal::open` now supports only application ID `0x544b5753`, layout v7,
or genuinely fresh input. The v4/v5/v6 upgrade chain and three previous-reader
parents/helper are removed. The current layout, sealed Saved records, exact
native clocks and operation/checkpoint identities keep their meaning. Direct
fresh initialization retains its atomic FULL schema/inventory/header commit.
`UNSUPPORTED_WRITE_JOURNAL` is an actionable `Incompatible` error, never an
upgrade/reset request. The bounded reader pool uses the same selected-layout
check. Unsupported standalone headers are checked by fixed read-only file
access before SQLite can create sidecars; a nonempty WAL remains authoritative
through read-only SQLite so valid current crash recovery is preserved.

Five current journal parents replace four obsolete/overlapping ordinary
parents. They cover exact current Saved restart/clock preservation with fresh
Working ownership, 104 header/mode/native/open-and-closed refusal combinations,
unchanged persistent bytes and directory entries, unmarked table/view inputs,
malformed and previously used unmarked files, an empty non-ASCII locator, and
three actual exit-86 initialization children. Existing checkpoint, COW,
publication, capture, namespace/adoption, retained-reader, admission/demand and
transport regressions remain. The coverage ledger explains every removed test;
previous executable tests are removed scope, not skipped current release gates.

Validation: **515 Windows / 523 Linux ordinary cases across 16 targets per
platform**, with 20 Windows / 19 Linux default ignores. **Five explicit actual
WinFsp/FUSE kernel parents per platform** pass separately, including fresh
native create/read/write/fsync/close, rename/replacement, retired destination
close, manifest creation and same-version peer range hydration/offline save.
The two independent closed-database header probes per platform also preserve
all files/entries; their child exit 101 is the expected refusal, not an ordinary
passing test. Repeated probes, the 104 combinations and crash children are not
extra unique harness cases. Strict release Clippy on both platforms, formatting
and diff checks pass. All actual test images remain unchanged across proof;
no Cargo runs in a platform release cache during its invocations. The loaded
WinFsp driver and five SDK/runtime hashes remain unchanged before/after proof.

Evidence: [greenfield-journal-checks.json](../../../test-evidence/wave-6/greenfield-journal-checks.json).
The 401 frozen inputs have SHA-256
`173e9ae9ed89fb9062c448b8161a444783bda2e0cc840026b898be3935668568`.
The exact source/log/driver archive is
`target/wave6/greenfield-journal-source-final.zip` (2,685,372 bytes), SHA-256
`ad6cbf636842ae812cad091f35b07c917d7e7c43380983d2c7a6734a1217966b`; exact member inventory and all member bytes were read
back and checked. Two initial source/image/log capsules are preserved with
capture-time hashes: the incorrect new restart assertion (45804f54, ef058fc5
archive), and the actual closed-WAL sidecar defect (7818691b, 43c1b351 archive).
The latter source had a green 514/522 ordinary plus five-kernel matrix, which
is preserved as a passing diagnostic that missed this case, not qualification
of the final correction. The permanent regression and independent actual-image
probes now cover the gap. No runtime change was made for the first test-only
correction; the later runtime defect is recorded separately.

This is current private-journal consolidation, not completion of WP6.G or
Wave 6. Descriptors/string events, legacy staging/deferred saves, default native
execution, metadata schema migrations and old RPC/duplicate scheduler paths
remain pending in the inventory. The Windows timestamp-policy failure and
structural admission reliability gap remain unwaived, as do remaining
namespace/lifetime, retention/GC, resource budgets, cache/journal/build and final
platform/peer/outage/storage/30-minute integration gates. Next trace the Core
file-content producers and generic internal byte-object consumers, then land
the current replacements before deleting their only working execution routes.

## Current Core metadata (CCR-WP6.G-3)

The Core store now directly initializes application ID `0x544b4d53`, version 1,
from `src/core/schema.sql`. Its 61 tables, 34 triggers and 15 explicit indexes
have a golden inventory based on 109 definitions independently captured from
the preceding CLI's fresh store, plus the unchanged `pending_saves` definition
formerly created by the runtime. The capture records the actual CLI digest and
command. Schema, branch and repository/device identities,
generation, journal epoch and header commit together in a FULL transaction.
Secondary handles validate the selected layout without running recovery or DDL.
Header, catalog and identities share one read snapshot when no writer transaction
already owns the connection, preventing mixed observations during fresh creation.
Unsupported input is refused before writable open or objects-directory creation;
the journal's standalone-header eligibility guard is shared without changing
its nonempty-WAL authority policy.

Removed Core startup work includes the branch UNIQUE migration, inbox ALTER
chain, module schema stamps, historical object/feed/snapshot backfill selection,
runtime pending-save DDL and adoption of unrecorded prior-format spools. Current
derived object-reference, feed and journal-index repair remains bounded and
restartable. Only declared disposable structures can be recreated; missing
required identities, events, receipts, journal sequence or save ownership state
are errors. Repair does not reset accepted sequence, epoch, peer acknowledgement,
permissions or exact save receipts. The separate catalog layout is still pending.

Seven new ordinary Store-format parents cover fresh/golden schema, concurrent
writer identity, exact current authority and typed/private Saved restart,
32 unsupported header/mode/lifetime combinations through four open APIs,
damaged current authority, three actual initialization exits and three actual
derived-repair exits. Existing inbox, index, snapshot, worker and integration
scenarios are retained or rebased as detailed in the
[coverage ledger](wp6-greenfield-inventory.md#core-metadata-coverage-ledger-ccr-wp6g-3).

The initial source `c146952c` exposed two different negatives. The new concurrency
test overconstrained permitted SQLite contention and could remove its temporary
directory before joining all writers; it now joins all attempts and retries only
`SQLITE_BUSY`, requiring all eight final identities to agree. Separately, the
retained prepared-error worker regression waited one second and returned empty
success where immediate `WORKER_BUSY` was required, on both platforms. An external
probe linked to that frozen library measured WAL checkout at 1.29 ms; the wait
was the transfer-family gate. The first correction made both WANT acquisitions
nonblocking; that passed the focused worker case and all seven Linux Store
parents but failed the retained cooperative admission/cancellation unit on both
platforms (Linux library: 125 passed, 1 failed). That intermediate source
`df58e308` is preserved in
`target/wave6/greenfield-schema-admission-diagnostic-before-fix.zip`, SHA-256
`c1cb833c02267e315ddf88540b7fbd910de806c2e5b4be5f331ba01bad7ee668`.
The revised family gate distinguishes a durable error projection from ordinary
admission: the former returns contention immediately, the latter retains the
cooperative shared deadline and cancellation/stop/run checks. The projection
marker is scoped to the held gate and clears on unwind. Both WANT phases use
the same helper; no test or service bound was removed. The existing 50 ms
prepared-commit service assertion and both-phase wait/cancel assertions remain.

A further static review found that separate autocommit validation queries could
observe different initialization commits. The `a2cbbd08` revision passed strict
Clippy, then its builds were intentionally interrupted before tests to add the
coherent read snapshot; Windows exit -1 and Linux exit 130 are recorded as that
interruption, not compiler failures. Its exact checkpoint is
`target/wave6/greenfield-schema-before-coherent-snapshot.zip`, SHA-256
`493c8d1f0e22f0b85c8cb9071ef9e8df5e6219a53a5be9c32cf7dc6fbd25a5d3`.
The concurrent initializer parent now uses four independent fresh roots, each
with eight writers and the same join/identity/contention assertions.

Initial source/log capsule: `target/wave6/greenfield-schema-initial-before-fix.zip`,
SHA-256 `c2c5cbb9321343f50e0a9edd81dea395ea9f0612bc55c8541853540c65e57d4c`.
The supplemental exact-source/isolated-reproduction/external-input capsule is
`target/wave6/greenfield-schema-initial-diagnostics-before-fix.zip`, SHA-256
`4c6936988c34a74cb8c85aa276006416f7c8f03cdcc568a3236adca52ffad18c`.
External modified Rust probes are explicitly additional inputs, not part of the
406-file application source inventory. Their source/library/executable hashes,
commands and output are preserved. The initial Linux smoke's missing worker-CLI
attestation is recorded as a diagnostic limitation; final qualification hashes
the CLI as well as each test harness.

The broader Windows run also exposed a test precondition race: direct controller
`inspect` was already running while the CLI's public hub snapshot was still
inside its documented 4 Hz publication interval. The unchanged CLI/schema/watch
assertions now follow an explicit published-running wait bounded to one second.
That is a test-only change. The preceding `8430e928` source, complete Linux
769-case/five-kernel proof and Windows 27 successful targets plus W2's 12 passes /
one failure remain in `target/wave6/greenfield-schema-status-publication-before-fix.zip`,
SHA-256 `0858eec5cb6517b154930efb912fd604bd4d4c137f140d8831b8704c9e97f375`.

Validation totals: **761 Windows / 769 Linux ordinary cases across 29 selected
targets/platform**, 29/28 default ignores, and **five actual WinFsp/FUSE kernel
parents/platform** separately. Strict release Clippy for all library/test targets,
formatting and diff checks pass. The prepared-error service sample is 3.80 ms
Windows / 1.30 ms Linux under the existing 50 ms bound; both-phase cooperative
wait/cancel coverage also passes. These are controlled correctness samples,
not the outstanding general performance/load gates.

The final test-only follow-up changed only `tests/w2_integration.rs` and its
executable. Every production/other-test/fixture input and the other 29 executable
images per platform, including the actual worker CLI, stayed byte-identical.
Evidence carries 28 Linux / 27 Windows targets and Linux's five kernel parents
with their original actual source SHA; it reruns W2 on both platforms, and runs
the outstanding Windows worker-status target and all five Windows kernel parents
at the final source. It does not relabel the earlier failing W2 attempt or claim
one uninterrupted final-source run. Repeated probes, matrix cells and crash
children are not additional unique passing cases. The loaded WinFsp driver and
five SDK/runtime hashes remain unchanged before/after proof.

Evidence: [greenfield-schema-checks.json](../../../test-evidence/wave-6/greenfield-schema-checks.json).
The 406 final inputs have SHA-256
`eee8f7101281aadd7397fb180125c44bc44cdea302b8faba0103613811806d8b`;
the preceding full-matrix source is
`8430e928d1ce0fc8fbd643f996d6d9325b821bedf790af19e3562cab7f58dcb9`.
Exact archives, member readback, raw commands/output, per-target source/image
provenance and the runner's exact original bytes are retained in the report.
The final source/log archive is `target/wave6/greenfield-schema-source-final.zip`
(2,795,253 bytes), SHA-256
`2f6b08290209bf1c4eebc5365330d15861818a67306732abeeb668e754e6e933`.
The five initial/intermediate capsules retain their capture-time digests; all
archive members were read back and checked.

WP6.G and Wave 6 remain incomplete. Next consolidate the separate catalog layout
and rebase its applicable lifecycle/pairing fixtures, then continue typed producer,
native default/namespace/lifetime and transport replacements in the inventory.
The Windows timestamp-policy failure, structural-demand reliability gap and all
remaining current-path retention/resource/build/platform/outage/storage/soak gates
remain open. Production content activation remains off.

## Current catalog (CCR-WP6.G-4)

2026-10-22. The previous goal turn was progress: CCR-WP6.G-3 completed its Core
implementation and staged Windows/Linux evidence. The next greenfield tranche
directly initializes one catalog layout, application ID `0x544b4353`, version 1,
from seven captured current table definitions. The preceding actual CLI image
and fresh eight-table capture are recorded in
`target/wave6/greenfield-catalog-before-inventory.json`; only the obsolete
`initial_sync` mirror is excluded from the new golden layout.

Schema, installation identity, initial verifier generation, initial lease counter
and header share one FULL transaction. Preflight occurs before lock/mount-path
creation and permission changes; unsupported input is refused without upgrade.
Existing required tables, installation/counters, verifier consistency and folder
intents are validated in a coherent read snapshot. `mount_gates` is the sole gate
source. A pending gate may remain after removing its mount path until the
controller satisfies/overrides and clears it; that legitimate restart state is
retained. No open-time gate, verifier or credential reconciliation remains.

`verifier_salt` remains as a current verifier/generation consistency check, not
as an older-writer rotation detector. Current verifier rotation and inbound
credential revocation remain atomic. Counter increments are checked for
exhaustion; missing identities/intents and malformed verifier state are errors,
never synthesized defaults. Invalid individual credential rows retain their
existing fail-closed handling. Immutable snapshot publication, request withdrawal,
unknown outcomes, actor panic rollback, directory-lock ownership and bounded
SQLite busy retries remain.

Three catalog upgrade/downgrade parents and their SQL oracle are retired. Current
restart/identity/credential assertions replace the applicable coverage. Mount
validation retains all 15 explicit path cases, canonical target and error/class
checks; locking now tests the real directory lock and current catalog. Security's
older-writer rotation scenario is rebased to the actual current command and
restart. The old-datadir peer scenario is rebased to fresh current restart and
bidirectional sync. The new catalog-format parents cover golden initialization,
exact current restart, 32 unsupported-header/mode/open-or-closed cases,
authoritative damage, initialization exits, rotation exit and generation
exhaustion. Existing command, lifecycle, publication, enrollment and revoke tests
remain in scope.

Initial source `5a25b215` was deliberately interrupted before runtime testing
after static review found that the new validator incorrectly prohibited a gate
on a now-headless folder. The gate/identity restart assertion was strengthened
before restarting qualification. Its exact source/log checkpoint is
`target/wave6/greenfield-catalog-gate-review-before-fix.zip`, SHA-256
`7b95b0d3dd2cfb6975151af01978a26d1cbe68ddbdc06f8ba83e2b665ed72bbc`.
This is a recorded intentional interruption, not a test or compiler failure.

At source `867d1d3e`, the seven format parents passed both platforms and the
19 actor parents passed Linux. The existing lease test expected the old
catch-all `CATALOG_UNAVAILABLE` for malformed as well as exhausted counters.
The new preflight correctly returns `INVALID_CATALOG` for malformed current
authority before writable access; valid exhaustion retains `CATALOG_UNAVAILABLE`.
That exact expectation mismatch was confirmed on both platforms and preserved in
`target/wave6/greenfield-catalog-counter-code-before-rebase.zip`, SHA-256
`e2d23239f84094970e591facc90adcb38bc1b3d4420c35b3b33f450716de2aee`.
Only the existing lease test's per-cause expectation changed; no-reuse, unchanged
stored-value and startup-refusal assertions remain. The full lease target then
passed on both platforms.

The broader Windows run at `89a6a6e2` then passed 34 ordinary targets and
reported 27 passed / 1 failed in `security`. Its
`code_paired_peers_enroll_on_their_next_connection` fixture drops the entire
`peer_credentials` table to test the retired additive migration. The selected
format refuses that missing authoritative structure with `INVALID_CATALOG`.
The capture before any source correction is
`target/wave6/greenfield-catalog-enrollment-schema-before-rebase.zip`, SHA-256
`0bc83a358c849ecd0839143b74504c84dc9a9668e815e9a442d4aa3e03225364`.
Linux confirmed the same 27-pass/1-fail result. Both then completed the other
42 ordinary targets, five actual kernel parents and the current restart/two-peer
scenario on unchanged source. The complete pre-correction capsule is
`target/wave6/greenfield-catalog-enrollment-schema-complete-before-rebase.zip`
(2,804,247 bytes), SHA-256
`23c4586352518b7090df6badb262e4cae69ecacd9cbdedb2a4024d155c1c5dc4`.

Only `tests/security.rs` changes in the final follow-up. The replacement removes
credential rows in stopped isolated current fixtures, keeps both installation
identities, requires matching credentials distinct from the lost key, authenticates
with that key and restores ready lanes after restart. No production change,
schema reconstruction or bound relaxation was made. Strict Clippy and optimized
security rebuilds pass on both systems; all **28 security cases** pass in each.
The other 42 successful targets and the five kernel/peer parents retain their
actual preceding source SHA and unchanged invoked images/worker CLI. Every
production and other test input is byte-identical. This is staged qualification,
not a claim of one uninterrupted final-source matrix.

Totals: **1,143 Windows / 1,151 Linux ordinary cases across 43 selected targets**,
43/42 default ignores; **five actual WinFsp/FUSE kernel parents and one separate
current restart/two-peer scenario per platform**. Matrix cells, initialization
exit children, diagnostics and reruns are not extra unique cases. All 44 actual
harness/worker-CLI images are checked before/after every invocation; no Cargo
runs in a platform release cache during proof. The loaded WinFsp driver and five
SDK/runtime hashes remain unchanged after the final security proofs. Formatting
and diff checks pass. Evidence:
[greenfield-catalog-checks.json](../../../test-evidence/wave-6/greenfield-catalog-checks.json).

The 410 final inputs have SHA-256
`369f32ecc173e83b3cbec0d94b4daf22c6c931648236fd103e8d5f9090411461`;
the preceding production/42-target proof source is
`89a6a6e246aa71b7ea6a29b2e161dc1447b3db7f438887b83dfef5af27a4c4ac`.
Exact source/image/runner/log and capsule member inventories are retained in the
report. The final source/log archive is
`target/wave6/greenfield-catalog-source-final.zip` (2,854,626 bytes), SHA-256
`6c0ad432059e5816572eb6c15c787e249985fe6ba93169082b4defb3a7a1c535`.
Every member was read back and checked. All four initial/intermediate capsules
retain their capture-time digests and checked member inventories.
Catalog consolidation is qualified; WP6.G and the full Wave 6 goal remain
active, with production activation off. Next complete typed producers and native
namespace/lifetime/default execution, then remove conversion, staging and duplicate
transport routes using the inventory's replacement dependencies. The Windows
timestamp-policy failure and structural-demand reliability gap remain unwaived,
along with remaining retention/resource/build/platform/outage/storage/soak gates.

## Native unlink and durable dispositions (CCR-WP6.G-5)

### Initial candidate (preserved failure)

2026-10-22. The previous goal turn completed CCR-WP6.G-4 and its staged catalog
qualification. This tranche implements an ordinary operation required before the
native path can become the default. The Core now commits an exact unlink receipt,
tombstone and retained-writer continuation together, using existing current
namespace tables; no schema migration or byte publication occurs. Later saves
of the unnamed writer keep it dead. Reusing the name creates a distinct entity.

The native coordinator checkpoints private bytes, reconciles exact metadata and
journal replies, and publishes namespace observation outside Engine. Read-only
or unopened typed files can acquire this ownership without publishing bytes.
FUSE routes unlink through that coordinator on its bounded callback threads.
WinFsp CanDelete remains validation-only; Cleanup receives its actual supplied
name and owns removal. Native POSIX delete and new-file delete-on-close now use
the retained-file route. The SDK's void Cleanup limitation is unchanged; failed
work must remain retained/reported rather than being described as a fallible
Windows close reply.

New regressions cover retained readers/writers, same-name replacement isolation,
exact receipt replay and changed-request refusal, damaged retirement evidence,
rollback/lost replies, three actual exit-86 metadata boundaries and Engine
availability during a delayed SQL commit. A new explicit actual WinFsp/FUSE parent
checks open-handle unlink, read/write/fsync, name reuse and Windows delete-on-close.
Qualification is in progress on frozen 414-input source
`23ded7b8c49838440d01ddd36a254c9ecfbd716e8293efd2ecd68a54d6e58856`.
Strict release Clippy passes on both platforms. Linux optimized builds pass;
its focused ordinary targets pass **31 namespace / 57 native-write cases**, with
3/7 default ignores. The new actual FUSE unlink parent passes separately,
including link count zero, retained reader/writer bytes, fsync and same-name
isolation. These are focused diagnostic results, not the proposed complete
11-target/six-kernel matrix or completion of this tranche. Windows optimized
builds also pass, with the same 31 namespace / 57 native-write ordinary cases
and 3/8 default ignores. The dedicated actual WinFsp parent passes, including
POSIX unlink with retained open readers/writers, same-name isolation and newly
created delete-on-close. Only that one dedicated kernel parent per platform has
run for this candidate; the earlier five parents have not been requalified
against these production changes.

The actual Windows fault probe then fails with exit 101. It arms persistent
`content.save.unlink_written` failure in the same actual kernel parent. The
delete disposition succeeds, Cleanup logs `FAULT_INJECTED` at mount op 9, and
the assertion that the name disappeared fails. This is a real missing behavior
in the initial candidate, not an obsolete test precondition. Core rolls back the
namespace event and has no independently durable pending deletion request to
recover. Saved-byte preservation alone does not preserve the requested deletion.
Do not count this negative as a passing kernel case or qualify the Windows path.

The exact checkpoint, raw results, all input/image hashes and actual failed
Windows harness plus worker CLI are preserved in
`target/wave6/native-unlink-initial-before-durable-cleanup.zip` (21,393,518 bytes),
SHA-256 `e4cb79b65e281b06074580abc6ebc875599404dfbeac786e8f76163622209968`.
Every member was read back and checked. Partial evidence:
[native-unlink-initial-checks.json](../../../test-evidence/wave-6/native-unlink-initial-checks.json).
Format/diff checks pass; source remains the same 414-input checkpoint. The loaded
WinFsp driver and five SDK/runtime hashes remain unchanged before/after these
proofs. No production source correction had landed at this initial checkpoint.

Next implement durable pending removal ownership and replay for the void Windows
Cleanup path. Account for cancellation, delete-on-close at open, multiple handles,
rename/name reuse, and stale workspace/operation ownership. Validate the placement
of durable requests against the SDK's CanDelete/SetDelete/Cleanup contract rather
than retrying an unrecorded deletion until a transient test passes. Complete the
permanent fault/restart regressions and the proposed ordinary/kernel matrix on
the corrected source. This finding does not waive any close/durability gate.

The prior replacement kernel parent still contains two temporary Windows guards
expecting error 50 for native delete and delete-on-close. Their premise changes
when removal is implemented. Rebase those guard calls without deleting the
replacement/retired-destination assertions; the new positive unlink parent owns
the newly supported behavior. No test was removed at this checkpoint.
WP6.G and the full goal remain active.

### Durable disposition correction (qualified file scope)

The preserved initial failure is the input to this correction. The selected Core
layout now advances to version 2, directly initialized with 62 tables, 34 triggers
and 16 explicit indexes. Its golden fixture preserves the preceding independent
110 definitions and adds the two explicitly specified deletion-authority objects.
There is no v1 reader, migration or development-store reset. Required intent
authority is not a disposable index and cannot be silently recreated on open.

Each native handle receives a fresh request UUID. WinFsp's SetDelete hook stages
or cancels that request durably before returning; new/existing delete-on-close
opens stage before their ACK. Core binds workspace/view, entity and stable byte
operation, checks capacity and sealed row identity, and fences checkout while
requests remain. Marking/canceling does not publish bytes or delete the name.
Namespace retirement atomically consumes all matching handle requests, including
replacement of an already marked destination. A failed Cleanup keeps the separate
intent; replay is restricted to an already durable request rather than retrying
an unrecorded fallible unlink.

Before installing native sessions, startup validates pending workspace/writer
authority, normalizes committed byte predecessors, preserves Saved and resolves
the current name by entity. It removes the name before ordinary Saved publication.
Clean Working-only captures remain intact without unnecessary byte work. The
private journal stays v7; its new read-only eligibility check neither promotes nor
discards Working. Old temporary refusal guards were removed only from the
replacement kernel parent's setup; every replacement/retired-handle assertion
remains, and dedicated positive/failure/cancellation parents own removal.

New Core tests cover idempotent marking/cancellation, checkout fencing, multiple
handles, rename/name reuse, byte continuations and superseded writers, 64-record
capacity/16-row pages, damaged sealed authority and four actual exit-86 commit
boundaries. New runtime tests cover clean/no-Saved restart, private Saved recovery
after persistent Cleanup failure, per-handle cancellation and stale recovery
without private-journal mutation. The added actual kernel parent exercises
WinFsp disposition cancellation and recovery of both existing-file and new-file
delete-on-close failures; FUSE must retain its fallible unlink behavior.

Two correction compile checkpoints were preserved before edits. Source
`095271a7` stopped at Clippy's collapsible-if check on both systems; it is in
`target/wave6/native-unlink-durable-lint-before-fix.zip`, SHA-256
`219cf2e40fb29732cc61a1e76d99ed62e3a1eabf2e417c0de984512420312138`.
After that expression-only correction, source `cd7b61ab` reached the new crash
helper and failed to compile its owned PathBuf argument on both systems. Its
checkpoint is `target/wave6/native-unlink-durable-test-helper-before-fix.zip`,
SHA-256 `304ca606b04f760d25bcf115a0eb56ae4ac573f1488baed3bcd3c73a955ddd6f`.
The helper now borrows the path. These were lint/test-helper compile failures,
not runtime failures or a relaxation of behavior/assertions.

The first executable correction, 418 inputs at source `24e0d68b`, passed strict
Clippy and optimized compilation on both platforms. Focused Linux execution
reported 34 namespace passes/two failures and 59 runtime passes/three failures;
store-format and feed targets passed separately. The new restart path tried to
load the synthetic root as an ordinary entity, and inspection rejected a proven
byte publication before adopting its successor. An independent compiled probe
also demonstrated that an existing request masked `STALE_CONTENT_SAVE` on a
repeated disposition ACK (`Ok(true)`, exit 101). These are implementation defects,
not fixture changes. Source, logs, actual failed Linux harnesses/worker CLI and
the compiled probe are preserved in
`target/wave6/native-unlink-durable-runtime-before-fix.zip`, SHA-256
`9ea3635e87da2962bd2acfa681d1123b9ad33d8ddf2cef549a8ee363a633c864`.

The correction stops traversal at the synthetic root, accepts only the exact
unowned post-publication generation during inspection, and revalidates current
stage eligibility before resolving a lost ACK. A final in-transaction recheck
rejects a silently fenced writer after insertion. Added permanent tests exercise
that trigger, repeated stale staging, safe cancellation after a fence and all
four lost stage/cancel reply boundaries.

Final frozen correction: 418 inputs, SHA-256
`b01a6d7c8fa1663274a7d49f3ec1c3e9be14046e438a22b0c7774c438569ec4f`.
Strict release Clippy, optimized builds, formatting and diff checks pass on both
platforms. The complete scoped matrix passes **512 Windows / 520 Linux ordinary
cases across 12 targets/platform**, with 23/22 default ignores, and **seven
explicit actual kernel parents/platform**. Every invocation checks all 13 actual
harness/worker-CLI images and the canonical source before and after; no Cargo runs
in that platform's release cache during proof. Diagnostic repetitions, child exits
and matrix cells are not additional unique passes. The same unchanged WinFsp
driver and five SDK/runtime hashes were observed before and after all proofs.

The seven kernel parents cover offline COW fsync, real-peer missing ranges,
native write/truncate/flush/reopen, retained-destination replacement, native
create/close/replacement, retained open unlink/name reuse, and persistent deletion
failure/restart/cancellation. The last parent verifies actual Windows SetDelete
cancellation and recovery of both existing-file deletion and new-file
delete-on-close, including their Saved bytes. FUSE keeps its fallible unlink
response and named bytes on failure. The replacement parent passes after removing
only its obsolete refusal guards.

Final evidence:
[native-unlink-durable-checks.json](../../../test-evidence/wave-6/native-unlink-durable-checks.json).
The source/log/driver archive is
`target/wave6/native-unlink-durable-source-final.zip` (2,741,521 bytes), SHA-256
`887ca30f5fb866a21f7b69aebe207582654b1ec12037767f97c42d91a99ae51c`.
Every archive member was read back and verified. The report also verifies all
four preceding failure/development capsules and embeds exact proof-driver bytes.

This qualifies file removal/dispositions in the isolated current path. It does
not qualify default typed execution, complete directory/lifetime/remote ordering,
descriptor/staging/transport removal, timestamp-policy or structural-demand
stability, or the other Wave 6 performance/storage/power-loss/soak gates. Production
activation stays off; WP6.G and the full goal remain active.

## Native directory creation (CCR-WP6.G-6)

2026-10-22. The preceding turn qualified native file unlink/dispositions at
418-input source `b01a6d7c`. Required directory operations still used inline Engine
commits. The creation replacement reserves the same bounded parent/name/entity
namespace ownership and performs one FULL pooled Core transaction outside Engine.
It binds workspace, branch/view, parent identity and initial metadata, and commits
the four directory registers with a separately sealed exact creation receipt.
No byte owner, private session/checkpoint, object root or byte publication is
manufactured. The selected metadata layout remains version 2 and journal v7.

WinFsp directory Create and threaded FUSE MKDIR now use this coordinator in native
fixtures. Metadata-only handles preserve ordinary enumeration cuts. Lost replies
resolve only exact committed receipts; a failed/expired ACK attaches no handle.
Current authority is rechecked after receipt insertion and before attaching a
handle. Historical replay cannot recreate a moved/deleted directory or replace
another occupant. Initial mode is part of creation rather than a second setter.

Five new Core parents cover exact/historical receipts, no byte owner, scope and
parent/permission/identity fencing, silent receipt/event/view changes, four
rollback/lost-reply boundaries and three actual exit-86 children. Five runtime
parents cover metadata handles/enumeration, failed/expired ACKs, namespace and
adapter reservations with independent reads, and inode-parent reuse. A new actual
WinFsp/FUSE parent delays the real directory commit, checks Engine availability
and independent kernel metadata, then verifies initial mode, nested directories
and native child files. This was the initial proposed coverage; final results
and the added parallel-namespace parent are recorded below.

The existing native-file creation kernel parent still requires four file-create
receipts. Its assertion now separately requires the new directory receipt from
its existing nested-directory setup, and reports both counts. All byte/replacement/
retired-destination assertions remain. This rebase reflects additional directory
durability rather than dropping a failing file scenario.

Initial 422-input source `79cd2a39` reached a test-only compile failure on both
platforms: a new test called the crate-private `cow_file` helper. Its unchanged
semantic assertion now matches the public `Staged::Cow` variant. Exact source,
logs and driver bytes are in
`target/wave6/native-directory-create-test-privacy-before-fix.zip`, SHA-256
`abbe3211c43057fb7637f7836962724ad629795560909856aa67648e13370781`.
The first Linux shell invocation also failed before compilation because its
generated driver used CRLF. Raw failed driver/preparation bytes and the error
are retained in `native-directory-create-linux-driver-line-ending-failure.json`;
the Linux driver now uses LF. Neither failure is a runtime pass.

The first executable 422-input source `2df4f9b6` passes strict release Clippy and
optimized builds on both platforms, with focused 42 namespace/69 runtime ordinary
passes per platform. Its new actual kernel parent fails on both systems: a named
stat in the same parent waits through an injected 500 ms mkdir commit and exceeds
the proposed 300 ms check, although Engine's try-lock passes. Linux sampling
records 89 `walk_component` statx waits with an idle `fuse-dispatch` thread and a
sleeping delayed request; a separately compiled existing-handle metadata control
finishes in 0.138745 ms. Windows' existing-handle control instead waits 501.89 ms.
The installed SDK documents FINE as a volume-wide exclusive guard for Create,
deleting Cleanup and rename. These failures are preserved, not counted as passes.

Source, all raw results, actual initial Linux/Windows harnesses and worker CLIs,
standalone probe images/drivers and the observed SDK header are in
`target/wave6/native-directory-create-kernel-wait-before-fix.zip` (56,239,321 bytes),
SHA-256 `efd81056a45025c7fb531abaf2a8f1a967bbf056c74820a6d7c28b2b840cfe70`.
Every archived input/image was checked on readback. The initial probe-control
attempts with a missing temporary fixture and missing Windows host dependency
search path are also retained as failed driver attempts, not implementation or
passing kernel evidence.

The correction uses the supported null SDK Enter/Leave operation guards only in
explicit native mounts. Rust name/entity gates and Core final checks remain
mandatory; kernel per-file serialization remains. Unconverted execution keeps
FINE until default-path replacement. The permanent parent retains the 50 ms
Engine and 300 ms existing-handle checks, and records concurrent same-parent named
stat as a separate measured correctness diagnostic. It does not claim the original
named-stat timing passed or waive the original W6 load/cache/build gates.

Final frozen 422-input source:
`afe3e9044da1bd8f8910679e96750716bf3f4f1efc527544e113a2faf05ce24b`.
The intermediate `402925aa` guard candidate passed lint/build on both platforms
and the adjusted directory kernel parent on both. Windows' independent handle
control, compiled with the same probe source and changed native library, drops
from 501.89 ms to 0.0361 ms during the unchanged 500 ms delay. Its source/log/control
checkpoint is `target/wave6/native-directory-create-guard-candidate-before-admission-fix.zip`,
SHA-256 `f804ca39b91fb4d20d32a6df0504f0847a24a59ca4d6ef2a2c268ace6789c0fd`.
These are focused diagnostic passes, not the complete final matrix.

Static resource review then moved workspace identity capture after name/entity
reservation, bounding pooled-connection allocation even for callers without
adapter slots. The final kernel parent uses a separate file not opened by the
test for named-metadata overlap, so opening the progress-control handle does not
warm that same name. A ninth kernel parent overlaps a committed native rename
with opening its new name and recreating its old one; retained writer fsync must
preserve both byte streams and entity identities.

Final qualification passes **522 Windows / 530 Linux ordinary cases across
12 targets/platform**, with 26/25 default ignores, and **nine explicit actual
kernel parents/platform**. These include the preceding seven native parents and
the two new directory/progress and concurrent-namespace parents. Every invocation
checks all 13 harness/worker-CLI images and the frozen source before and after.
Strict release Clippy, optimized builds and format/diff checks pass. No Cargo runs
in a platform release cache during its proof. Repeats and actual exit children
are not additional unique passing cases.

In the final directory parent, existing-handle metadata takes 0.0389 ms Windows /
0.113704 ms Linux under the 500 ms commit delay. The separately measured name not
opened by the test takes 0.1689 ms Windows / 551.348025 ms Linux. The latter remains
a kernel-path/cache/load diagnostic, not a passing named-lookup release gate.
The same loaded WinFsp driver and five SDK/runtime hashes remain unchanged before
and after all proofs; no installation/reboot occurred.

The separate actual Windows time-policy probe still fails with exit 101 at the
unchanged timestamp equality assertion (current-image line 1999), followed by
Engine-poison messages caused by its assertion while holding the lock. The raw
failure, exact source and actual failed harness/worker CLI are in
`target/wave6/native-directory-create-windows-time-policy-current-failure.zip`
(21,715,429 bytes), SHA-256
`3e9ac3ab51342dfe505bc617993ad00c23f8bdd739e999b3a009cf0a0f8a126c`.
It is not one of the nine passing parents and remains an unwaived release gate.

Final evidence:
[native-directory-create-checks.json](../../../test-evidence/wave-6/native-directory-create-checks.json).
The verified source/log/driver archive is
`target/wave6/native-directory-create-source-final.zip` (2,785,918 bytes), SHA-256
`2f51ae57a7ac2da080ea937c7a7d05e354c5bbee5b0fd4ac1705344badddd6cd`.
Every member was read back. The report also verifies the preceding G5 source
archive and all four current development/failure checkpoints, with exact proof
driver bytes embedded.

Next replace directory rename/removal/basic setters with scoped metadata writes
and exact lost-reply handling, including parent/name ordering against pooled
native producers and durable directory dispositions. Their current Core helpers
still perform checks before the general commit and the adapter holds Engine.
Complete directory metadata/lifetime, default typed execution, remaining WP6.G
removals and every outstanding Wave 6 gate remain required. Production activation
stays off; WP6.G and the full goal remain active.

## Native directory mutations and dispositions (CCR-WP6.G-7)

Directory basic, rename and removal now use pooled FULL writers outside Engine,
with complete directory snapshots and sealed exact receipts. A command checks
current scope, source/target identity, empty replacement/removal and bounded
ancestry inside its writer. Partial basic updates preserve unspecified fields;
receipt replay returns historical evidence without replaying a later name.
No file save intent, private checkpoint or byte owner is created for a directory.

Windows SetDelete/delete-on-close acknowledgements persist per-handle requests;
cancellation is per handle. File and directory requests share a 64-request bound
and directory recovery pages contain at most 16 entries. Owned basic/rename
updates pending snapshots atomically; remove/replacement consumes them. Native
child creation or moving a child into a pending-deletion directory refuses in
Core. Checkout refuses either request kind. Startup validates and replays exact
directory requests before opening the private file journal.

Handles retain directory metadata separately from the shared session. Owned
commands advance handles of the same existence; restoration does not grant an
older handle authority over the restored directory. FUSE assigns distinct inode
incarnations and preserves this rule even for setattr without an explicit handle.
Windows basic/GetInfo responses use each handle's metadata. Existing enumeration
cuts remain stable; rewinding an obsolete incarnation refuses.

Selected fresh metadata is layout 3 (`0x544b4d53`), with 63 tables, 34 triggers
and 17 explicit indexes. The independent v3 fixture retains all preceding 112
definitions and adds the explicit directory-request table/index. There is no
layout-2 reader or upgrade. Private journal v7 and catalog v1 are unchanged.

Final scoped evidence:
[native-directory-metadata-checks.json](../../../test-evidence/wave-6/native-directory-metadata-checks.json)
(109,636 bytes). Source: 429 inputs, SHA-256
`96ffcdb3cd2a713a01836bea438f54192dcc815810b1dad1ef80f5ed1fb15ea1`.
Strict release Clippy, format/diff checks and 12 release targets/platform pass:
539 Windows/548 Linux ordinary cases, 29/28 default ignores and eleven actual
kernel parents/platform. Repeated diagnostics and exit helpers are not extra
unique passes. All 13 harness/CLI images/platform remain unchanged through proof.
The two new kernel parents cover directory metadata/rename/empty replacement/
retained handles/name reuse and persistent Cleanup failure/cancellation/restart;
FUSE must return a removal failure. Three new actual exit86 boundaries verify
atomic removal and consumption of all pending requests. The synthetic ROOT
regression, post-receipt generation substitution, stale schema assertions and
lint checkpoints remain in the exact failure capsules recorded above/report.

The source/log/driver archive is
`target/wave6/directory-changes-current-source-final.zip` (2,744,880 bytes), SHA-256
`baeb6b6df4309b661fbe63ca3c80c652c3b7b4f5919ca47a1482ab6e3626688f`.
Every archive member was read back; all four development capsules and current
source/images were rechecked. Loaded WinFsp driver and five SDK/runtime hashes
remain unchanged before/after proof; no install/reboot occurred.

The full goal remains active. Complete remote/deadline/lost-observation lifetimes,
default typed producers/execution, the remaining greenfield removals and all
original release gates are open. The prior actual Windows timestamp-policy
failure remains unresolved and was not rerun in this directory-only proof.
The structural demand timing gap, real build/load/outage/physical-power-loss/
soak gates are unwaived. Transport work must simplify the existing custom HMAC
handshake/enrollment wrapping and use standard TLS/WSS or the enclosing VPN as
the channel-security boundary; it must not add another security layer.

## Contracts and baseline (WP6.0)

CCR-WP6.0-1 introduces `model::content`, `model::workspace`,
`util::phases` and `WAVE_6_CONTRACTS.md`. Existing wire/IPC/events/schema are
unchanged. Golden binary fixtures have an independent Python reference encoder.
The six release `content_format` tests passed on Windows.

The selected representation is fixed 64 KiB, direct through 64 KiB, canonical
empty direct content, non-padded tails, indexed 256-entry pages with
domain-separated identities and a 256 TiB new-format file-size limit.
Provider/session/coordination limits and callback durability mapping are
recorded before runtime consumers. Foundation counters exist; instrumenting
all core/adapter/SQLite phases remains work, not measured evidence.

An actual Wave 5 release executable was built from an isolated source snapshot
of `02a6f9d`; `git diff b1b8e52 02a6f9d -- src Cargo.toml Cargo.lock build.rs
native ui` is empty. Build command:

```
python scripts/build.py --platform windows --profile release --keep-build-cache --jobs 4 --offline --output target/wave6/baseline
```

Executable SHA-256:
`7d9c8a386b64a563c118c4107a5bd3f165ebf59d152c06e71fedcdb46f522dbe`.
The binary's content-derived version is `0.1.0+8029c80fce14`.
It is retained locally under `target/wave6/baseline/windows-x86_64`.

The actual executable reopened a disposable store after `PRAGMA user_version=600`
and `config('schema:content')='999'` were installed. It exited **0**, so a schema
marker does not fence old binaries. Activated stores require isolation plus a
tested launcher fence. That fence is a WP6.6 implementation/qualification item;
the probe is not proof that the fence already exists.

The generated .NET harness has 8 libraries, 128 small classes each, a dependent
executable, offline package sources, Release configuration, build parallelism 4,
and all `obj`/`bin` inside the tested workspace. It validates the executable's
answer, no-op compilation avoidance and incremental compilation of only the
edited library. Each scenario has one warm-up and five recorded repetitions.
No OS cache flushing or antivirus exclusions were performed. The host's
antivirus policy was left unchanged, not independently characterized.

```
python scripts/wave6_build.py --label native-wave6-baseline --antivirus-policy "Host policy unchanged; no exclusions added"
python scripts/wave6_build.py --label wave5-mounted-baseline --exe target/wave6/baseline/windows-x86_64/tkfs.exe --fence-probe --antivirus-policy "Host policy unchanged; no exclusions added"
```

| Warm median | Native | Wave 5 mounted, no peers | Ratio |
|---|---:|---:|---:|
| Clean | 4331.69 ms | 5683.76 ms | 1.312 |
| One-file incremental | 1340.22 ms | 1852.31 ms | 1.382 |
| No-op | 433.04 ms | 894.15 ms | 2.065 |

The Wave 6 no-op target is 683.04 ms on this native baseline; the Wave 5 result
does not meet it. Targets have not been relaxed. Clean mounted samples range
5667.54–6325.42 ms, so retain the individual samples and noise. These are not
p99 measurements or real-user-solution results. The baseline has Wave 5's
weaker burst-close contract; the new durability work must be disclosed in the
final comparison.

Evidence: `test-evidence/wave-6/{native-baseline,wave5-mounted-baseline}.json`,
plus CPU and disk metadata. The earlier complete build run had a shutdown
failure (`BUSY_VIEW: 19 open contexts, 0 unsaved files`) due to retained
MSBuild handles. Its failed report is preserved as
`wave5-mounted-shutdown-failure.json`. The corrected harness force-stops only
its newly created isolated worker, preserving dirty work. The rerun completed
successfully. Earlier harness errors (unsupported `dotnet clean --no-restore`,
Windows path separators in rebuild assertions, precreating a WinFsp mount
directory) were corrected; failed local artifacts remain under `test-runs`.

Remaining baseline matrix: controlled cold runs, explicit-flush comparison,
interruption/watch/remote timestamp workloads, sync-load and multi-repository
saturation. The user's actual solution has not been copied or measured.
No Wave 6 build gate has been claimed.

## Content foundation (WP6.1, in progress)

Integration ownership transfers to `src/content/{provider,local}.rs` and
`tests/content_provider.rs` for implementation of the frozen content contracts.
The modules are public library seams, not connected to mounted writes or peer
protocol. Current additions implement bounded immutable reads, streaming
construction, chunk-local constant-length overwrites, separate manifest CAS,
root pins and immutable internal snapshot handles. Multiple handles to the same
canonical store share gates, operation limits and pin ownership. Live pins keep
that ownership alive even when the provider is closed and reopened.

Fresh Windows release verification:

| Check | Outcome |
|---|---|
| `content_format` | 6 passed |
| `content_provider` focused tests | 9 passed, 1 qualification initially ignored |
| Explicit ignored 1 GiB amplification qualification | 1 passed, 3.17 s |
| Clippy for library and both affected test targets, `-D warnings` | passed |
| Python syntax checks for the new harnesses/reference encoder | passed |

Commands and source/test-executable hashes are recorded in
`test-evidence/wave-6/foundation-checks.json`. The first provider test run
exposed a measurement bug: a missing deduplication lookup charged expected
bytes as reads. Read accounting now starts only after opening and validating
the object; the rerun passed. Phase totals on failed I/O still include attempted
bytes and timers may nest; they must not be presented as exclusive OS I/O
accounting.

The 1 GiB fixture streams a repeated chunk with bounded memory. A 4 KiB
contained overwrite created exactly one new data chunk and two pages, without
whole-file hashing/reading. A boundary-crossing overwrite created two chunks.
Logical edits and unique CAS writes are distinct measurements; this repeated
fixture is not a general compression/throughput result. Other tests cover
distinct data chunks, missing unrelated content, corrupt demanded content,
forged parent/child lengths, legacy whole-file verification, full overwrite of
an absent base chunk, cancellation/deadlines, snapshot isolation, pins across
provider reopen, and a delayed/failing nonlocal provider.

`scripts/wave6_chunk_tradeoffs.py` records a deterministic 16/64/256 KiB
representation experiment in `test-evidence/wave-6/chunk-tradeoffs.json`.
For an 8 MiB source at 64 KiB, contained/cross-boundary 4 KiB overwrites need
one/two new chunks; insertion at the beginning invalidates all 129 resulting
chunks, insertion in the middle invalidates 65, and identical atomic replacement
reuses all chunk identities but full re-import still hashes 8 MiB. This is a
representation simulation, not a TKFS runtime performance gate.

Bounded reachability traversal now reads one manifest page per slice and returns
at most 256 verified edges. It reads no data chunks and retains its root pin.
A missing/corrupt parent leaves the frontier intact for retry; children are
never admitted before verifying that parent. Both focused traversal tests passed
on Windows. Durable roots and pending-save integration are still being connected.
The present code exposes no deletion/GC operation.

## Metadata and durable admission (WP6.2, in progress)

CCR-WP6.2-1 extends the contract with a distinct projected descriptor field and
reference-scoped publication provenance. Core derivation, incremental successors,
fork/publication and restore retain typed register values. Event acceptance
requires only descriptor validation; no provider is opened. Legacy strings and
historical payloads retain their meaning and serialized shape.

Root metadata and an outbox row commit with each descriptor event. A bounded
object consumer commits root effects and its own cursor atomically. Its replay
ignores older generations. Reference withdrawal takes effect against metadata
authority immediately; it does not wait for the object cursor. The maintenance
thread reconciles durable admission on a dedicated connection with zero SQLite
busy wait, independent of the semantic engine lock.

Eight admission tests passed: content-independent activation/restart,
metadata/outbox rollback on a real SQLite trigger failure, cursor/effect rollback
and replay, generation-scoped revocation, upgrade-required refusal without
quarantine, duplicate/offline conflict convergence to the causal oracle,
worker-loop replay while the semantic engine lock is held, and preservation of
an already-staged unsupported descriptor as an incompatible pending episode.

CCR-WP5.2-3 is addressed with a projection-counter proof around view publication.
Overlapping object bookkeeping keeps the patched view; overlapping metadata
invalidates it. Preparation also captures and revalidates the branch journal
generation after taking SQLite's writer. Three deterministic real-SQLite
interleavings passed, alongside the four existing commit unit tests (7 passed).
The affected Windows release checks now cover 139 integration tests plus those
7 commit unit tests (146 passed). The successful target counts are:
`activation_inbox` 22, `availability_index` 23, `content_admission` 8,
`content_format` 6, `content_provider` 11, `contracts` 41, `offlock_commit` 20,
and `startup_snapshots` 8. Two long qualifications remain ignored in these
invocations: the 300-seed off-lock campaign and the separately run earlier
1 GiB content test. Strict Clippy passed for the library, new content/admission
tests and contracts; formatting and diff checks passed.

The initial broad run stopped at the contract audit: new error codes were not
registered and adding one to the frozen historical table changed its pinned
row count. This was fixed with a named Wave 6 error extension, preserving the
historical rows, and the contract test now verifies that extension. The missing
targets, contract failures and new content tests were rerun successfully.
Commands, artifact/source identities and limitations are retained in
`test-evidence/wave-6/metadata-admission-checks.json`. These are package-level
checks, not the full final integration suite or a release qualification.

## Object engine and demand (WP6.3, in progress)

CCR-WP6.3-1 adds typed object identities, a reusable immutable page DAG,
reference-relative positions, bounded range-to-transfer preparation, receipt
recovery and generation-owned scheduling. Root admission seeds one job; page
verification/expansion happens outside SQLite before a bounded transaction
revalidates the root generation. Maintenance reconciles roots, receipt evidence
and local pages on its own connection without taking the semantic engine lock.

An initial per-reference edge prototype failed an explicit scaling regression:
a 4 KiB revision of a 64 MiB file reread five manifest pages, including unchanged
subtrees. It was replaced before transport integration. The passing regression
now permits at most the two changed pages. Global page adjacency and typed-object
availability are stored once; new revisions do not replay reused descendants,
and identical chunks do not cause unbounded verification updates. Root-relative
range lookup uses indexed child positions. Page aliases allow work to find a
different valid root after withdrawing its original witness; stale alias history
is paged with a persisted cursor, at most 256 aliases per resolution.

Serving proves membership from immutable provider pages in addition to current
metadata authorization, so a forged derived index cannot disclose a private
chunk. An explicit position can be served while the sender's graph cursor lags.
Verification alone does not make private deduplicated bytes publishable. Actual
shared receipt is checkpointed in a bounded synced intent before CAS installation
and SQL indexing. Three process-crash boundaries, an actual failed SQLite receipt
write, corrupt recovery evidence and verified repair are covered. Repair retains
the corrupt file in quarantine. Legacy reads retain their whole-file contract;
legacy descriptors are explicitly refused by the bounded chunk-frame engine.

The scheduler reserves foreground, structural and background capacity globally,
per peer and per repository. Independent waiter cancellation/deadlines survive
joined installation; stale lease/drop tokens cannot release newer work. A full
provider's 64-waiter queue does not consume another provider's queue. Actual
range-to-transfer admission under six background jobs across three repositories
passed the 250 ms bound, and source backoff allows an alternate source immediately.
That test leaves the destination admission consumer behind: demand seeds only
its committed root and obtains service without waiting for a maintenance tick.
This is engine-level controlled evidence, not the required real peer/load campaign.
Verification buffers are weighted and bounded: blocked save producers retain
foreground read and structural traversal capacity. Explicit foreground preparation
runs before background alias repair, with a shared 32-candidate/1 MiB slice budget.

The engine checkpoint's Windows release checks passed 80 tests: `content_admission` 8,
`content_objects` 14, `content_provider` 12, `content_scheduler` 5 and
`contracts` 41. The separately invoked 1 GiB provider amplification test also
passed (1.95 s). Strict Clippy, formatting and diff checks are recorded in
`test-evidence/wave-6/object-engine-checks.json` with source/executable identities.
The former failing scaling regression is now a normal passing test, not ignored.
The full integration suite and cross-platform qualification have not been rerun.

## Typed object wire adapters (WP6.3, still in progress)

CCR-WP6.3-2 connects the immutable object engine to real loopback and authenticated
peer bulk handlers. The explicit receiver-driven `pull_one` exchange negotiates
object capability, obtains a route/run/generation-owned worker offer, proves and
reads one source frame, installs it with the durable receipt path, then ACKs the
source credit. Offers and frame credits cannot be released by stale tokens or
another authenticated session. Expired unacknowledged frames close their session
before returning byte credit. New frames have their own bounded inbound reserve,
dedicated content loopback sessions and a constant-work ACK path. Normal feature
activation remains unavailable.

Real-session tests now hydrate a manifest and three chunks with both workers'
semantic engine locks held. Other cases deny unnegotiated, bare-hash and forged
private-member reads, preserve newer offer/ACK ownership, reclaim credit on
session close and real timeout, and return real SQLite writer contention within
250 ms. An authenticated foreground frame also passes the 250 ms controlled
bound while two stalled save producers hold six background verification units.
Another real-session test holds eight legacy reads across two bulk sessions,
occupying both legacy session quotas and the old worker handler pool. The v1
frame and its ACK still finish within the controlled 250 ms bound.
This is source/frame service evidence, not the final continuously scheduled
cross-repository saturation campaign.

Integration exposed two gaps that were fixed: received verified pages still
needed expansion work, and source reads must carry foreground priority through
the provider buffer gate. Per-source/lane/kind keyset scans also prevent a
backed-off prefix from hiding all later objects; a regression confirms another
object gets a real transfer offer on the next slice. Repeated empty scans leave
SQLite's change count unchanged after their initial cursor. The earlier
engine-only evidence remains in its own report; current transport checks are
recorded in `test-evidence/wave-6/content-transport-checks.json`.

This checkpoint passed 98 unique focused checks: admission 8, objects 16,
provider 12, scheduler 5, real transport 9, contracts 41 and seam stubs 7.
Three selected legacy inbound checks also passed (server-wide limits across
session churn, role/route codes and shutdown), as did a separate 1 GiB
amplification invocation (2.13 s). Strict Clippy and formatting/diff checks
passed. The report distinguishes the 97-test invocation and the subsequent
9-test transport rerun after adding the legacy-saturation regression; overlapping
tests are counted once. These are package checks, not the full final suite.

## Continuous typed-content dispatch (WP6.3, in progress)

CCR-WP6.3-3 registers every outgoing bulk runner with a bounded daemon-wide
dispatcher. Four preparation threads, including two foreground probes, are
separate from four network threads; planner loopback sessions are separate from
frame/completion sessions. All admitted frames retain the common scheduler's
class, peer, repository and byte reservations. A slow network operation never
owns a preparation thread or the old two-repository turn gate. There are at most
four queued frames, 256 lane bindings and 512 working repository groups, with a
rotating deferred window. Source polling and worker/capability caches are bounded.

Real authenticated sessions and actual Store/CAS workers now exercise automatic
hydration through `objects::run_bulk_lane`. A three-repository test actually
stalls two background source reads for three seconds, then requires foreground
admission within 250 ms and its bytes before those reads finish. A complementary
case requires background bytes while a foreground source is stalled. These are
short controlled checks; the required 30-minute mixed-load run remains pending.

The first reconnect regression failed: an old source read held its transfer
slot until injected two-second worker I/O returned, even after closing its peer
session. The original one-second replacement gate was retained. A per-request
cancellation bridge now releases that source lease promptly while preserving
unrelated calls on the shared worker session. The exact regression then passed
in 0.30 s; the subsequent full transport run also passed. Outgoing capability,
read and completion calls observe cancellation too. Another test keeps the lane
registration alive to prevent socket-close cleanup from hiding the bug: lane
and daemon cancellation drain actual transfer ownership within 250 ms while the
source I/O is still stalled. Late registration Drop cannot remove a newer lane.

Preparation now advances an unaligned 1 MiB demand through its seventeenth
chunk instead of reverifying the first 16 until every slice's byte budget is
exhausted. The regression also removes a real cached leaf and root page, observes
their absence, invalidates derived availability, and refetches only the missing
object in each case. Required manifest pages participate in completion checks;
verified cached leaf adjacency alone is insufficient. Waiter windows rotate, and
lane filtering precedes the foreground candidate cap. V1 per-op body limits
are now 64 KiB for read/complete and zero for other v1 operations. Historical
operation limits remain unchanged.

The dispatcher view exposes bounded per-class admission/completion/failure/retry
counters and errors without starting content state on observation. Cursor-only
movement is not reported as verified progress. Full user-facing engine status
and bounded error-episode integration remain WP6.6.

Verification and source/executable identities for this checkpoint are retained
in `test-evidence/wave-6/continuous-dispatch-checks.json`. The initial reconnect
failure and its fix are recorded there; no timing gate was loosened.

The broader legacy lane check also exposed a separate real cleanup bug. The
600-object cross-source handoff regression failed twice in the serial lane
suite, although its isolated retry passed in 0.81 s. Bounded failure diagnostics
on the second run found 139 objects still leased to the stopped source's two
pull slots, with no exchange errors. A want could regrant an already-begun failed
object under the other slot; the duplicate check skipped it before backoff
handling, while settlement remembered only its originally planned slot. The
backoff check now precedes duplicate suppression, returning that regranted lease
under the slot that actually received it. The existing 40-second availability
and 15-second completion bounds are retained. This fixes that orphaned-lease
case; it does not provide generation fencing for a later lane run.

After that fix, the full serial legacy lane target passed 53 tests (18 long
qualifications ignored) in 88.91 s. The final focused content invocation passed
104 tests: admission 8, objects 17, provider 12, scheduler 5, transport 14,
contracts 41 and seam stubs 7. Session-core 59 and inbound 20 passed earlier in
this checkpoint, before the last legacy duplicate cleanup; their executable
stages are identified in the report. Strict Clippy and formatting/diff checks
passed. The final controlled samples measured foreground admission at 29.54 ms,
background availability under stalled foreground at 61.81 ms, lane/daemon cancel
drain at 57.84/27.74 ms, and replacement availability at 97.81 ms. Each sample is
one package-test observation, not a percentile or sustained-load result.

## Ordered retained pull ownership (WP6.3, in progress)

CCR-WP6.3-4 addresses CCR-WP5.INT-2 in the retained whole-file pull path.
Each lane uses generation-qualified local lease IDs. Both slots share a fence
ordered by a durable daemon incarnation and a monotonic lane-run number, while
retaining separate IDs for pipelining. A new run resumes immediately from the
durable spool offset. It neither inherits old cleanup authority nor waits for
the old lease's 60-second expiry. No peer byte format or whole-file identity
changes, and this does not activate typed metadata/content.

The catalog commits its next incarnation under its existing exclusive daemon
lock with SQLite synchronous=FULL before lanes start. A real child process exits
immediately after incarnation 2 commits; reopening advances to 3. Malformed and
exhausted values fail closed. Concurrent lane allocation is monotonic without
wall-clock dependence. The worker retains at most 16,384 scope fences and refuses
new scopes when full without evicting a fence; existing scopes still advance.

Worker tests hold a want after its SQL scan and a write after durable byte work,
then admit a newer run. Delayed old requests cannot reclaim, renew or release
the newer lease or alter its source backoff. Stale and unregistered-future
releases are harmless. Returned supersession is `IN_FLIGHT_ELSEWHERE` ownership
contention, avoiding a failure-breaker event and preserving the historical
cancellation code's classification. An actual authenticated bulk reconnect with
real Store/CAS workers obtains a 150,000-byte legacy file before the old source's
two-second read finishes; the first measured availability was 118.45 ms against
the unchanged one-second package bound.

The first complete package run passed 180 checks: catalog 21, real content
transport 15, contracts 41, generation/parser/crash checks 4, legacy lanes 53
and worker object tests 46. The 18 existing long lane qualifications stayed
ignored. The default 10,000-object worker test ran, with 40 writes and 28.6 s
measured transfer work; it is not counted as a mounted-build qualification.
This run preceded the final supersession error-class adjustment. The subsequent
affected checks and exact source/executable stages are recorded in
`test-evidence/wave-6/lease-generations-checks.json`; the unrelated expensive
10,000-object case is not repeated solely for that error-class change.
The final-source rerun passed 105 checks (transport 15, contracts 41,
generation/parser/crash 4 and worker objects 45, with that one already-passed
worker case filtered). The actual returned stale-call class is asserted not to
count as failure. Native legacy reconnect availability was 113.86 ms. Strict
Clippy and formatting/diff checks passed. These are package checks, not final
Wave 6 platform/load/build qualification.

CCR-WP6.3-5 below covers the next retained legacy cross-repository foreground
admission step. The retained acceptor push-offer ownership still needs audit,
followed by mounted range demand and recoverable private saves. The complete legacy
job/spool/lease budget audit also remains; the new fence cap alone does not
establish those bounds. Sustained fairness,
shutdown/reconnect load and large working-set latency still need qualification.
Catalog restore while adopting an existing worker also needs a WP6.6 fence;
restoring an older counter must not silently reuse its lease incarnation.
Metadata descriptor capability negotiation is required before descriptors leave fixture stores;
the object capability probe alone does not satisfy it.

Mounted adapters/write-session recovery, caching/journal, metadata capability
negotiation, deployment fencing and activation (WP6.3–6.6) and integration
qualification remain pending.
In particular, explicit root-pin inventory is not a complete retention audit and
does not authorize deleting any object.

## Reserved retained whole-file demand (WP6.3, in progress)

CCR-WP6.3-5 adds a foreground compatibility adapter to the fixed content
dispatcher. It observes actual Store demand independently of the folder
heartbeat and typed-content activation. Each preparation selects one demanded
identity through indexed SQL with zero busy timeout and a 50 ms reply deadline.
The existing background two-repository gate remains in effect; foreground work
uses the shared scheduler's reserved capacity and bounded network worker pool.
Transfers are capped at 256 KiB with 64 KiB reads/chunks and a charged one-MiB
job. Whole-file parts remain private until the native verifier installs them.

Unique ordered `c` frame IDs share the background lane's parent run generation.
They promote the demanded object without fencing unrelated background work.
Native worker tests resume from the durable offset, refuse an older active
frame's writes/wants, and prove that delayed frame/parent cleanup cannot release
or apply source backoff to its replacement. Existing native privacy tests now
exercise the new bounded source-read operation as well as `objects.read`.

The optional source reservation returns an exact single-use credit before
streaming. A receiver ACK during active streaming retains the source budget;
the active handler also holds an independent permit reference through session
close, expiry or shutdown. An authenticated native transport test covers
abandoned credit, stale ACK, duplicate use, active ACK and byte/credit receipts.
Unsupported old peers retain their historical pull path, without a claim that
they provide the new source reservation.

The actual three-repository regression stalls two nonempty background files
for two seconds under one daemon/peer/lane, then admits new metadata and demand
for the third without changing its published folder heartbeat. Its 1,048,599
bytes require five foreground frames. With typed activation disabled, admission
was 22.06 ms against the unchanged 250 ms bound, and verified availability was
219.43 ms against the one-second bound. The pre-disabled-fixture run was
59.48/248.61 ms. The first successful focused invocation passed 108 checks: transport 17,
contracts 41, parser/catalog crash 4 and native worker 46. Its one previously
passed 10,000-object case was filtered, not counted again. Further regression
results and exact source/executable stages are retained in
`test-evidence/wave-6/legacy-foreground-checks.json`. The final regression run
passed transport 17, retained legacy lanes 53 and session core 59. Across both
invocations, 220 distinct checks passed; the repeated transport tests are counted
once. The 18 existing long lane qualifications remain ignored. Strict Clippy,
formatting and diff checks passed. Source-set SHA-256:
`7f7b8c920be5f070dedfbd4e246d9213fc186045d9e1e525de5dda8caedb60e3`.

Test corrections are explicit: the original naive stall setup accidentally
spent faults on empty historical revisions; the corrected setup verifies shared
empty receipts first and asserts both real background files are still missing
and leased. That corrected pre-adapter case failed the 250 ms admission bound.
The larger transfer initially completed all five frames but its verifier asked
the provider for more than its one-MiB read limit; bounded verification reads
corrected that test. Neither failure was addressed by relaxing a timing bound.

This is short native package evidence for CCR-WP5.INT-1, not the final sustained
fairness/platform/build gate. Still pending: full retained legacy job/spool/lease
budget and acceptor push-generation audits, cancellation within legacy
first-serve hashing, large working-set/idle-poll overhead, mounted range demand,
recoverable private saves and WP6.4–6.6 integration/qualification. Ordinary
typed-content activation remains off.

## Pull-pool ownership across lane counts (WP6.3, in progress)

CCR-WP6.3-6 gives every real peer supervisor an ordered pool identity before
starting its lanes. Lanes and reconnects retain that identity; a replacement
supervisor receives a newer one. The local opaque pull ID appends the pool
counter, including foreground frame IDs. A delayed lane of an older context
cannot acquire a new pool merely because its run starts later. Production
contexts refuse unregistered pools; fixtures without a supervisor use the
documented lazy registration path.

Worker fences now order daemon incarnation, pool and lane run separately. This
closes the count-dependent scope gap: changing the bulk lane count, and later
returning to a previous count, cannot make an old layout current again. It also
handles old-layout requests for a shard not previously seen by the worker.
Canonical classic IDs and the previous qualified form remain recognizable;
after a newer family is claimed, they cannot release its leases or change its
source backoff. The historical opaque session behavior, including its loose
lane-name interpretation, remains for classic requests before a new family
fence is established. A classic opaque name resembling another peer's lane is
still opaque, as it was previously. New qualified requests retain strict peer
and lane validation.

Both worker family/run tables and the daemon context registry have explicit
16,384-entry bounds. Worker fences are never evicted for capacity. Existing
owners can advance while full. The daemon registry only reclaims dead weak
contexts, so a live old lane retains its original identity. Native tests cover
capacity, delayed higher-numbered old runs, count reversal, daemon restart,
stale writes/releases/backoff and malformed/bounded IDs. Expiry of an already
superseded lease no longer backs off a healthy replacement source.

The authenticated native reconnect test now explicitly establishes a verified
empty-history receipt before arming its read stall, then asserts the actual
nonempty file is still missing and leased. The same helper exercises a real
one-to-two-lane resize. The first resize sample restored 150,000 verified bytes
in 9.25 ms while the predecessor's two-second read was still in flight. The
tests retain the one-second limit, verify the newer worker fence after old
cleanup, and exercise the existing foreground-demand regression as well.

Final-source checks passed: authenticated transport 18, native worker 51,
generation/parser/catalog-crash 7 and contracts 41 (117 total), plus strict
Clippy and formatting/diff checks. The latest actual pool-resize availability
was 8.55 ms; unchanged-heartbeat foreground admission/availability was
21.87/214.98 ms for five frames with typed activation disabled. The preceding
broader invocation passed 198 checks, including retained lanes 53 and peer
supervisors 30. Across both stages there are 200 distinct passed checks; the
evidence explicitly separates the earlier lane/supervisor executables from the
final four targets. The 18 long lane qualifications remain ignored, and the
already-passed 10,000-object worker case was filtered. These do not constitute
the final platform/load/build gate.

`test-evidence/wave-6/pool-generations-checks.json` retains commands, raw outputs,
samples, source/executable hashes and stage limitations. It also records the
caught classic-lane compatibility regression and the failed concurrent Windows
link attempt; that attempt was retried only after both earlier tool processes
were observed terminal. Final source-set SHA-256:
`82483906af8734f7a0a8c9649480bbbd93b3d216a2cbbc0659b45afcc546eb1f`.

The pull pool's local `PeerContext` identity is not an authenticated remote
push-pool identity. CCR-WP6.3-7 below adds the separate inbound ownership
lifecycle. Full legacy budgets, hash cancellation, mounted reads,
recoverable saves, final saturation/build/platform qualification and WP6.6
compatibility/activation remain pending.

## Authenticated inbound push ownership (WP6.3, in progress)

CCR-WP6.3-7 replaces stable local inbound push lease IDs with authenticated
session, acceptor pool and per-offer frame identities. Sessions receive their
root before serving requests; old open sessions retain their original root.
Each new offer advances its frame, and the native worker keeps that frame floor
after object leases are released. Reconnect, count reversal, delayed writes,
late wants and exact cleanup cannot restore an older owner. The acceptor orders
pool-count changes when authenticated connections become admissible; the
unchanged legacy hello does not attest a persistent sender-side pool epoch.

Shared weak ownership slots coordinate offers for a worker/peer/lane. The slot
stays alive while an offer or handler can still finish. Publication and short
backoff/status mutations revalidate session/frame and pool, without holding
ownership locks across worker RPC, hashing, streaming or durable I/O. A late
reply cannot overwrite a newer offer, even after that newer offer was consumed.
Old reaper RPCs use exact IDs and no longer block replacement preparation.
There are at most 4,096 slots and 16,384 acceptor peer-pool identities; existing
identities keep working at capacity. Pruning removes expired offers/dead weak
slots and rechecks concurrent creation. Closed-session cleanup cannot remove a
replacement session's offer table entry.

Actual authenticated peer/loopback tests with real Store/CAS workers exercise
a two-second old release, a two-second delayed offer publication, and a bulk
pool resize while the old session remains open. The new owner pushes and
verifies 150,000 bytes with typed activation disabled. Initial same-count
admissions were 0.44/0.43 ms against the unchanged 250 ms bound. Native worker
tests cover frame/session/pool precedence, late writes/releases, the retained
frame floor and another source acquiring a predecessor's remaining object.

The first broader run caught an unknown-size handoff: a new frame's exclusion
did not physically remove its predecessor's old lease. Fresh wants now retire
excluded leases only when their owner is already superseded, without an extra
release-RPC wait, and preserve a current other source's lease. A native test
asserts both cases. Legacy lane fixtures retain exact qualified strings in
their lease tables and release logs; only historical peer/lane assertions
normalize their view. The delayed-reap regression now asserts prompt admission
and that the old release finds no ownership of the replacement.

The native worker also rechecks ownership at the source-backoff mutation after
an old corrupt-push verdict. A two-second pause after collecting that verdict
allows a replacement to install verified bytes first; the old result then
cannot recreate its memory retry. The replacement completed in 5.43 ms in the
final sample. Durable local-failure SQL projection and same-run foreground
error interleavings still need the full legacy audit.

The complete ownership invocation passed 319 checks. After adding that final
mutation guard and its native regression, the four affected targets passed
127 checks; there are 320 distinct checks across both source stages. Both
stages passed strict Clippy. The evidence retains separate manifests and
executables in `test-evidence/wave-6/push-generations-checks.json`, rather than
claiming the earlier library/lane/supervisor executables contain the last guard.
Final source-set SHA-256:
`a9a0154c02a2829eb2e6dd0d74138452f14ff2e058a22b1f1551466de501166b`.
The 20 ignored checks (18 long legacy lane scenarios and two library probes)
and one filtered, previously qualified 10,000-object worker case remain
explicitly recorded. This is not the final platform/load/build gate.

This work does not establish the remaining full legacy job/byte/spool budget or
close-release helper fanout bounds. Those, hash cancellation, sustained fairness,
mounted range reads, recoverable private saves, caching/change journal,
capability/deployment fencing and final Windows/FUSE/build/load qualification
remain pending. Ordinary typed-content activation remains off.

## Legacy read cancellation (WP6.3, in progress)

CCR-WP6.3-8 makes legacy first-serve SHA-256 verification interruptible between
64 KiB reads. Ordinary loopback object reads and the reserved foreground legacy
read borrow the request's deadline and cancellation/disconnect signal and also
observe worker shutdown. Foreground permit waits check the same request. The
checked Store range seam checks around bounded byte reads and rechecks the
committed shared reference/shareable triad after potentially long verification.
An interrupted hash does not become trusted, quarantine a good file or create
an object retry. The current filesystem call itself remains OS-bound.

Retained background push and inbound pull now pass their stop condition into
the ordered worker read stream. An empty result window wakes to check it, and
stopping the consumer cancels other pending calls. The existing read-window
threads remain bounded by their caller's configured window. Refusals that ran
nothing retain their historical retry, within the original deadline, with
interruptible backoff. Request cancellation preserves unrelated users of the
shared loopback connection. Dial cancellation and the complete retained legacy
thread/byte/spool budget remain unqualified.

The first source stage passed 174 checks: library 85, provider 12, authenticated
transport 23 and native worker 54. Its six actual worker cancellation scenarios
use a 3,145,741-byte untrusted file, ordinary/reserved reads, deadline expiry,
disconnect and force stop. Each entered the actual hash before cancellation;
the tests observed handler/permit return after the injected 250 ms I/O stall,
unchanged availability markers, no object retry/quarantine, and a subsequent
read still requiring verification. That stage preceded the background stream
bridge and sharing-withdrawal scenarios; final qualification is recorded below.

The final source passed 337 checks: library 85, provider 12, authenticated
transport 25, contracts 41, inbound 20, generations 10, retained lanes 53,
native worker 54, peer supervisors 30 and seams 7. All nine new native hash
scenarios passed, as did strict Clippy for all test targets and formatting/diff
checks. External background cancellation and consumer stop returned
in 30.30/35.55 ms while actual hash I/O was paused for 400 ms; the slow handler
was observed returning separately. The six ordinary/reserved deadline,
disconnect and shutdown cases drained in 250.39–252.95 ms with a 250 ms injected
storage stall. Sharing withdrawn during first-serve verification returned no
bytes. Cancellation remains cooperative across individual storage calls.

The unchanged-heartbeat legacy foreground scenario still admitted within
40.35 ms and made all five frames available within 237.30 ms while two other
repositories stalled for two seconds, with typed activation disabled. These
are single controlled samples under the retained bounds, not sustained fairness
or final performance qualification. The 21 ignored checks (18 long legacy lane
scenarios, two library probes and a large provider experiment) and one filtered,
previously qualified 10,000-object worker case remain outstanding.

`test-evidence/wave-6/legacy-hash-checks.json` retains command results, fault
configuration, native samples, source/executable manifests and raw development
failures. Its 364-file manifest includes src/tests/native/ui and Cargo/build
inputs, and therefore differs in coverage from earlier checkpoint
manifests. Source-set SHA-256:
`1324fe18e7a6d400b7e6a56e48d3c3c5290b9606a0d20cb7b3ebbcc0bfc35958`.

Full legacy budgets, bounded close-release work, durable local-failure error
projection, mounted range demand, recoverable saves, the change journal/cache,
compatibility/retention/activation and final platform/build/load gates remain
pending. Ordinary typed-content activation remains off.

## Bounded offer cleanup and closed-session authority (WP6.3, in progress)

CCR-WP6.3-9 replaces a thread per closed inbound session with two process-wide
release workers. Close only marks entries in the existing 4,096-offer table and
wakes the workers. There is no separate queue of copied release batches.
Oldest eligible offers are claimed first; two guarded client release attempts
may run at once, including explicit sweeps. Closed offers are removed when
claimed, silent live offers retain their historical released marker, and every
RPC carries the exact qualified lease ID. The guards survive RPC and unwind.
Workers retry missing startup capacity at most once per second on later offers
and contain an attempt's panic. Their fixed process lifetime is explicit.

The authenticated registration guard also retires its push session before
unregistering. Close and guarded offer/status publication use the same short
pool gate. This prevents a delayed want from publishing after close even when
the close initially saw no offer. Sibling sessions and pool identity remain
independent. An already-admitted data prefix may still become durable under
its original, unsuperseded lineage; it then releases only its own lease.
Closing authority does not discard that valid prefix or create source backoff.

Final source passed 329 checks: library 85, authenticated transport 28,
generations 11, native worker 54, retained lanes 53, inbound 20, peer supervisors
30, contracts 41 and seams 7. Strict Clippy compiled/linted all test targets;
formatting and diff checks passed. The 20 ignored cases (18 long legacy lane
qualifications and two library probes), the previously qualified filtered
10,000-object worker case and the unchanged provider target are not claimed
executed here.

Actual native evidence includes 22 authenticated want/close connections with
both release workers stalled for 1.5 seconds. The additional 20-connection burst
completed in 21.32 ms; peak want admission was 0.45 ms against the unchanged
250 ms bound. Active cleanup and the fixed thread count stayed at two, an
explicit sweep could not bypass the cap, a pending closed offer stayed leased,
and it drained after 1,488.97 ms. Reconnect then installed 150,000 verified bytes
without source backoff. A separate late-publication case closed before offer
publication and left no lease after 1,507.08 ms, without a replacement owner.
The prefix regression paused after admitting 65,536 bytes while no spool offset
existed, closed the session, observed exactly that durable offset and no lease,
then resumed and verified the complete 150,000-byte file. All used legacy
content with typed activation disabled. These are controlled samples, not the
final sustained-load gate.

`test-evidence/wave-6/offer-cleanup-checks.json` retains commands, native samples,
faults, raw outputs, source/executable identity and remaining qualifications.
Its 364-file source manifest matched before and after the final invocation:
`d4249bc4c27548ab0e5dce8d8b2f43b670e3e9e8ff1e456b8f40d02bbea60b5c`.

This change qualifies the release helper count and client attempt ownership,
not all retained legacy memory, job, connection or shutdown budgets. A timed-out
client attempt does not prove its remote handler already stopped. Large-backlog
tails, cleanup fairness across worker instances/repositories, durable failure
projection, the other legacy budgets, mounted saves/ranges, cache/journal,
compatibility/retention and final platform/build/load gates remain pending.
Ordinary typed-content activation remains off.

## Work ownership through error projection (WP6.3, in progress)

CCR-WP6.3-10 extends each legacy worker object lease with a separate work ticket.
The writer retains it through verdict handling and SQL/source retry projection,
after the ordinary lease can already be released. A new grant replaces the
ticket even if it reuses the same session ID or changes only the foreground
frame within one parent run. Finishing the replacement cannot revive the old
ticket. Release/exclusion/expiry retires the matching ticket. Weak object and
source-family registries have 65,536/16,384 entry limits and reclaim only dead
entries; active leases/writers retain their coordination state.

Memory retry mutation checks ticket and run under the worker state lock.
Local SQL retry projection takes source-family/object gates and rechecks
ownership, then releases the shared state lock before SQL. Claims use
nonblocking gate acquisition and explicit `WORKER_BUSY`/resource contention.
Hashing, copying and spool/verification I/O stay outside these gates. The
projection connection uses zero SQLite busy timeout. A verified available row
also wins over a collected local failure within one savepoint, protecting an
independent install and snapshot-upgrade contention. Direct Store failure
semantics and wire/schema shapes remain unchanged.

The broader source passed 336 checks: library 88, authenticated transport 29,
native worker 57, generations 11, retained lanes 53, inbound 20, peer supervisors
30, contracts 41 and seams 7. The implementation was then unchanged while two
stronger worker regressions were added; the final worker suite passed 59 checks.
There are 338 distinct checks across both stages. Strict Clippy for all test
targets and formatting/diff checks passed. The 20 ignored historical/probe
cases, filtered previously qualified 10,000-object case and unchanged provider
target are not claimed executed here.

The actual authenticated native peer/loopback case installed 150,000 verified
bytes in 11.58 ms while an old real spool-open failure paused for two seconds;
the old verdict left no SQL retry. Worker interleavings cover same session,
same-parent foreground frame, another source, independent verified install and
explicit cancellation. The stronger claim-only test admitted the same session
in 1.12 ms and kept its lease/error state intact while replacement bytes were
still absent, so the available-copy shortcut cannot mask a ticket failure.
During an already-prepared one-second SQL commit pause, WAL/read service took
0.97 ms and the shared worker/engine locks stayed free. A different source's
claim of the same object was deferred while another object's claim succeeded
in 1.05 ms. Those retained the 50 ms service bounds. Same-parent stale source
retry was also rejected after a 3.52 ms verified replacement. These are
controlled correctness/service samples, not a full saturation gate.

`test-evidence/wave-6/effect-ownership-checks.json` retains both 365-file source
and executable stages, commands, raw logs and fault/sample details. Broader
source SHA-256:
`94b6a419d841b0509500703ac34b444dbc162ff2b9f6cf0548eaaa2aecc51424`.
Final test-only follow-up SHA-256:
`a97dc84bde59600137d3103009b1e5eb89b3a25c1677ec388b0151d32c9a4447`.
Only `tests/objects_v3_worker.rs` changed between those stages; implementation
and other tests stayed byte-identical, and the CLI binary hash was unchanged.

This closes the examined worker failure-projection windows, not every legacy
effect. Historical reused `a`/`b` or opaque release IDs still lack an exact
per-round token after uncertain RPC timeout; daemon foreground task/status
mutations need their own audit. Full live-capacity/resource stress, legacy
job/byte/spool/connection budgets, long cleanup/fairness/shutdown, mounted COW
reads/recoverable saves, cache/journal, compatibility/retention and final
platform/build/load gates remain pending. Content activation remains off.

The capacity audit must also address the pre-existing legacy `grant` path's
`holders.clear()` at 65,536 entries: an existing-hash regrant at capacity can
forget other live leases' source provenance. The new effect registries do not
waive or qualify that behavior.

## Bounded lease ownership and exact download rounds (WP6.3, in progress)

CCR-WP6.3-11 repairs the legacy capacity gap identified above. An existing-hash
regrant no longer clears other hashes' source provenance. Both lease and holder
tables are bounded at 65,536 keys. Holder cleanup removes only orphaned or
mismatched entries; one bounded grant batch can reclaim up to 256 already-fenced
owners in one pressure scan, without source backoff. Live opaque/current owners
remain retained. A released lease's delayed writer still retains its effect
slot, so exhaustion continues refusing a new key until that proof drains.
Write admission preserves its contention/resource error per refused item,
without spool I/O or source/local failure projection.

Supporting workers now advertise a local, read-only legacy capability independent
of typed activation. A download round keeps one fresh nonce across its two a/b
IDs and all its wants/writes. Worker round floors are independent per slot,
retained after release, and bounded to 16,384 lane scopes. Advancing a cannot
retire b or foreground c work; foreground c can promote background work even
when its own frame counter is smaller. Kept leases and all settlement/exit
cleanup retain their exact acquired session string. This closes the examined
uncertain-release gap for negotiated round ownership.

The daemon caches negotiation by immutable worker instance, up to 16,384
instances per lane run. `UNSUPPORTED_OPERATION` retains the previous stable IDs
so older workers can continue short rounds; transient probe failures are retried
and version mismatch is `PEER_NEEDS_UPGRADE`. The unsupported-worker and
transient-failure cases were exercised with the explicit fake-worker harness.
Real W5 executable interoperation remains a separate qualification. Historical
or arbitrary opaque IDs on unsupported workers keep their previous ambiguity
after an uncertain release; the round-fence promise is conditional on negotiation.

Final Windows release verification passed **350 distinct checks** in one final
invocation: 91 library, 32 content transport, 41 contracts, 20 inbound, 12 lease
generation, 54 lane, 63 worker, 30 peer supervisor and 7 seam checks. Strict
Clippy passed for the library and all test targets; formatting and diff checks
passed. Two opt-in library probes and 18 long lane qualifications remain ignored;
the historical 10,000-object worker probe was filtered from the final invocation.
An earlier draft passed that probe, which does not waive the final retained gate.
The unchanged provider target was not rerun in this checkpoint.

Controlled native samples from that final source:

- Full-table grant tests retained all 65,536 owners, with a
  14.46 ms pressure scan, and reclaimed one fenced owner
  in 0.50 ms. A real Store-backed worker RPC
  test with seeded full in-memory lease/holder tables refused both want/write,
  created no spool and added neither source nor local retry. This is boundary
  evidence, not 65,536 concurrent native transfers or a full memory/load gate.
- A native release handler stalled 2,500 ms past its 2,000 ms client deadline.
  The next round acquired its lease in 0.35 ms;
  the delayed handler then rejected stale authority and could neither release
  the new lease nor add its 60-second hold. The replacement verified 150,000 bytes.
- An authenticated peer download verified 300,000 bytes in
  242.73 ms across 5 observed
  round IDs beneath one unchanged lane run, with a 60-second heartbeat and no
  source backoff. Native a/b floor, foreground promotion, retained-floor and
  full round-scope capacity regressions also passed through real worker dispatch.

The first broader attempt passed library/transport tests but the contract audit
caught an unregistered version error name and a unit-test measurement label.
The version error now uses the existing upgrade-required code; the measurement
label is explicitly listed as non-code. The final invocation reran all nine
targets after that correction. Development failures/results are retained and
are not added to the final distinct count.

A separate intermittent automatic-dispatch assertion was diagnosed rather than
accepted after a lucky retry: the manifest's CAS installation succeeded, then
its receipt-index write returned `SQLITE_BUSY`. Recovery could hydrate the file
without a successful manifest RPC callback. Waiting for that callback counter
could never succeed. The in-process RPC fixture lacks normal worker maintenance;
it now drives the same bounded receipt-recovery component while no frame runs.
The test requires complete byte verification, complete range indexing, verified
public receipt evidence for every job and actual structural/background network
admissions under the unchanged five-second limit. A new native case holds the
WAL writer after CAS installation, observes the rejected COMPLETE and proves
the retained receipt recovers. Production counters and receipt code are unchanged.

`test-evidence/wave-6/round-ownership-checks.json` retains raw outputs, the
365-file implementation/test/build manifest, executable hashes, commands,
controlled samples and the pre-correction source stage. Final source SHA-256
(sorted path/hash records):
`7923e8b41bc4175728ae1f3d048380936e8c0365eabc5462841f9297c93a116b`.

Full WP6.3 physical job/byte/spool/connection/remote-handler budgets and daemon
foreground-task/error/status mutations still require audit. Large cleanup
backlogs, cross-worker/repository fairness, real thread startup failures,
complete shutdown/cancellation and sustained 30-minute load/recovery remain
pending. Mounted COW reads/recoverable write sessions, cache/change journal,
compatibility/downgrade/deployment fences, retention/reclamation and the matched
platform/build/real-user-solution gates also remain pending. The full Wave 6
goal stays active and ordinary typed activation remains off.

Two concrete follow-ups remain in that audit: distinct round IDs can increase
settlement/exit release RPC fanout across the bounded kept set, requiring retired
ID pruning or bounded asynchronous cleanup; and the typed dispatcher's
`SQLITE_BUSY` failure statistics need contextual readiness/recovery treatment.

## Confirmed round cleanup and SQLite readiness (WP6.3, in progress)

CCR-WP6.3-12 addresses the two concrete follow-ups recorded above. The existing
16,384-worker-instance capability cache retains at most two confirmed round
floors per worker. Only a successfully decoded want reply, including an empty
reply, confirms its floor. Allocation, malformed/refused/timed-out/in-flight
calls do not. Delayed older replies cannot regress a confirmation.

Settlement and exit cleanup drop only IDs strictly below a confirmed matching
slot/peer/lane/incarnation/run/pool floor at the same worker instance. Current
and future IDs, other lineages/slots, foreground c and opaque IDs stay retained.
Surviving releases keep their exact acquired IDs. Worker/remote-replica
replacement cleanup retains the old endpoint and route, while a stable ID
actually reacquired on the same worker settles with its current round. Daemon
stopping still preserves worker adoption. Successful wants therefore bound the
producer's surviving round history to its two slots, instead of accumulating a
release session for each old round.

The typed dispatcher now treats zero-wait `SQLITE_BUSY` as contextual readiness:
retry after the existing 25 ms delay, increment retries and leave failure
statistics unchanged. The global/wire error table remains frozen. The forced
post-CAS WAL conflict test requires real receipt recovery, complete verified
bytes/range indexing/public receipt evidence, readiness retries and zero
dispatch failures. The automatic fixture also now waits for dispatcher startup
within its original deadline rather than unwrapping a not-yet-created view.

Verification passed **353 distinct Windows release checks**: 94 library, 32
content transport, 41 contracts, 20 inbound, 12 generation, 54 lane, 63 worker,
30 peer supervisor and 7 seam checks. Strict Clippy passed for all test targets;
formatting and diff checks passed. Two opt-in library probes and 18 long lane
qualifications remain ignored, and the historical 10,000-object worker probe
was filtered. Unchanged provider/content-object targets were not rerun here;
no retained gate is waived.

The native cleanup boundary seeded 4,096 real worker lease/holder records,
negotiated capabilities and advanced actual a/b worker floors through successful
empty loopback wants. The production grouping helper skipped 4,094 retired IDs
and kept two current hashes. An uncapped native fault counter observed exactly
two release RPCs, each stalled 100 ms; cleanup took 201.72 ms
against its 1,000 ms controlled bound, with no source backoff. This is a seeded
worker/protocol boundary, not 4,096 real producer rounds or a complete
shutdown/fairness/saturation gate. The ordinary lane lifecycle/resumption tests
also passed. The forced receipt conflict recovered 1
intent and verified 150,000 bytes with failure statistics
`[0, 0, 0]` and readiness retries `[0, 3, 9]`.

After the nine-target run, exactly one line inside the private library test
module changed: the hit counter became uncapped, so unexpected extra release
calls cannot hide after two hits. All 94 library cases and strict Clippy passed
again. All production bytes and other source files stayed identical; the CLI
and other test executable hashes were checked unchanged. Repeated library
checks count once. `test-evidence/wave-6/retired-rounds-checks.json` retains both
365-file source/executable stages, exact commands, raw outputs and development
failures. Broad source SHA-256:
`c3834f02b7b4108aaf2b22009edd804847a223be487946c2fd9c8bb8702c618c`.
Final test-only observation SHA-256:
`b5566e5fc79847694ad79d7564ab5e16631edaf61fae09a348760aa957a668e0`.

Large current batches still require paged releases. With 4,096 retained hashes
and 256-hash pages, two current sessions can require up to 17 existing attempts;
individual RPC deadlines do not prove a global shutdown-time bound. Complete
legacy physical job/byte/spool/connection/remote-handler budgets, asynchronous
cleanup/fairness, late foreground preparation, other daemon error/status effects
and error episode resolution still require audit. Real old-worker interoperation,
thread startup failure, sustained 30-minute load/recovery, mounted COW/durable
write sessions, cache/journal, compatibility/retention and final platform/build
gates also remain pending. The full Wave 6 goal stays active and ordinary
typed activation remains off.

## Foreground preparation and source-error ownership (WP6.3, in progress)

CCR-WP6.3-13 closes three foreground legacy gaps. A delayed native
`LEGACY_PREPARE` now checks its actual incoming deadline, cancellation or
disconnect, and worker shutdown before selection and grant, and after grant.
A failed final check removes only that exact session's grants. Parent/round
authority never rewinds; the diagnostic proving request retirement runs outside
worker/family guards. Ordinary legacy WANT and direct JSON dispatch do not gain
the full loopback request-context contract here.

Each foreground Task now captures an object/source-bound proof. Concurrent
sources share the epoch before the next verified local install, but cancelling
one attempt leaves other sources current. Proof validation and daemon backoff
mutation share the existing short lock. A daemon verified-install notification
retires all earlier incoming attempts for that object and clears source history.
Dropping a newer proof cannot resurrect an older one. The weak epoch table has
16,384 keys; pressure reclaims only dead entries, and live capacity refuses a
new root. Cancelled in-flight Tasks retain their slot until physical drop.
I/O, verification and cleanup RPCs stay outside this lock. Binding and daemon
cancellation use short-lived child tokens; their hooks cannot accumulate on
the long-lived daemon token.

The Task rechecks ownership before writes, after the peer reply and before
error/success projection. It retires its attempt before cleanup and selects a
source hold only while current and uncancelled; its scheduler reservation stays
held through the existing release attempt. This is not remote-handler retirement
acknowledgement. Local `Progressed` means a private prefix. Only local verified
`Completed`/`Skipped`, after stream and peer-receipt validation, can clear the
whole-file history. A successful partial foreground frame no longer claims a
whole-file install.

Verification passed **359 distinct Windows release checks** in one final
nine-target invocation: 97 library, 35 content transport, 41 contracts, 20
inbound, 12 generation, 54 lane, 63 worker, 30 peer supervisor and 7 seam checks.
Strict Clippy for all test targets, formatting and diff checks passed. Two
opt-in library probes and 18 long lane qualifications remain ignored; the
historical 10,000-object worker probe was filtered. Unchanged provider/content
object targets were not rerun, and no retained gate is waived. Earlier focused
runs count only as development evidence.

The final native evidence includes:

- Six actual loopback prepare cases: a 250 ms stall at server entry or grant,
  stopped by deadline, disconnect or worker shutdown. All retired within
  251.05 ms against the unchanged 1,000 ms controlled bound, left no old lease
  or source backoff, and preserved the replacement lease when the worker stayed
  running. Shutdown cases deliberately have no replacement lease.
- An old foreground source verdict stalled 1,500 ms, then cancellation and an
  independently connected replacement source verified 150,000 bytes in
  128.33 ms against its 1,000 ms controlled bound. The obsolete Task drained its
  scheduler reservation without adding daemon or worker backoff. The replacement
  uses the retained background lane; this proves ownership/availability, not
  that background traffic shares the common scheduler's physical budget.
- A 524,305-byte legacy file retained a private durable 262,144-byte prefix.
  After a real background source failure and a 2,500 ms foreground completion
  stall, the successful partial foreground receipt preserved source history and
  whole-file reads remained unavailable. This directly exercises the previous
  premature-history-clear path.

Three production-helper library regressions cover independent source
cancellation, install/replacement/no-resurrection and the full 16,384-key live
capacity boundary, including cancelled proofs retained by in-flight work. The
capacity test is not concurrent native transfer or a memory/saturation gate.
`test-evidence/wave-6/foreground-owner-checks.json` retains raw logs, commands,
all 365 source/build/test inputs, executable hashes and controlled samples.
The source was frozen before final lint/tests and every input hash checked
again afterward. Its SHA-256 is:
`10ecb3be2a3e3d70562c2d87b82bf5a40dd699daa0d45a28483b58c7307d3572`.

The foreground proof does not yet guard all background/upload/status writers.
Independent local installation still needs a verified-install observer; group
and catalog/source replacement cancellation, permanent c-frame retirement or
replay, reply-lost and release-WORKER_BUSY races, ordinary WANT late grants and
error-episode resolution remain in the audit. Current paged cleanup does not
prove a global shutdown bound. Complete physical legacy job/byte/spool/
connection/remote-handler budgets, common background admission, asynchronous
cleanup/fairness, actual thread startup failure and sustained 30-minute load/
recovery remain pending. Mounted COW reads/recoverable write sessions,
cache/journal, activation/migration/downgrade/deployment fences, retention and
matched Windows/FUSE/WSL build gates (with a real user solution when available)
also remain pending. The full Wave 6 goal stays active and ordinary typed
activation remains off.

## Dispatcher group lifetime and deferred ownership (WP6.3, in progress)

CCR-WP6.3-14 extends foreground ownership to dispatcher group/catalog lifetime
and typed work. Each Task has a weakly linked cancellation child of both its
group and lane binding. Either parent retires that Task without closing its
shared connection or cancelling the other parent. Linked children do not add
per-request hooks to long-lived tokens. The group itself is a dispatcher child.

A single nonblocking refresh builder validates live owners against the complete
folder snapshot and ready source/replica/lane set, including full worker
endpoint and local-device identity. It cancels changed groups before publishing
replacements and reuses unchanged groups. A weak registry retains at most 1,024
current owners, including work outside the 512-repository rotating page.
Page rotation alone cannot cancel that work. Dead/cancelled entries may leave
the registry; a full live registry defers new groups without evicting current
authority. The fixed preparation/network pools and bounded queue continue
owning cancelled physical Tasks while they drain. Selecting a group drops its
page snapshot before I/O.

Admission, completion and error projections now share a short group mutex;
group cancellation returning is a barrier for those mutations. No transport,
worker call, Task destruction or cleanup RPC holds it. Queue producers and
consumers reject cancelled groups, and an obsolete poll result cannot admit
new work. The foreground adapter's object proof consequently retires when
source/catalog/group authority changes, even if its lane stays connected.
Typed reads also check cancellation before failure projection or data install
and ACK any returned parseable frame. A typed Task keeps its scheduler
reservation through the exact-offer release attempt, and the offer keeps its
outgoing turn until that call returns. This fixes the previous release ordering;
it does not acknowledge physical retirement of every remote handler.

The new native catalog-withdrawal cases use real authenticated sessions and
workers. A legacy verdict is stalled for 1,000 ms, then its catalog disappears;
the group retires while its still-executing Task keeps its reservation until
unwind. It cannot record a failure, completion or source hold. Restoring the
catalog verifies 150,000 bytes through the original connection. A typed read
stalled for 1,500 ms likewise retires its destination Task/scheduler/local-worker
reservations and resumes on the original connection. Neither case cancels the
lane or closes its connection. The destination checks do not claim immediate
release of every stalled source handler/credit.

Seven new library regressions cover linked-parent independence, weak retention,
concurrent cancellation, deferred owners across page rotations, dead-owner GC,
the full 1,024-owner refusal boundary and the short projection barrier. Registry
capacity/rotation use the production helper; they are not a native saturation
test with 1,024 repositories. Native worker/device replacement and every
catalog change interleaving are not newly claimed by these two native cases.

Final verification passed **368 distinct Windows release checks** in one
nine-target invocation: 104 library, 37 transport, 41 contracts, 20 inbound,
12 generation, 54 lane, 63 worker, 30 peer supervisor and 7 seam checks. Strict
Clippy for all test targets, formatting and diff checks passed. Two opt-in
library probes and 18 long lane qualifications remain ignored; the historical
10,000-object worker probe was filtered. Unchanged provider/content-object
targets were not rerun. No retained gate is waived, and draft runs do not add
to the distinct count.

In the final native samples, legacy group withdrawal took 43.10 ms against
250 ms, retained the old Task's reservation until actual unwind, then verified
150,000 bytes in 67.99 ms after catalog restoration. Typed destination drain
took 118.86 ms against 250 ms, and replacement verified its 39 bytes in
86.11 ms against 1,000 ms. Both preserved the connection and recorded no
obsolete failure/completion; the legacy case also checked daemon and worker
source backoff stayed absent. These are controlled interleavings, not full
load/fairness or remote-handler retirement qualification.

`test-evidence/wave-6/group-lifetime-checks.json` retains final commands,
individual samples, raw outputs and executable hashes, plus earlier development
stages. All 365 source/build/test inputs were frozen before final lint/tests
and every hash verified afterward. Final source SHA-256:
`8c3c5e2422d62605e2d1d9e3af6fa5527afc26de6721a78d9e8e3ac4a62683da`.

Complete legacy physical budgets/common background admission, bounded
asynchronous cleanup/global shutdown, remaining background/upload/status error
writers, independent local-install observation, per-class error-episode
resolution, ordinary WANT late grants and c-frame/reply-lost/release-WORKER_BUSY
retirement remain outstanding. Executor fairness under complete saturation,
real thread startup failure, old-worker compatibility and sustained 30-minute
load/recovery still need qualification. The mounted-save, performance,
migration/activation/deployment/retention and platform gates above also remain
outstanding. The full Wave 6 goal stays active and ordinary typed activation
stays off.

## Common legacy download admission and staged ownership (WP6.3, in progress)

CCR-WP6.3-15 brings retained legacy incoming streams into the same scheduler
pool as joined object frames. Unjoined batches own a separate random token/
generation rather than an artificial single-object key. The existing eight
jobs/8 MiB, four per peer/repository and class reservations cover both types.
`usage` reports the aggregate; `joined_usage`/`stream_usage` distinguish them.
Each legacy pull requests at most 256 KiB with a conservative 1 MiB reservation.
Global refusal now precedes the local lane window, peer slot and plan mutation;
it returns readiness after 25 ms without a source error or new reader/writers.
The old wire operation, durable offsets and whole-file verifier stay intact.

The client reader, accepted buffered/pending pieces and actual worker write
batches retain independent proofs. Empty writes retain work even without
payload. The last sink proof remains held through its lane-credit hook, so
new global admission cannot race old window accounting. Cancellation or the
old runner's 500 ms exit wait does not return a still-running write's proof.
Dispatcher `started` now measures executor acceptance separately from queue
admission, under the same group projection barrier.

The first broad run exposed a real fairness regression: retaining an old
8 MiB key share with new 256 KiB requests let one wholly stalled source take
both peer background slots. The effective share now uses the capped payload;
a threshold notification wakes re-pull independently of writes/final replies.
The unchanged other-repository bound passed again in 0.83 seconds in its
focused check. A subsequent boundary check caught a rejected third request
temporarily entering the lane's reservation peak. Common admission now occurs
first, and sink proofs remain through credit publication. All failed stages
are retained rather than replaced by successful retries.

The historical test's minimum 8 MiB payload occupancy per lane is explicitly
superseded by Wave 6's common budget. It still completes the same 24/72 MiB
workload within 60 seconds, retains the <=16 MiB bound and requires two pulls
under link pressure. It now additionally requires an exercised >=256 KiB
payload and <=512 KiB actual outstanding payload/window in that fixture. The
focused corrected workload passed in 8.85 seconds. This allocation contract
change does not qualify sustained default-window throughput, and does not
waive ignored historical long qualifications.

Final native controlled samples fill six real source streams across six
repositories while a foreground reader receives actual executor capacity in
58.05 ms and verifies 43 bytes in 67.22 ms, against the unchanged 250 ms bound.
Observed peak common use is seven jobs/7 MiB. Empty and 16,000-byte transfers
stall their actual worker write for 750 ms after the stream ends; both retain
their reservation after cancellation and after the runner has joined, then
drain on physical client-work retirement. The original fixture incorrectly
treated runner join as write retirement; it now observes those separately.
These are client stream/staging/write-call reservations, not whole-process
RSS or remote-handler retirement acknowledgement.

Final verification passed **378 distinct Windows release checks** in one
ten-target invocation: 104 library, 8 scheduler, 39 transport, 41 contracts,
20 inbound, 12 generation, 54 lane, 63 worker, 30 peer supervisor and 7 seam
checks. Strict Clippy for all test targets, formatting and diff checks passed.
Two library probes and 18 long lane qualifications remain ignored; the
historical 10,000-object worker probe was filtered. Unchanged provider/content
object targets were not rerun. No ignored long qualification is waived; the
historical allocation-assertion change above is explicit.

Matched boundary probes ran the retained previous executable and the final
executable on this host with identical fixtures/configuration/faults, one
warm-up and five measured repetitions per case. All 24 executions passed and
are not added to the distinct count. Short-round 300,000-byte availability
median changed from 250.29 to 264.98 ms (+5.9%; ranges 228.08–251.35 and
240.64–314.52 ms). This case uses a 128 KiB lane window. Foreground admission
behind two other stalled repositories changed from 35.35 to 10.04 ms median;
1,048,599-byte availability changed from 249.36 to 207.56 ms. These individual
samples/ranges are controlled boundary evidence, not a p99 or a sustained
default-window bulk/native/W5/W6 mounted-build qualification.

`test-evidence/wave-6/legacy-stream-checks.json` retains all final commands,
source/executable identities, samples, raw logs, failed development stages and
matched repetitions. All 365 source/build/test hashes were frozen before the
final invocation and checked afterward. Final source SHA-256:
`c31e8d7d7a0b056da58346b4dc78cda44c3c85cbf1136fbda272a328916c4fce`.

Complete legacy upload/inbound/source/connection/thread/spool budgets and
remote-handler retirement remain pending. Cross-repository/source fairness
under every partial-stall interleaving, sustained default-window throughput,
actual thread startup failure, bounded asynchronous cleanup/global shutdown,
remaining daemon error/status effects, independent local-install observation,
ordinary WANT late grants and c-frame/reply-lost/release-WORKER_BUSY retirement
also remain pending. In particular, common admission pressure while earlier
sink writes are still retiring can end a download round and incur its 25 ms
readiness retry; this deserves throughput measurement before WP6.3 closure.
Mounted recoverable COW saves, cache/journal, compatibility/migration/retention
and the full platform/build/load gates remain outstanding. The full Wave 6
goal stays active; ordinary typed activation stays off.

## Same-round local retirement and measured download flow (WP6.3, in progress)

CCR-WP6.3-16 addresses the concrete pressure/retry follow-up above. A native
4 MiB transfer with one 250 ms worker-write stall reproduced eight actual
round nonces. After a stream ended, its pending local writer still held common
capacity; treating that as unrelated admission pressure unnecessarily finished
the round, regranted leases and incurred a 25 ms readiness delay.

The round now waits for its own buffered/pending/writing/retiring sink work
when no source stream is active. Without its own work able to return capacity,
it still yields the existing 25 ms retry to other scopes. Cancellation, errors
and the ten-second round cap stay in force. A successful admission clears the
previous refusal flag. A short atomic phase covers proof retirement after
ordinary byte/settlement publication; `CreditsReturned` wakes the consumer
after those proofs drop. The reader likewise drops its proof before publishing
`PullEnded`. The retirement guard resets its phase even on unwind, preserving
another live stream's reservation. No scheduler/sink lock covers I/O, hashing
or the controlled retirement stall. Budgets, payloads, hashes, offsets and wire
messages are unchanged.

Final native samples verify all 4 MiB in 408.38 ms with the 250 ms write stall,
and all 1,048,583 bytes in 354.75 ms with five 150 ms post-credit/pre-capacity
stalls. Both observe one round nonce, one lane run, a 60-second heartbeat and
no source backoff. Complete byte verification uses immutable provider ranges
of at most 1 MiB. The first draft fixture had mistakenly asked for one 4 MiB
range and hit the unchanged provider bound; the corrected pre-fix fixture
verified the bytes and then failed on the eight observed nonces. That fixture
failure and the genuine churn reproduction are retained separately.

The existing 72 MiB write-limited and 24 MiB link-limited workload now records
times while retaining all its completion, deadline, payload/window and pipeline
checks. The instrumented control's production bytes exactly match the prior
qualified `c31e...` source; only two integration-test files differ. Its measured
test file remains byte-identical in the final candidate, and its executable was
preserved before production edits. One warm-up plus five measured repetitions
per binary passed in all 12 process invocations:

- With a 3 ms fake-worker memory-spool write delay, 72 MiB median transfer time
  changed from 6,667.92 ms to 1,151.40 ms (82.7% lower); ranges were
  6,659.53–6,705.45 and 1,143.78–1,162.00 ms. Median throughput changed from
  10.80 to 62.53 MiB/s.
- With a 20 MiB/s, 2 ms latency link proxy, 24 MiB medians were 1,772.29 and
  1,776.77 ms (+0.25%); ranges were 1,763.94–1,781.43 and
  1,771.30–1,781.64 ms. Both kept two pulls active and at most 512 KiB of actual
  outstanding payload/window.

These are authenticated-session, fake-worker memory-spool measurements.
They do not qualify real-disk fsync, cold caches, native/W5/W6 mounted builds,
p99 or W6-BUILD/W6-LOAD. An initial complete control run overlapped a separate
native fixture compilation/test; its raw results are retained but excluded,
and the entire control series was repeated without another task compile/test
running. No individual sample was selectively replaced.

Final verification passed **381 distinct Windows release checks** in one
ten-target invocation: 105 library, 8 scheduler, 41 transport, 41 contracts,
20 inbound, 12 generation, 54 lane, 63 worker, 30 peer supervisor and 7 seam
checks. Strict Clippy for all test targets, formatting and diff checks passed.
Two library probes and 18 long lane qualifications remain ignored; the
historical 10,000-object worker probe was filtered. Unchanged provider/content
object targets were not rerun. No ignored historical qualification is waived;
timing repetitions and draft checks do not add to the distinct count.

`test-evidence/wave-6/stream-drain-checks.json` retains commands, individual
samples, raw outputs, development failures and both source/executable stages.
All 365 final source/build/test inputs were frozen before final lint/tests and
each hash rechecked afterward. Final source SHA-256:
`a1427a88ee1be9bbc4f3d2bcf488ba5dfa0d2160a37a878d1f4bf958bb5d11c7`.

Complete upload/inbound/source/connection/thread/spool budgets, remote-handler
retirement, retained round bookkeeping bounds, all partial-stall/fairness/load
interleavings, real disk/build performance, actual thread startup failure,
bounded asynchronous cleanup/global shutdown and remaining late effects still
need qualification. Ordinary WANT late grants, error/status episodes,
independent local-install observation and c-frame/reply-lost/release-WORKER_BUSY
retirement also remain pending. Mounted recoverable COW saves, cache/journal,
compatibility/migration/retention and final platform/build/load gates remain
outstanding. The full Wave 6 goal stays active; typed activation remains off.

## Bounded round metadata and empty-write completion (WP6.3, in progress)

CCR-WP6.3-17 bounds retained download bookkeeping independently of transfer
volume or the ten-second quantum. One round remembers at most 4,096 distinct
objects, covering its plan/deduplication/begun/failed/corrupt history and sink
object table. Wants limit their item count to remaining room, including
immediately reserving promoted demand entries. Returned overflow leases are
released by exact session without a source hold/error. Accepted work finishes
before the ordinary content yield starts another round; live work is not
evicted. Byte-plan estimates now saturate rather than overflow.

Completed pull records are removed when their single status report is emitted,
instead of accumulating every previous request in the round. At most eight
records may be live/pending; admission refuses a ninth before allocating its
resources. Peak diagnostics are per-round maxima, not total daemon metadata
or RSS. A production-helper test keeps six incomplete predecessors while
cycling 50,000 completed records through one temporary slot. Another fills
the 4,096-object boundary, checks overflow refusal/deduplication and exercises
u64::MAX byte estimates. These are bounded model tests, not native 50,000-file
saturation evidence.

This exposed an existing empty-payload status bug. The actual native worker
write was stalled for 600 ms, but the old code emitted successful
`ExchangeFinished` with zero installed objects as soon as the empty source
stream ended. Accepted empty writes now have independent internal receipts.
The collector waits for source completion, all bytes and those empty receipts;
settlement updates the object count in the same event vector before collection.
Rejected/probe-only frames do not create receipts. Final accounting closes
after sink finish joins/drains or discards its remaining client work. No peer
wire, canonical hash, lease or durable offset changes. The final real
SQLite/CAS/loopback test observed no premature completion and exactly one
installed-object completion, including after joining the runner.

The authenticated fake-worker boundary scenario completed 4,353 distinct
hash-verified objects in 167.27 ms, with all 4,353 completion counts preserved,
a 4,096-object per-round peak, one object-budget yield and two retained pull
records at peak. Its explicit 60-second test quantum isolates the object
boundary; production stays at ten seconds. This is a controlled memory-spool
fixture, not a native repository/build or full aggregate-memory qualification.

Matched flow probes preserved the previously qualified executable and reran
the same 72/24 MiB workloads with one warm-up plus five measured repetitions
per binary. All 12 process invocations passed, retaining the existing deadline,
two-pull and <=512 KiB payload/window checks. Removing only the newly added
metadata-boundary test function reconstructs the control test file SHA-256;
the measured workload/checks are unchanged.

- Write-limited 72 MiB median changed from 1,147.33 to 1,219.47 ms (+6.3%);
  ranges were 1,116.85–1,174.20 and 1,176.57–1,262.42 ms.
- Link-limited 24 MiB median changed from 1,784.41 to 1,800.60 ms (+0.9%);
  ranges were 1,774.63–1,789.04 and 1,778.85–1,969.31 ms.

These preserve the measured cost of the added bounds; they are authenticated
sessions with fake-worker memory spools/delays, not real-disk fsync, cold-cache,
p99 or mounted W6-BUILD/W6-LOAD evidence. No native/W5 regression gate is claimed
passed from this internal control comparison.

Final verification passed **385 distinct Windows release checks** in one
ten-target invocation: 107 library, 8 scheduler, 42 transport, 41 contracts,
20 inbound, 12 generation, 55 lane, 63 worker, 30 peer supervisor and 7 seam
checks. Strict Clippy for all test targets, formatting and diff checks passed.
Two library probes and 18 long lane qualifications remain ignored; the
historical 10,000-object worker probe was filtered. Unchanged provider/content
object targets were not rerun. No ignored gate is waived and timing/draft
repetitions do not add to the distinct count.

`test-evidence/wave-6/round-bookkeeping-checks.json` retains exact commands,
individual samples, raw outputs, the reproduced status failure, development
failures and source/executable identities. All 365 final source/build/test
inputs were frozen before final lint/tests and checked again afterward.
Final source SHA-256:
`4d9530d83976531a3928a73217bb6ffb3379d5848782753ebabb7d4b2b79fbd8`.

Aggregate discovery/kept-scope/connection/thread/spool limits and legacy
upload/inbound/source accounting remain pending, as do physical remote-handler
retirement, all partial-stall/fairness/load interleavings, real disk/build
performance, actual thread startup failure and bounded asynchronous cleanup/
global shutdown. Remaining late effects/error episodes, independent local
installation observation, ordinary WANT late grants and c-frame/reply-lost/
release-WORKER_BUSY retirement still need audit. Mounted recoverable COW saves,
cache/journal, compatibility/migration/retention and final platform/build/load
gates remain outstanding. The full Wave 6 goal stays active; activation is off.

## Request-owned ordinary WANT admission (WP6.3, in progress)

CCR-WP6.3-18 extends the real loopback request deadline and connection/CANCEL
predicate to ordinary `objects.want`. Previously only the foreground legacy
prepare used that context: a timed-out/disconnected ordinary WANT could still
reach its grant mutation after selection returned. Both the shared legacy
cancellation token and worker stopping flag are checked. JSON/direct dispatch,
which has no responder predicate, uses a 60-second deadline and observes worker
shutdown; no connection-disconnect guarantee is claimed for that surface.

The existing selection/grant checks now apply to ordinary WANT as well. The
post-grant rollback tracks only fresh grants from this request. Previously it
also retired leases already held by the same session, including the effect
tickets of earlier writers. Cancellation of a retry now preserves those leases,
expiry/provenance and tickets. The final check/rollback shares the grant lock;
a replacement cannot slip between them. The bounded grant page remains at most
256 objects. Store selection and stop diagnostics run outside the grant lock;
no per-request thread or retained cancellation hook was added. Applied ordered
run/pool/round floors and explicit exclusions remain monotonic.

One new library case uses actual SQLite/CAS state with two missing objects. It
reoffers a previously granted lease while granting the other, observes stop at
the post-grant check, and requires the previous lease/ticket to survive while
the new holder/effect disappears without source backoff. This is deterministic
admission testing, not arbitrary delivery-loss or saturation qualification.

The native loopback case now covers 24 internal scenarios: ordinary background
WANT with qualified a-round IDs and foreground legacy prepare with c-frame IDs;
entry/pre-grant stalls; deadline/disconnect/shutdown; and no replacement versus
an already admitted replacement. Each injects a 500 ms stall (200 ms request
timeout for the deadline case), keeps the one-second retirement ceiling and
checks no old lease/source backoff plus exact unchanged replacement ownership.
Final observed retirement was 499.45–501.40 ms. Its clock starts after stall entry and before
provoking the stop; the deadline case includes waiting for that deadline, and
shutdown includes stopping the server. These are conservative admission/drain
observations, not network p99 or full physical remote-handler retirement.

Development outputs are retained. The original implementation failed the
ordinary WANT request-stop diagnostic observation. That log did not directly
observe an orphan lease and is not described as doing so. Two expanded-fixture
attempts supplied WANT-only fields to strict LegacyPrepare, which rejected the
request before the grant stall; operation-specific headers fixed that fixture.
An initial abbreviated unit name with `--exact` ran zero tests; its later fully
qualified invocation and final full library run executed the case. None of
these draft/focused repetitions increases the distinct check count.

Final verification passed **386 distinct Windows release checks** in one
ten-target invocation: 108 library, 8 scheduler, 42 transport, 41 contracts,
20 inbound, 12 generation, 55 lane, 63 worker, 30 peer supervisor and 7 seam
checks. Strict Clippy across all test targets, formatting and diff checks
passed. Two library probes and 18 historical long lane cases remain ignored;
one historical 10,000-object worker probe was filtered. Unchanged provider/
content-object targets were not rerun; no ignored gate is waived. The existing
full lane flow workloads were retained, but no matched native/W5/mounted build
or long load/performance gate was run for this admission change.

`test-evidence/wave-6/want-cancellation-checks.json` records exact commands,
source/executable identities, final native samples and all development logs.
All 365 Cargo/build/src/tests/native/ui inputs were frozen before final lint
and the ten-target run and rechecked afterward. Relative to the previous
qualified input set, only `src/runtime/objects_rpc.rs`,
`src/runtime/server.rs` and `tests/content_transport.rs` changed.
Final source SHA-256:
`7ebc8975ee2495e38bb1be14aadc99aa1d48dede10b08f11d157cb0d75115ec3`.

Cancellation/reply loss after the final grant check, long store-scan cancellation
slices, c-frame replay and release-WORKER_BUSY retirement still need work;
this change does not provide reply delivery acknowledgement. Remaining late
error/status effects and independent local-install observation are also open.
Legacy upload/source/inbound aggregate jobs/bytes/connection/thread/spool limits,
aggregate discovery/kept scopes, bounded asynchronous cleanup/global shutdown
and full physical-handler/fairness/load qualification remain pending. Mounted
recoverable dirty-range COW sessions, cache/journal, compatibility/migration/
downgrade/deployment/retention and final platform/build/handoff gates remain
outstanding. The full Wave 6 goal stays active; activation remains off.

## Private COW write sessions and durable checkpoints (WP6.4, in progress)

CCR-WP6.4-1 adds `content::write::WriteJournal` and `WriteSession`, a private
provider-independent mutable layer over pinned immutable content. Save intent
captures the operation UUID, branch, workspace/entity/base generation, immutable
base descriptor and content/alive causal parents. It does not reconstruct those
parents from a newer view during recovery. This is a pending save episode;
the future metadata commit path must fence/retire it and allocate a fresh
operation/base for later writes after publishing a revision.

A 64 KiB dirty chunk stores its changed bytes plus an 8 KiB coverage bitmap.
Neither writes nor checkpoints read the base. Verified range reads fetch only
required uncovered base bytes through the provider. Logical shrink clips both
patch coverage and base visibility, so growth produces zeros without resurrecting
old bytes. No full-base staging file is created. The local overlay encoding is
not a canonical chunk/manifest or a peer wire type. A real LocalContent/CAS case
checks immutable base isolation, private edits, live pin lifetime and recovery;
an unavailable fake 1 GiB base exercises full/partial patches without hydration.

The separate `write-sessions-v1.sqlite` journal maintains distinct working and
saved maps. Ordinary mutations use NORMAL transactions; they do not acknowledge
a recoverable close. Explicit checkpoints verify pending payloads and atomically
commit the saved map and scoped intent with FULL durability. Later unflushed
writes/truncations cannot replace that durable selection. Resume discards those
uncheckpointed edits, retains the operation/parents and installs a fresh owner
inside its transaction. Old handles and delayed checkpoints are fenced; newer
checkpoints make old preparation selections fail stale. Cancel after FULL commit
returns an error while retaining the durable checkpoint.

The journal charges 73,728 bytes per distinct pending chunk, enforcing a shared
64 MiB ceiling before allocation and a limit of 64 dirty intent groups. Pressure
checkpoints synchronously before retrying. The real SQLite/WAL byte-pressure
case used distinct sparse eight-byte patches, not one deduplicated payload for
every index. Peak charge was 67,092,480 bytes under the 67,108,864-byte ceiling;
the group-pressure phase ended with three dirty groups under its 64-group limit.
These are private journal limits, not proof of the still-pending four-worker/
64-job canonical executor, total worker RSS or a bounded saved-disk inventory.

Records are bounded/checksummed, metadata proofs include operation/chunk scope,
and payloads are hash-verified. Oversized records are rejected before copying.
Private SQLite lock waits poll at 1 ms; VM progress hooks check cancellation and
deadline every 1,000 instructions and are removed by the lock guard. A real
recursive SQL trigger was interrupted at its 80 ms deadline (80.06 ms observed,
500 ms test ceiling), rolled back without replacing the old checkpoint, and
allowed a subsequent successful checkpoint. Blocking OS I/O remains OS-bound.
Providers run outside that journal lock; metadata engine locks must not span
these operations.

Repeated overwrites retained two payloads while the prior checkpoint was pinned
by the saved map, then one after the next checkpoint. In that 199-write fixture,
the database/WAL file lengths were 266,240 / 2,163,032 bytes; these are file-length
gauges, not device-write or fsync counters. A cursor-driven reclaimer examined
402 indexed payload keys in pages of at most 256, removed 400 orphan payloads
and preserved both saved and current data. Pending checkpoint enumeration is
also paged; consumers must reconcile again after changes and include those base
roots in retention. Complete runtime retention/startup cleanup is not wired yet.

Seventeen new tests include a 240-mutation seeded byte oracle, shrink/grow/holes,
stale and cancelled reads, owner replacement, late checkpoint fencing, corrupt
payload/oversized intent rejection, incompatible journal versions, cancellation
after durable commit, resource backpressure, native CAS and two real child-process
exits. Exit before checkpoint recovered the previous byte value 31; exit after
FULL checkpoint recovered 32. Both children exit 86 before destructors and are
covered through the parent test; they are not counted as extra successful tests.
This is process-exit/WAL recovery evidence, not a power-cut or mounted close claim.

The matched internal 4 KiB probe used one warm-up plus five measured repetitions
at each selected base size, with the same API and an unavailable fake provider.
The timer includes write and FULL private checkpoint, excluding setup/readback.
Each mode retained exactly one 73,728-byte overlay payload and read zero provider
bytes. Medians were 1.8330 ms for a 1 MiB selected base and 1.6473 ms
for 1 GiB; ranges were 1.6318–3.1347 and 1.5929–1.9081 ms. These are real
SQLite private-checkpoint measurements over fake descriptors, not real 1 GiB
hydration, canonical chunk/manifest amplification, native/W5/mounted builds,
p99 or full load qualification.

Final verification passed **168 distinct Windows release checks** in one
six-target invocation: 108 library, 8 admission, 6 content-format, 17 object,
12 provider and 17 write-session checks. Strict Clippy across all test targets,
formatting and diff checks passed. The four ignored items are two library
opt-in probes, the provider real-1-GiB canonical amplification qualification,
and the subprocess helper explicitly run by its parent. No qualification gate
is waived. Unchanged legacy integration targets were not rerun; the previous
386-check report belongs to its earlier source/dependency fingerprint.

The new rusqlite hooks feature uses the already cached 0.37 dependency offline;
Cargo.lock is unchanged. A first compile caught guard coercions, strict lint
caught `is_multiple_of` style, and a complete first 168-check run passed before
the audit added final cancellation checks after SQLite lookups/no-op returns.
Final strict lint and all six targets were rerun after those changes. Earlier
passes and focused repetitions do not increase the distinct count.

`test-evidence/wave-6/write-session-checks.json` retains exact commands, all
samples, child exit evidence, raw development/final outputs and executable
identities. All 367 Cargo/build/src/tests/native/ui inputs were frozen before
final lint/tests and checked afterward. Relative to the previous qualified
input set, only Cargo.toml, the content module registry and the new source/test
files changed. Final source SHA-256:
`7079c513a59a76828997e349626a535dfdeee90c7fa842c1410dc6dabab0e751`.

Canonical chunk/manifest preparation including resize, metadata/outbox generation
and causal/idempotency checks, proven recovery retirement, namespace/create/
delete/rename ordering and per-operation corrupt-work/backpressure policy remain
open. The four-save-worker executor, pending/unused-session cleanup, worker
startup/shutdown and full root-retention integration are also required. No
mounted callback uses this module yet: WinFsp cleanup/FlushFileBuffers and FUSE
FLUSH/FSYNC behavior and promises remain unchanged. ENOSPC/power-loss, complete
adapter recovery and all final platform/build/load/amplification gates still
need qualification. WP6.3 aggregate legacy budgets/late effects and WP6.5/6
cache/journal, compatibility/migration/activation/retention/handoff work also
remain outstanding. The full Wave 6 goal stays active; activation remains off.

## Canonical checkpoint preparation and complete-map integrity (WP6.4, in progress)

CCR-WP6.4-2 adds `WriteSession::prepare` and opaque `PreparedSave` pins. A selected
durable checkpoint can now produce canonical direct/chunk/manifest content.
The builder classifies indexed spans, reuses intact hashes from the captured
base or verified ancestor pages, and retains one page per tree level. Resize
crosses direct/manifest and root-depth boundaries without a whole-base copy;
only changed paths and boundary chunks are materialized. Zero-filled growth
uses a bounded shared zero DAG. Missing required structure returns its object
key for hydration; fully replaced output needs no old structure or data.

Needed legacy bytes are converted from the local whole-file object in one
sequential input stream with final SHA-256 validation. Conversion occurs before
builder buffer admission, with selected-owner checks during waits and reads.
One conversion pin is cached per live session/target; repeated preparation in
the fixture read 131,072 bytes (containing chunk plus duplicate verification),
compared with 2,162,821 bytes for the first 2,097,285-byte legacy conversion/
preparation. Parallel legacy preparations finished within their five-second
deadlines without nested buffer permits. This cache is not a persistent
conversion index; restart can require conversion again before metadata commit.

The six-size/four-stage matrix compares prepared descriptors with the independent
streaming importer, including shrink/grow/holes and root-shape transitions.
Native CAS tests preserve missing unrelated chunks, avoid fetching a fully
overwritten old chunk, and reject missing structure rather than staging the
file. Completely replaced output also prepares from unavailable chunked and
legacy bases. A logical 2^48-byte zero-filled version used one data object and
four manifest pages and supported a later verified partial edit. That is a
content/provider DAG test, not a 256-TiB mounted allocation. Delayed/cancelled
preparations retain recovery records and return no successful preparation.
Stale preparations waiting behind six unrelated background buffers retired in
5.062–12.186 ms, below the 250 ms bound, before those producers were released.

Auditing this path found and reproduced a flaw in the preceding private journal:
a deleted saved-map row could be interpreted as unchanged content and return a
successful preparation of the old bytes. Presence-only payload checks were
insufficient. v2 records bind complete sorted working/saved maps with SHA-256
and counts; scope seals and an inventory count/XOR also detect vanished intents.
Mutations verify existing maps before replacing their proofs. Reads/preparation
verify selected maps, and preparation checks again after building objects.
Missing metadata/payloads are corruption rather than holes. A valid saved
record/map remains recoverable when a newer buffered header is damaged; resume
fences old handles with a fresh UUID and discards only uncheckpointed work.

The private family file remains `write-sessions-v1.sqlite`, now at user_version 2.
Experimental v1 and future versions are refused without rewriting/discarding
them. The v1 refusal fixture preserves its old record bytes. No live migration,
metadata schema, canonical encoding or peer wire change is introduced here.
The normal runtime still does not construct/use this journal or preparation path.

Reclamation now reconciles scope/map integrity before deleting any payload,
processing at most 256 combined scope/map/payload records per call. Opaque
cursors bind the journal instance, SQLite data_version and total_changes;
foreign commits, local mutations and cross-instance cursors restart the audit.
A foreign deletion of an already-audited prefix was detected and all 300
payloads retained. Missing map or whole-intent cases also retain their bytes.
The 400-orphan fixture still completes in bounded pages while preserving both
saved and current references. Opportunistic replacement maintenance drives one
step, but the runtime must still drive remaining pages and qualify progress
under continuous mutation/load.

A second reproduced issue coupled optional cleanup failure to healthy writes:
another damaged scope made an unrelated file write fail after its mutation had
already committed. Maintenance now retains one bounded independent error episode
instead. The actual healthy write/checkpoint remains successful and recoverable;
explicit cleanup still returns its error and keeps bytes. Cancellation/deadline
does not create a failure episode. The episode clears only after a complete
healthy pass, demonstrated after restoring the damaged metadata. Worker status
wiring is still required; no mounted/user-visible recovery workflow is claimed.

The real prechunked 1 GiB pipeline probe imports a genuine uniform byte stream,
then copies/syncs its small deduplicated CAS graph into an isolated candidate
store. One warm-up plus five measured pairs compare the existing canonical-only
`LocalContent::overwrite` with write + FULL private checkpoint + preparation.
Every pair produced the identical descriptor, one new 64 KiB data object and
two manifest pages. The candidate read one 64 KiB base chunk and four structural
pages, with 236,064 canonical hash-accounted bytes. The two-boundary case created
exactly two data objects and left the base unchanged.

- Canonical-only control median: 5.6009 ms; range 5.5089–5.8276 ms.
- Journal/checkpoint/preparation median: 13.2294 ms; range 12.9319–17.6553 ms.
- Measured ratio: 2.362x. The additional recovery-intent FULL transaction
  and integrity validation are absent from the control; this cost is retained.

The input is uniform/deduplicated, not arbitrary 1 GiB user content. These
canonical file counters exclude private overlay/SQLite metadata hashing, WAL
page/device writes and actual device fsync counts. This is no native/W5/mounted
build or equivalent-durability regression pass, and no build/load/physical-I/O
gate is waived. The historical direct provider 1-GiB contained/cross-boundary
probe was separately executed and passed on the same final source.

Final verification passed **185 distinct Windows release checks**: one
six-target invocation with 108 library, 8 admission, 6 format, 17 object,
12 provider and 33 write-session checks (184 total), plus that explicitly
selected historical provider probe. Strict Clippy across all test targets,
formatting and diff checks passed. The main invocation reports four ignored
items: two library probes remain unexecuted; the provider probe was explicitly
run, and the subprocess helper is exercised twice by its parent (exit 86), not
counted as additional successful tests. Eighteen long lane and historical
10,000-object worker probes were not selected or waived; the old integration/
platform suite is not represented by these package checks.

`test-evidence/wave-6/canonical-prepare-checks.json` retains raw failures for
missing-map silent fallback and unrelated cleanup failure, intermediate
182/183-check runs, exact commands, all samples and source/executable identities.
Draft repetitions do not increase the distinct count. All 367 source/build/test
inputs were frozen before final lint/tests and rechecked afterward. Relative
to the preceding qualified input set, only `src/content/local.rs`,
`src/content/write.rs` and `tests/content_write.rs` changed.
Final source SHA-256:
`3fac865517f723ccbbe3ae901d9e7b7a2e1e0aececb5b6879d9dbcd216331c05`.

Canonical preparation now exists, but atomic entity/generation/causal/idempotency
metadata/outbox publication, receipt-owned retirement and safe operation/base
rollover while newer edits continue remain open. Namespace/create/delete/rename
ordering, the four-worker/64-job executor, independent journal read/write
coordination under slow fsync, startup/shutdown, full retention and maintenance
status integration still need implementation/qualification. No WinFsp cleanup/
FlushFileBuffers or FUSE FLUSH/FSYNC promise changed. Full adapter crash matrix,
ENOSPC/power-loss, device amplification, native/W5/mounted build/load and final
platform gates remain pending, alongside WP6.3 legacy budgets/late effects and
WP6.5/6 cache/change-journal, compatibility/migration/activation/retention/handoff.
The full Wave 6 goal stays active; activation remains off.


## Metadata save ownership and atomic publication receipts (WP6.4, in progress)

CCR-WP6.4-3 adds the metadata-owned part of canonical save publication in
`src/core/content_saves.rs`, initialized by `core.rs` and observed by the
existing transactional commit path. It remains restricted to isolated
`fixture:content_v1` stores. Normal mounted/runtime sessions do not call the
new APIs; activation stays off. No live store was opened/migrated for this work.

`begin_content_save` captures an existing live file's persistent workspace UUID,
branch, per-entity local generation, operation UUID owner, base descriptor,
content/alive parents and immutable event template. Device, clocks, applicable
basic-register update and the namespace frontier are retained rather than
reconstructed during recovery. Checkout epoch is read from durable config,
separately from the entity generation and stale in-memory Store fields.
The frontier is bounded at 256 IDs and each checksummed envelope at 64 KiB.
Limit failures roll back ownership/config/operation writes atomically.

Starting a newer operation fences old publication through both the local
generation and UUID owner. Deleting/recreating the counter at the same numeric
value cannot transfer an old operation's right. Ordinary local content, alive,
location and basic commits invalidate the owner in their event transaction;
foreign Store handles exercise rename, delete, replacement, permission changes
and legacy writes. A remote concurrent descriptor remains a causal competitor,
with both alternatives preserved by the independent derivation oracle.

`select_content_checkpoint` persists the complete selected checkpoint record.
Older/different equal sequences cannot replace it, and a newer selection fences
an old prepared callback. Publication checks the pin belongs to this native CAS,
then revalidates workspace/checkout/entity owner and selected sequence under
`BEGIN IMMEDIATE`. Event, admission metadata/outbox and checksummed receipt
commit together under FULL/WAL. Nested caller transactions and weaker SQLite
durability are rejected. No journal lock, file hashing/copying or hydration is
held in that transaction.

A missing persistence check was reproduced on exact source `b639c398...`:
a real SQLite trigger ignored the receipt UPDATE and the prototype returned
successful `SaveReceipt`. The retained failing test/log and full source manifest
are in the evidence. Exact affected-row and persisted selection/receipt checks,
plus event/root/outbox evidence validation, now reject ignored or discarded
writes. Both before-update IGNORE and after-update discard cases pass. Failures
roll back event/outbox/receipt and ownership, leaving the journal intact.

The operation UUID is the immutable event ID. Lost-reply replay returns the
matching original receipt before consulting obsolete local ownership, so it
cannot reapply an event after later checkout/deletion. A short WAL read snapshot
checks receipt evidence against the stored immutable event and root/outbox;
unknown operation returns absence. Serialized receipt bytes or absence are not
journal deletion authority. Recovery data remains retained in every fixture.

Four actual child process exit-86 boundaries cover before metadata, event
written, receipt written and committed. Fresh Store/journal ownership rebuilds
canonical content and commits/replays once. Real WAL writer acquisition polls
deadline/cancel with 1 ms busy retries and restores the prior timeout. A long
recursive SQLite trigger is interrupted at the request deadline and rolls back
all publication rows. A 500 ms injected metadata pause permits independent
journal mutation and saved-checkpoint read within 250 ms. This proves lock
independence, not isolation from actual device fsync latency or panic/unwind
cleanup. Native device fsync is not interruptible by the SQL VM hook.

The expanded first qualification exposed 24 unregistered write/publication
codes, including private-journal codes from the preceding package: 269 cases
passed and the global code-literal audit failed before the final three targets
ran, on exact source `672c0e8f...`. That result is preserved. The Wave 6 error
extension now classifies superseded work as Contention, stable capture/sequence
bounds as NeedsUserAction, unsupported private versions as Incompatible and
corrupt/lost publication or violated invariants as Fatal in their own save/
journal/connection scope. Historical classes remain unchanged. Contract tests
verify stale work does not count as failure and damaged saves do not auto-retry;
actual runtime status scoping is still required.

The publication cost probe uses one warm-up and five measured pairs in the
same build over identical 19-byte canonical descriptors. Control commits the
captured typed event plus admission outbox; candidate adds ownership/selection
validation and durable receipt/readback. Both use FULL/WAL and include the new
mutation observer. Capture, journal checkpoint, metadata selection and canonical
preparation are outside both timed regions; canonical logical counters remain
unchanged across candidate publication.

- Event/outbox control median: 1.4015 ms;
  range 1.3239–1.4707 ms.
- Publication with receipt median: 2.5643 ms;
  range 2.4290–2.8377 ms.
- Measured ratio: 1.830x, retained as component cost.

This is not an old-executable, native/W5/mounted regression or full-save build
comparison. Additional capture/selection recovery transactions are not free;
full-pipeline batching/latency, physical SQLite/WAL/device accounting and matched
build/load gates still need measurement. No gate was relaxed by these samples.

Final verification reports **303 passed cases**: one twelve-target
Windows release invocation (302 total) and the separately selected
historical provider 1-GiB contained/cross-boundary probe. Counts by target:
lib: 108, activation_inbox: 22, content_admission: 8, content_format: 6, content_objects: 17, content_provider: 12, content_saves: 23, content_write: 33, contracts: 41, derive_oracle: 15, replication_regressions: 9, startup_snapshots: 8. Historical targets retain their existing helper test cases; the new
publication helper runs through its parent at four exit boundaries and is not
counted again. Strict Clippy for all library/test targets, formatting and diff
checks passed. The raw ignored declarations are retained; library and long
oracle opt-ins, long lane/10,000-object worker and unselected integration/platform
qualifications were not waived or represented by this package run.

`test-evidence/wave-6/save-publication-checks.json` retains commands, source and
executable identities, raw failures/development stages, all final outputs and
individual samples. Draft repetitions do not increase the final count. All
369 Cargo/build/src/tests/native/ui input hashes were frozen before final
lint/qualification and rechecked afterward. Relative to the preceding qualified
source, five existing inputs changed and two were added: core registry/commit,
write-record validation visibility, Wave 6 error taxonomy/contract assertions,
and the new publication module/tests. Final source SHA-256:
`fa33bf277f83613b8496594e73586c1ccd8cdac22d715c8929dafe5a5539fcda`.

The metadata selection is a coordinator-owned authority, not an atomic observer
of the mutable private journal. Runtime integration must order checkpoint
selection, preparation, publication, resume, receipt adoption and rollover.
One operation publishes at most one checkpoint. Later buffered/durable edits
remain in the old journal; safe base/operation rollover and receipt-owned
retirement have no API yet. Capture currently covers existing live files,
not pending create/delete/rename recovery or mounted namespace semantics.

Next work remains the real session coordinator, panic/unwind ownership cleanup,
four-worker/64-job executor, save batching, off-lock slow metadata preparation,
independent journal read/write coordination under actual slow FULL fsync,
unused-operation/pending-root retention and scoped status integration. WinFsp/
FUSE close/flush/fsync, complete crash/ENOSPC/power-loss, total amplification,
two-peer/WSL/native/W5/mounted build/load and soak qualification remain open,
alongside WP6.3 budgets/late effects and WP6.5/6 change-journal/cache/compatibility/
migration/activation/retention/handoff. The complete Wave 6 goal stays active.


## Publication unwind and failed-connection recovery (WP6.4, in progress)

CCR-WP6.4-4 closes a reproduced ownership leak in the metadata publication
component. On exact 369-input source `ffa4275e...`, an injected panic before
metadata returned through the fixture's unwind handler while the real SQLite
connection still held its outer writer. The retained failing assertion is
`writer leaked after content.save.before_metadata`. The before manifest and log
are preserved; no later binary hash is attributed to that earlier source.

The wrapper now catches unwind only for cleanup: it clears the request's SQL
progress handler, undoes tentative publication/cache state, releases the writer
and resumes the original panic payload. Four panic boundaries verify no
before-commit event/outbox/receipt survives, while after-commit replay returns
the original receipt once. Cancelling the old request after cleanup no longer
interrupts a new independent recursive SQL query; busy timeout is restored and
a second connection can acquire the real writer. An actual SQLite commit hook
rejects COMMIT, exercising automatic transaction rollback and cached visibility
before a successful retry. This is not a power-loss/device-failure simulation.

Actual SQLite authorizer denial of ROLLBACK exercises both returned failure and
unwind after event writing. The failed Store is marked unusable in correctness
state outside disposable caches, its derived caches are cleared, and cleanup
attempts an empty in-memory replacement plus close of the real connection. A
new real writer and fresh Store/journal owner then recover and publish once.
Secondary replacement/close failures remain attached to rollback/original
failure; unwind logs the secondary episode and preserves the original payload.
Callers must discard the failed handle even if secondary cleanup fails.
`SyncStorePool` now discards marked returns despite autocommit on the empty
connection, and the next checkout obtains a healthy Store. It does not reconnect
and retry an ambiguous save automatically. Recovery checkpoints are not retired.

Required operation/capture/owner/workspace/epoch/event records that disappear,
have the wrong SQLite type, exceed the record cap or fail validation now carry
`CORRUPT_CONTENT_SAVE`/Fatal. They no longer surface as unknown retryable SQL
errors. Optional receipt lookup for an unknown operation still returns absence.
An actual SQLite authorization failure retains its typed rusqlite cause rather
than a corrupt-content label; real contention/interruption/deadline cases keep
their own outcomes. The quarantined-handle code is explicitly Fatal. No new
status retry loop or unrelated engine quarantine is introduced.

A private branch publishes bytes identical to another prepared descriptor while
retaining private/non-publishable admission, POSIX mode and creation time. Both
private recovery intents remain retained. Canonical encodings, the private v2
journal, metadata schema, peer messages and activation policy are unchanged;
only volatile Store ownership, error normalization and pool return behavior
change. The real mount/runtime does not yet use this save component.

Final verification passed **292 cases** in one eight-target Windows release
invocation: lib: 108, activation_inbox: 22, content_admission: 8, content_saves: 30, content_transport: 42, content_write: 33, contracts: 41, startup_snapshots: 8. The new publication target reports 30 successes and one
ignored helper; its parent invokes the helper at four real exit-86 boundaries.
The write helper is likewise covered through its parent. Two library opt-ins
remain ignored. Strict Clippy on all library/test targets, format and diff checks
passed. The earlier 303-case source/probe/oracle result remains a separate report;
unselected provider, oracle, replication, long lane/worker and platform runs are
not attributed to this new source or waived. Draft repetitions add no successes.

The same-build publication probe retains one warm-up plus five paired samples
over identical 19-byte descriptors, with unchanged logical canonical counters.
Control event/outbox median is 1.5755 ms (range
1.3219–1.7016); candidate with receipt
median is 2.1393 ms (range
1.9866–2.6958), ratio
1.358x. Both use FULL/WAL. Capture/checkpoint/selection/canonical
preparation are excluded; this is component cost, not a full-save/native/W5/
mounted build regression pass, physical device accounting or a relaxed gate.

`test-evidence/wave-6/save-recovery-checks.json` retains the commands, outputs,
faults, before/final source identities, current executable hashes and all
individual samples. All 369 Cargo/build/src/tests/native/ui inputs were frozen
before final strict lint/qualification and rechecked afterward. Exactly five
existing inputs changed relative to the preceding qualified source: core Store,
publication module, error taxonomy, pool return and publication tests. No input
was added/removed. Final source SHA-256:
`df0bc121936d337b6d8b2f445c37b8cca236560164ecb90bd9f0416cb7ed553c`.

The next required step remains the actual session/metadata coordinator and safe
base/operation rollover with receipt-owned retirement while later writes survive.
Current v2 saved-map scanning and reclamation/rebase behavior still need fixed
local-work/amplification qualification after large and overlapping saves; a
single-patch/19-byte publication fixture is not evidence for that workload.
Pending create/delete/rename recovery, four-worker/64-job execution/batching,
off-lock slow metadata preparation, real slow-fsync journal coordination,
pending-root/unused-operation retention and scoped status remain open. Empty
replacement allocation/OS close failure, actual ENOSPC/power loss, global
shutdown bounds and full WinFsp/FUSE/two-peer/WSL native/W5/W6 build/load/soak
gates remain unqualified, alongside WP6.3 budgets/late effects and WP6.5/6
cache/change-journal/compatibility/migration/activation/retention/handoff. The
complete Wave 6 goal stays active and activation remains off.


## Exact receipt history and metadata continuation (WP6.4, in progress)

CCR-WP6.4-5 adds the metadata half of a save-generation handoff. The immutable
`content_save_history` archive retains bounded checksummed capture, selected
checkpoint and receipt records, indexed by operation/checkpoint and uniquely by
operation/generation. Publication commits and verifies the archive together with
event, root metadata/admission outbox and current receipt in FULL/WAL. Ignored or
discarded history cannot acknowledge a save. No committed event is rewritten.

Exact `content_checkpoint_receipt` lookup and old committed retries now survive
later generations without replacing the latest capture or selection. The old
operation-only lookup describes the current generation; its absence must not be
used as historical receipt absence or deletion authority. Missing parent history
proved by a successor is corruption. Pre-archive current receipts can be backfilled
only after validating their actual immutable event/root/outbox evidence.

`continue_content_save` archives/verifies the committed parent, claims the next
local entity generation and records one immutable successor capture/mapping in a
FULL/WAL transaction. The operation UUID stays stable for future journal adoption
without renaming all patches. Initial event UUIDs remain unchanged; successor IDs
hash a versioned domain, operation/generation and predecessor checkpoint digest.
One generation publishes one checkpoint. Its byte base is the actual committed
descriptor and its content/alive parents are the committed parent's event, even
when remote competing content is currently visible. The new frontier/clock/basic
template is captured once; lost-response replay does not recapture newer state.

Local rename/delete/basic/new-operation and checkout changes fence creation.
Existing handoffs can be retrieved as immutable recovery evidence after later
generations, validated against current or archived child captures through indexed
lookups. Retrieval is not permission for stale publication. Record persistence,
ownership, successor mapping and parent history are checked before acknowledgement.
Uncommitted parent is Contention; lost continuation persistence is scoped Fatal.

The opaque, non-deserializable `SaveContinuation` binds previous checkpoint,
next intent, canonical native store path and a retained base pin. It is not byte
availability proof, journal deletion authority or a completed mutable rollover.
The v2 private journal remains unchanged and cannot adopt that next intent yet.
Tests deliberately retain the original journal and use separate fixture journals
for subsequent metadata generations. The original later checkpoint/bytes remain
readable and are not discarded by an older receipt or continuation.

Ten new parent cases cover exact/multi-generation receipt replay, latest-selection
isolation, later original-journal work retained, remote content alternatives and
the causal oracle, local/checkout fences, discarded writes, corrupt/missing
history/mappings and initial capture byte-codec compatibility. Initial capture
serialization omits the optional predecessor field, preserving its previous bytes
and checksum; this is not an actual old-executable upgrade/downgrade rehearsal.
Four additional real child exit-86 boundaries cover archive writing and before/
written/committed continuation, alongside four existing publication boundaries.
Continuation unwind and real authorizer-denied rollback preserve the committed
parent and recover through a fresh Store. Helper invocations are not extra passes.

Final verification passed **227 reported cases** in one six-target Windows
release invocation: lib: 108, activation_inbox: 22, content_admission: 8, content_saves: 40, contracts: 41, startup_snapshots: 8. The publication target has 40 successes and one
ignored helper, exercised by its parents at eight process-exit boundaries. Two
library opt-ins remain ignored. Strict Clippy across all library/test targets,
format and diff checks passed. Earlier source totals and unselected provider,
write, transport, oracle, replication, long lane/worker/platform qualifications
are not attributed to this source or waived. Draft repetition adds no successes.

The publication probe retains one warm-up and five measured same-build pairs on
identical 19-byte descriptors. Control event/outbox median is
1.4044 ms (range 1.3191–1.6699);
candidate with receipt/history median is 2.7496 ms (range
2.4869–3.3152), ratio
1.958x. Both use FULL/WAL; capture/checkpoint/selection, canonical
preparation and continuation are outside timing. Logical canonical counters stay
unchanged. These are component costs, not native/W5/mounted or full-save build
regression, physical-device amplification or a performance gate adjustment.

`test-evidence/wave-6/save-continuation-checks.json` retains commands, outputs,
development stages, samples and current source/executable identities. All 369
Cargo/build/src/tests/native/ui inputs were frozen before final strict lint/
qualification and rechecked afterward. Only publication module, error extension
and publication tests changed; no input was added/removed. Final source SHA-256:
`f472823b02ae98f201affcfe76e83c2f6dceb1d782cce8b3d87ff0225b67b8bd`.

The next receiver is a correctness/performance requirement, not a small base
pointer replacement. Current v2 saved maps contain cumulative patches against
the original base, and map verification scans those rows. To adopt the canonical
parent while retaining later edits, it must identify later patch generations,
atomically fence old handles and preserve truncate/grow zero semantics without
rekeying/copying all historical patches under one broad transaction. Physical
retirement/reclamation must be bounded and retain unknown reachability and
in-flight readers; an opaque metadata handoff alone does not permit deletion.

A concrete code-inspection counterexample for receiver design: start with a
10-byte base, checkpoint an append with data through byte 100, then truncate to
50 and grow to 100 after that checkpoint. Current `base_visible_len` stays 10
for both states. Switching to the 100-byte canonical parent and simply ignoring
its old patches can resurrect bytes 50..100. Durable truncation-after-checkpoint
watermarks or an equivalent immutable map difference are needed, not just the
original low-water mark. This is an implementation constraint identified from
the current code, not a new passing workload or a frozen private-v3 format.

The full task still requires actual same-journal handoff/rollover and receipt-owned
retirement, crash recovery of each cross-database boundary, fixed local work after
large/overlapping saves, namespace/create/delete/rename recovery, four-worker/
64-job scheduling/batching, off-lock slow metadata preparation, slow-fsync journal
coordination, retention/status and all WinFsp/FUSE/two-peer/WSL/native/W5/W6 build/
load/amplification/soak gates. WP6.3 budgets/late effects and WP6.5/6 cache/change
journal/compatibility/migration/activation/retention/handoff remain open. The
complete Wave 6 objective stays active; no live store/mount was activated.


### WP6.4 immutable overlay index foundation (CCR-WP6.4-6)

The pending journal receiver now has `content::overlay_map`: a separate immutable
SQLite sparse index with authenticated patch and truncation-sequence roots.
The active private v2 journal, metadata continuation implementation, runtime and
peer/canonical formats are unchanged. This is a prerequisite for bounded adoption,
not same-journal rollover or a mounted save implementation. No live data was
migrated and activation remains off.

Sixteen-way radix nodes are bounded to 1 KiB, scoped to operation UUID/kind/level/
prefix and hashed with a private versioned domain. Branch summaries include leaf
count, maximum mutation sequence and minimum cut length; demand reads verify
hash/body/shape/stats and never treat missing or malformed storage as a hole.
Oversized and wrongly typed bodies are rejected before BLOB materialization.
Patch point paths have eight branch levels; cut sequences use all 64 bits.
An independent Python binary/root vector and actual FULL/WAL reopen retain the
format contract. Returned roots must be sealed/persisted by the caller's intent
transaction and pinned for lifetime, independently of byte availability.

Sequence floors hide published entries without copying old leaves. Physical
`entries()` includes hidden leaves. Relative truncation minima over
`(published_sequence, target_sequence]` retain shrink/grow zero semantics; the
test byte oracle distinguishes an earlier Saved snapshot from later Working
cuts. Subtree truncation and sorted streaming bulk construction avoid preserving
point-update history for every seed entry. No collection is performed.

Walk/prune cursors bound memory by depth/fanout and poison partial failed work.
Pruning stages immutable parents in pages and yields a replacement root only on
completion; budget-one tests commit each staging transaction and retain the old
root. Restart after page rollback/commit failure, failed install or cancellation.
The embedding collector still needs its own root-set/owner fence and reader pins.
Each page permits 1..256 node read/install units; each install has an additional
bounded verification SELECT, separately counted by `node_checks`. The unit
budget does not describe all SQL, payload, WAL or device operations.

An actual BEFORE INSERT RAISE(IGNORE) reproduced a false successful nonexistent
root on source `c567bf9015fca9b67627bada645647fa43087d56b892e47c8412ed21887fbe6b`.
All 371 frozen inputs were checked unchanged before fixing it. Each install now
compares exact persisted bytes before success. Final cases cover ignored, deleted
and altered writes, plus ignored insertion with an exact preexisting node. Actual
SQL failures retain their cause. Caller-owned transactions are mandatory; the
new missing-transaction code is Fatal within that save scope. FULL persistence,
SQLite progress/busy policy and atomic intent publication belong to the caller.

Final Windows release qualification passed **237 cases** in one invocation:
lib 108, content_saves 40, content_write 33, contracts 41 and overlay_map 15.
Four ignored cases are two library opt-ins and two child helpers exercised by
their parents; child exits and draft repetitions add no reported successes.
Strict Clippy across all library/test targets, format and diff checks passed.
The map cases include a 320-step BTreeMap mutation/truncation oracle, immutable
forks, scope/version/corruption fences, mixed-generation pruning of both kinds
against an independent sequence filter, cancellation poisoning and u64-tail cuts.

The first expanded attempt failed only because the existing literal classifier
read binary magic `TKOM` as an error code. Its frozen source, four invoked binary
hashes, 221 successes/one failure and raw output are retained independently.
The exact magic was added to the existing non-code list beside `TKFC`/`TKFM`;
no error class or gate was relaxed. Final results above replace that attempt.

At both 5,000 and 50,000 seeded entries, a point read uses nine traversal reads;
an update uses nine reads/nine installs/nine readbacks and 7,491 encoded bytes.
Pruning old generations while retaining one newer patch uses nine reads/eight
installs/eight readbacks and 7,416 bytes in five budget-four pages. Bulk seeds
retain 5,340/53,339 nodes respectively. Each fixture's FULL/WAL seed commits
before timing. One warm-up and five measurements per size time only component
work, with transaction begin/commit/fsync and payload work excluded. Median
read/update/prune milliseconds are 0.0254/0.0644/0.0614
at 5,000 and 0.0258/0.1737/0.1779 at 50,000. Raw samples/ranges
remain in evidence. Fixed logical node counts do not establish constant SQLite
B-tree/cache/WAL/device cost or latency, or pass a native/W5/mounted build,
full-save, scale/amplification or physical-I/O gate.

`test-evidence/wave-6/overlay-map-checks.json` retains commands, outputs, samples,
before-fix failures, source manifests and final executable hashes. All 371
Cargo/build/src/tests/native/ui inputs were frozen before final lint/tests and
rechecked afterward. Added: map module/tests. Changed: content exports, error
registry and classifier's exact binary-magic exception. Final source SHA-256:
`c0c64cac62f4205dd3fb41a966bfce58bbe559a8ce4105a95101b82cf63ae87c`.

Next work remains the actual private-journal receiver: sealed durable map roots,
sequence/floor/cut state, stable operation UUID with a new owner/base pin, exact
checkpoint history and retry markers, staging/revalidation of later Saved and
Working work and cross-database recovery. The old v2 maps still scan all rows;
this new module does not remove that cost until integration. Receipt-owned
retirement, root registry/reader retention, bounded physical reclamation, slow
fsync coordination, namespace intents, save executor/batching and runtime/status
integration remain pending. The full Wave 6 goal, including WP6.3 remaining
budgets/late effects, WP6.5 cache/change journal, WP6.6 compatibility/migration/
activation/retention/handoff and every platform/build/load/soak gate, stays active.


### WP6.4 actual immutable-root journal integration (CCR-WP6.4-7)

`WriteJournal` now uses the immutable sparse maps, replacing the mutable
`changes`/`saved_chunks` indexes and complete Saved-map verification scans.
Working has cumulative patch/cut roots and a separate pending-patch root; Saved
has independent immutable roots in its checksummed bounded checkpoint. Root
bodies are verified before summaries are used, descendants on demand. Canonical
preparation uses authenticated range-existence queries to skip untouched spans.
The map's new range query is checked against an independent sequence-filtered
BTreeMap at boundary and full-u32 key ranges, including missing-root failures.

Ordinary writes path-copy their Working/pending paths and verify payload storage.
Shrinking drops complete subtrees, clips the last partial patch and records a
sequence/length cut. A clipped Saved boundary consumes pending quota. Growing
cannot restore old masked bytes or increase base visibility. Saved snapshots
retain their old bytes through newer Working writes/truncations; resume assigns
a fresh owner from Saved even if the newer buffered header is damaged.

FULL/WAL checkpoint verifies only the bounded pending patches and payloads
against Working, then commits Saved root references and clears pending state.
Global dirty-group/pending-byte accounting uses an indexed dirty-session query;
64 groups/64 MiB and 1 MiB producer limits remain. NORMAL ordinary buffering is
unchanged. The private connection mutex still spans FULL checkpoint work;
independent reads under actual slow fsync and the executor/batching are pending.

Private `user_version` is 3 at the unchanged journal path/application ID.
Experimental v1/v2 and future/foreign schemas are refused before mutation;
tests retain their raw records. Mandatory private root serialization changes the
checkpoint codec. No live migration, old-executable downgrade/fencing rehearsal
or production activation occurred. Historical experimental checkpoint metadata
also needs an explicit compatibility policy before activation. Canonical bytes,
peer wires, metadata event identities and SaveIntent capture codec stay unchanged.

Complete Working/Saved state, flags/count, seal, inventory and payloads are read
back before acknowledgement. Actual ignored header/payload writes and altered
Saved/inventory writes roll back without false success; the prior checkpoint
and newer healthy Working bytes remain readable. Missing/malformed required SQL
record values become scoped corruption, retaining actual SQL I/O/authorization/
interruption causes. Existing deadline/cancel/progress-hook/owner/crash tests pass.

Payload collection audits inventory and all live Working/Saved/pending/cut roots
in pages, read-back marking payloads in SQLite with a fresh pass UUID. Deletion
begins only after the entire audit verifies. Own/foreign commits or another
journal instance restart it. Missing roots/intents and ignored marks cannot
authorize deletion. Combined node/scope validation plus payload examinations
stay <=256 per page; additional mark/readback SQL is not hidden in that unit.
Maintenance failures remain independently visible and do not fail unrelated
already committed healthy writes. One mark row per hash is overwritten by later
passes and removed with collected payloads. Historical immutable nodes are still
retained, and continuous root changes can prevent a long audit finishing; bounded
page work does not qualify history storage, reclamation progress or long soak.

Final Windows release verification passed **243 cases** in one five-target
invocation: lib 109, content_saves 40, content_write 37, contracts 41, overlay_map
16. Four ignored cases are two library opt-ins and two child helpers invoked by
their parents; helpers and development repeats add no successes. Four new write
cases cover header/inventory false acknowledgements, ignored payload, ignored GC
marks and explicit v2 refusal. Existing byte/oracle, sparse/grow/shrink, independent
Saved recovery, native preparation, bounded collection, foreign-commit/instance,
deadline/cancel, child process exit and metadata receipt/history suites reran.
Strict Clippy across all library/test targets, format and diff checks passed.

A new library probe measures the actual journal after committed 5,000/50,000-leaf
Saved roots. Each size bulk-seeds one authenticated sparse root and one shared
73,728-byte masked payload before timing; this excludes initial edit-history
cost and is not a dense full-file workload. One warm-up and five measurements
read one byte, change it and FULL-checkpoint, including transaction/fsync/payload
verification and one bounded maintenance page. SQLite total_changes deltas are
256 rows at both sizes, below the 400-row bound. That counter excludes reads and
is not physical write-byte amplification. Median read milliseconds:
0.2309 / 0.1787; write+FULL-checkpoint medians:
3.4512 / 4.4790.
Measured save ranges are 3.1276–9.7323
and 4.0927–20.5917 ms.
All samples and standalone index samples remain in evidence. These are component
results, not mounted/native/W5/W6 build, complete save-pipeline, scale, physical
device/WAL amplification or slow-fsync coordination release-gate evidence.

`test-evidence/wave-6/root-journal-checks.json` retains commands, outputs, source/
binary hashes and samples. All 371 Cargo/build/src/tests/native/ui inputs were
frozen before final strict lint/qualification and rechecked afterward. Changed
since the previous stage: journal, map, journal/map tests and the classifier's
exact measurement-label exception. No input was added/removed. Source SHA-256:
`a6206b64d4b373e9096c68d7f3fcca8f3ef6c3e64a2655e91469b304548c589a`.

Next remains actual same-journal SaveContinuation adoption: retain exact prior
checkpoint evidence, stage later Saved/Working/cut roots in bounded pages,
revalidate concurrent work, atomically replace owner/intent/base pin, persist
lost-reply markers and recover each cross-database boundary. The root sequence
floor is not advanced yet. Receipt-owned retirement, root registry/read lifetimes,
node reclamation and collection progress remain required. Namespace intents,
runtime adapters/status, save workers/batching and every original platform/
build/load/soak gate stay open, along with remaining WP6.3/6.5/6.6 work. The full
Wave 6 goal remains active and activation stays off.


### WP6.4 same-journal continuation adoption (CCR-WP6.4-8)

The private receiver now implements `WriteSession::adopt_continuation` on the
same operation UUID/journal, with an actual new owner and native base pin. It
checks the opaque metadata handoff, canonical store/provider identity and exact
private checkpoint history. Working/Saved roots are staged in <=64 node-unit
pages with the mutex released between commits. Their relative cut minima and
publication floor preserve independent later Saved and buffered Working views.
The final FULL/WAL transaction revalidates the complete captured state, stores
a retained predecessor and atomically switches owner/intent/roots. A concurrent
mutation/checkpoint/resume returns stale; no staged result replaces later work.

The new session preserves pending ordinary bytes/quota and invalidates old
handles/preparations. A marker binds old owner, exact predecessor and archive;
a committed lost-response retry verifies them and returns the same current new
owner without another transition/archive, preserving that owner's later edits.
Resume clears the marker and assigns a fresh owner, so old retries cannot steal
it. Later continuations likewise fence older handoffs. A clean rebased Saved
baseline supports resume but is excluded from pending/public checkpoint output
and rejected by metadata selection, avoiding a synthetic no-op event. Metadata
live generation/view/workspace checks still govern actual publication.

`checkpoint_records` contains indexed checksummed identities, not delete/read
authority for superseded selections. `retained_overlays` stores independently
bounded Working/Saved records, scoped/sealed by archive ID and operation. Its
seal joins inventory; collector audits current sessions and archives before any
private payload deletion. Missing/altered/whole-row-lost archives or discarded
writes fail closed. Actual canonical-byte deletion does not authorize reclaiming
the predecessor payload. No receipt-owned deletion is performed. Native old-base
CAS root registration, in-flight lifetimes and automatic recovery using retained
predecessors still need runtime integration; private payload retention alone is
not a complete native base lifetime/availability or recovery guarantee.

Transaction cleanup also closes a false-replay hazard: an actual denied rollback
could otherwise leave tentative marker/header changes visible to the next caller.
The guard clears the request hook, marks a non-cache failed-connection flag,
retries rollback and attempts replacement/physical close. Future requests fail
`WRITE_JOURNAL_CONNECTION_FAILED` even after successful replacement; discard and
reopen before recovery. A library case tests actual authorizer-denied rollback
for returned failure and unwind after the tentative owner write, verifies no
replay acknowledgement and reopens the original Saved checkpoint. OS close,
allocation failure, ENOSPC and power loss remain separate platform gates.

Private `user_version` is 4 for the publication flag/index, history, archives and
marker. V1/V2/V3, future and foreign schemas are refused without rewriting their
recovery records. V3 root-era schema/raw-byte refusal is covered explicitly;
this is not migration or an old-executable upgrade/downgrade rehearsal. Canonical
content/wires, SaveIntent capture codec and event identity algorithms stay fixed.

Final Windows release qualification passed **255 cases** in one five-target
invocation: lib 111, content_saves 49, content_write 38, contracts 41, overlay_map
16. Four ignored cases are two library opt-ins and two child helpers invoked by
their parents; repeated drafts/helpers add no successes. Nine added save parents
cover a real same-journal event chain/no-op baseline, independent Saved/Working
shrink/grow, six returned-failure/lost-response boundaries, concurrent staging,
canonical loss/archive retention, missing history/discarded retention, wrong
store/provider, six real child exit-86 adoption boundaries, and cancellation/SQL
deadline. Existing metadata-only fixture generations remain labeled as such.
Strict all-library/test Clippy, format and diff checks passed on the frozen source.

A new library probe uses genuine metadata handoffs after 5,000/50,000 published
patches with both a later Saved edit and another buffered edit. One warm-up and
five measurements use separate real disk/FULL-WAL fixtures. Setup bulk-seeds a
complete patch root and an independently built equivalent zero DAG; its initial
PreparedSave is a trusted private fixture over that root/pin, not the actual
full 50,000-patch preparation pipeline. Initial edits, preparation and metadata
handoff are outside adoption timing. Later Saved native preparation and byte
checks run afterward. Timed receiver work includes preflight, staging, header/
archive/history verification and final FULL fsync. It uses <=18 staging reads,
<=16 installs/readbacks and <=32 changed SQLite rows at both sizes, leaving one
Saved patch; old recovery roots remain retained. Staging counters exclude
preflight/header queries and total_changes is not read/statement/physical byte
accounting. Median adoption milliseconds: 5.0086 / 1.5934;
ranges 1.1904–6.4795 / 1.1967–1.8736.
Raw samples remain in evidence. This does not pass full-save, mounted/native/W5/
W6 build, physical/WAL/device amplification, long storage or soak gates.

The first unbound compile caught generic ContentProvider lacking an identity
method; identity is checked using its returned pin outside the journal mutex.
An unfrozen development test tool session ended with code 1 and only Compiling
logged, without Rust diagnostics or test results; cargo/rustc were absent when
checked afterward. Cause is unproven. Final compilation used one Cargo job;
no successes or source/binary qualification are attributed to that aborted run.

`test-evidence/wave-6/adoption-checks.json` retains commands, raw outputs,
development attempts, current source/executable identities and component samples.
All 371 Cargo/build/src/tests/native/ui inputs were frozen before strict lint/
qualification and checked unchanged afterward. Only journal, metadata selection,
error registry and save/write/classifier tests changed; no input added/removed.
Source SHA-256:
`5ddb4ed0b71bc92beb1477956cb0a79fe485036bdc883174a4698b09c78fcce5`.

The full Wave 6 objective remains active. Receipt-owned logical/physical
retirement, checkpoint/node/archive reclamation and collection progress are not
implemented; historical storage grows conservatively. Native root registration/
read lifetimes and automatic retained recovery, namespace intents, four-worker/
64-job scheduling/batching, actual slow-fsync independent access, checkout/
shutdown, runtime adapters/status and unused captures still need work. All
original platform/build/load/outage/soak/physical amplification and remaining
WP6.3/6.5/6.6 compatibility/migration/activation/retention/handoff gates remain
required. Activation stays off and no failed gate is waived.

The six new hard-exit cases use the published baseline. Later Saved/Working
truncate/grow views pass live adoption and postcommit reopen, but the next crash
qualification must repeat every private boundary with that overlapping state;
baseline exits are not evidence for the full overlapping-save crash matrix.


### WP6.4 overlapping-save process-exit matrix (CCR-WP6.4-8 qualification)

The six private adoption boundaries now run against both the published baseline
and distinct later Saved/Working states: twelve real child process exits with
code 86. Production code and formats are unchanged; only publication test/helper
source changed. The exact older parent is passed separately to the child so
recovery cannot accidentally continue the newest unpublished checkpoint.

The overlap fixture publishes 100 bytes at sequence 2, shrinks to 50 and grows
to 100, then FULL-checkpoints Saved at sequence 4. The child resumes under a new
owner and writes ordinary buffered bytes at 75, producing Working/leaf sequence
6 with valid length 89. Each precommit exit retains the original intent/floor
and no archive; the committed exit has next intent/publication floor and exactly
one retained archive. Parent inspection before resume verifies actual persisted
Working sequence, base visibility, authenticated leaf/payload SHA-256, buffered
bytes and masks for the zero gap. Product collection audits typed scope/header
seals, inventory and all current/retained roots before recovery changes state.

Fresh resume intentionally selects promised Saved and discards ordinary Working;
observing buffered bytes in SQLite is not a new checkpoint promise. Precommit
cases retry adoption; committed cases resume the rebased Saved and reject the
old handoff. Every overlap case recovers Saved sequence 4 with visibility 50,
reproduces the 100-byte precrash Saved view, builds native content equal to byte
import and publishes/replays exactly one next event/admission (two admissions
including the older parent). Exact old receipt and derived-state oracle remain
valid, and archive count stays one. Baseline cases remain clean without a no-op
publication. These are actual process exits/reopens, not power loss or ENOSPC.

Final same-source Windows release qualification passed **255 reported cases**:
lib 111, content_saves 49, content_write 38, contracts 41 and overlay_map 16.
The enlarged crash matrix remains one parent case; neither its twelve helper
invocations nor six added overlap executions inflate the reported total. Two
library opt-ins and two parent-invoked helpers remain ignored. Strict Clippy
across all library/test targets, format and diff checks passed. All five suites
reran; earlier totals are not added or retroactively relabeled.

`test-evidence/wave-6/overlap-crash-checks.json` preserves commands, twelve matrix
records, raw outputs and executable/source identities. All 371 Cargo/build/src/
tests/native/ui inputs were frozen before lint/qualification and rechecked
unchanged. The only source delta is `tests/content_saves.rs`; no input was added
or removed. Source SHA-256:
`b880ddbeacc978c4341b9f5415e9f81f974bcdd6985293998585a2ed232580b0`.

This closes the previously documented baseline-only hard-exit evidence gap for
these six adoption boundaries. Native base/root registration and in-flight
lifetimes, automatic retained recovery, receipt-owned retirement, bounded
checkpoint/node/archive reclamation and collection progress still need work.
Namespace intents, executor/batching, actual slow-fsync independent access,
checkout/shutdown/status and runtime adapters remain incomplete. The complete
Wave 6 objective, all remaining WP6.3/6.5/6.6 work and original platform/native/
W5/W6 build/load/outage/soak/physical amplification/compatibility gates stay
active. No live store was activated and no failed gate was waived.


### WP6.4 private root source and immutable retained reads (CCR-WP6.4-9)

The journal now enumerates recovery base FileRevisions from current sessions and
retained predecessors in pages of 1..64 records, validating typed headers/scope
seals and inventory count/XOR. Unsaved sessions are included by the source query.
Cursors bind instance/SQLite own and foreign change tokens; a completion fence is
available only after the whole source audit and can be revalidated. Partial or
stale results are not complete reachability. No complete-root vector/permanent
pin is eagerly loaded at startup. Any journal write/maintenance can conservatively
invalidate a pass; continuous-write progress is not qualified.

Opaque source roots can acquire a same-store native base pin outside the journal
mutex and revalidate afterward. These pins protect lifetime, not availability.
The source is private recovery data only: metadata/history/conflict/snapshot and
explicit/in-flight roots plus a native deletion barrier still need reconciliation
before collection. No native deletion or whole-store reachability authority is
returned, and no automatic runtime registration is implied.

`open_retained_checkpoint` exposes immutable archived Saved bytes independently
of the active owner. It checks archive/seal/root identity, reserves one of at most
64 read tickets per journal under its mutex, pins the provider base outside the
mutex and rechecks. Failed pin/acquisition and reader drop return the ticket.
The reader owns journal/provider/base lifetimes after old sessions close and the
journal reopens. Future retirement must inspect these tickets atomically under
the journal mutex; no archive retirement is introduced here. Shared range code
applies the same valid-mask/base-visibility rules to live and retained readers.
Base byte I/O is outside the private mutex, with archive/context checks at each
chunk and after completion; error output must be discarded.

`restore_native` explicitly reconstructs and pins retained Saved content using
native COW/zero spans and normal verified installation. It yields content, not
PreparedSave or metadata ownership/publication authority. A needed legacy base
converts once per reader/target before builder buffer acquisition; full masking
avoids hydration of an absent legacy base. Cached conversion works after removal
of the legacy source file. Necessary unavailable old-base bytes/structure still
fail; automatic live fallback and native reconciliation remain future work.

Final Windows release qualification passed **277 reported cases** in seven
selected targets: lib 112, content_provider 12, content_retention 9, content_saves
49, content_write 38, contracts 41, overlay_map 16. Strict Clippy across all
library/test targets, format and diff checks passed. Five ignored cases are two
library opt-ins, the provider 1GiB opt-in and two parent-invoked process helpers;
helper/scenario/draft invocations are not additional successes. Earlier source
and unselected platform/performance results are not attributed to this source.

Nine new integration parents use actual two-generation metadata publications/
adoptions and disk-backed SQLite/native CAS. They verify bounded roots and native
pins/reopen; cursor/fence staleness and whole-scope inventory loss; cross-instance
limits/cancellation; base pin and Saved bytes after all live sessions close;
actual canonical-manifest deletion followed by reconstruction without a metadata
event; slow base read alongside current write/FULL checkpoint; cancelled base I/O
releasing pin/ticket; reader quota/failed pin cleanup; and archive corruption.
The slow-I/O case bounds the independent current operation with a 300ms deadline;
it does not establish independent reads under slow private FULL fsync. A new
library parent uses trusted private archive fixtures for legacy cached conversion
and full replacement without the missing base, compared to native byte import.
Existing provider, live journal, byte/oracle, publication/crash and map suites
reran after extracting shared overlay application. No schema/canonical/wire/
event/capture encoding changes; private user_version remains 4.

`test-evidence/wave-6/retained-reader-checks.json` retains exact commands, outputs,
source/executable hashes and development diagnostics. All 373 Cargo/build/src/
tests/native/ui inputs were frozen before final lint/qualification and checked
unchanged afterward. Added: private retention module and integration target;
changed: journal exports/reader registry/shared range helper. No input removed.
Source SHA-256:
`8db8182fa56f84c58e1241ae0d364ed7928949f4f9f07b3acf7e68cc933038b0`.

The full Wave 6 goal remains active. Runtime native-root registration across all
sources, deletion/in-flight barriers, automatic retained fallback, receipt-owned
logical/physical retirement, node/checkpoint/archive reclamation and collection
progress/storage bounds are still incomplete. Namespace intents, save executor/
batching, slow-fsync independent journal access, checkout/shutdown/status and
mount adapters still need work. WP6.3 residual budgets/late effects, WP6.5 caches/
change journal, WP6.6 compatibility/isolated-store/old-executable migration/
downgrade/activation/retention/handoff, and every original platform/build/load/
outage/soak/physical amplification gate remain required. Activation stays off.

### WP6.4 bounded orphan private node collection (CCR-WP6.4-10)

Private maintenance now reclaims superseded immutable map nodes in addition to
unreferenced payloads. Bounded walks report verified node hashes; the collector
marks all current Working/pending/Saved and retained predecessor patch/cut roots
at floor zero, verifies mark readbacks and completes the existing sealed scope
inventory count/XOR audit before sweeping. An epoch only authorizes deletion
after that complete audit. Superseded checkpoint identity records remain; they
do not grant arbitrary historical private byte reads. Archived roots remain
protected even when canonical bytes are unavailable.

Audit, payload and node candidate work share 256 units per page. The existing
removed/examined fields remain payload counts; new node counters are separate.
Mark writes/readbacks are extra SQL operations inside a traversal unit, not a
256-statement or physical-I/O bound. Existing instance/SQLite own/foreign change
tokens restart stale passes. Each adoption reserves a bounded journal-local RAII
staging lease under the journal mutex before any NORMAL staging commit. While
one exists, node sweep defers with a continuation, protecting not-yet-installed
roots through final FULL commit, failure or unwind. Up to four stages are
admitted; the runtime four-worker/64-job save executor is still unimplemented.

Node marks use connection-local FILE-backed TEMP storage keyed by node hash.
Reopen loses marks/cursors and re-audits; committed roots protect their nodes,
while abandoned staging nodes are orphans. Main/temp SQLite caches are configured
1536/512 KiB, preserving the former aggregate 2048 KiB configuration. This does
not establish an RSS or disk bound. No persistent schema/canonical/wire/event/
capture codec changes; private user_version stays 4. SQL row deletion does not
promise file shrinking, freelist reuse or sustained storage progress.

Final Windows release qualification passed **281 reported cases** in seven
selected targets: lib 113, content_provider 12, content_retention 9, content_saves
50, content_write 39, contracts 41, overlay_map 17. Strict Clippy across all
library/test targets, format and diff checks passed. Five ignored cases remain:
two library opt-ins, the provider 1GiB opt-in and two parent-invoked process
helpers. Child scenarios, draft runs and prior-source totals are not extra
successes.

Four new parents cover exact verified walk-node enumeration with small pages;
200 hot-chunk rewrites ending with exactly nine live nodes and correct Saved
bytes after reopen; real metadata adoption paused after staging commit while
collection defers, followed by valid Working/Saved bytes after commit/collection;
and dropped TEMP marks refusing deletion before a successful retry removes a
malformed unreferenced node, also checking cache configuration. Existing retained
reader reopen coverage now completes GC with its ticket/base pin held, then
reads archived bytes after live handles/store close. Implicit maintenance already
removed obsolete hot-chunk nodes, so its explicit final pass removes zero and
examines the nine live nodes; this is not a physical storage/amplification claim.

The 5K/50K root-journal probe allows at most 600 changed SQL rows rather than 400
because each audited node now adds a TEMP mark DML. This source measured 512 rows
per point write/FULL checkpoint at both scales, including its bounded maintenance
page. Counts include TEMP writes and exclude readbacks/physical I/O. Component
timings remain samples, not mounted build/latency gates or a waiver of any gate.

`test-evidence/wave-6/node-gc-checks.json` retains exact commands, outputs,
source/executable hashes and intermediate diagnostics. All 373 Cargo/build/src/
tests/native/ui inputs were frozen before final lint/qualification and checked
unchanged afterward. Six existing inputs changed: overlay map, write journal,
retention/save/write/map tests. No input added or removed. Source SHA-256:
`2824e3f495542b856d7b476ebdc8a5180fd85727e7e5f11de1970d4f4290cbba`.

The full Wave 6 goal remains active. Receipt-owned archive/checkpoint retirement,
native root registration across all sources/deletion barriers and actual native
GC/reader races, automatic live fallback, continuous-mutation collection progress
and long-run DB/temp/WAL/device storage bounds remain open. Namespace intents,
runtime/mount integration, executor/batching, slow-FULL-fsync independent journal
access, checkout/shutdown/status, residual WP6.3/6.5/6.6 work and every original
platform/build/load/outage/soak/compatibility gate remain required. Activation
stays off; no live workspace was migrated.

### WP6.4 runtime content-save coordinator (CCR-WP6.4-11)

Added a runtime component connecting private checkpoints, canonical preparation,
metadata selection/publication and view refresh, exact continuation and private
adoption. A stable SaveFile replaces its session only after successful adoption;
writes/truncation/checkpoints/adoption serialize per file, while canonical
preparation releases both file and metadata locks. New canonical content stays
pinned across publication until its continuation base pin is acquired/adopted.
Range reads capture the session without retaining the file mutex through base
I/O; stale output still must be discarded.

The executor has four fixed workers and at most 64 outstanding operations,
including active jobs and producers checkpointing before queue insertion.
Operation-key exclusion and UUID-owned RAII permits prevent duplicate jobs and
stale releases; saturation has no synchronous fallback. One same-store journal
Arc is required for all files; independent same-path or foreign journals cannot
multiply the private dirty-byte budget. A weak per-engine registration survives
cancelled owners while remaining workers/reservations finish, preventing a
replacement pool on that engine from multiplying blocked threads.

Enqueue preserves a newer Saved while an older committed parent needs adoption.
A bounded exact operation/generation metadata history lookup validates the
capture/intent/receipt and finds that parent after lost reply/reopen. It grants
no publication/deletion authority. Unpublished stale work can retry the latest
Saved. An adoption that committed before its reply can replay its old owner
marker. Explicit flush repeats through the carried work until clean or deadline;
it never waits for a peer. Enqueue alone is not a mounted close/fsync boundary:
lost-reply recovery may first adopt committed state whose carried Working still
needs checkpoint/publication. Adapter integration must enforce the full fence.

Operation cancellation links to pool stop; cancelling only a ticket wait leaves
the accepted operation running. Engine acquisition polls against context.
Errors/caught job panics return permits and retain recovery data; the pool does
not collect a completed-job history. Shutdown closes admission and caps its drain
wait at the earlier caller deadline or five seconds. Uninterruptible OS/provider
I/O and blocking cancellation hooks are not killed; timed-out workers stay
registered. Global lifecycle deadlines are not qualified by this local wait API.

The component is explicit and headless: no startup, RPC, WinFsp or FUSE callback
constructs it. Mounted engines and engines with legacy sessions/handles are
refused. This intermediate fence must be replaced by actual provider-backed
adapter integration; it does not close that requirement. Existing typed-content
fixture gating and legacy adapter behavior remain. No persistent schema,
canonical, wire, event or capture encoding change; private user_version stays 4.

Final Windows release qualification passed **301 reported cases** in nine
selected targets: lib 113, content_executor 11, content_provider 12,
content_retention 9, content_saves 50, content_write 39, contracts 41,
overlay_map 17 and pending_saves 9. Strict Clippy across all library/test targets,
format and diff checks passed. Six ignored cases are two library opt-ins, the
provider 1GiB opt-in and three parent-invoked child helpers. The preliminary
eleven-case executor run, child scenarios and earlier-source totals are not
additional reported passes.

Eleven new parents use the actual journal/metadata/native CAS: two-generation
flush with writes during preparation; lost replies at four publication/adoption
points; unpublished superseded retry; reopen with a committed parent and newer
Saved; caught preparation panic/worker reuse; four active plus 60 queued jobs,
saturation rejection, independent metadata, short shutdown timeout then
cancelled-job recovery retention; independent cancelled ticket wait; engine
acquisition timeout/permit cleanup; foreign/independent journal refusal; mounted
callback guard; and four real child process exits with fresh recovery, byte
checks, one publication and the derivation oracle. The saturation case leaves
64 Saved checkpoints, not canonical publications. Its 4000ms blocking fault
tests a bounded timeout and eventual drain, not arbitrary OS/provider shutdown.

`test-evidence/wave-6/executor-checks.json` retains exact commands, outputs,
source/executable hashes and development diagnostics. All 375 Cargo/build/src/
tests/native/ui inputs were frozen before the final strict lint/nine-target
qualification and checked unchanged afterward. Added runtime module/integration
target; changed journal accessors, metadata exact-generation history lookup,
Engine registration and runtime module registry. No input removed. Source SHA-256:
`9458e85f6d4ac7eb87be1e37be6c36606a4f1ef9e2d3765c48b029937bb0d783`.

Three known live draft compilations were deliberately stopped for reviewed pin
lifetime, journal-budget/API and test-fixture corrections. They were not
observation timeouts, test failures or passing qualification. Their raw logs
remain separate; final evidence uses only the later frozen source/run.

The full Wave 6 goal remains active. Namespace/create/delete/rename/atomic
replacement/open-handle recovery, startup reconciliation/autonomous retries,
Engine quiet/checkout/shutdown/status wiring, mounted reads/callback durability,
compatible checkpoint/metadata batching and slow-fsync independent private reads
remain required. Native root registration/deletion barriers/actual GC races,
receipt-owned retirement and sustained storage progress remain open, along with
all remaining WP6.3/6.5/6.6 work and the complete original platform, build/load,
outage, soak, physical amplification and compatibility gates. Activation stays
off and no live workspace was migrated.

### WP6.4 compatible private checkpoint groups (CCR-WP6.4-12)

The private journal now checkpoints up to 64 distinct same-instance owners in
one FULL transaction. It validates all requested owners/headers/roots before
advancing only their dirty sessions, preserving result order and the existing
pending-payload verification. Unrequested Working stays buffered. A failure
acknowledges none; a lost reply can leave the whole Saved group committed.
Single-file and quota checkpoint paths share this implementation.

The runtime exposes enqueue_batch and a bounded flush_files slice. It reserves
the whole group against the existing 64-outstanding budget before allocating
sorted file/guard/job vectors, locks files in UUID order, checkpoints once,
resolves committed older parents and queues independent publication/adoption on
the same four workers. Returned UUID/ticket pairs use that order. Duplicate,
oversized, foreign or earlier-pending groups cannot partially queue. Error after
checkpointing retains Saved for retry. Flush slices drain earlier pipelines and
repeat compatible groups through commit/adoption until clean or deadline. No
writer thread, inline fallback or enlarged budget was added.

Process-local checkpoint_work counters record successful logical FULL checkpoint
COMMIT calls and dirty sessions advanced, including empty/read-only transactions
if a COMMIT is issued. Empty input opens none. These counters reset on reopen and
exclude ordinary writes, adoption, metadata and canonical installation. They
are not physical fsync/WAL/device measurements or durable authority. A group of
64 private checkpoints is not one fsync for 64 complete saves; metadata remains
per revision. Headless/unintegrated callback and isolated-fixture gates remain.

Final Windows release qualification passed **307 reported cases** in nine
selected targets: lib 113, content_executor 14, content_provider 12,
content_retention 9, content_saves 50, content_write 42, contracts 41,
overlay_map 17 and pending_saves 9. Strict Clippy across library/all test targets,
format and diff checks passed. Seven ignored cases remain: two library opt-ins,
the provider 1GiB opt-in and four parent-invoked child helpers. The preliminary
executor14/write42 run, child scenarios and prior-source totals are not extra
reported successes.

Six new parents cover 64 real runtime files sharing one private checkpoint before
64 independent publications, byte checks and the derivation oracle; pre-commit
failure retaining Working without partial Saved/queue/slot updates and successful
retry; duplicate/oversized/foreign/pending groups refusing partial insertion;
both prior Saved surviving a failed group while newer Working stays visible,
followed by ordered retry of only requested owners; stale/foreign/duplicate
private owner refusal; and two real process exits before/after FULL commit with
fresh reopens of four files showing all-old or all-new Saved. The new ignored
helper runs only through that parent. This is process-exit recovery, not power
loss, device fsync counting or a mounted/build/performance gate.

`test-evidence/wave-6/checkpoint-batch-checks.json` retains commands, outputs,
source/executable hashes and development diagnostics. All 375 Cargo/build/src/
tests/native/ui inputs were frozen before final strict lint/nine-target release
qualification and checked unchanged afterward. Four existing inputs changed:
private journal, runtime coordinator and their write/executor tests; none added
or removed. Source SHA-256:
`14e70bde45deaaccd1016591608e17a2f94e072a958f8d85d55155384555b9e9`.

An initial new write-test compile lacked local fault imports; fixed before its
behavior run. The allocation-admission order was corrected while that draft was
compiling, so it is unbound intermediate evidence; final source was frozen and
all nine suites reran. No draft source/binary/pass attribution substitutes for
the final run.

The complete Wave 6 goal stays active. Automatic coalescing of independent close
producers, compatible metadata batching, slow-fsync independent private readers,
complete mounted close/fsync recovery fences, namespace/open-handle recovery,
startup retry/enumeration and Engine lifecycle/status integration remain open.
Native retention/deletion barriers/actual GC-reader races, receipt-owned record
retirement and sustained storage progress still need qualification. All other
WP6.3/6.5/6.6 work and original platform/build/load/outage/soak/physical storage/
compatibility gates remain required. No schema/canonical/wire/event changes;
private user_version remains 4, activation stays off, no live workspace migrated.

### WP6.4 descriptor-backed mounted local reads (CCR-WP6.4-13)

WinFsp and FUSE now select immutable typed descriptors for native reads. Entry
size remains the descriptor's validated length even when the manifest or an
unrelated chunk is absent. The adapters acquire a provider pin before
acknowledging open, do provider construction/pinning/range I/O outside Engine,
then revalidate the store, view, handle and selected descriptor. A bad provider
pin is refused, failed provisional opens are released, and concurrent losing
pins are dropped outside the metadata mutex. Lease release must be bounded and
perform no disk/network I/O. Providers own any asynchronous cleanup separately.

The session retains its pin until close or clean revision refresh. In-flight
reads retain their own immutable selection, so a concurrent revision change
cannot mix versions; old output fails STALE_VIEW and is discarded. A refresh to
the same descriptor reuses its binding. Clean non-writer sessions now refresh
the complete Entry metadata as well as their bytes, correcting stale handle
length/timestamps/revision. Dirty and writable bases retain their existing
rules; an unlinked open file retains its bytes.

Every provider read is at most 1 MiB. The public range API permits short reads
at that bound; WinFsp fills its borrowed destination through bounded calls to
the actual requested length/EOF. This avoids false EOF at the provider buffer
boundary. FUSE allocates only after typed selection within its existing 1 MiB
request/reply cap; open uses the existing bounded threaded dispatcher. Ready
verified ranges do not need other chunks, while missing/corrupt demanded data
fails rather than returning zeros or empty content. Native writable open,
snapshot, resize and direct staged read remain explicit guards pending COW
write integration. Existing WinFsp disabled caching and FUSE direct I/O remain.

Adapter contexts poll Engine acquisition with a deadline and link to the
registered object-worker stop token. Cancellation hooks now run outside the
inflight-objects mutex while preserving the post-cancellation state snapshot.
These changes do not forcibly interrupt arbitrary provider/OS blocking I/O or
qualify whole-runtime shutdown. Default LocalContent is constructed lazily
off-lock; a programmatic one-time provider seam permits independent providers
before any descriptor session. Neither path activates typed content in user
workspaces.

Final release qualification passed **359 reported cases on Windows** and
**366 on Linux/WSL**, in eleven selected targets on each: library 113/120,
content_mount_reads 10, content_transport 42, content_executor 14,
content_provider 12, content_retention 9, content_saves 50, content_write 42,
contracts 41, overlay_map 17 and pending_saves 9. Strict Clippy across the
library/all test targets, formatting and diff checks passed. Both platforms
used Rust/Clippy 1.96.0; WSL uses a named toolchain and keeps its default 1.88.
The per-platform executions are not 725 distinct cases. Eight ignored cases
remain in each main run: two library opt-ins, the provider 1GiB opt-in, four
parent-invoked crash helpers and the kernel mount opt-in.

The ten new ordinary parents exercise descriptor length/pins and ready ranges
with a truly absent unrelated chunk, missing manifests/corrupt demanded bytes,
empty/EOF and write guards, bounded provider calls with a larger WinFsp ABI
destination, slow pin/read with independent metadata, wrong pin refusal,
concurrent refresh and stale output, same-reference pin reuse/full metadata
refresh, legacy zero-wait and generation fences, Engine-lock deadlines and
mid-read cancellation/lifetime. Each platform also passed one explicit ignored
parent using a real kernel mount in a child process. These are two separate
platform results, with no extra child success counted. Both real mounts verify
a 257-byte ready range of a 196608-byte descriptor with its third chunk absent,
a complete 2097163-byte file, missing-demand and native-write refusal, then
refresh the same OS handle to 11 new bytes with updated size/timestamp. Pins
release after close, health stays clear and graceful detach succeeds. Only
temporary isolated fixture stores were mounted.

`test-evidence/wave-6/mounted-read-checks.json` retains commands, raw outputs,
source and all eleven executable hashes per platform, distinct supplemental
mount results and development diagnostics. All 377 Cargo/build/src/tests/native/
ui inputs were frozen before final lint/tests and checked unchanged afterward.
Two inputs were added (the runtime reader and integration target), eleven
existing inputs changed and none removed. Source SHA-256:
`c270ea38e1ee759e98913b4db3d4d2da435b9b6869bbf03667d1e2ec5df58892`.
A local ignored ZIP of the exact frozen inputs was read back and verified
against every member hash; its identity is recorded with the raw evidence.

Linux 1.96 strict lint exposed six existing Unix diagnostics: two nested
conditionals and four signal function-pointer casts. Corrected without warning
suppression. Cross-platform regression work also fixed a test observation race:
legacy bytes could be available before the fifth frame's completion counter
advanced. The test now observes that acknowledgement before cancellation under
the same one-second bound, preserving admission/availability timing. The legacy
retained-save writer test could finish its 32MiB save before collecting its
minimum 20 samples (17 free of 18). Its existing byte-store fault point now
delays 400ms, with an asserted hit and RAII reset; minimum samples and free ratio
remain unchanged, and writer-before-store would still fail. Preliminary failed
lint/test logs and one Windows link attempt against a still-running earlier
test binary remain separate. Final suites ran after source refreeze; no draft,
repeated run, helper invocation or earlier-source total is an extra success.

The full Wave 6 goal remains active. Foreground missing-range demand/lease
scheduling and availability-index repair are not yet connected to these
callbacks. Native COW mounted writes, durable namespace/open-handle recovery,
complete close/fsync/startup/quiet/checkout/shutdown/status integration,
automatic close-producer coalescing, metadata batching and slow-fsync independent
private readers remain required. Complete native root registration/deletion
barriers/actual GC races, automatic live retained fallback, receipt-owned
retirement and sustained storage progress remain open. All residual WP6.3,
WP6.5 and WP6.6 work and original platform/two-peer/ENOSPC/power-loss/OS-close/
allocation/build/load/outage/soak/physical amplification/compatibility gates
remain required. No schema/canonical/wire/event changes, private user_version 4,
no live activation or migration, and no gate waived.

### WP6.4 mounted missing-range demand and availability repair (CCR-WP6.4-14)

Native adapter reads now register missing ranges on the existing foreground
content scheduler. Ready local reads keep the direct provider path. The waiter
captures the exact session content event/entity reference, branch and checkout
generation; it revalidates them throughout Engine/SQL waits and at completion.
A newer event with identical bytes still stales an older waiter. Only the same
native provider owner and a currently granted shared reference can start network
demand. Private observations can clear stale local availability but never create
a remote waiter. Independent providers and typed legacy references retain their
own unavailable behavior; the ordinary legacy string hydration path is unchanged.

Demand uses the stable local workspace UUID already used by save intents. Its
existing config row is allocated lazily on first missing-range demand, off
Engine, and reused after reopen and by later saves. Read generation describes
the captured workspace view, with the immutable reference selecting the content
event; it confers no save/publication ownership. No schema/canonical/wire/event/
capture changes or new activation path were introduced.

A wait owns one <=1 MiB range lease until verification or failure. Actual
missing/corrupt object observations invalidate only that key's verified index
bit, preserving receipts and permissions. A new missing key or a contradicted
verified index rearms the lease, so an earlier satisfied index cannot hide
required repair. Joined installation ownership remains in the object engine;
independent callers retain independent deadlines/cancellation and pins.

Native waits charge a process-wide limit of 64 before opening secondary handles
or allocating demand state, plus the adapter's existing waiter slots. Ready
ranges need no slot. The new nowait pool checkout applies zero SQLite busy
timeout before view refresh, including for new connections, restores the prior
setting on return and keeps at most eight idle handles. SQL busy retries poll
the actual request context/selection; SQLite/provider work stays off Engine.
Public range contexts weak-link worker stop without cancelling caller tokens,
and adapter fences are checked during Engine and SQL acquisition. Cooperative
waiting is bounded; arbitrary blocking provider/OS I/O/fsync is not forcibly
interrupted. No new worker thread, retry daemon or inline transfer fallback.

Regression qualification uncovered a preexisting legacy authority race: global
stream reservation could fail, then its own credit could return before the
pending-work check. Seeing no pending work after that return prematurely ended
an unfinished round and changed its nonce. The sink now increments a local
credit epoch after dropping returned proofs and before clearing its retiring
count. Progress across refusal/pending makes the runner retry capacity before
yielding; no progress still yields to other scopes. Empty retirement does not
advance the epoch, and unwind releases proofs/count. This is readiness evidence,
not a durable generation or new authority; budgets and round caps stay intact.

Final qualification passed **369 reported cases on Windows** and **376 on
Linux/WSL**, in eleven targets per platform: library 113/120,
content_mount_reads 18, content_transport 44, content_executor 14,
content_provider 12, content_retention 9, content_saves 50, content_write 42,
contracts 41, overlay_map 17 and pending_saves 9. Strict Clippy for library/all
test targets, formatting and diff checks passed. Rust/Clippy 1.96.0 is used on
both platforms. Nine default ignores remain per platform: two library opt-ins,
the provider 1GiB opt-in, four parent-invoked crash helpers and two kernel mount
opt-ins. Platform repetitions are not 745 distinct cases.

Eight new ordinary reader parents cover exact waiter scope/stale verified-index
repair plus independent metadata and stable UUID reuse after reopen/save;
independent cancellation of two waiters; same-byte new-event and grant-withdrawal
fences; a held SQLite writer during first UUID allocation under a 35ms deadline,
with prior timeout restored; adapter slot saturation while a ready range still
reads, then stall/drain; private missing content without remote demand or UUID
allocation; worker stop without cancelling the caller token; and 64 pending
native readers, extra refusal and capacity/pin/idle-connection drain. A new
real-peer adapter parent reads 257 bytes of a three-chunk revision while the
source itself lacks the unrelated third chunk. The existing two-other-repository
saturation test now triggers demand through the actual adapter read API, keeping
the 250ms admission bound and requiring the result before stalled background
completion. One new legacy parent forces two 600ms retirements and an 850ms
post-refusal/pre-pending stall; data completes within the existing 5s bound under
one original nonce. The existing unwind unit also verifies zero/one credit-epoch
increments without adding another reported case.

Two explicit ignored parents passed separately on each platform, with one real
child apiece: the prior local mounted read/refresh test and a new real WinFsp/FUSE
mount fetching a missing range through an authenticated peer. The application
reads the correct 257 bytes and known 196608-byte length while the source's third
chunk remains absent. These four platform/parent executions are supplemental,
not added child successes or evidence of the full mounted build/load matrix.
Only isolated temporary fixture stores and peers were used.

`test-evidence/wave-6/mounted-demand-checks.json` retains commands, outputs,
per-platform source/executable identities and separate supplemental mount
results. All 377 Cargo/build/src/tests/native/ui inputs were frozen before final
qualification and rechecked afterward; nine existing inputs changed, none added
or removed. Source SHA-256:
`5489abad90d74c8f979d30a57c23bf6fc27d9e3eb08ce14787f6e9421b887b4f`.
The local ignored source ZIP is read back against every member hash.

Cargo selecting opt-ins rebuilt two Linux test paths. Final evidence therefore
uses a direct sequential rerun of all eleven actual release test images after
the opt-ins, without compilation, with SHA-256 checks before/after each run and
at completion. Supplemental images also remain unchanged since their own log
completion. The first verifier encountered a vanished WSL /tmp build cache at
the next invocation; no missing image was attributed. Linux was rebuilt and
requalified in persistent `/home/figloalds/.cache/tkfs-wave6-release`, then its
images directly verified. Original Cargo runs and verifier repetitions remain
raw evidence, not additional counted successes. Earlier mutable Windows guards
and the Linux two-nonce failure remain visible in the diagnostic record; no
assertion, timing bound, resource limit or original release gate was waived.

The complete Wave 6 goal remains active. Native mounted COW writes and complete
close/fsync/namespace/open-handle/startup/retry/lifecycle/status integration,
automatic close-producer coalescing, compatible metadata batching and slow-fsync
independent private readers remain required. Full root registration/deletion
barriers/actual GC races/live retained fallback/receipt-owned retirement and
sustained storage progress remain open. All residual WP6.3, WP6.5 and WP6.6 work
and original platform/two-peer/build/load/outage/soak/ENOSPC/power-loss/OS-close/
allocation/physical amplification/compatibility/activation/handoff gates remain
required. Isolated native demand tests do not close that complete release matrix.

### WP6.4 save metadata outside Engine (CCR-WP6.4-15)

The existing headless `SaveExecutor` held Engine while selecting checkpoints,
publishing a prepared revision and obtaining continuation metadata. That included
SQLite writer waits and FULL commit/fsync. These save-specific operations now
use zero-timeout secondary connections outside Engine. Short Engine acquisitions
check root, active intent branch, checkout generation, stop and switching; the
existing observation publication follows durable commit and rechecks that view.
The coordinator also refuses a quarantined primary and a caller's open primary
transaction rather than escaping it through a secondary connection.

Fenced receipt observation now explicitly advances `EngineVitals.store_revision`
without SQL so the status cell notices a secondary publication. The primary's
`total_changes` sees only its own writes. Review found this before final freeze;
the blocked-writer parent additionally asserts the revision advances. Retry may
advance it again: it is a freshness signal, not a durable event count.

When registered, the worker's connection pool is cloned by sharing its Arc and
eight-idle limit. Standalone headless callers use their own pool. Existing four
save workers and 64 outstanding operations remain; no additional threads or
larger queues. SQL writer acquisition polls the job deadline/cancellation, and
the secondary restores its previous busy timeout on return. A connection with
an open transaction or failed rollback is quarantined/closed, never reused.
Panic on a secondary cannot poison Engine. Private Saved and pending state stay
recoverable through failure, receipt replay and final continuation/adoption.

Six new executor parents exercise a real publication transaction stalled for
1000ms while Engine readonly metadata, another private write and another enqueue
remain live; an external SQLite writer with a 200ms job deadline followed by
successful retry; publication panic without Engine poison; SQLite authorizer
denial of rollback on an actual seeded worker-pool connection, quarantine and
fresh retry; old committed-branch receipt refusal after checkout; and refusal
of a caller primary transaction with Saved retained until retry after rollback.
Existing checkpoint-group64, bounded queue/shutdown, lost replies, newer writes,
exact journal ownership and real process-exit recovery tests remain relevant.

Final qualification passed **301 reported cases on Windows** and **308 on
Linux/WSL**, in eight targets per platform: library 113/120, content_executor
20, content_retention 9, content_saves 50, content_write 42, contracts 41,
overlay_map 17 and pending_saves 9. Six default ignores remain on each platform.
Strict Clippy for library/all test targets, formatting and diff checks passed
with Rust/Clippy 1.96.0. These are scoped save regressions; mounted read/transport
and kernel opt-ins were not rerun in this stage. Platform repetitions, crash
children, preliminary runs and direct verification do not add distinct cases.

`test-evidence/wave-6/save-metadata-checks.json` records commands, outputs,
source/executable identities and limitations. All 377 Cargo/build/src/tests/
native/ui inputs were frozen before final qualification and rechecked afterward;
four inputs changed from the mounted-demand stage, none added/removed. Source
SHA-256:
`9213252c24184e6d9b67f15f4a0d62fdb37637de7aafaf389800fb2b3ddc321d`.
Every archived source ZIP member was read back against its manifest. After all
compilation, each platform's eight actual executable images were hashed and
directly run sequentially without compilation, with matching hashes before/
after each run and at completion. Reported totals use that verified execution
once per case.

Initial compile diagnostics, the pre-status-invalidation snapshot and incomplete
inline WSL exit capture remain diagnostic records. The final saved Bash runner
captures lint/test exit codes explicitly through a login shell; an initial
non-login invocation lacked Cargo on PATH. No source assertion or release gate
was weakened for those harness corrections, and repetitions add no successes.

This is a concurrency prerequisite, not mounted COW write activation. The
mounted/legacy-session guard remains enforced. Other legacy primary metadata
mutations may themselves still hold Engine while waiting for SQLite; short
observation metadata reads also stay under Engine. The proof covers independent
readonly metadata and other private writes, not concurrent SQLite writers or
mounted notification capture (which is None in this headless path).

Native COW write/read/truncate/close/fsync callbacks, durable namespace and
startup/retry/open-handle/lifecycle/status integration remain incomplete.
Automatic close grouping, compatible metadata batching, private-reader
independence during FULL fsync, complete root/retention/deletion/GC/storage
progress, aggregate runtime budgets/shutdown/soak, WP6.5/6.6 and all original
Wave 6 release gates remain required. No schema/canonical/wire/event change,
private user_version 4, no live activation/migration or gate waived.

### WP6.4 native existing-file COW adapter integration (CCR-WP6.4-16)

An explicitly installed native workspace now attaches existing descriptor files
to the private `WriteJournal`/`SaveExecutor` pipeline. Installation requires the
existing isolated fixture flag before mounting/opening; no production path
automatically activates repositories. Existing-file readers and writers share
one stable `SaveFile`. Metadata capture compares branch, checkout generation,
descriptor and content/alive parents inside the real SQLite writer transaction
before superseding ownership, then final installation revalidates the handle.
Pinning, private creation and all byte work remain outside Engine. Same-entity
preparations join one key; at most 64 distinct preparations are accepted.

Actual WinFsp/FUSE callbacks route native reads, writes, atomic append,
constrained writes, truncate and allocation semantics to the private file.
The per-file gate orders mutable reads, writes and flushes outside Engine,
including large borrowed requests processed in bounded pieces. Cached length
tracks private commit even after a lost buffered-write reply. Explicit fsync
and native close/cleanup, including burst close, wait for FULL Saved, local
metadata publication and continuation/adoption; no peer wait. Native failures
retain dirty/Saved data and use existing mount reporting, including WinFsp void
cleanup. FUSE release now runs on the bounded request worker pool, and native
O_TRUNC/size setattr performs preparation and byte work off Engine.

Private buffered/checkpointed epochs prevent an under-Engine retain from
claiming newer native data was checkpointed. Force-detach persist/release/lost
and forced worker stop checkpoint native files outside Engine first. On explicit
installation/reopen, bounded Saved enumeration restores up to 64 active-branch
existing-file operations as visible dirty/nonquiet sessions before handles open.
Uncommitted ordinary Working is still not a promise. Committed earlier receipts
replay without another metadata event. Inactive, deleted or competing operations
remain recovery-required with their private data retained.

Eleven ordinary native parents cover explicit activation refusal; joined two
writers/reader and one publication; full overwrite without an absent base chunk;
shrink/grow holes and cached length after a lost write reply; 128 atomic appends;
a started 350ms write ordering before flush while Engine remains available;
same-byte newer-event rejection before owner creation; a 100ms metadata writer
deadline with Saved visible after reopen and retry; the actual force-detach
retain phase stalled 350ms before journal commit while Engine stays acquirable;
real exits86 before preparation and after native publication with exactly one
replayed event; and a 1000ms native publication stall with another native file's
private write/read plus readonly metadata completing within 300ms.

The first final-source Linux direct-image regression failed the existing
same-round legacy native-write test. Its retained trace identified a local
`WORKER_BUSY: object effect commit in progress`, followed by round retirement
after 3.5 MiB and a second round for the remaining 512 KiB. The source was the
write admission try-lock overlapping the same-family `objects.want` effect
snapshot. Experimental progress-observer/counter changes did not fix it and
were removed; their diagnostic sources and failures remain archived.
Write admission now waits outside Engine/lease state/object gates within the
existing shared two-second order deadline, checks cancellation/stop and stale
run ownership, then revalidates through `claim_run` under the acquired gate.
Twenty isolated repetitions of the originally failing case passed after this
fix (diagnostic repetitions, not additional cases). A new controlled actual
want/write test forces a 350ms family-gate hold and requires the complete 4 MiB,
one sampled live lease nonce and no source backoff. A new unit checks
deadline/stop refusal without claiming or changing work. The final Windows
direct-image sample records two overall download-job invocations and one live
nonce. The report initially rejected a stricter count-one check; inspection
confirmed that counter includes empty anti-entropy invocations. The sample does
not attribute its second invocation or prove zero redundant rounds/bytes. The
unfiltered count and limitation are retained, with no Rust assertion/source
change or rerun to erase the sample. No release gate or assertion was relaxed.

Final evidence: [native-write-checks.json](../../../test-evidence/wave-6/native-write-checks.json).
All 379 Cargo/build/src/tests/native/ui inputs were frozen as
`69686ba60c111130de930885358585f18fd640141811af6e004acb2f4e833ed1`.
Strict release Clippy with `-D warnings`, formatting and diff checks passed.
Twelve selected targets passed **388 Windows / 395 Linux** main cases, with
11 default ignores on each platform; these are separate platform counts.
Actual executable hashes were checked before/after direct execution of all
twelve images and again after the additional lifecycle build. Four focused
legacy stop/detach cases passed on each platform, without changing those twelve
images; these are not complete lifecycle suites or extra native-kernel tests.
Each platform also passed one explicit isolated real-kernel parent (WinFsp or
FUSE): write 7 bytes, truncate to 70,001, append 4, fsync, verify 70,005 bytes,
close/reopen equal and verify the metadata oracle. Child successes and repeated
Cargo/direct runs are not additional distinct cases. With native publication
held for 1,000ms, the other native file's private I/O plus readonly metadata
took 0.879ms Windows / 0.753ms Linux in the final samples (300ms bound).
Every member of the exact source archive was read back and hashed. Prior failed
and rejected diagnostic sources/logs remain separate; no user mount, migration
or production activation occurred.

Durable native creates/names/delete/rename/atomic replacement and retained
open-handle identity remain incomplete. Unsupported open-handle namespace paths
are refused, and void-cleanup delete semantics are not qualified. Native COW
base reads/preparation do not yet register authenticated remote demand; missing
bytes retain an unavailable result and Saved data. Generic provider mounted
COW, long-lived writer metadata/time rebasing, autonomous startup/namespace
recovery, complete production lifecycle/status and panic/accept-error buffered
exit handling remain open. Automatic close coalescing/metadata batching, private
readers during journal FULL fsync, complete native retention/GC/storage progress,
aggregate runtime budgets/shutdown/soak, WP6.5/6.6 and every original release
gate remain required. No persistent schema/canonical/wire/event change, private
user_version 4, live activation/migration or gate waived.

### WP6.4 authenticated missing-base hydration (CCR-WP6.4-17)

Native COW reads and canonical preparation can now register authenticated
remote demand for an unavailable immutable base. A process-local optional
session hook uses the actual SaveIntent event/entity/workspace/descriptor,
native provider identity, current grant generation and checkout/file ownership
fences. Secondary SQL and byte verification run outside Engine. Adoption keeps
the hook with the new intent; explicit Saved recovery attaches it to the new
worker. The generic provider API and durable records are unchanged.

Missing builder pages carry a local one-byte path position. For structural
absence, rebuild releases its buffers/journal guards before waiting, then revalidates Saved and
retries. Structural demands use their reserved class and exclude data chunks;
partial base reads separately demand the required bytes. Full overwrites do
not hydrate old data, and unchanged chunks can remain absent. Native mutable
reads reserve adapter slots before the file gate; missing-base waiters share
the existing global 64 limit. Every wait checks deadlines, cancellation,
worker/fence state and reference authorization, including same-byte regrant.

Five new transport parents cover a needed range with another private chunk
already changed, partial flush with an unrelated source chunk absent, full
overwrite with old bytes absent at both peers, Saved reopen/retry with one
publication, and deadline/withdraw/regrant/stop/cancel preserving Saved/private
bytes while releasing demand. A callback test proves capacity is reserved
before waiting on a started write; an object-engine test proves structural
requests use their own class without fetching the data chunk, then foreground
data work proceeds after release. An explicit isolated real-kernel parent
writes seven bytes, hydrates only the needed base for fsync, verifies/reopens
the first 64 KiB and preserves an unavailable unrelated chunk.

The first full-overwrite diagnostic failed because standard error-chain
traversal did not recover an anyhow context payload. Extraction now uses
anyhow's context-aware downcast; the initial four parents then passed. A test
accessor visibility error was fixed using the existing public Cow variant,
without broadening the production API. Early builds/runs were diagnostic;
final qualification uses a frozen source set and exact executable hashes.

The first complete attempt froze 380 inputs as
`cffe482c0337b4f749c0a79a328f9e07caedcea414c34dd6fc30192ed808909a`.
Strict Clippy passed on both platforms. Windows passed all 13 selected targets,
three explicit isolated real-kernel parents and direct verification of its
13 executable hashes. Linux's transport suite failed the existing
`native_capacity_return_between_refusal_and_pending_snapshot_keeps_round_authority`:
two live nonces, six successful pulls, 1,048,583 bytes written. All five new COW
parents passed in that same run. This failure prevents complete stage
qualification; no assertion was relaxed or failing sample discarded. The exact
380 inputs and raw attempt outputs were archived/read back in
`target/wave6/cow-base-attempt-cffe-before-credit-trace.zip`
(SHA256 `ae046f36b6b47838e5de797a100d8babcbdffe89f890090d16162903e1dbac26`).
At that point, error propagation, local capacity retirement and same-family
readiness were still under diagnosis. Windows's frozen-source process completed before any
diagnostic source change.

Thirty isolated diagnostic repetitions of the capacity-return case passed,
but the instrumented full transport sequence failed the existing pending-write
case. Its round exited with `local_error=WORKER_BUSY: object effect commit in
progress` and reason `failed`, after 3,932,160 bytes, followed by a 262,144-byte
round. This identifies want readiness independently of the capacity hypothesis;
no additional StreamLease/credit-observer change was made. The exact diagnostic
source/log snapshot is `target/wave6/cow-base-credit-diagnostic-snapshot.zip`
(SHA256 `d31c28c8ee953483ab3f8653120fcc6c959e83a1aaa61a8116515318d10231ad`).
All 380 original inputs were restored exactly before implementing the fix.
Both want phases now share the cooperative family-gate helper and one existing
two-second deadline, checking caller cancellation while waiting and revalidating
ownership under each acquired gate. The existing post-grant cancellation
rollback remains. A new unit drives both actual want phases through wait,
success and cancellation without new grants. A diagnostic grant/shutdown case
then exposed the worker-stop fallback bypassing the caller's cancellation
reason/observation; the helper now invokes that caller check before its generic
worker stop result. The existing 24-scenario offer-retirement parent passed
afterward, without assertion or timing changes.

Final mechanism evidence: [cow-base-checks.json](../../../test-evidence/wave-6/cow-base-checks.json).
All 380 inputs were frozen as
`6a3f096f991e2a9898f1e1a7256068913041a73e75b20a6ea993e8bd85843140`.
Strict release Clippy (`-D warnings`), formatting and diff checks passed on
the recorded Rust 1.96.0 toolchains. Thirteen selected targets reported
**413 Windows / 420 Linux** main passes, with 12 default ignores/platform.
The preexisting nonignored `receipt_crash_worker` driver accounts for one
ordinary early-return pass/platform, not independent recovery evidence; its
actual crash parent is reported separately in the same suite. Three explicit
isolated real-kernel parents passed per platform: authenticated partial COW
preparation/fsync/reopen, authenticated readonly range hydration, and existing
native write/truncate/fsync/reopen. Child successes and repeated Cargo/direct
runs are not additional cases. All 13 executable hashes matched before/after
direct execution and the kernel runs, and all source-archive members were
read back and hashed. Failed attempts/diagnostics remain retained.
This qualifies the interim authenticated mechanism and regression fix, not
the unfinished no-peer fsync/builder-independence contracts described below.

Generic-provider canonical rebuilding, legacy conversion hydration and fetching
missing reused chunks of a new root before peers learn its new authorized
reference remain unqualified. Native namespace/startup/lifecycle completion,
batching, complete retention/GC/storage progress, aggregate budgets/shutdown/
soak, WP6.5/6.6 and all original release gates remain open. No activation,
migration, live CAS deletion or schema/canonical/wire/event change occurred.

Contract audit found a required next step: this interim integration can fetch
base bytes/pages during fsync, while the authoritative plan requires fsync
without a peer. Necessary hydration must move before mutation acknowledgement,
and the fsync coordinator must remain local-only. Also, structural absence
releases builder buffers before waiting, but partial-data reads from inside
rebuild still hold those buffers; that guarantee is not complete. These are
implementation gaps, not a reinterpretation of the plan as merely avoiding
replication acknowledgement. Authentication/preparation evidence does not
close either contract. This preflight/local-fsync change takes priority over
further native namespace work.

The next implementation should preflight only the mutation's required base
bytes/manifest paths against a stable Working selection, without hashing or
installing a canonical result and without forcing a FULL checkpoint for each
ordinary write. A preflight visitor must return/release its buffers before
remote recovery, then revalidate owner/sequence. Native write/resize should
finish that preflight before acknowledgement. Canonical flush/fsync must use
only verified local inputs and retain/report unavailable data immediately;
disconnecting the peer after a successful mutation must not prevent fsync.
Full overwrites must continue avoiding old chunk bytes. Rework the interim
tests around that required boundary rather than preserving network-in-fsync
as a compatibility promise. The original complete Wave 6 scope stays intact.

### CCR-WP6.4-18 — pre-mutation base preflight and strictly local fsync (2026-10-21)

The native coordinator now preflights prospective write/resize against an exact
Working selection before changing private bytes. A read-only canonical-shape
visitor verifies required existing pages/old byte gaps, skips fully covered old
chunks and new zero ranges, and constructs no new content hashes, CAS objects
or FULL checkpoints. Input verification still hashes existing objects. Missing
native inputs return from the visitor before authenticated BaseWait: all three
background buffer permits, cursor/scratch and journal guards are released;
file state is also unlocked. A concurrent private mutation invalidates the
selection before incoming bytes can apply. Local preflight and bounded write
pieces check request cancellation and the mount's atomic fence.

Recovered/headlessly modified sessions include existing dirty paths in their
next native preflight. Partial failures retain that process-local marker; full
native success clears it. Canonical prepare and checkpoint reads now use only
local content and cannot invoke BaseWait. Fsync first retains FULL Saved, then
prepares/publishes locally. Missing required local base after an acknowledged
write returns unavailability immediately, retains Saved/private bytes and can
retry after local restoration, with zero demand-hook calls. Legacy conversion
stays local; a fully private replacement can resize/prepare without the absent
original object. No durable/schema/wire/event format changed.

Reworked authenticated cases disconnect the peer after write/resize and before
fsync, including the isolated real WinFsp/FUSE write/sync/reopen parent. Three
new transport parents cover missing-local-base fsync, stale preflight preserving
concurrent bytes, and shrink/grow zeroes. A new 1 GiB deduplicated-tree/4 KiB
edit parent bounds preflight to 128 KiB existing data reads and at most eight
manifest page reads, with no CAS install or FULL checkpoint; fixture seeding
is excluded. A new legacy unit also checks local preflight cancellation.

The first missing-local-base diagnostic used an unsupported peer `hello` probe
after its fsync assertions; changed to the existing authenticated capabilities
probe. The new legacy unit initially failed compilation because `checkpoint`
returns an option; corrected its unwrapping. Draft Linux then passed all eight
ordinary COW parents and the legacy unit. These diagnostics are not additional
unique passes or full qualification. No assertion, timing gate or scope relaxed.

Final evidence: [local-fsync-checks.json](../../../test-evidence/wave-6/local-fsync-checks.json).
Qualification used 380 frozen source inputs:
`24b26c2ccd572c15b34fbb7f543a927912aecb4880b4970c21d4e464ba6cbfb9`.
Strict release Clippy (`-D warnings`), formatting and diff checks passed.
Thirteen selected targets reported **418 Windows / 425 Linux** main passes,
with 12 default ignores/platform. The nonignored `receipt_crash_worker` driver
accounts for one early-return pass/platform, not independent recovery proof;
its actual crash parent is included in the same suite. Three explicit isolated
kernel parents passed per platform: new offline COW write/fsync/reopen,
authenticated readonly range, and native write/truncate/fsync/reopen. Child
successes and repeated Cargo/direct executions are not additional cases.

All 13 actual executable images per platform matched their recorded hashes
before/after direct execution and the kernel runs. All 380 exact source-archive
members were read back and hashed. Archive:
`target/wave6/local-fsync-source-final.zip`, SHA256
`3b51fb821297d558e50b79f1547cdae3c3239eef1800c47b0347c6829b8a5f74`.
Final direct Windows/Linux samples respectively: missing-local-base fsync
failed in **2.9952 / 6.178277 ms**, zero demand-hook calls, Saved/private bytes
retained and local-restoration retry successful (300 ms bound). Independent
canonical import while preflight waited completed in **1.6669 / 4.184768 ms**,
with file state/journal available and preflight buffer permits released
(300 ms bound). The 1 GiB/4 KiB edit read **131,072 bytes and six pages** on
both platforms, installed zero CAS objects and forced no FULL checkpoint;
preflight took **4.896 / 4.703179 ms**, excluding fixture seeding. No timing
target was relaxed. These mechanism results do not replace full build/load,
RSS, storage-amplification or release qualification.

This corrects CCR17's network-in-fsync and partial-data buffer-wait gaps; the
earlier evidence is preserved as history. Structural demand after dispatcher
idle/saturation still has unqualified admission latency (existing backoff can
reach 640 ms against the original 250 ms gate). Generic-provider rebuilding,
remote legacy conversion and serving missing reused chunks before peers learn
a newly published root's authorized reference remain open. Native namespace,
startup/lifecycle, batching, complete retention/GC/storage progress, aggregate
budgets/shutdown/soak, WP6.5/6.6 and every original release gate remain required.
No production activation, user mount, migration or live CAS deletion occurred.

### CCR-WP6.4-19 — structural demand after idle behind stalled background (2026-10-21)

The CCR18 admission gap was reproduced without changing the timing policy.
An authenticated transport parent waits for seven actual empty structural
preparations of its own repository, removes its verified local root manifest,
and starts a COW write while two other actual background source reads are
stalled for 4,000 ms. Per-group diagnostic snapshots expose capped empty-poll
counts and remaining scheduled delays; neither a sleep nor another group's
counter stands in for the tested idle phase. The new snapshot is bounded by
the existing group page and performs no SQL or provider I/O.

Baseline: **639 ms** remained after seven empty preparations; the new demand
was not admitted by **250.36645 ms**, missing the original **250 ms** gate.
The write eventually completed at **693.925347 ms**; page repair, private bytes
and subsequent local fsync succeeded. Both stalled background completion
counts remained zero. No assertion was relaxed. Exact 380 source inputs
(`e7a8c416a7b79a344bc86b2e601e621a18ff32118ebeb60dd0608665ae009b34`)
and raw failure outputs were archived/read back in
`target/wave6/structural-idle-before-fix.zip`, SHA256
`9680c96ec556a0346af0d198b6da1b83e3039237c55cea2aa606bd70a73e7720`.
That archive's index used Windows pathlib ordering for its aggregate source
hash. Readback of the unchanged exact members with the final qualification's
UTF-8 string ordering yields canonical source identity
`9fc85d9db557f49b08369a07932ce9d006b00a08629a768debb9e689870c8fd0`;
both identities and algorithms are retained in
`target/wave6/structural-idle-baseline-identity.json`. The original archive is
unchanged.

Empty structural preparations now poll again after **50 ms**, independently
of foreground's existing 10 ms and background's unchanged exponential delay
up to 640 ms. Existing workers, class/byte/queue admission, authorization,
source selection and cancellation remain. This adds no notification dependency
and does not claim a global latency guarantee for arbitrary repository counts.
The focused corrected Linux case passed at **50.714661 ms** admission and
**85.921549 ms** write completion, with background completion counts still zero.
This preliminary case is not an additional unique pass or full qualification.

Final evidence: [structural-idle-checks.json](../../../test-evidence/wave-6/structural-idle-checks.json).
Qualification used 380 frozen inputs:
`cb68b61144bdc9edb3cde9d7183d2604b4b00ba126943b08f08adba06897f435`.
Strict release Clippy (`-D warnings`), formatting and diff checks passed.
Thirteen selected targets reported **419 Windows / 426 Linux** main passes,
with 12 default ignores/platform. One `receipt_crash_worker` early-return
driver/platform is included in those harness counts, not independent recovery
proof; its actual crash parent is in the same suite. Three explicit isolated
real-kernel parents passed per platform. Repeated Cargo/direct runs and child
successes are not additional cases. All 13 actual executable hashes/platform
matched before/after direct execution and kernels, and every exact member of
the 380-input source archive was read back/hashed.

Final direct Windows/Linux admission was **94.2927 / 51.516882 ms**, below
the unchanged **250 ms** gate, after seven actual empty preparations. Remaining
scheduled delay snapshots were 48/49 ms. Both background completion counters
remained zero while their actual source reads were stalled for 4,000 ms.
Write completion was **171.2483 / 100.979839 ms**; repaired page, private bytes
and local fsync passed. Archive `target/wave6/structural-idle-source-final.zip`,
SHA256 `7aca750c7d9be9c87d7375f74f5b4b602d580de81ad44985b82d8f51e6c31bb9`.
This closes the explicit new-COW structural repair after idle/two-stalled-peer
scenario, not aggregate fairness/CPU/RPC cost, all repository/source counts,
full outage/load/30-minute soak or any other original Wave 6 gate. Production
activation stays off.

Next implementation priority is native namespace/lifetime completion. Audit:
`Engine::open_with_attributes(create=...)` still calls the legacy creator;
new files do not acquire native descriptors through `NativeWrites::install`.
Also, ordinary local `basic`/`location` changes deliberately fence publication
ownership in `core/content_saves.rs`. A long-lived native writer needs an
explicit validated metadata/time continuation rebase, not removal of those
fences or silent parenting of competing remote changes. Preserve the existing
stale-publication tests, stable open-file identity and crash-safe receipt/journal
handoff while adding create/rename/delete/replacement semantics. Namespace
metadata and canonical work must stay outside Engine waits/fsync. Immediate
durable namespace commits and eventual compatible batching need separate
correctness/performance evidence; successful content fsync alone does not
qualify namespace recovery or atomic replacement.

### CCR-WP6.4-20 — owned basic-info metadata and capture rebase (2026-10-21)

Core now offers `apply_content_save_basic` for an exact registered SaveIntent.
Under one FULL WAL writer, a private proof exempts only its exact local
basic-info event from ordinary ownership invalidation. Workspace, branch/view,
generation/owner and unselected/uncommitted capture are checked again inside
the event savepoint. Ordinary metadata continues fencing stale saves. Content
and alive parents, descriptor and private operation remain exact; no provider,
canonical or private-journal byte work occurs in this Core operation.

The basic event, bounded/checksummed request/event receipt and updated capture
plus successor lookup commit atomically. A mutation UUID supports exact
lost-reply replay after later publications; changing the request with that UUID
is rejected. The metadata event cannot reuse the future byte event's identity.
An optional capture policy binds explicit write/change-time retention to one
private sequence. Older selection is rejected, later bytes expire retention,
exact repeated selection preserves clocks, and superseding selection changes
template/selection together before publication. Published events never change.

Ten new ordinary parents cover bytes/parents stability, timestamp sequence
ordering, superseding selection, selected-capture refusal, ordinary metadata
fencing, successor/lost-reply replay, event-ID reuse, a preserved remote content
competitor and actual process exits before/during/after metadata commit. Eight
initial focused Linux parents passed. Integrity audit then added two stronger
parents; the first attempt reported one pass/two failures: ignored receipt and
valid-but-reverted selected template incorrectly acknowledged success. Exact
381 inputs/source
`1c02b5e0320a3e8f6101b160bac4bb8fc2d650b8031baaf9ceccfd24d7643cf5`
and raw failure outputs were archived/read back in
`target/wave6/owned-basic-before-integrity-fix.zip`, SHA256
`f82da0707f5d52619fb1b66e14809748baea60bfd92530499b4c991ad8f5efd4`.
Exact receipt/current-capture/successor/selected-template readback now rejects
ignored/deleted/reverted writes and rolls back the whole mutation. All ten
focused Linux parents then passed, including all four silent-write scenarios
and the three actual process exits. No assertion or gate was relaxed.

The initial `f6bcdeaa…` freeze was superseded before any final qualifier launched.
First frozen qualification used 381 source inputs:
`96db7eff4b96db65c7c84b8fecae04f858350b2a47711acbeb6855b919bf6198`.
Both strict Clippy runs passed, but this attempt failed Linux's
`classify_covers_every_code_literal_in_src`: 40 contract cases passed and seven
new codes lacked classification. The 60-case save suite and preceding targets
passed; later targets/kernel/direct phases did not finish. The Windows build
was deliberately stopped after verifying its process identity/creation time,
because it shared that deterministic gate failure. It was not restarted due
to an observation timeout. Exact 381 inputs and raw outputs are retained/read
back in `target/wave6/owned-basic-classification-attempt-96db.zip`, SHA256
`e247842cdb0929038aeaadcdef9a2e59cce6b8d226d8008972ef256e34faf57d`.

The seven codes now have explicit Wave 6 classifications and assertions in the
existing scope contract. Save-in-progress is contention without failure-count
escalation; receipt/continuation/owner/UUID integrity violations and invalid
internal timestamp policy are fatal only in the pending mutation/save scope.
Recovery bytes stay retained; historical classifications are unchanged.
The corrected Linux runs passed all 41 contracts and 60 saves before refreezing.
Final evidence: [owned-basic-checks.json](../../../test-evidence/wave-6/owned-basic-checks.json).
Qualification used 381 frozen inputs:
`6f40fb3668ccb903ca7b96c24f762089fe629daaa614ed562e463a113a9b2c89`.
Strict release Clippy (`-D warnings`), formatting and diff checks passed.
Thirteen selected targets reported **429 Windows / 436 Linux** main passes,
with 12 default ignores/platform. One nonignored `receipt_crash_worker`
early-return driver/platform is included in those harness counts, not
independent recovery proof; its actual crash parent is in the same suite.
The ten new ordinary Core parents include one parent driving three actual
process exits. Child successes and repeated Cargo/direct invocations are not
extra unique cases. Three existing isolated real-kernel parents passed per
platform; these are regressions, not native basic-info integration proof.

All 13 actual executable hashes/platform matched before/after direct execution
and kernel runs. All 381 exact source-archive members were read back/hashed.
Archive `target/wave6/owned-basic-source-final.zip`, SHA256
`2bf3c6d8f8baee919b6d9ce0df3d98a70517d4660dfc9063116869525bf1c464`.
The metadata-only scenario preserves content/parents, private operation and
Saved bytes before later publication; all ten new Core cases passed on both
platforms. Failed integrity/classification attempts remain retained. Native
basic-info callbacks are not wired by this stage, and the original full Wave 6
scope remains active.

This adds `content_save_metadata_changes` and an optional private metadata
capture field. New code reads old captures; older binaries using strict unknown
field rejection cannot read captures containing the new policy. WP6.6 still
requires its actual compatibility fence/migration/downgrade rehearsal.
Private write-journal version 4 and canonical/wire/event encodings are unchanged.
Receipt lifecycle/retirement and storage amplification remain part of the full
retention audit; a metadata receipt is not deletion authority.

Native integration must preserve metadata-only operation when base objects are
unavailable. The strict unselected-capture primitive above is not sufficient
for a failed, already-selected native save. A checkpoint-bound owned metadata
operation must supersede/rebase that selection without editing a published
event or forcing canonical publication. Before acknowledging metadata tied to
private sequence N, retain that sequence durably so recovery cannot strand a
policy beyond its newest recoverable Saved. A private FULL checkpoint can do
this without publishing unrelated bytes. Keep it outside Engine.

Windows timestamp-disable/reset flags must follow the handle that actually
wrote each private version. Persist their relevant write/change-time mask with
Working/Saved at the existing NORMAL/FULL boundaries, not a new FULL per
ordinary write; restore it for Saved recovery. Explicit timestamp retention
expires on a later byte mutation, while per-handle disabled updates apply to
that handle's later writes. Failed preflight without changed bytes must not
clear retention. These native callback/lifetime rules, create/rename/delete/
replacement, batching, complete retention/GC, aggregate budgets, WP6.5/6.6 and
all original full release gates remain required. Activation remains off.

### CCR-WP6.4-21 — native mutation timestamps and private journal v5 (2026-10-21)

Native write/resize now records the actual mutating handle's write/change-time
disable mask and mutation milliseconds in Working's existing NORMAL transaction
and Saved's existing FULL checkpoint. An absent policy remains omitted for
headless/old records; an automatic native write is explicitly distinct from
that absence. Saved recovery restores its own mask/clock and discards a newer
unacknowledged overwrite/policy. Ordinary writes gain no new FULL boundary.

Atomic session/file mirrors carry committed private sequence/time plus length.
Completion updates cached basic info only if a private sequence changed,
including partial writes with lost replies. Failed preflight cannot reset a
later explicit timestamp. No journal/provider work occurs under Engine during
that observation. Core selection uses the recorded mutation time, not fsync
time, and combines the last writer's mask with sequence-bound explicit time
retention. Native captures/successors seed base times without advancing disabled
timestamps prematurely. Older recovered captures can add a basic change without
parenting a new remote basic competitor.

Private journal **user_version 5** fences old strict readers before storing
new fields. Valid v4 inventory is checked, then only the version header changes
under a FULL WAL transaction; payloads, records, seals and checkpoint identities
are not rewritten. Unsupported v1/v2/v3/future/foreign formats still fail closed.
Three actual process exits before/during/after upgrade preserve Saved, expected
header4/header5 atomicity and successful reopen/retry. The v4 fixture uses the
same row schema and absent-policy bytes as actual v4 records.

Before new release builds, both attested CCR20 v4 `content_write` images were
copied and hashed into `target/wave6/compat-ccr20-v4`. Their source identity is
`6f40fb3668ccb903ca7b96c24f762089fe629daaa614ed562e463a113a9b2c89`;
Windows image SHA256
`ad9a5d9a97809e76bf14eaf052b100992e4d0c868810f924d6523f5a1f2ab3b3`,
Linux image SHA256
`81829cc8bf597e114bcefcd38b5367ef065e719854e17ec5ed45210536bf477c`.
The explicit compatibility parent invokes that real old child driver against
v5 data, requires exit101/`WRITE_JOURNAL_NEEDS_UPGRADE`, and checks image,
records/seals and header remain unchanged. This is private-reader refusal,
not proof that the older legacy executable cannot open an activated workspace;
the full WP6.6 launcher/store fence remains required.

Three new ordinary private-journal parents cover buffered policy/Saved recovery,
exact v4 upgrade and upgrade process exits. One new native writer parent
alternates three writes through two handles, checking pre-flush metadata
visibility and exact basic-info equality after delayed fsync. It sets handle
masks directly and explicitly does not exercise the native metadata setter.
The focused Linux cases and actual v4-reader compatibility parent passed.
Draft runs/repetitions are not additional unique cases or final qualification.

Final evidence: [native-times-checks.json](../../../test-evidence/wave-6/native-times-checks.json).
Qualification used 381 frozen inputs:
`b57d63f17b93c014a742832553d820455943eef5192a7004da55ab4772d9c84a`.
Strict release Clippy (`-D warnings`), formatting and diff checks passed.
Thirteen selected targets reported **433 Windows / 440 Linux** main passes,
with 13 default ignores/platform. The ordinary early-return
`receipt_crash_worker` driver/platform remains in those harness counts, not
independent recovery proof; its actual crash parent is in the same suite.
The new upgrade parent drives three actual process exits; repeated/child runs
are not additional unique cases. Three existing real-kernel regression parents
and one explicit actual-v4-reader compatibility parent passed per platform.
The intentionally rejected old child (exit101) is not an old-test pass.

All 13 actual executable hashes/platform matched before/after direct execution,
kernel and compatibility runs; old reference-image hashes also stayed exact.
All 381 source-archive members were read back/hashed. Archive
`target/wave6/native-times-source-final.zip`, SHA256
`e590a8e0399bb8c7ab76456dfcfa2ae5e5e877157813447c2cbc4315ba8f0f1d`.
The two-handle writer scenario confirms cached basic-info visibility and exact
timestamps across delayed fsync. It still explicitly does not call a native
set-basic callback or qualify all path/inode metadata surfaces.

The first Linux compatibility-driver call failed before invoking its parent:
Windows denied reading the UNC WSL mirror of the NTFS reference image. The
driver now hashes that file through its direct host path and executes the
same image through `/mnt/c`. Rust source and executable images did not change;
Cargo was not restarted. Compatibility/kernel/direct phases continued against
the same images. The structured diagnostic and final driver source are retained.

Native set-basic callbacks and checkpoint-bound metadata rebasing for failed
selected saves are still required. Partial-field freshness, disable/reset
requests and durable sequence anchoring must be integrated before claiming
complete metadata/time continuation. No production activation, user-store
migration, downgrade, live CAS deletion or full release gate is implied.

The new visibility check reads cached handle/session basic info, not every
path/inode metadata surface. Source audit finds FUSE `Filesystem::entry/attr`
using committed Store basic info and a legacy-content size fallback without
an open session. Add actual kernel tests for pre-open descriptor length and
dirty timestamps before fsync, then fix the renderer/cache path. Metadata
activation also refreshes session basic info; preserve private-clock ordering
against that refresh and explicit timestamp requests. These remain native
integration/WP6.5 work, not qualified by a cached-session assertion.

### CCR-WP6.4-22 — native attributes and read-only close isolation (2026-10-21)

The existing actual-kernel native write parent now checks typed descriptor
length before open, the private mutation timestamp through both path and handle
stat, and a read-only open/stat/close without publishing another writer's dirty
buffer. Truncate, explicit fsync, read/reopen equality and the derived-state
oracle remain in the same parent. Strengthening it is not a new unique case.

Actual failures were retained with exact 381-input source archives and raw logs:

- `native-attrs-size-before-fix.zip`: FUSE returned size 0 instead of 196608 before
  open. Source `71f53dbf…`, archive SHA256
  `7ab856de97975b5d6dab566460ca1c8355413f8af3e59e65b9265208b36d43cc`.
- `native-attrs-clock-after-size.zip`: after the size-only fix, FUSE path mtime
  remained 73 ms behind the private mutation. Source `442a4794…`, archive SHA256
  `ce0d834d516639375e7010ee91daf88a51c3e01005f94de03f7290fe16078db9`.
- `native-attrs-readonly-close-before-fix.zip`: WinFsp's temporary read-only
  metadata handle published the other writer's buffer on cleanup/close. Source
  `ebe1f8f8…`, archive SHA256
  `16f0acb5702f5e5d4dfe6dab3f62a2d0a7a4be3602cd11aab1d282b1cfe473ac`.
- `native-attrs-recovery-close-before-fix.zip`: the first read-only skip broke
  the unchanged process-exit recovery parent (both platforms), leaving an
  already FULL Saved revision unpublished. Source `25fe91f0…`, archive SHA256
  `349e86c0cdd375ca3b291125adbcdce29e9a29f70b2808acfd487a7c04fc21b6`.

FUSE now uses the typed descriptor's declared length without CAS/provider work
when no session exists and native COW session basic info when one does. A
cleanup-specific save entry point skips uncheckpointed bytes for read-only
native handles; WinFsp cleanup, FUSE flush and native release use it. The
recoverability check and complete save run under the same per-file mutation
gate. Already FULL Saved recovery/failed saves can still finish, preserving
the original recovery assertion rather than weakening it.
Explicit fsync/FlushFileBuffers
and volume flush retain ordinary save semantics. Writable native close still
requires its FULL durability boundary; legacy behavior is unchanged.

One new ordinary regression arms the private checkpoint fault and proves that
read-only cleanup/close never reaches it; explicit fsync through a read-only
handle still saves the writer. A second new parent stalls an already-started
write under the mutation gate and checks that read-only cleanup waits, rechecks
the now-buffered state and leaves it unpublished until writable close.
A draft compile failed on a missing qualified
enum name before any tests ran, then was corrected. Focused runs and final
qualification are being recorded separately; compile/repetitions do not count
as extra test passes. The concurrent test also initially named a nonexistent
`Stall` action instead of the existing `Hang`; that compile failure preceded
test execution, was retained with source `0ba8a2a1…` in
`native-attrs-race-compile-before-fix.zip` (SHA256
`61c86cc47643885f551524f0c43635b5feb117cc78fe9588c1a75a31d26311ab`),
and was corrected before final qualification.

Final evidence: [native-attrs-checks.json](../../../test-evidence/wave-6/native-attrs-checks.json).
The 381 frozen inputs hash to
`781885a4b2880ad973ac226f50e4f59eea4f6ec0e3ff39c224b95ddc67e94c8b`.
Formatting, diff and strict release Clippy checks passed. Thirteen selected
targets reported **435 Windows / 442 Linux** main harness cases, with 13
default ignores/platform. These include one ordinary early-return receipt
driver/platform, not independent recovery evidence. Two new ordinary parents
and an existing strengthened kernel parent do not turn repeated invocations
or child processes into additional unique cases.

Three actual WinFsp/FUSE kernel parents and the actual-v4-reader compatibility
parent passed per platform. All 13 executable hashes/platform stayed exact
before/after direct execution, kernel and compatibility checks. Every exact
source member and all five development-failure archive inventories were read
back and checked. Final archive `target/wave6/native-attrs-source-final.zip`,
SHA256 `d4854048cce9126bf2c6e073f812c8137b739be1f13945e6417980d4c212f631`.
The final Windows qualification uses PowerShell 7/UTF-8; the report also decodes
the preserved UTF-16 PowerShell 5 draft logs without rewriting their bytes.

Before any subsequent build, both current private-v5 `content_write` images
were copied to `target/wave6/compat-ccr22-v5`. Their hashes match the completed
CCR22 attestation: Windows `e3b5115d774f6f18…`, Linux `d9bd470338cf5213…`.
Preserving them is preparation for future format compatibility checks, not an
additional executed compatibility claim.

The local path/handle sequence does not establish every directory/cache/
notification surface or private clock ordering across metadata activation.
Native set-basic callbacks, selected-save continuation, durable sequence
anchoring, partial-field freshness and per-handle disable/reset requests still
need integration. The full Wave 6 plan and release gates remain in progress.

Source audit also identifies an unqualified multi-mutation clock sequence:
an automatic write-time update followed by another handle's write with that
clock disabled, without an intervening fsync. Journal v5 records the last
mutation's mask and one mutation clock; the current two-handle test flushes
between its writes. Add the buffered sequence and Saved/restart variants before
claiming that every per-field clock survives mixed masks. This is an audit lead,
not yet a reproduced failure or a waived contract.

### CCR-WP6.4-23 — exact private clock pairs (2026-10-21)

The buffered mixed-mask regression reproduced the audit lead: the first
automatic write advanced the cached write-time, but a second handle's write
with that clock disabled caused fsync to publish a value 44 ms older. The
unchanged change-time and exact BasicInfo comparison isolate the lost field.
Failure source `c859ff79…`, 381 inputs, is preserved with the raw log in
`target/wave6/native-clock-batch-before-fix.zip`, SHA256
`edb4f7229f410659a64344f5e2d7f74e5fc24f5261e237c5b5d3acafe94cf2bc`.

The private v6 journal records the exact resulting write/change FILETIMEs,
the mutating handle's mask and the mutation milliseconds in the same existing
NORMAL write transaction and FULL Saved checkpoint. The runtime supplies prior
visible clocks while owning the per-file mutation gate. A disabled field keeps
that value, including a previous buffered write or explicit metadata value.
Core selects the recorded pair while retaining same-sequence explicit metadata
retention. Recovered native sessions seed their visible cache from Saved.

Clock mirrors now use short memory-only mutexes; no cache guard spans journal,
provider, fsync or Engine work. The separate atomic length/sequence mirrors
remain. No new FULL transaction is added to ordinary writes. The initial
expanded argument list tripped strict Clippy; a named time-policy argument
groups the handle mask and prior pair. Clippy passed after that change.

Focused Linux checks passed for the buffered sequence with masks 2/4/6 and
three actual process exits before publication, followed by exact recovered
cache/bytes and single-publication checks. Existing native-close recovery
assertions remain unchanged. These draft repetitions are not final source/
executable qualification or additional unique cases.

Headless v4/v5 records omit native policy and retain exact record/checkpoint
bytes during the FULL header upgrade to v6. Native v5 records lack enough
information to infer every earlier disabled field; they require recovery/
export with their matching v5 reader and are refused before header mutation.
This conservative development-format behavior is not the full WP6.6 production
migration/export workflow. Native set-basic integration and every remaining
package/release gate stay required.

Final evidence: [native-clock-checks.json](../../../test-evidence/wave-6/native-clock-checks.json).
The 381 frozen inputs hash to
`5be800d3157b70573577c7fb71ca2841f65de07fb3507123138b9a2354ad4a6e`.
Formatting, diff and strict release Clippy passed on both platforms. Cargo
built 13 selected targets with `--no-run`; one hash-attested direct execution
of each target reported **438 Windows / 445 Linux** main harness cases and
14 default ignores/platform. The ordinary early-return receipt driver is
included once/platform, not independent recovery proof. Three new ordinary
parents contain the mixed-mask subtrials, Saved clock exits and validation.
The two existing upgrade parents now exercise v4/v5 and six actual exits.
Repeated invocations and children are not additional unique cases.

Three actual WinFsp/FUSE kernel parents and two explicit compatibility parents
passed per platform. Actual v4/v5 images refused v6 with exit101 and unchanged
records/image hashes. The v5 parent also executed the old native writer with
exit86 after its actual v5 Saved commit; the v6 reader refused that fixture,
preserving its records, header and physical main DB/WAL hashes. Those intended
child exits are not old-test passes. All 13 current executable hashes/platform
stayed exact through direct/kernel/compatibility execution. Every source archive
member was read back/hashed. Final archive
`target/wave6/native-clock-source-final.zip`, SHA256
`c28ef25bee55e5cba97c4dde308694704ec8e8aefe5282bd4091089fb53ba095`.

The next native metadata integration must apply partial fields under the Core
writer, anchor buffered bytes with Saved, rebase failed selected saves and
adopt already committed saves without publishing newer unrelated bytes.
Sequence-bound explicit time/attribute overrides must survive recovery and
expire only after a later byte mutation. Mask-only handle changes need no
unrelated content publication. Native namespace identity/lifecycle, root
retirement, complete metadata/cache/change-journal behavior, build/load/storage
gates and production compatibility remain open under the complete plan.

### CCR-WP6.4-24 — checkpoint-bound partial native metadata (2026-10-21)

Core now overlays partial basic fields under its writer transaction, binds
them to the exact FULL private byte checkpoint, and rebases an unpublished
selected save. Final receipt/capture/selection readbacks reject silent SQL
reversion. Same-revision explicit clocks and attributes accumulate; a later
byte revision expires them. The strict unselected-only API remains supported.

The native coordinator checkpoints outside Engine under the mutation gate.
An adoption-only executor job finishes a previously committed revision without
checkpointing or publishing newer private bytes. Failed metadata requests use
an exact read-only receipt lookup; absence does not reapply a failed command.
Committed replies that expire before observation are reconciled before later
native writes/truncates/saves. A coherent cached observation separates byte
revision from owner fencing and the canonical published/adopted floor.

Focused Linux checks passed eight Core metadata parents, 25 native parents and
21 executor parents. Actual WinFsp/FUSE `file.set_times` and Linux chmod passed,
with private bytes still unpublished before explicit fsync. These focused
runs are diagnostics, not final frozen-source qualification. Recovery includes
actual process exit after committed metadata and before byte publication.
Read-only cleanup now avoids publishing FULL Saved bytes while the writer is
still active; orphan/restart recovery remains supported.

The expanded actual Windows disable/reset probe failed. WinFsp 2.1 converts
`-1` into zero before `SetBasicInfo`, so the callback cannot distinguish disable
from unchanged. See its [driver source](https://github.com/winfsp/winfsp/blob/v2.1/src/sys/fileinfo.c#L1223-L1230).
The real failing assertion is retained in the separate explicit ignored
`isolated_kernel_windows_time_policy_requires_an_observable_driver_contract`
test. Shared decoder tests are not proof of this OS contract; it remains an
unresolved required gate, without a waiver or heuristic interpretation of zero.
Failure archive `target/wave6/checkpoint-basic-win-flags-runtime-before-fix.zip`,
exact source `a7e40753b76fde6eeed4584ffd2ef10d182337a64b1d8535b27317e4a3178572`,
SHA256 `54f02a338e277930ac833eb1e2ab7aee4c03ef3d323e603338577eca45d0e959`.

Other preserved attempts cover own-committed receipt classification, the clean
checkpoint fixture, the active-writer read-only-close regression and the Win32
test closure compilation error. The new threaded FUSE route also initially
sent legacy metadata-only requests through the size handler. An actual Linux
regression reproduced chmod444 failing with `ACCESS_DENIED` while attempting a
writable open. The fix dispatches metadata-only legacy requests without opening
or truncating; the test now checks chmod644/444 both preserve content identity
and leave no handles/sessions. Exact failure source `2cdb77ff…` is preserved in
`target/wave6/checkpoint-basic-legacy-route-runtime-actual-before-fix.zip`,
SHA256 `6329b59cff41b7acaf4590a0832d02f91b1b37819b34585fd80860ceba27d6da`.
A prior invocation used the wrong exact module prefix and executed zero tests;
it is diagnostic output, not a passing regression check. The corrected invocation
reproduced the failure, and the fixed test passed. Strict Linux Clippy also
required simplifying the native eligibility predicate.

The full plan remains active. Native namespace,
complete remote/cache/lifecycle behavior, batching, aggregate resource budgets,
build/load/storage gates and production compatibility are still required.

Final focused evidence for CCR24:
[checkpoint-basic-checks.json](../../../test-evidence/wave-6/checkpoint-basic-checks.json).
The 385 frozen inputs hash to
`5416d2836ce42302d3fd2ed9e9f1ef4f49c05037f20ce0e16d759eaf3d39187d`.
Formatting, diff and strict release Clippy passed. Fifteen selected targets
reported **458 Windows / 466 Linux** main harness cases, with 15 Windows / 14
Linux default ignores. The early-return receipt driver is included once per
platform, not independent recovery proof. Repetitions and subprocesses are not
extra unique cases. Three explicit real kernel parents and two actual-reader
compatibility parents passed per platform; all 15 current executable hashes
stayed exact through direct/kernel/compatibility execution.

The additional Windows time-policy probe was explicitly executed against the
same current image and failed with exit101. Its exact current source, raw log
and index are preserved in
`target/wave6/checkpoint-basic-windows-time-policy-current-failure.zip`, SHA256
`c471e404f57a5797947a8a87f6f0ade1f6637daffafcbf77f9bb128fc891eadd`.
The report records the loaded WinFsp 2.1.25156.ddca7bd driver path and SHA256,
which matches the root-bin driver. Source references explain the normalization;
they are not a claim that the driver was rebuilt from hash-matched source.
The failing probe is neither an approved test nor a waived release requirement.
Every exact source archive member and all eight failure archive inventories
were read back and hashed. Full Wave 6 implementation/qualification remains open.
Source archive `target/wave6/checkpoint-basic-source-final.zip`, SHA256
`80bccebfc436b8bfe1e963c3d5db814fa6b3e9ad2ca8a76706ea148364cbd438`.

### CCR-WP6.4-25 — buffered native FUSE truncate (2026-10-21)

The actual Linux kernel regression armed a failing
`content.write.before_checkpoint` hook around `File.set_len`. The existing
native SETATTR path hit that hook and returned EIO, proving it attempted FULL
checkpointing during ordinary truncate. Exact 385-input source `bfe7386e…`
and the raw failure are preserved in
`target/wave6/checkpoint-basic-buffered-truncate-runtime-before-fix.zip`, SHA256
`2dd8aa55a6b3758eacbaf0ab087a1befb692269e21e704fb0cd6727ecf06a8ca`.
A preceding fixture compilation error used a nonexistent fault snapshot API;
the test uses the existing `fault::fired()` counter. It was not a runtime pass.

The fix removes the immediate native FUSE byte save. A size change through an
open handle remains buffered until fsync/close; the path-only temporary handle
still completes its durable close before success. Legacy size handling is
unchanged. The existing actual kernel parent now requires successful set_len
with zero checkpoint-hook hits and zero canonical byte publications, then
append/fsync/exact read/reopen/oracle checks. Windows passed this strengthened
parent and all 25 ordinary native parents; Linux passed the same parent,
25 ordinary native parents and 124 library parents.
This is targeted delta coverage, not a repeated full-suite or build timing claim.

Evidence: [buffered-truncate-checks.json](../../../test-evidence/wave-6/buffered-truncate-checks.json).
The 385 inputs hash to
`6cbb2304f220f1e6669e3efbc4fa1183b665b86766104ab2918e3ea904693a63`;
only `src/fuse_linux.rs` and `tests/content_mount_writes.rs` differ from CCR24.
Formatting, diff and strict release Clippy passed on both platforms. Actual
images stayed unchanged before/after direct and kernel execution. Three default
Windows native ignores and four combined Linux library/native ignores are not
passes. No new unique parent was added, and children/repetitions are not counted
again. Every exact source member and both attempt archive inventories were read
back/hashed. Archive `target/wave6/buffered-truncate-source-final.zip`, SHA256
`4f53ae36f1f020fe2cfb3808849ed0dedc4d1b94101b7f1aaf4f451d11d32513`.

The prior fifteen-target report remains historical at its own exact source.
The required WinFsp disable/reset probe remains failing as recorded there;
this FUSE-only production change did not rerun or resolve it. Batching,
private-read independence during FULL journal work, native namespace/lifecycle,
root retirement, aggregate budgets, complete cache/change journal, build/load/
storage gates and production compatibility remain required. Wave 6 stays active.

### CCR-WP6.4-26 — live private reads during FULL checkpoint (in progress)

Two Linux regressions reproduced CONTENT_DEADLINE while the writer stalled
1500 ms at `content.write.before_checkpoint_commit`: an independent SaveFile in
the same journal, and a native reader of the very file being flushed. Each
reader had a 500 ms request deadline and requires completion under 300 ms while the
checkpoint thread is still active. The writer is joined before assertions;
failed reads do not abandon its accepted save.

Exact source `57d2cda33da8c082cefd7031750125bfbcb09a70d270fed2bf8e57d3243345ae`
and raw failures are preserved separately:
`target/wave6/checkpoint-basic-private-reader-other-file-before-fix.zip`, SHA256
`c957f3e3599cecc493f19ce275598af1d1cbbbf95b2406b262c49782828c9201`, and
`target/wave6/checkpoint-basic-private-reader-same-file-before-fix.zip`, SHA256
`c6597df74f874b69dcbf1c95a3a47bfe2c4eabf0a30f2251d66b891b889cd4bb`.
An earlier fixture compilation failure attempted to clone SaveExecutor;
wrapping the existing executor in Arc fixes that fixture without changing
production code. The compile attempt is retained, not a runtime result.

The draft implementation adds up to four lazily opened read-only private WAL
connections with 512 KiB cache limits. Each phase uses a short read transaction;
no SQL snapshot/permit spans provider or network I/O. Existing owner/sequence,
map and payload checks remain. SaveFile mirrors its current session through a
separate short memory-only mutex, updated unconditionally after durable adoption
even if the caller deadline expires. Native reads use the byte-mutation gate;
writes/truncates retain ordering through that gate while save/metadata FULL
work keeps its existing mutation ownership gate. This preserves the existing
started-write/read ordering test while allowing reads during durability work.

Strict Clippy passed on both platforms. The focused Linux run passed 126 library,
22 executor, 26 native and 46 private-journal parents, including the two new
read regressions, capacity/deadline/cancellation and failed/panicking transaction
cleanup. These diagnostic repetitions are not extra unique final cases. Timing
samples and zero-publication observations are now emitted by both read parents.
The final 386-input qualification is recorded below. Retained-reader
acquisition, aggregate budgets, every
adoption/read/GC race, physical slow-disk/load behavior and the complete release
matrix remain required; a synthetic transaction stall is not a physical disk
or build-performance pass. No private format version or activation changes.

Final evidence: [private-reader-checks.json](../../../test-evidence/wave-6/private-reader-checks.json).
The 386 frozen inputs hash to
`9d1150f4307125969d47d43cd65fb07395b78968da05d047be8cdc323bdc9c4a`.
Formatting, diff and strict release Clippy passed. Fifteen selected targets
reported **462 Windows / 470 Linux** ordinary harness cases, with 15 Windows /
14 Linux default ignores. Four new unique parents comprise the two reader
regressions and two pool unit parents. The ordinary early-return receipt driver
is included once/platform, not independent recovery proof. Repetitions and
children are not extra unique passes. Three explicit actual kernel parents and
two actual-reader compatibility parents passed/platform. All 15 executable
hashes/platform stayed exact through direct/kernel/compatibility execution.

Independent-file read times were **25.6476 ms Windows / 0.304571 ms Linux**;
native same-file times were **0.4596 ms / 0.208562 ms**. Each returned correct
private bytes under the original 300 ms bound, with zero canonical publications
and the writer still stalled before FULL commit. The actual Windows time-policy
probe was also rerun at this exact source and still failed with exit101. Its
current-source failure archive is
`target/wave6/private-reader-windows-time-policy-current-failure.zip`, SHA256
`95f45fa4b45830772973ffc5c194edd13fbd3bb0d9e22dcc3549088af07cfe49`.
This remains a mandatory unresolved gate, not a passing test or waiver.

Every exact source member and all four failure archive inventories were read
back and hashed. Archive `target/wave6/private-reader-source-final.zip`, SHA256
`5fbc2dcd824f4a380e2f72f524d6e3afca026334b6b16eefdcd74fd7a173f663`.
The metadata/namespace/lifecycle, retained-root/GC, batching, aggregate resource,
physical disk/storage progress, cache/change-journal, matched build/load,
production compatibility and full integration gates remain open. Wave 6 stays
active and content activation remains off.

### CCR-WP6.4-27 — owned existing-file rename (2026-10-21)

Actual WinFsp and FUSE probes initially rejected rename of an open native COW
file. Exact 386-input source `d1e55679346a5a8dd18e7046dae7542b07238a4da3aa13f56b5c2d2d2abdccea`
and the raw failures are preserved in
`target/wave6/private-reader-native-rename-before-windows.zip`, SHA256
`ec4a55d3ef1e59c060b920a30f58341518333ef01f4e39100a8679f3df9851ea`,
and `target/wave6/private-reader-native-rename-before-linux.zip`, SHA256
`f4287b1c607e070e22ee23210d3aeece10a9437e8eb1dd7c885e1e5c09110846`.

Core now commits an exact namespace receipt and location event under FULL WAL,
rebases the pending byte capture dependencies and selection, and retains the
same byte operation/content/alive parents. The opaque metadata owner protects
only the source entity. An optional Core replacement still fences the replaced
entity's save; it cannot exempt every entity in a multi-change event. Ordinary
local namespace events outside this command retain their existing fences.
The receipt records the raw request and committed event, so replay after a
later rename or parent-directory move does not resolve the old path again.

The native coordinator FULL-checkpoints private bytes, adopts an already
committed save without publishing newer bytes, and tracks a pending rename.
Read-only exact receipt reconciliation observes committed reply loss; absence
never reissues an uncommitted move. Writes/truncates/saves/basic metadata resolve
pending rename observation before later mutation. Activation updates the live
session's name/parent/location revision while preserving private bytes and
content/alive ancestry. Mounted Windows notifications use root invalidation
when no bounded pre-commit directory snapshot is available.

WinFsp op12 and threaded FUSE12/45 use this path for an existing native file.
Focused actual kernel runs passed on both platforms: rename away/back through
one open writer, read private bytes through the new path, keep zero byte
publications, then truncate/write/fsync/read/reopen/oracle. Current strengthened
assertions also require the stable operation UUID and exactly two rename receipts.
FUSE legacy rename retains target-settle ordering through its threaded fallback.

Focused Linux checks passed 10 Core namespace parents (one includes three actual
exit86 children), 30 native parents, the existing eight Core partial-metadata
parents and 60 save parents. A later capacity parent also passed; native final
coverage now selects 31 ordinary parents. They cover newer Saved rebinding, basic retention, replaced
owner fencing, remote content/location alternatives, lost reply, moved parent,
clean continuation, same-sequence mismatch, reserved byte-event identity and
silent SQL capture/selection/receipt reversion. Three fixture compile errors used Conflict.kind instead of field, attempted a
crate-private validate method and called the private cow_file convenience method;
production validation/assertions remain, and exact attempts/logs are preserved.
These are diagnostic repetitions, not extra unique final passes. The final
16-target qualification is recorded below.

Native replacement of an existing destination remains explicitly refused,
including a race that creates a destination after observation (Core no-replace
returns collision). This is a required continuation, not a waiver of atomic
replace or retained destination handles. Native create/unlink/delete-on-close,
complete rename/lifecycle, root retirement, batching, budgets, cache/journal,
matched build/load/storage, migration and full integration gates remain open.
No production activation or private journal format change.

Final evidence: [native-rename-checks.json](../../../test-evidence/wave-6/native-rename-checks.json).
The 390 frozen inputs hash to
`3c422b14a9ea7538f14ec958a5141e17d688a7aae77861b3c7ef2887d7712778`.
Formatting, diff and strict release Clippy passed. Sixteen selected targets
reported **477 Windows / 485 Linux** ordinary harness cases, with 16 Windows /
15 Linux default ignores. Fifteen new unique parents comprise 10 Core namespace
parents (one includes three actual exit86 children) and five native parents.
The existing kernel parent was strengthened, not counted as another unique
parent. The early-return receipt driver remains included once/platform, not
independent recovery proof. Repetitions and children are not extra unique cases.

Three explicit actual kernel parents and two actual-reader compatibility parents
passed/platform. All 16 executable hashes/platform stayed exact through
direct/kernel/compatibility execution. The actual Windows time-policy probe was
rerun against the same current image and still failed with exit101. Its exact
source/raw-log archive is
`target/wave6/native-rename-windows-time-policy-current-failure.zip`, SHA256
`bb9b98f62b489762665720170176d71da72d2b8591eab4fc42865464cec3ad70`.
The driver contract remains mandatory and unresolved; it is not a pass or waiver.

Every exact source member and all six failure archive inventories were read
back/hashed. Source archive `target/wave6/native-rename-source-final.zip`, SHA256
`aa4b2a798fe19366ac007109b23f25df73679795eb621de464bb26f4cd41c78c`.
Native existing-destination replacement, create, unlink/delete-on-close and
destination-handle lifetime are still required. Complete read/adoption/GC and
startup/status/shutdown behavior, root/receipt retirement, compatible batching,
aggregate budgets, cache/change-journal and matched build/load/storage gates,
production compatibility/migration and the full integration matrix remain open.
The full Wave 6 goal stays active; content activation stays off.

### CCR-WP6.4-28 — destination retirement Core/private foundation (2026-10-21)

The full Wave6 objective remains active. Native replacement still has its guard;
this step supplies the durable Core/private handoff needed before removing it.
No production store is activated/migrated and no CAS deletion is enabled.

The reproduced pre-fix case replaced a file with an open private writer, then
failed its canonical publication with `STALE_CONTENT_SAVE`. Exact390-input
source SHA256 `7aabb302171b8c9801e0219f34ba430ecb4a3c91d77d16b1a6b8e6f704897cb8`
and raw Linux failure are preserved in
`target/wave6/native-rename-retired-destination-before-linux.zip`, archive
SHA256 `29812bd892e79cf8b536cda2d55196ba9999683ed3e78e53fdba80d4097d4f77`.

Core now binds source and exact destination FULL checkpoints, commits rename
and destination retirement/ownership in one FULL transaction and verifies both
captures/selections plus the handoff record. The removed destination can publish
and continue saving with `alive=false`, keeping bytes/history without restoring
the name. Its event identity includes retirement causality. Ordinary local
restore still fences it; earlier published successor replay recognizes the
namespace transition. Owned retired basic setters preserve removal. Transactional
checkpoint selection also refreshes newer native clocks/policies on both sides.

Private `NamespaceContinuation` adoption changes intent/owner under FULL WAL,
keeps every Working/Saved/pending map, base, native clock and byte sequence, and
records exact lost-reply identity without pruning bytes. It preserves newer
buffered writes and fences old handles. Journal7 explicitly fences older
readers; headless4/5 and exact-clock6 upgrade headers atomically, while native5
clock ambiguity still requires matching-reader recovery/export.

Before new builds, CCR27 v6 content_write images were copied and their exact
hashes checked: Windows `4390ccc8cd33b5d1e90b472d57ed929b85e203441acd3b4f1ed569256dc89ab3`
(3936768 bytes), Linux `e7529f1d1c905ed2fb89e45fbf448ec66bfe177b62447bab022d781210e073a1`
(5679336 bytes), under `target/wave6/compat-ccr27-v6/`. Their source remains
CCR27 `3c422b14a9ea7538f14ec958a5141e17d688a7aae77861b3c7ef2887d7712778`.

New tests exercise clean/pre-published destinations, later saves without name
resurrection, old successor replay, newer source/destination native clocks,
ordinary restore fencing, wrong destination/reserved identity, silent SQL
capture/selection/owner/handoff reversion and lost namespace/private replies.
One new parent drives nine actual exit86 children around namespace, private
adoption and canonical publication, then recovers exactly one retired save.
The existing upgrade crash parent now includes version6 (nine children total),
which is extended coverage of one existing parent, not extra unique cases.

A mixed intermediate diagnostic build overlapped test/implementation edits and
ran the prior live-only basic setter against the strengthened retired-basic
assertion (`FILE_NOT_FOUND`). The raw v2 log is retained; it is not attributed
to the later exact source or counted as final qualification. Subsequent frozen
Core namespace execution passed all20 ordinary parents. The journal suite caught
an old future-format fixture still using7; its refusal assertion must now use8.
Exact393-input source `072d4222aed5d7f68453b8b6473de1716191f299e4b550ab31e018a6b957c53b`
and log are preserved in `target/wave6/retired-destination-future-format-fixture-before-fix.zip`,
SHA256 `8e9ce17ecff0d359fa8b1eafe39fc4f0d85c0ad4c551b4ddc3be950a40ac212f`.
Strict Clippy also required range syntax `4..=6`. The exact source/raw logs are
in `target/wave6/retired-destination-clippy-range-before-fix-canonical.zip`,
source `1cfb2e7c388e1f2591554963c2193c5e103792d8aba6c24a2f9ea21bf7874251`,
archive SHA256 `e12aabaa8afe9d0ffb44fa4d388b01355bed27350e3729c96fd1529dae2d3185`.
Its canonical index supersedes a retained diagnostic index whose aggregate
used Windows Path ordering; every source member/hash is identical. No old
failure source/log is relabeled at the corrected implementation.

Native dual-file admission/ownership gates, destination session observation,
loss-of-reply reconciliation and automatic startup completion of retired Saved
records are still required before native replacement is enabled. All native
create/unlink/delete-on-close and complete lifetime/remote/cache behavior,
root/receipt retirement and GC safety, compatible batching and aggregate budgets,
change journal, matched builds/load/storage, production compatibility/migration
and full WinFsp/FUSE/peer/outage/soak integration remain mandatory. The real
Windows disable/reset probe remains unresolved and required.

The final source-literal contract audit caught two new unregistered codes at
source `b719855ccceb5e2c8f675a65d93d12e75721b8559ca030a501ecb2e0e7ca3806`.
Exact source and raw Windows failure are preserved in
`target/wave6/retired-destination-error-code-audit-before-fix.zip`, SHA256
`1b71f5e3c87f04ee547c9718ba0709d437004dcac45d216f8129b99925478300`.
`STALE_CONTENT_DESTINATION` is now Contention; the inner selector's required
metadata transaction is Fatal. The wrong-destination parent asserts the wire
class. This is a shared error-contract extension under CCR28.

A Windows full transport execution at source
`072d4222aed5d7f68453b8b6473de1716191f299e4b550ab31e018a6b957c53b`
missed the unchanged250ms structural-admission gate:251.2949ms, after seven idle
polls while both background jobs remained stalled. The same image's isolated
repetition passed92.2784ms; the next full run passed93.6402ms. These repetitions
do not erase the observed violation or qualify aggregate scheduler/load stability.
Exact first failure source/raw output is in
`target/wave6/retired-destination-transport-windows-first-failure.zip`, SHA256
`f9bb1bfb3fe68715ed1ce8576e1f70aaa7871d19b7d774cfbfea6a6c4d4690bd`.
No timing bound was increased and no failure assertion was disabled.

The v6 compatibility parent now additionally executes the preserved actual v6
native writer to create Saved+exit86, upgrades that journal to7 with byte-exact
Working/Saved/seal records before resume, then recovers exact native clocks and
private bytes. The intermediate b719 source passed this parent on both platforms,
as well as actual v4/v5/v6 refusal and all three real kernel parents/platform.
Those intermediate runs are diagnostics; final source/image proof follows below.

Final CCR28 evidence: [retired-destination-checks.json](../../../test-evidence/wave-6/retired-destination-checks.json)
(1195234 bytes). The393 frozen inputs hash to
`2433be80a38604feb8dfa553e32e02f26c351216893bd0406b51124a27cf060a`.
Formatting, diff checks and strict release Clippy passed on both systems.
Sixteen selected targets reported **487 Windows /495 Linux** ordinary harness
cases, with18 Windows/17 Linux default ignores. Ten new ordinary parents are
namespace/retirement cases; one drives nine actual exit86 children. Extending
the earlier upgrade parent to version6 adds coverage, not unique ordinary cases.
Three explicit actual kernel parents and three actual-reader compatibility
parents passed/platform. The actual v6 native writer also created Saved+exit86;
its upgrade preserved exact records, clocks and recovered bytes on both systems.
All16 image hashes/platform stayed exact through direct/kernel/compatibility
execution. No Cargo ran in that platform's cache during its proof.

The final structural-admission samples were94.9796ms Windows and54.315024ms
Linux, under the unchanged250ms bound. The earlier Windows251.2949ms failure
remains preserved and unresolved as a broader stability/load qualification;
passing repetitions do not waive that requirement. The current-image actual
Windows time-policy probe still failed exit101. Its exact393-input archive is
`target/wave6/retired-destination-windows-time-policy-current-failure.zip`,
SHA256 `f85f16f21a4ffbbe6704ba85176888044516b6902832579b8c2ad577a44286be`.
The actual driver/module installation hashes and loaded SxS path were rechecked;
this remains a required driver-contract gap, not a pass or waiver.

Every source member and all six failure ZIP inventories were read back/hashed.
Failure ZIP digests were checked against the identities recorded at capture.
Where final runners reused a loose log pathname, immutable exact raw copies were
extracted from the already attested failure ZIP and independently hashed; the
old failure was not compared with or relabeled as the later successful log.
The earlier raw archive members and archive digests remain unchanged.
Final source archive `target/wave6/retired-destination-source-final.zip`,
2532042 bytes, SHA256
`d94f2eba4ab098f6a1bebc097e1753e3c1e1d9ccdd788f198e4055192881b4d0`.

This completes the Core/private retirement foundation only. Native dual-file
ownership/admission, retained destination FD semantics, lost-reply observation
and startup completion remain next required work; existing-destination native
replacement still returns unsupported. Native create/unlink/delete-on-close,
all read/adoption/GC and remote/cache races, complete startup/status/shutdown,
root/receipt retirement, batching and aggregate budgets, cache/change journal,
matched build/load/storage, actual legacy workspace fencing and production
activation/migration/export/downgrade plus the full integration matrix all remain
mandatory. The full Wave6 goal remains active and activation remains off.


### CCR-WP6.4-29 — native replacement and retained destination FD (2026-10-21)

This sequential integration supersedes CCR28's native replacement guard for
existing typed files only. The complete Wave6 scope remains mandatory. Native
create, legacy source/destination promotion/migration and unlink/delete-on-close
remain unfinished; fixture support is not production activation.

The implementation reserves adapter capacity before sorted source/destination
entity and mutation gates; promotes typed read-only/unopened destinations; takes
both FULL checkpoints and invokes the atomic Core replacement. Each file can
reconcile its own namespace handoff after a lost Core/private reply without
publishing newer buffers. Startup adopts and queues retired Saved operations,
then sweeps abandoned sources, without synchronous preparation under Engine.
Clean already-published Saved records remain preserved but do not masquerade as
unpublished handoff checkpoints.

A new bounded branch/parent/name reservation closes a deferred-create race:
`lookup_visible` observes pending names, and creates plus legacy adapter rename
check the reservation while checkpoint work runs off Engine. Core also verifies
the observed directory identity inside the writer transaction and binds that
identity to exact receipts. Optional omitted request fields preserve old receipts.
New tests check existing pending names, competing immediate/deferred creates,
unrelated-name availability, failure release and a directory identity swap.

Actual FUSE diagnostics exposed STATX returning ENOENT before GETATTR fallback,
then fd set_times without FATTR_FH. Both paths now retain the unnamed open inode;
nlink is0. Actual WinFsp diagnostics required SupportsPosixUnlinkRename for the
explicit fixture mode. Existing ordinary mounts keep it off; new native unlink
and delete-on-close requests are rejected before ACK pending their integration.
Cleanup of a destination already retired by rename verifies Core proof.
The expanded actual-kernel parent reads/stats/sets times/writes/fsyncs the old FD,
then saves a final write by close alone while the new path keeps source bytes.

The native NTFS negative control initially assumed std rename was classic-only.
Rust1.96 uses a POSIX FileRenameInfoEx fallback after access denied. The negative
control now calls MoveFileExW directly; the positive mounted scenario still uses
actual std rename. Its initial failure and all source/callback failures remain
archived; no failing assertion was disabled. One Linux failure source capsule
was reconstructed from the exact prior capsule plus its sole STATX patch, as
recorded in its index; later Windows edits are excluded. Mixed intermediate
builds are diagnostics, not complete source/image qualification.

Both platforms' strict release Clippy passed. Source395 inputs frozen at
`7a971ff69386a9fbdca52619cf63069cf5210a0c3b682f7913e24a3d65638806`;
16-target final builds/proofs are running. The Linux runner initially had CRLF
shell parse errors before compiling; corrected only that runner to LF and
restarted after its terminal failure. Live builds are not restarted on wait expiry.
The loaded WinFsp service and all five actual driver/SDK file hashes were
rechecked unchanged from CCR28. No installation or production activation occurred.


Primary callback references used for the diagnosed behavior:
[Linux6.8 FUSE dir.c](https://raw.githubusercontent.com/torvalds/linux/v6.8/fs/fuse/dir.c)
(STATX ENOSYS fallback),
[WinFsp2.1 fileinfo.c](https://raw.githubusercontent.com/winfsp/winfsp/v2.1/src/sys/fileinfo.c)
(volume POSIX capability and rename semantics),
[FILE_RENAME_INFORMATION](https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/ns-ntifs-_file_rename_information)
(open destination POSIX behavior), and the installed Rust1.96 source
`library/std/src/sys/fs/windows.rs` (MoveFileExW followed by FileRenameInfoEx
fallback). These references explain behavior; installed module hashes and
actual mounted assertions provide the platform evidence. No source reference
is represented as proof of an exact rebuilt kernel/driver image.


The first frozen qualification passed all four actual-kernel parents and all
three actual-reader compatibility parents on both platforms; Linux also passed
all21 Core namespace parents in a direct same-image diagnostic. Its new
pending-name test failed on both platforms during fixture cleanup: closing a
legacy deferred handle queues a save; the test then used raw Core delete before
`settle_entity`, leaving the name pending or racing its publication. Actual
adapters settle before removing/replacing that entity. The fixture now settles,
asserts its deferred name retired, then deletes; collision/reservation assertions
are unchanged. All other new legacy fixture closes also settle before cleanup.
This does not qualify the original legacy burst-close recovery gap or native
create; those remain mandatory implementation work.

Exact first frozen source395 identity
`7a971ff69386a9fbdca52619cf63069cf5210a0c3b682f7913e24a3d65638806`
and failed Linux log are in
`target/wave6/native-replacement-deferred-fixture-unsettled-before-fix.zip`,
SHA256 `167becd8e37ee8cd8401b3d6a87d4f2d91af99299badf7f75eb95705cd0b0bc0`;
the corresponding Windows capsule is
`target/wave6/native-replacement-deferred-fixture-unsettled-windows-before-fix.zip`,
SHA256 `5cbc658291f2b9397a129360968fbf3ae26e89bc03c7bb095faf6d439f5c3a08`.
Both builds/proof processes were terminal before the test-only edit. Corrected
source frozen at
`39ed1695f6be2e20560819d34cac6f5dbaffa907ef97deafa8e4c83c5e62f656`;
final lint/build/proof repeats are running against this identity. No live build
was restarted because a wait returned early.


Final CCR29 evidence:
[native-replacement-checks.json](../../../test-evidence/wave-6/native-replacement-checks.json)
(1372314 bytes),395 inputs frozen at
`39ed1695f6be2e20560819d34cac6f5dbaffa907ef97deafa8e4c83c5e62f656`.
Strict release Clippy, formatting and diff checks passed on both systems.
Sixteen targets/platform passed **501 Windows /509 Linux** ordinary harness
cases, with20 Windows/19 Linux default ignores. Fourteen new ordinary parents
compared with CCR28 are13 runtime cases and one Core observed-parent case; one
runtime parent drives nine actual exit86 children, counted once. Four explicit
actual-kernel parents and three actual-reader compatibility parents passed on
each platform. All16 images/platform retained exact hashes before/after direct,
kernel and compatibility calls. No Cargo used that platform's release cache
during proof. Actual v4/v5/v6 readers refused7; actual v5 ambiguity remained
refused unchanged; actual v6 native Saved upgrade preserved clocks/bytes/records.

The four kernel parents include actual open-destination replacement, old-FD
read/metadata/set_times/write/fsync and final close without explicit fsync.
The new name preserves source bytes and old retired saves remain alive=false.
Linux checks nlink0; Windows also checks direct MoveFileExW classic failure,
explicit native NTFS POSIX success, mounted std rename and early rejection of
unimplemented unlink/delete-on-close requests.

Final structural samples were93.3811ms Windows and54.622432ms Linux under the
unchanged250ms bound. Earlier CCR28 observed251.2949ms remains an unresolved
aggregate scheduler/load reliability qualification; these samples do not waive
it. The current-image Windows time-policy probe still fails exit101. Its exact
source/raw probe capsule is
`target/wave6/native-replacement-windows-time-policy-current-failure.zip`,
SHA256 `3d8a064d7eedfb014f4eff7c9364f880c6233e21745de016d8519e1fe597740c`.
The loaded WinFsp driver/SDK identities remain unchanged. No test-signing,
driver replacement, reboot or production activation occurred.

Every final source member and all nine retained failure capsule inventories
were read back/hashed. Capture-time archive digests and immutable exact raw-log
copies are checked; reused loose log filenames never relabel an earlier failure.
Final source archive `target/wave6/native-replacement-source-final.zip`,
2545509 bytes, SHA256
`170078764a35377a7821fe8506ba30ebb410c99a049094186a39dd121fa53da1`.

Next required implementation includes native create, legacy source/destination
promotion/migration and unlink/delete-on-close. Complete namespace/lifetime,
read/adoption/GC and remote/cache races, root/namespace-receipt retirement and
physical progress, batching/global byte/job/connection/shutdown budgets,
cache/change journal, matched native/W5/W6 builds, actual old legacy workspace
fencing, capability negotiation/activation/resumable migration/recovery/export/
downgrade, and every original platform/peer/outage/ENOSPC/power-loss/storage/load/
30-minute/final integration gate remain mandatory. The full Wave6 goal stays
active; this scoped native replacement qualification does not close it.


### CCR-WP6.4-30 — typed native creation and initial byte owner (2026-10-21)

The previous goal turn made verified progress (CCR29 native replacement); the
full Wave6 scope remains active. Rechecked the actual worktree, plan, previous
1372314-byte evidence and absence of live Cargo/rustc before this extension.
New regular files through explicitly installed NativeWrites now commit a typed
empty namespace event with their initial owned byte operation and sealed exact
receipt under one FULL Core transaction. Independent mutation/entity/operation
UUIDs and observed parent/view/basic metadata are request identity. Existing
capture logic is factored into a transaction-only helper. Core revalidates name,
parent, active view and reserved identities, then reads back capture, owner,
receipt and canonical creation fields before ACK. Exact replay after publication,
move/delete returns the old receipt without recreating the name or granting
current ownership. New table `content_save_create_changes` retains bounded
sealed records; its eventual receipt/root retirement is mandatory future work.

NativeWrites verifies and pins the empty root once off Engine. Runtime reserves
adapter capacity, parent/name and entity key before off-lock Core/private-journal
creation. Ownership is checked again after private preparation. Native WinFsp
create and threaded FUSE MKNOD/CREATE attach the Cow file before open ACK. Initial
POSIX mode/attributes/times belong to the creation event and cannot accidentally
fence the new owner through an ordinary post-create setter. Existing readonly
empty selections can share the installed session. All subsequent writes/close/
fsync use the established native path; clean empty close publishes no bytes.
No production feature was activated. Default legacy Engine/Core paths and
existing explicit fixture seams remain available; legacy promotion/migration and
unlink/delete-on-close are still required.

The first test compile found use of a private staging helper and String patterns
against str literals. Tests now inspect the public Cow variant and borrow the
error code. Its exact399-input source
`d2e2f0bf2328b58f2096e69697832133312aa46372bd4e847d06611cd4bafe34`
and raw Linux compiler failure are preserved in
`target/wave6/native-create-test-compile-before-fix.zip`, SHA256
`9ae4d8b72a0e434f8e235e461390225172bb6408ccbd705bdca6e70e249edd8a`.
Both initial lint processes were terminal before the test correction. The
permission test's umask is confined to its isolated kernel child and does not
change the harness process or user environment.

Corrected source399 frozen at
`27246339e7ca79d7b832aa7b05ca1077eec44ea737583ebc4a498d01002e6fa6`.
Strict release Clippy passed Windows/Linux. Linux focused release execution
passed4 Core creation parents and8 runtime parents, one driving nine actual
exit86 children before/after creation event, owner/receipt commit, private setup,
FULL checkpoint, canonical commit and successful close. It verifies typed empty
creation, buffered bytes, readonly permissions, exact failed/lost/expired replies,
name/capacity reservation, concurrent folded-name creators and an owner fenced
during private preparation. Core sabotage tests ignore/delete receipts/captures
or null the owner and require full rollback. All source and focused executable
hashes stayed exact during those diagnostics.

The actual FUSE kernel parent also passed: create typed empty content before
writing, preserve buffered size, explicit fsync, replace a newly created open
destination, save both final writers by close without explicit final fsync,
leave a clean empty file with no byte publication, and create/reopen content
above64KiB as a manifest. It requires four creation receipts and four byte
publications, with derived-state oracle equality. Windows actual-kernel and the
full16-target/platform proofs are running; Linux focused results do not qualify
them. Live builds will be polled to terminal status rather than restarted on
observation timeout.


A further integrity review identified missing exact initial-capture comparison.
Without changing the frozen399 source inputs or mutating a release cache, a
standalone Rust probe linked the actual optimized Linux Core rlib. It installs
a valid-shape, correctly checksummed capture with an access-time field changed
by17 through AFTER INSERT triggers at both `content_save_operations` and
`content_save_create_changes`. Both executions unexpectedly returned successful
creation ACK, then failed the probe assertion (exit101). Thus the passing4/8
focused cases and five kernel parents do not close initial-capture integrity.

The exact source272 identity and raw negative probes are preserved in
`target/wave6/native-create-valid-capture-before-fix.zip`, SHA256
`2a596d6efff47e0913fa5789f48e412299852cf6a9275a18515ba97e8b519353`.
Its index includes the separate probe source, full direct rustc command, source
hash, exact static probe image and linked-library hashes, verified unchanged
before/after both runs. The retained executable
`target/wave6/native-create-capture-probe-linux` is not overwritten by later
builds. Probe source outside the399-input application inventory is included
explicitly in the failure capsule. The toolchain/libraries do not constitute an
unrecorded source change or a passing build-only test.

Windows has now also passed the real native-create kernel parent and all five
explicit kernel parents. Current ordinary-suite qualification is still running;
the initial source stays frozen until both proof processes terminate. Next fix
must compare the full expected capture after insertion and again after creation
receipt insertion, with a permanent valid-capture substitution regression. The
Wave6 scope remains unchanged and production activation remains off.


Both initial proof processes terminated successfully before the integrity fix.
The complete initial513 Windows/521 Linux matrix, five actual-kernel parents
and three actual-reader parents/platform are preserved as diagnostics in
`target/wave6/native-create-initial-matrix-before-capture-fix.zip`, SHA256
`a6c50dba20dd34e5356814576ccdee32592f20926c8d4e31d1218c9acd65ad82`.
Those passes do not override the independently reproduced invalid ACK.

The capture helper now returns the complete expected Capture, reads it back
with empty selection/receipt and checks current ownership before returning.
Native creation compares that entire capture again after receipt insertion.
The permanent Core regression substitutes valid sealed captures at both insertion
boundaries and requires full namespace rollback. The existing runtime owner-fence
parent now also opens a readonly descriptor while private preparation stalls;
its success branch requires that reader to share later private writes, and its
fenced branch still refuses the creator handle. The actual kernel parent now
creates the manifest file under a newly created directory.

Corrected399-input source frozen at
`7c15299df8e908600e49f93eb310a816f1ed86e4ac3d70525b8f5f445a9fc150`.
Strict release Clippy passed both platforms. Final full release/proof repetitions
are running. Expected ordinary count is514 Windows/522 Linux (8 new runtime
parents,5 Core creation parents compared with CCR29); this is a target count,
not yet the final result. All five installed WinFsp files and the loaded service
path/state were rechecked unchanged; no driver installation or activation occurred.


Final CCR30 evidence:
[native-create-checks.json](../../../test-evidence/wave-6/native-create-checks.json)
(1258512 bytes). The399 frozen inputs hash to
`7c15299df8e908600e49f93eb310a816f1ed86e4ac3d70525b8f5f445a9fc150`.
Strict release Clippy, formatting and diff checks passed. Sixteen targets/platform
passed **514 Windows /522 Linux** ordinary cases, with22 Windows/21 Linux default
ignores. Thirteen new ordinary parents compared with CCR29 are8 runtime cases
(one drives nine actual exit86 children) and5 Core creation cases, including
valid sealed capture substitution at both insert boundaries. Children and
repetitions are not extra unique passes.

Five explicit actual-kernel parents and three actual-reader compatibility parents
passed/platform. The expanded creation kernel creates a manifest file inside a
new directory; direct empty creation, buffered write, explicit fsync, newly-created
open destination replacement, durable final close without explicit fsync and
empty close without byte publication all passed. The readonly-open-during-private-
preparation case verifies both successful shared private bytes and fenced-owner
refusal. All16 executable hashes/platform stayed exact before/after direct,
kernel and compatibility calls. No Cargo used a platform's release cache during
its proof. Actual v4/v5/v6 private readers refused7; actual v5 clock ambiguity
remained refused unchanged; actual v6 native upgrade recovered exact records,
clocks and bytes. This does not supply the full old legacy workspace fence.

The unchanged standalone probe source was linked to the corrected optimized
Core library. Both valid-capture substitution probes now return
`CONTENT_METADATA_CONTINUATION_LOST` and exit0. Its image
`target/wave6/native-create-capture-probe-after-fix-linux` has SHA256
`cc6f3aaf92d680194f9744f34dee1798eb7265659af848a0329a75c2e8c65573`.
The prior actual executable, source and compiler/library hashes remain preserved;
its two unexpected-ACK exits101 are not relabeled as passing tests. The initial
513/521 passing matrix is separately labeled a passing diagnostic for source272,
which missed this later-discovered defect. Both that matrix and the negative
proof remain in immutable source/log capsules. The final permanent regression
covers the defect in both platform suites.

Final structural samples were92.5753ms Windows and50.920146ms Linux under the
unchanged250ms bound. Earlier CCR28 observed251.2949ms remains a real aggregate
scheduler/load reliability gap; these samples do not waive it. The current-image
Windows time-policy probe still fails exit101. Exact399-input source/raw output
is in `target/wave6/native-create-windows-time-policy-current-failure.zip`,
SHA256 `e65f1e9b1478a9246cff1581d8781d50f5ca53049dffe5c4902cc64ea06f4f2a`.
All five actual installed WinFsp file hashes and loaded driver identity were
rechecked unchanged. No driver replacement, test-signing, reboot, production
activation or live workspace migration occurred.

Four diagnostic/failure capsules were independently read back with their exact
source inventories, capture-time archive hashes and raw members. One is the
initial passing matrix; the other three retain the test compile failure,
valid-capture negative proof and current time-policy failure. Reused loose log
paths never relabel old results. Every final source member was also read back
and hashed. Final source archive `target/wave6/native-create-source-final.zip`,
2558528 bytes, SHA256
`c5b45d82704eeec1761f125aaae9b95db8c0564fe2c2a6b5c947bba1ce6a5829`.

Next required implementation includes existing legacy source/destination promotion
and migration, native unlink/delete-on-close and directory namespace mutation
outside broad Engine durability locks. Complete namespace/lifetime, read/adoption/
GC and remote/cache races, root/create/namespace-receipt retirement and physical
progress barriers, batching and aggregate byte/job/connection/queue/shutdown
budgets, cache/change journal, matched native/W5/W6 builds, capable-peer negotiation,
actual old legacy workspace fencing, explicit activation/resumable migration/
matching-reader recovery/export/downgrade and every original WinFsp/FUSE/WSL/
two-peer/outage512MiB45sec/ENOSPC/power-loss/OS-close/storage/load/30-minute/final
integration gate remain mandatory. The complete Wave6 objective stays active;
this fixture regular-file creation qualification does not close it.
