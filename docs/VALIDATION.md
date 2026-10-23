# Validation

Scope amendment, 2026-10-22: apply the [personal-use authority
scope](revamp/AUTHORITY_SCOPE.md). Current acceptance checks DVFS pairing/route
binding, reference scope, stale work, integrity, durability and filesystem
semantics. Users, ACLs and public-service/hostile-user hardening are not gates.
Historical security-suite results remain evidence of those exact images;
custom HMAC/enrollment tests are not mandatory target mechanisms. Rebase their
applicable authority scenarios when simplifying implementation, with replacement
coverage recorded. This documentation review neither skips current correctness
tests nor claims new qualification.

Pairing acceptance includes an online/reachable server refusing peer use with
missing or incorrect codes, accepting the correct code, and preserving the
admission boundary across unpair/reconnect. After admission peers are trusted;
retained scope/ownership checks exercise filesystem correctness, not malicious
authenticated users. These are required scenarios, not newly executed results.

How this release is checked, how to run the checks, and what they measured.
Four layers:

1. the **test suite** (`cargo test`): unit, integration and multi-daemon tests,
   about a thousand tests on Windows, a subset on Linux;
2. the **qualification scenarios** (`scripts/qualify.ps1`, `scripts/qualify.sh`):
   release-build scenarios with performance and behaviour floors, recorded
   as evidence JSON;
3. the **desktop manual checklist** (below), for what the window itself does;
4. the **soak** and the **two-machine lab**, run for the final integration.

**2026-10-19 closure:** the user requested integrating Wave 5 into `master`
while skipping long tests. The completed 2026-10-18 evidence below is preserved,
not rerun or relabeled as fresh qualification. Current focused checks,
omissions and continuation decisions are recorded in the
[closure handoff](revamp/notes/wave-5-handoff.md) and
`test-evidence/qualification-wave-5.json`.

## Wave 6 development qualification

**Scope amendment, 2026-10-21:** this is a greenfield release. The current phase is
the [WP6.G interlude](revamp/WAVE_6_PLAN.md#31-wp6g--greenfield-interlude-in-progress).
Prior-version readers, legacy promotion/migration, mixed-version peers,
previous-executable workspace/launcher fences and downgrade workflows are no
longer acceptance gates. Existing reports below remain historical evidence;
their references to those gates being mandatory are superseded. WP6.G is in
progress; the [inventory and coverage ledger](revamp/notes/wp6-greenfield-inventory.md)
distinguishes removed private-journal upgrades/readers from still-pending
execution and transport consolidation. Its checks must use fresh stores, one
current content/save/transport
path, current-format restart/recovery and current-version peers, with explicit
refusal of unsupported formats. Preserve applicable regression coverage when
removing compatibility-only tests. Wave 5 remains an isolated performance
baseline; all current-path safety, Windows/FUSE, timing/build, load and failure
gates remain, including the known timestamp-policy and scheduling failures.

Wave 6 remains in progress. Worker startup now initializes current native COW
without a fixture activation marker; Core producers and execution/transport
consolidation remain incomplete. The
[implementation record](revamp/notes/wp6-progress.md) distinguishes completed
package evidence, failed attempts and outstanding release gates. Per-package
JSON under `test-evidence/wave-6/` records source hashes, commands, actual test
images, individual samples and omissions. These focused results do not replace
the complete Windows/Linux matrix, matched build measurements, current-format
recovery and retention rehearsals, two-peer outage/load qualification or 30-minute soak in
the [Wave 6 plan](revamp/WAVE_6_PLAN.md#5-build-performance-work-and-acceptance).

[CCR-WP6.G-12 evidence](../test-evidence/wave-6/native-exit-retention-checks.json)
records scoped default-worker and fatal-retention coverage: 288 ordinary tests,
one exact off-lock/rollback/Saved regression and three actual WinFsp/FUSE kernel
parents per platform. The final change affects only the kernel fixture's bounded
observation of delayed Close callbacks. Unchanged harness/CLI hashes retain their
successful results from the preceding source; the report identifies both cohorts
and preserves the intermediate Windows BUSY_VIEW failure and exact failed images.
Strict all-test Clippy and selected builds pass on both platforms. Core producers,
legacy execution/representation/transfer removal and complete release gates
remain open; default startup alone is not a complete single current path.

[CCR-WP6.G-13 evidence](../test-evidence/wave-6/core-current-producers-checks.json)
separates its producer and notification cohorts. The producer source passes nine
complete targets (275 cases/platform) and three actual kernel parents/platform;
the broader eleven-target matrix remains 318 pass / 36 fail on each system.
Current notification-source verification passes 13 focused ordinary cases and
one actual default-kernel parent per platform. It changes only the typed-root
notification comparison and its regression; previous image results are not
presented as executions of this source. Both cohorts pass strict all-test release
Clippy and their selected builds. Failed transfer/lifecycle harnesses and CLI
images, older source/logs and all remaining gates stay recorded and unwaived.

[CCR-WP6.G-14 evidence](../test-evidence/wave-6/current-content-codec-checks.json)
qualifies removal of legacy descriptor codecs, whole-file provider conversion,
both conversion caches and fallback save capture. Each platform passes 395 cases
across 16 runtime targets (including eight internal write/retention cases), in
two explicitly identified cohorts: 359 from 14 complete successful targets, then
36 from the two corrected test files. The initial matrix's 393 passes/two
failures and actual failed images remain preserved. All unaffected harness
hashes were verified unchanged; earlier executions are not called executions of
the final source. Strict all-test release Clippy passes on both sources/systems.
Three actual WinFsp/FUSE parents pass again with final selected CLI images; the
Linux CLI hash changed after rebuild, so its old CLI-dependent results were not
carried. SDK/loaded-driver hashes remain unchanged. Bare strings, standalone
Engine/staging, duplicate transport and all remaining Wave 6 gates stay open.

[CCR-WP6.G-15 evidence](../test-evidence/wave-6/native-saved-lifecycle-checks.json)
records 305 successful cases/platform: 295 across eight integration targets and
ten internal write/exit cases, in two source cohorts. The initial cohort passes
304 and fails one old create expectation; its actual failed executor/CLI images
are preserved. The final test-only correction compares the complete committed
empty entry during blocked publication and requires clean owned-fixture teardown.
Its executor/lifecycle targets pass 46 cases/platform; 259 earlier cases retain
their source/image identity and unchanged harness hashes. One explicit actual
WinFsp/FUSE parent passes again with each final CLI. Current force-stop/Saved
restart, rollback/connection replacement, metadata writer contention, failed
COMMIT followed by failed retention, lock-free heartbeat counting and grouped/
adoption count transitions are covered. Strict all-test Clippy, selected builds
and unchanged SDK/loaded-driver checks pass. The old standalone poison hook is
removed; other standalone execution/staging and transport routes remain pending.

[CCR-WP6.G-16 evidence](../test-evidence/wave-6/mount-default-current-checks.json)
records 209 Windows /215 Linux ordinary passes across six targets, plus two
explicit actual-kernel parents per platform, all on one frozen source/image
cohort. The new parent uses public direct mounting without RPC startup: typed
import/COW rewrite, create/rename/truncate/fsync, idempotent service reuse and
recovery of an exact Saved revision that was not canonical beforehand. It also
checks unsupported private-journal bytes/sidecars and mountpoint preservation
with fresh valid metadata. Existing RPC-default kernel coverage passes alongside
it. Controller force-detach/dispatch-loss cases use current Saved and off-lock
retry. Strict all-test Clippy, selected builds and unchanged SDK/loaded-driver
checks pass. The C optional mode is removed; remaining Rust fallbacks and full
startup ordering/removal/release gates are still open.

[CCR-WP6.G-17 evidence](../test-evidence/wave-6/current-handle-content-checks.json)
records one final 434-input source cohort: 227 Windows/233 Linux passes across
seven complete targets, eight focused passes per platform, and two explicit
actual-kernel parents per platform. The scoped ordinary totals are therefore
235/241. The focused cases cover chunk-local first edits, corrupt-chunk write
and truncation refusal without false dirty state, prior accepted edits, verified
range progress, deadline/fence/capacity release, immutable selection refresh and
actual mounted missing/progress reads. Whole-file handle hydration/reference
staging/copy conversion is removed. Strict all-test release Clippy and selected
builds pass on both platforms; all eleven selected images/platform and the
installed SDK/loaded-driver identities stay fixed through proof.

Three whole broader targets remain diagnostic: `local_fs_throughput` 11 pass/28
fail, `partial_content` 1/7 and `worker_concurrency` 22/7 on each platform. They
are not qualified passing targets and their repetitions do not add to 235/241.
Exact failed harnesses/CLIs and logs are preserved in the final capsule. Earlier
candidates preserve the discovered premature-dirty failure and a test-fixture
cleanup abort; the latter is corrected without changing production poison
handling. The final diagnostic completes all 29 worker cases and retains its
seven original failures. Applicable obligations still need current-path
replacements; retired APIs/formats are not restored to satisfy old expectations.

[CCR-WP6.G-18 evidence](../test-evidence/wave-6/descriptor-only-metadata-checks.json)
records one final source/image cohort: 281 passes/platform across eleven complete
targets and one focused bulk-fixture parent, for **282 scoped ordinary passes**
per platform. Both explicit current RPC/direct actual-kernel parents pass per
platform. The full 45-case feed target retains its original pagination, 5K-event
and idle/private-tail bounds. Current store/header/restart and descriptor tests
pass; strict all-test release Clippy and selected builds pass on both systems.
All thirteen selected harness/CLI images/platform and the SDK/loaded-driver
identities stay fixed through proof. This is not the complete harness-selftest
target or whole-repository runtime suite.

The first runtime candidate passes 11 admission cases and fails one because
public nonlocal commit reads the old whole-file object before type validation.
Its actual failed harness/CLI are preserved before correction. The production
correction passes ten complete targets (236/platform), two actual-kernel parents
and 41 feed cases, but four bulk-feed fixtures still synthesize old strings;
their actual failed images are preserved before replacing that shortcut with
public Core commits. A subsequent fixture collision precondition error and its
failed harnesses/CLIs are also preserved. The final focused fixture covers full
projection, typed bytes, root/outbox handoff, rollback, restart and receiver
convergence. Status/preview/demand consumers and earlier diagnostics remain
unqualified in their recorded scopes; no old-format support or gate waiver is
inferred from their failures.

[CCR-WP6.G-19 evidence](../test-evidence/wave-6/current-status-checks.json)
records one final source/image cohort: **319 scoped ordinary passes/platform**,
318 across fourteen complete targets and one focused bulk-fixture parent. The
new complete targets include ten current-status cases, nine scheduler cases and
eighteen typed-object cases. Both explicit current RPC/direct actual-kernel
parents pass/platform. Strict all-test release Clippy, selected builds, format
and diff checks pass on both platforms; all sixteen chosen harness/CLI hashes
per platform and the WinFsp SDK/loaded-driver identity remain unchanged.

Status coverage includes direct/empty/metadata-only roots, two-level manifests
with repeated chunks and missing tail pages, canonical adjacency checks, known
corruption and repair, receipt provenance independent of private dedup, rollback
and foreign verification commits. A real worker without peers completes local
indexing. Actual folder-details RPC cases prove typed details, stale generation
refusal and Engine availability during a delayed snapshot followed by checkout.
This does not qualify hidden in-place corruption detection by status, uncached
large-store performance, preview/demand APIs, old tables, standalone saves or the
full repository suite. The initial unused-import static failure and the later
inspection finding about no-peer local verification are preserved separately;
the latter interrupted ongoing compilations before any runtime proof. A later
candidate has two failed parents/platform: an incorrect empty-fixture directory
assumption and a missing-descriptor error name absent from classification.
Exact failed status/contracts harnesses and CLIs were preserved before the
fixture correction and reuse of the established Fatal error contract. Earlier
source-specific runtime failures remain unwaived in their recorded scopes.

[CCR-WP6.G-20 evidence](../test-evidence/wave-6/reference-consumer-checks.json)
records **94 scoped ordinary passes/platform** in one source/image cohort: 90
across five complete targets and four exact content_transport parents. The full
new inspection target has 12 cases. Strict all-test release Clippy, selected
builds/CLI and format/diff checks pass on Windows and Linux; seven chosen image
hashes per platform remain fixed through proof. No new kernel-mount or full
content_transport/repository/release qualification is claimed.

Coverage includes current empty/direct/binary/manifest prefixes, historical
conflict alternatives, exact requested ranges and verified receipt completion,
independent cancellation/withdrawal, session deadlines and large reply bodies,
joined explicit requests and stale-owner cancellation, global prefetch limits,
capacity for other workers/readers, status on a session with simultaneous
previews, checkout fences, shutdown/pin release, private missing data and SQL
contention. Two new actual paired-peer/loopback cases hydrate preview/prefetch
while an unrelated source chunk stays absent; existing typed-frame and adapter
range cases also pass. The latter retains its original 250ms admission bound.

Static candidates preserve the Windows GUI namespace error and source findings
about global prefetch and handler reservations. No runtime image was invoked
for either static candidate. Earlier source-specific receipt/readiness, timestamp,
scheduler and broader diagnostic failures remain unwaived; these focused cases
do not relabel those full-target failures. Kernel evidence from G19 belongs to
its earlier source and is not a G20 execution.

[CCR-WP6.G-21 evidence](../test-evidence/wave-6/current-adapter-checks.json)
records 194 ordinary passes across five complete targets/platform, one
exact adapter-range pass/platform, nine focused FUSE unit passes on Linux and
eight actual-kernel parents/platform. Strict all-test release Clippy, selected
builds/CLI and format/diff checks pass; seven Windows/eight Linux chosen images remain fixed
through proof. Kernel cases exercise default RPC/direct startup, create/close,
directory mutation, retained open unlink/replacement, offline fsync and real-peer
range hydration. This qualifies current-only adapter mutation dispatch, not full
transport/library/repository, standalone removal, build/load/soak or release gates.
Earlier source-specific failures remain recorded and unwaived.


[CCR-WP6.G-23 evidence](../test-evidence/wave-6/journal-recovery-checks.json)
records two complete targets (86 ordinary cases/platform), both actual RPC/direct
WinFsp/FUSE parents/platform, strict release all-test Clippy and selected builds.
The new pending-directory regression covers RPC/direct unsupported-journal
refusal without namespace mutation, then current recovery of the retained intent.
The earlier G22 load/suite counts remain tied to their own source. Full Wave 6
and release qualification remain open.


[CCR-WP6.G-22 evidence](../test-evidence/wave-6/deferred-creation-checks.json)
records 358 passes across eleven complete targets/platform, plus three exact
adapter/current-lifecycle cases on Windows and those three plus nine FUSE units
on Linux: 361/370 scoped ordinary passes. Sixteen actual-kernel parents pass
per platform, including every rebased local filesystem case. The 300-file copy,
eight writers x150 files/5s operation and probe limits, atomic replacement,
absent/progressing reads, graceful detach, open/unlinked handles and fsync during
concurrent closes retain actual invocation and byte oracles. Missing mount
capability no longer counts as a passing local scenario.

Strict all-test release Clippy, staged selected builds/CLI and format/diff checks
pass. Fourteen Windows/fifteen Linux image hashes stay fixed throughout proof.
The 26-function ledger replaces old standalone snapshot/spool/deferred test
mechanisms with named current owners, without relabeling historical failures.
Standalone staging/pending APIs/table, full transport/worker-lifecycle/library/
repository, matched builds, storage/outage/aggregate load/soak and release gates
remain open. No format/header/layout change or old-data upgrade is introduced.
The first concurrent Linux execution failed with EIO; diagnostic replay decoded
STALE_CONTENT_VIEW on rename. Its exact initial/debug source and failed harness/
CLI capsules remain in the report. The final source retains operation handles
for both sessions and tests last-user-close plus deadline cleanup deterministically.


The first implemented greenfield tranche is
[CCR-WP6.G-2](revamp/notes/wp6-progress.md#current-private-journals-ccr-wp6g-2):
[greenfield-journal-checks.json](../test-evidence/wave-6/greenfield-journal-checks.json)
records 515 Windows / 523 Linux ordinary passes across 16 targets/platform,
20/19 default ignores, and five actual WinFsp/FUSE parents per platform counted
separately. The private journal accepts only v7 or genuinely fresh input;
prior-layout upgrades/readers are removed. Five new journal parents include
104 nonmutating open/closed refusal combinations and three actual initialization
exits. A separate actual-image probe caught a gap the initial green matrix
missed: read-only SQLite created empty WAL/SHM sidecars for closed unsupported
WAL-mode input. The corrected standalone-header guard, permanent regression,
independent rerun and exact before/final source/image/log capsules are recorded.
Strict release Clippy, formatting and diff checks pass. These results qualify
this journal change; other WP6.G routes/default-path acceptance and final Wave 6
gates remain pending in the inventory.

[CCR-WP6.G-3](revamp/notes/wp6-progress.md#current-core-metadata-ccr-wp6g-3)
selects the directly initialized Core metadata layout and retains bounded current
derived-index repair. [greenfield-schema-checks.json](../test-evidence/wave-6/greenfield-schema-checks.json)
records 761 Windows / 769 Linux ordinary cases across 29 selected targets, 29/28
default ignores and five real kernel parents/platform. The final follow-up is
test-only: W2 waits for the public hub's published readiness before the unchanged
CLI assertions. Exact input/image comparisons carry successful unchanged targets
with their original source SHA; final W2 runs on both platforms and outstanding
Windows worker-status/kernel cases are recorded separately. The preceding failed
Windows status assertion and complete Linux matrix are preserved. Strict release
Clippy, formatting/diff checks and before/after WinFsp SDK/driver checks pass.
This qualifies the Core change. The separate catalog tranche follows below;
descriptor, default-execution, transport and final Wave 6 gates remain open.

[CCR-WP6.G-4](revamp/notes/wp6-progress.md#current-catalog-ccr-wp6g-4) selects the
directly initialized seven-table catalog. Its
[greenfield-catalog-checks.json](../test-evidence/wave-6/greenfield-catalog-checks.json)
records **1,143 Windows / 1,151 Linux ordinary passes across 43 selected targets**,
43/42 default ignores, five actual kernel parents/platform and one separately
invoked current restart/two-peer scenario/platform. The last fixture rebase is
test-only: replace a dropped-table migration fixture with credential-row loss in
stopped current fixtures, preserving identities and requiring new authenticated
credentials after restart. All 28 security cases rerun on both platforms. The
other 42 targets, five kernel cases and peer scenario retain their actual preceding
source SHA and unchanged invoked images; all production and other inputs match.
This is staged evidence, not one uninterrupted final-source run. Both original
27-pass/1-fail security results and exact before/final source/log capsules remain.
Strict release Clippy, format/diff checks and before/after WinFsp SDK/driver checks
pass. This qualifies catalog consolidation; the typed filesystem/transport path
is still optional and the remaining WP6.G/Wave 6 gates remain open.

The isolated COW base tests use real SQLite, verified CAS and authenticated
loopback/peer sessions. A focused ordinary run creates only temporary stores:

```powershell
cargo test -j 1 --release --locked --offline --test content_transport native_cow_ -- --test-threads=1 --nocapture
```

Actual mount tests are explicit ignored opt-ins and require working WinFsp
or FUSE. The authenticated COW fsync test runs its mount in an isolated child
process and fails if that real mount or peer scenario cannot run:

```powershell
cargo test -j 1 --release --locked --offline --test content_transport -- --exact isolated_kernel_cow_write_prepares_base_then_fsync_succeeds_offline --ignored --test-threads=1 --nocapture
```

This test requires authenticated hydration before the kernel write returns,
disconnects the peer, then checks local fsync and reopen equality. Ordinary
tests also remove a required local base object after an acknowledged write and
require immediate local failure, zero demand-hook calls, retained Saved/private
bytes and successful retry after restoring that local object. The 1 GiB
deduplicated-tree/4 KiB edit test bounds preflight reads/pages and asserts no CAS
installation or FULL checkpoint before fsync. Fixture seeding is excluded.
Earlier CCR17 evidence retains its interim fsync-network and buffer-permit gaps
as history; the current preflight/local-fsync boundary is recorded separately.

The structural-idle case observes seven empty structural preparations of the
tested repository, removes its local manifest page, then starts a native COW
write behind two stalled background source reads. It requires admission within
the original 250 ms bound and checks repaired bytes plus local fsync:

```powershell
cargo test -j 1 --release --locked --offline --test content_transport -- --exact native_cow_structural_repair_admits_within_250ms_after_idle_behind_stalled_background --test-threads=1 --nocapture
```

The implementation record preserves the failing unmodified timing policy and
distinguishes this scenario from the full aggregate fairness/load matrix.

Core owned-basic tests use real FULL WAL metadata and private Saved fixtures:

```powershell
cargo test -j 1 --release --locked --offline --test content_saves owned_basic_ -- --test-threads=1 --nocapture
```

They include actual process exits and SQL triggers that silently ignore/delete
receipts or revert otherwise valid captures. The implementation record retains
the pre-fix failures. These Core checks do not claim WinFsp/FUSE set-basic
callback integration; existing kernel scenarios are separate regression checks.

Native attribute/close isolation uses an ordinary checkpoint-fault regression
and the existing isolated kernel write parent on actual WinFsp/FUSE:

```powershell
cargo test -j 1 --release --locked --offline --test content_mount_writes -- --exact read_only_native_cleanup_and_close_keep_other_writers_buffered_but_fsync_saves --test-threads=1 --nocapture
cargo test -j 1 --release --locked --offline --test content_mount_writes -- --exact read_only_cleanup_rechecks_checkpoint_state_after_a_started_native_write --test-threads=1 --nocapture
cargo test -j 1 --release --locked --offline --test content_mount_writes -- --exact isolated_kernel_mount_writes_truncates_flushes_and_reopens_native_content --ignored --test-threads=1 --nocapture
```

The kernel parent checks typed size before open, private mutation time through
path and handle stat, and read-only open/stat/close without publishing dirty
bytes, followed by truncate/fsync/read/reopen equality. Its current expanded
scenario also applies native `file.set_times` on both platforms and Linux chmod
without publishing bytes before fsync. It uses existing descriptor files in
temporary stores; native namespace creation remains a separate requirement.
The owned-rename extension moves the open file away/back, reads private bytes
through the new name, keeps the same private file/operation, and requires two
namespace receipts with zero byte publications before later fsync.
The strengthened truncate assertion blocks `content.write.before_checkpoint`
during `File.set_len`, requires zero hook hits and zero byte publications, then
checks explicit fsync and exact read/reopen bytes. Native truncate through an
open handle must remain buffered; this does not weaken fsync or close durability.
The concurrent cleanup case waits for an already-started write, then requires
the recoverability check under that gate to leave newly buffered bytes private.
The existing process-exit recovery parent still requires exactly one published
Saved revision after reopening/closing a read-only handle.

Checkpoint-bound partial metadata has a separate Core target and native
coordinator cases, including selected-save rebasing, adoption without newer-byte
publication, exact receipt reconciliation after reply expiry, and restart:

```powershell
cargo test -j 1 --release --locked --offline --test content_metadata -- --test-threads=1 --nocapture
cargo test -j 1 --release --locked --offline --test content_mount_writes native_basic_setter_ -- --test-threads=1 --nocapture
```

The Windows-only ignored
`isolated_kernel_windows_time_policy_requires_an_observable_driver_contract`
is a retained **failing** actual-kernel probe. WinFsp 2.1 rewrites disable (-1)
to unchanged (0) before its callback. Direct native decoder disable/reset tests
do not establish OS behavior. Keep this gate open; its failure must not be
included in successful kernel counts or treated as an activation waiver.

Exact native clock-pair checks cover two buffered writes and one fsync with
write/change/both clocks disabled on the second handle, plus three real process
exits before publication and exact Saved clock recovery:

```powershell
cargo test -j 1 --release --locked --offline --test content_mount_writes -- --exact native_time_masks_preserve_earlier_buffered_clocks_before_one_fsync --test-threads=1 --nocapture
cargo test -j 1 --release --locked --offline --test content_mount_writes -- --exact saved_native_clock_pairs_survive_process_exit_before_publication_and_reopen --test-threads=1 --nocapture
```

Private v7 compatibility qualification also needs the three attested reference images
and their exact SHA256 environment values (`TKFS_V4_READER`/`_SHA256` and
`TKFS_V5_READER`/`_SHA256` and `TKFS_V6_READER`/`_SHA256`). The explicit ignored parents execute actual old
readers against v7. The v5 parent additionally creates a real v5 native Saved
fixture with the old writer, requires upgrade refusal and compares persistent
DB/WAL hashes. Headless v4/v5 and exact-clock v6 upgrade parents preserve record/checkpoint
bytes and drive nine actual exits. This does not supply production migration,
native-v5 recovery/export or old legacy executable workspace isolation.

Live private reads during FULL checkpoint have independent-file and native
same-file regressions. Both stall at the real private transaction's
`content.write.before_checkpoint_commit` for 1,500 ms and require reads under
300 ms, correct bytes and zero canonical publications while the writer remains
stalled. `WAVE6_PRIVATE_READER` records individual times and marks the physical
disk-stall claim false. Two library parents check the four-connection capacity,
deadline/cancel/reuse and failed/panicking read-snapshot cleanup. These controlled
stalls do not replace physical slow-disk, aggregate resource or build/load gates.

Owned namespace durability has a separate temporary-store target:

```powershell
cargo test -j 1 --release --locked --offline --test content_namespace -- --test-threads=1 --nocapture
```

It covers lost reply, newer Saved binding, replaced-writer fencing, remote
alternatives, moved parent identity and silent capture/selection/receipt SQL
reversion. Its crash parent runs three actual exit86 children before rename,
before commit and after commit, then checks recovered name/private bytes and
exactly one publication. Native runtime tests also cover reply expiry before
later write, no retry after collision disappears, and adapter admission before
the file gate. Native replacement has its own runtime and actual-kernel cases
below; unlink/delete-on-close and complete namespace lifetime remain required.

The Core/private destination-retirement contract adds exact-destination atomic
replacement, a namespace-only journal handoff and publication with `alive=false`.
Its additional crash parent executes nine actual exits around namespace commit,
private adoption and canonical publication, then reopens and finishes exactly
one retired publication without restoring the name. Further parents check
clean and previously published destinations, old successor replay, newer native
clocks, retained buffered maps, lost replies, ordinary restore fencing and silent
destination capture/selection/owner/handoff reversion. Native replacement now
adds a separate set of runtime/crash parents and an actual kernel parent:

```powershell
cargo test -j 1 --release --locked --offline --test content_mount_writes replacement:: -- --test-threads=1 --nocapture
cargo test -j 1 --release --locked --offline --test content_mount_writes -- --exact replacement::isolated_kernel_native_replacement_preserves_open_destination --ignored --test-threads=1 --nocapture
```

The kernel parent requires retained open destination reads, metadata/set_times,
write/fsync and a final close without explicit fsync, while the new name keeps
the source's bytes. Linux checks nlink0. Windows first verifies a native NTFS
classic-vs-POSIX control, then uses actual std rename through WinFsp; native
unlink and delete-on-close were required to fail before ACK at that historical
checkpoint. CCR-WP6.G-5 now has a positive dedicated kernel parent for those
operations; the correction removes the replacement parent's two temporary
refusal guards while retaining all replacement assertions. That parent passes
in the corrected G5 matrix. The initial G5 candidate remains preserved as failed:
persistent unlink commit failure on actual WinFsp logs a void Cleanup error and
leaves the name visible. The exact initial source, actual failed Windows harness,
worker CLI and raw positive/negative results are preserved in
[native-unlink-initial-checks.json](../test-evidence/wave-6/native-unlink-initial-checks.json).
Its two focused ordinary targets pass 31+57 cases per platform and its one
dedicated actual kernel parent passes per platform. Those results do not replace
the failing Cleanup probe, the unrun 11-target/six-kernel matrix or required
durable deletion/cancellation/restart coverage. The correction closes that scoped
gap in
[native-unlink-durable-checks.json](../test-evidence/wave-6/native-unlink-durable-checks.json):
512 Windows/520 Linux ordinary cases across 12 targets/platform and seven actual
kernel parents/platform, strict release Clippy and unchanged source/13 image
identities/platform. The new actual Windows parent verifies cancellation and
restart of existing-file and new-file delete-on-close under persistent metadata
failure, retaining Saved bytes. FUSE must return the unlink error and preserve
the file. The source/log/image failure capsules remain intact; repeated diagnostics
and actual exit helpers do not add unique case counts. Complete directory/default
execution and the remaining Wave 6 gates stay open.
Runtime cases cover sorted dual-file ownership, both saved checkpoints without
byte publication, lost Core/private replies, destination reconciliation before
source observation, startup progress while canonical preparation stalls, nine
actual exit86 boundaries, pending-name collisions and reservation release.

CCR-WP6.G-6 directory creation and native SDK guard proof is
[native-directory-create-checks.json](../test-evidence/wave-6/native-directory-create-checks.json):
522 Windows/530 Linux ordinary cases across 12 targets/platform and nine explicit
actual kernel parents/platform, with 26/25 default ignores. Five new Core and
five runtime parents cover metadata-only creation, initial mode, exact receipt
replay, scope/parent fencing, rollback/lost replies, three exit86 children,
expired ACKs, reservations and enumeration. The two added kernel parents exercise
real mkdir with delayed commit/independent existing-handle metadata/nested files,
and committed rename with parallel new-name open and old-name recreation. Initial
same-parent named-stat failures and actual failed images are preserved. The final
test records an unopened-name lookup separately; Linux's observed wait is not a
passing named-lookup/cache/load gate. The Windows handle wait is fixed by the
supported native SDK null operation guards, with Rust namespace/entity ownership
and Core validation retained. The unchanged current-image Windows time-policy
probe still fails exit101 and has its own exact source/harness/CLI archive.
G6 did not qualify directory rename/delete/basic metadata or durable directory
dispositions. Their scoped G7 result is
[native-directory-metadata-checks.json](../test-evidence/wave-6/native-directory-metadata-checks.json):
429 frozen inputs, strict release Clippy on both platforms, 539 Windows/548 Linux
ordinary cases across 12 targets/platform, 29/28 default ignores and eleven
actual kernel parents/platform. All 13 images/platform remain unchanged through
proof. New coverage includes full-scope rollback after receipt insertion, shared
64-request disposition capacity, three actual exit86 removal boundaries, restored
directory inode separation, empty-directory replacement with old handles and
persistent Windows Cleanup failure/cancellation/restart. FUSE removal remains
fallible. Exact failed source/logs/images and the current source archive are
recorded in the report. Default execution, full remote/deadline/lost-observation
lifetimes, the unresolved Windows timestamp probe and all other original Wave 6
release gates remain open. No new transport-security gate is introduced; the
selected channel boundary is standard TLS/WSS or the enclosing VPN.
CCR-WP6.G-8 removes custom HMAC/nonces/mutual proofs and enrollment wrapping.
Scoped [authentication simplification evidence](../test-evidence/wave-6/basic-transport-auth-checks.json)
records strict Clippy on both platforms, 272 ordinary cases across six Windows
targets and 335 across seven Linux targets, plus two actual peer/kernel parents
per platform. The Windows broader typed-transfer target separately passed 53
and failed one receipt/readiness counter assertion; its source and actual failed
images are preserved, and the failure remains unwaived. Directory post-commit
deadline reconciliation is being corrected separately; this is not whole-wave
qualification or an additional security release gate.
CCR-WP6.G-9's [directory observation evidence](../test-evidence/wave-6/directory-observation-checks.json)
passes 184 ordinary cases per platform in explicitly recorded stages: 130
namespace/runtime cases on the production correction, then 54 transfer cases
after a test-only diagnostic startup guard. No explicit kernel parents were run
in this stage. Retained metadata/Cleanup now recovers the owned removal receipt
after a deadline, without adopting a later restoration. Read-only enumeration
and full remote lifetimes are still open. The older Windows receipt-counter
failure remains unresolved despite the passing reruns.
CCR-WP6.G-10's [retained enumeration evidence](../test-evidence/wave-6/directory-enumeration-checks.json)
adds 78 ordinary cases and two actual directory kernel parents per platform on
its explicit source, with 13 Windows/12 Linux default ignores. The unchanged
standalone reproduction now enumerates after the committed-removal deadline.
All two harness/CLI hashes per platform and the five WinFsp SDK/driver files
remain unchanged through proof. This closes that owned-removal enumeration gap;
full remote/concurrent-parent lifetimes and all other original release gates
remain open.
A Core parent swaps the target directory's identity before commit and verifies
exact parent-bound receipts after that directory moves again. Final CCR29 proof
is [native-replacement-checks.json](../test-evidence/wave-6/native-replacement-checks.json):
501 Windows/509 Linux ordinary cases across16 targets/platform, four explicit
actual-kernel parents and three actual-reader compatibility parents/platform.
All images were hashed before/after execution against395 frozen inputs; strict
release Clippy, formatting and diff checks passed. The separate current-image
Windows time-policy probe still fails. The initial unsettled legacy fixture
failure and corrected cleanup ordering are retained with exact source/logs.
These scoped
proofs do not supply native create, legacy migration, complete remote/cache
ordering or any other remaining Wave6 gate.

Typed native regular-file creation has separate Core/runtime and actual-kernel
parents. Its initial namespace event and byte-owner capture must commit together;
byte writes remain buffered, and fsync/close retain their existing FULL contract:

```powershell
cargo test -j 1 --release --locked --offline --test content_namespace create:: -- --test-threads=1 --nocapture
cargo test -j 1 --release --locked --offline --test content_mount_writes create:: -- --test-threads=1 --nocapture
cargo test -j 1 --release --locked --offline --test content_mount_writes -- --exact create::isolated_kernel_native_create_close_and_replace_preserve_typed_bytes --ignored --test-threads=1 --nocapture
```

The real kernel parent creates direct empty content, writes privately, fsyncs,
replaces a newly-created open target, saves final source and retired-destination
writes by close without explicit final fsync, and creates/reopens manifest
content in a new directory. Runtime/Core parents cover permissions, pending-name
capacity, concurrent creators, reply expiry, stale owner preparation, readonly
opens during preparation and nine actual process exits. Sealed but altered valid
captures must fail at both insertion boundaries; a missing/corrupt-record test
alone cannot prove that. The initial green matrix missed that defect; its exact
negative probe, source and initial passing diagnostics are preserved separately.
Final scoped proof is
[native-create-checks.json](../test-evidence/wave-6/native-create-checks.json):
399 frozen inputs,514 Windows/522 Linux ordinary cases across16 targets/platform,
five actual-kernel parents and three actual-reader compatibility parents/platform,
plus before/after direct capture-integrity probes with retained image identities.
All selected images stayed unchanged during proof. Strict release Clippy,
formatting and diff checks passed. The current-image Windows time-policy probe
still fails and is archived separately. This regular-file work does not qualify
legacy promotion, unlink/delete-on-close,
all directory mutation/lock surfaces or the remaining Wave6 release gates.

Use the full command/toolchain/target-cache details in the relevant evidence
when reproducing a recorded result. A successful compile is not a test pass;
an ignored case, an early-return child driver and a parent-invoked helper are
not additional scenario proofs. Preserved earlier Wave 5 floors below remain
outstanding for changed Wave 6 sources until explicitly rerun and attributed.

## The test suite

```powershell
$env:CARGO_TARGET_DIR = "C:\tkfs-target\main"
cargo fmt --check
cargo build --all-targets
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

- Windows needs the WinFsp SDK (`WINFSP_DIR`) and builds the Slint desktop.
  On a shared machine, run the full suite through
  `scripts\suite-lock.ps1 cargo test --all-targets` so only one full suite
  runs at a time (the multi-daemon tests are timing-sensitive under load).
- Linux (or WSL) runs the same targets; FUSE tests skip without `/dev/fuse`.
  The subset that matters most there:
  `cargo test --test orchestrator --test session_core --test session_handshake --test worker_lifecycle --test mount_lifecycle`.
- Long scenarios are `#[ignore]`d in the normal suite; they are the
  qualification scenarios below and the release runs of the fuzzers:
  `cargo test --release --test derive_oracle -- --ignored`
  (`TKFS_DERIVE_SEEDS`, `TKFS_DERIVE_SEED_START`; the stress run
  `fuzz_stress_release`), `cargo test --release --test feed_fuzz -- --ignored`
  (`TKFS_FEED_SEEDS`, `TKFS_FEED_SEED_START`), and the crash workloads
  `cargo test --release --test seal_path -- --ignored`.
- `tests/docs_cli.rs` checks that every command in [CLI](CLI.md) exists with
  the options it shows, and that every command of the binary is documented.

What the suite covers, by area (file names under `tests/`):

| Area | Tests |
|---|---|
| Store, activation, conflicts | `core`, `activation_inbox`, `derive_oracle` (randomized histories checked against the full reducer), `offlock_commit`, `startup_snapshots`, `recovery`, `directory_progress`, `availability_index` |
| Metadata feed | `feed_v7` (acks from evidence only, rejections, regressions, private branches, forged origins, large events), `feed_fuzz` (seeded random topologies of 2–4 replicas with lost requests and replies, partial activation, restarts and restores; the oracle after every step, convergence, acks and per-id evidence at the end) |
| Objects | `objects_v3_worker` (durability, crash resume, authorization, leases), `objects_v3_lanes` (both directions, backoffs, handoff, slow links), `partial_content`, `availability` |
| Sessions | `session_core` (framing, multiplexing, heartbeats, deadlines, WSS), `session_handshake` (ordinary hashed pairing admission, lane/version validation, cancellation, TLS) |
| Pairing and application lifecycle | `security` (existing local endpoint tokens, stored peer credentials, revocation on unpair within 1 s, rotation, reconnect, no code, secret or token in status or logs); custom HMAC/transcript tests are retired scope |
| Operations | `logging` (log rotation, redaction, the desktop's daemon log, `worker.log` rotation, diagnostics bundles without secrets) |
| Daemon | `catalog_actor` (including the 6298d0b catalog upgrade and downgrade), `folder_controller` (lifecycle, adoption, gate), `management_api`, `peer_supervisor`, `meta_scheduler`, `inbound_v3`, `status_hub`, `orchestrator` (real daemons over the network), `w1/w2/w3_integration` |
| Worker | `worker_lifecycle`, `worker_process`, `worker_concurrency`, `mount_lifecycle` (WinFsp and FUSE) |
| Desktop | `gui_model` (every chip, row and action from status snapshots), the binary's own window tests |
| Contracts | `contracts` (frozen wire shapes, error-code table), `harness_selftest`, `seam_stubs` |

## Qualification

The scenarios live in `tests/qual_*.rs`, run in release with
`--ignored --test-threads=1`, and print `QUAL {json}` records that
`scripts/qualify.ps1` compares with `tests/qualification-floors.json`.

```powershell
$env:CARGO_TARGET_DIR = "C:\tkfs-target\qual"
powershell -NoProfile -File scripts\qualify.ps1 -All                      # every scenario and floor
powershell -NoProfile -File scripts\qualify.ps1 -Wave 3                   # floors of waves 1-3
powershell -NoProfile -File scripts\qualify.ps1 -Wave 3 -Files qual_wp3_int_e2e -Merge
powershell -NoProfile -File scripts\qualify.ps1 -SelfTest                 # the runner itself
```

```sh
CARGO_TARGET_DIR=$HOME/tkfs-target/qual bash scripts/qualify.sh --wave 3   # Linux subset
```

| Option | Meaning |
|---|---|
| `-Wave N` / `-All` | Run the files of waves ≤ N (all) and enforce their floors. `-All` also raises the budgets the long scenarios need (`-BudgetS 2400`, `-FileTimeoutMin 150`) unless they are given, and writes `qualification-all.json`. |
| `-Files a,b` / `-Filter NAME` | Only these files / scenarios. |
| `-Merge` | Keep the other files' results from the existing evidence and replace only those run now. |
| `-BudgetS S` | Measured-phase budget per scenario (`TKFS_QUAL_BUDGET_S`, default 120). |
| `-FileTimeoutMin M` | Kill a file's run after M minutes (default 90). |
| `-Priority` | `Idle`, `BelowNormal` (default) or `Normal` for the test processes. |
| `-Baseline` | Record every floor without enforcing it (`qualification-baseline.json`). |
| `-NoBuild` | Reuse the release test binaries. |

- Evidence goes to `test-evidence/qualification-<kind>.json` (the git commit
  and dirty files at the start, toolchain, hardware, every file's exit code
  and result line, every scenario's metrics, every floor's verdict); logs to
  `test-runs/qualify-<kind>-<time>/`.
- A file passes only if cargo exits 0 and its last `test result:` line says
  `ok.`. A floor is `pass`, `fail`, `missing`, `incomplete` (its scenario
  timed out) or `not_run`. The script exits 0 only when every enforced floor
  holds and every selected scenario passed or was skipped.
- `TKFS_BASELINE_BIN` (a `tkfs` binary built from `6298d0b`) enables the
  scenario that upgrades one of two previous-release daemons and checks that
  both sides fail closed; without it that scenario is skipped.
- `TKFS_QUAL_SCALE` (default 1) scales scenario sizes down for quick dry runs;
  `TKFS_QUAL_ENFORCE=0` reports a scenario's floors without failing it.

### Floors and the latest measurements

Release build, Windows 11, AMD Ryzen 9 7950X3D (16 cores, 128 GB), a shared
development machine (no other build running at the start). Measured on
2026-10-17 by the final integration (`test-evidence/qualification-all.json`,
`qualify.ps1 -All -Priority Normal` at `27802c3`: 91 scenarios, 90 passed,
1 skipped — SIGTERM has no Windows equivalent — and 54 of 54 floors held;
scenario data on a SATA SSD, `TMP` on `F:`, as WP4.2's run). The baseline
column is the previous release (`6298d0b`); `test-evidence/qualification-final.json`
holds the §15 before/after table with the Linux numbers too.

| Floor | Scenario | Baseline | Floor | Measured |
|---|---|---|---|---|
| Remote linear create, per event, 2K / 10K | `qual_wp1_6_activation` | 15.9 s @2K | ≤ 5 ms | 0.63 / 0.90 ms |
| Local create after a resolved conflict (p90) | `qual_wp1_6_activation` | 4.2 s | ≤ 10 ms | 1.2 ms |
| Local create beside an unresolved conflict, 10K events (p90 of 100; WP4.INT re-review) | `qual_wp1_6_activation` | 262–436 ms (before the scoped conflict gate) | ≤ 20 ms | 2.8–3.1 ms (three runs, measured apart from `-All`; renames 76–91 ms and deletes 70–77 ms, reported: their cut count is O(history) with or without conflicts) |
| Two-way backlog, per event, 2K / 6.4K | `qual_wp1_7_derive` | 15.9 s / 218 s | ≤ 100 / 300 ms | 35 / 132 ms |
| 50 concurrent namespace ops on 5K history (max) | `qual_wp1_7_derive` | seconds to minutes | ≤ 500 ms | 229 ms |
| Restart to Ready at 20K objects | `qual_wp1_8_objects` | full re-hash | ≤ 3 s | 0.95 s |
| `content_status` at 50K objects | `qual_wp1_8_objects` | 6.5 s | < 50 ms | 0.06 ms |
| 12K tiny pending objects complete | `qual_wp1_8_objects` | page cliff | completes | completes |
| Worker `watch` latency | `qual_wp1_4_worker` | no `watch` op | ≤ 1.5 s | 1.0 s |
| Fast call behind a slow one (p99) / control under 30 MiB queued | `qual_wp1_9_session` | head-of-line blocking | < 50 ms | 0.2 / 2.2 ms |
| Session loopback throughput | `qual_wp1_9_session` | JSON+hex request/response | ≥ 300 MiB/s | 2,732 MiB/s |
| Healthy repo beside a hung one (legacy lanes) | `qual_wp1_12_legacy` | lane-wide resets, 30 s backoff | ≤ 45 s | 0.7 s |
| Reconnects at `sync_interval_ms = 20000` over 60 s (legacy, v3) | `qual_wp1_12_legacy`, `qual_wp3_1_transport` | every idle cycle | 0 | 0 / 0 |
| Restarts of a crashing worker in 20 s | `qual_wp1_12_legacy` | every 2 s | ≤ 5 | 5 |
| Management API p99 under lifecycle churn (lifecycle / scale) | `qual_wp2_1_lifecycle`, `qual_scale` | 23–41 s | < 100 ms | 1.3 / 2.0 ms |
| Status subscription latency | `qual_wp2_1_lifecycle` | 5 s poll | ≤ 1 s | 26 ms |
| Shutdown with a busy folder | `qual_wp2_1_lifecycle` | aborts on the first busy folder | ≤ 30 s | 0.39 s |
| File-system op p99 during a 2K catch-up | `qual_wp2_3_worker` | seconds | < 50 ms | 12 ms |
| Status p99 during activation | `qual_wp2_3_worker` | blanked | < 20 ms | 7.5 ms |
| Ready with 5K staged events | `qual_wp2_3_worker` | unbounded startup drain | < 3 s | 0.18 s |
| Engine-lock hold for a slow event at 2K | `qual_wp2_4_offlock` | full replay | < 50 ms | 1.7 ms |
| 150K-event feed / 8 MiB event | `qual_wp2_5_feed` | `INVENTORY_TOO_LARGE` / stall | completes | completes |
| Idle exchange at 50K events (CPU per side / bytes) | `qual_wp2_5_feed` | 1.13 s / 251 KB @6.4K | ≤ 10 ms / 4 KiB | 0.8 ms / 1,621 B |
| Extra delay of a healthy repo beside a hung one (v3) | `qual_wp3_1_transport` | 30 s backoff | < 2 s | 1.0 ms |
| Worker to worker, 256 MiB object / 10K tiny objects | `qual_wp3_2_objects` | about 2 MiB/s / stop-and-wait pages | ≥ 100 MiB/s / ≥ 300/s | 120 MiB/s / 907/s |
| 256 MiB object between daemons on loopback / 1 GiB object | `qual_wp3_int_e2e`, `qual_scale` | 2.0 MiB/s | ≥ 40 MiB/s | 108 / 115 MiB/s |
| 1 GiB through a 100 Mbit/s, 50 ms RTT proxy | `qual_wp3_int_e2e` | not measured | ≥ 80 % of the link | 97.9 % |
| 10K × 4 KiB files between daemons (e2e / scale) | `qual_wp3_int_e2e`, `qual_scale` | page cliff | ≥ 300 files/s | 771 / 770 files/s |
| 10K-event catch-up between daemons | `qual_wp3_int_e2e` | hours | ≤ 60 s | 10.8 s |
| Objects before activation | `qual_wp3_int_e2e` | 0-byte bulk rounds | ≥ 1 | 54 |
| Mutual pairing, duplicate bytes | `qual_wp3_int_e2e` | up to 2× | ≤ 1.1× | 1.002× |
| Two-way backlog between daemons | `qual_wp3_int_e2e` | slow path | ≤ 180 s | 3.7 s |
| Idle exchange between daemons | `qual_wp3_int_e2e` | 251 KB @6.4K | ≤ 4 KiB | 1,913 B |
| 150K-event feed / 8 MiB event between daemons | `qual_wp3_int_e2e` | stall | completes | completes |
| Remote linear activation 2K / 6.4K, two-way backlog (daemons) | `qual_scale` | 15.9 s / 218 s | ≤ 5 / 5 / 100 ms | 0.86 / 1.04 / 1.9 ms |
| Startup to Ready at 50K objects | `qual_scale` | minutes | < 10 s | 2.6 s |
| Idle worker CPU (5K events, 10K objects) | `qual_scale` | 4 sweeps + 2 parses per 500 ms per lane × repo | ≤ 1 % | 0.52 % |
| Bytes written by seals in a 150K catch-up / longest seal | `qual_wp4_4_seal` | 14 GB / 2.9 s per seal | ≥ 10× less / < 50 ms | 72× / 8.6 ms |
| Seal after a 150K-commit gap on another branch | `qual_wp4_4_seal` | 355 ms | < 50 ms | 0.30 ms |
| Restart at 50K entries, rows against inline layout | `qual_wp4_4_seal` | 1.0 | ≤ 1.15 | 1.006 |

The Linux subset (`qualify.sh --all` in WSL 2, Ubuntu, kernel 6.18,
scenario data on the image's ext4, `test-evidence/qualification-all-linux.json`,
at `2a7d96b` with `qual_wp1_9_session` re-run at `2e62858`): 92 scenarios,
91 passed, 1 skipped (the 6298d0b upgrade needs a Linux baseline binary);
52 of 54 floors held, every enforced one. The two misses are the
Windows-only tiny-file floors (219 and 196 files/s; WSL's ext4 image runs a
bare create/write/fsync/rename loop at about 140/s), recorded and not
enforced there. Linux measured, among others: idle worker CPU 0.37 %,
startup at 50K objects 2.2 s, the 10K catch-up 12.9 s, the longest seal
16.9 ms, 1 GiB through the 50 ms proxy at 97.9 % of the link.

Floors bound by fsync latency (tiny files, the 256 MiB worker transfer, the
catch-up) and by single slow operations (`fs_p99_during_catchup`,
`concurrent_namespace_ops_5k`) depend on the disk and miss on a busy
machine: a preliminary `-All` run of this integration with 25 release fuzz
and crash processes running beside it measured 28 tiny objects/s, 263 files/s,
a 72 s catch-up and a 74 ms FS p99 (five floors missed); the evidence above
is the run on a quiet machine. On the NVMe system disk the tiny-file
floors measured 310–315/s at Wave 3 and 180–280/s in WP4.5's runs, made
while that disk was full (0–11 GB free); WP4.2's and this run kept scenario
data on the SATA SSD (`F:`), where they hold with margin.

### Interoperability and upgrade checks

Historical Wave 3 evidence below is retained. These prior-version gates are
superseded by WP6.G. CCR-WP6.G-4 rebases the old-datadir sync scenario to
`qual_wp3_int_e2e::current_restart_then_sync` and replaces the catalog's
upgrade/downgrade parents with current-format restart, identity, gate and
credential coverage. Its qualification is recorded separately in the current
implementation record; old fixture results are not relabeled as current passes.

- `qual_wp3_1_transport::upgrade_against_a_6298d0b_peer_fails_closed_on_both_sides`
  (needs `TKFS_BASELINE_BIN`): two previous-release daemons paired both ways,
  one upgraded in place: the upgraded dialer shows the peer `incompatible`
  with `PEER_NEEDS_UPGRADE` within milliseconds and never offers the code; the
  old side's lanes report `PEER_NEEDS_UPGRADE` within a second; the upgraded
  side lists the old side's attempts.
- `qual_wp3_1_transport::v2_peer_fails_closed`, `qual_wp3_int_e2e::v2_peer_fails_closed`:
  both directions against the legacy client.
- `qual_wp3_int_e2e::legacy_fixture_upgrade_then_sync`: a data directory
  written by the previous release (`tests/fixtures/legacy/datadir`) is opened
  by the new daemon and its folder syncs to a new host and back.
- `catalog_actor` opens the previous release's catalog, checks every folder,
  gate, peer and the verifier survive, and runs a previous-release daemon on
  the migrated catalog.

## Desktop manual checklist

The window's flows are tested through the view model (`gui_model`) and the
command pool; these checks cover the window itself. Run them on Windows with
two hosts (or two data directories on one machine).

1. Start `tkfs gui` with no daemon running: the banner says it is starting the
   daemon and names `DATA/logs/orchestrator.log`; the folders appear.
2. Create a folder (**New folder**): the row shows `Starting…` (busy), then
   `Running`, `Mounted`, `Not shared with any peer`.
3. Pair with a second host (**Pair with a host**, with the code from its
   **Settings**), browse and receive a folder: the received row shows
   `Initial sync from <host>` and `Waiting for initial sync [Mount now]`, then
   `Up to date` and `Mounted`.
4. Copy a large file into one side: the other side's row shows
   `Downloading x of y · rate · ETA` with a progress bar, then `Up to date`.
5. Stop the other host's daemon: the row shows `Waiting for <host> (offline N
   min)`; the sidebar badge `Offline N min`.
6. Create a folder whose mount path already exists: `Mount failed [Retry]`
   with `[needs action] MOUNT_PATH_MUST_NOT_EXIST` and its suggestion.
7. Open a file in a mounted folder and stop it: `Stopping — 1 file open
   [Force stop]`; Force stop asks for confirmation.
8. Pause sync: `Paused [Resume]`.
9. Generate a new pairing code in Settings: it is shown once and cleared when
   Settings closes; the other host's badge becomes `Unauthorized — re-pair`.
   Then generate another code and leave Settings through **Shut down the
   daemon** (both Cancel, and Shut down followed by **Start the daemon**):
   reopening Settings shows no code.
10. Pair with a host running the previous release: its badge reads
    `Peer needs upgrade`.
11. Shut the daemon down from Settings: the banner offers `Start the daemon`;
    nothing restarts it until clicked. `tkfs manage shutdown` from a terminal
    has the same effect on an open window. Then start the daemon from a
    terminal (`tkfs orchestrator`): the window reconnects; end that daemon's
    process: the window starts a new one.
12. While a Stop is in flight (a folder with open files), Start another
    folder: it starts at once.
13. Screenshots for documentation are rendered without a window:

    ```powershell
    $env:TKFS_GUI_RENDER = "out.bmp"
    $env:TKFS_GUI_RENDER_VIEW = "folder:<state>:sync"    # or folders, activity, peer:<installation>
    tkfs gui
    ```

    `TKFS_GUI_RENDER_SNAPSHOT=status.json` renders a saved
    `manage status --json` snapshot instead of the running daemon's;
    `TKFS_GUI_RENDER_SIZE=WxH` sets the size.

## Soak

`tests/qual_wp4_int_soak.rs` (release, ignored in the suite):

```powershell
cargo test --release --test qual_wp4_int_soak -- --ignored --nocapture
```

Two real daemons are paired both ways, each through its own
`FaultProxy` (`tests/support/proxy.rs`), and share one folder. For two hours
(`TKFS_SOAK_MINUTES`) both sides edit continuously through their workers:
new files, saves, directories, renames, deletes and a few files of 1–4 MiB,
one operation every 1–4 s on each side (`TKFS_SOAK_PAUSE_MS_MIN` / `_MAX`),
mostly on names of their own and 10 % on a pool both sides edit
(`TKFS_SOAK_SHARED_PERCENT`), so concurrent edits conflict now and then.
Meanwhile both links cycle every 20–60 s through healthy, latency (5–100 ms
one way), bandwidth caps (256 KiB/s–20 MiB/s), stalls, resets, resets
after some bytes, a blackhole, refused connections, and stalls after some
bytes with jittery latency. Then the edits stop,
the links heal, and it passes when, within 30 min
(`TKFS_SOAK_SETTLE_MINUTES`):

- both folders read `up_to_date` (none ends stalled, blocked, needing
  attention or backing off), every lane of both peers is ready, nothing is
  staged and every outbox row is acknowledged;
- both replicas hold the same shared journal (equal feeds) and the same
  projection, and all content (`content.complete`);
- the derived-state oracle (`Store::verify_derived_state`) is empty on both
  stores, checked with the folders stopped.

Its `QUAL` records keep the edits, the fault phases, a status sample of both
folders every minute and the final state.

Two heavier workloads of the same daemons, links and checks are WP5.1's
acceptance (`tests/qual_wp5_1_concurrent.rs`, `conflict_storm` and
`fast_two_way_editing`, each about 2.5 h; `qualify.ps1 -Wave 5 -Files
qual_wp5_1_concurrent -Heavy -Filter <name>` runs one with its floors):
both sides edit an operation every 50–400 ms each for two hours, (a) every
operation on one shared name pool (a conflict storm), (b) 10 % shared; (b)
also requires the activation backlog to have no upward trend over the
second hour (at most 2 events/min).

**Results of Wave 5** (release, WP5.INT's final code: the storm and the fast
editing at `90ad052`, the soak at `492e00b`, whose code is the same; all three at the same
time on the shared development machine, beside the release oracles, the
WSL test subset and the other WP5.1 scenarios during their first hour; the
details are in
`test-evidence/qualification-wave-5.json` and the WP5.INT notes):

| Run | Edits (A / B) | At the end | Settle | Oracles | Result |
|---|---|---|---|---|---|
| default soak, 2 h (an operation every 1–4 s, 10 % shared) | 2,408 / 2,459 (765 refused, e.g. a name taken) | 5,606 shared events each, 0 unresolved conflicts; 180 fault phases, 261 proxy resets, 1,201 connections, 507 MB through the proxies | 0.02 s | empty | **pass** (7,210 s); 172 of the 240 per-minute folder samples `up_to_date`, the others the fault in progress (`backoff` 28, `offline` 20, transfers 19) |
| (a) conflict storm, 2 h (every operation on the shared pool) | 13,749 / 13,596 | 24,993 shared events, **24,269 unresolved conflicts** | **198 s** (≤ 30 min) | empty (8.9 s / 8.7 s) | **pass** (7,425 s); backlog over the second hour +3.9 / +5.0 events/min, peaks 304 / 391 |
| (b) fast two-way editing, 2 h (10 % shared) | 16,589 / 16,500 | 31,341 shared events, 52 unresolved conflicts | **12.6 s** | empty (19.5 s / 19.4 s) | **pass** (7,262 s); backlog trend over the second hour **+0.38 / +0.41 events/min** (≤ 2), peaks 199 / 213 |

Before Wave 5 (WP4.INT, `9dc5720`): the default soak passed; a two-hour storm
did not converge (9,399 unresolved conflicts, 1,549 and 2,617 events still
waiting after the 30 min settle, the oracle unfinished after 3 h) and fast
editing fell behind (the backlog growing from 140 to 1.5K events over 91
minutes), because a conflicting event's full replay grew with the history
and the open conflicts. WP5.1's slow path costs the event's cones instead
(about 8 ms of CPU per concurrent namespace event at 5K events and 1K open
conflicts, against 269 ms; a full rebuild at 9K open conflicts in about 4 s,
against 519–549 s; *Floors* above). What grows with the open conflicts is
the conflict list each conflict-changing commit writes (README, *Known
limitations*): the storm's backlog peaked at a few hundred events during
fault phases and cleared within minutes of the last edit.

The Wave 4 runs (`9dc5720`, `test-evidence/qualification-final.json`): the
default soak passed (7,257 s: 4,889 edits, 180 fault phases, 5,630 shared
events each, settled in 0.01 s, oracles empty), and the conflict storms of
8 and 20 minutes passed (236 and 1,300 unresolved conflicts).

## Two-machine lab

`test-evidence/lab/` holds the two-machine lab run of 2026-10-16: the
previous release (`6298d0b`) against the Wave 4 base of this one
(`e8c2979`, before the Wave 4 packages merged), on two Windows machines (a
laptop as the source and listener, the 16-core desktop as the dialer and
receiver) whose peer traffic crossed an `ssh -L` tunnel over Wi-Fi (about
7 MB/s each way, 2-3 ms). The harness (`lab_e2e.py` 1.4, `mounted_check.py`
1.1) is not part of this repository; each `results-*.json` records its
parameters, the binaries' hashes, the link probes and the machines' load,
and `results-comparison.md` puts the runs side by side. Every cell is one
run (n = 1), the desktop was shared with other builds, and the link varied
between runs: compare the builds with each other, not with the probe.

| Scenario | previous, 200 files | this, 200 files | previous, 2,000 files | this, 2,000 files |
|---|---|---|---|---|
| S1 receive: every path and hash visible | 161.6 s | 2.0 s | not reached in 30 min | 6.8 s |
| S1 receive: all content (543-603 MB) | 188.5 s (2.9 MB/s) | 75.3 s (7.2 MB/s, the tunnel's ceiling) | 478 of 2,001 files at 30 min | 84.2 s (7.2 MB/s) |
| S2 two-way edits (200 each way) converged | 1031.6 s | 52.5 s | 0 of 200 and 7 of 200 at 30 min | 120.8 s |
| S3 45 s outage during a 512 MiB transfer: restore to complete | 215.3 s | 152.0 s | transfer never started (backlog) | 156.4 s |
| S3 restore to transfer resumed | 28.0 s | 85.8 s | — | 88.5 s |
| Worker `status` calls answered busy or stale (S1) | 63 of 95 | 0 of 38 | 454 of 892 | 0 of 45 |

Every completed transfer resumed from its durable offset and verified. Two
results are worse in the measured build: after the outage the lanes
reconnected in 36-37 s against 28 s (the dial backoff grows during the 45 s
outage, 1 s doubling up to 60 s) and the transfer resumed about 50 s after
that. The second wait is consistent with the base's lease rules: the pull's lease
from before the outage lapsed (60 s without progress) and the lapse backed
the object off for its only source for another 60 s. WP4.5, merged after
this run, releases the leases of a round whose session was lost (no lapse,
no stall backoff); the lab was not run again on the final build, so that
gain is not measured here. The laptop's ingest of 2,000 files into its
WinFsp mount took 163.6 s against 102.6 s, measured once on a laptop shared
with Windows servicing.

The mounted check (this build only) received the 201 files into a WinFsp
mount: the gate opened when the metadata was complete and the mount
attached 2.1 s after `receive`, while the 512 MiB file was still
downloading. All 201 files were listed with the right sizes and 200 hashed
correctly through the mount; reading the large file failed with "invalid
argument" (`STATUS_FILE_IS_OFFLINE`: a read waits up to 15 s for content
that is still downloading, then fails; README, *Initial-sync gate*). A file
written through the mount reached the laptop and hashed correctly there,
and the folder stopped cleanly with its mount point removed.

**Wave 5** addresses the three findings and measures them on one machine
(the coordinator re-runs the lab afterwards): a read of content still
downloading now waits while the download progresses and returns the bytes
(`qual_wp5_2_local::mounted_read_of_a_512mib_object_while_it_downloads`);
after a 45 s outage in the middle of a 512 MiB transfer every lane is
`Ready` and bytes flow again within about a second of the restore, from the
durable offset (`qual_wp5_3_recovery`, four outage kinds); and copying
2,000 files into a WinFsp mount is more than twice as fast as with the
previous release on the same machine (`qual_wp5_2_local::copy_2000_small_files_into_a_mount`;
*Floors* above).

## Earlier validation records

The validation records of the previous release (October 2026: the mount
failure that blocked a server's sync, the parallel object lanes, the
startup-snapshot benchmark, the Visual Studio directory-cleanup stall) are in
the git history of this file. Their findings fed the root-cause analysis in
`docs/revamp/ROOT_CAUSES.md`; their measurements describe the previous
release, not this one.
