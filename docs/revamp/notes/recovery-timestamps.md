# Recovery with equal or backwards timestamps

Date: 2026-10-24. Part of the open mount lifecycle blocker.

The metadata-cost candidate `3f3be093...` passed 31 of 32 VM lifecycle cases,
then failed directory-mount scratch recovery after an injected flush failure.
Shutdown still exited with code 1 and released the mount within 0.108 s. Its
[failed report](../../../test-evidence/wave-6/mount-lifecycle-metadata-cache-failed-20261024.json)
is retained; this candidate was not performance-qualified.

Recovery previously discarded a non-receipted scratch whenever its modification
time was no newer than the saved clean marker. That is not proof of unchanged
bytes. A deterministic VM replay preserved 110,592 correct bytes, set only their
timestamp equal to the marker, and the old binary recovered zero bytes
([before-fix report](../../../test-evidence/wave-6/recovery-equal-timestamp-before-20261024.json)).
The original failure did not retain its pre-recovery scratch, so its precise
timestamp cannot be reconstructed. Six ordinary replays passed. The forced
timestamp replay establishes the data-loss hole independently.

Recovery now compares the materialized bytes with the immutable base before
discarding an apparently clean scratch. Different lengths or content are
re-sealed even with equal or backwards timestamps. Receipt-based exactly-once
recovery is unchanged. A regression covers both a growing file and a same-size
overwrite, and was observed failing before the fix and passing afterwards.

The VM harness now preserves the whole pre-recovery store and reports recovered
length/hash. Its matrix adds deterministic equal-timestamp recovery for both
mount types (34 cases total). The fixed release is `cf54ca85...`; all 144 Rust
tests, format, Clippy and the bounded helper self-test pass. Its expanded VM
matrix passes all 34 cases
([report](../../../test-evidence/wave-6/mount-lifecycle-recovery-sandbox-20261024.json)).
Caching and flush-before-Cleanup remain enabled. Copy still misses the target
(11.59x native); this lifecycle result does not close performance acceptance.

The live seal retry had the same timestamp shortcut after a receipt commit.
A regression reproduced a second write being dropped when its timestamp was
older than that receipt. Retries now compare the currently materialized bytes
with the committed content: different bytes get a successor event, while a
matching retry advances the scratch record without a duplicate event. Both
regressions and all 147 Rust tests, format and Clippy pass. This follow-up is
not included in `cf54ca85...` or the queued `e38f887a...` VM candidate and still
needs qualification in its own release.
