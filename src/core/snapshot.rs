//! Disposable startup snapshots. The semantic journal remains authoritative.
//!
//! # Format 2 (WP4.4): incremental seals
//!
//! A branch's startup snapshot is its *anchor*, the `startup_snapshots` row
//! (`branch, cursor, payload, checksum`; the table and its positional INSERT
//! are unchanged, REVAMP_PLAN §9), plus, for a large projection, sealed rows
//! in two side tables. The anchor's JSON holds the identity (repository,
//! device, branch descriptor), the cursor (the branch's newest journal
//! sequence the snapshot covers), the journal binding and the frontier, and
//! then one of two layouts:
//!
//! - **inline** (while the whole anchor stays within [`inline_limit`],
//!   1 MiB by default): the whole projection and every conflict row, as in
//!   format 1. A seal rewrites it, a cost the periodic interval (scaled by
//!   the anchor's size, [`seal_interval`]) keeps at about 4 KiB per event,
//!   and at most the limit per seal.
//! - **rows** (larger): the anchor holds only the projection's header (its
//!   current conflict list and pending list; every head register belongs to
//!   an entry, so entries and heads are rows) and a digest of the sealed
//!   rows; `snapshot_entries(branch, entity, entry, heads)` holds each
//!   entity's entry and head registers exactly as a `current_parts` row
//!   does, and `snapshot_conflicts(branch, id, payload, resolved_by)` each
//!   conflict row. A seal rewrites the small anchor and only the rows that
//!   changed since the previous seal: O(change), whatever the projection's
//!   size.
//!
//! **What changed** is recorded durably, in the writing transaction, by
//! triggers: every `current_parts` insert or update marks its entity and
//! every `conflicts` insert, update or delete marks its row in
//! `snapshot_dirty(branch, kind, key)`. A fast commit and the startup replay
//! write exactly the entities they change as `current_parts` rows; a slow
//! commit passes the entities its projection changed (its stored-projection
//! delta) to its forced seal, which also takes its header from the
//! projection it holds; a full rewrite whose change is unknown (a rebuild)
//! seals in full. A seal clears the branch's marks in its transaction, so
//! the marks are exactly the change since the anchor (a rolled-back seal or
//! commit rolls its marks back with it). Fast commits never change the
//! header (a successor adds no conflict and needs no pending event), so a
//! seal without a projection keeps the previous anchor's header.
//!
//! **Journal binding.** `journal_hash` is, as in format 1, the SHA-256 over
//! the branch's journal rows (sequence, id, payload) up to the cursor. The
//! anchor also stores the hash's resumable state (`journal_state`), so a
//! seal hashes only its own branch's rows since the previous seal instead
//! of the whole history: a range scan of the per-branch journal index
//! `journal_branch(seq, branch)` (trigger-maintained, `incremental_schema`),
//! so rows other branches added in between cost nothing. Startup recomputes
//! the hash from the journal (O(history), as before) and checks that the
//! stored state finalizes to it; a rebuild's seal recomputes it from the
//! journal ([`Changed::Rebuild`]).
//!
//! **Integrity.** The anchor's checksum covers the anchor; the rows are
//! covered by the anchor's `rows.digest`, the XOR of one SHA-256 per row
//! (order independent, so a seal updates it from the rows it rewrites). A
//! startup that resumes a rows snapshot reads and checks every row; any
//! mismatch, like a damaged anchor, a changed journal or an unknown format,
//! falls back to full reconstruction (snapshots are disposable, invariant
//! 5). A seal never fails on what the snapshot tables hold: an anchor,
//! sealed row or change mark that is unreadable (not an integer or UTF-8
//! text where a seal writes one) or inconsistent is no previous seal, and
//! the seal is full, which replaces it (`previous`, `seal_rows_incremental`).
//!
//! **Restore.** A startup stores the sealed projection as the branch's
//! stored projection, then replays the tail. A large rows snapshot is
//! stored as its header plus one `current_parts` row per entity, and only
//! the rows (and conflict rows) that differ from the sealed ones are
//! written: an open at the tip writes nothing of the projection.
//!
//! **Crash safety.** Every seal runs inside its caller's transaction (the
//! commit's savepoint, the startup replay's savepoint, a rebuild's), so the
//! anchor, the rows, the marks and the commit are atomic (invariant 4). Fault
//! points (`core::fault`, exit 86) sit after each step: `seal_entries_written`,
//! `seal_conflicts_written`, `seal_marks_cleared`, `before_startup_snapshot`,
//! `startup_snapshot_written`, and at startup `startup_snapshot_restored` and
//! `startup_suffix_applied`.
//!
//! **Downgrade.** An older binary finds a format-2 anchor incompatible and
//! reconstructs every branch at its first open (rewriting `current_state`
//! and deleting `current_parts`), then seals format 1; it never reads the
//! side tables, and their stale rows are replaced at this binary's next
//! full seal (an older anchor makes the next open reconstruct too).
//!
//! # Stored projection header (`current_meta`, CCR-WP2.INT-5)
//!
//! The stored projection is `current_state` overlaid with `current_parts`
//! (one row per changed entity). A slow commit whose event only changes
//! some entities and the projection's conflict (or pending) list used to
//! rewrite the whole `current_state`, because the lists live in it: an
//! O(projection) write under the lock for every conflict-producing event.
//! `current_meta(branch, payload)` now holds `{conflicts, pending}` when they
//! differ from `current_state`'s, so such a commit is a delta too; a full
//! rewrite deletes the row. Older binaries never read it: they reconstruct
//! (above), which rewrites `current_state` with the lists.
use super::*;
use std::{
    borrow::Cow,
    cell::Cell,
    collections::HashMap,
    sync::{Arc, OnceLock},
    time::Instant,
};

// Bump when projection/validation semantics or the sealed layout change,
// even without a SQL migration. 2: incremental seals (WP4.4).
const FORMAT: u32 = 2;

/// The shortest journal distance between two periodic seals of a branch.
pub const MIN_SEAL_INTERVAL: i64 = 128;
/// The longest: it also bounds a startup's replay after the last seal
/// (`try_resume_snapshot` refuses a longer tail).
pub const MAX_SEAL_INTERVAL: i64 = 1024;
/// Sealed payload bytes per event of interval (see [`seal_interval`]).
const SEAL_BYTES_PER_EVENT: i64 = 4096;

/// The periodic seal interval of a branch whose anchor payload has
/// `saved_bytes` bytes (CCR-WP2.4-3, applied by WP3.INT). A seal rewrites
/// the whole anchor, so the interval grows with it, `saved_bytes / 4 KiB`
/// events (each event pays for about 4 KiB of sealing), from
/// [`MIN_SEAL_INTERVAL`] (every anchor under 512 KiB) to
/// [`MAX_SEAL_INTERVAL`] (from 4 MiB). No seal yet: the minimum. A rows
/// anchor is small (its rows are sealed incrementally), so a large
/// projection seals every 128 events at O(change) each. The snapshot stays
/// disposable; a longer interval only lengthens a startup's replay.
pub fn seal_interval(saved_bytes: Option<i64>) -> i64 {
    saved_bytes.map_or(MIN_SEAL_INTERVAL, |bytes| {
        (bytes / SEAL_BYTES_PER_EVENT).clamp(MIN_SEAL_INTERVAL, MAX_SEAL_INTERVAL)
    })
}

/// The sealed cursor of a branch and its anchor payload's length, which
/// [`seal_interval`] reads (`octet_length` reads no payload). Every reader
/// of the seal cadence uses this query: the decision (`seal_due`) and
/// [`Store::seal_due_in`] (the activator's forecast).
pub const SEALED_SQL: &str =
    "SELECT cursor,octet_length(payload) FROM startup_snapshots WHERE branch=?1";

/// The default largest inline anchor (see the module docs). Beyond it a
/// projection is sealed as rows.
pub const INLINE_LIMIT: usize = 1024 * 1024;

/// A serialized entry is longer than this (its keys alone are), so a
/// projection with more than `limit / MIN_ENTRY_BYTES` entries is sealed
/// as rows without serializing it first.
const MIN_ENTRY_BYTES: usize = 100;

thread_local! {
    static INLINE_OVERRIDE: Cell<Option<usize>> = const { Cell::new(None) };
}

/// Sets the calling thread's inline limit (`None`: the process default) and
/// returns the previous override. Tests use it to seal small projections
/// as rows (`Some(0)`) or every projection inline (`Some(usize::MAX)`, the
/// WP3.INT layout).
#[doc(hidden)]
pub fn set_thread_inline_limit(limit: Option<usize>) -> Option<usize> {
    INLINE_OVERRIDE.with(|cell| cell.replace(limit))
}

/// The largest anchor sealed inline: the thread's override, else
/// `TKFS_SNAPSHOT_INLINE_LIMIT` (bytes, read once per process), else
/// [`INLINE_LIMIT`].
pub fn inline_limit() -> usize {
    static ENV: OnceLock<Option<usize>> = OnceLock::new();
    INLINE_OVERRIDE.with(Cell::get).unwrap_or_else(|| {
        ENV.get_or_init(|| {
            std::env::var("TKFS_SNAPSHOT_INLINE_LIMIT")
                .ok()
                .and_then(|value| value.trim().parse().ok())
        })
        .unwrap_or(INLINE_LIMIT)
    })
}

/// The head registers a `current_parts` (and `snapshot_entries`) row holds.
pub(super) const PART_FIELDS: [&str; 5] = ["kind", "location", "alive", "content", "basic"];

/// The `(entry, heads)` texts of `entity`'s row in `p` (which must hold the
/// entry): what `current_parts` and `snapshot_entries` store.
pub(super) fn part_texts(p: &Projection, entity: &str) -> Result<(String, String)> {
    let entry = p
        .entries
        .get(entity)
        .context("INVALID_SNAPSHOT: no entry for a sealed row")?;
    let heads: BTreeMap<_, _> = PART_FIELDS
        .into_iter()
        .filter_map(|field| {
            let key = register(entity, field);
            p.heads.get(&key).map(|v| (key, v))
        })
        .collect();
    Ok((
        serde_json::to_string(entry)?,
        serde_json::to_string(&heads)?,
    ))
}

/// The anchor as read back. Writers build the JSON text in this field
/// order (`anchor_payload`).
#[derive(Deserialize)]
struct Anchor {
    format: u32,
    repo: String,
    device: String,
    branch: Branch,
    cursor: i64,
    journal_hash: String,
    journal_state: String,
    projection: Projection,
    conflicts: Vec<Conflict>,
    frontier: Vec<String>,
    #[serde(default)]
    rows: Option<RowSet>,
}

/// The anchor without its projection and conflicts (an inline anchor's
/// bulk is skipped, not built).
#[derive(Deserialize)]
struct AnchorHead {
    format: u32,
    repo: String,
    device: String,
    branch: Branch,
    cursor: i64,
    journal_hash: String,
    journal_state: String,
    #[serde(default)]
    rows: Option<RowSet>,
}

/// A rows snapshot's sealed rows: counts, payload bytes and the XOR of the
/// rows' digests (hex).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct RowSet {
    entries: u64,
    conflicts: u64,
    bytes: u64,
    digest: String,
}

/// The order-independent digest of a set of rows.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct RowsDigest([u8; 32]);

impl RowsDigest {
    fn toggle(&mut self, digest: [u8; 32]) {
        for (a, b) in self.0.iter_mut().zip(digest) {
            *a ^= b;
        }
    }
    fn hex(&self) -> String {
        hex::encode(self.0)
    }
    fn parse(text: &str) -> Option<Self> {
        let bytes = hex::decode(text).ok()?;
        Some(Self(bytes.try_into().ok()?))
    }
}

fn digest_fields(kind: &[u8], fields: &[Option<&str>]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(kind);
    for field in fields {
        match field {
            Some(text) => {
                digest.update((text.len() as u64).to_le_bytes());
                digest.update(text.as_bytes());
            }
            None => digest.update(u64::MAX.to_le_bytes()),
        }
    }
    digest.finalize().into()
}

fn entity_digest(branch: &str, entity: &str, entry: &str, heads: &str) -> [u8; 32] {
    digest_fields(
        b"tkfs-snapshot-entity",
        &[Some(branch), Some(entity), Some(entry), Some(heads)],
    )
}

fn conflict_digest(branch: &str, id: &str, payload: &str, resolved_by: Option<&str>) -> [u8; 32] {
    digest_fields(
        b"tkfs-snapshot-conflict",
        &[Some(branch), Some(id), Some(payload), resolved_by],
    )
}

/// Resumable SHA-256 of the journal binding (FIPS 180-4). `sha2` 0.10 keeps
/// its state private (the `compress` feature is off and `Cargo.toml` is
/// frozen), so the compression function is here; it is checked against
/// `sha2` for every length up to three blocks and for random inputs.
mod journal {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    const H0: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    fn compress(state: &mut [u32; 8], block: &[u8]) {
        let mut w = [0u32; 64];
        for (word, bytes) in w.iter_mut().zip(block.chunks_exact(4)) {
            *word = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = *state;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ (!e & g);
            let t1 = h
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        for (word, value) in state.iter_mut().zip([a, b, c, d, e, f, g, h]) {
            *word = word.wrapping_add(value);
        }
    }

    /// SHA-256 whose state can be stored and resumed.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub(super) struct JournalHash {
        state: [u32; 8],
        /// Bytes after the last whole block (< 64).
        buffer: Vec<u8>,
        /// Bytes hashed so far.
        length: u64,
    }

    impl Default for JournalHash {
        fn default() -> Self {
            Self {
                state: H0,
                buffer: Vec::with_capacity(64),
                length: 0,
            }
        }
    }

    impl JournalHash {
        pub(super) fn update(&mut self, mut data: &[u8]) {
            self.length = self.length.wrapping_add(data.len() as u64);
            if !self.buffer.is_empty() {
                let take = (64 - self.buffer.len()).min(data.len());
                self.buffer.extend_from_slice(&data[..take]);
                data = &data[take..];
                if self.buffer.len() < 64 {
                    return;
                }
                let block = std::mem::take(&mut self.buffer);
                compress(&mut self.state, &block);
                self.buffer = block;
                self.buffer.clear();
            }
            let mut blocks = data.chunks_exact(64);
            for block in &mut blocks {
                compress(&mut self.state, block);
            }
            self.buffer.extend_from_slice(blocks.remainder());
        }
        /// One journal row, encoded as the format-1 journal hash did.
        pub(super) fn row(&mut self, seq: i64, id: &str, payload: &str) {
            self.update(&seq.to_le_bytes());
            for value in [id, payload] {
                self.update(&(value.len() as u64).to_le_bytes());
                self.update(value.as_bytes());
            }
        }
        /// The digest of everything hashed so far (hex); the state stays
        /// resumable.
        pub(super) fn finalize(&self) -> String {
            let mut state = self.state;
            let mut tail = self.buffer.clone();
            tail.push(0x80);
            while tail.len() % 64 != 56 {
                tail.push(0);
            }
            tail.extend_from_slice(&self.length.wrapping_mul(8).to_be_bytes());
            for block in tail.chunks_exact(64) {
                compress(&mut state, block);
            }
            let mut out = Vec::with_capacity(32);
            for word in state {
                out.extend_from_slice(&word.to_be_bytes());
            }
            hex::encode(out)
        }
        /// The state as text (hex): the 8 state words, the length, then the
        /// buffered tail.
        pub(super) fn encode(&self) -> String {
            let mut out = Vec::with_capacity(40 + self.buffer.len());
            for word in self.state {
                out.extend_from_slice(&word.to_be_bytes());
            }
            out.extend_from_slice(&self.length.to_be_bytes());
            out.extend_from_slice(&self.buffer);
            hex::encode(out)
        }
        pub(super) fn decode(text: &str) -> Option<Self> {
            let bytes = hex::decode(text).ok()?;
            if bytes.len() < 40 || bytes.len() >= 40 + 64 {
                return None;
            }
            let mut state = [0u32; 8];
            for (word, chunk) in state.iter_mut().zip(bytes[..32].chunks_exact(4)) {
                *word = u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
            }
            let length = u64::from_be_bytes(bytes[32..40].try_into().ok()?);
            let buffer = bytes[40..].to_vec();
            (length % 64 == buffer.len() as u64).then_some(Self {
                state,
                buffer,
                length,
            })
        }
    }

    #[cfg(test)]
    mod tests {
        use super::JournalHash;
        use rand::{Rng, SeedableRng, rngs::StdRng};
        use sha2::{Digest, Sha256};

        #[test]
        fn matches_sha2_for_every_length_and_split() {
            let data: Vec<u8> = (0..200u32).map(|i| (i * 37 + 11) as u8).collect();
            for len in 0..data.len() {
                let expected = hex::encode(Sha256::digest(&data[..len]));
                for split in [0, 1, len / 2, len.saturating_sub(1), len] {
                    let split = split.min(len);
                    let mut hash = JournalHash::default();
                    hash.update(&data[..split]);
                    // Resumed through its text form.
                    let mut hash = JournalHash::decode(&hash.encode()).unwrap();
                    hash.update(&data[split..len]);
                    assert_eq!(hash.finalize(), expected, "len {len} split {split}");
                }
            }
        }

        #[test]
        fn matches_sha2_for_random_writes() {
            let mut rng = StdRng::seed_from_u64(7);
            for _ in 0..200 {
                let mut reference = Sha256::new();
                let mut hash = JournalHash::default();
                for _ in 0..rng.gen_range(0..20) {
                    let len = rng.gen_range(0..300);
                    let chunk: Vec<u8> = (0..len).map(|_| rng.r#gen()).collect();
                    reference.update(&chunk);
                    hash.update(&chunk);
                    if rng.gen_bool(0.3) {
                        hash = JournalHash::decode(&hash.encode()).unwrap();
                    }
                }
                assert_eq!(hash.finalize(), hex::encode(reference.finalize()));
            }
            assert!(JournalHash::decode("zz").is_none());
            assert!(JournalHash::decode(&"00".repeat(39)).is_none());
            // A tail that disagrees with the length is refused.
            let mut text = JournalHash::default().encode();
            text.push_str("00");
            assert!(JournalHash::decode(&text).is_none());
        }
    }
}
use journal::JournalHash;

/// Current derived branch-index repair, independent of schema creation.
const JOURNAL_BRANCH_CHUNK: i64 = 10_000;
const JOURNAL_BRANCH_MARKER: &str = "repair:journal:ready";
const JOURNAL_BRANCH_CURSOR: &str = "repair:journal:cursor";

pub(super) fn repair_journal_index(db: &Connection) -> Result<()> {
    repair_journal_index_with(db, JOURNAL_BRANCH_CHUNK, &mut |_| Ok(()))
}

fn repair_journal_index_with(
    db: &Connection,
    chunk: i64,
    after_chunk: &mut dyn FnMut(i64) -> Result<()>,
) -> Result<()> {
    let value = |key: &str| -> Result<Option<String>> {
        Ok(db
            .query_row("SELECT value FROM config WHERE key=?1", [key], |r| r.get(0))
            .optional()?)
    };
    if value(JOURNAL_BRANCH_MARKER)?.as_deref() == Some("1") {
        return Ok(());
    }
    {
        let mut cursor: i64 = if let Some(cursor) = value(JOURNAL_BRANCH_CURSOR)? {
            cursor.parse().context("INVALID_STORE_METADATA")?
        } else {
            in_savepoint(db, || {
                db.execute("DELETE FROM journal_branch", [])?;
                db.execute("INSERT INTO config VALUES(?,'0')", [JOURNAL_BRANCH_CURSOR])?;
                Ok(())
            })?;
            0
        };
        ensure!(cursor >= 0, "INVALID_STORE_METADATA");
        loop {
            let upper: Option<i64> = db.query_row(
                "SELECT max(seq) FROM (SELECT seq FROM journal_sequence WHERE seq>?1 ORDER BY seq LIMIT ?2)",
                params![cursor, chunk],
                |r| r.get(0),
            )?;
            let Some(upper) = upper else { break };
            in_savepoint(db, || {
                db.execute(
                    "INSERT OR REPLACE INTO journal_branch(seq,branch) SELECT s.seq,e.branch FROM journal_sequence s JOIN events e ON e.id=s.event WHERE s.seq>?1 AND s.seq<=?2",
                    params![cursor, upper],
                )?;
                // An upsert: the cursor key is missing when an earlier build
                // crashed between creating the table and writing its keys
                // (an `UPDATE` would then record no progress, and a crash
                // would restart the build from the beginning).
                db.execute(
                    "INSERT INTO config(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    params![JOURNAL_BRANCH_CURSOR, upper.to_string()],
                )?;
                Ok(())
            })?;
            crate::util::fault::trigger("store.index.journal.page")?;
            cursor = upper;
            crate::startup::progress("indexing the journal");
            after_chunk(upper)?;
        }
    }
    in_savepoint(db, || {
        db.execute(
            "INSERT INTO config(key,value) VALUES(?1,'1') ON CONFLICT(key) DO UPDATE SET value='1'",
            [JOURNAL_BRANCH_MARKER],
        )?;
        db.execute("DELETE FROM config WHERE key=?1", [JOURNAL_BRANCH_CURSOR])?;
        Ok(())
    })
}

/// Runs `f` in its own savepoint (a transaction of its own outside one):
/// all of its writes or none.
fn in_savepoint(db: &Connection, f: impl FnOnce() -> Result<()>) -> Result<()> {
    db.execute_batch("SAVEPOINT snapshot_step")?;
    match f() {
        Ok(()) => Ok(db.execute_batch("RELEASE snapshot_step")?),
        Err(error) => {
            let _ = db.execute_batch("ROLLBACK TO snapshot_step; RELEASE snapshot_step");
            Err(error)
        }
    }
}

/// The projection tables' change counter (WP3.INT): every row written to or
/// deleted from `current_state`, `current_parts` or `current_meta`, by any
/// connection or binary, bumps `projection_version.version` in the same
/// transaction. A connection's cached view records the counter it was built
/// at (`Store::adopt_foreign_commits`): another connection's commit that
/// left the counter alone (feed staging, object registration, status writes)
/// changed none of the rows the view was loaded from, so the view stays
/// fresh instead of reloading the whole projection (O(entries)) after every
/// such commit. A side table (§9): nothing is ALTERed, and a downgraded
/// binary's writes keep it exact through the triggers.
/// The projection tables' change counter as `db` sees it now (`None` if the
/// table is missing: a handle on a store no current binary opened yet).
pub(super) fn projection_version(db: &Connection) -> Option<i64> {
    db.prepare_cached("SELECT version FROM projection_version WHERE id=1")
        .and_then(|mut statement| statement.query_row([], |r| r.get(0)))
        .ok()
}

/// The stored projection of `branch`: `current_state`, its conflict and
/// pending lists replaced by `current_meta`'s when that row exists
/// (CCR-WP2.INT-5), overlaid with the `current_parts` rows.
pub(super) fn load_projection(db: &Connection, branch: &str) -> Result<Projection> {
    let payload: String = db.query_row(
        "SELECT payload FROM current_state WHERE branch=?",
        [branch],
        |r| r.get(0),
    )?;
    let mut p: Projection = serde_json::from_str(&payload)?;
    drop(payload);
    if let Some(header) = load_header(db, branch)? {
        p.conflicts = header.conflicts;
        p.pending = header.pending;
    }
    let mut statement =
        db.prepare_cached("SELECT entry,heads FROM current_parts WHERE branch=?")?;
    let rows = statement.query_map([branch], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (entry, heads) = row?;
        let entry: Entry = serde_json::from_str(&entry)?;
        p.entries.insert(entry.id.clone(), entry);
        let heads: BTreeMap<String, Vec<Version>> = serde_json::from_str(&heads)?;
        p.heads.extend(heads);
    }
    Ok(p)
}

/// The stored projection's header (`current_meta`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Header {
    pub(super) conflicts: Vec<Conflict>,
    pub(super) pending: Vec<String>,
}

fn load_header(db: &Connection, branch: &str) -> Result<Option<Header>> {
    let payload: Option<String> = db
        .prepare_cached("SELECT payload FROM current_meta WHERE branch=?")?
        .query_row([branch], |r| r.get(0))
        .optional()?;
    payload
        .map(|payload| Ok(serde_json::from_str(&payload)?))
        .transpose()
}

/// Stores `p`'s conflict and pending lists as `branch`'s header
/// (`current_meta`), so that a commit that changes them writes only them and
/// its changed entities' `current_parts` rows (CCR-WP2.INT-5).
pub(super) fn write_header(
    db: &Connection,
    branch: &str,
    p: &Projection,
    anchors: Option<&AnchorCache>,
) -> Result<()> {
    let payload = format!(
        "{{\"conflicts\":{},\"pending\":{}}}",
        conflicts_text(anchors, branch, &p.conflicts)?,
        serde_json::to_string(&p.pending)?
    );
    debug_assert_eq!(
        payload,
        serde_json::to_string(&Header {
            conflicts: p.conflicts.clone(),
            pending: p.pending.clone(),
        })?
    );
    db.prepare_cached("INSERT INTO current_meta(branch,payload) VALUES(?,?) ON CONFLICT(branch) DO UPDATE SET payload=excluded.payload")?
        .execute(params![branch, payload])?;
    Ok(())
}

/// `serde_json::to_string(conflicts)`, from `anchors`' text of each record
/// when it holds the same record (WP5.1): a commit that changes a few of
/// thousands of records serializes those few. Without a cache, serialized.
fn conflicts_text(
    anchors: Option<&AnchorCache>,
    branch: &str,
    conflicts: &[Conflict],
) -> Result<String> {
    let Some(mut cache) = anchors.and_then(|anchors| anchors.records.try_borrow_mut().ok()) else {
        return Ok(serde_json::to_string(conflicts)?);
    };
    let old = cache.remove(branch).unwrap_or_default();
    let mut texts: HashMap<String, (Conflict, Arc<str>)> = HashMap::with_capacity(conflicts.len());
    let mut out = String::with_capacity(old.values().map(|(_, t)| t.len() + 1).sum::<usize>() + 2);
    out.push('[');
    let mut old = old;
    for (k, c) in conflicts.iter().enumerate() {
        if k > 0 {
            out.push(',');
        }
        let text = match old.remove(&c.id) {
            Some((cached, text)) if cached == *c => text,
            _ => Arc::from(serde_json::to_string(c)?),
        };
        out.push_str(&text);
        texts.insert(c.id.clone(), (c.clone(), text));
    }
    out.push(']');
    cache.insert(branch.to_owned(), texts);
    Ok(out)
}

/// `current_state` is being rewritten with the whole projection (its lists
/// included): the header row goes.
pub(super) fn clear_header(db: &Connection, branch: &str) -> Result<()> {
    db.prepare_cached("DELETE FROM current_meta WHERE branch=?")?
        .execute([branch])?;
    Ok(())
}

/// The branch's newest journal sequence (0: none). A join over the
/// branch's events: full seals and startup only; a commit passes its own
/// event's sequence.
fn cursor(db: &Connection, branch: &str) -> Result<i64> {
    Ok(db.query_row("SELECT coalesce(max(s.seq),0) FROM journal_sequence s JOIN events e ON e.id=s.event WHERE e.branch=?", [branch], |r| r.get(0))?)
}

/// The format-1 journal hash (SHA-256 over the branch's rows up to
/// `cursor`), with `sha2`: startup's check.
fn journal_hash(db: &Connection, branch: &str, cursor: i64) -> Result<String> {
    let mut digest = Sha256::new();
    let mut statement = db.prepare("SELECT s.seq,e.id,e.payload FROM journal_sequence s JOIN events e ON e.id=s.event WHERE e.branch=? AND s.seq<=? ORDER BY s.seq")?;
    let mut rows = statement.query(params![branch, cursor])?;
    while let Some(row) = rows.next()? {
        let seq: i64 = row.get(0)?;
        digest.update(seq.to_le_bytes());
        for column in [1, 2] {
            let value = row.get_ref(column)?.as_str()?;
            digest.update((value.len() as u64).to_le_bytes());
            digest.update(value.as_bytes());
        }
        crate::startup::progress("checking snapshot journal");
    }
    Ok(hex::encode(digest.finalize()))
}

/// A branch's journal rows in `(?1, ?2]` (branch `?3`), in journal order:
/// the range of `journal_branch_order`, then the journal row and the event
/// by their keys (`tests::branch_journal_reads_only_the_branchs_range`).
const BRANCH_JOURNAL_SQL: &str = "SELECT s.seq,e.id,e.payload FROM journal_branch b CROSS JOIN journal_sequence s ON s.seq=b.seq CROSS JOIN events e ON e.id=s.event WHERE b.branch=?3 AND b.seq>?1 AND b.seq<=?2 AND e.branch=?3 ORDER BY b.seq";

/// The resumable journal hash of `branch` over its rows in `(after,
/// upto]`, continuing `hash`, and how many rows it hashed. With a previous
/// seal's state this reads only the branch's own rows since that seal: a
/// range scan of `journal_branch` (`(branch, seq)`), then two point lookups
/// per row, whatever other branches wrote in between.
fn extend_journal(
    db: &Connection,
    branch: &str,
    mut hash: JournalHash,
    after: i64,
    upto: i64,
) -> Result<(JournalHash, u64)> {
    let mut statement = db.prepare_cached(BRANCH_JOURNAL_SQL)?;
    let mut rows = statement.query(params![after, upto, branch])?;
    let mut hashed = 0;
    while let Some(row) = rows.next()? {
        hash.row(
            row.get(0)?,
            row.get_ref(1)?.as_str()?,
            row.get_ref(2)?.as_str()?,
        );
        hashed += 1;
    }
    Ok((hash, hashed))
}

/// The resumable journal hash of `branch` from its first row to `cursor`
/// (full seals), read from the journal itself, and how many rows it hashed.
fn full_journal(db: &Connection, branch: &str, cursor: i64) -> Result<(JournalHash, u64)> {
    let mut hash = JournalHash::default();
    let mut statement = db.prepare("SELECT s.seq,e.id,e.payload FROM journal_sequence s JOIN events e ON e.id=s.event WHERE e.branch=? AND s.seq<=? ORDER BY s.seq")?;
    let mut rows = statement.query(params![branch, cursor])?;
    let mut hashed = 0;
    while let Some(row) = rows.next()? {
        hash.row(
            row.get(0)?,
            row.get_ref(1)?.as_str()?,
            row.get_ref(2)?.as_str()?,
        );
        hashed += 1;
        crate::startup::progress("sealing startup snapshot");
    }
    Ok((hash, hashed))
}

/// The identity a snapshot binds: repository, device and branch descriptor.
#[derive(Clone, PartialEq, Eq)]
struct Identity {
    repo: String,
    device: String,
    branch: Branch,
}

impl Identity {
    fn read(db: &Connection, branch: &str) -> Result<Self> {
        let config = |key: &str| -> Result<String> {
            Ok(db
                .prepare_cached("SELECT value FROM config WHERE key=?")?
                .query_row([key], |r| r.get(0))?)
        };
        Ok(Self {
            repo: config("repo")?,
            device: config("device")?,
            branch: db
                .prepare_cached("SELECT id,name,shared FROM branches WHERE id=?")?
                .query_row([branch], |r| {
                    Ok(Branch {
                        id: r.get(0)?,
                        name: r.get(1)?,
                        shared: r.get(2)?,
                    })
                })?,
        })
    }
}

/// The anchor's JSON, in `Anchor`'s field order. Every seal writes through
/// this one function, so equal state gives equal bytes whichever path
/// sealed it (an off-lock prepared commit or the inline commit).
#[allow(clippy::too_many_arguments)]
fn anchor_payload(
    identity: &Identity,
    cursor: i64,
    journal: &JournalHash,
    projection_json: &str,
    conflicts_json: &str,
    frontier: &[String],
    rows: Option<&RowSet>,
) -> Result<String> {
    let mut payload = String::with_capacity(projection_json.len() + conflicts_json.len() + 1024);
    payload.push_str(&format!("{{\"format\":{FORMAT},\"repo\":"));
    payload.push_str(&serde_json::to_string(&identity.repo)?);
    payload.push_str(",\"device\":");
    payload.push_str(&serde_json::to_string(&identity.device)?);
    payload.push_str(",\"branch\":");
    payload.push_str(&serde_json::to_string(&identity.branch)?);
    payload.push_str(&format!(",\"cursor\":{cursor},\"journal_hash\":"));
    payload.push_str(&serde_json::to_string(&journal.finalize())?);
    payload.push_str(",\"journal_state\":");
    payload.push_str(&serde_json::to_string(&journal.encode())?);
    payload.push_str(",\"projection\":");
    payload.push_str(projection_json);
    payload.push_str(",\"conflicts\":");
    payload.push_str(conflicts_json);
    payload.push_str(",\"frontier\":");
    payload.push_str(&serde_json::to_string(frontier)?);
    if let Some(rows) = rows {
        payload.push_str(",\"rows\":");
        payload.push_str(&serde_json::to_string(rows)?);
    }
    payload.push('}');
    Ok(payload)
}

fn read_frontier(db: &Connection, branch: &str) -> Result<Vec<String>> {
    Ok(db
        .prepare_cached("SELECT event FROM event_frontier WHERE branch=? ORDER BY event")?
        .query_map([branch], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?)
}

/// Every conflict row of `branch`, with `resolved_by` from its column: an
/// inline anchor's `conflicts`.
fn read_conflicts(db: &Connection, branch: &str) -> Result<Vec<Conflict>> {
    let mut conflicts = Vec::new();
    let mut statement =
        db.prepare_cached("SELECT payload,resolved_by FROM conflicts WHERE branch=? ORDER BY id")?;
    let mut rows = statement.query([branch])?;
    while let Some(row) = rows.next()? {
        let mut conflict: Conflict = serde_json::from_str(row.get_ref(0)?.as_str()?)?;
        conflict.resolved_by = row.get(1)?;
        conflicts.push(conflict);
    }
    Ok(conflicts)
}

/// A projection without its entries and heads (a rows anchor's header):
/// the JSON of `Projection { conflicts, pending, .. }`, serialized from the
/// borrowed lists (WP5.1: no copy of the conflict list per seal).
fn header_json(conflicts: &[Conflict], pending: &[String]) -> Result<String> {
    header_json_cached(None, "", conflicts, pending)
}

/// `header_json`, with the records' text from `anchors` (`conflicts_text`).
fn header_json_cached(
    anchors: Option<&AnchorCache>,
    branch: &str,
    conflicts: &[Conflict],
    pending: &[String],
) -> Result<String> {
    #[derive(Serialize)]
    struct HeaderRef<'h> {
        entries: BTreeMap<String, Entry>,
        heads: BTreeMap<String, Vec<Version>>,
        conflicts: &'h [Conflict],
        pending: &'h [String],
    }
    if anchors.is_none() {
        return Ok(serde_json::to_string(&HeaderRef {
            entries: BTreeMap::new(),
            heads: BTreeMap::new(),
            conflicts,
            pending,
        })?);
    }
    let text = format!(
        "{{\"entries\":{{}},\"heads\":{{}},\"conflicts\":{},\"pending\":{}}}",
        conflicts_text(anchors, branch, conflicts)?,
        serde_json::to_string(pending)?
    );
    debug_assert_eq!(
        text,
        serde_json::to_string(&HeaderRef {
            entries: BTreeMap::new(),
            heads: BTreeMap::new(),
            conflicts,
            pending,
        })?
    );
    Ok(text)
}

/// The header (`projection` field) of a rows anchor's JSON, as text: the
/// value between the anchor's `"projection"` key and its `"conflicts":[]`
/// (`anchor_payload`'s field order; a JSON string never holds an unescaped
/// quote, so neither marker can occur inside a value).
fn header_text(payload: &str) -> Option<&str> {
    const START: &str = ",\"projection\":";
    const END: &str = ",\"conflicts\":[],\"frontier\":";
    let start = payload.find(START)? + START.len();
    let end = payload.rfind(END)?;
    (start <= end).then(|| &payload[start..end])
}

/// The last anchor a store handle wrote for each branch (WP5.1): a seal that
/// finds the stored anchor byte-equal to it trusts it (its checksum was
/// computed from these bytes, and its head was parsed from them) without
/// hashing or parsing the anchor again, and reuses its header text when the
/// projection's lists did not change since (`Seal::lists_unchanged`). A
/// rolled-back seal leaves a different stored anchor, which is then read as
/// before.
#[derive(Default)]
pub(crate) struct AnchorCache {
    anchors: std::cell::RefCell<HashMap<String, Written>>,
    /// Per branch: the serialized text of each record of the last conflict
    /// list this handle wrote, with the record (`conflicts_text`).
    records: std::cell::RefCell<HashMap<String, RecordTexts>>,
}

/// A conflict list's records by id, each with its JSON text.
type RecordTexts = HashMap<String, (Conflict, Arc<str>)>;

/// One anchor as written.
struct Written {
    payload: String,
    checksum: String,
    cursor: i64,
    identity: Identity,
    journal: JournalHash,
    rows: Option<RowSet>,
}

impl AnchorCache {
    fn put(&self, branch: &str, written: Written) {
        if let Ok(mut anchors) = self.anchors.try_borrow_mut() {
            anchors.insert(branch.to_owned(), written);
        }
    }
}

fn write_anchor(db: &Connection, branch: &str, cursor: i64, payload: &str) -> Result<String> {
    fault("before_startup_snapshot");
    let checksum = hash(payload.as_bytes());
    db.prepare_cached("INSERT INTO startup_snapshots VALUES(?,?,?,?) ON CONFLICT(branch) DO UPDATE SET cursor=excluded.cursor,payload=excluded.payload,checksum=excluded.checksum")?
        .execute(params![branch, cursor, payload, &checksum])?;
    fault("startup_snapshot_written");
    Ok(checksum)
}

/// Writes the anchor and remembers it in `input.anchors`.
#[allow(clippy::too_many_arguments)]
fn write_anchor_cached(
    db: &Connection,
    branch: &str,
    identity: &Identity,
    cursor: i64,
    journal: &JournalHash,
    rows: Option<&RowSet>,
    payload: String,
    input: &Seal<'_>,
) -> Result<u64> {
    let checksum = write_anchor(db, branch, cursor, &payload)?;
    let bytes = payload.len() as u64;
    if let Some(anchors) = input.anchors {
        anchors.put(
            branch,
            Written {
                payload,
                checksum,
                cursor,
                identity: identity.clone(),
                journal: journal.clone(),
                rows: rows.cloned(),
            },
        );
    }
    Ok(bytes)
}

/// The branch's previous seal, when a seal may continue from it: its
/// anchor's checksum holds, it is format 2 for this identity, its cursor is
/// at most the new one and its journal state finalizes to its hash.
struct Previous {
    cursor: i64,
    journal: JournalHash,
    /// A rows anchor: its rows and header (conflict and pending lists), as
    /// text.
    rows: Option<(RowSet, String)>,
    /// The anchor is byte for byte the last one this handle wrote.
    ours: bool,
}

/// The anchor row's `(cursor, payload, checksum)`, or `None` when a column
/// does not hold what a seal writes (an integer, UTF-8 texts): a damaged
/// anchor is no previous seal (snapshots are disposable, invariant 5).
fn anchor_columns(r: &rusqlite::Row<'_>) -> rusqlite::Result<Option<(i64, String, String)>> {
    let cursor = r.get_ref(0)?.as_i64().ok();
    let payload = r.get_ref(1)?.as_str().ok().map(str::to_owned);
    let checksum = r.get_ref(2)?.as_str().ok().map(str::to_owned);
    Ok(cursor
        .zip(payload)
        .zip(checksum)
        .map(|((c, p), s)| (c, p, s)))
}

/// The previous seal of the branch, `None` when there is none a seal may
/// continue from. Never an error for anything the snapshot tables hold:
/// any unreadable, damaged or foreign anchor is no previous seal, and the
/// seal is full (which replaces it).
fn previous(
    db: &Connection,
    identity: &Identity,
    cursor: i64,
    anchors: Option<&AnchorCache>,
) -> Result<Option<Previous>> {
    // The anchor this handle wrote last, still stored (its checksum, cursor
    // and length unchanged): continue from what it wrote, without reading
    // the payload (WP5.1). A stored anchor that differs from it in any byte
    // has another checksum (another writer's seal, a rolled-back one); one
    // damaged behind the checksum is replaced by this seal anyway.
    if let Some(anchors) = anchors
        && let Ok(written) = anchors.anchors.try_borrow()
        && let Some(written) = written.get(&identity.branch.id)
    {
        let stored: Option<(Option<i64>, Option<String>, Option<i64>)> = db
            .prepare_cached(
                "SELECT cursor,checksum,length(CAST(payload AS BLOB)) FROM startup_snapshots WHERE branch=?",
            )?
            .query_row([&identity.branch.id], |r| {
                Ok((
                    r.get_ref(0)?.as_i64().ok(),
                    r.get_ref(1)?.as_str().ok().map(str::to_owned),
                    r.get_ref(2)?.as_i64().ok(),
                ))
            })
            .optional()?;
        if let Some((Some(saved), Some(checksum), Some(length))) = stored
            && saved == written.cursor
            && checksum == written.checksum
            && usize::try_from(length).ok() == Some(written.payload.len())
        {
            if written.identity != *identity || saved > cursor {
                return Ok(None);
            }
            let rows = match &written.rows {
                None => None,
                Some(rows) => match header_text(&written.payload) {
                    Some(header) => Some((rows.clone(), header.to_owned())),
                    None => return Ok(None),
                },
            };
            return Ok(Some(Previous {
                cursor: saved,
                journal: written.journal.clone(),
                rows,
                ours: true,
            }));
        }
    }
    let row = db
        .prepare_cached("SELECT cursor,payload,checksum FROM startup_snapshots WHERE branch=?")?
        .query_row([&identity.branch.id], anchor_columns)
        .optional()?;
    let Some(Some((saved, payload, checksum))) = row else {
        return Ok(None);
    };
    // The anchor this handle wrote last, unchanged: its checksum and head
    // are known (WP5.1).
    if let Some(anchors) = anchors
        && let Ok(written) = anchors.anchors.try_borrow()
        && let Some(written) = written.get(&identity.branch.id)
        && written.payload == payload
        && written.checksum == checksum
    {
        if written.identity != *identity || written.cursor != saved || saved > cursor {
            return Ok(None);
        }
        let rows = match &written.rows {
            None => None,
            Some(rows) => match header_text(&payload) {
                Some(header) => Some((rows.clone(), header.to_owned())),
                None => return Ok(None),
            },
        };
        return Ok(Some(Previous {
            cursor: saved,
            journal: written.journal.clone(),
            rows,
            ours: true,
        }));
    }
    if hash(payload.as_bytes()) != checksum {
        return Ok(None);
    }
    let Ok(head) = serde_json::from_str::<AnchorHead>(&payload) else {
        return Ok(None);
    };
    if head.format != FORMAT
        || head.repo != identity.repo
        || head.device != identity.device
        || head.branch != identity.branch
        || head.cursor != saved
        || saved > cursor
    {
        return Ok(None);
    }
    let Some(journal) = JournalHash::decode(&head.journal_state) else {
        return Ok(None);
    };
    if journal.finalize() != head.journal_hash {
        return Ok(None);
    }
    let rows = match head.rows {
        None => None,
        Some(rows) => {
            let Ok(anchor) = serde_json::from_str::<Anchor>(&payload) else {
                return Ok(None);
            };
            if RowsDigest::parse(&rows.digest).is_none() {
                return Ok(None);
            }
            Some((
                rows,
                header_json(&anchor.projection.conflicts, &anchor.projection.pending)?,
            ))
        }
    };
    Ok(Some(Previous {
        cursor: saved,
        journal,
        rows,
        ours: false,
    }))
}

/// How a seal sealed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SealMode {
    /// The whole projection in the anchor.
    #[default]
    Inline,
    /// Every row rewritten (the first rows seal, or a change not known).
    RowsFull,
    /// Only the rows that changed since the previous seal.
    RowsIncremental,
}

/// What one seal did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct SealReport {
    pub(super) mode: SealMode,
    /// Anchor plus row bytes written.
    pub(super) bytes: u64,
    pub(super) entity_rows: u64,
    pub(super) conflict_rows: u64,
    /// Journal rows hashed for the journal binding.
    pub(super) journal_rows: u64,
    /// The seal used a projection JSON serialized off-lock.
    pub(super) used_json: bool,
    pub(super) micros: u64,
    /// Of `micros`: reading the previous seal and the journal binding (the
    /// rest writes the rows and the anchor).
    pub(super) journal_micros: u64,
}

/// Seals written on a store handle (`Store::seal_stats`): how many of each
/// layout, the bytes they wrote (anchors plus rows) and their durations. A
/// seal whose transaction rolled back counts too.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealStats {
    pub seals: u64,
    pub inline: u64,
    pub rows_full: u64,
    pub rows_incremental: u64,
    pub bytes: u64,
    pub entity_rows: u64,
    pub conflict_rows: u64,
    /// Journal rows hashed (an incremental seal hashes its branch's rows
    /// since its previous seal; a full seal the branch's whole journal).
    pub journal_rows: u64,
    pub max_micros: u64,
    pub total_micros: u64,
    /// Of `total_micros`: reading the previous seals and extending their
    /// journal bindings (telemetry: what a slow seal spent its time on).
    pub journal_micros: u64,
}

impl SealStats {
    pub(super) fn add(&mut self, report: &SealReport) {
        self.seals += 1;
        match report.mode {
            SealMode::Inline => self.inline += 1,
            SealMode::RowsFull => self.rows_full += 1,
            SealMode::RowsIncremental => self.rows_incremental += 1,
        }
        self.bytes += report.bytes;
        self.entity_rows += report.entity_rows;
        self.conflict_rows += report.conflict_rows;
        self.journal_rows += report.journal_rows;
        self.max_micros = self.max_micros.max(report.micros);
        self.total_micros += report.micros;
        self.journal_micros += report.journal_micros;
    }
}

/// What changed since the previous seal, besides the marks.
#[derive(Clone, Copy)]
pub(super) enum Changed<'a> {
    /// Only the marked entities and conflict rows (fast commits, the
    /// startup replay).
    Marked,
    /// The marked ones and these entities (a slow commit's delta).
    Plus(&'a BTreeSet<String>),
    /// Anything (a rewrite whose change is not known): a full seal.
    All,
    /// A rebuild: a full seal that trusts nothing of the previous one, its
    /// journal binding included (recomputed from the journal), so a
    /// snapshot a startup refused is replaced, never continued.
    Rebuild,
}

/// A seal's inputs.
pub(super) struct Seal<'a> {
    /// The branch's newest journal sequence (a commit's own event's); `None`:
    /// read it (O(history)).
    pub(super) cursor: Option<i64>,
    /// The branch's whole projection as stored now, if the caller holds it.
    pub(super) projection: Option<&'a Projection>,
    /// Its JSON, serialized off-lock (a prepared slow commit).
    pub(super) json: Option<&'a str>,
    pub(super) changed: Changed<'a>,
    /// The handle's anchor cache (WP5.1; `None`: read every anchor in full).
    pub(super) anchors: Option<&'a AnchorCache>,
    /// The projection's conflict and pending lists are the stored ones
    /// before this commit (a fast commit, or a slow commit whose delta left
    /// them): the previous anchor's header, when it is this handle's, is
    /// still the header.
    pub(super) lists_unchanged: bool,
}

/// Whether `branch` is sealed as rows (it has sealed rows). A hint, read
/// with one index probe: a prepared slow commit serializes its projection
/// off-lock only for an inline seal or a full rewrite.
pub(super) fn sealed_as_rows(db: &Connection, branch: &str) -> bool {
    db.prepare_cached("SELECT EXISTS(SELECT 1 FROM snapshot_entries WHERE branch=?)")
        .and_then(|mut statement| statement.query_row([branch], |r| r.get(0)))
        .unwrap_or(false)
}

/// The sealed cursor of `branch` and its anchor's length ([`SEALED_SQL`]):
/// `None` without an anchor, `Some((None, _))` when the cursor is not an
/// integer (a damaged anchor: its seal is due now, which replaces it).
fn sealed(db: &Connection, branch: &str) -> Result<Option<(Option<i64>, Option<i64>)>> {
    Ok(db
        .prepare_cached(SEALED_SQL)?
        .query_row([branch], |r| {
            Ok((
                r.get_ref(0)?.as_i64().ok(),
                r.get_ref(1)?.as_i64_or_null().ok().flatten(),
            ))
        })
        .optional()?)
}

/// Whether a periodic seal of `branch` is due at journal tip `tip` (the
/// cadence of [`seal_interval`], read with [`SEALED_SQL`]).
pub(super) fn seal_due(db: &Connection, branch: &str, tip: i64) -> Result<bool> {
    Ok(match sealed(db, branch)? {
        Some((Some(saved), bytes)) => tip - saved >= seal_interval(bytes),
        _ => true,
    })
}

/// Seals `branch` in the caller's transaction (module docs): incrementally
/// when its previous seal is a valid rows anchor and the change is known,
/// else in full, inline while the anchor fits [`inline_limit`]. Whatever
/// the snapshot tables hold, a seal never fails on them: a damaged anchor,
/// sealed row or change mark makes the seal full (which replaces them).
pub(super) fn seal(db: &Connection, branch: &str, input: Seal<'_>) -> Result<SealReport> {
    let started = Instant::now();
    let cursor = match input.cursor {
        Some(cursor) => cursor,
        None => cursor(db, branch)?,
    };
    let identity = Identity::read(db, branch)?;
    let previous = match input.changed {
        Changed::Rebuild => None,
        _ => previous(db, &identity, cursor, input.anchors)?,
    };
    let (journal, journal_rows) = match &previous {
        Some(previous) => extend_journal(
            db,
            branch,
            previous.journal.clone(),
            previous.cursor,
            cursor,
        )?,
        None => full_journal(db, branch, cursor)?,
    };
    let frontier = read_frontier(db, branch)?;
    let journal_micros = started.elapsed().as_micros() as u64;
    let incremental = match previous {
        Some(Previous {
            rows: Some((rows, header)),
            ours,
            ..
        }) if matches!(input.changed, Changed::Marked | Changed::Plus(_)) => seal_rows_incremental(
            db, branch, &identity, cursor, &journal, &frontier, &input, rows, header, ours,
        )?,
        _ => None,
    };
    let mut report = match incremental {
        Some(report) => report,
        None => seal_full(db, branch, &identity, cursor, &journal, &frontier, &input)?,
    };
    report.journal_rows = journal_rows;
    report.journal_micros = journal_micros;
    report.micros = started.elapsed().as_micros() as u64;
    Ok(report)
}

/// A full seal: inline when the anchor fits, else every row.
fn seal_full(
    db: &Connection,
    branch: &str,
    identity: &Identity,
    cursor: i64,
    journal: &JournalHash,
    frontier: &[String],
    input: &Seal<'_>,
) -> Result<SealReport> {
    let projection = match input.projection {
        Some(projection) => Cow::Borrowed(projection),
        None => Cow::Owned(load_projection(db, branch)?),
    };
    ensure!(projection.pending.is_empty(), "INCOMPLETE_SNAPSHOT");
    let limit = inline_limit();
    // Rows need every head register to belong to an entry (it always does:
    // `project` makes an entry for every register it has).
    let rows_possible = projection.heads.keys().all(|key| {
        key.rsplit_once('/').is_some_and(|(entity, field)| {
            PART_FIELDS.contains(&field) && projection.entries.contains_key(entity)
        })
    });
    // A lower bound of the inline anchor's size, read without serializing
    // anything: its entries and its conflict rows (`octet_length` reads no
    // payload), so a large conflict history is not serialized only to find
    // it over the limit.
    let too_large = projection.entries.len().saturating_mul(MIN_ENTRY_BYTES) > limit || {
        let conflict_bytes: i64 = db
            .prepare_cached(
                "SELECT coalesce(sum(octet_length(payload)),0) FROM conflicts WHERE branch=?",
            )?
            .query_row([branch], |r| r.get(0))?;
        usize::try_from(conflict_bytes)
            .unwrap_or(usize::MAX)
            .saturating_add(projection.entries.len().saturating_mul(MIN_ENTRY_BYTES))
            > limit
    };
    if !(rows_possible && too_large) {
        let json = match input.json {
            Some(json) => Cow::Borrowed(json),
            None => Cow::Owned(serde_json::to_string(projection.as_ref())?),
        };
        let conflicts = serde_json::to_string(&read_conflicts(db, branch)?)?;
        let payload = anchor_payload(identity, cursor, journal, &json, &conflicts, frontier, None)?;
        if payload.len() <= limit || !rows_possible {
            // An inline anchor replaces any sealed rows.
            db.prepare_cached("DELETE FROM snapshot_entries WHERE branch=?")?
                .execute([branch])?;
            db.prepare_cached("DELETE FROM snapshot_conflicts WHERE branch=?")?
                .execute([branch])?;
            clear_marks(db, branch)?;
            let bytes =
                write_anchor_cached(db, branch, identity, cursor, journal, None, payload, input)?;
            return Ok(SealReport {
                mode: SealMode::Inline,
                bytes,
                used_json: input.json.is_some(),
                ..SealReport::default()
            });
        }
    }
    // Every row.
    db.prepare_cached("DELETE FROM snapshot_entries WHERE branch=?")?
        .execute([branch])?;
    db.prepare_cached("DELETE FROM snapshot_conflicts WHERE branch=?")?
        .execute([branch])?;
    let mut digest = RowsDigest::default();
    let mut rows = RowSet::default();
    {
        let mut insert = db.prepare_cached(
            "INSERT INTO snapshot_entries(branch,entity,entry,heads) VALUES(?,?,?,?)",
        )?;
        for entity in projection.entries.keys() {
            crate::startup::progress("sealing startup snapshot");
            let (entry, heads) = part_texts(&projection, entity)?;
            insert.execute(params![branch, entity, entry, heads])?;
            digest.toggle(entity_digest(branch, entity, &entry, &heads));
            rows.entries += 1;
            rows.bytes += (entry.len() + heads.len()) as u64;
        }
    }
    fault("seal_entries_written");
    {
        let mut select = db.prepare_cached(
            "SELECT id,payload,resolved_by FROM conflicts WHERE branch=? ORDER BY id",
        )?;
        let mut insert = db.prepare_cached(
            "INSERT INTO snapshot_conflicts(branch,id,payload,resolved_by) VALUES(?,?,?,?)",
        )?;
        let mut found = select.query([branch])?;
        while let Some(row) = found.next()? {
            let id = row.get_ref(0)?.as_str()?;
            let payload = row.get_ref(1)?.as_str()?;
            let resolved_by: Option<&str> = row.get_ref(2)?.as_str_or_null()?;
            insert.execute(params![branch, id, payload, resolved_by])?;
            digest.toggle(conflict_digest(branch, id, payload, resolved_by));
            rows.conflicts += 1;
            rows.bytes += payload.len() as u64;
        }
    }
    fault("seal_conflicts_written");
    clear_marks(db, branch)?;
    fault("seal_marks_cleared");
    rows.digest = digest.hex();
    let header = header_json(&projection.conflicts, &projection.pending)?;
    let payload = anchor_payload(
        identity,
        cursor,
        journal,
        &header,
        "[]",
        frontier,
        Some(&rows),
    )?;
    let anchor_bytes = write_anchor_cached(
        db,
        branch,
        identity,
        cursor,
        journal,
        Some(&rows),
        payload,
        input,
    )?;
    Ok(SealReport {
        mode: SealMode::RowsFull,
        bytes: anchor_bytes + rows.bytes,
        entity_rows: rows.entries,
        conflict_rows: rows.conflicts,
        ..SealReport::default()
    })
}

fn clear_marks(db: &Connection, branch: &str) -> Result<()> {
    db.prepare_cached("DELETE FROM snapshot_dirty WHERE branch=?")?
        .execute([branch])?;
    Ok(())
}

/// The marked keys of `branch` of one kind (0: entities, 1: conflicts);
/// `None` when a mark's key is not UTF-8 text (a damaged mark names no
/// known key: the seal is full).
fn marks(db: &Connection, branch: &str, kind: i64) -> Result<Option<BTreeSet<String>>> {
    let mut statement =
        db.prepare_cached("SELECT key FROM snapshot_dirty WHERE branch=? AND kind=?")?;
    let mut rows = statement.query(params![branch, kind])?;
    let mut keys = BTreeSet::new();
    while let Some(row) = rows.next()? {
        let Ok(key) = row.get_ref(0)?.as_str() else {
            return Ok(None);
        };
        keys.insert(key.to_owned());
    }
    Ok(Some(keys))
}

/// A sealed row as read back for an incremental seal: `Ok(None)` without
/// a row, `Err(())` when the row does not hold the UTF-8 texts a seal
/// writes (a damaged row: the seal is full, which replaces every row).
type SealedRow<T> = std::result::Result<Option<T>, ()>;

fn sealed_entity(
    statement: &mut rusqlite::CachedStatement<'_>,
    branch: &str,
    entity: &str,
) -> Result<SealedRow<(String, String)>> {
    let row = statement
        .query_row(params![branch, entity], |r| {
            Ok(match (r.get_ref(0)?.as_str(), r.get_ref(1)?.as_str()) {
                (Ok(entry), Ok(heads)) => Some((entry.to_owned(), heads.to_owned())),
                _ => None,
            })
        })
        .optional()?;
    Ok(match row {
        None => Ok(None),
        Some(Some(texts)) => Ok(Some(texts)),
        Some(None) => Err(()),
    })
}

fn sealed_conflict(
    statement: &mut rusqlite::CachedStatement<'_>,
    branch: &str,
    id: &str,
) -> Result<SealedRow<(String, Option<String>)>> {
    let row = statement
        .query_row(params![branch, id], |r| {
            Ok(
                match (r.get_ref(0)?.as_str(), r.get_ref(1)?.as_str_or_null()) {
                    (Ok(payload), Ok(resolved_by)) => {
                        Some((payload.to_owned(), resolved_by.map(str::to_owned)))
                    }
                    _ => None,
                },
            )
        })
        .optional()?;
    Ok(match row {
        None => Ok(None),
        Some(Some(texts)) => Ok(Some(texts)),
        Some(None) => Err(()),
    })
}

/// An incremental rows seal: the rows of the entities and conflicts that
/// changed since the previous seal, then the anchor. `None` when the
/// previous seal cannot be continued (a malformed digest, a damaged mark or
/// sealed row): the caller seals in full, which deletes and rewrites every
/// row of the branch in the same transaction, so rows this one wrote before
/// it found the damage are replaced too.
#[allow(clippy::too_many_arguments)]
fn seal_rows_incremental(
    db: &Connection,
    branch: &str,
    identity: &Identity,
    cursor: i64,
    journal: &JournalHash,
    frontier: &[String],
    input: &Seal<'_>,
    mut rows: RowSet,
    header: String,
    ours: bool,
) -> Result<Option<SealReport>> {
    let Some(mut digest) = RowsDigest::parse(&rows.digest) else {
        return Ok(None);
    };
    let Some(mut entities) = marks(db, branch, 0)? else {
        return Ok(None);
    };
    let Some(conflict_marks) = marks(db, branch, 1)? else {
        return Ok(None);
    };
    if let Changed::Plus(extra) = input.changed {
        entities.extend(extra.iter().cloned());
    }
    let (mut written_entities, mut written_conflicts, mut written_bytes) = (0u64, 0u64, 0u64);
    // A marked entity without a `current_parts` row (its row was folded
    // into `current_state` since) reads the stored projection, once.
    let mut loaded: Option<Projection> = None;
    {
        let mut old_row = db.prepare_cached(
            "SELECT entry,heads FROM snapshot_entries WHERE branch=? AND entity=?",
        )?;
        let mut part =
            db.prepare_cached("SELECT entry,heads FROM current_parts WHERE branch=? AND entity=?")?;
        let mut upsert = db.prepare_cached(
            "INSERT INTO snapshot_entries(branch,entity,entry,heads) VALUES(?,?,?,?) ON CONFLICT(branch,entity) DO UPDATE SET entry=excluded.entry,heads=excluded.heads",
        )?;
        let mut delete =
            db.prepare_cached("DELETE FROM snapshot_entries WHERE branch=? AND entity=?")?;
        for entity in &entities {
            let new: Option<(String, String)> = match input.projection {
                Some(projection) => projection
                    .entries
                    .contains_key(entity)
                    .then(|| part_texts(projection, entity))
                    .transpose()?,
                None => match part
                    .query_row(params![branch, entity], |r| Ok((r.get(0)?, r.get(1)?)))
                    .optional()?
                {
                    Some(texts) => Some(texts),
                    None => {
                        if loaded.is_none() {
                            loaded = Some(load_projection(db, branch)?);
                        }
                        let projection = loaded.as_ref().expect("loaded above");
                        projection
                            .entries
                            .contains_key(entity)
                            .then(|| part_texts(projection, entity))
                            .transpose()?
                    }
                },
            };
            let Ok(old) = sealed_entity(&mut old_row, branch, entity)? else {
                return Ok(None);
            };
            if old == new {
                continue;
            }
            if let Some((entry, heads)) = &old {
                digest.toggle(entity_digest(branch, entity, entry, heads));
                rows.entries = rows.entries.saturating_sub(1);
                rows.bytes = rows
                    .bytes
                    .saturating_sub((entry.len() + heads.len()) as u64);
            }
            match &new {
                Some((entry, heads)) => {
                    upsert.execute(params![branch, entity, entry, heads])?;
                    digest.toggle(entity_digest(branch, entity, entry, heads));
                    rows.entries += 1;
                    rows.bytes += (entry.len() + heads.len()) as u64;
                    written_bytes += (entry.len() + heads.len()) as u64;
                }
                None => {
                    delete.execute(params![branch, entity])?;
                }
            }
            written_entities += 1;
        }
    }
    fault("seal_entries_written");
    {
        let mut current =
            db.prepare_cached("SELECT payload,resolved_by FROM conflicts WHERE id=? AND branch=?")?;
        let mut old_row = db.prepare_cached(
            "SELECT payload,resolved_by FROM snapshot_conflicts WHERE branch=? AND id=?",
        )?;
        let mut upsert = db.prepare_cached(
            "INSERT INTO snapshot_conflicts(branch,id,payload,resolved_by) VALUES(?,?,?,?) ON CONFLICT(branch,id) DO UPDATE SET payload=excluded.payload,resolved_by=excluded.resolved_by",
        )?;
        let mut delete =
            db.prepare_cached("DELETE FROM snapshot_conflicts WHERE branch=? AND id=?")?;
        for id in conflict_marks {
            let new: Option<(String, Option<String>)> = current
                .query_row(params![id, branch], |r| Ok((r.get(0)?, r.get(1)?)))
                .optional()?;
            let Ok(old) = sealed_conflict(&mut old_row, branch, &id)? else {
                return Ok(None);
            };
            if old == new {
                continue;
            }
            if let Some((payload, resolved_by)) = &old {
                digest.toggle(conflict_digest(
                    branch,
                    &id,
                    payload,
                    resolved_by.as_deref(),
                ));
                rows.conflicts = rows.conflicts.saturating_sub(1);
                rows.bytes = rows.bytes.saturating_sub(payload.len() as u64);
            }
            match &new {
                Some((payload, resolved_by)) => {
                    upsert.execute(params![branch, id, payload, resolved_by])?;
                    digest.toggle(conflict_digest(
                        branch,
                        &id,
                        payload,
                        resolved_by.as_deref(),
                    ));
                    rows.conflicts += 1;
                    rows.bytes += payload.len() as u64;
                    written_bytes += payload.len() as u64;
                }
                None => {
                    delete.execute(params![branch, id])?;
                }
            }
            written_conflicts += 1;
        }
    }
    fault("seal_conflicts_written");
    clear_marks(db, branch)?;
    fault("seal_marks_cleared");
    rows.digest = digest.hex();
    // A fast commit never changes the header; a slow commit passes the
    // projection it holds, whose lists, when unchanged, are the header of
    // this handle's previous anchor (WP5.1: its text is reused).
    let header = match input.projection {
        Some(_) if ours && input.lists_unchanged => header,
        Some(projection) => header_json_cached(
            input.anchors,
            branch,
            &projection.conflicts,
            &projection.pending,
        )?,
        None => header,
    };
    let payload = anchor_payload(
        identity,
        cursor,
        journal,
        &header,
        "[]",
        frontier,
        Some(&rows),
    )?;
    let anchor_bytes = write_anchor_cached(
        db,
        branch,
        identity,
        cursor,
        journal,
        Some(&rows),
        payload,
        input,
    )?;
    Ok(Some(SealReport {
        mode: SealMode::RowsIncremental,
        bytes: anchor_bytes + written_bytes,
        entity_rows: written_entities,
        conflict_rows: written_conflicts,
        ..SealReport::default()
    }))
}

/// Seals `branch` in full from its stored projection and journal (a
/// rebuild): nothing of the previous seal is reused.
pub(super) fn persist(db: &Connection, branch: &str) -> Result<()> {
    seal(
        db,
        branch,
        Seal {
            cursor: None,
            projection: None,
            json: None,
            changed: Changed::Rebuild,
            anchors: None,
            lists_unchanged: false,
        },
    )?;
    Ok(())
}

/// A rows anchor's rows as read back and checked against its digest.
struct SealedRows {
    entries: BTreeMap<String, (Entry, BTreeMap<String, Vec<Version>>)>,
    /// Each entity row's digest (`entity_digest`), which the restore
    /// compares with the stored `current_parts` rows.
    digests: HashMap<String, [u8; 32]>,
    conflicts: Vec<(String, String, Option<String>)>,
}

/// Reads and checks a rows anchor's rows (counts, bytes, digest; every row
/// parses).
fn read_rows(db: &Connection, branch: &str, expected: &RowSet) -> Result<SealedRows> {
    let mut digest = RowsDigest::default();
    let mut found = RowSet::default();
    let mut entries = BTreeMap::new();
    let mut digests = HashMap::new();
    {
        let mut statement =
            db.prepare("SELECT entity,entry,heads FROM snapshot_entries WHERE branch=?")?;
        let mut rows = statement.query([branch])?;
        while let Some(row) = rows.next()? {
            crate::startup::progress("loading startup snapshot");
            let entity = row.get_ref(0)?.as_str()?;
            let entry = row.get_ref(1)?.as_str()?;
            let heads = row.get_ref(2)?.as_str()?;
            let row_digest = entity_digest(branch, entity, entry, heads);
            digest.toggle(row_digest);
            found.entries += 1;
            found.bytes += (entry.len() + heads.len()) as u64;
            let parsed: Entry = serde_json::from_str(entry)?;
            ensure!(
                parsed.id == entity,
                "SNAPSHOT_VERSION_OR_IDENTITY: a sealed row names another key"
            );
            let heads: BTreeMap<String, Vec<Version>> = serde_json::from_str(heads)?;
            entries.insert(entity.to_owned(), (parsed, heads));
            digests.insert(entity.to_owned(), row_digest);
        }
    }
    let mut conflicts = vec![];
    {
        let mut statement = db.prepare(
            "SELECT id,payload,resolved_by FROM snapshot_conflicts WHERE branch=? ORDER BY id",
        )?;
        let mut rows = statement.query([branch])?;
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            let payload: String = row.get(1)?;
            let resolved_by: Option<String> = row.get(2)?;
            digest.toggle(conflict_digest(
                branch,
                &id,
                &payload,
                resolved_by.as_deref(),
            ));
            found.conflicts += 1;
            found.bytes += payload.len() as u64;
            let parsed: Conflict = serde_json::from_str(&payload)?;
            ensure!(
                parsed.id == id,
                "SNAPSHOT_VERSION_OR_IDENTITY: a sealed row names another key"
            );
            conflicts.push((id, payload, resolved_by));
        }
    }
    found.digest = digest.hex();
    ensure!(
        found == *expected,
        "SNAPSHOT_CHECKSUM: the sealed rows differ from the anchor's digest"
    );
    Ok(SealedRows {
        entries,
        digests,
        conflicts,
    })
}

/// What [`Store::check_startup_snapshot`] found: the sealed state, as a
/// startup would restore it.
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotCheck {
    pub cursor: i64,
    /// Sealed as rows (else inline).
    pub rows: bool,
    /// The sealed projection.
    pub projection: Projection,
    /// The sealed conflict rows: `(payload, resolved_by)` parsed, with the
    /// payload's own `resolved_by` cleared.
    pub conflicts: BTreeMap<String, (Conflict, Option<String>)>,
    pub frontier: Vec<String>,
}

impl Store {
    /// Journal commits (of any branch) until the next periodic seal of
    /// `branch`: its seal comes with its first commit once this many commits
    /// were made, so `<= 1` means its next commit seals (CCR-WP2.3-5; the
    /// activator's seal forecast). No seal yet: 0. The cadence is
    /// [`seal_interval`] of the anchor's size (a rows anchor is small: 128).
    pub fn seal_due_in(&self, branch: &str) -> Result<i64> {
        let tip: i64 = self
            .db
            .prepare_cached("SELECT coalesce(max(seq),0) FROM journal_sequence")?
            .query_row([], |r| r.get(0))?;
        Ok(match sealed(&self.db, branch)? {
            Some((Some(saved), bytes)) => seal_interval(bytes) - (tip - saved),
            // No seal yet, or a damaged one: the next commit seals.
            _ => 0,
        })
    }

    /// Seals `branch` now, from the rows marked since its previous seal (a
    /// full seal when it has none): the startup drain's seal after fast
    /// activations.
    pub(super) fn seal_branch(&self, branch: &str) -> Result<()> {
        let report = {
            let cache = self.view_cache.borrow();
            let fresh = cache
                .as_ref()
                .filter(|view| view.branch == branch && view.changes == self.db.total_changes());
            seal(
                &self.db,
                branch,
                Seal {
                    cursor: None,
                    projection: fresh.map(|view| &view.projection),
                    json: None,
                    changed: Changed::Marked,
                    anchors: None,
                    lists_unchanged: false,
                },
            )?
        };
        self.record_seal(&report);
        Ok(())
    }

    /// Checks `branch`'s startup snapshot as a startup would (checksum,
    /// format, identity, cursor, journal hash and state, sealed rows) and
    /// returns the sealed state (tests and diagnostics; O(history)).
    #[doc(hidden)]
    pub fn check_startup_snapshot(&self, branch: &str) -> Result<SnapshotCheck> {
        let (saved, payload, checksum): (i64, String, String) = self.db.query_row(
            "SELECT cursor,payload,checksum FROM startup_snapshots WHERE branch=?",
            [branch],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        ensure!(hash(payload.as_bytes()) == checksum, "SNAPSHOT_CHECKSUM");
        let anchor: Anchor = serde_json::from_str(&payload)?;
        let identity = Identity::read(&self.db, branch)?;
        ensure!(
            anchor.format == FORMAT
                && anchor.repo == identity.repo
                && anchor.device == identity.device
                && anchor.branch == identity.branch,
            "SNAPSHOT_VERSION_OR_IDENTITY"
        );
        ensure!(
            anchor.cursor == saved && saved >= 0 && saved <= cursor(&self.db, branch)?,
            "SNAPSHOT_CURSOR"
        );
        let journal = journal_hash(&self.db, branch, saved)?;
        ensure!(anchor.journal_hash == journal, "SNAPSHOT_JOURNAL_CHANGED");
        ensure!(
            JournalHash::decode(&anchor.journal_state).map(|state| state.finalize())
                == Some(journal),
            "SNAPSHOT_JOURNAL_CHANGED: the journal state does not finalize to the journal hash"
        );
        let mut projection = anchor.projection;
        let mut conflicts = BTreeMap::new();
        let rows = match &anchor.rows {
            None => {
                for mut conflict in anchor.conflicts {
                    let resolved = conflict.resolved_by.take();
                    conflicts.insert(conflict.id.clone(), (conflict, resolved));
                }
                false
            }
            Some(rows) => {
                ensure!(
                    projection.entries.is_empty() && projection.heads.is_empty(),
                    "INVALID_SNAPSHOT: a rows anchor holds entries"
                );
                let sealed = read_rows(&self.db, branch, rows)?;
                for (entity, (entry, heads)) in sealed.entries {
                    projection.entries.insert(entity, entry);
                    projection.heads.extend(heads);
                }
                for (id, payload, resolved) in sealed.conflicts {
                    let mut conflict: Conflict = serde_json::from_str(&payload)?;
                    conflict.resolved_by = None;
                    conflicts.insert(id, (conflict, resolved));
                }
                true
            }
        };
        Ok(SnapshotCheck {
            cursor: saved,
            rows,
            projection,
            conflicts,
            frontier: anchor.frontier,
        })
    }

    pub(super) fn resume_snapshot(&mut self, branch: &Branch) -> Result<bool> {
        // Restoring the base and applying the suffix is atomic. In particular,
        // an unsupported concurrent/namespace event must leave no partial view.
        self.db.execute_batch("SAVEPOINT startup_replay")?;
        let result = self.try_resume_snapshot(branch);
        self.view_cache.borrow_mut().take();
        match result {
            Ok(true) => {
                self.db.execute_batch("RELEASE startup_replay")?;
                self.count_resume();
                Ok(true)
            }
            result => {
                self.db
                    .execute_batch("ROLLBACK TO startup_replay; RELEASE startup_replay")?;
                if let Err(error) = result {
                    eprintln!("Startup snapshot fallback for {}: {error:#}", branch.id);
                }
                Ok(false)
            }
        }
    }

    fn try_resume_snapshot(&mut self, branch: &Branch) -> Result<bool> {
        let saved: Option<(i64, String, String)> = self
            .db
            .query_row(
                "SELECT cursor,payload,checksum FROM startup_snapshots WHERE branch=?",
                [&branch.id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let Some((saved_cursor, payload, checksum)) = saved else {
            return Ok(false);
        };
        crate::startup::progress("loading startup snapshot");
        ensure!(hash(payload.as_bytes()) == checksum, "SNAPSHOT_CHECKSUM");
        // An older format (or a damaged one) is reconstructed.
        let head: AnchorHead =
            serde_json::from_str(&payload).context("SNAPSHOT_VERSION_OR_IDENTITY")?;
        ensure!(
            head.format == FORMAT
                && head.repo == self.repo
                && head.device == self.device
                && head.branch == *branch,
            "SNAPSHOT_VERSION_OR_IDENTITY"
        );
        let snapshot: Anchor = serde_json::from_str(&payload)?;
        drop(payload);
        // The cursor lies within the branch's journal: its newest row in the
        // per-branch index (one index probe; the journal hash below checks
        // every row up to the cursor against the journal itself, and a tip
        // the index were missing could only refuse the snapshot).
        let tip: i64 = self.db.query_row(
            "SELECT coalesce(max(seq),0) FROM journal_branch WHERE branch=?",
            [&branch.id],
            |r| r.get(0),
        )?;
        ensure!(
            snapshot.cursor == saved_cursor && saved_cursor >= 0 && saved_cursor <= tip,
            "SNAPSHOT_CURSOR"
        );
        let journal = journal_hash(&self.db, &branch.id, saved_cursor)?;
        ensure!(snapshot.journal_hash == journal, "SNAPSHOT_JOURNAL_CHANGED");
        ensure!(
            JournalHash::decode(&snapshot.journal_state).map(|state| state.finalize())
                == Some(journal),
            "SNAPSHOT_JOURNAL_CHANGED: the journal state does not finalize to the journal hash"
        );
        ensure!(
            snapshot.projection.pending.is_empty(),
            "INCOMPLETE_SNAPSHOT"
        );
        let tail: Vec<(i64, String)> = self.db.prepare("SELECT s.seq,e.payload FROM journal_sequence s JOIN events e ON e.id=s.event WHERE e.branch=? AND s.seq>? ORDER BY s.seq")?
            .query_map(params![branch.id, saved_cursor], |r| Ok((r.get(0)?, r.get(1)?)))?.collect::<rusqlite::Result<_>>()?;
        // Bound startup work even when an older writer never consolidated.
        if tail.len() > MAX_SEAL_INTERVAL as usize {
            return Ok(false);
        }
        let restored = self.restore_snapshot(branch, snapshot)?;
        // The restored projection is the view the replay's successors read
        // (no reload of what was just written). Without a tail nothing reads
        // it (`resume_snapshot` drops the view).
        if !tail.is_empty() {
            self.seed_view(&branch.id, restored)?;
        }
        let mut last = saved_cursor;
        for (seq, payload) in &tail {
            crate::startup::progress("replaying snapshot suffix");
            let event: Event = serde_json::from_str(payload)?;
            ensure!(
                event.repo == self.repo && event.branch == *branch,
                "SNAPSHOT_EVENT_IDENTITY"
            );
            // A cursor orders acceptance, not causality. Legacy journals with a
            // prerequisite later in acceptance order require full reconstruction.
            for parent in event
                .dependencies
                .iter()
                .chain(event.changes.iter().flat_map(|c| c.parents.iter()))
            {
                let parent_seq: Option<i64> = self
                    .db
                    .query_row(
                        "SELECT seq FROM journal_sequence WHERE event=?",
                        [parent],
                        |r| r.get(0),
                    )
                    .optional()?;
                ensure!(
                    parent_seq.is_some_and(|parent| parent < *seq),
                    "SNAPSHOT_NON_TOPOLOGICAL_SUFFIX"
                );
            }
            // Reuse only proven incremental rules. Conflicting edits, reviews
            // and directory changes that suppress descendants use full replay.
            let mut delta = self.content_successor(&event)?;
            // Namespace successor helpers consult the active branch's review
            // history. Other branches conservatively use full reconstruction.
            if delta.is_none() && event.branch.id == self.active {
                delta = self.creation_successor(&event)?;
            }
            if delta.is_none() && event.branch.id == self.active {
                delta = self.namespace_successor(&event)?;
            }
            let Some(delta) = delta else { return Ok(false) };
            let written_from = self.db.total_changes();
            for entity in delta.entries.keys() {
                let (entry, heads) = part_texts(&delta, entity)?;
                self.db.execute("INSERT INTO current_parts VALUES(?,?,?,?) ON CONFLICT(branch,entity) DO UPDATE SET entry=excluded.entry,heads=excluded.heads", params![branch.id, entity, entry, heads])?;
            }
            for parent in event
                .dependencies
                .iter()
                .chain(event.changes.iter().flat_map(|c| c.parents.iter()))
            {
                self.db.execute(
                    "DELETE FROM event_frontier WHERE branch=? AND event=?",
                    params![branch.id, parent],
                )?;
            }
            self.db.execute(
                "INSERT INTO event_frontier VALUES(?,?)",
                params![branch.id, event.id],
            )?;
            self.patch_replayed_view(&branch.id, written_from, &event, delta);
            last = *seq;
            fault("startup_suffix_applied");
        }
        if !tail.is_empty() {
            crate::startup::progress("sealing startup snapshot");
            // The replay wrote its entities' rows (marked); the header is
            // the sealed one (successors change neither list).
            let report = {
                let cache = self.view_cache.borrow();
                let fresh = cache.as_ref().filter(|view| {
                    view.branch == branch.id && view.changes == self.db.total_changes()
                });
                seal(
                    &self.db,
                    &branch.id,
                    Seal {
                        cursor: Some(last),
                        projection: fresh.map(|view| &view.projection),
                        json: None,
                        changed: Changed::Marked,
                        anchors: None,
                        lists_unchanged: false,
                    },
                )?
            };
            self.record_seal(&report);
        }
        Ok(true)
    }

    /// Restores the stored projection, conflict rows and frontier of
    /// `branch` from its (checked) snapshot and returns the projection. The
    /// sealed state is unchanged by this, so the marks its writes leave are
    /// cleared.
    fn restore_snapshot(&mut self, branch: &Branch, snapshot: Anchor) -> Result<Projection> {
        let id = branch.id.as_str();
        let projection = match &snapshot.rows {
            None => {
                self.db
                    .execute("DELETE FROM current_parts WHERE branch=?", [id])?;
                self.db.execute("INSERT INTO current_state VALUES(?,?) ON CONFLICT(branch) DO UPDATE SET payload=excluded.payload", params![id, serde_json::to_string(&snapshot.projection)?])?;
                self.db
                    .execute("DELETE FROM conflicts WHERE branch=?", [id])?;
                for c in &snapshot.conflicts {
                    self.db.execute(
                        "INSERT INTO conflicts VALUES(?,?,?,?)",
                        params![c.id, id, serde_json::to_string(c)?, c.resolved_by],
                    )?;
                }
                snapshot.projection
            }
            Some(rows) => {
                ensure!(
                    snapshot.projection.entries.is_empty() && snapshot.projection.heads.is_empty(),
                    "INVALID_SNAPSHOT: a rows anchor holds entries"
                );
                // Every row is checked before any is restored.
                let sealed = read_rows(&self.db, id, rows)?;
                let header = serde_json::to_string(&snapshot.projection)?;
                let mut projection = snapshot.projection;
                for (entity, (entry, heads)) in sealed.entries {
                    projection.entries.insert(entity, entry);
                    projection.heads.extend(heads);
                }
                if projection.entries.len() <= super::commit::FOLD_MAX_ENTRIES {
                    // A projection a slow commit would fold is stored as one
                    // `current_state` row, as an inline restore stores it.
                    self.db
                        .execute("DELETE FROM current_parts WHERE branch=?", [id])?;
                    self.db.execute("INSERT INTO current_state VALUES(?,?) ON CONFLICT(branch) DO UPDATE SET payload=excluded.payload", params![id, serde_json::to_string(&projection)?])?;
                } else {
                    // A large one is stored as the header plus one
                    // `current_parts` row per entity, equal to the sealed
                    // rows. Only what differs from them is written (the
                    // rows changed since the seal): an open at the tip
                    // rewrites nothing, instead of every row (each with its
                    // triggers) and the commit of all those pages.
                    self.db.execute("INSERT INTO current_state VALUES(?1,?2) ON CONFLICT(branch) DO UPDATE SET payload=excluded.payload WHERE current_state.payload IS NOT excluded.payload", params![id, header])?;
                    self.restore_parts(id, sealed.digests)?;
                }
                // The conflict rows, likewise: those that differ from the
                // sealed ones are replaced.
                self.db.execute("DELETE FROM conflicts WHERE branch=?1 AND NOT EXISTS(SELECT 1 FROM snapshot_conflicts s WHERE s.branch=?1 AND s.id=conflicts.id AND s.payload IS conflicts.payload AND s.resolved_by IS conflicts.resolved_by)", [id])?;
                self.db.execute("INSERT INTO conflicts(id,branch,payload,resolved_by) SELECT s.id,s.branch,s.payload,s.resolved_by FROM snapshot_conflicts s WHERE s.branch=?1 AND NOT EXISTS(SELECT 1 FROM conflicts c WHERE c.id=s.id AND c.branch=?1)", [id])?;
                projection
            }
        };
        clear_header(&self.db, id)?;
        self.db
            .execute("DELETE FROM event_frontier WHERE branch=?", [id])?;
        for event in &snapshot.frontier {
            self.db
                .execute("INSERT INTO event_frontier VALUES(?,?)", params![id, event])?;
        }
        clear_marks(&self.db, id)?;
        fault("startup_snapshot_restored");
        Ok(projection)
    }

    /// Makes `branch`'s `current_parts` rows exactly its sealed entity rows,
    /// whose digests (`entity_digest`, checked against the anchor) are
    /// `sealed`: one pass over the stored rows compares each one's digest
    /// with its sealed row's; a row that differs, is not UTF-8 text or has
    /// no sealed row is deleted, and every sealed row without an equal
    /// stored one is copied from `snapshot_entries`. An open at the tip
    /// writes nothing; after a tail, the tail's entities.
    fn restore_parts(&self, branch: &str, mut sealed: HashMap<String, [u8; 32]>) -> Result<()> {
        let mut stale = vec![];
        {
            let mut statement = self
                .db
                .prepare("SELECT rowid,entity,entry,heads FROM current_parts WHERE branch=?")?;
            let mut rows = statement.query([branch])?;
            while let Some(row) = rows.next()? {
                crate::startup::progress("loading startup snapshot");
                let rowid: i64 = row.get(0)?;
                let texts = (
                    row.get_ref(1)?.as_str(),
                    row.get_ref(2)?.as_str(),
                    row.get_ref(3)?.as_str(),
                );
                let equal = match texts {
                    (Ok(entity), Ok(entry), Ok(heads))
                        if sealed.get(entity)
                            == Some(&entity_digest(branch, entity, entry, heads)) =>
                    {
                        Some(entity)
                    }
                    _ => None,
                };
                match equal {
                    // Kept: the sealed row needs no copy.
                    Some(entity) => {
                        sealed.remove(entity);
                    }
                    None => stale.push(rowid),
                }
            }
        }
        let mut delete = self
            .db
            .prepare_cached("DELETE FROM current_parts WHERE rowid=?")?;
        for rowid in stale {
            delete.execute([rowid])?;
        }
        let mut copy = self.db.prepare_cached("INSERT INTO current_parts(branch,entity,entry,heads) SELECT branch,entity,entry,heads FROM snapshot_entries WHERE branch=? AND entity=?")?;
        for entity in sealed.keys() {
            copy.execute(params![branch, entity])?;
        }
        Ok(())
    }

    /// Makes `projection`, just restored as `branch`'s stored projection,
    /// the connection's view (fresh for the writes made so far; it never
    /// adopts other connections' commits).
    fn seed_view(&self, branch: &str, projection: Projection) -> Result<()> {
        let view = super::commit::slow_commit_view(
            branch,
            projection,
            None,
            self.db.total_changes(),
            self.data_version()?,
            !self.db.is_autocommit(),
            None,
        );
        *self.view_cache.borrow_mut() = view;
        Ok(())
    }

    /// Keeps the view the suffix's successors read in step with the rows one
    /// replayed event just wrote (WP3.INT), as a fast commit patches it: the
    /// writes move `total_changes`, so the next successor reloaded the whole
    /// projection, O(entries) per suffix event (minutes for the 1024-event
    /// suffix a large folder may have since CCR-WP2.4-3). Only a view that was
    /// fresh right before those writes (`written_from`) is patched; any other
    /// is dropped. The patched view lives only inside the startup replay
    /// (`resume_snapshot` drops it), and it never adopts other connections'
    /// commits (`projection_version: None`).
    fn patch_replayed_view(
        &self,
        branch: &str,
        written_from: u64,
        event: &Event,
        delta: Projection,
    ) {
        let mut cache = self.view_cache.borrow_mut();
        let patched = cache.as_mut().is_some_and(|view| {
            view.branch == branch
                && view.changes == written_from
                && super::commit::patch_view(view, event, delta).is_ok()
        });
        match cache.as_mut() {
            Some(view) if patched => {
                view.changes = self.db.total_changes();
                view.transactional = !self.db.is_autocommit();
                view.projection_version = None;
            }
            _ => *cache = None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// WP5.1: a seal that trusts the handle's cached anchor (no hash, no
    /// parse) and reuses its header text writes the same anchor, byte for
    /// byte, as a seal that reads the stored anchor in full and serializes
    /// the header, after fast commits and after slow commits with and
    /// without a change of the conflict list (rows layout).
    #[test]
    fn a_cached_anchor_seals_the_same_bytes() {
        let previous_limit = set_thread_inline_limit(Some(1));
        let dir = tempfile::tempdir().unwrap();
        let mut s = Store::open(dir.path(), None).unwrap();
        let file = s.create("f.txt", "file").unwrap();
        s.create("g.txt", "file").unwrap();
        let entry = s.entry(&file.id).unwrap();
        let (content, alive) = (
            entry.revisions.get("content").cloned(),
            entry.revisions.get("alive").cloned(),
        );
        // Concurrent saves: slow commits that change the conflict list.
        s.write_with_base(&file.id, content.clone(), alive.clone(), b"one")
            .unwrap();
        s.write_with_base(&file.id, content, alive, b"two").unwrap();
        let branch = s.active.clone();
        let anchor = |s: &Store| -> String {
            s.db.query_row(
                "SELECT payload FROM startup_snapshots WHERE branch=?",
                [&branch],
                |r| r.get(0),
            )
            .unwrap()
        };
        for step in 0..4 {
            match step {
                0 => {}
                1 => {
                    s.create(&format!("h{step}.txt"), "file").unwrap();
                }
                _ => {
                    let target = s.lookup("g.txt").unwrap();
                    let content = target.revisions.get("content").cloned();
                    s.write_revision(&target.id, content, format!("{step}").as_bytes())
                        .unwrap();
                }
            }
            let projection = load_projection(&s.db, &branch).unwrap();
            let cursor = cursor(&s.db, &branch).unwrap();
            let changed = BTreeSet::from([file.id.clone()]);
            let seal_with = |anchors: Option<&AnchorCache>, lists_unchanged: bool| {
                s.db.execute_batch("SAVEPOINT cached_anchor").unwrap();
                let report = seal(
                    &s.db,
                    &branch,
                    Seal {
                        cursor: Some(cursor),
                        projection: Some(&projection),
                        json: None,
                        changed: Changed::Plus(&changed),
                        anchors,
                        lists_unchanged,
                    },
                )
                .unwrap();
                let written = anchor(&s);
                s.db.execute_batch("ROLLBACK TO cached_anchor; RELEASE cached_anchor")
                    .unwrap();
                (report.mode, written)
            };
            let plain = seal_with(None, false);
            assert_eq!(plain.0, SealMode::RowsIncremental, "step {step}");
            // Prime the cache with the stored anchor as this handle's own.
            let anchors = AnchorCache::default();
            let stored: (i64, String, String) =
                s.db.query_row(
                    "SELECT cursor,payload,checksum FROM startup_snapshots WHERE branch=?",
                    [&branch],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .unwrap();
            let head: AnchorHead = serde_json::from_str(&stored.1).unwrap();
            anchors.put(
                &branch,
                Written {
                    payload: stored.1.clone(),
                    checksum: stored.2.clone(),
                    cursor: stored.0,
                    identity: Identity::read(&s.db, &branch).unwrap(),
                    journal: JournalHash::decode(&head.journal_state).unwrap(),
                    rows: head.rows.clone(),
                },
            );
            assert_eq!(
                seal_with(Some(&anchors), true),
                plain,
                "step {step}: cached, reused header"
            );
            assert_eq!(
                seal_with(Some(&anchors), false),
                plain,
                "step {step}: cached"
            );
        }
        set_thread_inline_limit(previous_limit);
    }

    /// WP5.1 (the WP4.INT review minor): a `journal_branch` build that starts
    /// without its cursor key (an earlier build crashed between creating the
    /// table and writing its keys) records its progress from the first
    /// chunk, so a crash after a chunk resumes after it instead of from the
    /// start; the resumed build completes the index row for row.
    #[test]
    fn a_journal_index_build_without_its_cursor_resumes_after_a_crash() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Store::open(dir.path(), None).unwrap();
        for i in 0..25 {
            s.create(&format!("f{i:03}"), "file").unwrap();
        }
        let db = &s.db;
        let indexed = |db: &Connection| -> Vec<(i64, String)> {
            db.prepare("SELECT seq,branch FROM journal_branch ORDER BY seq")
                .unwrap()
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap()
        };
        let expected = indexed(db);
        assert!(expected.len() >= 25);
        let cursor = |db: &Connection| -> Option<String> {
            db.query_row(
                "SELECT value FROM config WHERE key=?1",
                [JOURNAL_BRANCH_CURSOR],
                |r| r.get(0),
            )
            .optional()
            .unwrap()
        };
        db.execute_batch(
            "DELETE FROM journal_branch; DELETE FROM config WHERE key LIKE 'repair:journal:%';",
        )
        .unwrap();
        // A crash after the first chunk of 10 rows.
        let mut first = None;
        let crashed = repair_journal_index_with(db, 10, &mut |upper| {
            first = Some(upper);
            anyhow::bail!("crash after a chunk")
        });
        assert!(crashed.is_err());
        let first = first.expect("one chunk ran");
        assert_eq!(
            cursor(db),
            Some(first.to_string()),
            "the first chunk's progress"
        );
        assert_eq!(indexed(db).len(), 10);
        // The resumed build starts after the recorded cursor.
        let mut chunks = vec![];
        repair_journal_index_with(db, 10, &mut |upper| {
            chunks.push(upper);
            Ok(())
        })
        .unwrap();
        assert!(
            chunks.iter().all(|&upper| upper > first),
            "{chunks:?} after {first}"
        );
        assert_eq!(indexed(db), expected);
        assert_eq!(cursor(db), None);
    }

    /// The anchor's JSON parses back into what was written, in both
    /// layouts, and the head parse skips the bulk.
    #[test]
    fn anchor_payload_round_trips() {
        let identity = Identity {
            repo: "r".into(),
            device: "d".into(),
            branch: Branch {
                id: "b".into(),
                name: "main".into(),
                shared: true,
            },
        };
        let mut journal = JournalHash::default();
        journal.row(1, "e", "{}");
        let rows = RowSet {
            entries: 2,
            conflicts: 1,
            bytes: 10,
            digest: RowsDigest::default().hex(),
        };
        for rows in [None, Some(&rows)] {
            let payload = anchor_payload(
                &identity,
                1,
                &journal,
                &serde_json::to_string(&Projection::default()).unwrap(),
                "[]",
                &["e".into()],
                rows,
            )
            .unwrap();
            let anchor: Anchor = serde_json::from_str(&payload).unwrap();
            assert_eq!(anchor.format, FORMAT);
            assert_eq!(anchor.cursor, 1);
            assert_eq!(anchor.frontier, vec!["e".to_string()]);
            assert_eq!(anchor.rows.as_ref(), rows);
            assert_eq!(anchor.journal_hash, journal.finalize());
            let head: AnchorHead = serde_json::from_str(&payload).unwrap();
            assert_eq!(head.journal_state, journal.encode());
        }
    }

    /// The order-independent row digest: toggling a row in and out restores
    /// the previous value, and the order rows are added in does not matter.
    #[test]
    fn rows_digest_is_order_independent() {
        let a = entity_digest("b", "x", "{}", "{}");
        let b = conflict_digest("b", "c", "{}", None);
        let c = conflict_digest("b", "c", "{}", Some("r"));
        assert_ne!(b, c);
        let mut one = RowsDigest::default();
        one.toggle(a);
        one.toggle(b);
        let mut two = RowsDigest::default();
        two.toggle(b);
        two.toggle(c);
        two.toggle(a);
        two.toggle(c);
        assert!(one == two);
        two.toggle(a);
        two.toggle(b);
        assert!(two == RowsDigest::default());
        assert_eq!(RowsDigest::parse(&one.hex()).map(|d| d.0), Some(one.0));
    }

    /// An incremental seal's journal read is a range of its own branch's
    /// rows in `journal_branch_order`, then key lookups: never a scan of the
    /// journal's rows in the range (every branch's), which made a seal after
    /// a large catch-up on another branch O(gap).
    #[test]
    fn branch_journal_reads_only_the_branchs_range() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(include_str!("schema.sql")).unwrap();
        // Two branches, interleaved, through the triggers.
        for i in 0..200 {
            let branch = if i % 10 == 0 { "own" } else { "other" };
            db.execute(
                "INSERT INTO events VALUES(?,?,'{}')",
                params![format!("e{i:03}"), branch],
            )
            .unwrap();
        }
        // (Lower case: `tests/contracts.rs` takes upper-case literals for
        // error codes.)
        let plan: Vec<String> = db
            .prepare(&format!("explain query plan {BRANCH_JOURNAL_SQL}"))
            .unwrap()
            .query_map(params![0, 1000, "own"], |r| r.get::<_, String>(3))
            .unwrap()
            .map(|step| step.map(|step| step.to_lowercase()))
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(plan.len(), 3, "{plan:?}");
        assert!(
            plan[0].contains("journal_branch_order") && plan[0].contains("branch=? and seq>?"),
            "{plan:?}"
        );
        assert!(plan[1].contains("primary key"), "{plan:?}");
        assert!(plan[2].contains("using index"), "{plan:?}");
        // And it reads exactly the branch's rows in the range.
        let (hash, hashed) = extend_journal(&db, "own", JournalHash::default(), 50, 150).unwrap();
        assert_eq!(hashed, 10);
        let mut expected = JournalHash::default();
        let mut statement = db
            .prepare("SELECT s.seq,e.id,e.payload FROM journal_sequence s JOIN events e ON e.id=s.event WHERE e.branch='own' AND s.seq>50 AND s.seq<=150 ORDER BY s.seq")
            .unwrap();
        let mut rows = statement.query([]).unwrap();
        while let Some(row) = rows.next().unwrap() {
            expected.row(
                row.get(0).unwrap(),
                row.get_ref(1).unwrap().as_str().unwrap(),
                row.get_ref(2).unwrap().as_str().unwrap(),
            );
        }
        assert_eq!(hash, expected);
    }
}
