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
//! seal hashes only the rows since the previous seal instead of the whole
//! history. Startup recomputes the hash from the journal (O(history), as
//! before) and checks that the stored state finalizes to it.
//!
//! **Integrity.** The anchor's checksum covers the anchor; the rows are
//! covered by the anchor's `rows.digest`, the XOR of one SHA-256 per row
//! (order independent, so a seal updates it from the rows it rewrites). A
//! startup that resumes a rows snapshot reads and checks every row; any
//! mismatch, like a damaged anchor, a changed journal or an unknown format,
//! falls back to full reconstruction (snapshots are disposable, invariant
//! 5).
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
use std::{borrow::Cow, cell::Cell, sync::OnceLock, time::Instant};

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
    let entry = p.entries.get(entity).context("SNAPSHOT_ENTRY_MISSING")?;
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

pub(super) fn initialize(db: &Connection) -> Result<()> {
    // Triggers also cover writes by older executables and repair tools. Sequence
    // numbers are local acceptance order, never CRDT version precedence.
    db.execute_batch("SAVEPOINT snapshot_schema;
        CREATE TABLE IF NOT EXISTS startup_snapshots(branch TEXT PRIMARY KEY,cursor INTEGER NOT NULL,payload TEXT NOT NULL,checksum TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS journal_sequence(seq INTEGER PRIMARY KEY AUTOINCREMENT,event TEXT NOT NULL UNIQUE);
        INSERT INTO journal_sequence(event) SELECT id FROM events e WHERE NOT EXISTS(SELECT 1 FROM journal_sequence s WHERE s.event=e.id) ORDER BY rowid;
        CREATE TRIGGER IF NOT EXISTS snapshot_event_insert AFTER INSERT ON events BEGIN
            INSERT INTO journal_sequence(event) VALUES(NEW.id);
        END;
        CREATE TRIGGER IF NOT EXISTS snapshot_event_delete AFTER DELETE ON events BEGIN
            DELETE FROM startup_snapshots;
            DELETE FROM journal_sequence WHERE event=OLD.id;
        END;
        CREATE TRIGGER IF NOT EXISTS snapshot_event_update AFTER UPDATE ON events BEGIN
            DELETE FROM startup_snapshots;
            UPDATE journal_sequence SET event=NEW.id WHERE event=OLD.id;
        END;
        RELEASE snapshot_schema;")?;
    projection_version_schema(db)?;
    incremental_schema(db)
}

/// `schema:snapshot` stamp of the format-2 side tables.
const SCHEMA_VERSION: i64 = 2;

/// The format-2 side tables (module docs): sealed rows, change marks and
/// their triggers, and the stored projection's header. New tables only
/// (§9); idempotent; its own savepoint. A mark is inserted only when absent
/// (`WHERE NOT EXISTS`, not `INSERT OR IGNORE`: inside a trigger, the
/// conflict policy of the statement that fired it, such as an upsert's
/// abort, would override the trigger's own).
fn incremental_schema(db: &Connection) -> Result<()> {
    db.execute_batch("SAVEPOINT snapshot_rows_schema;
        CREATE TABLE IF NOT EXISTS snapshot_entries(branch TEXT NOT NULL,entity TEXT NOT NULL,entry TEXT NOT NULL,heads TEXT NOT NULL,PRIMARY KEY(branch,entity));
        CREATE TABLE IF NOT EXISTS snapshot_conflicts(branch TEXT NOT NULL,id TEXT NOT NULL,payload TEXT NOT NULL,resolved_by TEXT,PRIMARY KEY(branch,id));
        CREATE TABLE IF NOT EXISTS snapshot_dirty(branch TEXT NOT NULL,kind INTEGER NOT NULL,key TEXT NOT NULL,PRIMARY KEY(branch,kind,key));
        CREATE TABLE IF NOT EXISTS current_meta(branch TEXT PRIMARY KEY,payload TEXT NOT NULL);
        CREATE TRIGGER IF NOT EXISTS snapshot_dirty_parts_insert AFTER INSERT ON current_parts BEGIN
            INSERT INTO snapshot_dirty(branch,kind,key) SELECT NEW.branch,0,NEW.entity
                WHERE NOT EXISTS(SELECT 1 FROM snapshot_dirty WHERE branch=NEW.branch AND kind=0 AND key=NEW.entity);
        END;
        CREATE TRIGGER IF NOT EXISTS snapshot_dirty_parts_update AFTER UPDATE ON current_parts BEGIN
            INSERT INTO snapshot_dirty(branch,kind,key) SELECT NEW.branch,0,NEW.entity
                WHERE NOT EXISTS(SELECT 1 FROM snapshot_dirty WHERE branch=NEW.branch AND kind=0 AND key=NEW.entity);
        END;
        CREATE TRIGGER IF NOT EXISTS snapshot_dirty_conflicts_insert AFTER INSERT ON conflicts BEGIN
            INSERT INTO snapshot_dirty(branch,kind,key) SELECT NEW.branch,1,NEW.id
                WHERE NOT EXISTS(SELECT 1 FROM snapshot_dirty WHERE branch=NEW.branch AND kind=1 AND key=NEW.id);
        END;
        CREATE TRIGGER IF NOT EXISTS snapshot_dirty_conflicts_update AFTER UPDATE ON conflicts BEGIN
            INSERT INTO snapshot_dirty(branch,kind,key) SELECT NEW.branch,1,NEW.id
                WHERE NOT EXISTS(SELECT 1 FROM snapshot_dirty WHERE branch=NEW.branch AND kind=1 AND key=NEW.id);
        END;
        CREATE TRIGGER IF NOT EXISTS snapshot_dirty_conflicts_delete AFTER DELETE ON conflicts BEGIN
            INSERT INTO snapshot_dirty(branch,kind,key) SELECT OLD.branch,1,OLD.id
                WHERE NOT EXISTS(SELECT 1 FROM snapshot_dirty WHERE branch=OLD.branch AND kind=1 AND key=OLD.id);
        END;
        CREATE TRIGGER IF NOT EXISTS projection_version_meta_insert AFTER INSERT ON current_meta BEGIN
            UPDATE projection_version SET version=version+1 WHERE id=1;
        END;
        CREATE TRIGGER IF NOT EXISTS projection_version_meta_update AFTER UPDATE ON current_meta BEGIN
            UPDATE projection_version SET version=version+1 WHERE id=1;
        END;
        CREATE TRIGGER IF NOT EXISTS projection_version_meta_delete AFTER DELETE ON current_meta BEGIN
            UPDATE projection_version SET version=version+1 WHERE id=1;
        END;
        RELEASE snapshot_rows_schema;")?;
    db.execute(
        "INSERT INTO config(key,value) VALUES('schema:snapshot',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value WHERE CAST(config.value AS INTEGER)<?1",
        [SCHEMA_VERSION],
    )?;
    Ok(())
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
fn projection_version_schema(db: &Connection) -> Result<()> {
    db.execute_batch("SAVEPOINT projection_version_schema;
        CREATE TABLE IF NOT EXISTS projection_version(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL);
        INSERT OR IGNORE INTO projection_version(id,version) VALUES(1,0);
        CREATE TRIGGER IF NOT EXISTS projection_version_state_insert AFTER INSERT ON current_state BEGIN
            UPDATE projection_version SET version=version+1 WHERE id=1;
        END;
        CREATE TRIGGER IF NOT EXISTS projection_version_state_update AFTER UPDATE ON current_state BEGIN
            UPDATE projection_version SET version=version+1 WHERE id=1;
        END;
        CREATE TRIGGER IF NOT EXISTS projection_version_state_delete AFTER DELETE ON current_state BEGIN
            UPDATE projection_version SET version=version+1 WHERE id=1;
        END;
        CREATE TRIGGER IF NOT EXISTS projection_version_parts_insert AFTER INSERT ON current_parts BEGIN
            UPDATE projection_version SET version=version+1 WHERE id=1;
        END;
        CREATE TRIGGER IF NOT EXISTS projection_version_parts_update AFTER UPDATE ON current_parts BEGIN
            UPDATE projection_version SET version=version+1 WHERE id=1;
        END;
        CREATE TRIGGER IF NOT EXISTS projection_version_parts_delete AFTER DELETE ON current_parts BEGIN
            UPDATE projection_version SET version=version+1 WHERE id=1;
        END;
        RELEASE projection_version_schema;")?;
    Ok(())
}

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
pub(super) fn write_header(db: &Connection, branch: &str, p: &Projection) -> Result<()> {
    let payload = serde_json::to_string(&Header {
        conflicts: p.conflicts.clone(),
        pending: p.pending.clone(),
    })?;
    db.prepare_cached("INSERT INTO current_meta(branch,payload) VALUES(?,?) ON CONFLICT(branch) DO UPDATE SET payload=excluded.payload")?
        .execute(params![branch, payload])?;
    Ok(())
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

/// The resumable journal hash of `branch` over its rows in `(after,
/// upto]`, continuing `hash`. With a previous seal's state this reads only
/// the rows since that seal (a range scan of the journal, then one point
/// lookup per row).
fn extend_journal(
    db: &Connection,
    branch: &str,
    mut hash: JournalHash,
    after: i64,
    upto: i64,
) -> Result<JournalHash> {
    let mut statement = db.prepare_cached(
        "SELECT s.seq,e.id,e.payload FROM journal_sequence s CROSS JOIN events e ON e.id=s.event WHERE s.seq>?1 AND s.seq<=?2 AND e.branch=?3 ORDER BY s.seq",
    )?;
    let mut rows = statement.query(params![after, upto, branch])?;
    while let Some(row) = rows.next()? {
        hash.row(
            row.get(0)?,
            row.get_ref(1)?.as_str()?,
            row.get_ref(2)?.as_str()?,
        );
    }
    Ok(hash)
}

/// The resumable journal hash of `branch` from its first row to `cursor`
/// (full seals).
fn full_journal(db: &Connection, branch: &str, cursor: i64) -> Result<JournalHash> {
    let mut hash = JournalHash::default();
    let mut statement = db.prepare("SELECT s.seq,e.id,e.payload FROM journal_sequence s JOIN events e ON e.id=s.event WHERE e.branch=? AND s.seq<=? ORDER BY s.seq")?;
    let mut rows = statement.query(params![branch, cursor])?;
    while let Some(row) = rows.next()? {
        hash.row(
            row.get(0)?,
            row.get_ref(1)?.as_str()?,
            row.get_ref(2)?.as_str()?,
        );
        crate::startup::progress("sealing startup snapshot");
    }
    Ok(hash)
}

/// The identity a snapshot binds: repository, device and branch descriptor.
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

/// A projection without its entries and heads (a rows anchor's header).
fn header_json(conflicts: &[Conflict], pending: &[String]) -> Result<String> {
    Ok(serde_json::to_string(&Projection {
        conflicts: conflicts.to_vec(),
        pending: pending.to_vec(),
        ..Projection::default()
    })?)
}

fn write_anchor(db: &Connection, branch: &str, cursor: i64, payload: &str) -> Result<()> {
    fault("before_startup_snapshot");
    db.prepare_cached("INSERT INTO startup_snapshots VALUES(?,?,?,?) ON CONFLICT(branch) DO UPDATE SET cursor=excluded.cursor,payload=excluded.payload,checksum=excluded.checksum")?
        .execute(params![branch, cursor, payload, hash(payload.as_bytes())])?;
    fault("startup_snapshot_written");
    Ok(())
}

/// The branch's previous seal, when a seal may continue from it: its
/// anchor's checksum holds, it is format 2 for this identity, its cursor is
/// at most the new one and its journal state finalizes to its hash.
struct Previous {
    cursor: i64,
    journal: JournalHash,
    /// A rows anchor: its rows and header (conflict and pending lists).
    rows: Option<(RowSet, Projection)>,
}

fn previous(db: &Connection, identity: &Identity, cursor: i64) -> Result<Option<Previous>> {
    let row: Option<(i64, String, String)> = db
        .prepare_cached("SELECT cursor,payload,checksum FROM startup_snapshots WHERE branch=?")?
        .query_row([&identity.branch.id], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .optional()?;
    let Some((saved, payload, checksum)) = row else {
        return Ok(None);
    };
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
            Some((rows, anchor.projection))
        }
    };
    Ok(Some(Previous {
        cursor: saved,
        journal,
        rows,
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
    /// The seal used a projection JSON serialized off-lock.
    pub(super) used_json: bool,
    pub(super) micros: u64,
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
    pub max_micros: u64,
    pub total_micros: u64,
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
        self.max_micros = self.max_micros.max(report.micros);
        self.total_micros += report.micros;
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
    /// Anything (a rebuild, a rewrite whose change is not known).
    All,
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
}

/// Whether `branch` is sealed as rows (it has sealed rows). A hint, read
/// with one index probe: a prepared slow commit serializes its projection
/// off-lock only for an inline seal or a full rewrite.
pub(super) fn sealed_as_rows(db: &Connection, branch: &str) -> bool {
    db.prepare_cached("SELECT EXISTS(SELECT 1 FROM snapshot_entries WHERE branch=?)")
        .and_then(|mut statement| statement.query_row([branch], |r| r.get(0)))
        .unwrap_or(false)
}

/// Whether a periodic seal of `branch` is due at journal tip `tip` (the
/// cadence of [`seal_interval`], read with [`SEALED_SQL`]).
pub(super) fn seal_due(db: &Connection, branch: &str, tip: i64) -> Result<bool> {
    let saved: Option<(i64, Option<i64>)> = db
        .prepare_cached(SEALED_SQL)?
        .query_row([branch], |r| Ok((r.get(0)?, r.get(1)?)))
        .optional()?;
    Ok(saved.is_none_or(|(saved, bytes)| tip - saved >= seal_interval(bytes)))
}

/// Seals `branch` in the caller's transaction (module docs): incrementally
/// when its previous seal is a valid rows anchor and the change is known,
/// else in full, inline while the anchor fits [`inline_limit`].
pub(super) fn seal(db: &Connection, branch: &str, input: Seal<'_>) -> Result<SealReport> {
    let started = Instant::now();
    let cursor = match input.cursor {
        Some(cursor) => cursor,
        None => cursor(db, branch)?,
    };
    let identity = Identity::read(db, branch)?;
    let previous = previous(db, &identity, cursor)?;
    let journal = match &previous {
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
    let mut report = match previous {
        Some(Previous {
            rows: Some((rows, header)),
            ..
        }) if !matches!(input.changed, Changed::All) => seal_rows_incremental(
            db, branch, &identity, cursor, &journal, &frontier, &input, rows, header,
        )?,
        _ => seal_full(db, branch, &identity, cursor, &journal, &frontier, &input)?,
    };
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
    let too_large = projection.entries.len().saturating_mul(MIN_ENTRY_BYTES) > limit;
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
            write_anchor(db, branch, cursor, &payload)?;
            return Ok(SealReport {
                mode: SealMode::Inline,
                bytes: payload.len() as u64,
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
    write_anchor(db, branch, cursor, &payload)?;
    Ok(SealReport {
        mode: SealMode::RowsFull,
        bytes: payload.len() as u64 + rows.bytes,
        entity_rows: rows.entries,
        conflict_rows: rows.conflicts,
        used_json: false,
        micros: 0,
    })
}

fn clear_marks(db: &Connection, branch: &str) -> Result<()> {
    db.prepare_cached("DELETE FROM snapshot_dirty WHERE branch=?")?
        .execute([branch])?;
    Ok(())
}

/// The marked keys of `branch` of one kind (0: entities, 1: conflicts).
fn marks(db: &Connection, branch: &str, kind: i64) -> Result<BTreeSet<String>> {
    Ok(db
        .prepare_cached("SELECT key FROM snapshot_dirty WHERE branch=? AND kind=?")?
        .query_map(params![branch, kind], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?)
}

/// An incremental rows seal: the rows of the entities and conflicts that
/// changed since the previous seal, then the anchor.
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
    header: Projection,
) -> Result<SealReport> {
    let mut digest = RowsDigest::parse(&rows.digest).context("SNAPSHOT_ROWS_DIGEST")?;
    let mut entities = marks(db, branch, 0)?;
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
            let old: Option<(String, String)> = old_row
                .query_row(params![branch, entity], |r| Ok((r.get(0)?, r.get(1)?)))
                .optional()?;
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
        for id in marks(db, branch, 1)? {
            let new: Option<(String, Option<String>)> = current
                .query_row(params![id, branch], |r| Ok((r.get(0)?, r.get(1)?)))
                .optional()?;
            let old: Option<(String, Option<String>)> = old_row
                .query_row(params![branch, id], |r| Ok((r.get(0)?, r.get(1)?)))
                .optional()?;
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
    // projection it holds.
    let header = match input.projection {
        Some(projection) => header_json(&projection.conflicts, &projection.pending)?,
        None => header_json(&header.conflicts, &header.pending)?,
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
    write_anchor(db, branch, cursor, &payload)?;
    Ok(SealReport {
        mode: SealMode::RowsIncremental,
        bytes: payload.len() as u64 + written_bytes,
        entity_rows: written_entities,
        conflict_rows: written_conflicts,
        used_json: false,
        micros: 0,
    })
}

/// Seals `branch` in full from its stored projection (a rebuild).
pub(super) fn persist(db: &Connection, branch: &str) -> Result<()> {
    seal(
        db,
        branch,
        Seal {
            cursor: None,
            projection: None,
            json: None,
            changed: Changed::All,
        },
    )?;
    Ok(())
}

/// A rows anchor's rows as read back and checked against its digest.
struct SealedRows {
    entries: BTreeMap<String, (Entry, BTreeMap<String, Vec<Version>>)>,
    conflicts: Vec<(String, String, Option<String>)>,
}

/// Reads and checks a rows anchor's rows (counts, bytes, digest; every row
/// parses).
fn read_rows(db: &Connection, branch: &str, expected: &RowSet) -> Result<SealedRows> {
    let mut digest = RowsDigest::default();
    let mut found = RowSet::default();
    let mut entries = BTreeMap::new();
    {
        let mut statement =
            db.prepare("SELECT entity,entry,heads FROM snapshot_entries WHERE branch=?")?;
        let mut rows = statement.query([branch])?;
        while let Some(row) = rows.next()? {
            crate::startup::progress("loading startup snapshot");
            let entity = row.get_ref(0)?.as_str()?;
            let entry = row.get_ref(1)?.as_str()?;
            let heads = row.get_ref(2)?.as_str()?;
            digest.toggle(entity_digest(branch, entity, entry, heads));
            found.entries += 1;
            found.bytes += (entry.len() + heads.len()) as u64;
            let parsed: Entry = serde_json::from_str(entry)?;
            ensure!(parsed.id == entity, "SNAPSHOT_ROW_IDENTITY");
            let heads: BTreeMap<String, Vec<Version>> = serde_json::from_str(heads)?;
            entries.insert(entity.to_owned(), (parsed, heads));
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
            ensure!(parsed.id == id, "SNAPSHOT_ROW_IDENTITY");
            conflicts.push((id, payload, resolved_by));
        }
    }
    found.digest = digest.hex();
    ensure!(found == *expected, "SNAPSHOT_ROWS_CHANGED");
    Ok(SealedRows { entries, conflicts })
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
        let saved: Option<(i64, Option<i64>)> = self
            .db
            .prepare_cached(SEALED_SQL)?
            .query_row([branch], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()?;
        Ok(saved.map_or(0, |(saved, bytes)| seal_interval(bytes) - (tip - saved)))
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
            "SNAPSHOT_JOURNAL_STATE"
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
                    "SNAPSHOT_HEADER"
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
        ensure!(
            snapshot.cursor == saved_cursor
                && saved_cursor >= 0
                && saved_cursor <= cursor(&self.db, &branch.id)?,
            "SNAPSHOT_CURSOR"
        );
        let journal = journal_hash(&self.db, &branch.id, saved_cursor)?;
        ensure!(snapshot.journal_hash == journal, "SNAPSHOT_JOURNAL_CHANGED");
        ensure!(
            JournalHash::decode(&snapshot.journal_state).map(|state| state.finalize())
                == Some(journal),
            "SNAPSHOT_JOURNAL_STATE"
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
        // (no reload of what was just written).
        self.seed_view(&branch.id, restored)?;
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
                    "SNAPSHOT_HEADER"
                );
                // Every row is checked before any is restored.
                let sealed = read_rows(&self.db, id, rows)?;
                let header = serde_json::to_string(&snapshot.projection)?;
                let mut projection = snapshot.projection;
                for (entity, (entry, heads)) in sealed.entries {
                    projection.entries.insert(entity, entry);
                    projection.heads.extend(heads);
                }
                self.db
                    .execute("DELETE FROM current_parts WHERE branch=?", [id])?;
                if projection.entries.len() <= super::commit::FOLD_MAX_ENTRIES {
                    // A projection a slow commit would fold is stored as one
                    // `current_state` row, as an inline restore stores it.
                    self.db.execute("INSERT INTO current_state VALUES(?,?) ON CONFLICT(branch) DO UPDATE SET payload=excluded.payload", params![id, serde_json::to_string(&projection)?])?;
                } else {
                    // A large one keeps its rows: the header, then one
                    // `current_parts` row per entity (copied, not serialized).
                    self.db.execute("INSERT INTO current_state VALUES(?,?) ON CONFLICT(branch) DO UPDATE SET payload=excluded.payload", params![id, header])?;
                    self.db.execute("INSERT INTO current_parts(branch,entity,entry,heads) SELECT branch,entity,entry,heads FROM snapshot_entries WHERE branch=?", [id])?;
                }
                self.db
                    .execute("DELETE FROM conflicts WHERE branch=?", [id])?;
                self.db.execute("INSERT INTO conflicts(id,branch,payload,resolved_by) SELECT id,branch,payload,resolved_by FROM snapshot_conflicts WHERE branch=?", [id])?;
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
}
