//! Private recovery root source and immutable retained reads on real SQLite/CAS.
//! No native deletion or mounted-retention qualification is inferred here.
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tkfs::{
    content::{
        Availability, ContentProvider, LocalContent, PinnedContent, ReadContext,
        write::{
            RETAINED_READ_LIMIT, RecoveryRoot, RecoveryRootsFence, WriteJournal, WriteSession,
        },
    },
    core::{Store, id},
    model::{
        content::{CHUNK_SIZE, FileContentRef},
        workspace::ByteRange,
    },
    util::Cancel,
};
use uuid::Uuid;
fn context() -> ReadContext {
    ReadContext {
        deadline: Instant::now() + Duration::from_secs(30),
        cancel: Cancel::new(),
    }
}
fn error<T>(result: anyhow::Result<T>, code: &str) {
    let err = result.err().expect("expected error");
    assert!(format!("{err:#}").contains(code), "{err:#}");
}
fn roots(journal: &WriteJournal) -> (Vec<RecoveryRoot>, RecoveryRootsFence) {
    let mut cursor = None;
    let mut out = Vec::new();
    loop {
        let page = journal.recovery_roots(cursor, 1, &context()).unwrap();
        assert!(page.examined <= 1 && page.roots.len() <= 1);
        out.extend(page.roots);
        if let Some(fence) = page.fence {
            assert!(page.next.is_none());
            return (out, fence);
        }
        cursor = page.next;
        assert!(cursor.is_some());
    }
}
struct Fixture {
    store: Store,
    native: Arc<LocalContent>,
    journal: Arc<WriteJournal>,
    current: WriteSession,
    archive: Uuid,
    base: FileContentRef,
    canonical: FileContentRef,
    expected: Vec<u8>,
    dir: tempfile::TempDir,
}
impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::initialize(dir.path(), "retained-reader", &id()).unwrap();
        store
            .db
            .execute("INSERT INTO config VALUES('fixture:content_v1','1')", [])
            .unwrap();
        let entry = store.create("file", "file").unwrap();
        let native = Arc::new(LocalContent::open(dir.path()).unwrap());
        let journal = WriteJournal::open(dir.path()).unwrap();
        let initial = journal
            .create(
                store.begin_content_save(&entry.id, &context()).unwrap(),
                native.clone(),
                &context(),
            )
            .unwrap();
        let mut expected: Vec<u8> = (0..3 * CHUNK_SIZE)
            .map(|i| ((i * 13 + 7) % 251) as u8)
            .collect();
        initial.write_at(0, &expected, &context()).unwrap();
        let cp = initial.checkpoint(&context()).unwrap().unwrap();
        let prepared = initial.prepare(&native, &cp, &context()).unwrap();
        let base = prepared.content().clone();
        store.select_content_checkpoint(&cp, &context()).unwrap();
        store.publish_content_save(&prepared, &context()).unwrap();
        let handoff = store.continue_content_save(&cp, &context()).unwrap();
        let second = initial
            .adopt_continuation(&handoff, native.clone(), &context())
            .unwrap();
        second.write_at(17, b"preserved delta", &context()).unwrap();
        expected[17..32].copy_from_slice(b"preserved delta");
        let cp2 = second.checkpoint(&context()).unwrap().unwrap();
        let prepared2 = second.prepare(&native, &cp2, &context()).unwrap();
        let canonical = prepared2.content().clone();
        store.select_content_checkpoint(&cp2, &context()).unwrap();
        store.publish_content_save(&prepared2, &context()).unwrap();
        let handoff2 = store.continue_content_save(&cp2, &context()).unwrap();
        let current = second
            .adopt_continuation(&handoff2, native.clone(), &context())
            .unwrap();
        let (all, _) = roots(&journal);
        let archive = all
            .iter()
            .find(|r| r.archive().is_some() && r.base().content == base)
            .unwrap()
            .archive()
            .unwrap();
        Self {
            store,
            native,
            journal,
            current,
            archive,
            base,
            canonical,
            expected,
            dir,
        }
    }
    fn db(&self) -> rusqlite::Connection {
        rusqlite::Connection::open(self.dir.path().join("write-sessions-v1.sqlite")).unwrap()
    }
}

#[test]
fn paged_roots_include_current_and_retained_bases_and_can_pin_after_reopen() {
    let f = Fixture::new();
    let (all, fence) = roots(&f.journal);
    assert_eq!(all.len(), 3);
    assert_eq!(all.iter().filter(|r| r.archive().is_some()).count(), 2);
    assert!(all.iter().any(|r| r.base().content == f.base));
    assert!(all.iter().any(|r| r.base().content == f.canonical));
    f.journal
        .validate_recovery_roots(&fence, &context())
        .unwrap();
    let root = all.iter().find(|r| r.archive() == Some(f.archive)).unwrap();
    assert!(!f.native.pinned_roots().1.contains(&f.base));
    let pin = f
        .journal
        .pin_native_recovery_root(root, &f.native, &context())
        .unwrap();
    assert_eq!(pin.reference(), &f.base);
    assert!(f.native.pinned_roots().1.contains(&f.base));
    let reopened = WriteJournal::open(f.dir.path()).unwrap();
    error(
        reopened.pin_native_recovery_root(root, &f.native, &context()),
        "STALE_WRITE_CHECKPOINT",
    );
    let (all, fence) = roots(&reopened);
    reopened
        .validate_recovery_roots(&fence, &context())
        .unwrap();
    assert_eq!(all.len(), 3);
    let foreign = tempfile::tempdir().unwrap();
    let wrong = LocalContent::open(foreign.path()).unwrap();
    error(
        f.journal.pin_native_recovery_root(root, &wrong, &context()),
        "CONTENT_SAVE_WRONG_STORE",
    );
    drop(pin);
    assert!(!f.native.pinned_roots().1.contains(&f.base));
}

#[test]
fn incomplete_or_changed_root_inventory_never_returns_a_valid_fence() {
    let f = Fixture::new();
    let page = f.journal.recovery_roots(None, 1, &context()).unwrap();
    assert!(page.fence.is_none());
    f.current.write_at(99, b"new", &context()).unwrap();
    error(
        f.journal.recovery_roots(page.next, 1, &context()),
        "STALE_WRITE_CHECKPOINT",
    );
    let (_, fence) = roots(&f.journal);
    f.db()
        .execute(
            "DELETE FROM retained_overlays WHERE id=?",
            [f.archive.to_string()],
        )
        .unwrap();
    error(
        f.journal.validate_recovery_roots(&fence, &context()),
        "STALE_WRITE_CHECKPOINT",
    );
    let mut cursor = None;
    let mut failed = false;
    for _ in 0..8 {
        match f.journal.recovery_roots(cursor, 1, &context()) {
            Ok(page) => {
                assert!(page.fence.is_none());
                cursor = page.next;
                assert!(cursor.is_some());
            }
            Err(err) => {
                assert!(err.to_string().contains("CORRUPT_WRITE_JOURNAL"));
                failed = true;
                break;
            }
        }
    }
    assert!(failed);
}

#[test]
fn root_cursors_and_fences_do_not_cross_journal_instances() {
    let f = Fixture::new();
    let page = f.journal.recovery_roots(None, 1, &context()).unwrap();
    let (_, fence) = roots(&f.journal);
    let other = WriteJournal::open(f.dir.path()).unwrap();
    error(
        other.recovery_roots(page.next, 1, &context()),
        "STALE_WRITE_CHECKPOINT",
    );
    error(
        other.validate_recovery_roots(&fence, &context()),
        "STALE_WRITE_CHECKPOINT",
    );
    error(
        f.journal.recovery_roots(None, 65, &context()),
        "CONTENT_BUFFER_LIMIT",
    );
    let ctx = context();
    ctx.cancel.cancel();
    error(f.journal.recovery_roots(None, 1, &ctx), "CONTENT_CANCELLED");
}

#[test]
fn retained_reader_pins_its_base_after_live_sessions_close_and_journal_reopens() {
    let f = Fixture::new();
    let Fixture {
        store,
        native,
        journal,
        current,
        archive,
        base,
        canonical: _,
        expected,
        dir,
    } = f;
    drop(current);
    drop(journal);
    assert!(!native.pinned_roots().1.contains(&base));
    let journal = WriteJournal::open(dir.path()).unwrap();
    let reader = journal
        .open_retained_checkpoint(archive, native.clone(), &context())
        .unwrap();
    assert!(native.pinned_roots().1.contains(&base));
    assert_eq!(journal.retained_reader_count(archive), 1);
    let mut cursor = None;
    loop {
        let page = journal.reclaim_orphans(cursor, &context()).unwrap();
        assert!(page.validated + page.examined + page.nodes_examined <= 256);
        assert!(!page.nodes_deferred);
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    drop(journal);
    drop(store);
    let mut bytes = vec![0; expected.len()];
    reader.read_range(0, &mut bytes, &context()).unwrap();
    assert_eq!(bytes, expected);
    drop(reader);
    assert!(!native.pinned_roots().1.contains(&base));
}

#[test]
fn retained_saved_bytes_restore_a_missing_current_canonical_root_without_new_publication() {
    let f = Fixture::new();
    let reader = f
        .journal
        .open_retained_checkpoint(f.archive, f.native.clone(), &context())
        .unwrap();
    let path = f
        .dir
        .path()
        .join("manifests-v1")
        .join(f.canonical.digest.to_string());
    std::fs::remove_file(&path).unwrap();
    error(
        f.current.read_range(0, &mut [0], &context()),
        "CONTENT_UNAVAILABLE",
    );
    let before: u64 = f
        .store
        .db
        .query_row("SELECT count(*) FROM events", [], |r| r.get(0))
        .unwrap();
    let restored = reader.restore_native(&f.native, &context()).unwrap();
    assert_eq!(restored.reference(), &f.canonical);
    assert!(path.exists());
    let mut bytes = vec![0; f.expected.len()];
    f.current.read_range(0, &mut bytes, &context()).unwrap();
    assert_eq!(bytes, f.expected);
    assert_eq!(
        f.store
            .db
            .query_row("SELECT count(*) FROM events", [], |r| r.get::<_, u64>(0))
            .unwrap(),
        before
    );
    assert!(f.current.checkpoint(&context()).unwrap().is_none());
}

struct SlowBase {
    native: Arc<LocalContent>,
    started: Arc<AtomicBool>,
    release: Arc<AtomicBool>,
}
impl ContentProvider for SlowBase {
    fn pin(&self, reference: &FileContentRef) -> anyhow::Result<PinnedContent> {
        self.native.pin(reference)
    }
    fn availability(
        &self,
        pin: &PinnedContent,
        range: ByteRange,
        ctx: &ReadContext,
    ) -> anyhow::Result<Availability> {
        self.native.availability(pin, range, ctx)
    }
    fn read_range(
        &self,
        pin: &PinnedContent,
        offset: u64,
        out: &mut [u8],
        ctx: &ReadContext,
    ) -> anyhow::Result<()> {
        self.started.store(true, Ordering::Release);
        while !self.release.load(Ordering::Acquire) {
            ctx.check()?;
            ctx.cancel.sleep(Duration::from_millis(1));
        }
        self.native.read_range(pin, offset, out, ctx)
    }
}
#[test]
fn retained_base_io_does_not_hold_the_journal_mutex_or_mix_new_owner_edits() {
    let f = Fixture::new();
    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let slow = Arc::new(SlowBase {
        native: f.native.clone(),
        started: started.clone(),
        release: release.clone(),
    });
    let reader = f
        .journal
        .open_retained_checkpoint(f.archive, slow, &context())
        .unwrap();
    let expected = f.expected[0];
    let job = std::thread::spawn(move || {
        let mut byte = [0];
        reader.read_range(0, &mut byte, &context()).unwrap();
        byte
    });
    let until = Instant::now() + Duration::from_secs(2);
    while !started.load(Ordering::Acquire) && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(started.load(Ordering::Acquire));
    let ctx = ReadContext {
        deadline: Instant::now() + Duration::from_millis(300),
        cancel: Cancel::new(),
    };
    let result = f
        .current
        .write_at(0, b"new owner", &ctx)
        .and_then(|_| f.current.checkpoint(&ctx).map(|_| ()));
    release.store(true, Ordering::Release);
    assert_eq!(job.join().unwrap(), [expected]);
    result.unwrap();
}

struct RejectPin;

#[test]
fn cancelled_retained_base_read_releases_its_ticket_and_pin() {
    let f = Fixture::new();
    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let slow = Arc::new(SlowBase {
        native: f.native.clone(),
        started: started.clone(),
        release,
    });
    let reader = f
        .journal
        .open_retained_checkpoint(f.archive, slow, &context())
        .unwrap();
    let ctx = context();
    let cancel = ctx.cancel.clone();
    let job = std::thread::spawn(move || reader.read_range(0, &mut [0], &ctx));
    let until = Instant::now() + Duration::from_secs(2);
    while !started.load(Ordering::Acquire) && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(started.load(Ordering::Acquire));
    cancel.cancel();
    error(job.join().unwrap(), "CONTENT_CANCELLED");
    assert_eq!(f.journal.retained_reader_count(f.archive), 0);
    assert!(!f.native.pinned_roots().1.contains(&f.base));
}

impl ContentProvider for RejectPin {
    fn pin(&self, _: &FileContentRef) -> anyhow::Result<PinnedContent> {
        anyhow::bail!("fixture pin failure")
    }
    fn availability(
        &self,
        _: &PinnedContent,
        _: ByteRange,
        _: &ReadContext,
    ) -> anyhow::Result<Availability> {
        unreachable!()
    }
    fn read_range(
        &self,
        _: &PinnedContent,
        _: u64,
        _: &mut [u8],
        _: &ReadContext,
    ) -> anyhow::Result<()> {
        unreachable!()
    }
}
#[test]
fn retained_reader_capacity_and_failed_pin_cleanup_are_bounded() {
    let f = Fixture::new();
    error(
        f.journal
            .open_retained_checkpoint(f.archive, Arc::new(RejectPin), &context()),
        "fixture pin failure",
    );
    assert_eq!(f.journal.retained_reader_count(f.archive), 0);
    let mut readers = Vec::new();
    for _ in 0..RETAINED_READ_LIMIT {
        readers.push(
            f.journal
                .open_retained_checkpoint(f.archive, f.native.clone(), &context())
                .unwrap(),
        );
    }
    error(
        f.journal
            .open_retained_checkpoint(f.archive, f.native.clone(), &context()),
        "CONTENT_RESOURCE_LIMIT",
    );
    assert_eq!(
        f.journal.retained_reader_count(f.archive),
        RETAINED_READ_LIMIT
    );
    readers.pop();
    readers.push(
        f.journal
            .open_retained_checkpoint(f.archive, f.native.clone(), &context())
            .unwrap(),
    );
    drop(readers);
    assert_eq!(f.journal.retained_reader_count(f.archive), 0);
}

#[test]
fn damaged_retained_record_is_not_a_readable_or_restorable_checkpoint() {
    let f = Fixture::new();
    let reader = f
        .journal
        .open_retained_checkpoint(f.archive, f.native.clone(), &context())
        .unwrap();
    f.db()
        .execute(
            "UPDATE retained_overlays SET saved=x'00' WHERE id=?",
            [f.archive.to_string()],
        )
        .unwrap();
    error(
        reader.read_range(0, &mut [0], &context()),
        "CORRUPT_WRITE_JOURNAL",
    );
    error(
        reader.restore_native(&f.native, &context()),
        "CORRUPT_WRITE_JOURNAL",
    );
    error(
        f.journal
            .open_retained_checkpoint(f.archive, f.native.clone(), &context()),
        "CORRUPT_WRITE_JOURNAL",
    );
    drop(reader);
    assert_eq!(f.journal.retained_reader_count(f.archive), 0);
}
