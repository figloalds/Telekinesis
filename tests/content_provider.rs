use std::{
    collections::BTreeMap,
    fs, io,
    sync::Arc,
    time::{Duration, Instant},
};
use tkfs::{
    content::{
        Availability, ContentProvider, LocalContent, PinnedContent, ReadContext,
        SnapshotRootHandle,
        local::{ObjectKey, ObjectKind},
    },
    model::{
        content::*,
        workspace::{ByteRange, WorkspaceId, limits},
    },
    util::{Cancel, phases::Phase},
};
use uuid::Uuid;

fn context() -> ReadContext {
    ReadContext {
        deadline: Instant::now() + Duration::from_secs(120),
        cancel: Cancel::new(),
    }
}

fn setup() -> (tempfile::TempDir, LocalContent) {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalContent::open(temp.path()).unwrap();
    (temp, store)
}

fn file_bytes(size: usize) -> Vec<u8> {
    (0..size)
        .map(|i| ((i / CHUNK_SIZE as usize) * 71 + i % 251) as u8)
        .collect()
}

#[test]
fn direct_boundary_and_manifest_ranges() {
    let (_temp, store) = setup();
    for size in [0, 1, 65_536, 65_537, 195_001] {
        let data = file_bytes(size);
        let reference = store
            .import(&mut data.as_slice(), size as u64, &context())
            .unwrap();
        assert_eq!(
            reference.kind,
            if size <= CHUNK_SIZE as usize {
                ContentKind::Direct
            } else {
                ContentKind::Manifest
            }
        );
        let pin = store.pin(&reference).unwrap();
        for offset in [0, size / 2, size.saturating_sub(1), size] {
            let count = (size - offset).min(70_000);
            let mut out = vec![0; count];
            store
                .read_range(&pin, offset as u64, &mut out, &context())
                .unwrap();
            assert_eq!(out, data[offset..offset + count]);
        }
        assert!(
            store
                .read_range(&pin, size as u64, &mut [0], &context())
                .is_err()
        );
    }
}

#[test]
fn partial_hydration_and_corruption_do_not_block_unrelated_ranges() {
    let (temp, store) = setup();
    let data = file_bytes(200_000);
    let reference = store
        .import(&mut data.as_slice(), data.len() as u64, &context())
        .unwrap();
    let pin = store.pin(&reference).unwrap();
    let second = Digest::bytes(&data[65_536..131_072]);
    let path = temp.path().join("objects").join(second.to_string());
    fs::remove_file(&path).unwrap();
    let mut out = [0; 17];
    store.read_range(&pin, 11, &mut out, &context()).unwrap();
    assert_eq!(out, data[11..28]);
    assert_eq!(
        store
            .availability(
                &pin,
                ByteRange {
                    offset: 65_536,
                    len: 1
                },
                &context()
            )
            .unwrap(),
        Availability::Missing
    );
    fs::write(&path, vec![0; 65_536]).unwrap();
    assert_eq!(
        store
            .availability(
                &pin,
                ByteRange {
                    offset: 65_536,
                    len: 1
                },
                &context()
            )
            .unwrap(),
        Availability::Corrupt
    );
    assert!(
        store
            .read_range(&pin, 65_530, &mut out, &context())
            .is_err()
    );
    // Requested tail still completes while another chunk is corrupt.
    store
        .read_range(&pin, 190_000, &mut out, &context())
        .unwrap();
    assert_eq!(out, data[190_000..190_017]);
}

#[test]
fn full_overwrite_of_missing_chunk_does_not_hydrate_partial_overwrite_does() {
    let (temp, store) = setup();
    let data = file_bytes(140_000);
    let reference = store
        .import(&mut data.as_slice(), data.len() as u64, &context())
        .unwrap();
    let pin = store.pin(&reference).unwrap();
    let first = Digest::bytes(&data[..65_536]);
    fs::remove_file(temp.path().join("objects").join(first.to_string())).unwrap();
    assert!(store.overwrite(&pin, 0, &[2; 4096], &context()).is_err());
    let changed = store
        .overwrite(&pin, 0, &vec![2; 65_536], &context())
        .unwrap();
    let changed = store.pin(&changed).unwrap();
    let mut out = [0; 100];
    store.read_range(&changed, 0, &mut out, &context()).unwrap();
    assert_eq!(out, [2; 100]);
    store
        .read_range(&changed, 130_000, &mut out, &context())
        .unwrap();
    assert_eq!(out, data[130_000..130_100]);
}

fn amplification(len: u64) {
    let (_temp, store) = setup();
    let base = store
        .import(&mut io::repeat(b'x'), len, &context())
        .unwrap();
    let pin = store.pin(&base).unwrap();
    let before = store.counters.snapshot();
    let offset = len / 2;
    let changed = store
        .overwrite(&pin, offset, &[7; 4096], &context())
        .unwrap();
    let after = store.counters.snapshot();
    let diff = |phase: Phase| after[phase as usize].bytes - before[phase as usize].bytes;
    let operations =
        |phase: Phase| after[phase as usize].operations - before[phase as usize].operations;
    assert_eq!(operations(Phase::Write), 1);
    assert_eq!(diff(Phase::Write), CHUNK_SIZE);
    assert_eq!(
        operations(Phase::ManifestWrite),
        ManifestNode::root_level(len).unwrap() as u64 + 1
    );
    assert!(
        diff(Phase::Hash) < CHUNK_SIZE * 5,
        "hashed more than bounded chunk/page work"
    );
    assert!(
        diff(Phase::Read) <= CHUNK_SIZE,
        "read beyond the modified chunk"
    );
    let changed_pin = store.pin(&changed).unwrap();
    let mut out = [0; 4096];
    store
        .read_range(&changed_pin, offset, &mut out, &context())
        .unwrap();
    assert_eq!(out, [7; 4096]);
    store
        .read_range(&pin, offset, &mut out, &context())
        .unwrap();
    assert_eq!(out, [b'x'; 4096], "mutated shared immutable base");
    let before = store.counters.snapshot();
    store
        .overwrite(&pin, CHUNK_SIZE - 2048, &[8; 4096], &context())
        .unwrap();
    let after = store.counters.snapshot();
    assert_eq!(
        after[Phase::Write as usize].operations - before[Phase::Write as usize].operations,
        2
    );
}

#[test]
fn indexed_cow_copies_only_touched_chunks_and_pages() {
    amplification(CHUNK_SIZE * 512);
}

#[test]
#[ignore = "release W6-AMPLIFICATION qualification: stream prechunked 1 GiB"]
fn one_gib_contained_and_cross_boundary_overwrite() {
    amplification(1024 * 1024 * 1024);
}

#[test]
fn snapshot_clones_retain_pins_until_last_owner_drops() {
    let (_temp, store) = setup();
    let reference = store.import(&mut &b"shared"[..], 6, &context()).unwrap();
    let pin = store.pin(&reference).unwrap();
    let mut entries = BTreeMap::new();
    entries.insert("file".to_owned(), pin);
    let snapshot = SnapshotRootHandle::new(
        WorkspaceId {
            repository: "repo".into(),
            workspace: Uuid::new_v4(),
        },
        5,
        entries,
    );
    let clone = snapshot.clone();
    drop(snapshot);
    assert_eq!(store.pinned_roots().1, vec![reference.clone()]);
    let changed = store
        .overwrite(clone.get("file").unwrap(), 0, b"other!", &context())
        .unwrap();
    assert_ne!(changed, reference);
    assert_eq!(clone.generation(), 5);
    assert_eq!(clone.get("file").unwrap().reference(), &reference);
    drop(clone);
    assert!(store.pinned_roots().1.is_empty());
}

#[test]
fn reopening_same_store_preserves_live_pin_ownership() {
    let (temp, store) = setup();
    let reference = store.import(&mut &b"shared"[..], 6, &context()).unwrap();
    let pin = store.pin(&reference).unwrap();
    drop(store);
    let reopened = LocalContent::open(temp.path()).unwrap();
    assert_eq!(reopened.pinned_roots().1, vec![reference]);
    let mut out = [0; 6];
    reopened.read_range(&pin, 0, &mut out, &context()).unwrap();
    assert_eq!(out, *b"shared");
    drop(pin);
    assert!(reopened.pinned_roots().1.is_empty());
}

#[test]
fn reachability_walk_is_bounded_pinned_and_resumable_after_missing_page() {
    let (temp, store) = setup();
    let len = CHUNK_SIZE * 257;
    let reference = store
        .import(&mut io::repeat(b'x'), len, &context())
        .unwrap();
    let pin = store.pin(&reference).unwrap();
    let mut walk = store.walk(&pin).unwrap();
    drop(pin);
    let root = store.walk_step(&mut walk, &context()).unwrap().unwrap();
    assert_eq!(root.children.len(), 2);
    assert!(!root.complete);
    let child_path = temp
        .path()
        .join("manifests-v1")
        .join(root.children[0].key.digest.to_string());
    let saved = fs::read(&child_path).unwrap();
    fs::remove_file(&child_path).unwrap();
    let pending = walk.pending_pages();
    assert!(store.walk_step(&mut walk, &context()).is_err());
    assert_eq!(walk.pending_pages(), pending);
    assert!(!walk.complete());
    assert_eq!(store.pinned_roots().1, vec![reference]);
    fs::write(&child_path, saved).unwrap();
    let before = store.counters.snapshot();
    let first = store.walk_step(&mut walk, &context()).unwrap().unwrap();
    assert_eq!(first.children.len(), FANOUT);
    assert_eq!(first.children[0].offset, 0);
    assert_eq!(first.children[255].offset, CHUNK_SIZE * 255);
    let last = store.walk_step(&mut walk, &context()).unwrap().unwrap();
    assert!(last.complete);
    assert_eq!(last.children.len(), 1);
    assert_eq!(last.children[0].offset, CHUNK_SIZE * 256);
    assert!(store.walk_step(&mut walk, &context()).unwrap().is_none());
    let after = store.counters.snapshot();
    assert_eq!(
        after[Phase::Read as usize].bytes,
        before[Phase::Read as usize].bytes,
        "reachability traversal read data chunks"
    );
    drop(walk);
    assert!(store.pinned_roots().1.is_empty());
}

#[test]
fn reachability_walk_never_admits_children_from_corrupt_parent() {
    let (temp, store) = setup();
    let reference = store
        .import(&mut io::repeat(b'x'), CHUNK_SIZE * 2, &context())
        .unwrap();
    let pin = store.pin(&reference).unwrap();
    let mut walk = store.walk(&pin).unwrap();
    let path = temp
        .path()
        .join("manifests-v1")
        .join(reference.digest.to_string());
    let mut bytes = fs::read(&path).unwrap();
    bytes[30] ^= 1;
    fs::write(path, bytes).unwrap();
    assert!(store.walk_step(&mut walk, &context()).is_err());
    assert_eq!(walk.pending_pages(), 1);
    assert!(!walk.complete());
}

#[test]
fn stalled_save_producers_leave_foreground_and_structural_verification_capacity() {
    use std::{
        io::Read,
        sync::{Barrier, Condvar},
        thread,
    };
    let (_temp, store) = setup();
    let store = Arc::new(store);
    let root = store
        .import(&mut io::repeat(1), CHUNK_SIZE * 2, &context())
        .unwrap();
    let pin = store.pin(&root).unwrap();
    let barrier = Arc::new(Barrier::new(3));
    let release = Arc::new((std::sync::Mutex::new(false), Condvar::new()));
    struct Slow {
        barrier: Arc<Barrier>,
        release: Arc<(std::sync::Mutex<bool>, Condvar)>,
        waited: bool,
    }
    impl Read for Slow {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            if !self.waited {
                self.waited = true;
                self.barrier.wait();
                let (mutex, condvar) = &*self.release;
                let mut ready = mutex.lock().unwrap();
                while !*ready {
                    ready = condvar.wait(ready).unwrap();
                }
            }
            out.fill(2);
            Ok(out.len())
        }
    }
    let mut jobs = vec![];
    for _ in 0..2 {
        let provider = store.clone();
        let barrier = barrier.clone();
        let release = release.clone();
        jobs.push(thread::spawn(move || {
            provider
                .import(
                    &mut Slow {
                        barrier,
                        release,
                        waited: false,
                    },
                    CHUNK_SIZE,
                    &context(),
                )
                .unwrap()
        }));
    }
    barrier.wait(); // both producers hold their bounded preparation buffers
    let started = Instant::now();
    let mut out = [0; 17];
    let read = store.read_range(&pin, 0, &mut out, &context());
    let elapsed = started.elapsed();
    let mut walk = store.walk(&pin).unwrap();
    let structural = store.walk_step(&mut walk, &context());
    *release.0.lock().unwrap() = true;
    release.1.notify_all();
    for job in jobs {
        job.join().unwrap();
    }
    read.unwrap();
    structural.unwrap();
    assert_eq!(out, [1; 17]);
    assert!(elapsed < Duration::from_millis(250));
    let (regular, structural, peak) = store.buffer_usage();
    assert_eq!((regular, structural), (0, false));
    assert!(peak <= 9 * CHUNK_SIZE as usize);
}

#[test]
fn cancelled_deadline_oversized_and_foreign_provider_requests_fail() {
    let (_temp, store) = setup();
    let (_other_temp, other) = setup();
    let reference = store.import(&mut &b"abc"[..], 3, &context()).unwrap();
    let pin = store.pin(&reference).unwrap();
    let cancelled = context();
    cancelled.cancel.cancel();
    assert!(
        store
            .read_range(&pin, 0, &mut [0], &cancelled)
            .unwrap_err()
            .to_string()
            .contains("CONTENT_CANCELLED")
    );
    let expired = ReadContext {
        deadline: Instant::now(),
        cancel: Cancel::new(),
    };
    assert!(
        store
            .read_range(&pin, 0, &mut [0], &expired)
            .unwrap_err()
            .to_string()
            .contains("CONTENT_DEADLINE")
    );
    assert!(other.read_range(&pin, 0, &mut [0], &context()).is_err());
    assert!(
        store
            .availability(
                &pin,
                ByteRange {
                    offset: 0,
                    len: limits::READ_BYTES as u64 + 1
                },
                &context()
            )
            .is_err()
    );
    assert!(
        store
            .install(
                ObjectKey {
                    kind: ObjectKind::Bytes,
                    digest: Digest::bytes(b"good")
                },
                b"bad",
                &context()
            )
            .is_err()
    );
}

#[test]
fn forged_child_length_is_rejected_after_parent_verification() {
    let (_temp, store) = setup();
    let child = ManifestNode {
        level: 0,
        total_len: 2,
        children: vec![ManifestChild {
            kind: ChildKind::Chunk,
            len: 2,
            digest: Digest::bytes(b"ab"),
        }],
    };
    store
        .install(
            ObjectKey {
                kind: ObjectKind::Manifest,
                digest: child.digest().unwrap(),
            },
            &child.encode().unwrap(),
            &context(),
        )
        .unwrap();
    // Valid root shape and hash, but its first child actually covers two bytes.
    let root = ManifestNode {
        level: 1,
        total_len: CHUNK_SIZE * 256 + 1,
        children: vec![
            ManifestChild {
                kind: ChildKind::Node,
                len: CHUNK_SIZE * 256,
                digest: child.digest().unwrap(),
            },
            ManifestChild {
                kind: ChildKind::Node,
                len: 1,
                digest: child.digest().unwrap(),
            },
        ],
    };
    store
        .install(
            ObjectKey {
                kind: ObjectKind::Manifest,
                digest: root.digest().unwrap(),
            },
            &root.encode().unwrap(),
            &context(),
        )
        .unwrap();
    let pin = store
        .pin(
            &FileContentRef::new(
                ContentKind::Manifest,
                root.digest().unwrap(),
                root.total_len,
            )
            .unwrap(),
        )
        .unwrap();
    assert_eq!(
        store
            .availability(&pin, ByteRange { offset: 0, len: 1 }, &context())
            .unwrap(),
        Availability::Corrupt
    );
}

/// A different provider has no CAS paths or TKFS storage dependency.
struct FakeProvider {
    id: Uuid,
    bytes: Arc<Vec<u8>>,
    delay: Duration,
    fail: bool,
}
impl ContentProvider for FakeProvider {
    fn pin(&self, reference: &FileContentRef) -> anyhow::Result<PinnedContent> {
        anyhow::ensure!(
            reference.digest == Digest::bytes(&self.bytes),
            "WRONG_FAKE_VERSION"
        );
        PinnedContent::new(reference.clone(), self.id, self.bytes.clone())
    }
    fn availability(
        &self,
        pin: &PinnedContent,
        range: ByteRange,
        ctx: &ReadContext,
    ) -> anyhow::Result<Availability> {
        ctx.validate(pin.reference(), range.offset, range.len.try_into()?)?;
        Ok(if self.fail {
            Availability::Missing
        } else {
            Availability::Available
        })
    }
    fn read_range(
        &self,
        pin: &PinnedContent,
        offset: u64,
        out: &mut [u8],
        ctx: &ReadContext,
    ) -> anyhow::Result<()> {
        ctx.validate(pin.reference(), offset, out.len())?;
        ctx.cancel.sleep(
            self.delay
                .min(ctx.deadline.saturating_duration_since(Instant::now())),
        );
        ctx.check()?;
        anyhow::ensure!(!self.fail, "FAKE_PROVIDER_UNAVAILABLE");
        out.copy_from_slice(&self.bytes[offset as usize..offset as usize + out.len()]);
        Ok(())
    }
}

#[test]
fn immutable_handle_seam_accepts_delayed_failing_nonlocal_provider() {
    let reference = FileContentRef::new(ContentKind::Direct, Digest::bytes(b"abc"), 3).unwrap();
    let mut fake = FakeProvider {
        id: Uuid::new_v4(),
        bytes: Arc::new(b"abc".to_vec()),
        delay: Duration::from_millis(2),
        fail: false,
    };
    let pin = fake.pin(&reference).unwrap();
    let mut out = [0; 2];
    fake.read_range(&pin, 1, &mut out, &context()).unwrap();
    assert_eq!(out, *b"bc");
    fake.fail = true;
    assert!(fake.read_range(&pin, 1, &mut out, &context()).is_err());
    fake.fail = false;
    fake.delay = Duration::from_secs(1);
    let ctx = ReadContext {
        deadline: Instant::now() + Duration::from_millis(5),
        cancel: Cancel::new(),
    };
    assert!(
        fake.read_range(&pin, 1, &mut out, &ctx)
            .unwrap_err()
            .to_string()
            .contains("DEADLINE")
    );
}
