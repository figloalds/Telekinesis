//! Private COW overlay semantics on real SQLite/WAL and a provider whose base
//! can be absent, delayed or cancelled. No mounted flush claim is made here.
use std::{
    fs,
    process::Command,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tkfs::{
    content::{
        Availability, ContentProvider, PinnedContent, ReadContext,
        write::{PATCH_BYTES, SaveIntent, WriteJournal, WriteSession},
    },
    model::{
        content::{CHUNK_SIZE, ContentKind, Digest, FileContentRef},
        workspace::{ByteRange, FileRevision, WorkspaceId, limits},
    },
    util::Cancel,
};
use uuid::Uuid;
fn context() -> ReadContext {
    ReadContext {
        deadline: Instant::now() + Duration::from_secs(120),
        cancel: Cancel::new(),
    }
}
struct Base {
    id: Uuid,
    available: AtomicBool,
    reads: Mutex<Vec<(u64, usize)>>,
    block: Mutex<Option<(Arc<Barrier>, Arc<Barrier>)>>,
}
impl Base {
    fn new(available: bool) -> Arc<Self> {
        Arc::new(Self {
            id: Uuid::new_v4(),
            available: AtomicBool::new(available),
            reads: Mutex::new(vec![]),
            block: Mutex::new(None),
        })
    }
}
fn original(position: u64) -> u8 {
    (position % 251) as u8
}
impl ContentProvider for Base {
    fn pin(&self, reference: &FileContentRef) -> anyhow::Result<PinnedContent> {
        PinnedContent::new(reference.clone(), self.id, Arc::new(()))
    }
    fn availability(
        &self,
        _: &PinnedContent,
        _: ByteRange,
        context: &ReadContext,
    ) -> anyhow::Result<Availability> {
        context.check()?;
        Ok(if self.available.load(Ordering::Relaxed) {
            Availability::Available
        } else {
            Availability::Missing
        })
    }
    fn read_range(
        &self,
        pin: &PinnedContent,
        offset: u64,
        out: &mut [u8],
        context: &ReadContext,
    ) -> anyhow::Result<()> {
        context.validate(pin.reference(), offset, out.len())?;
        self.reads.lock().unwrap().push((offset, out.len()));
        if let Some((entered, release)) = self.block.lock().unwrap().take() {
            entered.wait();
            release.wait();
        }
        context.check()?;
        anyhow::ensure!(
            self.available.load(Ordering::Relaxed),
            "CONTENT_UNAVAILABLE"
        );
        for (i, b) in out.iter_mut().enumerate() {
            *b = original(offset + i as u64);
        }
        context.check()
    }
}
fn intent(length: u64) -> SaveIntent {
    SaveIntent {
        operation: Uuid::new_v4(),
        branch: "branch".into(),
        content_parent: Some("captured-content-parent".into()),
        alive_parent: Some("captured-alive-parent".into()),
        base: FileRevision {
            workspace: WorkspaceId {
                repository: "repo".into(),
                workspace: Uuid::new_v4(),
            },
            entity: "entity".into(),
            generation: 7,
            content: FileContentRef::new(
                if length <= CHUNK_SIZE {
                    ContentKind::Direct
                } else {
                    ContentKind::Manifest
                },
                if length == 0 {
                    Digest::bytes(b"")
                } else {
                    Digest::bytes(b"selected immutable base")
                },
                length,
            )
            .unwrap(),
        },
    }
}
fn setup(
    length: u64,
    available: bool,
) -> (
    tempfile::TempDir,
    Arc<WriteJournal>,
    Arc<Base>,
    Arc<WriteSession>,
) {
    let dir = tempfile::tempdir().unwrap();
    let journal = WriteJournal::open(dir.path()).unwrap();
    let base = Base::new(available);
    let session = Arc::new(
        journal
            .create(intent(length), base.clone(), &context())
            .unwrap(),
    );
    (dir, journal, base, session)
}
fn read(session: &WriteSession, len: usize) -> Vec<u8> {
    let mut out = vec![0; len];
    session.read_range(0, &mut out, &context()).unwrap();
    out
}

#[test]
fn native_clock_seed_validation_and_noop_mutations_preserve_the_saved_pair() {
    let (_dir, _journal, _base, session) = setup(0, false);
    session
        .write_at_with_times(0, b"native", 6, [101, 202], &context())
        .unwrap();
    let saved = session.checkpoint(&context()).unwrap().unwrap();
    let clocks = saved.native_times().unwrap();
    assert_eq!([clocks.write_time, clocks.change_time], [101, 202]);
    session
        .write_at_with_times(0, b"", 0, [303, 404], &context())
        .unwrap();
    session
        .set_len_with_times(6, 0, [303, 404], &context())
        .unwrap();
    assert_eq!(session.checkpoint(&context()).unwrap(), Some(saved.clone()));
    for (mask, prior) in [
        (1, [101, 202]),
        (8, [101, 202]),
        (2, [i64::MAX as u64 + 1, 202]),
    ] {
        let error = session
            .write_at_with_times(0, b"x", mask, prior, &context())
            .unwrap_err();
        assert!(format!("{error:#}").contains("INVALID_TIMESTAMP_POLICY"));
        assert_eq!(session.checkpoint(&context()).unwrap(), Some(saved.clone()));
        assert_eq!(read(&session, 6), b"native");
    }
    session.write_at(0, b"head", &context()).unwrap();
    assert!(
        session
            .checkpoint(&context())
            .unwrap()
            .unwrap()
            .native_times()
            .is_none()
    );
}
#[test]
fn native_time_policy_is_buffered_with_bytes_and_restored_from_saved() {
    let (dir, journal, base, session) = setup(0, false);
    let before = tkfs::util::time::now_ms();
    session
        .write_at_with_times(0, b"native", 2, [101, 202], &context())
        .unwrap();
    assert!(journal.pending(None, 64, &context()).unwrap().is_empty());
    let saved = session.checkpoint(&context()).unwrap().unwrap();
    let times = saved.native_times().unwrap();
    assert_eq!(times.disabled, 2);
    assert_eq!(times.write_time, 101);
    assert_eq!(times.change_time, tkfs::core::filetime(times.modified_ms));
    assert!(times.modified_ms >= before && times.modified_ms <= tkfs::util::time::now_ms());
    let resumed = journal
        .resume(saved.intent.operation, base.clone(), &context())
        .unwrap();
    assert_eq!(
        resumed
            .checkpoint(&context())
            .unwrap()
            .unwrap()
            .native_times(),
        Some(times)
    );
    resumed
        .write_at_with_times(
            0,
            b"x",
            0,
            [times.write_time, times.change_time],
            &context(),
        )
        .unwrap();
    assert_eq!(
        journal.pending(None, 64, &context()).unwrap(),
        vec![saved.clone()]
    );
    let recovered = WriteJournal::open(dir.path())
        .unwrap()
        .resume(saved.intent.operation, base.clone(), &context())
        .unwrap();
    assert_eq!(read(&recovered, 6), b"native");
    assert_eq!(
        recovered
            .checkpoint(&context())
            .unwrap()
            .unwrap()
            .native_times(),
        Some(times)
    );
    assert!(base.reads.lock().unwrap().is_empty());
}

#[path = "content_write/formats.rs"]
mod formats;

fn overlay_root(
    db: &rusqlite::Connection,
    operation: Uuid,
    slot: &str,
) -> tkfs::content::overlay_map::MapRoot {
    assert!(["working", "saved"].contains(&slot));
    let bytes: Vec<u8> = db
        .query_row(
            &format!("SELECT {slot} FROM sessions WHERE id=?"),
            [operation.to_string()],
            |r| r.get(0),
        )
        .unwrap();
    let record: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    serde_json::from_value(record["value"]["roots"]["patches"].clone()).unwrap()
}
fn discard_overlay_root(db: &rusqlite::Connection, operation: Uuid, slot: &str) {
    let root = overlay_root(db, operation, slot);
    assert_eq!(
        db.execute(
            "DELETE FROM private_overlay_nodes_v1 WHERE hash=?",
            [root.digest().0.as_slice()]
        )
        .unwrap(),
        1
    );
}

#[test]
fn overwrite_of_absent_gib_base_and_checkpoint_never_hydrates_old_bytes() {
    let (_dir, journal, base, session) = setup(1 << 30, false);
    let bytes = vec![81; CHUNK_SIZE as usize];
    session
        .write_at(19 * CHUNK_SIZE, &bytes, &context())
        .unwrap();
    session
        .write_at(37 * CHUNK_SIZE + 400, &[3; 100], &context())
        .unwrap();
    assert!(base.reads.lock().unwrap().is_empty());
    let checkpoint = session.checkpoint(&context()).unwrap().unwrap();
    assert_eq!(checkpoint.length, 1 << 30);
    assert!(base.reads.lock().unwrap().is_empty());
    let mut out = vec![0; bytes.len()];
    session
        .read_checkpoint(&checkpoint, 19 * CHUNK_SIZE, &mut out, &context())
        .unwrap();
    assert_eq!(out, bytes);
    assert!(base.reads.lock().unwrap().is_empty());
    assert!(
        session
            .read_range(37 * CHUNK_SIZE, &mut out, &context())
            .unwrap_err()
            .to_string()
            .contains("CONTENT_UNAVAILABLE")
    );
    assert_eq!(
        journal.pending(None, 256, &context()).unwrap(),
        vec![checkpoint]
    );
    println!(
        "WAVE6_WRITE {}",
        serde_json::json!({"scenario":"absent_gib_base","base_bytes":1u64<<30,"overwritten_bytes":65536,"partial_patch_bytes":100,"hydration_during_write_checkpoint":0,"queued_bytes_after_checkpoint":journal.queued_patch_bytes(&context()).unwrap()})
    );
}

#[test]
fn local_cas_pins_and_verified_ranges_survive_private_edits_and_recovery() {
    use tkfs::content::LocalContent;
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(LocalContent::open(dir.path()).unwrap());
    let bytes: Vec<_> = (0..3 * CHUNK_SIZE + 17).map(original).collect();
    let root = provider
        .import(&mut bytes.as_slice(), bytes.len() as u64, &context())
        .unwrap();
    let mut save = intent(bytes.len() as u64);
    save.base.content = root.clone();
    let journal = WriteJournal::open(dir.path()).unwrap();
    let session = journal.create(save, provider.clone(), &context()).unwrap();
    assert!(provider.pinned_roots().1.contains(&root));
    session
        .write_at(CHUNK_SIZE + 313, &[71; 4096], &context())
        .unwrap();
    let saved = session.checkpoint(&context()).unwrap().unwrap();
    let mut expected = bytes.clone();
    expected[CHUNK_SIZE as usize + 313..CHUNK_SIZE as usize + 4409].fill(71);
    assert_eq!(read(&session, expected.len()), expected);
    let pin = provider.pin(&root).unwrap();
    let mut unchanged = vec![0; bytes.len()];
    provider
        .read_range(&pin, 0, &mut unchanged, &context())
        .unwrap();
    assert_eq!(unchanged, bytes);
    drop(pin);
    drop(session);
    assert!(provider.pinned_roots().1.is_empty());
    assert_eq!(
        journal.pending(None, 256, &context()).unwrap(),
        vec![saved.clone()]
    );
    let resumed = journal
        .resume(saved.intent.operation, provider.clone(), &context())
        .unwrap();
    assert!(provider.pinned_roots().1.contains(&root));
    assert_eq!(read(&resumed, expected.len()), expected);
}

#[test]
fn four_kib_checkpoint_cost_does_not_hydrate_or_stage_the_selected_base() {
    for base_bytes in [1u64 << 20, 1u64 << 30] {
        for repetition in 0..6 {
            let (dir, journal, base, session) = setup(base_bytes, false);
            let offset = base_bytes - CHUNK_SIZE + 317;
            let start = Instant::now();
            session.write_at(offset, &[79; 4096], &context()).unwrap();
            let saved = session.checkpoint(&context()).unwrap().unwrap();
            let elapsed = start.elapsed();
            let mut out = [0; 4096];
            session
                .read_checkpoint(&saved, offset, &mut out, &context())
                .unwrap();
            assert_eq!(out, [79; 4096]);
            assert!(base.reads.lock().unwrap().is_empty());
            let db =
                rusqlite::Connection::open(dir.path().join("write-sessions-v1.sqlite")).unwrap();
            let (payloads, payload_bytes): (usize, usize) = db
                .query_row("SELECT count(*),sum(length(data)) FROM payloads", [], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .unwrap();
            assert_eq!((payloads, payload_bytes), (1, PATCH_BYTES));
            assert_eq!(journal.queued_patch_bytes(&context()).unwrap(), 0);
            println!(
                "WAVE6_WRITE {}",
                serde_json::json!({"scenario":"four_kib_checkpoint_scaling","base_bytes":base_bytes,"warmup":repetition==0,"repetition":repetition,"elapsed_ms":elapsed.as_secs_f64()*1000.0,"patch_bytes":4096,"overlay_payload_bytes":payload_bytes,"provider_read_bytes":0,
                "database_file_len":fs::metadata(dir.path().join("write-sessions-v1.sqlite")).unwrap().len(),"wal_file_len":fs::metadata(dir.path().join("write-sessions-v1.sqlite-wal")).map(|m|m.len()).unwrap_or(0)})
            );
        }
    }
}

#[test]
fn mixed_writes_shrinks_growth_and_holes_match_a_byte_oracle() {
    let (_dir, _journal, _base, session) = setup(3 * CHUNK_SIZE + 41, true);
    let mut oracle: Vec<u8> = (0..3 * CHUNK_SIZE + 41).map(original).collect();
    let mut seed = 0x918a_7561_9102_4f33u64;
    let mut random = || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        seed
    };
    let mut previous = None;
    for step in 0..240 {
        if step % 4 == 0 {
            let length = (random() % (5 * CHUNK_SIZE)) as usize;
            session.set_len(length as u64, &context()).unwrap();
            oracle.resize(length, 0);
        } else {
            let offset = (random() % (oracle.len() as u64 + CHUNK_SIZE / 2 + 1)) as usize;
            let bytes = vec![(random() >> 32) as u8; (random() % 24000 + 1) as usize];
            session.write_at(offset as u64, &bytes, &context()).unwrap();
            oracle.resize(oracle.len().max(offset + bytes.len()), 0);
            oracle[offset..offset + bytes.len()].copy_from_slice(&bytes);
        }
        if step % 7 == 0 {
            assert_eq!(read(&session, oracle.len()), oracle, "step {step}");
        }
        if step % 19 == 0 {
            let saved = session.checkpoint(&context()).unwrap().unwrap();
            if let Some(prior) = previous.take() {
                assert!(
                    session
                        .read_checkpoint(&prior, 0, &mut [], &context())
                        .is_err()
                );
            }
            let mut out = vec![0; oracle.len()];
            session
                .read_checkpoint(&saved, 0, &mut out, &context())
                .unwrap();
            assert_eq!(out, oracle);
            previous = Some(saved);
        }
    }
    assert_eq!(read(&session, oracle.len()), oracle);
    println!(
        "WAVE6_WRITE {}",
        serde_json::json!({"scenario":"mixed_oracle","seed":"918a756191024f33","mutations":240,"final_bytes":oracle.len()})
    );
}

#[test]
fn checkpoint_survives_later_unflushed_edits_and_resume_fences_old_handles() {
    let (dir, journal, base, old) = setup(2 * CHUNK_SIZE, false);
    old.write_at(0, &vec![41; CHUNK_SIZE as usize], &context())
        .unwrap();
    let saved = old.checkpoint(&context()).unwrap().unwrap();
    old.set_len(0, &context()).unwrap();
    old.write_at(0, b"unpromised replacement", &context())
        .unwrap();
    let mut bytes = vec![0; CHUNK_SIZE as usize];
    old.read_checkpoint(&saved, 0, &mut bytes, &context())
        .unwrap();
    assert_eq!(bytes, vec![41; bytes.len()]);
    let resumed = journal
        .resume(old.operation(), base.clone(), &context())
        .unwrap();
    for result in [
        old.write_at(0, b"stale", &context()),
        old.set_len(0, &context()),
        old.checkpoint(&context()).map(|_| ()),
    ] {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("STALE_WRITE_SESSION")
        );
    }
    assert_eq!(resumed.len(&context()).unwrap(), 2 * CHUNK_SIZE);
    assert_eq!(
        read(&resumed, CHUNK_SIZE as usize),
        vec![41; CHUNK_SIZE as usize]
    );
    drop(old);
    drop(resumed);
    drop(journal);
    let reopened = WriteJournal::open(dir.path()).unwrap();
    let recovered = reopened
        .resume(saved.intent.operation, base, &context())
        .unwrap();
    assert_eq!(
        read(&recovered, CHUNK_SIZE as usize),
        vec![41; CHUNK_SIZE as usize]
    );
    assert_eq!(
        reopened.pending(None, 256, &context()).unwrap(),
        vec![saved]
    );
}

#[test]
fn a_truncated_checkpoint_does_not_resurrect_overlay_or_base_on_growth() {
    let (_dir, journal, base, session) = setup(4 * CHUNK_SIZE, true);
    session
        .write_at(CHUNK_SIZE + 5, &[91; 1000], &context())
        .unwrap();
    session
        .write_at(3 * CHUNK_SIZE, &[92; 1000], &context())
        .unwrap();
    session.checkpoint(&context()).unwrap();
    session.set_len(CHUNK_SIZE + 17, &context()).unwrap();
    session.set_len(4 * CHUNK_SIZE, &context()).unwrap();
    session
        .write_at(3 * CHUNK_SIZE + 20, &[93; 10], &context())
        .unwrap();
    let saved = session.checkpoint(&context()).unwrap().unwrap();
    let restored = journal
        .resume(session.operation(), base, &context())
        .unwrap();
    let mut expected: Vec<_> = (0..CHUNK_SIZE + 17).map(original).collect();
    expected[CHUNK_SIZE as usize + 5..CHUNK_SIZE as usize + 17].fill(91);
    expected.resize(4 * CHUNK_SIZE as usize, 0);
    expected[3 * CHUNK_SIZE as usize + 20..3 * CHUNK_SIZE as usize + 30].fill(93);
    assert_eq!(read(&restored, expected.len()), expected);
    assert_eq!(saved.base_visible_len, CHUNK_SIZE + 17);
}

#[test]
fn concurrent_mutation_and_cancellation_never_return_mixed_successful_reads() {
    for cancel in [false, true] {
        let (_dir, _journal, base, session) = setup(2 * CHUNK_SIZE, true);
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        *base.block.lock().unwrap() = Some((entered.clone(), release.clone()));
        let reading = session.clone();
        let ctx = context();
        let stop = ctx.cancel.clone();
        let job = std::thread::spawn(move || {
            reading.read_range(0, &mut vec![0; 2 * CHUNK_SIZE as usize], &ctx)
        });
        entered.wait();
        if cancel {
            stop.cancel();
        } else {
            session.write_at(CHUNK_SIZE, &[77; 10], &context()).unwrap();
        }
        release.wait();
        let error = job.join().unwrap().unwrap_err().to_string();
        assert!(
            error.contains(if cancel {
                "CONTENT_CANCELLED"
            } else {
                "STALE_WRITE_CHECKPOINT"
            }),
            "{error}"
        );
    }
}

#[test]
fn byte_and_job_backpressure_checkpoint_synchronously_and_remain_bounded() {
    let (dir, journal, base, session) = setup(1 << 30, false);
    let limit = limits::DIRTY_BYTES / PATCH_BYTES;
    let mut peak = 0;
    for index in 0..limit + 17 {
        session
            .write_at(
                index as u64 * CHUNK_SIZE,
                &(index as u64).to_le_bytes(),
                &context(),
            )
            .unwrap();
        let used = journal.queued_patch_bytes(&context()).unwrap();
        peak = peak.max(used);
        assert!(used <= limits::DIRTY_BYTES);
    }
    assert!(
        journal
            .pending(None, 256, &context())
            .unwrap()
            .iter()
            .any(|c| c.intent.operation == session.operation())
    );
    assert!(base.reads.lock().unwrap().is_empty());
    for _ in 0..limits::SAVE_QUEUE + 2 {
        let next = journal.create(intent(0), base.clone(), &context()).unwrap();
        next.set_len(1, &context()).unwrap();
    }
    let db = rusqlite::Connection::open(dir.path().join("write-sessions-v1.sqlite")).unwrap();
    let jobs: usize = db
        .query_row("SELECT count(*) FROM sessions WHERE dirty=1", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert!(jobs <= limits::SAVE_QUEUE);
    let page = journal.pending(None, 7, &context()).unwrap();
    assert_eq!(page.len(), 7);
    let next = journal
        .pending(Some(page.last().unwrap().intent.operation), 256, &context())
        .unwrap();
    assert!(
        next.iter()
            .all(|c| c.intent.operation > page.last().unwrap().intent.operation)
    );
    println!(
        "WAVE6_WRITE {}",
        serde_json::json!({"scenario":"checkpoint_backpressure","dirty_byte_limit":limits::DIRTY_BYTES,"charged_patch_bytes":PATCH_BYTES,"patch_peak":peak,"dirty_jobs_after":jobs,"job_limit":limits::SAVE_QUEUE,"base_reads":0})
    );
}

#[test]
fn bounded_node_collection_reclaims_superseded_paths_without_losing_saved_bytes() {
    let (dir, journal, base, session) = setup(0, false);
    session.write_at(0, b"initial", &context()).unwrap();
    let first = session.checkpoint(&context()).unwrap().unwrap();
    for value in 0..200u16 {
        session
            .write_at(0, &value.to_le_bytes(), &context())
            .unwrap();
    }
    let saved = session.checkpoint(&context()).unwrap().unwrap();
    let db = rusqlite::Connection::open(dir.path().join("write-sessions-v1.sqlite")).unwrap();
    let mut cursor = None;
    let mut removed = 0;
    let mut examined = 0;
    loop {
        let page = journal.reclaim_orphans(cursor, &context()).unwrap();
        assert!(page.validated + page.examined + page.nodes_examined <= 256);
        assert!(!page.nodes_deferred);
        removed += page.nodes_removed;
        examined += page.nodes_examined;
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    let nodes: usize = db
        .query_row("SELECT count(*) FROM private_overlay_nodes_v1", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(nodes, 9);
    assert!(examined >= 9);
    let mut bytes = [0; 7];
    session
        .read_checkpoint(&saved, 0, &mut bytes, &context())
        .unwrap();
    let mut expected = *b"initial";
    expected[..2].copy_from_slice(&199u16.to_le_bytes());
    assert_eq!(bytes, expected);
    assert!(
        session
            .read_checkpoint(&first, 0, &mut [0], &context())
            .is_err()
    );
    let fresh = WriteJournal::open(dir.path()).unwrap();
    let resumed = fresh.resume(session.operation(), base, &context()).unwrap();
    assert_eq!(read(&resumed, 7), expected);
    println!(
        "WAVE6_NODE_GC {}",
        serde_json::json!({"scenario":"hot_chunk","writes":200,"live_nodes_after_pass":nodes,
        "nodes_removed_in_explicit_pass":removed,"nodes_examined_in_explicit_pass":examined,"page_units":256,"saved_reopened":true})
    );
}

#[test]
fn failed_group_keeps_all_prior_saved_and_retry_advances_only_requested_owners() {
    use tkfs::util::fault::{self, FaultAction, FaultConfig};
    let (_dir, journal, base, first) = setup(0, false);
    let second = journal.create(intent(0), base.clone(), &context()).unwrap();
    let excluded = journal.create(intent(0), base, &context()).unwrap();
    first.write_at(0, b"old", &context()).unwrap();
    second.write_at(0, b"old", &context()).unwrap();
    let old = journal
        .checkpoint_group(&[first.as_ref(), &second], &context())
        .unwrap();
    first.write_at(0, b"new", &context()).unwrap();
    second.write_at(0, b"new", &context()).unwrap();
    excluded.write_at(0, b"private", &context()).unwrap();
    let before = journal.checkpoint_work();
    let mut config = FaultConfig::new("content.write.before_checkpoint_commit", FaultAction::Fail);
    config.count = Some(1);
    fault::configure(Some(config));
    let failure = journal.checkpoint_group(&[first.as_ref(), &second], &context());
    fault::configure(None);
    assert!(failure.is_err());
    assert_eq!(journal.checkpoint_work(), before);
    for (session, checkpoint) in [(first.as_ref(), &old[0]), (&second, &old[1])] {
        let mut bytes = [0; 3];
        session
            .read_checkpoint(checkpoint.as_ref().unwrap(), 0, &mut bytes, &context())
            .unwrap();
        assert_eq!(&bytes, b"old");
        assert_eq!(read(session, 3), b"new");
    }
    let saved = journal
        .checkpoint_group(&[&second, first.as_ref()], &context())
        .unwrap();
    assert_eq!(
        saved[0].as_ref().unwrap().intent.operation,
        second.operation()
    );
    assert_eq!(
        saved[1].as_ref().unwrap().intent.operation,
        first.operation()
    );
    let after = journal.checkpoint_work();
    assert_eq!(after.commits - before.commits, 1);
    assert_eq!(after.sessions - before.sessions, 2);
    let pending = journal.pending(None, 256, &context()).unwrap();
    assert_eq!(pending.len(), 2);
    assert!(
        pending
            .iter()
            .all(|cp| cp.intent.operation != excluded.operation())
    );
    assert_eq!(read(&excluded, 7), b"private");
}

#[test]
fn group_refuses_duplicate_stale_or_foreign_owners_before_any_saved_changes() {
    let (_dir, journal, base, session) = setup(0, false);
    session.write_at(0, b"new", &context()).unwrap();
    assert!(
        journal
            .checkpoint_group(&[session.as_ref(), session.as_ref()], &context())
            .is_err()
    );
    assert!(
        journal
            .checkpoint_group(&vec![session.as_ref(); 65], &context())
            .is_err()
    );
    let (_other_dir, _other, _other_base, foreign) = setup(0, false);
    assert!(
        journal
            .checkpoint_group(&[session.as_ref(), foreign.as_ref()], &context())
            .is_err()
    );
    session.checkpoint(&context()).unwrap();
    let fresh = journal
        .resume(session.operation(), base, &context())
        .unwrap();
    fresh.write_at(0, b"latest", &context()).unwrap();
    let before = journal.checkpoint_work();
    assert!(
        journal
            .checkpoint_group(&[session.as_ref()], &context())
            .is_err()
    );
    assert_eq!(journal.checkpoint_work(), before);
    assert!(
        journal
            .checkpoint_group(&[], &context())
            .unwrap()
            .is_empty()
    );
    assert_eq!(read(&fresh, 6), b"latest");
}

#[test]
#[ignore = "parent invokes this actual grouped-checkpoint process helper"]
fn batch_checkpoint_crash_child() {
    let Some(root) = std::env::var_os("TKFS_BATCH_CHILD_ROOT") else {
        return;
    };
    let journal = WriteJournal::open(std::path::Path::new(&root)).unwrap();
    let base = Base::new(false);
    let mut sessions = Vec::new();
    for checkpoint in journal.pending(None, 256, &context()).unwrap() {
        let session = journal
            .resume(checkpoint.intent.operation, base.clone(), &context())
            .unwrap();
        session.write_at(0, b"new!", &context()).unwrap();
        sessions.push(session);
    }
    assert_eq!(sessions.len(), 4);
    journal
        .checkpoint_group(&sessions.iter().collect::<Vec<_>>(), &context())
        .unwrap();
    panic!("batch crash point did not terminate child");
}

#[test]
fn process_exit_at_group_checkpoint_boundaries_preserves_all_old_or_all_new_saved() {
    for (point, expected) in [
        ("content.write.before_checkpoint_commit", b"old!"),
        ("content.write.checkpointed", b"new!"),
    ] {
        let (dir, journal, base, first) = setup(0, false);
        let mut sessions = vec![first];
        for _ in 1..4 {
            sessions.push(Arc::new(
                journal.create(intent(0), base.clone(), &context()).unwrap(),
            ));
        }
        for session in &sessions {
            session.write_at(0, b"old!", &context()).unwrap();
        }
        journal
            .checkpoint_group(
                &sessions.iter().map(|s| s.as_ref()).collect::<Vec<_>>(),
                &context(),
            )
            .unwrap();
        drop(sessions);
        drop(journal);
        drop(base);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "batch_checkpoint_crash_child",
                "--ignored",
                "--nocapture",
            ])
            .env("TKFS_BATCH_CHILD_ROOT", dir.path())
            .env("TKFS_FAULT_POINT", point)
            .env("TKFS_FAULT_ACTION", "exit")
            .env("TKFS_FAULT_COUNT", "1")
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86));
        let journal = WriteJournal::open(dir.path()).unwrap();
        let base = Base::new(false);
        let saved = journal.pending(None, 256, &context()).unwrap();
        assert_eq!(saved.len(), 4);
        for checkpoint in saved {
            let session = journal
                .resume(checkpoint.intent.operation, base.clone(), &context())
                .unwrap();
            assert_eq!(read(&session, 4), expected);
        }
    }
}

#[test]
fn rewriting_a_chunk_reclaims_only_unreferenced_payloads() {
    let (dir, _journal, _base, session) = setup(CHUNK_SIZE, false);
    session.write_at(0, &[1; 100], &context()).unwrap();
    session.checkpoint(&context()).unwrap();
    for n in 2..200u8 {
        session.write_at(0, &[n; 100], &context()).unwrap();
    }
    let db = rusqlite::Connection::open(dir.path().join("write-sessions-v1.sqlite")).unwrap();
    let count: usize = db
        .query_row("SELECT count(*) FROM payloads", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2);
    session.checkpoint(&context()).unwrap();
    let count: usize = db
        .query_row("SELECT count(*) FROM payloads", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
    println!(
        "WAVE6_WRITE {}",
        serde_json::json!({"scenario":"repeated_overlay_overwrite","writes":199,"payloads_before_checkpoint":2,"payloads_after_checkpoint":count,
        "database_bytes":fs::metadata(dir.path().join("write-sessions-v1.sqlite")).unwrap().len(),
        "wal_bytes":fs::metadata(dir.path().join("write-sessions-v1.sqlite-wal")).map(|m|m.len()).unwrap_or(0)})
    );
}

#[test]
fn orphan_cleanup_pages_keep_both_saved_and_uncheckpointed_payloads() {
    let (_dir, journal, base, session) = setup(CHUNK_SIZE, false);
    session.write_at(0, &[255], &context()).unwrap();
    let saved = session.checkpoint(&context()).unwrap().unwrap();
    for index in 1..=400u64 {
        session
            .write_at(index * CHUNK_SIZE, &index.to_le_bytes(), &context())
            .unwrap();
    }
    session.set_len(0, &context()).unwrap();
    let other = journal.create(intent(0), base, &context()).unwrap();
    other.write_at(0, b"live", &context()).unwrap();
    let mut cursor = None;
    let mut removed = 0;
    let mut examined = 0;
    loop {
        let page = journal.reclaim_orphans(cursor, &context()).unwrap();
        assert!(page.examined <= 256);
        removed += page.removed;
        examined += page.examined;
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(removed, 400);
    assert_eq!(examined, 402);
    assert_eq!(read(&other, 4), b"live");
    let mut out = [0];
    session
        .read_checkpoint(&saved, 0, &mut out, &context())
        .unwrap();
    assert_eq!(out, [255]);
    println!(
        "WAVE6_WRITE {}",
        serde_json::json!({"scenario":"bounded_overlay_reclamation","orphan_payloads_removed":removed,"examined":examined,"page_limit":256,"saved_and_current_preserved":true})
    );
}

#[test]
fn discarded_journal_headers_and_inventory_cannot_acknowledge_a_checkpoint() {
    for trigger in [
        "CREATE TEMP TRIGGER discard BEFORE UPDATE ON sessions BEGIN SELECT RAISE(IGNORE); END",
        "CREATE TEMP TRIGGER discard AFTER UPDATE OF saved ON sessions BEGIN UPDATE sessions SET saved=OLD.saved WHERE id=NEW.id; END",
        "CREATE TEMP TRIGGER discard AFTER UPDATE ON inventory BEGIN UPDATE inventory SET record=OLD.record WHERE id=NEW.id; END",
    ] {
        let (dir, journal, _base, session) = setup(0, false);
        session.write_at(0, b"old", &context()).unwrap();
        let prior = session.checkpoint(&context()).unwrap().unwrap();
        session.write_at(0, b"new", &context()).unwrap();
        // Triggers must be visible to the journal's own connection.
        let trigger = trigger.replace("TEMP TRIGGER", "TRIGGER");
        let db = rusqlite::Connection::open(dir.path().join("write-sessions-v1.sqlite")).unwrap();
        db.execute_batch(&trigger).unwrap();
        let err = session.checkpoint(&context()).unwrap_err();
        assert!(err.to_string().contains("CORRUPT_WRITE_JOURNAL"), "{err:#}");
        assert_eq!(journal.pending(None, 256, &context()).unwrap(), vec![prior]);
        assert_eq!(read(&session, 3), b"new");
        db.execute_batch("DROP TRIGGER discard").unwrap();
        let saved = session.checkpoint(&context()).unwrap().unwrap();
        assert_eq!(saved.sequence, 2);
        assert_eq!(journal.queued_patch_bytes(&context()).unwrap(), 0);
    }
}

#[test]
fn discarded_payload_install_rolls_back_the_buffered_mutation() {
    let (dir, journal, _base, session) = setup(0, false);
    session.write_at(0, b"old", &context()).unwrap();
    let saved = session.checkpoint(&context()).unwrap().unwrap();
    let db = rusqlite::Connection::open(dir.path().join("write-sessions-v1.sqlite")).unwrap();
    db.execute_batch(
        "CREATE TRIGGER discard BEFORE INSERT ON payloads BEGIN SELECT RAISE(IGNORE); END",
    )
    .unwrap();
    let err = session.write_at(0, b"new", &context()).unwrap_err();
    assert!(err.to_string().contains("CORRUPT_WRITE_JOURNAL"), "{err:#}");
    assert_eq!(read(&session, 3), b"old");
    assert_eq!(journal.pending(None, 256, &context()).unwrap(), vec![saved]);
    assert_eq!(journal.queued_patch_bytes(&context()).unwrap(), 0);
}

#[test]
fn discarded_gc_marks_never_authorize_payload_deletion() {
    let (dir, journal, _base, session) = setup(0, false);
    session.write_at(0, b"live", &context()).unwrap();
    session.checkpoint(&context()).unwrap();
    let db = rusqlite::Connection::open(dir.path().join("write-sessions-v1.sqlite")).unwrap();
    db.execute_batch(
        "CREATE TRIGGER discard BEFORE INSERT ON gc_marks BEGIN SELECT RAISE(IGNORE); END",
    )
    .unwrap();
    assert!(journal.reclaim_orphans(None, &context()).is_err());
    assert_eq!(read(&session, 4), b"live");
    assert_eq!(
        db.query_row("SELECT count(*) FROM payloads", [], |r| r
            .get::<_, usize>(0))
            .unwrap(),
        1
    );
}

#[test]
fn experimental_v2_records_are_refused_without_a_lossy_implicit_migration() {
    let dir = tempfile::tempdir().unwrap();
    let db = rusqlite::Connection::open(dir.path().join("write-sessions-v1.sqlite")).unwrap();
    db.execute_batch(
        "PRAGMA application_id=1414223699; PRAGMA user_version=2;
        CREATE TABLE sessions(id TEXT PRIMARY KEY,working BLOB,saved BLOB,dirty INTEGER,seal BLOB);
        CREATE TABLE changes(op TEXT,idx INTEGER,hash TEXT,valid INTEGER,proof BLOB);
        CREATE TABLE saved_chunks(op TEXT,idx INTEGER,hash TEXT,valid INTEGER,proof BLOB);
        INSERT INTO sessions VALUES('old-operation',x'0102',x'0304',1,x'05');",
    )
    .unwrap();
    assert!(WriteJournal::open(dir.path()).is_err());
    let bytes: (Vec<u8>, Vec<u8>) = db
        .query_row("SELECT working,saved FROM sessions", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(bytes, (vec![1, 2], vec![3, 4]));
    assert_eq!(
        db.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        2
    );
}

#[test]
fn experimental_v3_roots_are_refused_without_modifying_their_recovery_records() {
    let dir = tempfile::tempdir().unwrap();
    let db = rusqlite::Connection::open(dir.path().join("write-sessions-v1.sqlite")).unwrap();
    db.execute_batch("PRAGMA application_id=1414223699; PRAGMA user_version=3;
        CREATE TABLE inventory(id INTEGER PRIMARY KEY,record BLOB NOT NULL);
        CREATE TABLE sessions(id TEXT PRIMARY KEY,working BLOB NOT NULL,saved BLOB,dirty INTEGER NOT NULL,queued INTEGER NOT NULL,seal BLOB NOT NULL);
        CREATE TABLE payloads(hash TEXT PRIMARY KEY,data BLOB NOT NULL);
        CREATE TABLE gc_marks(hash TEXT PRIMARY KEY,epoch TEXT NOT NULL) WITHOUT ROWID;
        CREATE TABLE private_overlay_nodes_v1(hash BLOB PRIMARY KEY,body BLOB NOT NULL) WITHOUT ROWID;
        INSERT INTO sessions VALUES('old-operation',x'0102',x'0304',1,1,x'05');").unwrap();
    assert!(WriteJournal::open(dir.path()).is_err());
    assert_eq!(
        db.query_row("SELECT working,saved FROM sessions", [], |r| Ok((
            r.get::<_, Vec<u8>>(0)?,
            r.get::<_, Vec<u8>>(1)?
        )))
        .unwrap(),
        (vec![1, 2], vec![3, 4])
    );
    assert_eq!(
        db.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        3
    );
}

#[test]
fn damaged_payloads_and_oversized_records_are_rejected_before_exposing_success() {
    let (dir, journal, _base, session) = setup(CHUNK_SIZE, false);
    session
        .write_at(0, &vec![55; CHUNK_SIZE as usize], &context())
        .unwrap();
    let prior = session.checkpoint(&context()).unwrap().unwrap();
    session
        .write_at(0, &vec![56; CHUNK_SIZE as usize], &context())
        .unwrap();
    let db = rusqlite::Connection::open(dir.path().join("write-sessions-v1.sqlite")).unwrap();
    let root = overlay_root(&db, session.operation(), "working");
    let value = tkfs::content::overlay_map::OverlayMap::new(
        &db,
        session.operation(),
        tkfs::content::overlay_map::MapKind::Patches,
    )
    .get(
        root,
        0,
        0,
        &mut tkfs::content::overlay_map::MapWork::default(),
        &context(),
    )
    .unwrap()
    .unwrap();
    let tkfs::content::overlay_map::MapValue::Patch { hash, .. } = value else {
        panic!("expected patch")
    };
    db.execute(
        "UPDATE payloads SET data=zeroblob(73728) WHERE hash=?",
        [hash.to_string()],
    )
    .unwrap();
    assert!(
        session
            .read_range(0, &mut [0; 1], &context())
            .unwrap_err()
            .to_string()
            .contains("CORRUPT_WRITE_JOURNAL")
    );
    assert!(session.checkpoint(&context()).is_err());
    assert_eq!(journal.pending(None, 256, &context()).unwrap(), vec![prior]);
    db.execute("UPDATE sessions SET working=zeroblob(1048576)", [])
        .unwrap();
    assert!(session.len(&context()).is_err());
}

#[test]
fn stopped_and_invalid_requests_do_not_create_new_dirty_state() {
    let (_dir, journal, _base, session) = setup(0, false);
    let stopped = context();
    stopped.cancel.cancel();
    assert!(journal.reclaim_orphans(None, &stopped).is_err());
    assert!(journal.reclamation_error().is_none());
    assert!(session.write_at(0, b"x", &stopped).is_err());
    let late = ReadContext {
        deadline: Instant::now() - Duration::from_millis(1),
        cancel: Cancel::new(),
    };
    assert!(session.set_len(1, &late).is_err());
    assert!(session.write_at(u64::MAX, b"x", &context()).is_err());
    assert!(
        session
            .write_at(0, &vec![1; limits::READ_BYTES + 1], &context())
            .is_err()
    );
    assert_eq!(session.len(&context()).unwrap(), 0);
    assert_eq!(journal.queued_patch_bytes(&context()).unwrap(), 0);
    assert!(session.checkpoint(&context()).unwrap().is_none());
}

#[test]
fn delayed_old_checkpoint_cannot_checkpoint_a_resumed_owners_new_edits() {
    use tkfs::util::fault::{self, FaultAction, FaultConfig};
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let _reset = Reset;
    let (_dir, journal, base, old) = setup(CHUNK_SIZE, false);
    old.write_at(0, b"old", &context()).unwrap();
    let prior = old.checkpoint(&context()).unwrap().unwrap();
    let mut config = FaultConfig::new("content.write.before_checkpoint", FaultAction::Hang);
    config.ms = Some(250);
    config.count = Some(1);
    fault::configure(Some(config));
    let late = old.clone();
    let job = std::thread::spawn(move || late.checkpoint(&context()));
    let until = Instant::now() + Duration::from_secs(1);
    while fault::fired() == 0 && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(fault::fired(), 1);
    let current = journal.resume(old.operation(), base, &context()).unwrap();
    current.write_at(0, b"new", &context()).unwrap();
    assert!(
        job.join()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("STALE_WRITE_SESSION")
    );
    assert_eq!(journal.pending(None, 256, &context()).unwrap(), vec![prior]);
    assert_eq!(read(&current, 3), b"new");
    assert_eq!(journal.queued_patch_bytes(&context()).unwrap(), PATCH_BYTES);
}

#[test]
fn cancellation_after_durable_checkpoint_retains_the_record_without_success() {
    use tkfs::util::fault::{self, FaultAction, FaultConfig};
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let _reset = Reset;
    let (_dir, journal, base, session) = setup(CHUNK_SIZE, false);
    session.write_at(0, b"retained", &context()).unwrap();
    let mut config = FaultConfig::new("content.write.checkpointed", FaultAction::Hang);
    config.ms = Some(250);
    config.count = Some(1);
    fault::configure(Some(config));
    let saving = session.clone();
    let ctx = context();
    let cancel = ctx.cancel.clone();
    let job = std::thread::spawn(move || saving.checkpoint(&ctx));
    let until = Instant::now() + Duration::from_secs(1);
    while fault::fired() == 0 && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(fault::fired(), 1);
    cancel.cancel();
    assert!(
        job.join()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("CONTENT_CANCELLED")
    );
    assert_eq!(journal.pending(None, 256, &context()).unwrap().len(), 1);
    let recovered = journal
        .resume(session.operation(), base, &context())
        .unwrap();
    assert_eq!(read(&recovered, 8), b"retained");
}

#[test]
fn sqlite_vm_deadline_rolls_back_checkpoint_and_removes_its_progress_hook() {
    let (dir, journal, _base, session) = setup(CHUNK_SIZE, false);
    session.write_at(0, b"a", &context()).unwrap();
    let prior = session.checkpoint(&context()).unwrap().unwrap();
    session.write_at(0, b"b", &context()).unwrap();
    let db = rusqlite::Connection::open(dir.path().join("write-sessions-v1.sqlite")).unwrap();
    db.execute_batch("CREATE TRIGGER stall_save BEFORE UPDATE OF saved ON sessions WHEN NEW.saved IS NOT NULL BEGIN
        SELECT sum(x) FROM (WITH RECURSIVE nums(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM nums WHERE x<100000000) SELECT x FROM nums); END;").unwrap();
    let ctx = ReadContext {
        deadline: Instant::now() + Duration::from_millis(80),
        cancel: Cancel::new(),
    };
    let began = Instant::now();
    assert!(
        session
            .checkpoint(&ctx)
            .unwrap_err()
            .to_string()
            .contains("CONTENT_DEADLINE")
    );
    let elapsed = began.elapsed();
    assert!(elapsed < Duration::from_millis(500), "{elapsed:?}");
    assert_eq!(journal.pending(None, 256, &context()).unwrap(), vec![prior]);
    db.execute_batch("DROP TRIGGER stall_save").unwrap();
    let saved = session.checkpoint(&context()).unwrap().unwrap();
    let mut out = [0];
    session
        .read_checkpoint(&saved, 0, &mut out, &context())
        .unwrap();
    assert_eq!(out, b"b"[..]);
    println!(
        "WAVE6_WRITE {}",
        serde_json::json!({"scenario":"sqlite_vm_deadline","deadline_ms":80,"returned_ms":elapsed.as_secs_f64()*1000.0,"bound_ms":500,"old_checkpoint_preserved":true,"subsequent_checkpoint_succeeded":true})
    );
}

#[test]
fn process_exit_before_and_after_checkpoint_preserves_the_promised_version() {
    for boundary in [
        "content.write.before_checkpoint",
        "content.write.checkpointed",
    ] {
        let (dir, journal, base, session) = setup(CHUNK_SIZE, false);
        session
            .write_at(0, &vec![31; CHUNK_SIZE as usize], &context())
            .unwrap();
        let prior = session.checkpoint(&context()).unwrap().unwrap();
        drop(session);
        drop(journal);
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "child_process_checkpoint",
                "--exact",
                "--ignored",
                "--nocapture",
            ])
            .env("TKFS_WRITE_CHILD_ROOT", dir.path())
            .env("TKFS_WRITE_CHILD_OP", prior.intent.operation.to_string())
            .env("TKFS_WRITE_CHILD_BOUNDARY", boundary)
            .output()
            .unwrap();
        assert_eq!(
            child.status.code(),
            Some(86),
            "{}",
            String::from_utf8_lossy(&child.stderr)
        );
        let journal = WriteJournal::open(dir.path()).unwrap();
        let session = journal
            .resume(prior.intent.operation, base, &context())
            .unwrap();
        let value = if boundary.ends_with("checkpointed") {
            32
        } else {
            31
        };
        assert_eq!(
            read(&session, CHUNK_SIZE as usize),
            vec![value; CHUNK_SIZE as usize]
        );
        println!(
            "WAVE6_WRITE {}",
            serde_json::json!({"scenario":"process_exit_checkpoint","boundary":boundary,"child_exit":86,"recovered_byte":value,"bytes":CHUNK_SIZE,"child_stdout":String::from_utf8_lossy(&child.stdout)})
        );
    }
}

#[test]
#[ignore = "subprocess helper; the parent test invokes both crash boundaries"]
fn child_process_checkpoint() {
    use tkfs::util::fault::{self, FaultAction, FaultConfig};
    let root = std::path::PathBuf::from(std::env::var("TKFS_WRITE_CHILD_ROOT").unwrap());
    let journal = WriteJournal::open(&root).unwrap();
    let session = journal
        .resume(
            Uuid::parse_str(&std::env::var("TKFS_WRITE_CHILD_OP").unwrap()).unwrap(),
            Base::new(false),
            &context(),
        )
        .unwrap();
    session
        .write_at(0, &vec![32; CHUNK_SIZE as usize], &context())
        .unwrap();
    fault::configure(Some(FaultConfig::new(
        &std::env::var("TKFS_WRITE_CHILD_BOUNDARY").unwrap(),
        FaultAction::Exit,
    )));
    session.checkpoint(&context()).unwrap();
    panic!("crash point did not exit");
}

fn native_session(
    root: &std::path::Path,
    provider: Arc<tkfs::content::LocalContent>,
    reference: FileContentRef,
) -> (Arc<WriteJournal>, Arc<WriteSession>) {
    let journal = WriteJournal::open(root).unwrap();
    let mut save = intent(reference.total_len);
    save.base.content = reference;
    let session = Arc::new(journal.create(save, provider, &context()).unwrap());
    (journal, session)
}

#[test]
fn canonical_prepare_matches_streaming_import_across_shape_changes() {
    use tkfs::content::LocalContent;
    for length in [
        0,
        333,
        CHUNK_SIZE,
        CHUNK_SIZE + 1,
        CHUNK_SIZE * 256,
        CHUNK_SIZE * 256 + 17,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let provider = Arc::new(LocalContent::open(dir.path()).unwrap());
        let mut oracle: Vec<_> = (0..length).map(original).collect();
        let base = provider
            .import(&mut oracle.as_slice(), length, &context())
            .unwrap();
        let (_journal, session) = native_session(dir.path(), provider.clone(), base);
        for stage in 0..4 {
            match stage {
                0 => {
                    let offset = oracle.len() / 2;
                    let patch = [73; 4096];
                    session.write_at(offset as u64, &patch, &context()).unwrap();
                    oracle.resize(oracle.len().max(offset + patch.len()), 0);
                    oracle[offset..offset + patch.len()].copy_from_slice(&patch);
                }
                1 => {
                    let length = oracle.len() / 3;
                    session.set_len(length as u64, &context()).unwrap();
                    oracle.resize(length, 0);
                }
                2 => {
                    let length = length + CHUNK_SIZE + 19;
                    session.set_len(length, &context()).unwrap();
                    oracle.resize(length as usize, 0);
                }
                _ => {
                    let offset = CHUNK_SIZE as usize - 32;
                    let patch = [74; 96];
                    session.write_at(offset as u64, &patch, &context()).unwrap();
                    oracle.resize(oracle.len().max(offset + patch.len()), 0);
                    oracle[offset..offset + patch.len()].copy_from_slice(&patch);
                }
            }
            let saved = session.checkpoint(&context()).unwrap().unwrap();
            let prepared = session.prepare(&provider, &saved, &context()).unwrap();
            let expected = provider
                .import(&mut oracle.as_slice(), oracle.len() as u64, &context())
                .unwrap();
            assert_eq!(
                prepared.content(),
                &expected,
                "base {length}, stage {stage}"
            );
            session.validate_prepared(&prepared, &context()).unwrap();
            session.write_at(0, b"a", &context()).unwrap();
            session.checkpoint(&context()).unwrap();
            assert!(session.validate_prepared(&prepared, &context()).is_err());
            if oracle.is_empty() {
                oracle.resize(1, 0);
            }
            oracle[0] = b'a';
        }
    }
    println!(
        "WAVE6_PREPARE {}",
        serde_json::json!({"scenario":"shape_oracle","base_lengths":[0,333,65536,65537,16777216,16777233],"stages_per_base":4,"compared_to":"independent streaming import"})
    );
}

#[test]
fn preparation_reuses_unhydrated_chunks_and_reports_missing_structure() {
    use tkfs::content::LocalContent;
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(LocalContent::open(dir.path()).unwrap());
    let bytes: Vec<_> = (0..3 * CHUNK_SIZE).map(original).collect();
    let base = provider
        .import(&mut bytes.as_slice(), bytes.len() as u64, &context())
        .unwrap();
    for index in [1, 2] {
        fs::remove_file(
            dir.path().join("objects").join(
                Digest::bytes(
                    &bytes[index * CHUNK_SIZE as usize..(index + 1) * CHUNK_SIZE as usize],
                )
                .to_string(),
            ),
        )
        .unwrap();
    }
    let (journal, session) = native_session(dir.path(), provider.clone(), base.clone());
    session
        .write_at(CHUNK_SIZE, &vec![87; CHUNK_SIZE as usize], &context())
        .unwrap();
    let saved = session.checkpoint(&context()).unwrap().unwrap();
    let prepared = session.prepare(&provider, &saved, &context()).unwrap();
    let mut out = vec![0; CHUNK_SIZE as usize];
    provider
        .read_range(prepared.pin(), CHUNK_SIZE, &mut out, &context())
        .unwrap();
    assert_eq!(out, vec![87; out.len()]);
    assert!(
        provider
            .read_range(prepared.pin(), 2 * CHUNK_SIZE, &mut out, &context())
            .is_err()
    );
    fs::remove_file(
        dir.path()
            .join("manifests-v1")
            .join(base.digest.to_string()),
    )
    .unwrap();
    let error = session.prepare(&provider, &saved, &context()).unwrap_err();
    assert!(LocalContent::unavailable_key(&error).is_some());
    assert_eq!(journal.pending(None, 256, &context()).unwrap(), vec![saved]);
}

#[test]
fn fully_replaced_output_needs_no_original_direct_bytes_or_manifest_structure() {
    use tkfs::content::LocalContent;
    for len in [32, 1 << 30] {
        let dir = tempfile::tempdir().unwrap();
        let target = LocalContent::open(dir.path()).unwrap();
        let journal = WriteJournal::open(dir.path()).unwrap();
        let fake = Base::new(false);
        let save = intent(len);
        let session = journal.create(save, fake.clone(), &context()).unwrap();
        let bytes = vec![89; 100_000];
        session.write_at(0, &bytes, &context()).unwrap();
        session.set_len(bytes.len() as u64, &context()).unwrap();
        let saved = session.checkpoint(&context()).unwrap().unwrap();
        let prepared = session.prepare(&target, &saved, &context()).unwrap();
        let expected = target
            .import(&mut bytes.as_slice(), bytes.len() as u64, &context())
            .unwrap();
        assert_eq!(prepared.content(), &expected);
        assert!(fake.reads.lock().unwrap().is_empty());
    }
}

#[test]
fn sparse_maximum_length_prepares_a_bounded_zero_dag() {
    use tkfs::{
        content::LocalContent,
        model::content::{MAX_FILE_SIZE, ManifestNode},
        util::phases::Phase,
    };
    let dir = tempfile::tempdir().unwrap();
    let target = Arc::new(LocalContent::open(dir.path()).unwrap());
    let journal = WriteJournal::open(dir.path()).unwrap();
    let session = journal
        .create(intent(0), Base::new(false), &context())
        .unwrap();
    session.set_len(MAX_FILE_SIZE, &context()).unwrap();
    let saved = session.checkpoint(&context()).unwrap().unwrap();
    let before = target.counters.snapshot();
    let prepared = session.prepare(&target, &saved, &context()).unwrap();
    let after = target.counters.snapshot();
    let data = after[Phase::Write as usize].operations - before[Phase::Write as usize].operations;
    let pages = after[Phase::ManifestWrite as usize].operations
        - before[Phase::ManifestWrite as usize].operations;
    assert_eq!(data, 1);
    assert_eq!(
        pages,
        ManifestNode::root_level(MAX_FILE_SIZE).unwrap() as u64 + 1
    );
    let mut out = [1; 97];
    target
        .read_range(prepared.pin(), MAX_FILE_SIZE - 97, &mut out, &context())
        .unwrap();
    assert_eq!(out, [0; 97]);
    let mut next = intent(MAX_FILE_SIZE);
    next.base.content = prepared.content().clone();
    let edited = journal.create(next, target.clone(), &context()).unwrap();
    edited
        .write_at(MAX_FILE_SIZE - 8192, &[91; 4096], &context())
        .unwrap();
    let saved = edited.checkpoint(&context()).unwrap().unwrap();
    let changed = edited.prepare(&target, &saved, &context()).unwrap();
    let mut patch = [0; 4096];
    target
        .read_range(changed.pin(), MAX_FILE_SIZE - 8192, &mut patch, &context())
        .unwrap();
    assert_eq!(patch, [91; 4096]);
    println!(
        "WAVE6_PREPARE {}",
        serde_json::json!({"scenario":"maximum_zero_dag","logical_bytes":MAX_FILE_SIZE,"new_data_objects":data,"new_manifest_pages":pages,"end_read_verified":true,"subsequent_partial_patch_verified":true})
    );
}

#[test]
fn chunked_preparation_is_bounded_and_parallel_saves_do_not_nest_buffer_permits() {
    use tkfs::{content::LocalContent, util::phases::Phase};
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(LocalContent::open(dir.path()).unwrap());
    let bytes: Vec<_> = (0..2 * 1024 * 1024 + 133).map(original).collect();
    let reference = provider
        .import(&mut bytes.as_slice(), bytes.len() as u64, &context())
        .unwrap();
    let (journal, session) = native_session(dir.path(), provider.clone(), reference.clone());
    session
        .write_at(13 * CHUNK_SIZE + 317, &[78; 4096], &context())
        .unwrap();
    let saved = session.checkpoint(&context()).unwrap().unwrap();
    let before = provider.counters.snapshot();
    let prepared = session.prepare(&provider, &saved, &context()).unwrap();
    let after = provider.counters.snapshot();
    let read_bytes = after[Phase::Read as usize].bytes - before[Phase::Read as usize].bytes;
    assert!(
        read_bytes <= 2 * CHUNK_SIZE,
        "preparation read {read_bytes} bytes"
    );
    let repeated = provider.counters.snapshot();
    let again = session.prepare(&provider, &saved, &context()).unwrap();
    let cached_read = provider.counters.snapshot()[Phase::Read as usize].bytes
        - repeated[Phase::Read as usize].bytes;
    assert_eq!(again.content(), prepared.content());
    assert!(
        cached_read <= 2 * CHUNK_SIZE,
        "repeated preparation read {cached_read} bytes"
    );
    let mut expected = bytes.clone();
    expected[13 * CHUNK_SIZE as usize + 317..13 * CHUNK_SIZE as usize + 4413].fill(78);
    let root = provider
        .import(&mut expected.as_slice(), expected.len() as u64, &context())
        .unwrap();
    assert_eq!(prepared.content(), &root);
    let mut jobs = Vec::new();
    for value in [79, 80] {
        let mut save = intent(bytes.len() as u64);
        save.base.content = reference.clone();
        let session = journal.create(save, provider.clone(), &context()).unwrap();
        session
            .write_at(CHUNK_SIZE + 71, &[value; 4096], &context())
            .unwrap();
        let saved = session.checkpoint(&context()).unwrap().unwrap();
        let target = provider.clone();
        jobs.push(std::thread::spawn(move || {
            let ctx = ReadContext {
                deadline: Instant::now() + Duration::from_secs(5),
                cancel: Cancel::new(),
            };
            let prepared = session.prepare(&target, &saved, &ctx)?;
            let mut out = [0; 4096];
            target.read_range(prepared.pin(), CHUNK_SIZE + 71, &mut out, &ctx)?;
            anyhow::ensure!(out == [value; 4096], "parallel save selected wrong bytes");
            Ok::<_, anyhow::Error>(prepared)
        }));
    }
    for job in jobs {
        job.join().unwrap().unwrap();
    }
    assert_eq!(provider.buffer_usage().0, 0);
    println!(
        "WAVE6_PREPARE {}",
        serde_json::json!({"scenario":"current_parallel_prepare","source_bytes":bytes.len(),"read_bytes":read_bytes,"cached_prepare_read_bytes":cached_read,"whole_file_conversion":false,"parallel_preparations":2,"deadline_ms":5000})
    );
}

#[test]
fn corrupted_current_base_chunk_never_returns_a_publishable_preparation() {
    use tkfs::content::LocalContent;
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(LocalContent::open(dir.path()).unwrap());
    let bytes = vec![93; 200_000];
    let base = provider
        .import(&mut bytes.as_slice(), bytes.len() as u64, &context())
        .unwrap();
    let mut bad = bytes[..CHUNK_SIZE as usize].to_vec();
    let digest = Digest::bytes(&bad);
    bad[0] ^= 1;
    fs::write(dir.path().join("objects").join(digest.to_string()), bad).unwrap();
    let (journal, session) = native_session(dir.path(), provider.clone(), base);
    session.write_at(70000, b"edit", &context()).unwrap();
    let saved = session.checkpoint(&context()).unwrap().unwrap();
    assert!(
        session
            .prepare(&provider, &saved, &context())
            .unwrap_err()
            .to_string()
            .contains("CORRUPT_CONTENT")
    );
    assert_eq!(journal.pending(None, 256, &context()).unwrap(), vec![saved]);
    assert_eq!(provider.buffer_usage().0, 0);
}

#[test]
fn missing_overlay_map_entries_are_corruption_instead_of_unchanged_base_bytes() {
    use tkfs::content::LocalContent;
    for durable in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let provider = Arc::new(LocalContent::open(dir.path()).unwrap());
        let bytes = vec![121; CHUNK_SIZE as usize];
        let root = provider
            .import(&mut bytes.as_slice(), CHUNK_SIZE, &context())
            .unwrap();
        let (_journal, session) = native_session(dir.path(), provider.clone(), root);
        session.write_at(17, &[122; 100], &context()).unwrap();
        let saved = if durable {
            session.checkpoint(&context()).unwrap()
        } else {
            None
        };
        let db = rusqlite::Connection::open(dir.path().join("write-sessions-v1.sqlite")).unwrap();
        discard_overlay_root(
            &db,
            session.operation(),
            if durable { "saved" } else { "working" },
        );
        let error = if let Some(saved) = saved {
            session
                .prepare(&provider, &saved, &context())
                .map(|_| ())
                .unwrap_err()
        } else {
            session
                .read_range(17, &mut [0; 100], &context())
                .unwrap_err()
        };
        assert!(
            error.to_string().contains("CORRUPT_WRITE_JOURNAL"),
            "{error}"
        );
    }
}

#[test]
fn recovery_uses_the_saved_map_when_a_newer_buffered_header_is_damaged() {
    let (dir, journal, base, old) = setup(CHUNK_SIZE, false);
    old.write_at(0, &vec![131; CHUNK_SIZE as usize], &context())
        .unwrap();
    let saved = old.checkpoint(&context()).unwrap().unwrap();
    old.write_at(0, &vec![132; CHUNK_SIZE as usize], &context())
        .unwrap();
    let db = rusqlite::Connection::open(dir.path().join("write-sessions-v1.sqlite")).unwrap();
    db.execute("UPDATE sessions SET working=zeroblob(1048576)", [])
        .unwrap();
    assert!(old.read_range(0, &mut [0], &context()).is_err());
    assert_eq!(journal.pending(None, 256, &context()).unwrap(), vec![saved]);
    let resumed = journal.resume(old.operation(), base, &context()).unwrap();
    assert_eq!(
        read(&resumed, CHUNK_SIZE as usize),
        vec![131; CHUNK_SIZE as usize]
    );
    assert!(old.write_at(0, b"obsolete", &context()).is_err());
    let mut cursor = None;
    let mut removed = 0;
    loop {
        let page = journal.reclaim_orphans(cursor, &context()).unwrap();
        assert!(page.validated + page.examined + page.nodes_examined <= 256);
        removed += page.removed;
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(removed, 1);
}

#[test]
fn reclamation_retains_payloads_when_a_map_or_entire_intent_disappears() {
    for entire_scope in [false, true] {
        let (dir, journal, _base, session) = setup(CHUNK_SIZE, false);
        session.write_at(0, b"preserved", &context()).unwrap();
        session.checkpoint(&context()).unwrap();
        let db = rusqlite::Connection::open(dir.path().join("write-sessions-v1.sqlite")).unwrap();
        discard_overlay_root(&db, session.operation(), "saved");
        if entire_scope {
            db.execute("DELETE FROM sessions", []).unwrap();
        }
        let mut cursor = None;
        let mut stopped = false;
        for _ in 0..8 {
            match journal.reclaim_orphans(cursor, &context()) {
                Ok(page) => {
                    assert_eq!(page.removed, 0);
                    cursor = page.next;
                    if cursor.is_none() {
                        break;
                    }
                }
                Err(error) => {
                    assert!(error.to_string().contains("CORRUPT_WRITE_JOURNAL"));
                    stopped = true;
                    break;
                }
            }
        }
        assert!(
            stopped,
            "damaged reachability was accepted as proof of absence"
        );
        let retained: usize = db
            .query_row("SELECT count(*) FROM payloads", [], |r| r.get(0))
            .unwrap();
        assert_eq!(retained, 1);
    }
}

#[test]
fn reclamation_restarts_its_bounded_audit_after_a_foreign_commit() {
    let (dir, journal, _base, session) = setup(400 * CHUNK_SIZE, false);
    for index in 0..300u64 {
        session
            .write_at(index * CHUNK_SIZE, &index.to_le_bytes(), &context())
            .unwrap();
    }
    session.checkpoint(&context()).unwrap();
    let first = journal.reclaim_orphans(None, &context()).unwrap();
    assert_eq!(first.validated, 256);
    assert_eq!(first.examined, 0);
    assert_eq!(first.removed, 0);
    assert!(first.next.is_some());
    let db = rusqlite::Connection::open(dir.path().join("write-sessions-v1.sqlite")).unwrap();
    discard_overlay_root(&db, session.operation(), "saved");
    let mut cursor = first.next;
    let mut stopped = false;
    for _ in 0..8 {
        match journal.reclaim_orphans(cursor, &context()) {
            Ok(page) => {
                assert!(page.validated + page.examined + page.nodes_examined <= 256);
                assert_eq!(page.removed, 0);
                cursor = page.next;
                if cursor.is_none() {
                    break;
                }
            }
            Err(error) => {
                assert!(error.to_string().contains("CORRUPT_WRITE_JOURNAL"));
                stopped = true;
                break;
            }
        }
    }
    assert!(stopped);
    let retained: usize = db
        .query_row("SELECT count(*) FROM payloads", [], |r| r.get(0))
        .unwrap();
    assert_eq!(retained, 300);
    println!(
        "WAVE6_PREPARE {}",
        serde_json::json!({"scenario":"corrupt_map_retention","audit_slice":256,"foreign_commit_invalidates_cursor":true,"retained_payloads":retained})
    );
}

#[test]
fn reclamation_cursors_cannot_transfer_an_audit_between_journal_instances() {
    let mut worlds = Vec::new();
    for _ in 0..2 {
        let (dir, journal, base, session) = setup(400 * CHUNK_SIZE, false);
        for index in 0..300u64 {
            session
                .write_at(index * CHUNK_SIZE, &index.to_le_bytes(), &context())
                .unwrap();
        }
        session.checkpoint(&context()).unwrap();
        worlds.push((dir, journal, base, session));
    }
    let first = worlds[0].1.reclaim_orphans(None, &context()).unwrap();
    assert_eq!(first.validated, 256);
    assert_eq!(first.examined, 0);
    let mut cursor = first.next;
    loop {
        let page = worlds[1].1.reclaim_orphans(cursor, &context()).unwrap();
        assert!(page.validated + page.examined + page.nodes_examined <= 256);
        assert_eq!(page.removed, 0);
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    let mut out = [0; 8];
    worlds[1]
        .3
        .read_range(299 * CHUNK_SIZE, &mut out, &context())
        .unwrap();
    assert_eq!(out, 299u64.to_le_bytes());
    for (dir, journal, base, session) in worlds {
        drop(session);
        drop(base);
        drop(journal);
        dir.close().unwrap();
    }
}

#[test]
fn damaged_reclamation_state_does_not_fail_unrelated_healthy_file_writes() {
    let (dir, journal, base, bad) = setup(CHUNK_SIZE, false);
    bad.write_at(0, b"bad scope", &context()).unwrap();
    bad.checkpoint(&context()).unwrap();
    let db = rusqlite::Connection::open(dir.path().join("write-sessions-v1.sqlite")).unwrap();
    let hash = overlay_root(&db, bad.operation(), "saved").digest();
    let body: Vec<u8> = db
        .query_row(
            "SELECT body FROM private_overlay_nodes_v1 WHERE hash=?",
            [hash.0.as_slice()],
            |r| r.get(0),
        )
        .unwrap();
    discard_overlay_root(&db, bad.operation(), "saved");
    let healthy = journal.create(intent(0), base.clone(), &context()).unwrap();
    healthy.write_at(0, b"one", &context()).unwrap();
    healthy.checkpoint(&context()).unwrap();
    healthy.write_at(0, b"two", &context()).unwrap();
    healthy.write_at(0, b"three", &context()).unwrap();
    assert!(
        journal
            .reclamation_error()
            .is_some_and(|e| e.contains("CORRUPT_WRITE_JOURNAL"))
    );
    healthy.checkpoint(&context()).unwrap();
    let recovered = journal
        .resume(healthy.operation(), base, &context())
        .unwrap();
    assert_eq!(read(&recovered, 5), b"three");
    db.execute(
        "INSERT INTO private_overlay_nodes_v1(hash,body) VALUES(?,?)",
        rusqlite::params![hash.0.as_slice(), body],
    )
    .unwrap();
    let mut cursor = None;
    loop {
        let page = journal.reclaim_orphans(cursor, &context()).unwrap();
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert!(journal.reclamation_error().is_none());
}

#[test]
fn late_or_cancelled_preparation_keeps_recovery_and_releases_buffer_capacity() {
    use tkfs::content::LocalContent;
    for cancel in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let target = Arc::new(LocalContent::open(dir.path()).unwrap());
        let bytes: Vec<_> = (0..3 * CHUNK_SIZE).map(original).collect();
        let base = target
            .import(&mut bytes.as_slice(), bytes.len() as u64, &context())
            .unwrap();
        let fake = Base::new(true);
        let journal = WriteJournal::open(dir.path()).unwrap();
        let mut save = intent(bytes.len() as u64);
        save.base.content = base;
        let session = Arc::new(journal.create(save, fake.clone(), &context()).unwrap());
        session
            .write_at(CHUNK_SIZE + 17, b"edit", &context())
            .unwrap();
        let saved = session.checkpoint(&context()).unwrap().unwrap();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        *fake.block.lock().unwrap() = Some((entered.clone(), release.clone()));
        let preparing = session.clone();
        let provider = target.clone();
        let selected = saved.clone();
        let ctx = context();
        let stop = ctx.cancel.clone();
        let job = std::thread::spawn(move || preparing.prepare(&provider, &selected, &ctx));
        entered.wait();
        if cancel {
            stop.cancel();
        } else {
            session.set_len(0, &context()).unwrap();
            session.checkpoint(&context()).unwrap();
        }
        release.wait();
        let error = job.join().unwrap().unwrap_err().to_string();
        assert!(
            error.contains(if cancel {
                "CONTENT_CANCELLED"
            } else {
                "STALE_WRITE_CHECKPOINT"
            }),
            "{error}"
        );
        assert_eq!(target.buffer_usage().0, 0);
        assert_eq!(journal.pending(None, 256, &context()).unwrap().len(), 1);
    }
}

#[test]
fn stale_preparations_retire_while_unrelated_saves_still_hold_all_background_buffers() {
    use std::{
        io::{self, Read},
        sync::{Condvar, mpsc},
    };
    use tkfs::content::LocalContent;
    struct Slow {
        entered: Arc<Barrier>,
        release: Arc<(Mutex<bool>, Condvar)>,
        waited: bool,
    }
    impl Read for Slow {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            if !self.waited {
                self.waited = true;
                self.entered.wait();
                let mut ready = self.release.0.lock().unwrap();
                while !*ready {
                    ready = self.release.1.wait(ready).unwrap();
                }
            }
            out.fill(201);
            Ok(out.len())
        }
    }
    for len in [32, 2 * CHUNK_SIZE] {
        let dir = tempfile::tempdir().unwrap();
        let provider = Arc::new(LocalContent::open(dir.path()).unwrap());
        let data = vec![190; len as usize];
        let base = provider
            .import(&mut data.as_slice(), data.len() as u64, &context())
            .unwrap();
        let (_journal, session) = native_session(dir.path(), provider.clone(), base);
        session.write_at(17, b"edit", &context()).unwrap();
        let saved = session.checkpoint(&context()).unwrap().unwrap();
        let entered = Arc::new(Barrier::new(3));
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let mut producers = Vec::new();
        for _ in 0..2 {
            let target = provider.clone();
            let entered = entered.clone();
            let release = release.clone();
            producers.push(std::thread::spawn(move || {
                target
                    .import(
                        &mut Slow {
                            entered,
                            release,
                            waited: false,
                        },
                        CHUNK_SIZE,
                        &context(),
                    )
                    .unwrap()
            }));
        }
        entered.wait();
        assert_eq!(provider.buffer_usage().0, 6);
        let preparing = session.clone();
        let target = provider.clone();
        let (tx, rx) = mpsc::channel();
        let job = std::thread::spawn(move || {
            let result = preparing.prepare(&target, &saved, &context());
            tx.send(result).unwrap();
        });
        std::thread::sleep(Duration::from_millis(100));
        assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
        session.set_len(0, &context()).unwrap();
        session.checkpoint(&context()).unwrap();
        let began = Instant::now();
        let ended = rx.recv_timeout(Duration::from_millis(250));
        let elapsed = began.elapsed();
        *release.0.lock().unwrap() = true;
        release.1.notify_all();
        for producer in producers {
            producer.join().unwrap();
        }
        job.join().unwrap();
        assert!(
            ended
                .expect("obsolete preparation waited for unrelated buffers")
                .unwrap_err()
                .to_string()
                .contains("STALE_WRITE_CHECKPOINT")
        );
        assert_eq!(provider.buffer_usage().0, 0);
        println!(
            "WAVE6_PREPARE {}",
            serde_json::json!({"scenario":"stale_buffer_wait","base_len":len,"retired_ms":elapsed.as_secs_f64()*1000.0,"bound_ms":250,"blocked_background_buffers":6,"retired_before_background_release":true})
        );
    }
}

#[test]
fn prepared_pipeline_matches_bounded_overwrite_on_a_prechunked_gib() {
    use tkfs::{content::LocalContent, model::content::ManifestNode, util::phases::Phase};
    let source = tempfile::tempdir().unwrap();
    let baseline = Arc::new(LocalContent::open(source.path()).unwrap());
    let len = 1u64 << 30;
    let root = baseline
        .import(&mut std::io::repeat(b'x'), len, &context())
        .unwrap();
    let base_pin = baseline.pin(&root).unwrap();
    let candidate = tempfile::tempdir().unwrap();
    let target = Arc::new(LocalContent::open(candidate.path()).unwrap());
    for folder in ["objects", "manifests-v1"] {
        for entry in fs::read_dir(source.path().join(folder)).unwrap() {
            let path = entry.unwrap().path();
            let copied = candidate
                .path()
                .join(folder)
                .join(path.file_name().unwrap());
            fs::copy(path, &copied).unwrap();
            fs::OpenOptions::new()
                .write(true)
                .open(copied)
                .unwrap()
                .sync_all()
                .unwrap();
        }
    }
    let journal = WriteJournal::open(candidate.path()).unwrap();
    for repetition in 0..6 {
        let value = 100 + repetition as u8;
        let patch = [value; 4096];
        let offset = len / 2;
        let start = Instant::now();
        let reference = baseline
            .overwrite(&base_pin, offset, &patch, &context())
            .unwrap();
        let baseline_ms = start.elapsed().as_secs_f64() * 1000.0;
        let mut save = intent(len);
        save.base.content = root.clone();
        let session = journal.create(save, target.clone(), &context()).unwrap();
        let before = target.counters.snapshot();
        let start = Instant::now();
        session.write_at(offset, &patch, &context()).unwrap();
        let saved = session.checkpoint(&context()).unwrap().unwrap();
        let prepared = session.prepare(&target, &saved, &context()).unwrap();
        let elapsed = start.elapsed();
        let after = target.counters.snapshot();
        assert_eq!(prepared.content(), &reference);
        let ops = |p: Phase| after[p as usize].operations - before[p as usize].operations;
        let bytes = |p: Phase| after[p as usize].bytes - before[p as usize].bytes;
        assert_eq!(ops(Phase::Write), 1);
        assert_eq!(bytes(Phase::Write), CHUNK_SIZE);
        assert_eq!(
            ops(Phase::ManifestWrite),
            ManifestNode::root_level(len).unwrap() as u64 + 1
        );
        assert_eq!(bytes(Phase::Read), CHUNK_SIZE);
        assert!(bytes(Phase::Hash) < 5 * CHUNK_SIZE);
        assert_eq!(journal.queued_patch_bytes(&context()).unwrap(), 0);
        println!(
            "WAVE6_PREPARE {}",
            serde_json::json!({"scenario":"gib_pipeline","base_bytes":len,"patch_bytes":4096,"warmup":repetition==0,"repetition":repetition,"baseline_overwrite_ms":baseline_ms,"journal_prepare_ms":elapsed.as_secs_f64()*1000.0,"new_data_objects":ops(Phase::Write),"data_write_bytes":bytes(Phase::Write),"base_data_read_bytes":bytes(Phase::Read),"manifest_pages_written":ops(Phase::ManifestWrite),"manifest_pages_read":ops(Phase::ManifestRead),"canonical_hash_bytes":bytes(Phase::Hash),"same_reference_as_overwrite":true})
        );
    }
    let mut save = intent(len);
    save.base.content = root.clone();
    let session = journal.create(save, target.clone(), &context()).unwrap();
    session
        .write_at(CHUNK_SIZE - 2048, &[8; 4096], &context())
        .unwrap();
    let saved = session.checkpoint(&context()).unwrap().unwrap();
    let before = target.counters.snapshot();
    let prepared = session.prepare(&target, &saved, &context()).unwrap();
    let after = target.counters.snapshot();
    assert_eq!(
        after[Phase::Write as usize].operations - before[Phase::Write as usize].operations,
        2
    );
    let mut out = [0; 4096];
    target
        .read_range(prepared.pin(), CHUNK_SIZE - 2048, &mut out, &context())
        .unwrap();
    assert_eq!(out, [8; 4096]);
    baseline
        .read_range(&base_pin, CHUNK_SIZE - 2048, &mut out, &context())
        .unwrap();
    assert_eq!(out, [b'x'; 4096]);
    println!(
        "WAVE6_PREPARE {}",
        serde_json::json!({"scenario":"gib_cross_boundary","base_bytes":len,"patch_bytes":4096,"new_data_objects":2,"base_unchanged":true})
    );
}
