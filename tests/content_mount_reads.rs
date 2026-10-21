//! Descriptor-backed runtime and actual isolated adapter reads. Typed fixtures
//! do not activate user repositories or implement native mounted writes.
use std::{
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tkfs::{
    content::{Availability, ContentProvider, LocalContent, PinnedContent, ReadContext},
    core::{Store, id},
    model::{
        content::{CHUNK_SIZE, ContentKind, Digest, FileContentRef},
        workspace::{ByteRange, limits},
    },
    runtime::{Engine, Shared, content::ContentWait, content_reads},
    util::Cancel,
};
fn context() -> ReadContext {
    ReadContext {
        deadline: Instant::now() + Duration::from_secs(20),
        cancel: Cancel::new(),
    }
}
fn wait() -> ContentWait {
    ContentWait::fixed(Duration::from_secs(20))
}
fn error<T>(result: anyhow::Result<T>, code: &str) {
    let error = result.err().expect("expected error");
    assert!(format!("{error:#}").contains(code), "{error:#}");
}
struct Fixture {
    shared: Shared,
    native: Arc<LocalContent>,
    reference: FileContentRef,
    data: Vec<u8>,
    entity: String,
    dir: tempfile::TempDir,
}
fn publish(store: &mut Store, entity: &str, reference: &FileContentRef) -> String {
    let change = store
        .current_change(entity, "content", serde_json::json!(reference))
        .unwrap();
    let event = store.event(&store.branch(&store.active).unwrap(), vec![change]);
    let id = event.id.clone();
    store.commit(event, true).unwrap();
    id
}
impl Fixture {
    fn new(size: usize) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::initialize(dir.path(), "mounted-content-read", &id()).unwrap();
        store
            .db
            .execute("INSERT INTO config VALUES('fixture:content_v1','1')", [])
            .unwrap();
        let entity = store.create("file", "file").unwrap().id;
        let native = Arc::new(LocalContent::open(dir.path()).unwrap());
        let data: Vec<u8> = (0..size)
            .map(|i| ((i / CHUNK_SIZE as usize) * 71 + i % 251) as u8)
            .collect();
        let reference = native
            .import(&mut data.as_slice(), size as u64, &context())
            .unwrap();
        publish(&mut store, &entity, &reference);
        Self {
            shared: Arc::new(Mutex::new(Engine::new(store))),
            native,
            reference,
            data,
            entity,
            dir,
        }
    }
    fn handle(&self) -> u64 {
        tkfs::runtime::lock_engine(&self.shared)
            .open("file", None, false)
            .unwrap()
            .0
    }
    fn open(&self) -> u64 {
        let h = self.handle();
        content_reads::prepare_open(&self.shared, h, wait()).unwrap();
        h
    }
    fn close(&self, h: u64) {
        tkfs::runtime::engine::close_handle(&self.shared, h).unwrap();
    }
    fn update(&self, reference: &FileContentRef) {
        let mut e = tkfs::runtime::lock_engine(&self.shared);
        let before = e.capture_before();
        let id = publish(&mut e.store, &self.entity, reference);
        let delta = e.activation_delta(&[id], before).unwrap();
        e.after_activation(&delta);
    }
    fn missing_chunk(&self, index: usize) {
        let start = index * CHUNK_SIZE as usize;
        let end = self.data.len().min(start + CHUNK_SIZE as usize);
        std::fs::remove_file(
            self.dir
                .path()
                .join("objects")
                .join(Digest::bytes(&self.data[start..end]).to_string()),
        )
        .unwrap();
    }
}
#[test]
fn descriptor_size_open_pin_and_ready_range_do_not_require_other_chunks() {
    let f = Fixture::new(3 * CHUNK_SIZE as usize);
    f.missing_chunk(2);
    {
        let e = tkfs::runtime::lock_engine(&f.shared);
        let entry = e.store.entry(&f.entity).unwrap();
        assert_eq!(e.visible_size(&entry).unwrap(), f.data.len());
    }
    let h = f.open();
    assert!(f.native.pinned_roots().1.contains(&f.reference));
    let mut bytes = [0; 257];
    let result = content_reads::read_content_range(&f.shared, h, 13, &mut bytes, &context())
        .unwrap()
        .unwrap();
    assert_eq!(result.count, 257);
    assert_eq!(&bytes, &f.data[13..270]);
    error(
        content_reads::read_content_range(&f.shared, h, 2 * CHUNK_SIZE, &mut bytes, &context()),
        "CONTENT_UNAVAILABLE",
    );
    assert_eq!(
        std::fs::read_dir(f.dir.path().join("staging"))
            .map(|dir| dir.count())
            .unwrap_or(0),
        0
    );
    f.close(h);
    assert!(!f.native.pinned_roots().1.contains(&f.reference));
}
#[test]
fn missing_manifest_and_corrupt_demanded_bytes_never_become_empty_or_zero_data() {
    for missing_root in [true, false] {
        let f = Fixture::new(3 * CHUNK_SIZE as usize);
        let h = f.open();
        if missing_root {
            std::fs::remove_file(
                f.dir
                    .path()
                    .join("manifests-v1")
                    .join(f.reference.digest.to_string()),
            )
            .unwrap();
        } else {
            std::fs::write(
                f.dir
                    .path()
                    .join("objects")
                    .join(Digest::bytes(&f.data[..CHUNK_SIZE as usize]).to_string()),
                vec![0; CHUNK_SIZE as usize],
            )
            .unwrap();
        }
        let mut out = [19; 64];
        let result = content_reads::read_content_range(&f.shared, h, 0, &mut out, &context());
        assert!(result.is_err());
        assert_eq!(
            tkfs::runtime::lock_engine(&f.shared)
                .session(h)
                .unwrap()
                .bytes
                .len(),
            f.data.len()
        );
        f.close(h);
    }
}
#[test]
fn empty_eof_and_read_only_guards_preserve_descriptor_semantics() {
    for size in [0, 17] {
        let f = Fixture::new(size);
        let h = f.open();
        let mut out = [33; 40];
        let result =
            content_reads::read_content_range(&f.shared, h, size as u64, &mut out, &context())
                .unwrap()
                .unwrap();
        assert_eq!(result.count, 0);
        assert_eq!(result.length, size as u64);
        assert_eq!(out, [33; 40]);
        error(
            tkfs::runtime::lock_engine(&f.shared).open("file", None, true),
            "PEER_NEEDS_UPGRADE",
        );
        let mut e = tkfs::runtime::lock_engine(&f.shared);
        let session = e.session_mut(h).unwrap();
        assert!(session.bytes.snapshot().is_err());
        assert!(session.bytes.resize(f.dir.path(), 0).is_err());
        assert!(session.bytes.read_at(0, &mut out).is_err());
        assert!(!session.dirty);
        drop(e);
        f.close(h);
    }
}
struct Probe {
    native: Arc<LocalContent>,
    engine: Weak<Mutex<Engine>>,
    pins: AtomicUsize,
    reads: Mutex<Vec<(u64, usize)>>,
    stage: u8,
    entered: AtomicBool,
    release: AtomicBool,
}
impl Probe {
    fn new(f: &Fixture, stage: u8) -> Arc<Self> {
        Arc::new(Self {
            native: f.native.clone(),
            engine: Arc::downgrade(&f.shared),
            pins: AtomicUsize::new(0),
            reads: Mutex::new(Vec::new()),
            stage,
            entered: AtomicBool::new(false),
            release: AtomicBool::new(false),
        })
    }
    fn gate(&self, context: Option<&ReadContext>) -> anyhow::Result<()> {
        self.entered.store(true, Ordering::Release);
        let until = Instant::now() + Duration::from_secs(3);
        while !self.release.load(Ordering::Acquire) {
            if let Some(context) = context {
                context.check()?;
            }
            anyhow::ensure!(Instant::now() < until, "CONTENT_DEADLINE");
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    }
    fn outside(&self) {
        assert!(
            self.engine.upgrade().unwrap().try_lock().is_ok(),
            "provider operation held engine mutex"
        );
    }
    fn entered(&self) {
        let until = Instant::now() + Duration::from_secs(2);
        while !self.entered.load(Ordering::Acquire) && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(self.entered.load(Ordering::Acquire));
    }
}
impl ContentProvider for Probe {
    fn pin(&self, reference: &FileContentRef) -> anyhow::Result<PinnedContent> {
        self.outside();
        self.pins.fetch_add(1, Ordering::Relaxed);
        if self.stage == 1 {
            self.gate(None)?;
        }
        if self.stage == 3 {
            return self.native.pin(&FileContentRef::new(
                ContentKind::Direct,
                Digest::bytes(b"wrong"),
                5,
            )?);
        }
        self.native.pin(reference)
    }
    fn availability(
        &self,
        pin: &PinnedContent,
        range: ByteRange,
        context: &ReadContext,
    ) -> anyhow::Result<Availability> {
        self.native.availability(pin, range, context)
    }
    fn read_range(
        &self,
        pin: &PinnedContent,
        offset: u64,
        out: &mut [u8],
        context: &ReadContext,
    ) -> anyhow::Result<()> {
        self.outside();
        self.reads.lock().unwrap().push((offset, out.len()));
        if self.stage == 2 {
            self.gate(Some(context))?;
        }
        self.native.read_range(pin, offset, out, context)
    }
}
fn install(f: &Fixture, probe: Arc<Probe>) {
    tkfs::runtime::lock_engine(&f.shared)
        .install_content_provider(probe)
        .unwrap();
}
#[test]
fn large_windows_destination_uses_bounded_calls_and_fuse_reply_stays_bounded() {
    let f = Fixture::new(2 * limits::READ_BYTES + 31);
    let probe = Probe::new(&f, 0);
    install(&f, probe.clone());
    let h = f.open();
    let mut out = vec![9; 2 * limits::READ_BYTES];
    assert_eq!(
        content_reads::read_content_range(&f.shared, h, 0, &mut out, &context())
            .unwrap()
            .unwrap()
            .count,
        limits::READ_BYTES
    );
    assert_eq!(
        content_reads::read_mount_range(&f.shared, h, 0, &mut out, wait())
            .unwrap()
            .unwrap()
            .count,
        out.len()
    );
    assert_eq!(out, f.data[..out.len()]);
    let bytes = content_reads::read_mount_vec(&f.shared, h, 0, usize::MAX, wait())
        .unwrap()
        .unwrap();
    assert_eq!(bytes.len(), limits::READ_BYTES);
    assert_eq!(bytes, f.data[..bytes.len()]);
    assert_eq!(probe.pins.load(Ordering::Acquire), 1);
    let calls = probe.reads.lock().unwrap();
    assert_eq!(calls.len(), 4);
    assert!(calls.iter().all(|(_, len)| *len <= limits::READ_BYTES));
    drop(calls);
    f.close(h);
}
#[test]
fn slow_pin_does_not_block_metadata_and_wrong_pin_is_not_installed() {
    let f = Fixture::new(67);
    let probe = Probe::new(&f, 1);
    install(&f, probe.clone());
    let h = f.handle();
    let shared = f.shared.clone();
    let job = std::thread::spawn(move || content_reads::prepare_open(&shared, h, wait()));
    probe.entered();
    let started = Instant::now();
    tkfs::runtime::lock_engine(&f.shared)
        .store
        .create("other", "file")
        .unwrap();
    assert!(started.elapsed() < Duration::from_millis(300));
    probe.release.store(true, Ordering::Release);
    job.join().unwrap().unwrap();
    f.close(h);
    let f = Fixture::new(67);
    let probe = Probe::new(&f, 3);
    install(&f, probe);
    let h = f.handle();
    error(
        content_reads::prepare_open(&f.shared, h, wait()),
        "CONTENT_PROVIDER_MISMATCH",
    );
    assert!(!f.native.pinned_roots().1.contains(&f.reference));
    f.close(h);
}
#[test]
fn revision_change_during_slow_read_discards_old_output_and_metadata_stays_live() {
    let f = Fixture::new(129);
    let probe = Probe::new(&f, 2);
    install(&f, probe.clone());
    let h = f.open();
    let shared = f.shared.clone();
    let job = std::thread::spawn(move || {
        let mut out = [0; 17];
        content_reads::read_content_range(&shared, h, 0, &mut out, &context())
    });
    probe.entered();
    let new = f
        .native
        .import(&mut b"new".as_slice(), 3, &context())
        .unwrap();
    let started = Instant::now();
    f.update(&new);
    assert!(started.elapsed() < Duration::from_millis(300));
    probe.release.store(true, Ordering::Release);
    error(job.join().unwrap(), "STALE_VIEW");
    let mut out = [0; 3];
    content_reads::read_content_range(&f.shared, h, 0, &mut out, &context()).unwrap();
    assert_eq!(&out, b"new");
    f.close(h);
}
#[test]
fn same_descriptor_refresh_reuses_pin_and_cancelled_read_returns_no_success() {
    let f = Fixture::new(129);
    let probe = Probe::new(&f, 0);
    install(&f, probe.clone());
    let h = f.open();
    std::thread::sleep(Duration::from_millis(3));
    f.update(&f.reference);
    {
        let e = tkfs::runtime::lock_engine(&f.shared);
        let current = e.store.entry(&f.entity).unwrap();
        assert_eq!(e.session(h).unwrap().entry.modified_ms, current.modified_ms);
        assert_eq!(e.session(h).unwrap().entry.content_ref, current.content_ref);
    }
    let mut out = [0; 5];
    content_reads::read_content_range(&f.shared, h, 0, &mut out, &context()).unwrap();
    assert_eq!(probe.pins.load(Ordering::Acquire), 1);
    let context = context();
    context.cancel.cancel();
    error(
        content_reads::read_content_range(&f.shared, h, 0, &mut out, &context),
        "CONTENT_CANCELLED",
    );
    f.close(h);
}
#[test]
fn legacy_zero_wait_and_stale_handle_keep_their_existing_meanings() {
    let f = Fixture::new(17);
    let legacy = {
        let mut e = tkfs::runtime::lock_engine(&f.shared);
        e.store.create("legacy", "file").unwrap();
        e.open("legacy", None, false).unwrap().0
    };
    content_reads::prepare_open(&f.shared, legacy, ContentWait::fixed(Duration::ZERO)).unwrap();
    assert!(
        content_reads::read_mount_vec(&f.shared, legacy, 0, 10, ContentWait::fixed(Duration::ZERO))
            .unwrap()
            .is_none()
    );
    f.close(legacy);
    let h = f.open();
    tkfs::runtime::lock_engine(&f.shared).store.generation += 1;
    error(
        content_reads::read_content_range(&f.shared, h, 0, &mut [0; 4], &context()),
        "STALE_VIEW",
    );
    tkfs::runtime::lock_engine(&f.shared)
        .release_handle(h)
        .unwrap();
}

#[test]
fn adapter_metadata_acquisition_obeys_its_deadline_before_provider_io() {
    let f = Fixture::new(17);
    let probe = Probe::new(&f, 0);
    install(&f, probe.clone());
    let h = f.open();
    let guard = tkfs::runtime::lock_engine(&f.shared);
    let started = Instant::now();
    error(
        content_reads::read_mount_range(
            &f.shared,
            h,
            0,
            &mut [0; 4],
            ContentWait::fixed(Duration::from_millis(30)),
        ),
        "CONTENT_DEADLINE",
    );
    assert!(started.elapsed() < Duration::from_millis(300));
    assert!(probe.reads.lock().unwrap().is_empty());
    drop(guard);
    f.close(h);
}

#[test]
fn cancellation_during_provider_read_keeps_the_open_pin_until_close() {
    let f = Fixture::new(17);
    let probe = Probe::new(&f, 2);
    install(&f, probe.clone());
    let h = f.open();
    let context = context();
    let cancel = context.cancel.clone();
    let shared = f.shared.clone();
    let job = std::thread::spawn(move || {
        content_reads::read_content_range(&shared, h, 0, &mut [0; 4], &context)
    });
    probe.entered();
    cancel.cancel();
    error(job.join().unwrap(), "CONTENT_CANCELLED");
    assert!(f.native.pinned_roots().1.contains(&f.reference));
    f.close(h);
    assert!(!f.native.pinned_roots().1.contains(&f.reference));
}

#[test]
fn a_private_missing_descriptor_never_registers_remote_demand() {
    let f = Fixture::new(31);
    {
        let mut e = tkfs::runtime::lock_engine(&f.shared);
        let branch = e.store.fork("private", false).unwrap();
        e.store.checkout(&branch.id).unwrap();
    }
    let ctx = worker(&f);
    f.missing_chunk(0);
    let h = f.open();
    let began = Instant::now();
    error(
        content_reads::read_content_range(&f.shared, h, 0, &mut [0; 3], &context()),
        "CONTENT_UNAVAILABLE",
    );
    assert!(began.elapsed() < Duration::from_millis(300));
    no_demand(&ctx);
    assert!(
        !ctx.sync_stores
            .checkout()
            .unwrap()
            .db
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM config WHERE key='content_save_workspace')",
                [],
                |r| r.get::<_, bool>(0)
            )
            .unwrap()
    );
    f.close(h);
}
#[test]
fn worker_stop_cancels_a_range_wait_without_cancelling_its_callers_token() {
    let f = Fixture::new(41);
    let ctx = worker(&f);
    f.missing_chunk(0);
    let h = f.open();
    let request = context();
    let caller = request.cancel.clone();
    let shared = f.shared.clone();
    let read = std::thread::spawn(move || {
        content_reads::read_content_range(&shared, h, 0, &mut [0; 3], &request)
    });
    pending_demand(&ctx);
    let state = tkfs::runtime::content_rpc::state(&ctx).unwrap();
    let began = Instant::now();
    tkfs::runtime::content_rpc::stop(&ctx);
    error(read.join().unwrap(), "CONTENT_CANCELLED");
    assert!(began.elapsed() < Duration::from_millis(300));
    assert!(!caller.is_cancelled());
    assert!(
        state
            .scheduler
            .snapshots(state.provider.identity(), &ctx.repo)
            .is_empty()
    );
    f.close(h);
}

#[test]
fn sixty_four_missing_range_waits_bound_admission_and_return_all_connection_and_waiter_capacity() {
    let f = Fixture::new(43);
    let ctx = worker(&f);
    f.missing_chunk(0);
    let h = f.open();
    let cancel = Cancel::new();
    let reads: Vec<_> = (0..64)
        .map(|_| {
            let shared = f.shared.clone();
            let context = ReadContext {
                deadline: Instant::now() + Duration::from_secs(20),
                cancel: cancel.child(),
            };
            std::thread::spawn(move || {
                content_reads::read_content_range(&shared, h, 0, &mut [0; 2], &context)
            })
        })
        .collect();
    let state = tkfs::runtime::content_rpc::state(&ctx).unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        let count = state
            .scheduler
            .snapshots(state.provider.identity(), &ctx.repo)
            .len();
        assert!(count <= 64);
        if count == 64 {
            break;
        }
        assert!(
            Instant::now() < until,
            "only {count} native readers became pending"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    let began = Instant::now();
    error(
        content_reads::read_content_range(&f.shared, h, 0, &mut [0; 1], &context()),
        "CONTENT_RESOURCE_LIMIT",
    );
    assert!(began.elapsed() < Duration::from_millis(300));
    cancel.cancel();
    for read in reads {
        error(read.join().unwrap(), "CONTENT_CANCELLED");
    }
    no_demand(&ctx);
    assert!(ctx.sync_stores.idle() <= 8);
    f.close(h);
    assert!(!f.native.pinned_roots().1.contains(&f.reference));
}

fn worker(f: &Fixture) -> Arc<tkfs::runtime::ctx::WorkerCtx> {
    let ctx = tkfs::worker::build_ctx(f.shared.clone(), f.dir.path(), false, None);
    tkfs::runtime::register_worker(&ctx);
    ctx
}
fn pending_demand(ctx: &tkfs::runtime::ctx::WorkerCtx) -> tkfs::content::scheduler::DemandSnapshot {
    let state = tkfs::runtime::content_rpc::state(ctx).unwrap();
    let until = Instant::now() + Duration::from_secs(3);
    loop {
        let demands = state
            .scheduler
            .snapshots(state.provider.identity(), &ctx.repo);
        if let Some(demand) = demands.into_iter().next() {
            return demand;
        }
        assert!(Instant::now() < until, "no range demand appeared");
        std::thread::sleep(Duration::from_millis(1));
    }
}
fn no_demand(ctx: &tkfs::runtime::ctx::WorkerCtx) {
    let state = tkfs::runtime::content_rpc::state(ctx).unwrap();
    assert!(
        state
            .scheduler
            .snapshots(state.provider.identity(), &ctx.repo)
            .is_empty()
    );
}
#[test]
fn a_missing_range_invalidates_its_stale_index_and_owns_one_exact_demand_until_ready() {
    use tkfs::model::content::ObjectKey;
    let f = Fixture::new(57);
    let ctx = worker(&f);
    let h = f.open();
    let reference = {
        let mut e = tkfs::runtime::lock_engine(&f.shared);
        let reference = tkfs::core::admission::reference_id(
            e.session(h).unwrap().content_base.as_ref().unwrap(),
            &f.entity,
        );
        e.store.seed_content_reference(&reference).unwrap();
        let edge = e
            .store
            .content_range_candidates(&reference, ByteRange { offset: 3, len: 7 })
            .unwrap()
            .remove(0);
        e.store
            .verify_content_edge(&f.native, &edge, &context())
            .unwrap();
        assert!(
            e.store
                .content_object_verified(ObjectKey::root(&f.reference))
                .unwrap()
        );
        reference
    };
    f.missing_chunk(0);
    let shared = f.shared.clone();
    let read = std::thread::spawn(move || {
        let mut out = [0; 7];
        content_reads::read_content_range(&shared, h, 3, &mut out, &context())
            .unwrap()
            .unwrap();
        out
    });
    let demand = pending_demand(&ctx).demand;
    assert_eq!(demand.reference, reference);
    assert_eq!(demand.revision.entity, f.entity);
    assert_eq!(demand.range, ByteRange { offset: 3, len: 7 });
    {
        let mut store = ctx.sync_stores.checkout().unwrap();
        assert!(
            !store
                .content_object_verified(ObjectKey::root(&f.reference))
                .unwrap()
        );
        assert_eq!(
            store.content_workspace_id(&context()).unwrap(),
            demand.revision.workspace
        );
    }
    let began = Instant::now();
    tkfs::runtime::lock_engine(&f.shared)
        .store
        .create("independent", "file")
        .unwrap();
    assert!(began.elapsed() < Duration::from_millis(300));
    f.native
        .import(&mut f.data.as_slice(), f.data.len() as u64, &context())
        .unwrap();
    assert_eq!(read.join().unwrap(), f.data[3..10]);
    no_demand(&ctx);
    f.close(h);
    let mut reopened = Store::open_existing(f.dir.path()).unwrap();
    assert_eq!(
        reopened.content_workspace_id(&context()).unwrap(),
        demand.revision.workspace
    );
    assert_eq!(
        reopened
            .begin_content_save(&f.entity, &context())
            .unwrap()
            .base
            .workspace,
        demand.revision.workspace
    );
}
#[test]
fn cancelling_one_range_waiter_keeps_the_other_waiter_and_immutable_pin() {
    let f = Fixture::new(29);
    let ctx = worker(&f);
    f.missing_chunk(0);
    let h = f.open();
    let (a, b) = (context(), context());
    let cancel = a.cancel.clone();
    let shared = f.shared.clone();
    let first = std::thread::spawn(move || {
        content_reads::read_content_range(&shared, h, 0, &mut [0; 3], &a)
    });
    pending_demand(&ctx);
    let shared = f.shared.clone();
    let second = std::thread::spawn(move || {
        let mut out = [0; 4];
        content_reads::read_content_range(&shared, h, 2, &mut out, &b).unwrap();
        out
    });
    let state = tkfs::runtime::content_rpc::state(&ctx).unwrap();
    let until = Instant::now() + Duration::from_secs(3);
    while state
        .scheduler
        .snapshots(state.provider.identity(), &ctx.repo)
        .len()
        != 2
    {
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(1));
    }
    let began = Instant::now();
    cancel.cancel();
    error(first.join().unwrap(), "CONTENT_CANCELLED");
    assert!(began.elapsed() < Duration::from_millis(300));
    assert_eq!(
        state
            .scheduler
            .snapshots(state.provider.identity(), &ctx.repo)
            .len(),
        1
    );
    assert!(f.native.pinned_roots().1.contains(&f.reference));
    f.native
        .import(&mut f.data.as_slice(), f.data.len() as u64, &context())
        .unwrap();
    assert_eq!(second.join().unwrap(), f.data[2..6]);
    no_demand(&ctx);
    f.close(h);
}
#[test]
fn waiting_range_is_fenced_by_same_bytes_new_revision_or_reference_withdrawal() {
    for withdraw in [false, true] {
        let f = Fixture::new(23);
        let ctx = worker(&f);
        f.missing_chunk(0);
        let h = f.open();
        let shared = f.shared.clone();
        let read = std::thread::spawn(move || {
            content_reads::read_content_range(&shared, h, 0, &mut [0; 3], &context())
        });
        let demand = pending_demand(&ctx).demand;
        if withdraw {
            ctx.sync_stores
                .checkout()
                .unwrap()
                .set_content_admission(
                    &demand.reference,
                    1,
                    tkfs::model::workspace::AdmissionAction::Withdraw,
                )
                .unwrap();
        } else {
            f.update(&f.reference);
        }
        error(
            read.join().unwrap(),
            if withdraw {
                "STALE_ADMISSION_GENERATION"
            } else {
                "STALE_VIEW"
            },
        );
        no_demand(&ctx);
        f.close(h);
    }
}
#[test]
fn waiting_range_deadline_includes_a_contended_first_workspace_identity_write() {
    let f = Fixture::new(27);
    let ctx = worker(&f);
    f.missing_chunk(0);
    let h = f.open();
    let db = rusqlite::Connection::open(f.dir.path().join("metadata.sqlite")).unwrap();
    db.execute_batch("BEGIN IMMEDIATE").unwrap();
    let began = Instant::now();
    error(
        content_reads::read_mount_vec(
            &f.shared,
            h,
            0,
            2,
            ContentWait::fixed(Duration::from_millis(35)),
        ),
        "CONTENT_DEADLINE",
    );
    assert!(began.elapsed() < Duration::from_millis(300));
    db.execute_batch("ROLLBACK").unwrap();
    let connection = ctx.sync_stores.checkout().unwrap();
    assert_eq!(
        connection
            .db
            .query_row("PRAGMA busy_timeout", [], |r| r.get::<_, u64>(0))
            .unwrap(),
        5000
    );
    drop(connection);
    no_demand(&ctx);
    f.close(h);
}
#[test]
fn mount_wait_slots_reject_an_extra_missing_read_but_allow_a_ready_range_and_drain_on_stall() {
    use tkfs::runtime::content::WaitSlots;
    static SLOTS: WaitSlots = WaitSlots::new(1);
    let f = Fixture::new(3 * CHUNK_SIZE as usize);
    let ctx = worker(&f);
    f.missing_chunk(2);
    let h = f.open();
    let mut waiting = wait();
    waiting.stall = Duration::from_millis(150);
    waiting.slots = Some(&SLOTS);
    let shared = f.shared.clone();
    let read = std::thread::spawn(move || {
        content_reads::read_mount_vec(&shared, h, 2 * CHUNK_SIZE, 5, waiting)
    });
    pending_demand(&ctx);
    assert_eq!(SLOTS.used(), 1);
    error(
        content_reads::read_mount_vec(&f.shared, h, 2 * CHUNK_SIZE, 2, waiting),
        "CONTENT_RESOURCE_LIMIT",
    );
    assert_eq!(
        content_reads::read_mount_vec(&f.shared, h, 0, 3, waiting)
            .unwrap()
            .unwrap(),
        f.data[..3]
    );
    error(read.join().unwrap(), "CONTENT_UNAVAILABLE");
    assert_eq!(SLOTS.used(), 0);
    no_demand(&ctx);
    f.close(h);
}

#[test]
#[ignore = "explicit isolated real WinFsp/FUSE mount qualification; never a user mount"]
fn isolated_kernel_mount_serves_partial_ranges_and_refreshes_the_open_handle() {
    use std::io::{Read, Seek, SeekFrom};
    if let Some(root) = std::env::var_os("TKFS_CONTENT_MOUNT_CHILD") {
        let root = std::path::PathBuf::from(root);
        let point = root.join("isolated-mount");
        let shared = Arc::new(Mutex::new(Engine::new(
            Store::open_existing(&root).unwrap(),
        )));
        let native = Arc::new(LocalContent::open(&root).unwrap());
        tkfs::mount::bind(&shared).unwrap();
        tkfs::mount::attach(&point).unwrap();
        struct Mounted;
        impl Drop for Mounted {
            fn drop(&mut self) {
                let _ = tkfs::mount::detach(true);
            }
        }
        let mounted = Mounted;
        let path = point.join("file");
        let mut file = std::fs::File::open(&path).unwrap();
        let original = file.metadata().unwrap();
        assert_eq!(original.len(), 3 * CHUNK_SIZE);
        let mut bytes = [0; 257];
        file.seek(SeekFrom::Start(13)).unwrap();
        file.read_exact(&mut bytes).unwrap();
        let expected: Vec<u8> = (13..270).map(|i| (i % 251) as u8).collect();
        assert_eq!(bytes.as_slice(), expected);
        assert!(!native.pinned_roots().1.is_empty());
        file.seek(SeekFrom::Start(2 * CHUNK_SIZE)).unwrap();
        assert!(file.read_exact(&mut [0; 1]).is_err());
        assert!(std::fs::OpenOptions::new().write(true).open(&path).is_err());
        assert_eq!(
            std::fs::read(point.join("large")).unwrap(),
            vec![33; 2 * limits::READ_BYTES + 11]
        );
        let replacement = b"replacement";
        let reference = native
            .import(
                &mut replacement.as_slice(),
                replacement.len() as u64,
                &context(),
            )
            .unwrap();
        {
            let mut e = tkfs::runtime::lock_engine(&shared);
            let entity = e.store.lookup("file").unwrap().id;
            let before = e.capture_before();
            let event = publish(&mut e.store, &entity, &reference);
            let delta = e.activation_delta(&[event], before).unwrap();
            e.after_activation(&delta);
        }
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut bytes = [0; 11];
        file.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, replacement);
        assert_eq!(file.metadata().unwrap().len(), replacement.len() as u64);
        assert_ne!(
            file.metadata().unwrap().modified().unwrap(),
            original.modified().unwrap()
        );
        drop(file);
        let until = Instant::now() + Duration::from_secs(3);
        while !native.pinned_roots().1.is_empty() && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(native.pinned_roots().1.is_empty());
        assert!(tkfs::runtime::lock_engine(&shared).health.is_none());
        tkfs::mount::detach(false).unwrap();
        drop(mounted);
        let result = serde_json::json!({"platform":std::env::consts::OS,"real_kernel_mount":true,"descriptor_bytes":3*CHUNK_SIZE,"ready_range_bytes":257,"unrelated_missing_chunk":true,"missing_demand_rejected":true,"open_handle_refresh_bytes":11,"timestamp_refreshed":true,"large_read_bytes":2*limits::READ_BYTES+11,"pin_released_after_close":true,"native_write_open_refused":true,"activation":"isolated fixture only"});
        std::fs::write(
            root.join("mount-result.json"),
            serde_json::to_vec(&result).unwrap(),
        )
        .unwrap();
        return;
    }
    let f = Fixture::new(3 * CHUNK_SIZE as usize);
    f.missing_chunk(2);
    let data = vec![33; 2 * limits::READ_BYTES + 11];
    let large = f
        .native
        .import(&mut data.as_slice(), data.len() as u64, &context())
        .unwrap();
    {
        let mut e = tkfs::runtime::lock_engine(&f.shared);
        let entry = e.store.create("large", "file").unwrap();
        publish(&mut e.store, &entry.id, &large);
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "isolated_kernel_mount_serves_partial_ranges_and_refreshes_the_open_handle",
            "--ignored",
            "--nocapture",
        ])
        .env("TKFS_CONTENT_MOUNT_CHILD", f.dir.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "kernel fixture failed {:?}\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value =
        serde_json::from_slice(&std::fs::read(f.dir.path().join("mount-result.json")).unwrap())
            .unwrap();
    println!("WAVE6_MOUNT_READ {result}");
}
