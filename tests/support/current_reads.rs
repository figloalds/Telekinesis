//! Current immutable range-read fixtures. Feed only complete verified chunks;
//! unverified spool growth is deliberately not a file-read progress signal.
#![allow(dead_code)]
use std::{
    path::Path,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};
use tkfs::{
    content::{
        LocalContent, ReadContext,
        receipt::{self, ReceivedFrame},
    },
    core::{Store, admission::reference_id, id},
    model::content::{CHUNK_SIZE, ContentKind, Digest, FileContentRef, ObjectKey, ObjectKind},
    runtime::{self, Engine, Shared, content::ContentWait, ctx::WorkerCtx},
    util::Cancel,
};

pub fn context() -> ReadContext {
    ReadContext {
        deadline: Instant::now() + Duration::from_secs(30),
        cancel: Cancel::new(),
    }
}
pub fn committed(shared: &Shared, path: &str, bytes: &[u8]) -> tkfs::core::Entry {
    let root = runtime::lock_engine(shared).store.root.clone();
    let mut store = Store::open_existing(&root).unwrap();
    let file = store.create(path, "file").unwrap();
    store
        .write_revision(&file.id, file.revisions.get("content").cloned(), bytes)
        .unwrap();
    store.lookup(path).unwrap()
}
pub struct CurrentRead {
    pub shared: Shared,
    pub ctx: Arc<WorkerCtx>,
    pub native: Arc<LocalContent>,
    pub reference: FileContentRef,
    pub reference_id: String,
    pub generation: u64,
    pub handle: u64,
    pub data: Vec<u8>,
    stop: Cancel,
    feeders: Mutex<Vec<thread::JoinHandle<()>>>,
}
impl CurrentRead {
    pub fn new(root: &Path, data: Vec<u8>) -> Self {
        let shared = Arc::new(Mutex::new(Engine::new(
            Store::initialize(root, "current-range-read", &id()).unwrap(),
        )));
        committed(&shared, "file", &data);
        Self::from_shared(shared, "file", data)
    }
    pub fn from_shared(shared: Shared, path: &str, data: Vec<u8>) -> Self {
        let root = runtime::lock_engine(&shared).store.root.clone();
        let native = Arc::new(LocalContent::open(&root).unwrap());
        let mut store = Store::open_existing(&root).unwrap();
        let entry = store.lookup(path).unwrap();
        let reference = entry.content_ref.clone().unwrap();
        assert_eq!(reference.total_len, data.len() as u64);
        let reference_id = reference_id(entry.revisions.get("content").unwrap(), &entry.id);
        store.seed_content_reference(&reference_id).unwrap();
        if reference.kind == ContentKind::Manifest {
            loop {
                let items: Vec<_> = store
                    .content_candidates(256, true)
                    .unwrap()
                    .into_iter()
                    .filter(|item| item.reference == reference_id)
                    .collect();
                if items.is_empty() {
                    break;
                }
                for item in items {
                    store
                        .expand_content_edge(&native, &item, &context())
                        .unwrap();
                }
            }
        }
        let generation = store.content_root(&reference_id).unwrap().0;
        let ctx = tkfs::worker::build_ctx(shared.clone(), &root, false, None);
        runtime::register_worker(&ctx);
        let handle = runtime::lock_engine(&shared)
            .open(path, None, false)
            .unwrap()
            .0;
        runtime::content_reads::prepare_open(
            &shared,
            handle,
            ContentWait::fixed(Duration::from_secs(30)),
        )
        .unwrap();
        Self {
            shared,
            ctx,
            native,
            reference,
            reference_id,
            generation,
            handle,
            data,
            stop: Cancel::new(),
            feeders: Mutex::default(),
        }
    }
    pub fn remove_chunks(&self, count: usize) {
        let root = runtime::lock_engine(&self.shared).store.root.clone();
        for bytes in self.data.chunks(CHUNK_SIZE as usize).take(count) {
            std::fs::remove_file(root.join("objects").join(Digest::bytes(bytes).to_string()))
                .unwrap();
        }
    }
    pub fn pending(&self) -> Vec<tkfs::content::scheduler::DemandSnapshot> {
        let state = runtime::content_rpc::state(&self.ctx).unwrap();
        state
            .scheduler
            .snapshots(state.provider.identity(), &self.ctx.repo)
    }
    pub fn wait_pending(&self) -> tkfs::content::scheduler::DemandSnapshot {
        let until = Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(demand) = self.pending().into_iter().next() {
                return demand;
            }
            assert!(Instant::now() < until, "no current range demand appeared");
            thread::sleep(Duration::from_millis(1));
        }
    }
    pub fn feed(&self, chunks: usize, per_step: usize, every: Duration) {
        assert!(per_step > 0);
        let (ctx, native, data, reference, generation, stop) = (
            self.ctx.clone(),
            self.native.clone(),
            self.data.clone(),
            self.reference_id.clone(),
            self.generation,
            self.stop.clone(),
        );
        self.feeders.lock().unwrap().push(thread::spawn(move || {
            let pieces: Vec<_> = data.chunks(CHUNK_SIZE as usize).take(chunks).collect();
            for batch in pieces.chunks(per_step) {
                stop.sleep(every);
                if stop.is_cancelled() {
                    return;
                }
                for bytes in batch {
                    let context = ReadContext {
                        deadline: Instant::now() + Duration::from_secs(30),
                        cancel: stop.clone(),
                    };
                    let mut store = ctx.sync_stores.checkout().unwrap();
                    let result = receipt::receive(
                        &mut store,
                        &native,
                        ReceivedFrame {
                            reference: &reference,
                            generation,
                            key: ObjectKey {
                                kind: ObjectKind::Bytes,
                                digest: Digest::bytes(bytes),
                            },
                            bytes,
                            source: &ctx.device,
                        },
                        &context,
                    );
                    if stop.is_cancelled() {
                        return;
                    }
                    result.unwrap();
                }
            }
        }));
    }
    pub fn finish(&self) {
        for thread in self.feeders.lock().unwrap().drain(..) {
            thread.join().unwrap();
        }
    }
    pub fn read(&self, offset: u64, len: usize, wait: ContentWait) -> anyhow::Result<Vec<u8>> {
        runtime::content_reads::read_mount_vec(&self.shared, self.handle, offset, len, wait)?
            .ok_or_else(|| anyhow::anyhow!("current descriptor was not selected"))
    }
}
impl Drop for CurrentRead {
    fn drop(&mut self) {
        self.stop.cancel();
        for thread in self.feeders.lock().unwrap().drain(..) {
            let result = thread.join();
            if !thread::panicking() {
                result.unwrap();
            }
        }
        let _ = runtime::lock_engine(&self.shared).release_handle(self.handle);
    }
}
