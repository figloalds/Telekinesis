//! Events first; paired peers fetch immutable objects by digest. No reference grants.
use crate::{
    content::{Kind, ObjectFetcher, PackStore, working::lock},
    core::Event,
    runtime::Engine,
};
use crate::{
    model::{
        content::{ContentKind, Digest, FileContentRef},
        errors::ErrorWire,
    },
    session::{Conn, Handler, Incoming, Priority},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

pub const BATCH: usize = 64;
pub const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;
// At most 2 MiB of direct payload, or about 8 MiB as JSON byte arrays plus
// metadata, below the default connection's 16 MiB reassembled-body bound.
pub(crate) const DIRECT_PREFETCH_BATCH: usize = BATCH / 2;
pub const FETCH_OP: &str = "objects.fetch";
#[derive(Clone, Serialize, Deserialize)]
pub struct Object {
    pub digest: Digest,
    pub kind: Kind,
    pub bytes: Vec<u8>,
}
pub trait ObjectSource: Send + Sync {
    fn fetch(&self, wanted: &[(Digest, Kind)]) -> Result<Vec<Object>>;
}
struct Source {
    source: Arc<dyn ObjectSource>,
    failures: u32,
    retry_at: Instant,
}
pub struct DigestFetch {
    packs: PackStore,
    peers: Mutex<BTreeMap<String, Arc<Mutex<Source>>>>,
    foreground: AtomicUsize,
}
impl DigestFetch {
    pub fn new(packs: PackStore) -> Arc<Self> {
        Arc::new(Self {
            packs,
            peers: Mutex::new(BTreeMap::new()),
            foreground: AtomicUsize::new(0),
        })
    }
    pub fn add(&self, peer: String, source: Arc<dyn ObjectSource>) {
        lock(&self.peers).insert(
            peer,
            Arc::new(Mutex::new(Source {
                source,
                failures: 0,
                retry_at: Instant::now(),
            })),
        );
    }
    pub fn remove(&self, peer: &str) {
        lock(&self.peers).remove(peer);
    }
    fn run(&self, wanted: &[(Digest, Kind)], background: bool) -> Result<()> {
        for batch in wanted.chunks(BATCH) {
            if background && self.foreground.load(Ordering::Acquire) > 0 {
                return Ok(());
            }
            let mut missing = batch
                .iter()
                .copied()
                .filter(|(d, _)| !self.packs.contains(*d).unwrap_or(false))
                .collect::<Vec<_>>();
            let sources: Vec<_> = lock(&self.peers).values().cloned().collect();
            let mut last = None;
            for source in sources.into_iter().take(3) {
                if missing.is_empty() {
                    break;
                }
                let mut source = lock(&source);
                if source.retry_at > Instant::now() {
                    continue;
                }
                let result = source.source.fetch(&missing).and_then(|objects| {
                    ensure!(
                        objects.len() <= missing.len()
                            && objects.iter().map(|o| o.bytes.len()).sum::<usize>()
                                <= MAX_BATCH_BYTES,
                        "OBJECT_BATCH_TOO_LARGE"
                    );
                    for object in objects {
                        ensure!(
                            missing.contains(&(object.digest, object.kind))
                                && Digest::bytes(&object.bytes) == object.digest,
                            "INVALID_PEER_OBJECT"
                        );
                        self.packs.put(object.kind, &object.bytes)?;
                    }
                    Ok(())
                });
                match result {
                    Ok(()) => {
                        source.failures = 0;
                        source.retry_at = Instant::now();
                    }
                    Err(error) => {
                        source.failures = source.failures.saturating_add(1);
                        source.retry_at = Instant::now()
                            + Duration::from_secs((1u64 << source.failures.min(6)).min(60));
                        last = Some(error);
                    }
                }
                missing.retain(|(d, _)| !self.packs.contains(*d).unwrap_or(false));
            }
            if !missing.is_empty() {
                return Err(last
                    .unwrap_or_else(|| anyhow::anyhow!("OBJECT_MISSING: no ready paired source")));
            }
        }
        if background {
            self.packs
                .flush_if_pending(wanted.iter().map(|(digest, _)| *digest))?;
        } else {
            self.packs.flush()?;
        }
        Ok(())
    }
    pub fn prefetch(&self, wanted: &[(Digest, Kind)]) -> Result<()> {
        self.run(wanted, true)
    }
    pub(crate) fn prefetch_direct_roots(
        &self,
        references: &[FileContentRef],
        stopping: &AtomicBool,
    ) {
        for batch in references.chunks(DIRECT_PREFETCH_BATCH) {
            if stopping.load(Ordering::Acquire) || self.foreground.load(Ordering::Acquire) > 0 {
                return;
            }
            let mut roots = Vec::with_capacity(DIRECT_PREFETCH_BATCH);
            for reference in batch {
                if reference.kind == ContentKind::Direct && reference.validate().is_ok() {
                    let root = (reference.digest, Kind::Chunk);
                    if !roots.contains(&root) {
                        roots.push(root);
                    }
                }
            }
            if roots.is_empty() {
                continue;
            }
            if self.prefetch(&roots).is_err() {
                // A missing neighbor must not leave valid received roots
                // pending indefinitely. Persist that requested subset without
                // retrying every missing root or fetching unrelated objects.
                let _ = self
                    .packs
                    .flush_if_pending(roots.iter().map(|(digest, _)| *digest));
            }
        }
    }
}
impl ObjectFetcher for DigestFetch {
    fn fetch(&self, wanted: &[(Digest, Kind)]) -> Result<()> {
        struct Foreground<'a>(&'a AtomicUsize);
        impl Drop for Foreground<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::AcqRel);
            }
        }
        self.foreground.fetch_add(1, Ordering::AcqRel);
        let _guard = Foreground(&self.foreground);
        self.run(wanted, false)
    }
}
pub struct SessionSource {
    pub conn: Conn,
    pub repo: String,
}
impl ObjectSource for SessionSource {
    fn fetch(&self, wanted: &[(Digest, Kind)]) -> Result<Vec<Object>> {
        let reply = self
            .conn
            .call(
                FETCH_OP,
                json!({"repo":self.repo}),
                serde_json::to_vec(wanted)?,
                Priority::Data,
                Duration::from_secs(10),
            )
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(serde_json::from_slice(&reply.body)?)
    }
}
#[derive(Serialize, Deserialize)]
pub struct FeedRequest {
    pub after: u64,
    pub events: Vec<Event>,
}
#[derive(Serialize, Deserialize)]
pub struct FeedReply {
    pub cursor: u64,
    pub events: Vec<Event>,
    pub more: bool,
}
pub struct Service {
    pub engine: Arc<Engine>,
}
impl Service {
    fn handle_request(&self, req: &Incoming) -> Result<Vec<u8>> {
        let repo = lock(&self.engine.store).repo.clone();
        ensure!(
            req.header.get("repo").and_then(|v| v.as_str()) == Some(repo.as_str()),
            "REPOSITORY_MISMATCH"
        );
        match req.op.as_str() {
            "meta.exchange" => {
                let request: FeedRequest = serde_json::from_slice(&req.body)?;
                ensure!(request.events.len() <= 256, "FEED_BATCH_TOO_LARGE");
                self.engine.apply_remote(&request.events)?;
                let rows = lock(&self.engine.store).events_after(request.after, 256)?;
                let cursor = rows.last().map(|(n, _)| *n).unwrap_or(request.after);
                let more = rows.len() == 256;
                Ok(serde_json::to_vec(&FeedReply {
                    cursor,
                    events: rows.into_iter().map(|(_, e)| e).collect(),
                    more,
                })?)
            }
            FETCH_OP => {
                let wanted: Vec<(Digest, Kind)> = serde_json::from_slice(&req.body)?;
                ensure!(wanted.len() <= BATCH, "OBJECT_BATCH_TOO_LARGE");
                let content = self.engine.working.content();
                let _pins = content.packs.pin(wanted.iter().map(|(d, _)| *d));
                let mut objects = Vec::new();
                let mut size = 0;
                for (digest, kind) in wanted {
                    if let Ok(bytes) = content.packs.get(digest) {
                        size += bytes.len();
                        ensure!(size <= MAX_BATCH_BYTES, "OBJECT_BATCH_TOO_LARGE");
                        objects.push(Object {
                            digest,
                            kind,
                            bytes: (*bytes).clone(),
                        });
                    }
                }
                Ok(serde_json::to_vec(&objects)?)
            }
            _ => anyhow::bail!("UNSUPPORTED_OPERATION"),
        }
    }
}
impl Handler for Service {
    fn handle(&self, req: Incoming) {
        match self.handle_request(&req) {
            Ok(body) => req.responder.reply(json!({}), body),
            Err(error) => req
                .responder
                .error(ErrorWire::coded("SYNC_FAILED", format!("{error:#}"))),
        }
    }
}
/// Cursors advance only after successful metadata apply. Retrying a feed page
/// is safe because event identity and explicit parent versions are immutable.
#[derive(Default)]
pub struct FeedCursor {
    pub sent: u64,
    pub received: u64,
}
pub fn exchange(engine: &Engine, conn: &Conn, cursor: &mut FeedCursor) -> Result<bool> {
    let (repo, rows) = {
        let s = lock(&engine.store);
        (s.repo.clone(), s.events_after(cursor.sent, 256)?)
    };
    let sent = rows.last().map(|(n, _)| *n).unwrap_or(cursor.sent);
    let request = FeedRequest {
        after: cursor.received,
        events: rows.into_iter().map(|(_, e)| e).collect(),
    };
    let reply = conn
        .call(
            "meta.exchange",
            json!({"repo":repo}),
            serde_json::to_vec(&request)?,
            Priority::Control,
            Duration::from_secs(10),
        )
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let reply: FeedReply = serde_json::from_slice(&reply.body).context("INVALID_FEED_REPLY")?;
    ensure!(
        reply.events.len() <= 256 && reply.cursor >= cursor.received,
        "INVALID_FEED_REPLY"
    );
    engine.apply_remote(&reply.events)?;
    cursor.sent = sent;
    cursor.received = reply.cursor;
    Ok(reply.more || request.events.len() == 256)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn indexed(home: &Path, digest: Digest) -> Result<bool> {
        let db = rusqlite::Connection::open_with_flags(
            home.join("objects/index.sqlite"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        Ok(db.query_row(
            "SELECT EXISTS(SELECT 1 FROM objects WHERE digest=?1)",
            [&digest.0[..]],
            |row| row.get(0),
        )?)
    }

    struct RootSource {
        packs: PackStore,
        requests: Mutex<Vec<Vec<(Digest, Kind)>>>,
        stop: Option<Arc<AtomicBool>>,
    }
    impl ObjectSource for RootSource {
        fn fetch(&self, wanted: &[(Digest, Kind)]) -> Result<Vec<Object>> {
            lock(&self.requests).push(wanted.to_vec());
            let mut objects = Vec::new();
            for (digest, kind) in wanted {
                if let Ok(bytes) = self.packs.get(*digest) {
                    objects.push(Object {
                        digest: *digest,
                        kind: *kind,
                        bytes: bytes.as_slice().to_vec(),
                    });
                }
            }
            if let Some(stop) = &self.stop {
                stop.store(true, Ordering::Release);
            }
            Ok(objects)
        }
    }
    fn direct_root(packs: &PackStore, n: u64) -> Result<FileContentRef> {
        let mut bytes = vec![255; 2048];
        bytes[..8].copy_from_slice(&n.to_le_bytes());
        FileContentRef::new(
            ContentKind::Direct,
            packs.put(Kind::Chunk, &bytes)?,
            bytes.len() as u64,
        )
    }

    #[test]
    fn direct_prefetch_bounds_deduplicates_and_skips_other_reference_types() -> Result<()> {
        let source_home = tempfile::tempdir()?;
        let target_home = tempfile::tempdir()?;
        let packs = PackStore::open(source_home.path())?;
        let mut references = (0..70)
            .map(|n| direct_root(&packs, n))
            .collect::<Result<Vec<_>>>()?;
        packs.flush()?;
        let manifest = FileContentRef::new(ContentKind::Manifest, Digest([3; 32]), 65_537)?;
        let mut invalid = FileContentRef::new(ContentKind::Direct, Digest([4; 32]), 1)?;
        invalid.format = 0;
        references.insert(0, manifest.clone());
        references.insert(1, invalid.clone());
        references.insert(3, references[2].clone());
        let source = Arc::new(RootSource {
            packs,
            requests: Mutex::new(Vec::new()),
            stop: None,
        });
        let target = PackStore::open(target_home.path())?;
        let fetch = DigestFetch::new(target.clone());
        fetch.add("source".into(), source.clone());
        let stopping = AtomicBool::new(false);
        fetch.prefetch_direct_roots(&references, &stopping);
        let requests = lock(&source.requests);
        assert_eq!(requests.len(), 3);
        assert!(requests.iter().all(|r| r.len() <= DIRECT_PREFETCH_BATCH));
        assert_eq!(requests.iter().map(Vec::len).sum::<usize>(), 70);
        assert!(
            requests
                .iter()
                .flatten()
                .all(|(d, k)| *d != manifest.digest && *d != invalid.digest && *k == Kind::Chunk)
        );
        drop(requests);
        for reference in references
            .iter()
            .filter(|r| r.kind == ContentKind::Direct && r.validate().is_ok())
        {
            assert!(indexed(target_home.path(), reference.digest)?);
            assert_eq!(
                target.get(reference.digest)?,
                source.packs.get(reference.digest)?
            );
        }
        let unrelated = target.put(Kind::Chunk, b"unrelated pending write")?;
        fetch.prefetch_direct_roots(&references, &stopping);
        assert_eq!(lock(&source.requests).len(), 3);
        assert!(!indexed(target_home.path(), unrelated)?);
        Ok(())
    }

    #[test]
    fn partial_direct_batch_preserves_independent_progress_and_retry_barriers() -> Result<()> {
        let source_home = tempfile::tempdir()?;
        let target_home = tempfile::tempdir()?;
        let packs = PackStore::open(source_home.path())?;
        let mut references = (0..63)
            .map(|n| direct_root(&packs, n))
            .collect::<Result<Vec<_>>>()?;
        let missing = FileContentRef::new(ContentKind::Direct, Digest::bytes(b"missing root"), 12)?;
        references.insert(0, missing.clone());
        packs.flush()?;
        let source = Arc::new(RootSource {
            packs,
            requests: Mutex::new(Vec::new()),
            stop: None,
        });
        let target = PackStore::open(target_home.path())?;
        let fetch = DigestFetch::new(target.clone());
        fetch.add("source".into(), source.clone());
        let stopping = AtomicBool::new(false);
        fetch.prefetch_direct_roots(&references, &stopping);
        assert_eq!(lock(&source.requests).len(), 2);
        assert!(!target.contains(missing.digest)?);
        for reference in &references[1..] {
            assert!(indexed(target_home.path(), reference.digest)?);
        }
        assert_eq!(
            source.packs.put(Kind::Chunk, b"missing root")?,
            missing.digest
        );
        source.packs.flush()?;
        fetch.prefetch_direct_roots(std::slice::from_ref(&missing), &stopping);
        assert!(indexed(target_home.path(), missing.digest)?);
        drop(fetch);
        drop(target);
        let reopened = PackStore::open(target_home.path())?;
        for reference in &references {
            assert_eq!(
                reopened.get(reference.digest)?,
                source.packs.get(reference.digest)?
            );
        }
        Ok(())
    }

    #[test]
    fn direct_prefetch_yields_to_foreground_and_stops_between_bounded_batches() -> Result<()> {
        let source_home = tempfile::tempdir()?;
        let target_home = tempfile::tempdir()?;
        let packs = PackStore::open(source_home.path())?;
        let references = (0..96)
            .map(|n| direct_root(&packs, n))
            .collect::<Result<Vec<_>>>()?;
        packs.flush()?;
        let stopping = Arc::new(AtomicBool::new(false));
        let source = Arc::new(RootSource {
            packs,
            requests: Mutex::new(Vec::new()),
            stop: Some(stopping.clone()),
        });
        let target = PackStore::open(target_home.path())?;
        let fetch = DigestFetch::new(target.clone());
        fetch.add("source".into(), source.clone());
        fetch.foreground.store(1, Ordering::Release);
        fetch.prefetch_direct_roots(&references, &stopping);
        assert!(lock(&source.requests).is_empty());
        fetch.foreground.store(0, Ordering::Release);
        stopping.store(true, Ordering::Release);
        fetch.prefetch_direct_roots(&references, &stopping);
        assert!(lock(&source.requests).is_empty());
        stopping.store(false, Ordering::Release);
        fetch.prefetch_direct_roots(&references, &stopping);
        assert_eq!(lock(&source.requests).len(), 1);
        for (index, reference) in references.iter().enumerate() {
            assert_eq!(
                indexed(target_home.path(), reference.digest)?,
                index < DIRECT_PREFETCH_BATCH
            );
        }
        Ok(())
    }

    #[test]
    fn batched_pass_preserves_manifest_chunk_traversal_and_empty_content() -> Result<()> {
        let source_home = tempfile::tempdir()?;
        let target_home = tempfile::tempdir()?;
        let content = crate::content::LocalContent::open(source_home.path())?;
        let direct = content.import(b"direct root")?;
        let empty = content.import(b"")?;
        let bytes = vec![73; 65_537];
        let manifest = content.import(&bytes)?;
        content.packs.flush()?;
        let source = Arc::new(RootSource {
            packs: content.packs.clone(),
            requests: Mutex::new(Vec::new()),
            stop: None,
        });
        let engine = Engine::open(target_home.path(), None)?;
        let fetch = DigestFetch::new(engine.working.content().packs.clone());
        fetch.add("source".into(), source);
        super::super::prefetch_pass(
            &engine,
            &fetch,
            &[direct.clone(), manifest.clone(), empty.clone()],
        );
        for (reference, expected) in [
            (&direct, b"direct root".as_slice()),
            (&manifest, bytes.as_slice()),
            (&empty, b"".as_slice()),
        ] {
            let mut got = vec![0; expected.len()];
            engine
                .working
                .content()
                .read_range(reference, 0, &mut got)?;
            assert_eq!(got, expected);
        }
        engine.shutdown()?;
        Ok(())
    }

    #[test]
    fn maximum_direct_batches_fit_authenticated_session_defaults() -> Result<()> {
        use crate::{
            model::protocol::{LaneRole, frame::ClientHello},
            pairing_code::Verifier,
            session,
        };
        struct Policy(Verifier);
        impl session::AcceptPolicy for Policy {
            fn installation(&self) -> String {
                "batch-source".into()
            }
            fn verifier(&self) -> Option<(Verifier, u64)> {
                Some((self.0.clone(), 1))
            }
            fn admit(&self, _: &ClientHello) -> std::result::Result<(), ErrorWire> {
                Ok(())
            }
            fn legacy_attempt(&self, _: std::net::SocketAddr, _: &serde_json::Value) {}
        }
        struct CountedService {
            inner: Service,
            requests: Arc<Mutex<Vec<usize>>>,
        }
        impl Handler for CountedService {
            fn handle(&self, req: Incoming) {
                if req.op == FETCH_OP {
                    let wanted: Vec<(Digest, Kind)> = serde_json::from_slice(&req.body).unwrap();
                    lock(&self.requests).push(wanted.len());
                }
                self.inner.handle(req);
            }
        }
        let source_home = tempfile::tempdir()?;
        let target_home = tempfile::tempdir()?;
        let source = Engine::open(source_home.path(), Some("repo"))?;
        let target = Engine::open(target_home.path(), Some("repo"))?;
        let mut references = Vec::new();
        let mut objects = Vec::new();
        for n in 0..33u64 {
            let mut bytes = vec![255; crate::model::content::CHUNK_SIZE as usize];
            bytes[..8].copy_from_slice(&n.to_le_bytes());
            let digest = source.working.content().packs.put(Kind::Chunk, &bytes)?;
            references.push(FileContentRef::new(
                ContentKind::Direct,
                digest,
                bytes.len() as u64,
            )?);
            objects.push(Object {
                digest,
                kind: Kind::Chunk,
                bytes,
            });
        }
        source.working.content().packs.flush()?;
        let wire = serde_json::to_vec(&objects[..DIRECT_PREFETCH_BATCH])?;
        let options = session::ConnOptions::default();
        assert_eq!(options.max_body, 16 * 1024 * 1024);
        assert!(wire.len() > 8 * 1024 * 1024 && wire.len() < options.max_body);
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        let (code, verifier) = Verifier::generate();
        let serving = source.clone();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let counted = requests.clone();
        let (ready, received) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            let result = (|| -> Result<session::Conn> {
                let deadline = Instant::now() + Duration::from_secs(5);
                let stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(e)
                            if e.kind() == std::io::ErrorKind::WouldBlock
                                && Instant::now() < deadline =>
                        {
                            std::thread::sleep(Duration::from_millis(1))
                        }
                        Err(e) => return Err(e.into()),
                    }
                };
                let (conn, _) = session::accept(
                    stream,
                    Arc::new(Policy(verifier)),
                    session::ConnOptions::default(),
                )
                .map_err(|e| anyhow::anyhow!("{e}"))?;
                conn.serve(Arc::new(CountedService {
                    inner: Service { engine: serving },
                    requests: counted,
                }));
                Ok(conn)
            })();
            let _ = ready.send(result);
        });
        let (client, _) = session::dial(
            &endpoint,
            session::client_hello("batch-target", LaneRole::Control, None, 1),
            &session::CodeAuth { code },
            None,
            options,
        )
        .map_err(|e| anyhow::anyhow!("{e}"))?;
        let server = received.recv_timeout(Duration::from_secs(10))??;
        worker.join().unwrap();
        let fetch = DigestFetch::new(target.working.content().packs.clone());
        fetch.add(
            "source".into(),
            Arc::new(SessionSource {
                conn: client.clone(),
                repo: "repo".into(),
            }),
        );
        fetch.prefetch_direct_roots(&references, &target.stopping);
        assert_eq!(*lock(&requests), vec![32, 1]);
        for object in objects {
            assert_eq!(
                target
                    .working
                    .content()
                    .packs
                    .get(object.digest)?
                    .as_slice(),
                object.bytes.as_slice()
            );
            assert!(indexed(target_home.path(), object.digest)?);
        }
        assert!(client.closed().is_none() && server.closed().is_none());
        client.close();
        server.close();
        source.shutdown()?;
        target.shutdown()?;
        Ok(())
    }

    #[test]
    fn prefetch_existing_objects_does_not_commit_unrelated_pending_writes() -> Result<()> {
        let home = tempfile::tempdir()?;
        let packs = PackStore::open(home.path())?;
        let wanted = packs.put(Kind::Chunk, b"already committed")?;
        packs.flush()?;
        let unrelated = packs.put(Kind::Chunk, b"foreground batch in progress")?;
        assert!(!indexed(home.path(), unrelated)?);
        let fetch = DigestFetch::new(packs.clone());
        fetch.prefetch(&[(wanted, Kind::Chunk)])?;
        assert!(indexed(home.path(), wanted)?);
        assert!(
            !indexed(home.path(), unrelated)?,
            "idle prefetch committed an unrelated foreground batch"
        );
        packs.flush()?;
        assert!(indexed(home.path(), unrelated)?);
        Ok(())
    }

    #[test]
    fn foreground_fetch_retains_its_unconditional_pack_barrier() -> Result<()> {
        let home = tempfile::tempdir()?;
        let packs = PackStore::open(home.path())?;
        let wanted = packs.put(Kind::Chunk, b"already committed")?;
        packs.flush()?;
        let fetch = DigestFetch::new(packs.clone());
        for request in [vec![(wanted, Kind::Chunk)], vec![]] {
            let bytes = format!("pending for {} requested objects", request.len());
            let pending = packs.put(Kind::Chunk, bytes.as_bytes())?;
            assert!(!indexed(home.path(), pending)?);
            fetch.fetch(&request)?;
            assert!(indexed(home.path(), pending)?);
        }
        Ok(())
    }

    #[test]
    fn background_prefetch_commits_requested_pending_objects() -> Result<()> {
        let home = tempfile::tempdir()?;
        let packs = PackStore::open(home.path())?;
        let wanted = packs.put(Kind::Chunk, b"requested pending object")?;
        assert!(!indexed(home.path(), wanted)?);
        let fetch = DigestFetch::new(packs.clone());
        fetch.prefetch(&[(wanted, Kind::Chunk)])?;
        assert!(indexed(home.path(), wanted)?);
        drop(fetch);
        drop(packs);
        let reopened = PackStore::open(home.path())?;
        assert_eq!(
            reopened.get(wanted)?.as_slice(),
            b"requested pending object"
        );
        Ok(())
    }

    struct PartialSource;
    impl ObjectSource for PartialSource {
        fn fetch(&self, wanted: &[(Digest, Kind)]) -> Result<Vec<Object>> {
            let bytes = b"downloaded first object".to_vec();
            let digest = Digest::bytes(&bytes);
            assert!(wanted.contains(&(digest, Kind::Chunk)));
            Ok(vec![Object {
                digest,
                kind: Kind::Chunk,
                bytes,
            }])
        }
    }

    #[test]
    fn downloaded_background_objects_are_committed_and_verified_after_reopen() -> Result<()> {
        let home = tempfile::tempdir()?;
        let packs = PackStore::open(home.path())?;
        let wanted = Digest::bytes(b"downloaded first object");
        let fetch = DigestFetch::new(packs.clone());
        fetch.add("source".into(), Arc::new(PartialSource));
        fetch.prefetch(&[(wanted, Kind::Chunk)])?;
        assert!(indexed(home.path(), wanted)?);
        drop(fetch);
        drop(packs);
        assert_eq!(
            PackStore::open(home.path())?.get(wanted)?.as_slice(),
            b"downloaded first object"
        );
        Ok(())
    }

    #[test]
    fn requested_background_commit_errors_remain_errors_and_can_be_retried() -> Result<()> {
        let home = tempfile::tempdir()?;
        let packs = PackStore::open(home.path())?;
        let wanted = packs.put(Kind::Chunk, b"must report failed barrier")?;
        let db = rusqlite::Connection::open(home.path().join("objects/index.sqlite"))?;
        db.execute_batch("CREATE TRIGGER fail_object_commit BEFORE INSERT ON objects BEGIN SELECT RAISE(ABORT,'barrier failure'); END;")?;
        let fetch = DigestFetch::new(packs.clone());
        let error = fetch.prefetch(&[(wanted, Kind::Chunk)]).unwrap_err();
        assert!(format!("{error:#}").contains("barrier failure"));
        assert!(packs.contains(wanted)? && !indexed(home.path(), wanted)?);
        db.execute_batch("DROP TRIGGER fail_object_commit")?;
        fetch.prefetch(&[(wanted, Kind::Chunk)])?;
        assert!(indexed(home.path(), wanted)?);
        assert_eq!(packs.get(wanted)?.as_slice(), b"must report failed barrier");
        Ok(())
    }

    #[test]
    fn retry_prefetch_commits_objects_left_pending_by_a_partial_download() -> Result<()> {
        let home = tempfile::tempdir()?;
        let packs = PackStore::open(home.path())?;
        let first = Digest::bytes(b"downloaded first object");
        let second = Digest::bytes(b"missing second object");
        let fetch = DigestFetch::new(packs.clone());
        fetch.add("partial".into(), Arc::new(PartialSource));
        assert!(
            fetch
                .prefetch(&[(first, Kind::Chunk), (second, Kind::Chunk)])
                .is_err()
        );
        assert!(packs.contains(first)? && !packs.contains(second)?);
        assert!(!indexed(home.path(), first)?);
        fetch.remove("partial");
        // No bytes are fetched by this successful retry; the earlier install
        // is nevertheless pending and still requires the pack barrier.
        fetch.prefetch(&[(first, Kind::Chunk)])?;
        assert!(indexed(home.path(), first)?);
        assert!(!indexed(home.path(), second)?);
        drop(fetch);
        drop(packs);
        assert_eq!(
            PackStore::open(home.path())?.get(first)?.as_slice(),
            b"downloaded first object"
        );
        Ok(())
    }
}
