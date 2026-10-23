//! Events first; paired peers fetch immutable objects by digest. No reference grants.
use crate::{
    content::{Kind, ObjectFetcher, PackStore, working::lock},
    core::Event,
    runtime::Engine,
};
use crate::{
    model::{content::Digest, errors::ErrorWire},
    session::{Conn, Handler, Incoming, Priority},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

pub const BATCH: usize = 64;
pub const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;
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
        self.packs.flush()?;
        Ok(())
    }
    pub fn prefetch(&self, wanted: &[(Digest, Kind)]) -> Result<()> {
        self.run(wanted, true)
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
