//! A worker's sync ops. Events first; objects move by digest. The worker only
//! serves: the orchestrator reads its outgoing feed, applies remote events,
//! answers `meta.exchange` for a peer, and moves objects with
//! `objects.missing` / `objects.fetch` / `objects.write`. No reference grants.
//!
//! Ops (header = JSON payload; body = JSON):
//! - `feed.outgoing {after}` → [`FeedPage`] of at most [`FEED_PAGE`] events.
//! - `feed.apply {}`, body `Vec<Event>` → `{"applied": usize}`.
//! - `meta.exchange {repo}`, body [`FeedRequest`] → [`FeedReply`].
//! - `objects.missing {limit}` → `Vec<(Digest, Kind)>`, `limit` ≤ [`BATCH`].
//! - `objects.fetch {repo}`, body `Vec<(Digest, Kind)>` → `Vec<Object>`.
//! - `objects.write {repo}`, body `Vec<Object>` → `{"stored": usize}`.
use super::demand::Demand;
use crate::{
    content::{Kind, working::lock},
    core::Event,
    model::content::{ContentKind, Digest},
    runtime::Engine,
    session::{Handler, Incoming},
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

pub const BATCH: usize = 64;
pub const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;
pub const FETCH_OP: &str = "objects.fetch";
/// Events per `feed.outgoing` page and per `meta.exchange` request or reply.
pub const FEED_PAGE: usize = 256;
/// Most events one `feed.apply` takes (the store's own read bound).
pub const MAX_APPLY: usize = 4096;
#[derive(Clone, Serialize, Deserialize)]
pub struct Object {
    pub digest: Digest,
    pub kind: Kind,
    pub bytes: Vec<u8>,
}
#[derive(Serialize, Deserialize)]
pub struct FeedRequest {
    pub after: u64,
    pub events: Vec<Event>,
}
#[derive(Serialize, Deserialize)]
pub struct FeedReply {
    pub cursor: u64,
    // An identical event carried in this request need not be echoed. Cursor
    // and more still describe the complete scanned page, including omissions.
    pub events: Vec<Event>,
    pub more: bool,
}
/// One page of a worker's journal: the events after the requested sequence,
/// `cursor` = the last one's sequence (or the request's when empty).
#[derive(Serialize, Deserialize)]
pub struct FeedPage {
    pub cursor: u64,
    pub events: Vec<Event>,
    pub more: bool,
}
pub struct Service {
    pub engine: Arc<Engine>,
    /// Readers waiting for objects; `objects.write` wakes them.
    pub demand: Arc<Demand>,
}
impl Service {
    /// Whether `op` is one of this service's ops (`feed.*`, `meta.exchange`,
    /// `objects.*`).
    pub fn routes(op: &str) -> bool {
        op.starts_with("feed.") || op == "meta.exchange" || op.starts_with("objects.")
    }
    fn page(&self, after: u64) -> Result<FeedPage> {
        let rows = lock(&self.engine.store).events_after(after, FEED_PAGE)?;
        Ok(FeedPage {
            cursor: rows.last().map(|(n, _)| *n).unwrap_or(after),
            more: rows.len() == FEED_PAGE,
            events: rows.into_iter().map(|(_, event)| event).collect(),
        })
    }
    fn feed_reply(&self, request: FeedRequest) -> Result<FeedReply> {
        ensure!(request.events.len() <= FEED_PAGE, "FEED_BATCH_TOO_LARGE");
        self.engine.apply_remote(&request.events)?;
        let page = self.page(request.after)?;
        // Validate/apply first, including collisions and unresolved parents.
        // Knowledge is limited to this request's exact bodies: author-based
        // filtering would lose unknown self-authored history after recovery.
        let received: BTreeMap<_, _> = request
            .events
            .iter()
            .map(|event| (event.id.as_str(), event))
            .collect();
        Ok(FeedReply {
            cursor: page.cursor,
            events: page
                .events
                .into_iter()
                .filter(|event| {
                    received
                        .get(event.id.as_str())
                        .is_none_or(|known| *known != event)
                })
                .collect(),
            more: page.more,
        })
    }
    /// Content objects of live entries in the current projection that the
    /// packs lack: every file's root first, then the chunks of manifests that
    /// are present. At most `limit` (clamped to [`BATCH`]), without duplicates.
    fn missing(&self, limit: usize) -> Result<Vec<(Digest, Kind)>> {
        let limit = limit.min(BATCH);
        let mut wanted = Vec::new();
        if limit == 0 {
            return Ok(wanted);
        }
        let entries = lock(&self.engine.store).projection().entries.clone();
        let content = self.engine.working.content();
        let references: Vec<_> = entries
            .values()
            .filter(|entry| !entry.deleted)
            .filter_map(|entry| entry.content.as_ref())
            .filter(|reference| reference.validate().is_ok())
            .collect();
        let mut seen = BTreeSet::new();
        // Adds an absent object; true once `limit` objects are listed.
        let mut full = |wanted: &mut Vec<_>, digest: Digest, kind: Kind| -> Result<bool> {
            if seen.insert(digest) && !content.packs.contains(digest)? {
                wanted.push((digest, kind));
            }
            Ok(wanted.len() >= limit)
        };
        for reference in &references {
            let kind = match reference.kind {
                ContentKind::Direct => Kind::Chunk,
                ContentKind::Manifest => Kind::Manifest,
            };
            if full(&mut wanted, reference.digest, kind)? {
                return Ok(wanted);
            }
        }
        for reference in references
            .iter()
            .filter(|reference| reference.kind == ContentKind::Manifest)
        {
            // Only a present manifest: `chunks` would otherwise block on the
            // fetcher, which is this worker's own demand.
            if !content.packs.contains(reference.digest)? {
                continue;
            }
            let Ok(chunks) = content.chunks(reference) else {
                continue;
            };
            for digest in chunks {
                if full(&mut wanted, digest, Kind::Chunk)? {
                    return Ok(wanted);
                }
            }
        }
        Ok(wanted)
    }
    fn fetch(&self, wanted: Vec<(Digest, Kind)>) -> Result<Vec<Object>> {
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
        Ok(objects)
    }
    /// Verifies every object before storing any, makes them durable, then
    /// wakes the readers waiting for them.
    fn write(&self, objects: Vec<Object>) -> Result<usize> {
        ensure!(
            objects.len() <= BATCH
                && objects.iter().map(|o| o.bytes.len()).sum::<usize>() <= MAX_BATCH_BYTES,
            "OBJECT_BATCH_TOO_LARGE"
        );
        for object in &objects {
            ensure!(
                Digest::bytes(&object.bytes) == object.digest,
                "INVALID_PEER_OBJECT: {} does not match its bytes",
                object.digest
            );
        }
        let packs = &self.engine.working.content().packs;
        for object in &objects {
            packs.put(object.kind, &object.bytes)?;
        }
        packs.flush()?;
        let digests: Vec<Digest> = objects.iter().map(|o| o.digest).collect();
        self.demand.arrived(&digests);
        Ok(objects.len())
    }
    /// Runs one op; the reply body on success.
    pub fn handle_request(&self, op: &str, header: &Value, body: &[u8]) -> Result<Vec<u8>> {
        if matches!(op, "meta.exchange" | FETCH_OP | "objects.write") {
            let repo = lock(&self.engine.store).repo.clone();
            ensure!(
                header.get("repo").and_then(Value::as_str) == Some(repo.as_str()),
                "REPOSITORY_MISMATCH"
            );
        }
        match op {
            "feed.outgoing" => Ok(serde_json::to_vec(
                &self.page(header["after"].as_u64().unwrap_or(0))?,
            )?),
            "feed.apply" => {
                let events: Vec<Event> = serde_json::from_slice(body)?;
                ensure!(events.len() <= MAX_APPLY, "FEED_BATCH_TOO_LARGE");
                let applied = self.engine.apply_remote(&events)?.len();
                Ok(serde_json::to_vec(&json!({"applied":applied}))?)
            }
            "meta.exchange" => {
                let request: FeedRequest = serde_json::from_slice(body)?;
                Ok(serde_json::to_vec(&self.feed_reply(request)?)?)
            }
            "objects.missing" => {
                let limit = header["limit"].as_u64().unwrap_or(BATCH as u64);
                let limit = usize::try_from(limit).unwrap_or(BATCH);
                Ok(serde_json::to_vec(&self.missing(limit)?)?)
            }
            FETCH_OP => Ok(serde_json::to_vec(
                &self.fetch(serde_json::from_slice(body)?)?,
            )?),
            "objects.write" => Ok(serde_json::to_vec(
                &json!({"stored":self.write(serde_json::from_slice(body)?)?}),
            )?),
            _ => anyhow::bail!("UNSUPPORTED_OPERATION"),
        }
    }
}
impl Handler for Service {
    fn handle(&self, req: Incoming) {
        match self.handle_request(&req.op, &req.header, &req.body) {
            Ok(body) => req.responder.reply(json!({}), body),
            Err(error) => req.responder.error(super::control::error_wire(&error)),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        content::ObjectFetcher,
        model::errors::ErrorWire,
        session::{Conn, Priority},
    };
    use anyhow::Context;
    use std::{
        path::Path,
        time::{Duration, Instant},
    };

    fn journal(engine: &Engine) -> Result<Vec<Event>> {
        Ok(lock(&engine.store)
            .events_after(0, 4096)?
            .into_iter()
            .map(|(_, event)| event)
            .collect())
    }

    #[test]
    fn feed_reply_omits_only_exact_current_request_echo_and_keeps_page_cursor() -> Result<()> {
        let source_home = tempfile::tempdir()?;
        let target_home = tempfile::tempdir()?;
        let source = Engine::open(source_home.path(), Some("repo"))?;
        let target = Engine::open(target_home.path(), Some("repo"))?;
        {
            let mut store = lock(&source.store);
            for n in 0..257 {
                store.create(&format!("file-{n}"), crate::core::EntryKind::File)?;
            }
            store.sync_metadata()?;
        }
        let events = journal(&source)?;
        let service = service(&target);
        let first = service.feed_reply(FeedRequest {
            after: 0,
            events: events[..256].to_vec(),
        })?;
        assert!(
            first.events.is_empty(),
            "the request already carries these exact events"
        );
        assert_eq!(first.cursor, 256);
        assert!(
            first.more,
            "pagination is based on scanned rows, not filtered replies"
        );
        let last = service.feed_reply(FeedRequest {
            after: first.cursor,
            events: events[256..].to_vec(),
        })?;
        assert!(last.events.is_empty());
        assert_eq!(last.cursor, 257);
        assert!(!last.more);
        assert_eq!(journal(&target)?, events);
        let replay = service.feed_reply(FeedRequest {
            after: 0,
            events: Vec::new(),
        })?;
        assert_eq!(replay.events, events[..256]);
        assert_eq!(replay.cursor, 256);
        assert!(replay.more);
        let tail = service.feed_reply(FeedRequest {
            after: replay.cursor,
            events: Vec::new(),
        })?;
        assert_eq!(tail.events, events[256..]);
        assert_eq!(tail.cursor, 257);
        assert!(!tail.more);
        source.shutdown()?;
        target.shutdown()?;
        Ok(())
    }

    #[test]
    fn feed_reply_preserves_independent_history_and_rejects_id_collisions() -> Result<()> {
        let source_home = tempfile::tempdir()?;
        let target_home = tempfile::tempdir()?;
        let source = Engine::open(source_home.path(), Some("repo"))?;
        let target = Engine::open(target_home.path(), Some("repo"))?;
        lock(&source.store).create("source", crate::core::EntryKind::File)?;
        lock(&target.store).create("independent", crate::core::EntryKind::File)?;
        let source_events = journal(&source)?;
        let independent = journal(&target)?;
        let service = service(&target);
        let reply = service.feed_reply(FeedRequest {
            after: 0,
            events: source_events.clone(),
        })?;
        assert_eq!(reply.events, independent);
        assert_eq!(reply.cursor, 2);
        assert!(!reply.more);
        let mut forged = source_events[0].clone();
        forged.state.name = "forged".into();
        let error = service
            .feed_reply(FeedRequest {
                after: 0,
                events: vec![forged],
            })
            .err()
            .unwrap();
        assert!(error.to_string().contains("EVENT_ID_COLLISION"));
        assert!(lock(&target.store).lookup("source").is_ok());
        assert!(lock(&target.store).lookup("forged").is_err());
        assert_eq!(journal(&target)?.len(), 2);
        source.shutdown()?;
        target.shutdown()?;
        Ok(())
    }

    #[test]
    fn feed_reply_keeps_waiting_children_and_recovers_unknown_self_authored_history() -> Result<()>
    {
        let source_home = tempfile::tempdir()?;
        let target_home = tempfile::tempdir()?;
        let restored_home = tempfile::tempdir()?;
        let source = Engine::open(source_home.path(), Some("repo"))?;
        {
            let mut store = lock(&source.store);
            store.sync_metadata()?;
            store.db.execute(
                "VACUUM INTO ?1",
                [restored_home
                    .path()
                    .join("metadata.sqlite")
                    .to_str()
                    .unwrap()],
            )?;
            let entry = store.create("file", crate::core::EntryKind::File)?;
            store.delete(&entry.id)?;
            store.sync_metadata()?;
        }
        let events = journal(&source)?;
        assert_eq!(events.len(), 2);
        let target = Engine::open(target_home.path(), Some("repo"))?;
        let service = service(&target);
        let waiting = service.feed_reply(FeedRequest {
            after: 0,
            events: vec![events[1].clone()],
        })?;
        assert!(waiting.events.is_empty());
        assert_eq!(waiting.cursor, 0);
        assert!(journal(&target)?.is_empty());
        let resolved = service.feed_reply(FeedRequest {
            after: 0,
            events: vec![events[0].clone()],
        })?;
        assert_eq!(resolved.events, vec![events[1].clone()]);
        assert_eq!(resolved.cursor, 2);
        let restored = Engine::open(restored_home.path(), None)?;
        assert_eq!(lock(&restored.store).device, lock(&source.store).device);
        assert!(journal(&restored)?.is_empty());
        let recovery = service.feed_reply(FeedRequest {
            after: 0,
            events: journal(&restored)?,
        })?;
        assert!(
            recovery
                .events
                .iter()
                .all(|event| event.author == lock(&restored.store).device)
        );
        assert_eq!(recovery.events, events);
        assert_eq!(restored.apply_remote(&recovery.events)?.len(), 2);
        assert_eq!(journal(&restored)?, events);
        assert!(lock(&restored.store).lookup("file").is_err());
        source.shutdown()?;
        target.shutdown()?;
        restored.shutdown()?;
        Ok(())
    }

    #[test]
    fn feed_reply_rejects_oversized_requests_before_any_apply() -> Result<()> {
        let source_home = tempfile::tempdir()?;
        let target_home = tempfile::tempdir()?;
        let source = Engine::open(source_home.path(), Some("repo"))?;
        let target = Engine::open(target_home.path(), Some("repo"))?;
        lock(&source.store).create("file", crate::core::EntryKind::File)?;
        let service = service(&target);
        let error = service
            .feed_reply(FeedRequest {
                after: 0,
                events: vec![journal(&source)?[0].clone(); 257],
            })
            .err()
            .unwrap();
        assert!(error.to_string().contains("FEED_BATCH_TOO_LARGE"));
        assert!(journal(&target)?.is_empty());
        source.shutdown()?;
        target.shutdown()?;
        Ok(())
    }

    #[test]
    fn authenticated_meta_exchange_pages_with_empty_echoes_and_coded_errors() -> Result<()> {
        use crate::pairing_code::Verifier;
        struct Policy(Verifier);
        impl crate::session::AcceptPolicy for Policy {
            fn installation(&self) -> String {
                "feed-test-server".into()
            }
            fn verifier(&self) -> Option<(Verifier, u64)> {
                Some((self.0.clone(), 1))
            }
            fn admit(
                &self,
                _: &crate::model::protocol::frame::ClientHello,
            ) -> std::result::Result<(), ErrorWire> {
                Ok(())
            }
            fn legacy_attempt(&self, _: std::net::SocketAddr, _: &serde_json::Value) {}
        }
        let source_home = tempfile::tempdir()?;
        let target_home = tempfile::tempdir()?;
        let source = Engine::open(source_home.path(), Some("repo"))?;
        let target = Engine::open(target_home.path(), Some("repo"))?;
        {
            let mut store = lock(&source.store);
            for n in 0..257 {
                store.create(&format!("file-{n}"), crate::core::EntryKind::File)?;
            }
            store.sync_metadata()?;
        }
        let events = journal(&source)?;
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        let (code, verifier) = Verifier::generate();
        let serving = target.clone();
        let (ready, received) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            let result = (|| -> Result<Conn> {
                let deadline = Instant::now() + Duration::from_secs(5);
                let stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error)
                            if error.kind() == std::io::ErrorKind::WouldBlock
                                && Instant::now() < deadline =>
                        {
                            std::thread::sleep(Duration::from_millis(1))
                        }
                        Err(error) => return Err(error.into()),
                    }
                };
                let (conn, _) = crate::session::accept(
                    stream,
                    Arc::new(Policy(verifier)),
                    crate::session::ConnOptions::default(),
                )
                .map_err(|error| anyhow::anyhow!("{error}"))?;
                conn.serve(Arc::new(service(&serving)));
                Ok(conn)
            })();
            let _ = ready.send(result);
        });
        let (client, _) = crate::session::dial(
            &endpoint,
            crate::session::client_hello(
                "feed-test-client",
                crate::model::protocol::LaneRole::Control,
                None,
                1,
            ),
            &crate::session::CodeAuth { code },
            None,
            crate::session::ConnOptions::default(),
        )
        .map_err(|error| anyhow::anyhow!("{error}"))?;
        let server = received.recv_timeout(Duration::from_secs(10))??;
        worker.join().unwrap();
        let reply = client
            .call(
                "meta.exchange",
                json!({"repo":"repo"}),
                serde_json::to_vec(&FeedRequest {
                    after: 0,
                    events: events[..256].to_vec(),
                })?,
                Priority::Control,
                Duration::from_secs(10),
            )
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        let reply: FeedReply = serde_json::from_slice(&reply.body)?;
        assert!(reply.events.is_empty());
        assert_eq!(reply.cursor, 256);
        assert!(reply.more);
        // A retry of the accepted first page is harmless; the tail follows.
        let exchange = |after: u64, events: &[Event]| -> Result<FeedReply> {
            let reply = client
                .call(
                    "meta.exchange",
                    json!({"repo":"repo"}),
                    serde_json::to_vec(&FeedRequest {
                        after,
                        events: events.to_vec(),
                    })?,
                    Priority::Control,
                    Duration::from_secs(10),
                )
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            Ok(serde_json::from_slice(&reply.body)?)
        };
        let retry = exchange(0, &events[..256])?;
        assert!(retry.events.is_empty());
        assert_eq!((retry.cursor, retry.more), (256, true));
        let tail = exchange(retry.cursor, &events[256..])?;
        assert!(tail.events.is_empty());
        assert_eq!((tail.cursor, tail.more), (257, false));
        // Failures carry the message's leading code.
        match client.call(
            "meta.exchange",
            json!({"repo":"other"}),
            Vec::new(),
            Priority::Control,
            Duration::from_secs(10),
        ) {
            Err(crate::model::orchestrator::CallError::Remote(error)) => {
                assert_eq!(error.code, "REPOSITORY_MISMATCH")
            }
            other => panic!("expected REPOSITORY_MISMATCH, got {:?}", other.map(|_| ())),
        }
        assert_eq!(journal(&source)?, events);
        assert_eq!(journal(&target)?, events);
        client.close();
        server.close();
        source.shutdown()?;
        target.shutdown()?;
        Ok(())
    }

    fn service(engine: &Arc<Engine>) -> Service {
        Service {
            engine: engine.clone(),
            demand: crate::orchestrator::demand::Demand::new(
                crate::orchestrator::demand::Seq::new(),
                engine.working.content().packs.clone(),
            ),
        }
    }

    fn call(
        service: &Service,
        op: &str,
        header: serde_json::Value,
        body: &[u8],
    ) -> Result<Vec<u8>> {
        service.handle_request(op, &header, body)
    }

    fn code(error: &anyhow::Error) -> String {
        crate::orchestrator::control::error_wire(error).code
    }

    #[test]
    fn feed_outgoing_pages_and_feed_apply_replicates() -> Result<()> {
        let source_home = tempfile::tempdir()?;
        let target_home = tempfile::tempdir()?;
        let source = Engine::open(source_home.path(), Some("repo"))?;
        let target = Engine::open(target_home.path(), Some("repo"))?;
        {
            let mut store = lock(&source.store);
            for n in 0..300 {
                store.create(&format!("file-{n}"), crate::core::EntryKind::File)?;
            }
            store.sync_metadata()?;
        }
        let events = journal(&source)?;
        assert_eq!(events.len(), 300);
        let outgoing = service(&source);
        let first: FeedPage =
            serde_json::from_slice(&call(&outgoing, "feed.outgoing", json!({"after":0}), &[])?)?;
        assert_eq!(first.events.len(), 256);
        assert_eq!(first.events, events[..256]);
        assert!(first.more);
        assert_eq!(first.cursor, 256);
        let last: FeedPage = serde_json::from_slice(&call(
            &outgoing,
            "feed.outgoing",
            json!({"after":first.cursor}),
            &[],
        )?)?;
        assert_eq!(last.events.len(), 44);
        assert_eq!(last.events, events[256..]);
        assert!(!last.more);
        assert_eq!(last.cursor, 300);
        let empty: FeedPage = serde_json::from_slice(&call(
            &outgoing,
            "feed.outgoing",
            json!({"after":last.cursor}),
            &[],
        )?)?;
        assert!(empty.events.is_empty() && !empty.more);
        assert_eq!(empty.cursor, 300, "an empty page keeps the cursor");

        let incoming = service(&target);
        let applied: serde_json::Value = serde_json::from_slice(&call(
            &incoming,
            "feed.apply",
            json!({}),
            &serde_json::to_vec(&events)?,
        )?)?;
        assert_eq!(applied, json!({"applied":300}));
        assert_eq!(journal(&target)?, events);
        let again: serde_json::Value = serde_json::from_slice(&call(
            &incoming,
            "feed.apply",
            json!({}),
            &serde_json::to_vec(&events)?,
        )?)?;
        assert_eq!(again, json!({"applied":0}), "re-applying is idempotent");
        assert_eq!(journal(&target)?, events);
        source.shutdown()?;
        target.shutdown()?;
        Ok(())
    }

    #[test]
    fn objects_write_stores_verified_objects_and_rejects_bad_digest() -> Result<()> {
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), Some("repo"))?;
        let service = service(&engine);
        let packs = engine.working.content().packs.clone();
        let good = Object {
            digest: Digest::bytes(b"abc"),
            kind: Kind::Chunk,
            bytes: b"abc".to_vec(),
        };
        let reply: serde_json::Value = serde_json::from_slice(&call(
            &service,
            "objects.write",
            json!({"repo":"repo"}),
            &serde_json::to_vec(&vec![good.clone()])?,
        )?)?;
        assert_eq!(reply, json!({"stored":1}));
        assert!(packs.contains(good.digest)?);
        assert!(
            indexed(home.path(), good.digest)?,
            "write flushes the packs"
        );

        // One bad object rejects the whole batch: its valid neighbour is not
        // stored either.
        let neighbour = Object {
            digest: Digest::bytes(b"xyz"),
            kind: Kind::Chunk,
            bytes: b"xyz".to_vec(),
        };
        let bad = Object {
            digest: Digest::bytes(b"abc"),
            kind: Kind::Chunk,
            bytes: b"abd".to_vec(),
        };
        let error = call(
            &service,
            "objects.write",
            json!({"repo":"repo"}),
            &serde_json::to_vec(&vec![neighbour.clone(), bad])?,
        )
        .unwrap_err();
        assert_eq!(code(&error), "INVALID_PEER_OBJECT", "{error:#}");
        assert!(!packs.contains(neighbour.digest)?);
        assert!(!packs.contains(Digest::bytes(b"abd"))?);

        let error = call(
            &service,
            "objects.write",
            json!({"repo":"other"}),
            &serde_json::to_vec(&vec![neighbour.clone()])?,
        )
        .unwrap_err();
        assert_eq!(code(&error), "REPOSITORY_MISMATCH", "{error:#}");
        assert!(!packs.contains(neighbour.digest)?);
        engine.shutdown()?;
        Ok(())
    }

    #[test]
    fn objects_missing_lists_unfetched_content_refs_then_demand_wakes_waiters() -> Result<()> {
        let source_home = tempfile::tempdir()?;
        let target_home = tempfile::tempdir()?;
        let source = Engine::open(source_home.path(), Some("repo"))?;
        let target = Engine::open(target_home.path(), Some("repo"))?;
        let mut large = vec![7; crate::model::content::CHUNK_SIZE as usize];
        large.extend_from_slice(b"tail");
        for (path, bytes) in [
            ("small", b"small".as_slice()),
            ("large", &large),
            ("gone", b"deleted".as_slice()),
        ] {
            let h = source.open_file(path, true, false, true)?;
            source.write(h, 0, bytes, false, false)?;
            source.flush(Some(h))?;
            source.close(h)?;
        }
        {
            let mut store = lock(&source.store);
            let gone = store.lookup("gone")?.id;
            store.delete(&gone)?;
            store.sync_metadata()?;
        }
        let small = source.lookup("small")?.content.unwrap();
        let manifest = source.lookup("large")?.content.unwrap();
        assert_eq!(manifest.kind, ContentKind::Manifest);
        let chunks = source.working.content().chunks(&manifest)?;
        assert_eq!(chunks.len(), 2);
        let outgoing = service(&source);
        let incoming = service(&target);
        call(
            &incoming,
            "feed.apply",
            json!({}),
            &serde_json::to_vec(&journal(&source)?)?,
        )?;
        let missing = |limit: u64| -> Result<Vec<(Digest, Kind)>> {
            Ok(serde_json::from_slice(&call(
                &incoming,
                "objects.missing",
                json!({"limit":limit}),
                &[],
            )?)?)
        };
        // Roots only while the manifest is absent; a deleted entry's content
        // is not wanted.
        let roots = missing(64)?;
        assert_eq!(roots.len(), 2, "{roots:?}");
        assert!(roots.contains(&(small.digest, Kind::Chunk)));
        assert!(roots.contains(&(manifest.digest, Kind::Manifest)));
        assert_eq!(missing(1)?.len(), 1);
        assert_eq!(missing(0)?.len(), 0);

        // Supply the manifest the way the orchestrator would: fetch from the
        // source, write into the target.
        let fetched = call(
            &outgoing,
            FETCH_OP,
            json!({"repo":"repo"}),
            &serde_json::to_vec(&vec![(manifest.digest, Kind::Manifest)])?,
        )?;
        call(&incoming, "objects.write", json!({"repo":"repo"}), &fetched)?;
        let listed = missing(64)?;
        assert_eq!(
            listed,
            vec![
                (small.digest, Kind::Chunk),
                (chunks[0], Kind::Chunk),
                (chunks[1], Kind::Chunk),
            ],
            "roots first, then the present manifest's chunks"
        );

        // A reader blocked on a missing chunk is woken by objects.write.
        let (sent, received) = std::sync::mpsc::channel();
        let demand = incoming.demand.clone();
        let wanted = chunks[0];
        let waiter = std::thread::spawn(move || {
            let _ = sent.send(demand.fetch(&[(wanted, Kind::Chunk)]));
        });
        let end = Instant::now() + Duration::from_secs(5);
        while !incoming.demand.pending().contains(&(wanted, Kind::Chunk)) {
            assert!(Instant::now() < end, "the fetch never registered demand");
            std::thread::sleep(Duration::from_millis(5));
        }
        let fetched = call(
            &outgoing,
            FETCH_OP,
            json!({"repo":"repo"}),
            &serde_json::to_vec(&vec![(wanted, Kind::Chunk)])?,
        )?;
        let stored: serde_json::Value = serde_json::from_slice(&call(
            &incoming,
            "objects.write",
            json!({"repo":"repo"}),
            &fetched,
        )?)?;
        assert_eq!(stored, json!({"stored":1}));
        received
            .recv_timeout(Duration::from_secs(1))
            .context("the blocked fetch was not woken within 1 s")??;
        waiter.join().unwrap();
        assert!(incoming.demand.pending().is_empty());
        assert_eq!(
            missing(64)?,
            vec![(small.digest, Kind::Chunk), (chunks[1], Kind::Chunk)]
        );
        source.shutdown()?;
        target.shutdown()?;
        Ok(())
    }

    #[test]
    fn worker_sync_ops_route_and_check_the_repository() -> Result<()> {
        for op in [
            "feed.outgoing",
            "feed.apply",
            "meta.exchange",
            "objects.missing",
            "objects.fetch",
            "objects.write",
        ] {
            assert!(Service::routes(op), "{op}");
        }
        for op in ["status", "watch", "feeds", "objectsx", "meta.other"] {
            assert!(!Service::routes(op), "{op}");
        }
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), Some("repo"))?;
        let service = service(&engine);
        for op in ["meta.exchange", FETCH_OP, "objects.write"] {
            let error = call(&service, op, json!({}), b"[]").unwrap_err();
            assert_eq!(code(&error), "REPOSITORY_MISMATCH", "{op}: {error:#}");
        }
        let error = call(&service, "feed.nothing", json!({}), &[]).unwrap_err();
        assert_eq!(code(&error), "UNSUPPORTED_OPERATION");
        engine.shutdown()?;
        Ok(())
    }

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
}
