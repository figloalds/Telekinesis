//! The orchestrator's link to one folder worker: one control lane for RPC,
//! `feed.*`, `meta.exchange` and the `watch` long poll, and N bulk lanes for
//! `objects.fetch` / `objects.write`. Every lane is a token-authenticated
//! loopback session whose `X-Tkfs-Lane` header makes the worker enforce the
//! lane's op policy; the link also refuses object ops on the control lane
//! itself, before anything is sent.
use super::{
    sync::{
        Fetched, HAVE_BATCH, HaveReply, MissingPage, OBJECT_BATCH, Object, WantedPage,
        encode_objects, json_runs,
    },
    worker::Ready,
};
use crate::{
    content::Kind,
    model::{
        content::Digest,
        errors::ErrorWire,
        protocol::{IPC_BINARY_OBJECTS, LaneRole},
    },
    session::{self, CallError, Conn, ConnHealth, ConnOptions, Priority, Reply},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

/// Deadline of one control or object call.
pub const CALL_DEADLINE: Duration = Duration::from_secs(10);
/// How long one `watch` parks on the worker.
pub const WATCH_WAIT_MS: u64 = 2000;
/// Deadline of one `watch`: its park plus slack for the reply.
pub const WATCH_DEADLINE: Duration = Duration::from_secs(4);

/// A `watch` reply: the worker's wake-up counter, its latest feed position,
/// its health summary, the objects its readers are waiting for and those they
/// are expected to want next.
#[derive(Debug, Deserialize)]
pub struct Watch {
    pub seq: u64,
    /// The value `feed.outgoing` pages after to find nothing new. An adopted
    /// worker from before this field reports 0 and so never signals a change.
    #[serde(default)]
    pub feed: u64,
    pub heartbeat: Value,
    pub demand: Vec<(Digest, Kind)>,
    /// Read-ahead, nearest first. An adopted worker from before this field has none.
    #[serde(default)]
    pub hints: Vec<(Digest, Kind)>,
}

pub struct WorkerLink {
    pub ready: Ready,
    control: Conn,
    bulk: Vec<Conn>,
}

impl WorkerLink {
    /// Dials the worker named by `ready`: one control lane, then `bulk_lanes`
    /// (at least one) bulk lanes. If any dial fails, the lanes already open
    /// are closed.
    pub fn connect(ready: &Ready, bulk_lanes: usize) -> Result<Arc<Self>, CallError> {
        let dial = |role: LaneRole| {
            session::dial_loopback_as(
                &ready.address,
                Some(&ready.token),
                role,
                ConnOptions::for_role(role),
            )
            .map_err(|error| CallError::Unavailable(error.to_string()))
        };
        let control = dial(LaneRole::Control)?;
        let mut bulk = Vec::with_capacity(bulk_lanes.max(1));
        for _ in 0..bulk_lanes.max(1) {
            match dial(LaneRole::Bulk) {
                Ok(conn) => bulk.push(conn),
                Err(error) => {
                    control.close();
                    bulk.iter().for_each(Conn::close);
                    return Err(error);
                }
            }
        }
        Ok(Arc::new(Self {
            ready: ready.clone(),
            control,
            bulk,
        }))
    }

    /// One call on the control lane. Object ops are refused here with
    /// `OP_NOT_ALLOWED`, without sending anything.
    pub fn call(
        &self,
        op: &str,
        payload: Value,
        body: Vec<u8>,
        deadline: Duration,
    ) -> Result<Reply, CallError> {
        if op.starts_with("objects.") {
            return Err(CallError::Remote(ErrorWire::coded(
                "OP_NOT_ALLOWED",
                format!("{op} is not allowed on a control lane"),
            )));
        }
        self.control
            .call(op, payload, body, Priority::Control, deadline)
    }

    /// [`Self::call`] with no body, its reply body parsed as JSON (`null`
    /// when empty).
    pub fn call_json(&self, op: &str, payload: Value) -> Result<Value, CallError> {
        let reply = self.call(op, payload, Vec::new(), CALL_DEADLINE)?;
        parse(op, &reply.body)
    }

    /// Long-polls the worker for a wake-up past `after_seq` (or pending
    /// demand), parked at most [`WATCH_WAIT_MS`].
    pub fn watch(&self, after_seq: u64) -> Result<Watch, CallError> {
        let reply = self.call(
            "watch",
            json!({"after_seq": after_seq, "wait_ms": WATCH_WAIT_MS}),
            Vec::new(),
            WATCH_DEADLINE,
        )?;
        parse("watch", &reply.body)
    }

    /// `objects.missing {limit, exclude, after}` on the first bulk lane
    /// (object ops are refused on the control lane): up to `limit` content
    /// objects the worker's live entries need and its packs lack, `exclude`
    /// (at most `sync::MAX_EXCLUDE`) left out, the scan resumed after the
    /// cursor `after`. The page's `cursor` resumes after it (`None`: the scan
    /// reached its end).
    pub fn missing(
        &self,
        limit: usize,
        exclude: &[Digest],
        after: Option<&str>,
    ) -> Result<MissingPage, CallError> {
        let reply = self.bulk[0].call(
            "objects.missing",
            json!({"limit": limit, "exclude": exclude, "after": after}),
            Vec::new(),
            Priority::Data,
            CALL_DEADLINE,
        )?;
        parse("objects.missing", &reply.body)
    }

    /// `objects.wanted {repo, limit, exclude, after}` on the first bulk lane:
    /// the objects the worker's readers are blocked on, and (`limit` > 0) a
    /// page of its `objects.missing` scan, resumed after `after`.
    pub fn wanted(
        &self,
        limit: usize,
        exclude: &[Digest],
        after: Option<&str>,
    ) -> Result<WantedPage, CallError> {
        let reply = self.bulk[0].call(
            "objects.wanted",
            json!({"repo": self.ready.repo, "limit": limit, "exclude": exclude, "after": after}),
            Vec::new(),
            Priority::Data,
            CALL_DEADLINE,
        )?;
        parse("objects.wanted", &reply.body)
    }

    /// Whether this worker takes a binary `objects.write` body. A worker adopted
    /// from before that says so in its record. It reads only JSON, and answers
    /// `objects.fetch` in JSON whatever it is asked.
    pub fn takes_binary(&self) -> bool {
        self.ready.ipc >= IPC_BINARY_OBJECTS
    }

    /// Reads up to [`OBJECT_BATCH`] objects from the worker on the bulk lane
    /// of the first digest; absent ones are left out of the reply.
    pub fn fetch(&self, wanted: &[(Digest, Kind)]) -> Result<Vec<Object>, CallError> {
        self.fetch_reply(wanted, self.takes_binary())
            .map(|fetched| fetched.objects)
    }

    /// [`Self::fetch`] with where the worker cut the reply to its byte
    /// bounds ([`Fetched`]): those of a binary reply when `binary`, else those of
    /// JSON, for a caller that has to pass them on as JSON.
    pub fn fetch_reply(
        &self,
        wanted: &[(Digest, Kind)],
        binary: bool,
    ) -> Result<Fetched, CallError> {
        let Some((first, _)) = wanted.first() else {
            return Ok(Fetched::default());
        };
        check_batch(wanted.len())?;
        let body = encode("objects.fetch", &wanted)?;
        let reply = self.bulk[self.lane_for(first)].call(
            "objects.fetch",
            json!({"repo": self.ready.repo, "binary": binary}),
            body,
            Priority::Data,
            CALL_DEADLINE,
        )?;
        Fetched::from_reply(&reply.header, &reply.body)
            .map_err(|error| bad_reply("objects.fetch", &format!("{error:#}")))
    }

    /// Stores up to [`OBJECT_BATCH`] objects in the worker on the bulk lane of
    /// the first digest; the number stored. To a worker that takes only JSON
    /// ([`Self::takes_binary`]) this can be several requests.
    pub fn write(&self, objects: Vec<Object>) -> Result<usize, CallError> {
        let Some(first) = objects.first() else {
            return Ok(0);
        };
        check_batch(objects.len())?;
        let lane = self.lane_for(&first.digest);
        if self.takes_binary() {
            let body = encode_objects(&objects);
            drop(objects);
            return self.write_body(lane, body);
        }
        let mut stored = 0;
        for run in json_runs(&objects) {
            stored += self.write_body(lane, encode("objects.write", &run)?)?;
        }
        Ok(stored)
    }

    /// One `objects.write` of `body` on `lane`; the number stored.
    fn write_body(&self, lane: usize, body: Vec<u8>) -> Result<usize, CallError> {
        let reply = self.bulk[lane].call(
            "objects.write",
            json!({"repo": self.ready.repo}),
            body,
            Priority::Data,
            CALL_DEADLINE,
        )?;
        let stored: Value = parse("objects.write", &reply.body)?;
        stored["stored"]
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| bad_reply("objects.write", "no stored count"))
    }

    /// `objects.have {repo}` on the first bulk lane (it carries no ownership):
    /// which of `digests` (at most `sync::HAVE_BATCH`) the worker holds, its
    /// packs flushed first.
    pub fn have(&self, digests: &[Digest]) -> Result<Vec<Digest>, CallError> {
        if digests.is_empty() {
            return Ok(Vec::new());
        }
        if digests.len() > HAVE_BATCH {
            return Err(CallError::Remote(ErrorWire::coded(
                "ARGUMENT_TOO_LARGE",
                format!("{} digests exceed the batch of {HAVE_BATCH}", digests.len()),
            )));
        }
        let body = encode("objects.have", &digests)?;
        let reply = self.bulk[0].call(
            "objects.have",
            json!({"repo": self.ready.repo}),
            body,
            Priority::Data,
            CALL_DEADLINE,
        )?;
        parse::<HaveReply>("objects.have", &reply.body).map(|reply| reply.held)
    }

    /// The bulk lane that carries objects batched behind `digest`.
    pub fn lane_for(&self, digest: &Digest) -> usize {
        digest.0[0] as usize % self.bulk.len()
    }

    /// Every lane's role and health, control first.
    pub fn health(&self) -> Vec<(LaneRole, ConnHealth)> {
        std::iter::once((LaneRole::Control, self.control.health()))
            .chain(self.bulk.iter().map(|conn| (LaneRole::Bulk, conn.health())))
            .collect()
    }

    /// Whether every lane is still open.
    pub fn alive(&self) -> bool {
        self.lanes().all(|conn| conn.closed().is_none())
    }

    /// Closes every lane; calls in flight fail with `ConnectionLost`.
    pub fn close(&self) {
        self.lanes().for_each(Conn::close);
    }

    fn lanes(&self) -> impl Iterator<Item = &Conn> {
        std::iter::once(&self.control).chain(&self.bulk)
    }
}

fn check_batch(len: usize) -> Result<(), CallError> {
    if len > OBJECT_BATCH {
        return Err(CallError::Remote(ErrorWire::coded(
            "OBJECT_BATCH_TOO_LARGE",
            format!("{len} objects exceed the batch of {OBJECT_BATCH}"),
        )));
    }
    Ok(())
}

/// The JSON request body (nothing is sent when it does not encode).
fn encode(op: &str, value: &impl serde::Serialize) -> Result<Vec<u8>, CallError> {
    serde_json::to_vec(value).map_err(|error| {
        CallError::Remote(ErrorWire::coded(
            "PROTOCOL_VIOLATION",
            format!("{op} request does not encode: {error}"),
        ))
    })
}

fn parse<T: serde::de::DeserializeOwned>(op: &str, body: &[u8]) -> Result<T, CallError> {
    let body = if body.is_empty() {
        b"null".as_slice()
    } else {
        body
    };
    serde_json::from_slice(body).map_err(|error| bad_reply(op, &error.to_string()))
}

fn bad_reply(op: &str, detail: &str) -> CallError {
    CallError::Remote(ErrorWire::coded(
        "PROTOCOL_VIOLATION",
        format!("malformed {op} reply: {detail}"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        orchestrator::sync::{BINARY_MAGIC, MAX_REPLY_JSON, decode_objects},
        session::{Handler, Incoming},
    };
    use std::{net::TcpListener, sync::Mutex, thread};

    const TOKEN: &str = "worker-client-test-token";

    /// A loopback worker that keeps each `objects.write` body and each
    /// `objects.fetch` header, stores whatever decodes, and fetches nothing.
    #[derive(Default)]
    struct Recording {
        written: Mutex<Vec<Vec<u8>>>,
        fetched: Mutex<Vec<Value>>,
    }

    impl Handler for Recording {
        fn handle(&self, req: Incoming) {
            match req.op.as_str() {
                "objects.write" => {
                    let stored = decode_objects(&req.body).unwrap().len();
                    self.written.lock().unwrap().push(req.body);
                    let reply = serde_json::to_vec(&json!({"stored": stored})).unwrap();
                    req.responder.reply(json!({}), reply);
                }
                _ => {
                    self.fetched.lock().unwrap().push(req.header.clone());
                    req.responder.reply(json!({}), b"[]".to_vec());
                }
            }
        }
    }

    /// A link to a new [`Recording`] worker that says it is of `ipc`.
    fn link_to(ipc: u32) -> (Arc<WorkerLink>, Arc<Recording>) {
        let worker = Arc::new(Recording::default());
        let handler: Arc<dyn Handler> = worker.clone();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("ws://{}", listener.local_addr().unwrap());
        thread::spawn(move || {
            let mut conns = Vec::new();
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                if let Ok(conn) = session::accept_loopback(
                    stream,
                    Some(TOKEN),
                    ConnOptions::for_role(LaneRole::Bulk),
                ) {
                    conn.serve(handler.clone());
                    conns.push(conn);
                }
            }
        });
        let ready = Ready {
            ready: true,
            repo: "repo".into(),
            device: "device".into(),
            instance: "instance".into(),
            pid: 1,
            address,
            token: TOKEN.into(),
            version: "test".into(),
            ipc,
            mounts: Vec::new(),
        };
        (WorkerLink::connect(&ready, 1).unwrap(), worker)
    }

    fn object(bytes: Vec<u8>) -> Object {
        Object {
            digest: Digest::bytes(&bytes),
            kind: Kind::Manifest,
            bytes,
        }
    }

    #[test]
    fn a_worker_gets_binary_from_the_ipc_that_takes_it_and_json_before_that() {
        let (current, worker) = link_to(IPC_BINARY_OBJECTS);
        assert!(current.takes_binary());
        assert_eq!(current.write(vec![object(vec![1, 2, 3])]).unwrap(), 1);
        assert_eq!(
            worker.written.lock().unwrap()[0][..BINARY_MAGIC.len()],
            BINARY_MAGIC
        );

        let (older, worker) = link_to(IPC_BINARY_OBJECTS - 1);
        assert!(!older.takes_binary());
        assert_eq!(older.write(vec![object(vec![1, 2, 3])]).unwrap(), 1);
        let written = worker.written.lock().unwrap();
        let sent: Vec<Object> = serde_json::from_slice(&written[0]).unwrap();
        assert_eq!(sent[0].bytes, [1, 2, 3]);
    }

    #[test]
    fn an_older_worker_gets_as_many_json_bodies_as_a_batch_needs() {
        // Fine as one binary body, and about 32 MiB as JSON, twice what a session
        // carries.
        let mib = 1024 * 1024;
        let objects: Vec<Object> = (0..4)
            .map(|n| {
                let mut bytes = vec![0xFF; 2 * mib];
                bytes[0] = n;
                object(bytes)
            })
            .collect();
        let (older, worker) = link_to(IPC_BINARY_OBJECTS - 1);
        assert_eq!(older.write(objects.clone()).unwrap(), 4);
        let written = worker.written.lock().unwrap();
        assert!(written.len() > 1, "{} bodies", written.len());
        let mut delivered = Vec::new();
        for body in written.iter() {
            assert!(body.len() <= MAX_REPLY_JSON, "{} bytes", body.len());
            let sent: Vec<Object> = serde_json::from_slice(body).unwrap();
            delivered.extend(sent.into_iter().map(|object| object.digest));
        }
        let all: Vec<Digest> = objects.iter().map(|object| object.digest).collect();
        assert_eq!(delivered, all);
    }

    #[test]
    fn a_fetch_says_which_encoding_its_bounds_are_for() {
        // An older worker ignores this and answers JSON, which reads the same.
        let (link, worker) = link_to(IPC_BINARY_OBJECTS - 1);
        let wanted = [(Digest::bytes(b"x"), Kind::Chunk)];
        assert!(link.fetch_reply(&wanted, true).unwrap().objects.is_empty());
        assert!(link.fetch_reply(&wanted, false).unwrap().objects.is_empty());
        let fetched = worker.fetched.lock().unwrap();
        assert_eq!(fetched[0]["binary"], true);
        assert_eq!(fetched[1]["binary"], false);
    }
}
