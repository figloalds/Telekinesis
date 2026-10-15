//! Owned by WP3.1 (a test helper, included with `#[path]` by
//! `tests/orchestrator.rs` and `tests/qual_wp3_1_transport.rs`).
//!
//! [`WireTap`]: a TCP relay in front of a v3 peer endpoint that reads what the
//! dialer sends (client → server) without changing it: the WebSocket upgrade,
//! then masked WebSocket frames carrying v3 frames (REVAMP_PLAN §6.2). Every
//! `REQUEST` is recorded (connection, session request id, op), with the
//! exchange identity its body carries: `MetaExchangeRequest.request` for
//! `meta.exchange` and the page's `request` for `objects.exchange`. Bodies
//! sent as `MORE` + `CHUNK` frames are reassembled first.
//!
//! Only clients originate requests (§6.2), so this direction is where a
//! replayed request would show: invariant 16 holds when no exchange identity
//! is ever seen twice ([`WireTap::duplicates`]), whatever faults the link
//! behind the tap (a `FaultProxy`) injects. That check means something only
//! when every exchange request was identified: a caller also asserts that
//! [`WireTap::unidentified`] is empty and [`WireTap::malformed`] is 0 (a
//! frame the parser cannot read, a header that is not JSON included).
//! A request whose body never finished crossing the tap (its connection
//! ended mid-body) is not recorded: the peer never received it either.
#![allow(dead_code)]

use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap},
    io::{Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};

/// One request the dialer sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Seen {
    /// The tap's connection number (1, 2, …).
    pub connection: u64,
    /// The session's request id.
    pub id: u64,
    pub op: String,
    /// The exchange identity in the body (`meta.exchange`,
    /// `objects.exchange`), if any.
    pub request: Option<String>,
}

struct Shared {
    target: SocketAddr,
    next: AtomicU64,
    closing: AtomicBool,
    seen: Mutex<Vec<Seen>>,
    live: Mutex<BTreeMap<u64, (TcpStream, TcpStream)>>,
    /// Malformed client → server streams (the parser gave up on them).
    malformed: AtomicU64,
}

/// See the module docs.
pub struct WireTap {
    addr: SocketAddr,
    shared: Arc<Shared>,
}

impl WireTap {
    /// Relays `127.0.0.1:0` → `target` (`ws://host:port` or `host:port`).
    pub fn start(target: &str) -> Self {
        let hostport = target
            .trim_start_matches("ws://")
            .split('/')
            .next()
            .unwrap_or_default();
        let target = hostport.to_socket_addrs().unwrap().next().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let shared = Arc::new(Shared {
            target,
            next: AtomicU64::new(1),
            closing: AtomicBool::new(false),
            seen: Mutex::new(Vec::new()),
            live: Mutex::new(BTreeMap::new()),
            malformed: AtomicU64::new(0),
        });
        let accept = shared.clone();
        thread::spawn(move || {
            for stream in listener.incoming() {
                if accept.closing.load(Ordering::Relaxed) {
                    return;
                }
                let Ok(client) = stream else { continue };
                let shared = accept.clone();
                thread::spawn(move || relay(shared, client));
            }
        });
        Self { addr, shared }
    }

    /// `ws://127.0.0.1:PORT`: dial or pair with this.
    pub fn endpoint(&self) -> String {
        format!("ws://{}", self.addr)
    }

    /// Every request seen so far, in order.
    pub fn seen(&self) -> Vec<Seen> {
        self.shared.seen.lock().unwrap().clone()
    }

    /// Requests of `op` seen so far.
    pub fn count(&self, op: &str) -> usize {
        self.seen().iter().filter(|seen| seen.op == op).count()
    }

    /// Objects v3 transfers seen so far (`objects.pull`, `objects.push`;
    /// WP3.INT). They carry no exchange identity: an interrupted one is never
    /// replayed but resumed by a fresh request from the receiver's durable
    /// offset (§6.4), which `objects_v3_lanes` checks byte for byte.
    pub fn objects_v3(&self) -> usize {
        self.count("objects.pull") + self.count("objects.push")
    }

    /// Connections relayed so far.
    pub fn connections(&self) -> u64 {
        self.shared.next.load(Ordering::Relaxed) - 1
    }

    /// Client streams the parser could not follow (expected: 0).
    pub fn malformed(&self) -> u64 {
        self.shared.malformed.load(Ordering::Relaxed)
    }

    /// Exchange requests (`meta.exchange`, `objects.exchange`) seen without
    /// an exchange identity (expected: none). [`WireTap::duplicates`] cannot
    /// tell a replay of one of these on a new connection.
    pub fn unidentified(&self) -> Vec<Seen> {
        self.seen()
            .into_iter()
            .filter(|seen| is_exchange(&seen.op) && seen.request.is_none())
            .collect()
    }

    /// Every request sent more than once: an exchange identity seen twice
    /// (anywhere), or a session request id reused on one connection.
    pub fn duplicates(&self) -> Vec<String> {
        let seen = self.seen();
        let mut out = vec![];
        let mut requests: HashMap<&str, &Seen> = HashMap::new();
        let mut ids: HashMap<(u64, u64), &Seen> = HashMap::new();
        for entry in &seen {
            if let Some(request) = entry.request.as_deref()
                && let Some(first) = requests.insert(request, entry)
            {
                out.push(format!(
                    "exchange {request} sent twice: {first:?} and {entry:?}"
                ));
            }
            if let Some(first) = ids.insert((entry.connection, entry.id), entry) {
                out.push(format!(
                    "session request id reused: {first:?} and {entry:?}"
                ));
            }
        }
        out
    }
}

impl Drop for WireTap {
    fn drop(&mut self) {
        self.shared.closing.store(true, Ordering::Relaxed);
        for (_, (a, b)) in std::mem::take(&mut *self.shared.live.lock().unwrap()) {
            let _ = a.shutdown(Shutdown::Both);
            let _ = b.shutdown(Shutdown::Both);
        }
        let _ = TcpStream::connect_timeout(&self.addr, Duration::from_millis(200));
    }
}

fn relay(shared: Arc<Shared>, client: TcpStream) {
    let Ok(upstream) = TcpStream::connect_timeout(&shared.target, Duration::from_secs(3)) else {
        let _ = client.shutdown(Shutdown::Both);
        return;
    };
    let _ = client.set_nodelay(true);
    let _ = upstream.set_nodelay(true);
    let connection = shared.next.fetch_add(1, Ordering::Relaxed);
    let (Ok(c), Ok(u)) = (client.try_clone(), upstream.try_clone()) else {
        return;
    };
    shared.live.lock().unwrap().insert(connection, (c, u));
    // Server → client: copied as is.
    let down = {
        let (Ok(mut from), Ok(mut to)) = (upstream.try_clone(), client.try_clone()) else {
            return;
        };
        thread::spawn(move || {
            let _ = std::io::copy(&mut from, &mut to);
            let _ = to.shutdown(Shutdown::Write);
        })
    };
    // Client → server: copied, and parsed.
    let mut parser = Parser::default();
    let (Ok(mut from), Ok(mut to)) = (client.try_clone(), upstream.try_clone()) else {
        return;
    };
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let n = match from.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        if to.write_all(&buffer[..n]).is_err() {
            break;
        }
        if !parser.broken {
            let mut found = vec![];
            parser.feed(&buffer[..n], &mut |id, op, request| {
                found.push(Seen {
                    connection,
                    id,
                    op,
                    request,
                })
            });
            if parser.broken {
                shared.malformed.fetch_add(1, Ordering::Relaxed);
            }
            if !found.is_empty() {
                shared.seen.lock().unwrap().extend(found);
            }
        }
    }
    let _ = to.shutdown(Shutdown::Write);
    let _ = down.join();
    shared.live.lock().unwrap().remove(&connection);
    let _ = client.shutdown(Shutdown::Both);
    let _ = upstream.shutdown(Shutdown::Both);
}

/// The client → server stream: an HTTP upgrade, then WebSocket frames.
#[derive(Default)]
struct Parser {
    buf: Vec<u8>,
    upgraded: bool,
    /// The message being assembled from WebSocket fragments.
    message: Vec<u8>,
    in_message: bool,
    /// Request bodies continued in CHUNK frames: id → (op, body so far).
    pending: HashMap<u64, (String, Vec<u8>)>,
    broken: bool,
}

const KIND_REQUEST: u8 = 5;
const KIND_CHUNK: u8 = 7;
const FLAG_MORE: u8 = 0b01;
const FLAG_LAST: u8 = 0b10;

impl Parser {
    fn feed(&mut self, bytes: &[u8], found: &mut dyn FnMut(u64, String, Option<String>)) {
        self.buf.extend_from_slice(bytes);
        if !self.upgraded {
            let Some(end) = self.buf.windows(4).position(|w| w == b"\r\n\r\n") else {
                return;
            };
            self.buf.drain(..end + 4);
            self.upgraded = true;
        }
        loop {
            let Some((fin, opcode, payload, used)) = self.frame() else {
                return;
            };
            self.buf.drain(..used);
            match opcode {
                // Control frames (close, ping, pong) may come between fragments.
                8..=10 => continue,
                0 if self.in_message => self.message.extend_from_slice(&payload),
                1 | 2 if !self.in_message => {
                    self.message = payload;
                    self.in_message = true;
                }
                _ => {
                    self.broken = true;
                    return;
                }
            }
            if fin {
                self.in_message = false;
                let message = std::mem::take(&mut self.message);
                self.v3(&message, found);
            }
        }
    }

    /// One complete WebSocket frame at the front of the buffer, unmasked:
    /// `(fin, opcode, payload, bytes used)`.
    fn frame(&self) -> Option<(bool, u8, Vec<u8>, usize)> {
        let b = &self.buf;
        if b.len() < 2 {
            return None;
        }
        let fin = b[0] & 0x80 != 0;
        let opcode = b[0] & 0x0f;
        let masked = b[1] & 0x80 != 0;
        let mut at = 2usize;
        let len = match b[1] & 0x7f {
            126 => {
                if b.len() < at + 2 {
                    return None;
                }
                let len = u16::from_be_bytes([b[at], b[at + 1]]) as usize;
                at += 2;
                len
            }
            127 => {
                if b.len() < at + 8 {
                    return None;
                }
                let mut raw = [0u8; 8];
                raw.copy_from_slice(&b[at..at + 8]);
                at += 8;
                usize::try_from(u64::from_be_bytes(raw)).ok()?
            }
            len => len as usize,
        };
        let mask = if masked {
            if b.len() < at + 4 {
                return None;
            }
            let mask = [b[at], b[at + 1], b[at + 2], b[at + 3]];
            at += 4;
            Some(mask)
        } else {
            None
        };
        if b.len() < at + len {
            return None;
        }
        let mut payload = b[at..at + len].to_vec();
        if let Some(mask) = mask {
            for (i, byte) in payload.iter_mut().enumerate() {
                *byte ^= mask[i % 4];
            }
        }
        Some((fin, opcode, payload, at + len))
    }

    /// One v3 frame (§6.2 layout). Every v3 frame, pre-auth ones included,
    /// has the 8-byte prefix and one JSON object header: anything else is a
    /// stream the tap cannot follow (`broken`), never a frame it skips.
    fn v3(&mut self, m: &[u8], found: &mut dyn FnMut(u64, String, Option<String>)) {
        if m.len() < 8 {
            self.broken = true;
            return;
        }
        let (kind, flags) = (m[0], m[1]);
        let header_len = u32::from_le_bytes([m[4], m[5], m[6], m[7]]) as usize;
        if m.len() < 8 + header_len {
            self.broken = true;
            return;
        }
        let Ok(header) = serde_json::from_slice::<Value>(&m[8..8 + header_len]) else {
            self.broken = true;
            return;
        };
        if !header.is_object() {
            self.broken = true;
            return;
        }
        let body = &m[8 + header_len..];
        let id = header["id"].as_u64().unwrap_or(0);
        match kind {
            KIND_REQUEST => {
                let op = header["op"].as_str().unwrap_or_default().to_owned();
                if flags & FLAG_MORE != 0 {
                    self.pending.insert(id, (op, body.to_vec()));
                } else {
                    let request = exchange_identity(&op, body, &header["header"]);
                    found(id, op, request);
                }
            }
            KIND_CHUNK => {
                let last = flags & FLAG_LAST != 0 || header["last"] == true;
                if let Some((_, so_far)) = self.pending.get_mut(&id) {
                    so_far.extend_from_slice(body);
                    if last && let Some((op, body)) = self.pending.remove(&id) {
                        let request = exchange_identity(&op, &body, &Value::Null);
                        found(id, op, request);
                    }
                }
            }
            _ => {}
        }
    }
}

/// The exchange identity a request carries: `request` of the JSON body (or,
/// for an `objects.exchange` sent in the header, of the header).
fn exchange_identity(op: &str, body: &[u8], header: &Value) -> Option<String> {
    if !is_exchange(op) {
        return None;
    }
    let value = if body.is_empty() {
        header.clone()
    } else {
        serde_json::from_slice::<Value>(body).ok()?
    };
    value["request"].as_str().map(str::to_owned)
}

/// The ops whose requests carry an exchange identity.
fn is_exchange(op: &str) -> bool {
    op == "meta.exchange" || op == "objects.exchange"
}
