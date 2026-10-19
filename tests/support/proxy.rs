//! `FaultProxy`: a TCP relay in front of a WebSocket endpoint that injects link
//! faults. Each direction of each connection has a reader thread and a writer
//! thread joined by a bounded queue, so latency is pipelined (not stop-and-wait)
//! and a stalled writer back-pressures the sender through TCP.
//!
//! Semantics (all proxy-wide, effective immediately for live connections):
//! - `set_latency(d)`: one-way delay added to every chunk in each direction
//!   (RTT grows by `2·d`).
//! - `set_bandwidth(bps)`: per-direction cap in bytes/second (0 = unlimited).
//! - `stall_for(d)`: no bytes are forwarded for `d` (reads still fill the queue).
//! - `stall_after(bytes, d)`: once `bytes` more bytes have been relayed (both
//!   directions), stall for `d` (one shot).
//! - `reset_all()`: shut down every live connection (both sockets).
//! - `reset_after(bytes)`: once `bytes` more bytes have been relayed, `reset_all` (one shot).
//! - `blackhole(true)`: keep connections open but silently drop all bytes.
//! - `refuse(true)`: close new connections immediately after accept.
//! - counters: `bytes_up` (client → target), `bytes_down`, `connections`
//!   (accepted and relayed), `active`, `refused`, `resets`.
use anyhow::{Context, Result};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::sync_channel,
    },
    thread,
    time::{Duration, Instant},
};

const CHUNK: usize = 16 * 1024;
const QUEUE: usize = 64;

struct Shared {
    target: SocketAddr,
    latency_us: AtomicU64,
    bandwidth: AtomicU64,
    stall_until: Mutex<Option<Instant>>,
    stall_after: Mutex<Option<(u64, Duration)>>,
    reset_after: Mutex<Option<u64>>,
    blackhole: AtomicBool,
    refuse: AtomicBool,
    closing: AtomicBool,
    bytes_up: AtomicU64,
    bytes_down: AtomicU64,
    connections: AtomicU64,
    refused: AtomicU64,
    resets: AtomicU64,
    next_id: AtomicU64,
    live: Mutex<BTreeMap<u64, (TcpStream, TcpStream)>>,
    pace: [Mutex<Instant>; 2],
}
impl Shared {
    fn total(&self) -> u64 {
        self.bytes_up.load(Ordering::Relaxed) + self.bytes_down.load(Ordering::Relaxed)
    }
    fn reset_all(&self) {
        let live = std::mem::take(&mut *self.live.lock().unwrap());
        // Count first: a peer may observe the shutdown before this thread resumes.
        self.resets.fetch_add(live.len() as u64, Ordering::Relaxed);
        for (_, (a, b)) in live {
            let _ = a.shutdown(Shutdown::Both);
            let _ = b.shutdown(Shutdown::Both);
        }
    }
    fn after_relay(&self) {
        let total = self.total();
        let stall = {
            let mut slot = self.stall_after.lock().unwrap();
            match *slot {
                Some((at, d)) if total >= at => {
                    *slot = None;
                    Some(d)
                }
                _ => None,
            }
        };
        if let Some(d) = stall {
            *self.stall_until.lock().unwrap() = Some(Instant::now() + d);
        }
        let reset = {
            let mut slot = self.reset_after.lock().unwrap();
            match *slot {
                Some(at) if total >= at => {
                    *slot = None;
                    true
                }
                _ => false,
            }
        };
        if reset {
            self.reset_all();
        }
    }
    fn wait_unstalled(&self) {
        loop {
            let until = *self.stall_until.lock().unwrap();
            match until {
                Some(t) if Instant::now() < t && !self.closing.load(Ordering::Relaxed) => {
                    thread::sleep((t - Instant::now()).min(Duration::from_millis(5)));
                }
                _ => return,
            }
        }
    }
    /// Token-bucket style pacing on a per-direction virtual clock.
    fn pace(&self, direction: usize, len: usize) {
        let rate = self.bandwidth.load(Ordering::Relaxed);
        if rate == 0 {
            return;
        }
        let start = {
            let mut next = self.pace[direction].lock().unwrap();
            let now = Instant::now();
            let start = (*next).max(now);
            *next = start + Duration::from_secs_f64(len as f64 / rate as f64);
            start
        };
        let now = Instant::now();
        if start > now {
            thread::sleep(start - now);
        }
    }
}

pub struct FaultProxy {
    shared: Arc<Shared>,
    addr: SocketAddr,
}
impl FaultProxy {
    /// Start relaying `127.0.0.1:0` → `target` (`ws://host:port`, `host:port`).
    pub fn start(target: &str) -> Self {
        Self::try_start(target).unwrap()
    }
    pub fn try_start(target: &str) -> Result<Self> {
        let hostport = target
            .trim_start_matches("ws://")
            .trim_start_matches("wss://")
            .split('/')
            .next()
            .unwrap_or_default();
        let target = hostport
            .to_socket_addrs()?
            .next()
            .context("FAULT_PROXY_TARGET")?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let shared = Arc::new(Shared {
            target,
            latency_us: AtomicU64::new(0),
            bandwidth: AtomicU64::new(0),
            stall_until: Mutex::new(None),
            stall_after: Mutex::new(None),
            reset_after: Mutex::new(None),
            blackhole: AtomicBool::new(false),
            refuse: AtomicBool::new(false),
            closing: AtomicBool::new(false),
            bytes_up: AtomicU64::new(0),
            bytes_down: AtomicU64::new(0),
            connections: AtomicU64::new(0),
            refused: AtomicU64::new(0),
            resets: AtomicU64::new(0),
            next_id: AtomicU64::new(1),
            live: Mutex::new(BTreeMap::new()),
            pace: [Mutex::new(Instant::now()), Mutex::new(Instant::now())],
        });
        let accept = shared.clone();
        thread::spawn(move || {
            for stream in listener.incoming() {
                if accept.closing.load(Ordering::Relaxed) {
                    break;
                }
                let Ok(client) = stream else { continue };
                if accept.refuse.load(Ordering::Relaxed) {
                    accept.refused.fetch_add(1, Ordering::Relaxed);
                    let _ = client.shutdown(Shutdown::Both);
                    continue;
                }
                let shared = accept.clone();
                thread::spawn(move || {
                    let _ = relay(shared, client);
                });
            }
        });
        Ok(Self { shared, addr })
    }
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
    /// `ws://127.0.0.1:PORT` — pair or connect to this instead of the target.
    pub fn endpoint(&self) -> String {
        format!("ws://{}", self.addr)
    }
    pub fn set_latency(&self, one_way: Duration) {
        self.shared
            .latency_us
            .store(one_way.as_micros() as u64, Ordering::Relaxed);
    }
    /// Per-direction cap in bytes per second; 0 removes the cap.
    pub fn set_bandwidth(&self, bytes_per_second: u64) {
        let now = Instant::now();
        for pace in &self.shared.pace {
            *pace.lock().unwrap() = now;
        }
        self.shared
            .bandwidth
            .store(bytes_per_second, Ordering::Relaxed);
    }
    pub fn stall_for(&self, d: Duration) {
        *self.shared.stall_until.lock().unwrap() = Some(Instant::now() + d);
    }
    pub fn stall_after(&self, bytes: u64, d: Duration) {
        *self.shared.stall_after.lock().unwrap() = Some((self.shared.total() + bytes, d));
    }
    pub fn reset_all(&self) {
        self.shared.reset_all();
    }
    pub fn reset_after(&self, bytes: u64) {
        *self.shared.reset_after.lock().unwrap() = Some(self.shared.total() + bytes);
    }
    pub fn blackhole(&self, on: bool) {
        self.shared.blackhole.store(on, Ordering::Relaxed);
    }
    pub fn refuse(&self, on: bool) {
        self.shared.refuse.store(on, Ordering::Relaxed);
    }
    /// Remove every fault (latency, bandwidth, stalls, blackhole, refuse); counters stay.
    pub fn clear(&self) {
        self.set_latency(Duration::ZERO);
        self.set_bandwidth(0);
        *self.shared.stall_until.lock().unwrap() = None;
        *self.shared.stall_after.lock().unwrap() = None;
        *self.shared.reset_after.lock().unwrap() = None;
        self.blackhole(false);
        self.refuse(false);
    }
    pub fn bytes_up(&self) -> u64 {
        self.shared.bytes_up.load(Ordering::Relaxed)
    }
    pub fn bytes_down(&self) -> u64 {
        self.shared.bytes_down.load(Ordering::Relaxed)
    }
    pub fn connections(&self) -> u64 {
        self.shared.connections.load(Ordering::Relaxed)
    }
    pub fn active(&self) -> usize {
        self.shared.live.lock().unwrap().len()
    }
    pub fn refused(&self) -> u64 {
        self.shared.refused.load(Ordering::Relaxed)
    }
    pub fn resets(&self) -> u64 {
        self.shared.resets.load(Ordering::Relaxed)
    }
}
impl Drop for FaultProxy {
    fn drop(&mut self) {
        self.shared.closing.store(true, Ordering::Relaxed);
        self.shared.reset_all();
        // Wake the blocking accept loop so it observes `closing`.
        let _ = TcpStream::connect_timeout(&self.addr, Duration::from_millis(200));
    }
}

fn relay(shared: Arc<Shared>, client: TcpStream) -> Result<()> {
    let upstream = TcpStream::connect_timeout(&shared.target, Duration::from_secs(3))?;
    client.set_nodelay(true)?;
    upstream.set_nodelay(true)?;
    let id = shared.next_id.fetch_add(1, Ordering::Relaxed);
    shared
        .live
        .lock()
        .unwrap()
        .insert(id, (client.try_clone()?, upstream.try_clone()?));
    shared.connections.fetch_add(1, Ordering::Relaxed);
    let up = pump(
        shared.clone(),
        client.try_clone()?,
        upstream.try_clone()?,
        0,
    );
    let down = pump(
        shared.clone(),
        upstream.try_clone()?,
        client.try_clone()?,
        1,
    );
    let _ = up.join();
    let _ = down.join();
    shared.live.lock().unwrap().remove(&id);
    let _ = client.shutdown(Shutdown::Both);
    let _ = upstream.shutdown(Shutdown::Both);
    Ok(())
}

/// One direction: a reader thread feeding a bounded queue and this writer.
fn pump(
    shared: Arc<Shared>,
    mut from: TcpStream,
    mut to: TcpStream,
    direction: usize,
) -> thread::JoinHandle<()> {
    let (tx, rx) = sync_channel::<(Instant, Vec<u8>)>(QUEUE);
    thread::spawn(move || {
        let mut buffer = vec![0; CHUNK];
        loop {
            match from.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if tx.send((Instant::now(), buffer[..n].to_vec())).is_err() {
                        break;
                    }
                }
            }
        }
    });
    thread::spawn(move || {
        for (arrived, bytes) in rx {
            let latency = Duration::from_micros(shared.latency_us.load(Ordering::Relaxed));
            let due = arrived + latency;
            let now = Instant::now();
            if due > now {
                thread::sleep(due - now);
            }
            shared.wait_unstalled();
            if shared.blackhole.load(Ordering::Relaxed) {
                continue;
            }
            shared.pace(direction, bytes.len());
            if to.write_all(&bytes).is_err() {
                break;
            }
            let counter = if direction == 0 {
                &shared.bytes_up
            } else {
                &shared.bytes_down
            };
            counter.fetch_add(bytes.len() as u64, Ordering::Relaxed);
            shared.after_relay();
        }
        // The source closed (or this side failed): propagate half-close.
        let _ = to.shutdown(Shutdown::Write);
    })
}
