//! `HungServer`: accepts the WebSocket upgrade, reads every message, never replies.
//! `redirect_runtime_json` points a worker discovery (`STATE/runtime.json`) at it,
//! which makes the baseline orchestrator's calls to that worker hang.
use anyhow::{Context, Result};
use serde_json::Value;
use std::{
    fs,
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};
use tkfs::ws;

pub struct HungServer {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    connections: Arc<AtomicU64>,
    received: Arc<AtomicU64>,
}
impl HungServer {
    pub fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let connections = Arc::new(AtomicU64::new(0));
        let received = Arc::new(AtomicU64::new(0));
        let (s, c, r) = (stop.clone(), connections.clone(), received.clone());
        thread::spawn(move || {
            for stream in listener.incoming() {
                if s.load(Ordering::Relaxed) {
                    break;
                }
                let Ok(stream) = stream else { continue };
                c.fetch_add(1, Ordering::Relaxed);
                let (s, r) = (s.clone(), r.clone());
                thread::spawn(move || hang(stream, s, r));
            }
        });
        Self {
            addr,
            stop,
            connections,
            received,
        }
    }
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
    pub fn endpoint(&self) -> String {
        format!("ws://{}", self.addr)
    }
    /// Accepted TCP connections.
    pub fn connections(&self) -> u64 {
        self.connections.load(Ordering::Relaxed)
    }
    /// Complete messages read (never answered).
    pub fn received(&self) -> u64 {
        self.received.load(Ordering::Relaxed)
    }
}
impl Drop for HungServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect_timeout(&self.addr, Duration::from_millis(200));
    }
}

fn hang(stream: TcpStream, stop: Arc<AtomicBool>, received: Arc<AtomicU64>) {
    let Ok(mut socket) = ws::accept(stream) else {
        return;
    };
    if let tungstenite::stream::MaybeTlsStream::Plain(tcp) = socket.get_mut() {
        let _ = tcp.set_read_timeout(Some(Duration::from_millis(200)));
    }
    while !stop.load(Ordering::Relaxed) {
        match ws::receive::<Value>(&mut socket) {
            Ok(_) => {
                received.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                let timeout = error
                    .chain()
                    .filter_map(|e| e.downcast_ref::<tungstenite::Error>())
                    .any(|e| matches!(e, tungstenite::Error::Io(io)
                        if matches!(io.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)));
                if !timeout {
                    return;
                }
            }
        }
    }
}

/// The original `runtime.json` bytes, to restore with `restore()`.
pub struct RuntimeRedirect {
    pub path: PathBuf,
    pub original: Vec<u8>,
}
impl RuntimeRedirect {
    pub fn restore(self) -> Result<()> {
        fs::write(&self.path, &self.original)?;
        Ok(())
    }
}

/// Rewrite `STATE/runtime.json` so its `address` is `addr` (`ws://host:port` or
/// `host:port`). Every other field is kept.
pub fn redirect_runtime_json(state: &Path, addr: &str) -> Result<RuntimeRedirect> {
    let path = state.join("runtime.json");
    let original = fs::read(&path).context("RUNTIME_JSON_MISSING")?;
    let mut discovery: Value = serde_json::from_slice(&original)?;
    let address = if addr.starts_with("ws://") {
        addr.to_owned()
    } else {
        format!("ws://{addr}")
    };
    discovery["address"] = Value::String(address);
    fs::write(&path, serde_json::to_vec_pretty(&discovery)?)?;
    Ok(RuntimeRedirect { path, original })
}
