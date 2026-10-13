//! One mount worker: local write staging and loopback WebSocket control.
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

pub mod content;
pub mod control;
pub mod engine;
pub mod export;
pub mod notify;
pub mod published;
pub mod server;

pub use content::prepare_content;
pub use engine::{Engine, Handle, Session};
pub use notify::{deliver_notifications, namespace_notifications};
pub use server::start_rpc;

pub type Shared = Arc<Mutex<Engine>>;
pub const MAX_FRAME: usize = 64 * 1024 * 1024;
/// Format 6 separates durable metadata from verified object availability.
/// Never negotiate down or acknowledge events a peer cannot project.
pub use crate::core::PEER_FORMAT;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Discovery {
    pub format: u32,
    pub address: String,
    pub state: String,
    pub mount: Option<String>,
    pub repo: String,
    pub device: String,
}
pub fn rpc(info: &Discovery, request: &str, payload: Value) -> Result<Value> {
    let mut socket = crate::ws::connect(&info.address)?;
    crate::ws::call(&mut socket, &json!({"request":request,"payload":payload}))
}
/// One worker connection per peer lane. Never share a socket between bulk and
/// control work, and never replay a request after an uncertain transport error.
#[derive(Default)]
pub struct RpcLane {
    connection: Option<(String, crate::ws::Socket)>,
    last_used: Option<std::time::Instant>,
}
impl RpcLane {
    pub fn call(&mut self, info: &Discovery, request: &str, payload: Value) -> Result<Value> {
        if self
            .connection
            .as_ref()
            .is_none_or(|(address, _)| address != &info.address)
            || self
                .last_used
                .is_some_and(|at| at.elapsed() >= Duration::from_secs(10))
        {
            self.connection = None;
            self.connection = Some((info.address.clone(), crate::ws::connect(&info.address)?));
        }
        let result = crate::ws::call(
            &mut self.connection.as_mut().unwrap().1,
            &json!({"request":request,"payload":payload}),
        );
        self.last_used = Some(std::time::Instant::now());
        if result
            .as_ref()
            .is_err_and(|error| error.downcast_ref::<crate::ws::RemoteError>().is_none())
        {
            self.connection = None;
        }
        result
    }
}
/// Engine lock seam. Every engine lock is meant to go through here so the
/// worker can apply one poison policy; today it is exactly `lock().unwrap()`.
pub fn lock_engine(e: &Shared) -> MutexGuard<'_, Engine> {
    e.lock().unwrap()
}
