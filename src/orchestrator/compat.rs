//! Owned by WP3.1.
//!
//! The legacy compatibility surface after the cutover to v3 networking
//! (REVAMP_PLAN §7.3, §14 WP3.1):
//!
//! * [`HubLegacyView`]: the legacy List/Inspect `sync` member and the Peers
//!   reply, projected by the StatusHub (`StatusHub::legacy_folder_sync`,
//!   `StatusHub::legacy_peers`) for the catalog's (outbound) peers. Before
//!   the cutover they came verbatim from the legacy lanes' `LinkStatus`; the
//!   keys are the same, `transport` is now `"tkfs-peer/3"`. The acceptor
//!   lists no peer (`[]`), as it always did: only dialed peers are in the
//!   catalog. The hub keeps the values 6298d0b reported (CCR-WP3.1-1 and
//!   CCR-WP3.1-2, applied in `hub.rs` by WP3.INT, which removed this
//!   module's adapters): a bulk lane never claims `caught_up`, and no lane
//!   claims it while the remote still has events to activate.
//! * [`worker_call`]: the `Folder` passthrough's loopback worker call, with
//!   the caller's request id (the worker's receipts make a retried mutation
//!   with the same id idempotent) and a hard deadline. It replaces the
//!   legacy lanes' `DeadlineLane`, deleted with them. It is a worker call on
//!   the worker's own loopback endpoint, never a peer call.
use super::hub::StatusHub;
use crate::{
    model::orchestrator::{CatalogApi, FolderRecord, LegacyNetworkView, PeerRecord},
    runtime::Discovery,
    ws,
};
use anyhow::Result;
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

/// The §7.3 legacy network view served from the hub (WP3.1 "Provides").
pub struct HubLegacyView {
    hub: Arc<StatusHub>,
    catalog: Arc<dyn CatalogApi>,
}

impl HubLegacyView {
    pub fn new(hub: Arc<StatusHub>, catalog: Arc<dyn CatalogApi>) -> Arc<Self> {
        Arc::new(Self { hub, catalog })
    }

    /// The peers this daemon dials, as the legacy replies listed them: every
    /// catalog peer record, in endpoint order (the catalog's order).
    fn outbound_peers(&self) -> Vec<PeerRecord> {
        self.catalog.snapshot().peers.clone()
    }
}

impl std::fmt::Debug for HubLegacyView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HubLegacyView").finish_non_exhaustive()
    }
}

impl LegacyNetworkView for HubLegacyView {
    fn folder_sync(&self, folder: &FolderRecord) -> Value {
        self.hub.legacy_folder_sync(folder, &self.outbound_peers())
    }

    fn peers(&self) -> Value {
        self.hub.legacy_peers(&self.outbound_peers())
    }
}

/// One JSON request to a worker's loopback endpoint (`info.address`), as
/// `{"request": request, "payload": payload}`, answered within `deadline`
/// (connect, write and read together). A fresh connection per call: the
/// `Folder` passthrough is a rare user action. A transport failure or the
/// deadline leaves the outcome uncertain, and the call is never replayed
/// (invariant 16): the caller may retry with the same request id, which the
/// worker's receipts make idempotent.
pub fn worker_call(
    info: &Discovery,
    request: &str,
    payload: Value,
    deadline: Duration,
) -> Result<Value> {
    let started = Instant::now();
    let mut socket = ws::connect(&info.address)?;
    // A zero timeout means "block forever" to the OS: keep at least 1 ms.
    let remaining = deadline
        .saturating_sub(started.elapsed())
        .max(Duration::from_millis(1));
    if let tungstenite::stream::MaybeTlsStream::Plain(tcp) = socket.get_mut() {
        tcp.set_read_timeout(Some(remaining))?;
        tcp.set_write_timeout(Some(remaining))?;
    }
    match ws::call(
        &mut socket,
        &json!({"request": request, "payload": payload}),
    ) {
        Ok(value) => Ok(value),
        Err(error) if error.downcast_ref::<ws::RemoteError>().is_some() => Err(error),
        Err(error) if timed_out(&error) => Err(error.context(format!(
            "REQUEST_TIMEOUT: no reply within {} ms",
            deadline.as_millis()
        ))),
        Err(error) => Err(error),
    }
}

/// Whether an error chain ends in a socket timeout (`WouldBlock` on Unix,
/// `TimedOut` on Windows).
fn timed_out(error: &anyhow::Error) -> bool {
    let timeout = |io: &std::io::Error| {
        matches!(
            io.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
        )
    };
    error.chain().any(|cause| {
        cause.downcast_ref::<std::io::Error>().is_some_and(timeout)
            || matches!(cause.downcast_ref::<tungstenite::Error>(),
                Some(tungstenite::Error::Io(io)) if timeout(io))
    })
}
