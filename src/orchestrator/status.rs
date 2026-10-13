use serde::Serialize;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

#[derive(Default, Clone, Serialize)]
pub(crate) struct LinkStatus {
    pub(crate) connected: bool,
    pub(crate) exchanges: u64,
    pub(crate) last_success_ms: Option<u64>,
    pub(crate) error: Option<String>,
    pub(crate) transport: Option<String>,
    pub(crate) parallel_bulk: bool,
    pub(crate) repositories: BTreeMap<String, RepositoryStatus>,
}
#[derive(Default, Clone, Serialize)]
pub(crate) struct RepositoryStatus {
    pub(crate) last_success_ms: Option<u64>,
    pub(crate) caught_up: bool,
    pub(crate) metadata_caught_up: bool,
    pub(crate) metadata_progress: Option<Value>,
    pub(crate) transfer: Option<Value>,
    pub(crate) object_inventory: Option<Value>,
    pub(crate) last_progress_ms: Option<u64>,
    pub(crate) total_sent_object_bytes: u64,
    pub(crate) total_received_object_bytes: u64,
    pub(crate) total_sent_wire_bytes: u64,
    pub(crate) total_received_wire_bytes: u64,
    pub(crate) error: Option<String>,
    pub(crate) consecutive_failures: u32,
    pub(crate) retry_at_ms: Option<u64>,
    #[serde(skip)]
    pub(crate) retry_at: Option<Instant>,
}
impl RepositoryStatus {
    pub(crate) fn failed(&mut self, error: &anyhow::Error) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        let delay = if format!("{error:#}").contains("WORKER_BUSY") {
            Duration::from_millis(50)
        } else {
            retry_delay(self.consecutive_failures)
        };
        self.caught_up = false;
        self.metadata_caught_up = false;
        self.error = Some(format!("{error:#}"));
        self.retry_at_ms = Some(crate::core::time() + delay.as_millis() as u64);
        self.retry_at = Some(Instant::now() + delay);
    }

    pub(crate) fn succeeded(&mut self, result: &Value) {
        self.last_success_ms = Some(crate::core::time());
        self.caught_up = result["caught_up"] == true;
        self.metadata_caught_up = result["metadata_caught_up"] == true;
        self.metadata_progress = result.get("metadata_progress").cloned();
        self.transfer = result.get("transfer").cloned();
        self.object_inventory = result.get("object_inventory").cloned();
        if result["metadata_advanced"] == true
            || result["transfer"]["sent_object_bytes"]
                .as_u64()
                .unwrap_or(0)
                > 0
            || result["transfer"]["received_object_bytes"]
                .as_u64()
                .unwrap_or(0)
                > 0
        {
            self.last_progress_ms = self.last_success_ms;
        }
        self.total_sent_wire_bytes = self
            .total_sent_wire_bytes
            .saturating_add(result["transfer"]["sent_wire_bytes"].as_u64().unwrap_or(0));
        self.total_received_wire_bytes = self.total_received_wire_bytes.saturating_add(
            result["transfer"]["received_wire_bytes"]
                .as_u64()
                .unwrap_or(0),
        );
        self.total_sent_object_bytes = self.total_sent_object_bytes.saturating_add(
            result["transfer"]["sent_object_bytes"]
                .as_u64()
                .unwrap_or(0),
        );
        self.total_received_object_bytes = self.total_received_object_bytes.saturating_add(
            result["transfer"]["received_object_bytes"]
                .as_u64()
                .unwrap_or(0),
        );
        self.error = None;
        self.consecutive_failures = 0;
        self.retry_at_ms = None;
        self.retry_at = None;
    }
}

pub(crate) fn retry_delay(failures: u32) -> Duration {
    Duration::from_secs((1u64 << failures.saturating_sub(1).min(5)).min(30))
}
