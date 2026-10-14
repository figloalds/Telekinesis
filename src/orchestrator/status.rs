use crate::model::errors::{ErrorClass, classify, code_in};
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
    /// Contention retries (`WORKER_BUSY`, `LOCAL_WORKER_UNAVAILABLE`, …) since
    /// the last successful exchange. They are never failures (§8, WP1.12).
    pub(crate) busy_retries: u32,
    pub(crate) retry_at_ms: Option<u64>,
    #[serde(skip)]
    pub(crate) retry_at: Option<Instant>,
}
impl RepositoryStatus {
    /// Records a failed exchange and schedules its retry. Returns `true` when
    /// the error is Contention: then only `busy_retries` and the retry time
    /// change (a 50–200 ms retry); `error`, `consecutive_failures`,
    /// `caught_up` and `metadata_caught_up` keep their values.
    pub(crate) fn failed(&mut self, error: &anyhow::Error) -> bool {
        if is_contention(error) {
            self.busy_retries = self.busy_retries.saturating_add(1);
            self.retry_after(contention_delay(self.busy_retries));
            return true;
        }
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.caught_up = false;
        self.metadata_caught_up = false;
        self.error = Some(format!("{error:#}"));
        self.retry_after(retry_delay(self.consecutive_failures));
        false
    }

    /// Forgets the retry schedule: the next attempt runs now and a later
    /// failure backs off from the first step again. The last error stays
    /// until an exchange succeeds.
    pub(crate) fn reset_backoff(&mut self) {
        self.consecutive_failures = 0;
        self.busy_retries = 0;
        self.retry_at_ms = None;
        self.retry_at = None;
    }

    fn retry_after(&mut self, delay: Duration) {
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
        self.busy_retries = 0;
        self.retry_at_ms = None;
        self.retry_at = None;
    }
}

pub(crate) fn retry_delay(failures: u32) -> Duration {
    Duration::from_secs((1u64 << failures.saturating_sub(1).min(5)).min(30))
}

/// Contention retry ladder of the legacy lanes: 50, 100, then 200 ms.
pub(crate) fn contention_delay(retries: u32) -> Duration {
    Duration::from_millis(50 << retries.saturating_sub(1).min(2))
}

/// Whether an exchange error is Contention for the legacy lanes: any code in
/// the error chain that §8 classifies as Contention (`WORKER_BUSY`, …), or
/// `LOCAL_WORKER_UNAVAILABLE` (the local worker is restarting; WP1.12 treats
/// it as Contention on the lanes although §8 lists it as Transient). The
/// whole chain is scanned because the lane wraps worker errors in its own
/// context (`LOCAL_SYNC_OFFER_FAILED: WORKER_BUSY`).
pub(crate) fn is_contention(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let text = cause.to_string();
        let mut rest = text.as_str();
        while let Some(code) = code_in(rest) {
            if code == "LOCAL_WORKER_UNAVAILABLE" || classify(code) == ErrorClass::Contention {
                return true;
            }
            // `code` borrows from `rest`: continue right after it.
            let end = code.as_ptr() as usize - rest.as_ptr() as usize + code.len();
            rest = &rest[end..];
        }
        false
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{Context, anyhow};
    use serde_json::json;

    fn caught_up() -> RepositoryStatus {
        let mut status = RepositoryStatus::default();
        status.succeeded(&json!({"caught_up":true,"metadata_caught_up":true}));
        status
    }

    #[test]
    fn worker_busy_is_contention_and_never_a_failure() {
        let mut status = caught_up();
        for (attempt, error) in [
            anyhow!("WORKER_BUSY"),
            anyhow!("WORKER_BUSY").context("LOCAL_SYNC_OFFER_FAILED"),
            anyhow::Error::new(crate::ws::RemoteError("WORKER_BUSY".into()))
                .context("REMOTE_SYNC_EXCHANGE_FAILED"),
        ]
        .into_iter()
        .enumerate()
        {
            assert!(status.failed(&error), "{error:#}");
            assert_eq!(status.consecutive_failures, 0);
            assert!(status.caught_up && status.metadata_caught_up);
            assert!(status.error.is_none());
            assert_eq!(status.busy_retries, attempt as u32 + 1);
            let wait = status.retry_at.unwrap() - Instant::now();
            assert!(wait <= Duration::from_millis(200), "{wait:?}");
            assert!(status.retry_at_ms.is_some());
        }
        status.succeeded(&json!({"caught_up":true}));
        assert_eq!(status.busy_retries, 0);
        assert!(status.retry_at.is_none() && status.retry_at_ms.is_none());
    }

    #[test]
    fn local_worker_unavailable_is_contention() {
        let mut status = caught_up();
        let error = anyhow!("WORKER_NOT_RUNNING").context("LOCAL_WORKER_UNAVAILABLE");
        assert!(status.failed(&error));
        assert_eq!(status.consecutive_failures, 0);
        assert!(status.caught_up);
        assert_eq!(status.busy_retries, 1);
    }

    #[test]
    fn contention_keeps_an_earlier_real_failure_visible() {
        let mut status = caught_up();
        assert!(!status.failed(&anyhow!("REQUEST_TIMEOUT").context("REMOTE_SYNC_EXCHANGE_FAILED")));
        assert_eq!(status.consecutive_failures, 1);
        assert!(!status.caught_up);
        assert!(status.failed(&anyhow!("WORKER_BUSY")));
        assert_eq!(status.consecutive_failures, 1);
        assert!(status.error.as_deref().unwrap().contains("REQUEST_TIMEOUT"));
    }

    #[test]
    fn other_errors_count_and_back_off() {
        let mut status = caught_up();
        for (failures, code) in ["CORRUPT_OBJECT", "SQLITE_BUSY", "REQUEST_TIMEOUT"]
            .into_iter()
            .enumerate()
        {
            let error = anyhow!("{code}: detail").context("LOCAL_SYNC_APPLY_FAILED");
            assert!(!status.failed(&error));
            assert_eq!(status.consecutive_failures, failures as u32 + 1);
            assert!(!status.caught_up && !status.metadata_caught_up);
            assert!(status.error.as_deref().unwrap().contains(code));
            let wait = status.retry_at.unwrap() - Instant::now();
            assert!(wait > retry_delay(failures as u32 + 1) - Duration::from_millis(100));
        }
        assert_eq!(status.busy_retries, 0);
    }

    #[test]
    fn contention_ladder_and_reset() {
        assert_eq!(contention_delay(1), Duration::from_millis(50));
        assert_eq!(contention_delay(2), Duration::from_millis(100));
        assert_eq!(contention_delay(3), Duration::from_millis(200));
        assert_eq!(contention_delay(40), Duration::from_millis(200));
        let mut status = caught_up();
        status.failed(&anyhow!("OBJECT_IO_ERROR"));
        status.failed(&anyhow!("OBJECT_IO_ERROR"));
        status.failed(&anyhow!("BUSY_VIEW"));
        status.reset_backoff();
        assert_eq!(status.consecutive_failures, 0);
        assert_eq!(status.busy_retries, 0);
        assert!(status.retry_at.is_none() && status.retry_at_ms.is_none());
        assert!(
            status.error.is_some(),
            "the last error stays until a success"
        );
    }

    #[test]
    fn contention_scan_reads_every_code_in_the_chain() {
        assert!(is_contention(&anyhow!(
            "REMOTE_SYNC_EXCHANGE_FAILED: LOCAL_SYNC_OFFER_FAILED: WORKER_BUSY"
        )));
        assert!(!is_contention(&anyhow!("SQLITE_BUSY: database is locked")));
        assert!(!is_contention(&anyhow!("device or resource busy")));
        assert!(!is_contention(
            &Err::<(), _>(anyhow!("WS_CLOSED"))
                .context("REMOTE_BULK_EXCHANGE_FAILED")
                .unwrap_err()
        ));
    }
}
