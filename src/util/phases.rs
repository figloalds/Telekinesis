//! Fixed-size phase measurements for Wave 6 qualification.
//!
//! Counters are per owner, never reset underneath running work. Snapshots are
//! approximate while operations run; sample deltas only at quiescent boundaries.
//! Latency buckets are upper bounds in microseconds, not exact percentiles.
use serde::Serialize;
use std::{
    array,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub enum Phase {
    Callback,
    EntityWait,
    SqlWriterWait,
    SqlExecution,
    FileSync,
    SqlCheckpoint,
    Hash,
    Copy,
    Read,
    Write,
    ManifestRead,
    ManifestWrite,
    SaveCheckpoint,
    DemandAdmission,
}

const PHASES: [&str; 14] = [
    "callback",
    "entity_wait",
    "sql_writer_wait",
    "sql_execution",
    "file_sync",
    "sql_checkpoint",
    "hash",
    "copy",
    "read",
    "write",
    "manifest_read",
    "manifest_write",
    "save_checkpoint",
    "demand_admission",
];
const BUCKETS: usize = 32;

struct Counter {
    operations: AtomicU64,
    bytes: AtomicU64,
    elapsed_us: AtomicU64,
    max_us: AtomicU64,
    histogram: [AtomicU64; BUCKETS],
}

impl Default for Counter {
    fn default() -> Self {
        Self {
            operations: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            elapsed_us: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
            histogram: array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

#[derive(Default)]
pub struct PhaseCounters {
    counters: [Counter; PHASES.len()],
}

#[derive(Clone, Debug, Serialize)]
pub struct PhaseSample {
    pub phase: &'static str,
    pub operations: u64,
    pub bytes: u64,
    pub elapsed_us: u64,
    pub max_us: u64,
    /// Buckets 0..30: <= 2^index us. Bucket 31: overflow (>2^30 us).
    pub latency_buckets: [u64; BUCKETS],
}

impl PhaseCounters {
    pub fn begin(&self, phase: Phase, bytes: u64) -> PhaseTimer<'_> {
        PhaseTimer {
            owner: self,
            phase,
            bytes,
            started: Instant::now(),
        }
    }

    pub fn record(&self, phase: Phase, bytes: u64, elapsed_us: u64) {
        let counter = &self.counters[phase as usize];
        counter.operations.fetch_add(1, Ordering::Relaxed);
        counter.bytes.fetch_add(bytes, Ordering::Relaxed);
        counter.elapsed_us.fetch_add(elapsed_us, Ordering::Relaxed);
        counter.max_us.fetch_max(elapsed_us, Ordering::Relaxed);
        let bucket = if elapsed_us <= 1 {
            0
        } else {
            (64 - (elapsed_us - 1).leading_zeros() as usize).min(BUCKETS - 1)
        };
        counter.histogram[bucket].fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> Vec<PhaseSample> {
        self.counters
            .iter()
            .zip(PHASES)
            .map(|(counter, phase)| PhaseSample {
                phase,
                operations: counter.operations.load(Ordering::Relaxed),
                bytes: counter.bytes.load(Ordering::Relaxed),
                elapsed_us: counter.elapsed_us.load(Ordering::Relaxed),
                max_us: counter.max_us.load(Ordering::Relaxed),
                latency_buckets: array::from_fn(|i| counter.histogram[i].load(Ordering::Relaxed)),
            })
            .collect()
    }
}

/// Includes failed work when dropped; errors must be counted separately by the owner.
pub struct PhaseTimer<'a> {
    owner: &'a PhaseCounters,
    phase: Phase,
    bytes: u64,
    started: Instant,
}

impl Drop for PhaseTimer<'_> {
    fn drop(&mut self) {
        self.owner.record(
            self.phase,
            self.bytes,
            self.started.elapsed().as_micros().min(u64::MAX as u128) as u64,
        );
    }
}
