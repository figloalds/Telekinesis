//! A persisted daemon incarnation plus an in-process monotonic run number.
use crate::model::{errors::ErrorWire, lease::Generation, orchestrator::Ctx};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

pub struct Clock {
    incarnation: u64,
    next: AtomicU64,
}
impl Clock {
    pub fn new(incarnation: u64) -> Self {
        Self {
            incarnation,
            next: AtomicU64::new(0),
        }
    }
    pub fn next(&self) -> Result<Generation, ErrorWire> {
        // Contexts manually constructed by tests have no catalog. Their zero
        // incarnation has process-wide ordering, without touching host files.
        static EPHEMERAL: AtomicU64 = AtomicU64::new(0);
        let counter = if self.incarnation == 0 {
            &EPHEMERAL
        } else {
            &self.next
        };
        let old = counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map_err(|_| ErrorWire::coded("CATALOG_UNAVAILABLE", "lease generation exhausted"))?;
        Ok(Generation {
            incarnation: self.incarnation,
            run: old + 1,
        })
    }
}
pub fn next(ctx: &Arc<Ctx>) -> Result<Generation, ErrorWire> {
    super::ctx::extras(ctx)
        .lease_clock
        .get_or_init(|| Clock::new(0))
        .next()
}
