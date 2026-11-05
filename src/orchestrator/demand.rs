//! Worker wake-ups and object demand.
//!
//! [`Seq`] is the counter a parked `watch` waits on: the heartbeat bumps it
//! every second and [`Demand`] bumps it whenever a read needs objects the
//! worker does not have. The orchestrator answers demand by writing the
//! objects into the worker's packs, which calls [`Demand::arrived`].
use crate::{
    content::{Kind, ObjectFetcher, PackStore},
    model::content::Digest,
    util::lock::lock,
};
use anyhow::Result;
use std::{
    collections::BTreeMap,
    sync::{Arc, Condvar, Mutex, PoisonError},
    time::{Duration, Instant},
};

/// A monotonically increasing wake-up counter. Whoever compares [`Seq::current`]
/// sees only bumps. A [`Seq::nudge`] rouses a [`Seq::wait_moved`] sleeper and
/// leaves the counter as it is.
pub struct Seq {
    state: Mutex<SeqMark>,
    changed: Condvar,
}

/// Where a [`Seq`] stood, for [`Seq::wait_moved`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SeqMark {
    /// What [`Seq::current`] returned.
    pub value: u64,
    nudges: u64,
}

impl Seq {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(SeqMark::default()),
            changed: Condvar::new(),
        })
    }

    /// Advances the counter, wakes every waiter and returns the new value.
    pub fn bump(&self) -> u64 {
        let mut state = lock(&self.state);
        state.value += 1;
        self.changed.notify_all();
        state.value
    }

    /// Rouses a [`Self::wait_moved`] sleeper. The counter does not move, so
    /// nothing that compares it takes this for a bump.
    pub fn nudge(&self) {
        lock(&self.state).nudges += 1;
        self.changed.notify_all();
    }

    pub fn current(&self) -> u64 {
        lock(&self.state).value
    }

    /// Where it stands now.
    pub fn mark(&self) -> SeqMark {
        *lock(&self.state)
    }

    /// Waits at most `timeout` for the counter to pass `after`; returns its
    /// value then (which equals `after` on timeout when nothing moved).
    pub fn wait_past(&self, after: u64, timeout: Duration) -> u64 {
        self.wait_until(timeout, |state| state.value > after).value
    }

    /// Waits at most `timeout` for a bump or a nudge since `since`.
    pub fn wait_moved(&self, since: SeqMark, timeout: Duration) {
        self.wait_until(timeout, |state| *state != since);
    }

    fn wait_until(&self, timeout: Duration, done: impl Fn(&SeqMark) -> bool) -> SeqMark {
        let end = Instant::now().checked_add(timeout);
        let mut state = lock(&self.state);
        while !done(&state) {
            let left = match end {
                Some(end) => end.saturating_duration_since(Instant::now()),
                None => Duration::from_secs(3600),
            };
            if left.is_zero() {
                break;
            }
            state = self
                .changed
                .wait_timeout(state, left)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        *state
    }
}

/// Objects some reader is waiting for, keyed by digest; the count is the
/// number of waiting fetches.
pub struct Demand {
    wanted: Mutex<BTreeMap<Digest, (Kind, usize)>>,
    cv: Condvar,
    seq: Arc<Seq>,
    packs: PackStore,
}

impl Demand {
    /// How long a read waits for the orchestrator to supply its objects.
    pub const FETCH_WAIT: Duration = Duration::from_secs(10);

    pub fn new(seq: Arc<Seq>, packs: PackStore) -> Arc<Self> {
        Arc::new(Self {
            wanted: Mutex::new(BTreeMap::new()),
            cv: Condvar::new(),
            seq,
            packs,
        })
    }

    pub fn pending(&self) -> Vec<(Digest, Kind)> {
        lock(&self.wanted)
            .iter()
            .map(|(digest, (kind, _))| (*digest, *kind))
            .collect()
    }

    /// Objects were stored: stop advertising them and wake their readers.
    pub fn arrived(&self, digests: &[Digest]) {
        let mut wanted = lock(&self.wanted);
        for digest in digests {
            wanted.remove(digest);
        }
        self.cv.notify_all();
    }

    fn fetch_within(&self, objects: &[(Digest, Kind)], wait: Duration) -> Result<()> {
        let missing = |packs: &PackStore| -> Result<Vec<(Digest, Kind)>> {
            let mut missing = Vec::new();
            for (digest, kind) in objects {
                if !packs.contains(*digest)? {
                    missing.push((*digest, *kind));
                }
            }
            Ok(missing)
        };
        let registered = missing(&self.packs)?;
        if registered.is_empty() {
            return Ok(());
        }
        let mut wanted = lock(&self.wanted);
        for (digest, kind) in &registered {
            wanted.entry(*digest).or_insert((*kind, 0)).1 += 1;
        }
        drop(wanted);
        self.seq.bump();
        let end = Instant::now() + wait;
        let mut wanted = lock(&self.wanted);
        let result = loop {
            drop(wanted);
            let absent = match missing(&self.packs) {
                Ok(absent) => absent,
                Err(error) => {
                    wanted = lock(&self.wanted);
                    break Err(error);
                }
            };
            wanted = lock(&self.wanted);
            if absent.is_empty() {
                break Ok(());
            }
            let left = end.saturating_duration_since(Instant::now());
            if left.is_zero() {
                let names: Vec<String> = absent.iter().map(|(d, _)| d.to_string()).collect();
                break Err(anyhow::anyhow!(
                    "OBJECT_MISSING: {} object(s) not supplied within {:?}: {}",
                    absent.len(),
                    wait,
                    names.join(", ")
                ));
            }
            // `arrived` notifies; the timeout bounds a write that bypassed it.
            wanted = self
                .cv
                .wait_timeout(wanted, left.min(Duration::from_millis(250)))
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        };
        for (digest, _) in &registered {
            if let Some(entry) = wanted.get_mut(digest) {
                entry.1 = entry.1.saturating_sub(1);
                if entry.1 == 0 {
                    wanted.remove(digest);
                }
            }
        }
        result
    }
}

impl ObjectFetcher for Demand {
    /// Registers the missing objects, wakes `watch` and waits until every one
    /// is in the packs, or fails `OBJECT_MISSING` after [`Demand::FETCH_WAIT`].
    fn fetch(&self, objects: &[(Digest, Kind)]) -> Result<()> {
        self.fetch_within(objects, Self::FETCH_WAIT)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seq_wait_past_returns_at_once_when_ahead_and_times_out_otherwise() {
        let seq = Seq::new();
        assert_eq!(seq.bump(), 1);
        assert_eq!(seq.wait_past(0, Duration::from_secs(5)), 1);
        let started = Instant::now();
        assert_eq!(seq.wait_past(1, Duration::from_millis(100)), 1);
        assert!(started.elapsed() >= Duration::from_millis(90));
        let bumper = {
            let seq = seq.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(50));
                seq.bump();
            })
        };
        assert_eq!(seq.wait_past(1, Duration::from_secs(5)), 2);
        bumper.join().unwrap();
    }

    #[test]
    fn a_nudge_rouses_a_moved_wait_and_leaves_the_counter_alone() {
        let seq = Seq::new();
        seq.bump();
        let since = seq.mark();
        // Nothing since: it times out.
        let started = Instant::now();
        seq.wait_moved(since, Duration::from_millis(50));
        assert!(started.elapsed() >= Duration::from_millis(40));
        // A nudge from before the wait is not lost, whichever came first.
        seq.nudge();
        assert_eq!(seq.current(), since.value);
        let started = Instant::now();
        seq.wait_moved(since, Duration::from_secs(30));
        assert!(started.elapsed() < Duration::from_secs(10));
        // One during the wait rouses it.
        let since = seq.mark();
        let nudger = {
            let seq = seq.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(50));
                seq.nudge();
            })
        };
        let started = Instant::now();
        seq.wait_moved(since, Duration::from_secs(30));
        assert!(started.elapsed() < Duration::from_secs(10));
        nudger.join().unwrap();
        // A bump does too.
        let since = seq.mark();
        seq.bump();
        let started = Instant::now();
        seq.wait_moved(since, Duration::from_secs(30));
        assert!(started.elapsed() < Duration::from_secs(10));
        // `wait_past` does not take a nudge for what it waits for.
        let at = seq.current();
        seq.nudge();
        let started = Instant::now();
        assert_eq!(seq.wait_past(at, Duration::from_millis(50)), at);
        assert!(started.elapsed() >= Duration::from_millis(40));
    }

    #[test]
    fn demand_registers_bumps_and_wakes_on_arrival() -> Result<()> {
        let root = tempfile::tempdir()?;
        let packs = PackStore::open(root.path())?;
        let seq = Seq::new();
        let demand = Demand::new(seq.clone(), packs.clone());
        let digest = Digest::bytes(b"wanted");
        let waiter = {
            let demand = demand.clone();
            std::thread::spawn(move || demand.fetch(&[(digest, Kind::Chunk)]))
        };
        assert!(seq.wait_past(0, Duration::from_secs(5)) >= 1);
        assert_eq!(demand.pending(), vec![(digest, Kind::Chunk)]);
        assert_eq!(packs.put(Kind::Chunk, b"wanted")?, digest);
        packs.flush()?;
        demand.arrived(&[digest]);
        let end = Instant::now() + Duration::from_secs(5);
        while !waiter.is_finished() {
            assert!(Instant::now() < end, "fetch did not return after arrival");
            std::thread::sleep(Duration::from_millis(10));
        }
        waiter.join().unwrap()?;
        assert!(demand.pending().is_empty());
        Ok(())
    }

    #[test]
    fn demand_times_out_with_object_missing_and_forgets_the_digest() -> Result<()> {
        let root = tempfile::tempdir()?;
        let packs = PackStore::open(root.path())?;
        let demand = Demand::new(Seq::new(), packs);
        let digest = Digest::bytes(b"never");
        let error = demand
            .fetch_within(&[(digest, Kind::Chunk)], Duration::from_millis(50))
            .unwrap_err();
        assert!(error.to_string().starts_with("OBJECT_MISSING"), "{error}");
        assert!(demand.pending().is_empty());
        Ok(())
    }
}
