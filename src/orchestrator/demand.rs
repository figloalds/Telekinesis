//! Worker wake-ups and object demand.
//!
//! [`Seq`] is the counter a parked `watch` waits on: the heartbeat bumps it
//! every second and [`Demand`] bumps it whenever a read needs objects the
//! worker does not have. The orchestrator answers demand by writing the
//! objects into the worker's packs, which calls [`Demand::arrived`].
//!
//! A sequential reader also hints what it will want next. A read that finds its
//! chunk already here costs no wake-up, and one wake-up then brings a window of
//! chunks and not only what one read touched. No one waits for a hint, and it
//! lasts [`Demand::HINT_TTL`], so a viewer left open stops asking.
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

type Objects = Vec<(Digest, Kind)>;

/// A chunk a reader is expected to want soon.
struct Hint {
    kind: Kind,
    /// How many times it was hinted and not taken back: once per open handle.
    holders: usize,
    expires: Instant,
    /// Its place among the hints: a reader's nearest chunks are hinted first.
    order: u64,
}

#[derive(Default)]
struct Wanted {
    /// What readers are waiting for, with the number of waiting fetches.
    reads: BTreeMap<Digest, (Kind, usize)>,
    hints: BTreeMap<Digest, Hint>,
    /// How many hints came so far, which gives each its [`Hint::order`].
    hinted: u64,
}

/// Objects some reader is waiting for, and those it hinted, keyed by digest.
pub struct Demand {
    wanted: Mutex<Wanted>,
    cv: Condvar,
    seq: Arc<Seq>,
    packs: PackStore,
}

impl Demand {
    /// How long a read waits for the orchestrator to supply its objects.
    pub const FETCH_WAIT: Duration = Duration::from_secs(10);
    /// How long a hint lasts unless it is hinted again.
    pub const HINT_TTL: Duration = Duration::from_secs(15);
    /// The most hints listed at once.
    pub const HINT_PENDING: usize = 256;

    pub fn new(seq: Arc<Seq>, packs: PackStore) -> Arc<Self> {
        Arc::new(Self {
            wanted: Mutex::new(Wanted::default()),
            cv: Condvar::new(),
            seq,
            packs,
        })
    }

    /// What readers are waiting for, then what they hinted.
    pub fn pending(&self) -> Objects {
        let (mut reads, hints) = self.listed();
        reads.extend(hints);
        reads
    }

    /// What readers are waiting for, and the unexpired hints in the order they
    /// came, at most [`Self::HINT_PENDING`], without what a reader waits for.
    pub fn listed(&self) -> (Objects, Objects) {
        self.listed_at(Instant::now())
    }

    fn listed_at(&self, now: Instant) -> (Objects, Objects) {
        let mut wanted = lock(&self.wanted);
        wanted.hints.retain(|_, hint| hint.expires > now);
        let reads = wanted
            .reads
            .iter()
            .map(|(digest, (kind, _))| (*digest, *kind))
            .collect();
        let mut hints: Vec<_> = wanted
            .hints
            .iter()
            .filter(|(digest, _)| !wanted.reads.contains_key(*digest))
            .collect();
        hints.sort_by_key(|(_, hint)| hint.order);
        let hints = hints
            .into_iter()
            .take(Self::HINT_PENDING)
            .map(|(digest, hint)| (*digest, hint.kind))
            .collect();
        (reads, hints)
    }

    /// Objects were stored: stop advertising them and wake their readers.
    pub fn arrived(&self, digests: &[Digest]) {
        let mut wanted = lock(&self.wanted);
        for digest in digests {
            wanted.reads.remove(digest);
            wanted.hints.remove(digest);
        }
        self.cv.notify_all();
    }

    fn hint_at(&self, objects: &[(Digest, Kind)], now: Instant) {
        if objects.is_empty() {
            return;
        }
        let mut guard = lock(&self.wanted);
        let wanted = &mut *guard;
        // An expired hint is a new one: its holders are gone.
        wanted.hints.retain(|_, hint| hint.expires > now);
        let expires = now + Self::HINT_TTL;
        for (digest, kind) in objects {
            wanted.hinted += 1;
            let hint = wanted.hints.entry(*digest).or_insert(Hint {
                kind: *kind,
                holders: 0,
                expires,
                order: wanted.hinted,
            });
            hint.holders += 1;
            hint.expires = expires;
        }
        drop(guard);
        self.seq.bump();
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
            wanted.reads.entry(*digest).or_insert((*kind, 0)).1 += 1;
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
            if let Some(entry) = wanted.reads.get_mut(digest) {
                entry.1 = entry.1.saturating_sub(1);
                if entry.1 == 0 {
                    wanted.reads.remove(digest);
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

    /// Lists the objects after what readers wait for, for [`Demand::HINT_TTL`]
    /// from now, and wakes `watch`. Hinting one again refreshes it, and a handle
    /// holds it until [`Self::unhint`], so it says each object once at a time.
    fn hint(&self, objects: &[(Digest, Kind)]) {
        self.hint_at(objects, Instant::now());
    }

    /// Takes back one hint of each object. One nobody holds any more is no
    /// longer listed; one that expired or arrived is already gone.
    fn unhint(&self, objects: &[(Digest, Kind)]) {
        let mut wanted = lock(&self.wanted);
        for (digest, _) in objects {
            if let Some(hint) = wanted.hints.get_mut(digest) {
                hint.holders = hint.holders.saturating_sub(1);
                if hint.holders == 0 {
                    wanted.hints.remove(digest);
                }
            }
        }
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

    fn chunks(name: &str, count: usize) -> Vec<(Digest, Kind)> {
        (0..count)
            .map(|index| {
                (
                    Digest::bytes(format!("{name}{index}").as_bytes()),
                    Kind::Chunk,
                )
            })
            .collect()
    }

    fn hinted_demand() -> Result<(tempfile::TempDir, Arc<Seq>, Arc<Demand>)> {
        let root = tempfile::tempdir()?;
        let seq = Seq::new();
        let demand = Demand::new(seq.clone(), PackStore::open(root.path())?);
        Ok((root, seq, demand))
    }

    #[test]
    fn hints_come_after_waiting_reads_in_pending() -> Result<()> {
        let (_root, seq, demand) = hinted_demand()?;
        let hints = chunks("hint", Demand::HINT_PENDING + 8);
        demand.hint(&hints);
        assert_eq!(seq.current(), 1, "a hint wakes `watch`");
        // In the order hinted, which is the reader's and not the digests'.
        assert_eq!(demand.pending(), hints[..Demand::HINT_PENDING]);
        // A reader starts waiting for a hint and for something else.
        let read = chunks("read", 1)[0];
        let waiting = [read, hints[3]];
        let waiter = {
            let demand = demand.clone();
            std::thread::spawn(move || demand.fetch_within(&waiting, Duration::from_millis(300)))
        };
        assert!(seq.wait_past(1, Duration::from_secs(5)) >= 2);
        let pending = demand.pending();
        let mut reads = waiting.to_vec();
        reads.sort_by_key(|(digest, _)| *digest);
        assert_eq!(pending[..2], reads, "what readers wait for goes first");
        assert_eq!(pending[2..5], hints[..3]);
        assert_eq!(pending[5], hints[4], "and is listed once");
        // The cap is on hints: nothing a reader waits for is left out.
        assert_eq!(pending.len(), 2 + Demand::HINT_PENDING);
        assert_eq!(demand.listed(), (reads, pending[2..].to_vec()));
        assert!(waiter.join().unwrap().is_err());
        assert_eq!(
            demand.pending()[..Demand::HINT_PENDING],
            hints[..Demand::HINT_PENDING]
        );
        Ok(())
    }

    #[test]
    fn a_hint_expires_unless_refreshed() -> Result<()> {
        let (_root, _seq, demand) = hinted_demand()?;
        let hints = chunks("hint", 2);
        let start = Instant::now();
        let after = |seconds: u64| start + Duration::from_secs(seconds);
        demand.hint_at(&hints, start);
        let ttl = Demand::HINT_TTL.as_secs();
        assert_eq!(demand.listed_at(after(ttl - 1)).1, hints);
        demand.hint_at(&hints[..1], after(ttl - 1));
        assert_eq!(demand.listed_at(after(ttl)).1, hints[..1]);
        assert_eq!(demand.listed_at(after(2 * ttl - 2)).1, hints[..1]);
        assert_eq!(demand.listed_at(after(2 * ttl)).1, Vec::new());
        // What expired has no holders left: one new hint, one unhint.
        demand.hint_at(&hints[..1], after(2 * ttl));
        demand.unhint(&hints[..1]);
        assert_eq!(demand.listed_at(after(2 * ttl)).1, Vec::new());
        Ok(())
    }

    #[test]
    fn unhint_removes_a_hint_only_when_no_handle_holds_it() -> Result<()> {
        let (_root, _seq, demand) = hinted_demand()?;
        let hints = chunks("hint", 2);
        demand.hint(&hints);
        demand.hint(&hints[..1]);
        demand.unhint(&hints);
        assert_eq!(demand.pending(), hints[..1]);
        demand.unhint(&hints);
        assert!(demand.pending().is_empty());
        // One that is gone is nothing to take back.
        demand.unhint(&hints);
        assert!(demand.pending().is_empty());
        Ok(())
    }

    #[test]
    fn arrival_clears_hints() -> Result<()> {
        let (_root, _seq, demand) = hinted_demand()?;
        let hints = chunks("hint", 2);
        demand.hint(&hints);
        demand.hint(&hints);
        demand.arrived(&[hints[0].0]);
        assert_eq!(demand.pending(), hints[1..]);
        Ok(())
    }
}
