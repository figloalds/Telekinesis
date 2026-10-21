//! Legacy object work owns its effects through the final error projection.
//! Weak registries retain a fence while any old writer can still finish.
use crate::{model::errors::ErrorWire, util::lock::lock};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, TryLockError, Weak},
};

const MAX_OBJECTS: usize = 64 * 1024;
const MAX_FAMILIES: usize = crate::model::lease::MAX_RUN_FENCES;
pub(crate) type Gate = Arc<Mutex<()>>;

#[derive(Debug, Default)]
pub struct Effects {
    objects: BTreeMap<String, Weak<Slot>>,
    families: BTreeMap<String, Weak<Mutex<()>>>,
    active: BTreeMap<String, Work>,
}
#[derive(Debug, Default)]
struct Slot {
    effect: Mutex<()>,
    current: Mutex<Weak<Ticket>>,
}
#[derive(Debug)]
struct Ticket {
    session: String,
}
#[derive(Debug, Clone)]
pub(crate) struct Work {
    slot: Arc<Slot>,
    ticket: Arc<Ticket>,
    pub(crate) family: Gate,
}
impl Work {
    pub(crate) fn current(&self) -> bool {
        lock(&self.slot.current)
            .upgrade()
            .is_some_and(|ticket| Arc::ptr_eq(&ticket, &self.ticket))
    }
    pub(crate) fn object_gate(&self) -> Result<MutexGuard<'_, ()>, ErrorWire> {
        try_gate(&self.slot.effect)
    }
}

pub(crate) fn try_gate(gate: &Mutex<()>) -> Result<MutexGuard<'_, ()>, ErrorWire> {
    match gate.try_lock() {
        Ok(guard) => Ok(guard),
        Err(TryLockError::Poisoned(error)) => Ok(error.into_inner()),
        Err(TryLockError::WouldBlock) => Err(ErrorWire::coded(
            "WORKER_BUSY",
            "object effect commit in progress",
        )),
    }
}
fn room<T>(map: &mut BTreeMap<String, Weak<T>>, key: &str, max: usize) -> Result<(), ErrorWire> {
    if map.contains_key(key) || map.len() < max {
        return Ok(());
    }
    map.retain(|_, value| value.strong_count() > 0);
    if map.len() >= max {
        return Err(ErrorWire::coded(
            "CONTENT_RESOURCE_LIMIT",
            "object effect owner capacity",
        ));
    }
    Ok(())
}
impl Effects {
    pub(crate) fn family(&mut self, key: &str) -> Result<Gate, ErrorWire> {
        if let Some(gate) = self.families.get(key).and_then(Weak::upgrade) {
            return Ok(gate);
        }
        room(&mut self.families, key, MAX_FAMILIES)?;
        let gate = Arc::new(Mutex::new(()));
        self.families.insert(key.into(), Arc::downgrade(&gate));
        Ok(gate)
    }
    pub(crate) fn grant(
        &mut self,
        hash: &str,
        session: &str,
        family: Gate,
    ) -> Result<(), ErrorWire> {
        let slot = if let Some(slot) = self.objects.get(hash).and_then(Weak::upgrade) {
            slot
        } else {
            room(&mut self.objects, hash, MAX_OBJECTS)?;
            let slot = Arc::new(Slot::default());
            self.objects.insert(hash.into(), Arc::downgrade(&slot));
            slot
        };
        let _effect = try_gate(&slot.effect)?;
        let ticket = Arc::new(Ticket {
            session: session.into(),
        });
        *lock(&slot.current) = Arc::downgrade(&ticket);
        self.active.insert(
            hash.into(),
            Work {
                slot: slot.clone(),
                ticket,
                family,
            },
        );
        Ok(())
    }
    pub(crate) fn work(&self, hash: &str) -> Option<Work> {
        self.active.get(hash).cloned()
    }
    pub(crate) fn ensure(
        &mut self,
        hash: &str,
        session: &str,
        family: Gate,
    ) -> Result<Work, ErrorWire> {
        if let Some(work) = self
            .work(hash)
            .filter(|work| work.ticket.session == session)
        {
            return Ok(work);
        }
        self.grant(hash, session, family)?;
        Ok(self.active[hash].clone())
    }
    /// A verdict releases its lease, but old writers keep the ticket alive.
    pub(crate) fn ended(&mut self, hash: &str) {
        self.active.remove(hash);
    }
    /// An explicit release/expiry retires even a verdict awaiting projection.
    pub(crate) fn cancel(&mut self, hash: &str, session: &str) {
        if let Some(slot) = self.objects.get(hash).and_then(Weak::upgrade) {
            let mut current = lock(&slot.current);
            if current
                .upgrade()
                .is_some_and(|ticket| ticket.session == session)
            {
                *current = Weak::new();
            }
        }
        if self
            .active
            .get(hash)
            .is_some_and(|work| work.ticket.session == session)
        {
            self.active.remove(hash);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn late_projection_ticket_does_not_regain_authority_after_replacement_finishes() {
        let mut state = Effects::default();
        let family = state.family("family").unwrap();
        state.grant("hash", "same-session", family.clone()).unwrap();
        let old = state.work("hash").unwrap();
        state.ended("hash");
        assert!(old.current());
        state.grant("hash", "same-session", family).unwrap();
        let new = state.work("hash").unwrap();
        state.ended("hash");
        assert!(!old.current() && new.current());
        drop(new);
        assert!(
            !old.current(),
            "a dropped replacement resurrected its predecessor"
        );
    }
    #[test]
    fn object_commit_gate_defers_only_its_object_and_cancel_retires_pending_effects() {
        let mut state = Effects::default();
        let family = state.family("a").unwrap();
        state.grant("a", "session", family.clone()).unwrap();
        let old = state.work("a").unwrap();
        state.ended("a");
        let guard = old.object_gate().unwrap();
        assert_eq!(
            state.grant("a", "new", family.clone()).unwrap_err().code,
            "WORKER_BUSY"
        );
        state.grant("b", "other", family.clone()).unwrap();
        drop(guard);
        state.cancel("a", "session");
        assert!(!old.current());
        state.grant("a", "new", family).unwrap();
        assert!(!old.current());
    }
    #[test]
    fn bounded_registries_keep_live_entries_and_reclaim_dead_ones() {
        let mut map = BTreeMap::new();
        let first = Arc::new(());
        map.insert("a".into(), Arc::downgrade(&first));
        map.insert("b".into(), Arc::downgrade(&Arc::new(())));
        assert!(room(&mut map, "c", 2).is_ok());
        let other = Arc::new(());
        map.insert("c".into(), Arc::downgrade(&other));
        assert_eq!(
            room(&mut map, "d", 2).unwrap_err().code,
            "CONTENT_RESOURCE_LIMIT"
        );
        assert!(room(&mut map, "a", 2).is_ok());
        assert!(Arc::ptr_eq(&map["a"].upgrade().unwrap(), &first));
    }
}
