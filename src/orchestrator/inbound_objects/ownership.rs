//! Local offer ownership. Weak slots live while any offer or handler can
//! still finish; a late result cannot resurrect an older session/offer.
use crate::{
    model::{errors::ErrorWire, orchestrator::Ctx},
    orchestrator::lease_clock::{self, PushSession},
    util::lock::lock,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};

type Order = (u64, u64, u64, u64);
const MAX_SLOTS: usize = 4096;
static SLOTS: Mutex<BTreeMap<String, Weak<Slot>>> = Mutex::new(BTreeMap::new());
struct Slot {
    latest: Mutex<Option<Order>>,
}

#[derive(Clone)]
pub(super) struct Owner {
    session: Arc<PushSession>,
    slot: Arc<Slot>,
    frame: u64,
}
impl Owner {
    pub(super) fn begin(
        ctx: &Arc<Ctx>,
        session: &Arc<PushSession>,
        key: &str,
        advance: bool,
    ) -> Result<Self, ErrorWire> {
        let slot = {
            let mut slots = lock(&SLOTS);
            if let Some(slot) = slots.get(key).and_then(Weak::upgrade) {
                slot
            } else {
                if slots.len() >= MAX_SLOTS {
                    slots.retain(|_, slot| slot.strong_count() > 0);
                    if slots.len() >= MAX_SLOTS {
                        return Err(ErrorWire::coded(
                            "CONTENT_RESOURCE_LIMIT",
                            "inbound offer ownership capacity",
                        ));
                    }
                }
                let slot = Arc::new(Slot {
                    latest: Mutex::new(None),
                });
                slots.insert(key.to_owned(), Arc::downgrade(&slot));
                slot
            }
        };
        let parent = (
            session.run.generation.incarnation,
            session.run.layout,
            session.run.generation.run,
        );
        let frame = {
            let mut latest = lock(&slot.latest);
            if !session.current()
                || latest.is_some_and(|(inc, pool, run, _)| (inc, pool, run) > parent)
            {
                return Err(ErrorWire::coded(
                    "IN_FLIGHT_ELSEWHERE",
                    "inbound push session superseded",
                ));
            }
            match *latest {
                Some((inc, pool, run, frame)) if !advance && (inc, pool, run) == parent => frame,
                _ => {
                    let frame = lease_clock::next(ctx)?.run;
                    *latest = Some((parent.0, parent.1, parent.2, frame));
                    frame
                }
            }
        };
        Ok(Self {
            session: session.clone(),
            slot,
            frame,
        })
    }
    pub(super) fn order(&self) -> Order {
        (
            self.session.run.generation.incarnation,
            self.session.run.layout,
            self.session.run.generation.run,
            self.frame,
        )
    }
    pub(super) fn current(&self) -> bool {
        self.session.current() && *lock(&self.slot.latest) == Some(self.order())
    }
    pub(super) fn while_current<T>(&self, apply: impl FnOnce() -> T) -> Option<T> {
        let latest = lock(&self.slot.latest);
        if *latest != Some(self.order()) {
            return None;
        }
        self.session.while_current(apply)
    }
    pub(super) fn lease(&self, base: &str) -> Result<String, ErrorWire> {
        crate::model::lease::qualify_push(base, self.session.run, self.frame)
    }
}
