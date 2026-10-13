//! Status and content summaries of one store.
use super::*;

impl Store {
    pub fn content_status(&self) -> Result<Value> {
        let objects: BTreeSet<_> = self
            .events()?
            .iter()
            .filter(|e| e.branch.shared)
            .flat_map(Event::objects)
            .collect();
        let available = self.shared_object_inventory()?;
        let partial = self.partial_objects()?;
        let projection = self.projection(&self.active)?;
        let current: BTreeSet<_> = projection
            .entries
            .values()
            .filter(|e| e.alive)
            .filter_map(|e| e.content.clone())
            .collect();
        let missing = projection
            .entries
            .values()
            .filter(|e| e.alive)
            .filter_map(|e| e.content.as_ref().map(|object| (e, object)))
            .filter(|(_, h)| !self.object_available(h))
            .take(20)
            .map(|(entry, object)| {
                let mut state = self.object_state(object)?;
                let mut path = vec![entry.name.clone()];
                let mut parent = &entry.parent;
                let mut seen = BTreeSet::new();
                while parent != ROOT && seen.insert(parent.clone()) {
                    let Some(e) = projection.entries.get(parent) else {
                        break;
                    };
                    path.push(e.name.clone());
                    parent = &e.parent;
                }
                path.reverse();
                state["path"] = json!(path.join("/"));
                Ok(state)
            })
            .collect::<Result<Vec<_>>>()?;
        let present = current.iter().filter(|h| self.object_available(h)).count();
        Ok(
            json!({"items":missing,"current_objects":current.len(),"current_available":present,
            "current_missing":current.len()-present,"shared_objects":objects.len(),
            "shared_available":objects.intersection(&available).count(),"downloading":partial.len(),
            "partial_bytes":partial.values().sum::<u64>(),"complete":objects.is_subset(&available),
            "requested":self.wanted_objects()?.len()}),
        )
    }
    pub fn outgoing_unacknowledged(&self) -> Result<i64> {
        Ok(self.db.query_row(
            "SELECT count(*) FROM outbox WHERE acknowledged=0",
            [],
            |r| r.get(0),
        )?)
    }
    pub fn status(&self) -> Result<Value> {
        let p = self.projection(&self.active)?;
        let outgoing = self.outgoing_unacknowledged()?;
        Ok(
            json!({"repo":self.repo,"device":self.device,"branch":self.branch(&self.active)?,"generation":self.generation,
            "content":self.content_status()?,"local_durability":"metadata:sqlite-full; content:per-object","outgoing_unacknowledged":outgoing,"incoming_pending":self.db.query_row("SELECT count(*) FROM incoming_events",[],|r|r.get::<_,i64>(0))?,"incoming_quarantined":self.db.query_row("SELECT count(*) FROM incoming_events WHERE error IS NOT NULL",[],|r|r.get::<_,i64>(0))?,"peer_protocol":PEER_FORMAT,
            "caught_up":"unknown-until-peer-roundtrip","pending_causal_events":p.pending,"unresolved_conflicts":self.conflicts()?.iter().filter(|c|c.resolved_by.is_none()).count()}),
        )
    }
}
