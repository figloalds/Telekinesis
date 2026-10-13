//! What is shared with peers: event inventories, peer knowledge, outgoing
//! metadata pages and outbox acknowledgements.
use super::derive::causal_order;
use super::*;

/// Sharing cache (owned by WP2.5; empty in Wave 0).
#[derive(Default)]
pub(crate) struct Cache {}

/// Schema hook for feed side tables (WP2.5). Wave 0: nothing to create.
pub(super) fn ensure_schema(_store: &mut Store) -> Result<()> {
    Ok(())
}

impl Store {
    pub fn known_by_peer(&self, peer: &str) -> Result<BTreeSet<String>> {
        Ok(self
            .db
            .prepare("SELECT event FROM peer_known WHERE peer=?")?
            .query_map([peer], |r| r.get(0))?
            .collect::<std::result::Result<_, _>>()?)
    }
    pub fn shared_event_inventory(&self) -> Result<BTreeSet<String>> {
        Ok(self
            .db
            .prepare("SELECT e.id FROM events e JOIN branches b ON b.id=e.branch WHERE b.shared=1")?
            .query_map([], |r| r.get(0))?
            .collect::<std::result::Result<_, _>>()?)
    }
    /// The configured authenticated peer advertises accepted, durable events,
    /// not staged manifests or object possession. Record this current receipt
    /// snapshot and reconcile the two-device outbox in the same transaction.
    /// A withdrawn event is queued again (for example after a peer restore).
    pub fn remember_peer_inventory(&mut self, peer: &str, known: &BTreeSet<String>) -> Result<()> {
        let tx = self.db.savepoint()?;
        tx.execute("DELETE FROM peer_known WHERE peer=?", [peer])?;
        for event in known {
            tx.execute("INSERT INTO peer_known VALUES(?,?)", params![peer, event])?;
        }
        tx.execute("UPDATE outbox SET acknowledged=EXISTS(SELECT 1 FROM peer_known p WHERE p.peer=? AND p.event=outbox.event)
            WHERE event IN (SELECT e.id FROM events e JOIN branches b ON b.id=e.branch WHERE b.shared=1)", [peer])?;
        tx.commit()?;
        Ok(())
    }
    pub fn shared_bundle(&self, known: &BTreeSet<String>) -> Result<Bundle> {
        let events: Vec<_> = self
            .events()?
            .into_iter()
            .filter(|e| e.branch.shared && !known.contains(&e.id))
            .collect();
        // Authorization starts with shared semantic events, never the object dir.
        let hashes: BTreeSet<_> = events.iter().flat_map(Event::objects).collect();
        let available = self.shared_object_inventory()?;
        let objects = hashes
            .into_iter()
            .filter(|h| available.contains(h))
            .map(|h| Ok((h.clone(), hex::encode(self.read_object(&h)?))))
            .collect::<Result<_>>()?;
        Ok(Bundle {
            repo: self.repo.clone(),
            branches: self.branches()?.into_iter().filter(|b| b.shared).collect(),
            events,
            objects,
        })
    }
    pub fn known_objects_by_peer(&self, peer: &str) -> Result<BTreeSet<String>> {
        Ok(self
            .db
            .prepare("SELECT hash FROM peer_objects WHERE peer=?")?
            .query_map([peer], |r| r.get(0))?
            .collect::<std::result::Result<_, _>>()?)
    }
    pub fn remember_peer_objects(&mut self, peer: &str, objects: &BTreeSet<String>) -> Result<()> {
        let tx = self.db.savepoint()?;
        tx.execute("DELETE FROM peer_objects WHERE peer=?", [peer])?;
        for h in objects {
            tx.execute("INSERT INTO peer_objects VALUES(?,?)", params![peer, h])?;
        }
        tx.commit()?;
        Ok(())
    }
    /// Manifests and objects can cross separately. A large atomic root remains
    /// visible after causal validation; content availability is tracked separately.
    pub fn shared_page(
        &self,
        known: &BTreeSet<String>,
        known_objects: &BTreeSet<String>,
        budget: usize,
    ) -> Result<Bundle> {
        self.shared_page_content(known, known_objects, budget, true)
    }
    /// Event manifests without reading CAS content. Reserved network lanes must
    /// never build bulk payloads only to discard them afterwards.
    pub fn shared_metadata_page(&self, known: &BTreeSet<String>, budget: usize) -> Result<Bundle> {
        self.shared_page_content(known, &BTreeSet::new(), budget, false)
    }
    fn shared_page_content(
        &self,
        known: &BTreeSet<String>,
        known_objects: &BTreeSet<String>,
        budget: usize,
        include_objects: bool,
    ) -> Result<Bundle> {
        let branches: Vec<_> = self.branches()?.into_iter().filter(|b| b.shared).collect();
        let mut used = serde_json::to_vec(&branches)?.len() + 1024;
        let mut events = vec![];
        // Inventory acknowledges committed events only. Sending a UUID-sorted
        // prefix can fill every page with children whose parents never fit, so
        // the receiver cannot ACK and the same prefix repeats forever.
        let journal = self.events()?;
        for e in causal_order(&journal)
            .into_iter()
            .filter(|e| e.branch.shared && !known.contains(&e.id))
        {
            let size = serde_json::to_vec(&e)?.len() + 1;
            ensure!(size < budget / 3, "EVENT_METADATA_TOO_LARGE");
            if used + size > budget / 3 {
                break;
            }
            used += size;
            events.push(e.clone());
        }
        let available = if include_objects {
            self.shared_object_inventory()?
        } else {
            BTreeSet::new()
        };
        let hashes: BTreeSet<_> = events.iter().flat_map(Event::objects).collect();
        let mut objects = BTreeMap::new();
        for h in hashes
            .into_iter()
            .filter(|h| include_objects && available.contains(h) && !known_objects.contains(h))
        {
            let length = self.object_size(&h)?;
            if length > MAX_FILE as u64 || used + length as usize * 2 + h.len() + 8 > budget {
                continue;
            }
            let bytes = self.read_object(&h)?;
            let size = bytes.len() * 2 + h.len() + 8;
            if used + size > budget {
                continue;
            }
            used += size;
            objects.insert(h, hex::encode(bytes));
        }
        let bundle = Bundle {
            repo: self.repo.clone(),
            branches,
            events,
            objects,
        };
        ensure!(
            serde_json::to_vec(&bundle)?.len() <= budget,
            "BUNDLE_TOO_LARGE"
        );
        Ok(bundle)
    }
    pub fn acknowledge(&self, events: &[String]) -> Result<()> {
        for rev in events {
            self.db.execute(
                "UPDATE outbox SET acknowledged=1 WHERE event=? AND acknowledged=0 AND event IN
                    (SELECT e.id FROM events e JOIN branches b ON b.id=e.branch WHERE b.shared=1)",
                [rev],
            )?;
        }
        Ok(())
    }
}
