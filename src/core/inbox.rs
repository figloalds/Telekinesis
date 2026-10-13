//! Durable inbox of received events and their causal activation: staging,
//! draining in causal order, link validation and the startup drain.
use super::*;

/// Inbox cache (owned by WP1.6 / WP2.4; empty in Wave 0).
#[derive(Default)]
pub(crate) struct Cache {}

/// Schema hook for inbox side tables (WP1.6). Wave 0: nothing to create.
pub(super) fn ensure_schema(_store: &mut Store) -> Result<()> {
    Ok(())
}

/// Phase 8 of `Store::open`: recover staged incoming events.
/// Wave 0: activation is never deferred, so this always drains.
pub(super) fn startup_drain(store: &mut Store, defer_activation: bool) -> Result<()> {
    let _ = defer_activation;
    crate::startup::progress("recovering incoming events");
    store.drain_incoming()?;
    Ok(())
}

impl Store {
    pub fn receive(
        &mut self,
        bundle: Bundle,
        allowed_devices: &BTreeSet<String>,
    ) -> Result<Vec<String>> {
        self.receive_with_budget(bundle, allowed_devices, None)
    }
    pub(crate) fn receive_with_budget(
        &mut self,
        bundle: Bundle,
        allowed_devices: &BTreeSet<String>,
        budget: Option<usize>,
    ) -> Result<Vec<String>> {
        ensure!(bundle.repo == self.repo, "REPOSITORY_MISMATCH");
        for b in &bundle.branches {
            ensure!(b.shared, "PRIVATE_BRANCH_REJECTED");
        }
        for e in &bundle.events {
            ensure!(
                e.branch.shared && allowed_devices.contains(&e.device),
                "UNAUTHORIZED_EVENT"
            );
        }
        let authorized: BTreeSet<_> = bundle.events.iter().flat_map(Event::objects).collect();
        for (h, data) in bundle.objects {
            ensure!(authorized.contains(&h), "UNAUTHORIZED_OBJECT");
            let bytes = hex::decode(data)?;
            ensure!(hash(&bytes) == h, "CORRUPT_OBJECT");
            self.put_object(&bytes)?;
            // Only actual verified inbound bytes grant reuse authorization. A
            // manifest/hash alone cannot relabel private-only local CAS content.
            self.db.execute(
                "INSERT OR IGNORE INTO received_shared_objects VALUES(?)",
                [h],
            )?;
        }
        let mut accepted = Vec::new();
        let tx = self.db.savepoint()?;
        for e in bundle.events {
            let payload = serde_json::to_string(&e)?;
            let committed: Option<String> = tx
                .query_row("SELECT payload FROM events WHERE id=?", [&e.id], |row| {
                    row.get(0)
                })
                .optional()?;
            if let Some(committed) = committed {
                ensure!(
                    serde_json::from_str::<Event>(&committed)? == e,
                    "EVENT_ID_REUSED"
                );
                accepted.push(e.id);
                continue;
            }
            let old: Option<String> = tx
                .query_row(
                    "SELECT payload FROM incoming_events WHERE id=?",
                    [&e.id],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(old) = old {
                ensure!(old == payload, "EVENT_ID_REUSED");
            } else {
                tx.execute(
                    "INSERT INTO incoming_events(id,payload) VALUES(?,?)",
                    params![e.id, payload],
                )?;
            }
        }
        tx.commit()?;
        accepted.extend(self.drain_incoming_budget(budget)?);
        for b in bundle.branches {
            if let Ok(existing) = self.branch(&b.id) {
                ensure!(existing == b, "BRANCH_DESCRIPTOR_CHANGED");
            } else {
                let pending = self
                    .db
                    .prepare("SELECT payload FROM incoming_events")?
                    .query_map([], |r| r.get::<_, String>(0))?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                if pending.iter().any(|payload| {
                    serde_json::from_str::<Event>(payload).is_ok_and(|e| e.branch.id == b.id)
                }) {
                    continue;
                }
                self.db
                    .execute("INSERT INTO branches VALUES(?,?,1)", params![b.id, b.name])?;
                self.rebuild(&b.id)?;
            }
        }
        Ok(accepted)
    }
    fn drain_incoming(&mut self) -> Result<Vec<String>> {
        self.drain_incoming_budget(None)
    }
    fn drain_incoming_budget(&mut self, budget: Option<usize>) -> Result<Vec<String>> {
        if !self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM incoming_events WHERE error IS NULL)",
            [],
            |r| r.get::<_, bool>(0),
        )? {
            return Ok(vec![]);
        }
        // One received frame is acknowledged only after activation completes.
        // Batch its validated mutations/receipts behind one SQLite durability
        // barrier; nested per-event savepoints still isolate semantic failures.
        // All referenced objects were installed durably before entering here.
        let name = format!("incoming_{}", id().replace('-', ""));
        self.db.execute_batch(&format!("SAVEPOINT {name}"))?;
        let result = self.drain_incoming_inner(budget);
        match result {
            Ok(accepted) => {
                if let Err(error) = self.db.execute_batch(&format!("RELEASE {name}")) {
                    let _ = self
                        .db
                        .execute_batch(&format!("ROLLBACK TO {name}; RELEASE {name}"));
                    return Err(error.into());
                }
                Ok(accepted)
            }
            Err(error) => {
                self.db
                    .execute_batch(&format!("ROLLBACK TO {name}; RELEASE {name}"))?;
                Err(error)
            }
        }
    }
    fn drain_incoming_inner(&mut self, budget: Option<usize>) -> Result<Vec<String>> {
        let mut accepted = vec![];
        let started = std::time::Instant::now();
        let mut attempts = 0;
        loop {
            let staged: Vec<(String, Option<String>)> = self
                .db
                .prepare("SELECT payload,error FROM incoming_events ORDER BY id")?
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect::<std::result::Result<_, _>>()?;
            let staged: Vec<(Event, Option<String>)> = staged
                .into_iter()
                .map(|(payload, error)| Ok((serde_json::from_str(&payload)?, error)))
                .collect::<Result<_>>()?;
            let mut known: BTreeMap<_, _> = self
                .events()?
                .into_iter()
                .map(|e| (e.id.clone(), e))
                .collect();
            let mut committed: BTreeSet<_> = known.keys().cloned().collect();
            for (e, _) in &staged {
                known.entry(e.id.clone()).or_insert_with(|| e.clone());
            }
            let quarantined: BTreeSet<_> = staged
                .iter()
                .filter(|(e, error)| error.is_some() && !committed.contains(&e.id))
                .map(|(e, _)| e.id.clone())
                .collect();
            let mut progress = false;
            for (e, error) in &staged {
                if budget.is_some_and(|maximum| {
                    attempts >= maximum
                        || attempts > 0 && started.elapsed() >= std::time::Duration::from_millis(50)
                }) {
                    return Ok(accepted);
                }
                crate::startup::progress("recovering incoming events");
                if error.is_some() {
                    continue;
                }
                let links = validate_incoming_links(e, &known, &quarantined, &committed);
                let result = match links {
                    Err(error) => Err(error),
                    Ok(())
                        if !e
                            .dependencies
                            .iter()
                            .chain(e.changes.iter().flat_map(|c| c.parents.iter()))
                            .all(|p| committed.contains(p)) =>
                    {
                        continue;
                    }
                    Ok(()) => {
                        attempts += 1;
                        self.commit_ready(e.clone(), false)
                    }
                };
                match result {
                    Ok(()) => {
                        self.db
                            .execute("DELETE FROM incoming_events WHERE id=?", [&e.id])?;
                        accepted.push(e.id.clone());
                        committed.insert(e.id.clone());
                        progress = true;
                    }
                    Err(error) if error.downcast_ref::<CausalPrerequisitesPending>().is_some() => {}
                    Err(error) => {
                        self.db.execute(
                            "UPDATE incoming_events SET error=? WHERE id=?",
                            params![format!("{error:#}"), e.id],
                        )?;
                        progress = true;
                    }
                }
            }
            if !progress {
                break;
            }
        }
        Ok(accepted)
    }
}
pub(super) fn validate_known_links(
    event: &Event,
    known: &BTreeMap<String, Event>,
    quarantined: &BTreeSet<String>,
) -> Result<()> {
    validate_incoming_links(event, known, quarantined, &BTreeSet::new())
}
fn validate_incoming_links(
    event: &Event,
    known: &BTreeMap<String, Event>,
    quarantined: &BTreeSet<String>,
    committed: &BTreeSet<String>,
) -> Result<()> {
    validate_direct_links(event, known, quarantined)?;
    let mut visited = BTreeSet::new();
    let mut todo = event.dependencies.clone();
    todo.extend(event.changes.iter().flat_map(|c| c.parents.iter().cloned()));
    while let Some(revision) = todo.pop() {
        ensure!(revision != event.id, "CAUSAL_CYCLE");
        if committed.contains(&revision) {
            continue;
        }
        if visited.insert(revision.clone())
            && let Some(parent) = known.get(&revision)
        {
            todo.extend(parent.dependencies.iter().cloned());
            todo.extend(
                parent
                    .changes
                    .iter()
                    .flat_map(|c| c.parents.iter().cloned()),
            );
        }
    }
    Ok(())
}
pub(super) fn validate_direct_links(
    event: &Event,
    known: &BTreeMap<String, Event>,
    quarantined: &BTreeSet<String>,
) -> Result<()> {
    for dependency in &event.dependencies {
        ensure!(
            !quarantined.contains(dependency),
            "QUARANTINED_CAUSAL_ANCESTOR: {dependency}"
        );
        if let Some(parent) = known.get(dependency) {
            ensure!(
                parent.branch == event.branch,
                "INVALID_CAUSAL_DEPENDENCY: {dependency}"
            );
        }
    }
    for change in &event.changes {
        for revision in &change.parents {
            ensure!(
                !quarantined.contains(revision),
                "QUARANTINED_CAUSAL_ANCESTOR: {revision}"
            );
            if let Some(parent) = known.get(revision) {
                ensure!(
                    parent.branch == event.branch
                        && parent
                            .changes
                            .iter()
                            .any(|c| c.entity == change.entity && c.field == change.field),
                    "INVALID_CAUSAL_PARENT: {revision}"
                );
            }
        }
    }
    Ok(())
}
