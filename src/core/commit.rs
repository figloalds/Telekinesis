//! The commit path: validation, the incremental successors (fast paths) and
//! the full-replay slow path with its forced snapshot seal.
use super::derive::persist_projection;
use super::*;

/// Commit cache (owned by WP2.4, e.g. its event cache; empty in Wave 0).
#[derive(Default)]
pub(crate) struct Cache {}

impl Store {
    pub fn commit(&mut self, event: Event, local: bool) -> Result<()> {
        if local {
            return self.commit_ready(event, true);
        }
        // Trusted core callers supply the actual local bytes explicitly. The
        // network path uses receive directly and never promotes global CAS hashes.
        let objects = event
            .objects()
            .into_iter()
            .map(|h| Ok((h.clone(), hex::encode(self.read_object(&h)?))))
            .collect::<Result<_>>()?;
        let revision = event.id.clone();
        let device = event.device.clone();
        self.receive(
            Bundle {
                repo: self.repo.clone(),
                branches: vec![],
                events: vec![event],
                objects,
            },
            &BTreeSet::from([device]),
        )?;
        let error: Option<String> = self
            .db
            .query_row(
                "SELECT error FROM incoming_events WHERE id=?",
                [&revision],
                |r| r.get(0),
            )
            .optional()?
            .flatten();
        if let Some(error) = error {
            bail!("{error}");
        }
        Ok(())
    }
    pub(super) fn commit_ready(&mut self, event: Event, local: bool) -> Result<()> {
        ensure!(event.repo == self.repo, "REPOSITORY_MISMATCH");
        ensure!(
            event.id.len() <= 128 && !event.id.is_empty() && event.device.len() <= 128,
            "INVALID_EVENT"
        );
        ensure!(
            !event.changes.is_empty() && event.changes.len() <= 10000,
            "INVALID_EVENT"
        );
        ensure!(local || event.branch.shared, "PRIVATE_EVENT_REJECTED");
        let payload = serde_json::to_string(&event)?;
        ensure!(payload.len() <= MAX_EVENT, "EVENT_METADATA_TOO_LARGE");
        name_key(&event.branch.name)?;
        ensure!(
            !event.branch.id.is_empty() && event.branch.id.len() <= 128,
            "INVALID_BRANCH_ID"
        );
        let existing: Option<String> = self
            .db
            .query_row("SELECT payload FROM events WHERE id=?", [&event.id], |r| {
                r.get(0)
            })
            .optional()?;
        if let Some(old) = existing {
            ensure!(old == payload, "EVENT_ID_REUSED");
            return Ok(());
        }
        let objects = event.objects();
        ensure!(
            event.object_sizes.keys().all(|h| objects.contains(h)),
            "UNREFERENCED_OBJECT_SIZE"
        );
        for object in &objects {
            valid_object(object)?;
            if let Some(size) = event.object_sizes.get(object) {
                ensure!(*size <= MAX_CONTENT, "FILE_TOO_LARGE");
                if let Ok(old) = self.object_size(object) {
                    ensure!(old == *size, "OBJECT_SIZE_CHANGED");
                }
            }
            // Newly authored bytes must exist; metadata decisions may refer to
            // previously accepted, currently unavailable revisions.
            if local
                && !self.db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM object_references WHERE hash=?)",
                    [object],
                    |r| r.get::<_, bool>(0),
                )?
            {
                self.verify_object(object)?;
            }
        }
        let mut seen = BTreeSet::new();
        for change in &event.changes {
            ensure!(
                seen.insert(register(&change.entity, &change.field)),
                "DUPLICATE_REGISTER"
            );
            ensure!(
                change.entity != ROOT && !change.entity.is_empty() && change.entity.len() <= 128,
                "INVALID_ENTITY"
            );
            ensure!(
                ["kind", "location", "content", "alive", "basic"].contains(&change.field.as_str()),
                "INVALID_FIELD"
            );
            ensure!(!change.parents.contains(&event.id), "CAUSAL_CYCLE");
            match change.field.as_str() {
                "location" => {
                    name_key(change.value["name"].as_str().context("INVALID_LOCATION")?)?;
                    ensure!(
                        change.value["parent"].as_str().is_some(),
                        "INVALID_LOCATION"
                    );
                }
                "kind" => ensure!(
                    [json!("file"), json!("directory")].contains(&change.value),
                    "INVALID_KIND"
                ),
                "alive" => ensure!(change.value.is_boolean(), "INVALID_ALIVE"),
                "content" => ensure!(change.value.is_string(), "INVALID_CONTENT"),
                "basic" => serde_json::from_value::<BasicInfo>(change.value.clone())?.validate()?,
                _ => {}
            }
        }
        if let Ok(b) = self.branch(&event.branch.id) {
            ensure!(b == event.branch, "BRANCH_DESCRIPTOR_CHANGED");
        }
        let mut fast = self.content_successor(&event)?;
        if fast.is_none() && local {
            fast = self.creation_successor(&event)?;
            if fast.is_none() {
                fast = self.namespace_successor(&event)?;
            }
        }
        let incremental = fast.is_some();
        let events = if fast.is_none() {
            let mut events: Vec<_> = self
                .events()?
                .into_iter()
                .filter(|e| e.branch.id == event.branch.id)
                .collect();
            events.push(event.clone());
            events
        } else {
            vec![]
        };
        let projection = if let Some(p) = fast.take() {
            p
        } else {
            project(&event.branch.id, &events)?
        };
        if !projection.pending.is_empty() {
            return Err(CausalPrerequisitesPending.into());
        }
        fault("before_sql");
        let tx = self.db.savepoint()?;
        for object in &objects {
            tx.execute(
                "INSERT OR IGNORE INTO object_references VALUES(?)",
                [object],
            )?;
        }
        for (object, size) in &event.object_sizes {
            tx.execute(
                "INSERT OR IGNORE INTO object_sizes VALUES(?,?)",
                params![object, size],
            )?;
        }
        // Explicit content authorship/publication grants sharing. A resolution
        // of an inbound hash alone must not expose private-only local bytes.
        if local && event.branch.shared && event.reviewed.is_empty() {
            for object in &objects {
                if self.root.join("objects").join(object).is_file() {
                    tx.execute(
                        "INSERT OR IGNORE INTO published_objects VALUES(?)",
                        [object],
                    )?;
                }
            }
        }
        tx.execute(
            "INSERT OR IGNORE INTO branches VALUES(?,?,?)",
            params![event.branch.id, event.branch.name, event.branch.shared],
        )?;
        tx.execute(
            "INSERT INTO events VALUES(?,?,?)",
            params![event.id, event.branch.id, payload],
        )?;
        if event.branch.shared {
            tx.execute("INSERT INTO outbox(event) VALUES(?)", [&event.id])?;
        }
        if incremental {
            for entity in event
                .changes
                .iter()
                .map(|c| &c.entity)
                .collect::<BTreeSet<_>>()
            {
                let heads: BTreeMap<_, _> = ["kind", "location", "alive", "content", "basic"]
                    .into_iter()
                    .filter_map(|field| {
                        let key = register(entity, field);
                        projection.heads.get(&key).map(|v| (key, v))
                    })
                    .collect();
                tx.execute("INSERT INTO current_parts VALUES(?,?,?,?) ON CONFLICT(branch,entity) DO UPDATE SET entry=excluded.entry,heads=excluded.heads",
                    params![event.branch.id,entity,serde_json::to_string(&projection.entries[entity])?,serde_json::to_string(&heads)?])?;
            }
        } else {
            persist_projection(&tx, &event.branch.id, &projection, &events, Some(&event.id))?;
        }
        for parent in event
            .dependencies
            .iter()
            .chain(event.changes.iter().flat_map(|c| c.parents.iter()))
        {
            tx.execute(
                "DELETE FROM event_frontier WHERE branch=? AND event=?",
                params![event.branch.id, parent],
            )?;
        }
        tx.execute(
            "INSERT INTO event_frontier VALUES(?,?)",
            params![event.branch.id, event.id],
        )?;
        snapshot::maybe_persist(&tx, &event.branch.id, !incremental)?;
        tx.commit()?;
        // Patch the view only if no other connection committed since it was
        // loaded (I6); otherwise drop it. Never fail after the commit.
        let data_version = self.data_version().ok();
        let mut cache = self.view_cache.borrow_mut();
        if incremental
            && cache.as_ref().is_some_and(|v| {
                v.branch == event.branch.id && Some(v.data_version) == data_version
            })
        {
            let view = cache.as_mut().unwrap();
            let entities: BTreeSet<_> = event.changes.iter().map(|c| &c.entity).collect();
            // A replacement moves one entry into another's old slot. Remove
            // all old slots first, independent of entity ID ordering.
            for entity in &entities {
                if let Some(old) = view.projection.entries.get(*entity).filter(|e| e.alive) {
                    view.slots
                        .remove(&(old.parent.clone(), name_key(&old.name)?));
                }
            }
            for entity in entities {
                let entry = &projection.entries[entity];
                if entry.alive {
                    view.slots.insert(
                        (entry.parent.clone(), name_key(&entry.name)?),
                        entity.clone(),
                    );
                }
            }
            view.projection.entries.extend(projection.entries);
            view.projection.heads.extend(projection.heads);
            view.changes = self.db.total_changes();
            view.transactional = !self.db.is_autocommit();
        } else {
            *cache = None;
        }
        fault("sql_committed");
        Ok(())
    }
    pub(super) fn content_successor(&self, event: &Event) -> Result<Option<Projection>> {
        if !event.reviewed.is_empty()
            || !event
                .changes
                .iter()
                .any(|c| ["content", "basic"].contains(&c.field.as_str()))
            || event.changes.iter().any(|c| {
                !["content", "alive", "basic"].contains(&c.field.as_str())
                    || (c.field == "alive" && c.value != true)
            })
        {
            return Ok(None);
        }
        let Ok(view) = self.view(&event.branch.id) else {
            return Ok(None);
        };
        let current = &view.projection;
        if !current.pending.is_empty() {
            return Ok(None);
        }
        for change in &event.changes {
            let Some(entry) = current.entries.get(&change.entity) else {
                return Ok(None);
            };
            let Some(heads) = current.heads.get(&register(&change.entity, &change.field)) else {
                if change.field == "basic" && change.parents.is_empty() && entry.alive {
                    continue;
                }
                return Ok(None);
            };
            if (entry.kind != "file" && change.field != "basic")
                || !entry.alive
                || heads.len() != 1
                || change.parents != vec![heads[0].revision.clone()]
            {
                return Ok(None);
            }
        }
        // Frontier dependencies are checked against accepted journal rows only.
        // A missing or cross-branch prerequisite falls back to full validation.
        for dependency in &event.dependencies {
            let branch: Option<String> = self
                .db
                .query_row("SELECT branch FROM events WHERE id=?", [dependency], |r| {
                    r.get(0)
                })
                .optional()?;
            if branch.as_deref() != Some(&event.branch.id) {
                return Ok(None);
            }
        }
        let mut p = Projection::default();
        for entity in event
            .changes
            .iter()
            .map(|c| &c.entity)
            .collect::<BTreeSet<_>>()
        {
            p.entries
                .insert(entity.clone(), current.entries[entity].clone());
            for field in ["kind", "location", "alive", "content", "basic"] {
                let key = register(entity, field);
                if let Some(heads) = current.heads.get(&key) {
                    p.heads.insert(key, heads.clone());
                }
            }
        }
        for change in &event.changes {
            p.heads.insert(
                register(&change.entity, &change.field),
                vec![Version {
                    revision: event.id.clone(),
                    device: event.device.clone(),
                    created_ms: event.created_ms,
                    parents: change.parents.clone(),
                    value: change.value.clone(),
                }],
            );
            let entry = p.entries.get_mut(&change.entity).unwrap();
            entry
                .revisions
                .insert(change.field.clone(), event.id.clone());
            if change.field == "content" {
                entry.content = change.value.as_str().map(str::to_owned);
            } else if change.field == "basic" {
                entry.basic = Some(serde_json::from_value(change.value.clone())?);
            }
        }
        for entity in event
            .changes
            .iter()
            .map(|c| &c.entity)
            .collect::<BTreeSet<_>>()
        {
            let modified = ["kind", "location", "alive", "content"]
                .iter()
                .filter_map(|field| {
                    p.heads
                        .get(&register(entity, field))
                        .and_then(|heads| heads.last())
                })
                .map(|v| v.created_ms)
                .max()
                .unwrap_or(0);
            p.entries.get_mut(entity).unwrap().modified_ms = modified;
        }
        // Each changed register has one head and this event parents it. All old
        // versions are ancestors, so no new historical pair/namespace conflict
        // is possible. Existing durable records remain untouched.
        Ok(Some(p))
    }
    /// A local fresh entry ordered after the entire accepted frontier cannot
    /// introduce a concurrent namespace cut. All other placements use replay.
    pub(super) fn creation_successor(&self, event: &Event) -> Result<Option<Projection>> {
        if event.device != self.device || !event.reviewed.is_empty() || event.changes.is_empty() {
            return Ok(None);
        }
        let entity = &event.changes[0].entity;
        if event
            .changes
            .iter()
            .any(|c| &c.entity != entity || !c.parents.is_empty())
        {
            return Ok(None);
        }
        let fields: BTreeMap<_, _> = event
            .changes
            .iter()
            .map(|c| (c.field.as_str(), &c.value))
            .collect();
        if !fields.contains_key("kind")
            || !fields.contains_key("location")
            || fields.get("alive") != Some(&&json!(true))
        {
            return Ok(None);
        }
        let kind = fields["kind"].as_str().context("INVALID_KIND")?;
        if (kind == "file" && !fields.contains_key("content"))
            || (kind == "directory" && fields.contains_key("content"))
        {
            return Ok(None);
        }
        let frontier = self
            .db
            .prepare("SELECT event FROM event_frontier WHERE branch=? ORDER BY event")?
            .query_map([&event.branch.id], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if event.dependencies != frontier {
            return Ok(None);
        }
        let Ok(view) = self.view(&event.branch.id) else {
            return Ok(None);
        };
        let current = &view.projection;
        if current.entries.contains_key(entity)
            || !current.pending.is_empty()
            || !current.conflicts.is_empty()
            || !self.conflicts()?.is_empty()
        {
            return Ok(None);
        }
        let parent = fields["location"]["parent"]
            .as_str()
            .context("INVALID_LOCATION")?;
        let name = fields["location"]["name"]
            .as_str()
            .context("INVALID_LOCATION")?;
        if parent != ROOT
            && !current
                .entries
                .get(parent)
                .is_some_and(|e| e.alive && e.kind == "directory")
        {
            return Ok(None);
        }
        let key = name_key(name)?;
        if view.slots.contains_key(&(parent.into(), key)) {
            return Ok(None);
        }
        let mut p = Projection::default();
        let mut entry = Entry {
            id: entity.clone(),
            kind: kind.into(),
            parent: parent.into(),
            name: name.into(),
            alive: true,
            created_ms: event.created_ms,
            modified_ms: event.created_ms,
            ..Default::default()
        };
        for change in &event.changes {
            entry
                .revisions
                .insert(change.field.clone(), event.id.clone());
            if change.field == "content" {
                entry.content = change.value.as_str().map(str::to_owned);
            }
            if change.field == "basic" {
                entry.basic = Some(serde_json::from_value(change.value.clone())?);
            }
            p.heads.insert(
                register(entity, &change.field),
                vec![Version {
                    revision: event.id.clone(),
                    device: event.device.clone(),
                    created_ms: event.created_ms,
                    parents: vec![],
                    value: change.value.clone(),
                }],
            );
        }
        p.entries.insert(entity.clone(), entry);
        Ok(Some(p))
    }
    /// A placement/deletion after the complete frontier adds only the new cut.
    /// If it preserves a valid namespace, no historical conflict can be added.
    /// Directory moves preserve descendant entity IDs; empty directory deletes
    /// are equally safe. Replaying every historical cut here can hold the mounted
    /// filesystem's engine and WinFsp namespace locks for minutes on IDE cleanup.
    pub(super) fn namespace_successor(&self, event: &Event) -> Result<Option<Projection>> {
        if event.device != self.device
            || !event.reviewed.is_empty()
            || !(1..=2).contains(&event.changes.len())
        {
            return Ok(None);
        }
        let replacement = if event.changes.len() == 2 {
            let location = event.changes.iter().find(|c| c.field == "location");
            let removed = event
                .changes
                .iter()
                .find(|c| c.field == "alive" && c.value == false);
            match (location, removed) {
                (Some(a), Some(b)) if a.entity != b.entity => Some((&a.entity, &b.entity)),
                _ => return Ok(None),
            }
        } else {
            None
        };
        if event.changes.iter().any(|change| {
            change.field != "location" && !(change.field == "alive" && change.value == false)
        }) {
            return Ok(None);
        }
        let frontier = self
            .db
            .prepare("SELECT event FROM event_frontier WHERE branch=? ORDER BY event")?
            .query_map([&event.branch.id], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if event.dependencies != frontier {
            return Ok(None);
        }
        let Ok(view) = self.view(&event.branch.id) else {
            return Ok(None);
        };
        let current = &view.projection;
        if !current.pending.is_empty()
            || !current.conflicts.is_empty()
            || !self.conflicts()?.is_empty()
        {
            return Ok(None);
        }
        let mut delta = Projection::default();
        for change in &event.changes {
            let Some(entry) = current.entries.get(&change.entity).filter(|e| e.alive) else {
                return Ok(None);
            };
            let key = register(&change.entity, &change.field);
            let Some(heads) = current.heads.get(&key) else {
                return Ok(None);
            };
            if heads.len() != 1 || change.parents != vec![heads[0].revision.clone()] {
                return Ok(None);
            }
            let mut entry = entry.clone();
            if change.field == "location" {
                let parent = change.value["parent"]
                    .as_str()
                    .context("INVALID_LOCATION")?;
                let name = change.value["name"].as_str().context("INVALID_LOCATION")?;
                if parent != ROOT
                    && !current
                        .entries
                        .get(parent)
                        .is_some_and(|e| e.alive && e.kind == "directory")
                {
                    return Ok(None);
                }
                let occupying = view.slots.get(&(parent.into(), name_key(name)?));
                if let Some((source, removed)) = replacement {
                    if source != &entry.id
                        || occupying != Some(removed)
                        || entry.kind != "file"
                        || current.entries[removed].kind != "file"
                    {
                        return Ok(None);
                    }
                } else if occupying.is_some_and(|e| e != &entry.id) {
                    return Ok(None);
                }
                // Validate here as well as in rename(): snapshot replay and
                // direct commits also use this path. Cycles need the reducer.
                if entry.kind == "directory" {
                    let mut ancestor = parent;
                    let mut seen = BTreeSet::new();
                    while ancestor != ROOT {
                        if ancestor == entry.id || !seen.insert(ancestor) {
                            return Ok(None);
                        }
                        let Some(dir) = current
                            .entries
                            .get(ancestor)
                            .filter(|e| e.alive && e.kind == "directory")
                        else {
                            return Ok(None);
                        };
                        ancestor = &dir.parent;
                    }
                }
                entry.parent = parent.into();
                entry.name = name.into();
            } else {
                // Deleting a parent with live children can suppress their
                // namespace and create conflicts, even on a sequential event.
                if current
                    .entries
                    .values()
                    .any(|e| e.alive && e.parent == entry.id)
                {
                    return Ok(None);
                }
                entry.alive = false;
            }
            entry
                .revisions
                .insert(change.field.clone(), event.id.clone());
            entry.modified_ms = entry.modified_ms.max(event.created_ms);
            for field in ["kind", "location", "alive", "content", "basic"] {
                let key = register(&entry.id, field);
                if let Some(heads) = current.heads.get(&key) {
                    delta.heads.insert(key, heads.clone());
                }
            }
            delta.heads.insert(
                key,
                vec![Version {
                    revision: event.id.clone(),
                    device: event.device.clone(),
                    created_ms: event.created_ms,
                    parents: change.parents.clone(),
                    value: change.value.clone(),
                }],
            );
            delta.entries.insert(entry.id.clone(), entry);
        }
        Ok(Some(delta))
    }
}
