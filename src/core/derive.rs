//! Deterministic derivation from the journal: projection, register and
//! namespace conflicts, namespace-history cuts and causal ordering.
use super::*;

/// Derivation cache (owned by WP1.7; empty in Wave 0).
#[derive(Default)]
pub(crate) struct Cache {}

pub(super) fn persist_projection(
    tx: &Connection,
    branch: &str,
    p: &Projection,
    events: &[Event],
    new_event: Option<&str>,
) -> Result<()> {
    tx.execute("DELETE FROM current_parts WHERE branch=?", [branch])?;
    tx.execute("INSERT INTO current_state VALUES(?,?) ON CONFLICT(branch) DO UPDATE SET payload=excluded.payload",params![branch,serde_json::to_string(p)?])?;
    // Derive historical concurrent pairs from the full activated DAG too, so a
    // fast-forward delivered before a competitor cannot hide the earlier pair.
    let mut history: BTreeMap<String, Vec<Version>> = BTreeMap::new();
    for e in events.iter().filter(|e| !p.pending.contains(&e.id)) {
        for c in &e.changes {
            history
                .entry(register(&c.entity, &c.field))
                .or_default()
                .push(Version {
                    revision: e.id.clone(),
                    device: e.device.clone(),
                    created_ms: e.created_ms,
                    parents: c.parents.clone(),
                    value: c.value.clone(),
                });
        }
    }
    let mut conflicts = p.conflicts.clone();
    if let Some(event) = events.iter().find(|e| Some(e.id.as_str()) == new_event) {
        for reviewed in &event.reviewed {
            let payload: Option<String> = tx
                .query_row(
                    "SELECT payload FROM conflicts WHERE id=? AND branch=?",
                    params![reviewed, branch],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(payload) = payload {
                conflicts.push(serde_json::from_str(&payload)?);
            }
        }
    }
    // Namespace collisions also have a deterministic historical basis. A later
    // rename arriving first must not erase evidence of concurrent same-name
    // placement. Sequential temp replacement is ordered by event context.
    let activated: Vec<_> = events
        .iter()
        .filter(|e| !p.pending.contains(&e.id))
        .collect();
    let mut locations = vec![];
    for e in &activated {
        for c in &e.changes {
            if c.field == "location" {
                locations.push((*e, c));
            }
        }
    }
    for (i, (a, ac)) in locations.iter().enumerate() {
        crate::startup::progress("rebuilding namespace conflicts");
        for (b, bc) in &locations[i + 1..] {
            if ac.entity == bc.entity
                || ac.value["parent"] != bc.value["parent"]
                || name_key(ac.value["name"].as_str().unwrap())?
                    != name_key(bc.value["name"].as_str().unwrap())?
            {
                continue;
            }
            if event_ancestors(a, events).contains(&b.id)
                || event_ancestors(b, events).contains(&a.id)
            {
                continue;
            }
            let mut entities = [ac.entity.clone(), bc.entity.clone()];
            entities.sort();
            let alternatives = [(*a, *ac), (*b, *bc)]
                .into_iter()
                .map(|(e, c)| Version {
                    revision: e.id.clone(),
                    device: e.device.clone(),
                    created_ms: e.created_ms,
                    parents: c.parents.clone(),
                    value: c.value.clone(),
                })
                .collect();
            conflicts.push(make_conflict(
                branch,
                &entities.join(","),
                "namespace",
                "same name / file-directory collision",
                alternatives,
                vec![],
            ));
        }
    }
    // Explicit reviews carry their observed cut. Recover namespace records from
    // it even when resolution was delivered before its prerequisites.
    for e in &activated {
        if !e.reviewed.is_empty() {
            let ancestors = event_ancestors(e, events);
            let cut: Vec<_> = events
                .iter()
                .filter(|prior| ancestors.contains(&prior.id))
                .cloned()
                .collect();
            conflicts.extend(project(branch, &cut)?.conflicts);
        }
    }
    // Every namespace causal cut is represented by an antichain of maximal
    // namespace-changing events. Include arbitrary-width concurrency, not just
    // pairs; content-only events add no new namespace projection. The explicit
    // budget refuses a commit before acknowledgement rather than losing history.
    conflicts.extend(namespace_history(branch, events)?);
    for (key, versions) in &history {
        crate::startup::progress("rebuilding register conflicts");
        let candidate = new_event.and_then(|id| versions.iter().position(|v| v.revision == id));
        if new_event.is_some() && candidate.is_none() {
            continue;
        }
        // The overwhelmingly common single-parent chain has no historical
        // concurrency. Detect it in O(revisions), without enumerating pairs.
        if linear_versions(versions) {
            continue;
        }
        let reach = VersionReach::new(versions);
        let (entity, field) = key.rsplit_once('/').unwrap();
        // Appending a validated event cannot change old/old register ancestry.
        // Keep their durable records and derive only new/old pairs on append.
        // Startup/repair still derives the entire historical set from the DAG.
        let pairs: Box<dyn Iterator<Item = (usize, usize)>> = if let Some(i) = candidate {
            Box::new(
                (0..versions.len())
                    .filter(move |&j| j != i)
                    .map(move |j| (i, j)),
            )
        } else {
            Box::new(
                (0..versions.len()).flat_map(|i| ((i + 1)..versions.len()).map(move |j| (i, j))),
            )
        };
        for (i, j) in pairs {
            crate::startup::progress("rebuilding register conflicts");
            let a = &versions[i];
            let b = &versions[j];
            if a.value != b.value && !reach.has(i, j) && !reach.has(j, i) {
                let mut bases: Vec<_> = versions
                    .iter()
                    .enumerate()
                    .filter(|(k, _)| reach.has(i, *k) && reach.has(j, *k))
                    .map(|(_, v)| v)
                    .cloned()
                    .collect();
                bases.sort_by(|a, b| a.revision.cmp(&b.revision));
                conflicts.push(make_conflict(
                    branch,
                    entity,
                    field,
                    "concurrent revisions",
                    vec![a.clone(), b.clone()],
                    bases,
                ));
            }
        }
    }
    let conflicts: Vec<_> = conflicts
        .into_iter()
        .map(|c| (c.id.clone(), c))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect();
    let namespace_ids: BTreeSet<_> = conflicts
        .iter()
        .filter(|c| c.field == "namespace")
        .map(|c| c.id.clone())
        .collect();
    let old: Vec<(String, String)> = tx
        .prepare("SELECT id,payload FROM conflicts WHERE branch=? AND json_extract(payload,'$.field')='namespace'")?
        .query_map([branch], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<std::result::Result<_, _>>()?;
    for (id, payload) in old {
        let c: Conflict = serde_json::from_str(&payload)?;
        if c.field == "namespace" && !namespace_ids.contains(&id) {
            tx.execute("DELETE FROM conflicts WHERE id=?", [id])?;
        }
    }
    {
        let mut insert=tx.prepare_cached("INSERT INTO conflicts(id,branch,payload) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload WHERE conflicts.payload<>excluded.payload")?;
        for c in &conflicts {
            insert.execute(params![c.id, branch, serde_json::to_string(c)?])?;
        }
    }
    for e in events {
        if !p.pending.contains(&e.id) && new_event.is_none_or(|id| id == e.id) {
            for reviewed in &e.reviewed {
                let c = conflicts
                    .iter()
                    .find(|c| &c.id == reviewed)
                    .context("INVALID_CONFLICT_REVIEW")?;
                let valid_review = if c.field == "namespace" {
                    e.changes.iter().any(|change| {
                        change.field == "location"
                            && c.entity.split(',').any(|entity| entity == change.entity)
                            && c.alternatives
                                .iter()
                                .any(|v| change.parents.contains(&v.revision))
                            && c.alternatives
                                .iter()
                                .filter(|v| {
                                    events
                                        .iter()
                                        .find(|prior| prior.id == v.revision)
                                        .is_some_and(|prior| {
                                            prior.changes.iter().any(|ch| {
                                                ch.entity == change.entity
                                                    && ch.field == "location"
                                                    && ch.value == v.value
                                            })
                                        })
                                })
                                .all(|v| change.parents.contains(&v.revision))
                    })
                } else {
                    e.changes.iter().any(|change| {
                        change.entity == c.entity
                            && change.field == c.field
                            && c.alternatives
                                .iter()
                                .all(|v| change.parents.contains(&v.revision))
                    })
                };
                ensure!(valid_review, "INVALID_CONFLICT_REVIEW");
                // Concurrent review events converge on a stable receipt as well.
                tx.execute("UPDATE conflicts SET resolved_by=CASE WHEN resolved_by IS NULL OR resolved_by < ? THEN ? ELSE resolved_by END WHERE id=? AND branch=?",params![e.id,e.id,reviewed,branch])?;
            }
        }
    }
    Ok(())
}
fn ancestors(version: &Version, versions: &[Version]) -> BTreeSet<String> {
    let index: BTreeMap<_, _> = versions.iter().map(|v| (v.revision.as_str(), v)).collect();
    let mut found = BTreeSet::new();
    let mut todo = version.parents.clone();
    while let Some(revision) = todo.pop() {
        if found.insert(revision.clone())
            && let Some(v) = index.get(revision.as_str())
        {
            todo.extend(v.parents.iter().cloned());
        }
    }
    found
}
fn event_ancestors(event: &Event, events: &[Event]) -> BTreeSet<String> {
    let index: BTreeMap<_, _> = events.iter().map(|e| (e.id.as_str(), e)).collect();
    let mut seen = BTreeSet::new();
    let mut todo = event.dependencies.clone();
    todo.extend(event.changes.iter().flat_map(|c| c.parents.iter().cloned()));
    while let Some(revision) = todo.pop() {
        if seen.insert(revision.clone())
            && let Some(e) = index.get(revision.as_str())
        {
            todo.extend(e.dependencies.iter().cloned());
            todo.extend(e.changes.iter().flat_map(|c| c.parents.iter().cloned()));
        }
    }
    seen
}
fn linear_versions(versions: &[Version]) -> bool {
    let mut children = BTreeSet::new();
    let mut roots = 0;
    for v in versions {
        if v.parents.is_empty() {
            roots += 1;
        }
        if v.parents.len() > 1 || v.parents.iter().any(|p| !children.insert(p)) {
            return false;
        }
    }
    roots == 1
}
struct VersionReach {
    rows: Vec<Vec<u64>>,
}
impl VersionReach {
    fn new(versions: &[Version]) -> Self {
        let index: BTreeMap<_, _> = versions
            .iter()
            .enumerate()
            .map(|(i, v)| (v.revision.as_str(), i))
            .collect();
        let mut children = vec![vec![]; versions.len()];
        let mut degree = vec![0; versions.len()];
        let mut parents = vec![vec![]; versions.len()];
        for (i, v) in versions.iter().enumerate() {
            for parent in &v.parents {
                if let Some(&j) = index.get(parent.as_str()) {
                    parents[i].push(j);
                    children[j].push(i);
                    degree[i] += 1;
                }
            }
        }
        let mut ready: Vec<_> = (0..versions.len()).filter(|&i| degree[i] == 0).collect();
        let mut rows = vec![vec![0; versions.len().div_ceil(64)]; versions.len()];
        while let Some(i) = ready.pop() {
            for &parent in &parents[i] {
                for word in 0..rows[i].len() {
                    rows[i][word] |= rows[parent][word];
                }
                rows[i][parent / 64] |= 1 << (parent % 64);
            }
            for &child in &children[i] {
                degree[child] -= 1;
                if degree[child] == 0 {
                    ready.push(child);
                }
            }
        }
        Self { rows }
    }
    fn has(&self, child: usize, parent: usize) -> bool {
        self.rows[child][parent / 64] & (1 << (parent % 64)) != 0
    }
}
pub(super) fn activated_ids(events: &[Event]) -> BTreeSet<String> {
    causal_order(events)
        .into_iter()
        .map(|e| e.id.clone())
        .collect()
}
pub(super) fn causal_order(events: &[Event]) -> Vec<&Event> {
    let by_id: BTreeMap<_, _> = events.iter().map(|e| (e.id.as_str(), e)).collect();
    let mut degrees = BTreeMap::new();
    let mut children: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    let mut ready = BTreeSet::new();
    for e in events {
        let parents: BTreeSet<_> = e
            .dependencies
            .iter()
            .chain(e.changes.iter().flat_map(|c| c.parents.iter()))
            .collect();
        degrees.insert(e.id.as_str(), parents.len());
        if parents.is_empty() {
            ready.insert(e.id.as_str());
        }
        for parent in parents {
            children
                .entry(parent.as_str())
                .or_default()
                .push(e.id.as_str());
        }
    }
    let mut active = Vec::new();
    while let Some(id) = ready.pop_first() {
        crate::startup::progress("validating causal history");
        active.push(by_id[id]);
        if let Some(children) = children.get(id) {
            for child in children {
                let degree = degrees.get_mut(child).unwrap();
                *degree -= 1;
                if *degree == 0 {
                    ready.insert(*child);
                }
            }
        }
    }
    active
}
fn namespace_history(branch: &str, events: &[Event]) -> Result<Vec<Conflict>> {
    let graph = FactGraph::events(events);
    let cones = NamespaceCones::new(events)?;
    let mut alive: BTreeMap<&str, Vec<(usize, &Change)>> = BTreeMap::new();
    for (i, e) in events.iter().enumerate() {
        for c in &e.changes {
            if c.field == "alive" {
                alive.entry(&c.entity).or_default().push((i, c));
            }
        }
    }
    let mut conflicts = BTreeMap::new();
    for members in cones.members() {
        crate::startup::progress("rebuilding namespace history");
        let mut relevant = BTreeSet::new();
        for (i, e) in events.iter().enumerate() {
            if e.changes.iter().any(|c| {
                members.contains(&c.entity)
                    && (["kind", "location"].contains(&c.field.as_str())
                        || (c.field == "alive"
                            && (c.value == false
                                || c.parents.iter().any(|parent| {
                                    graph.index.get(parent.as_str()).is_some_and(|&j| {
                                        events[j].changes.iter().any(|p| {
                                            p.entity == c.entity
                                                && p.field == "alive"
                                                && p.value == false
                                        })
                                    })
                                }))))
            }) {
                relevant.insert(i);
            }
        }
        let primary: Vec<_> = relevant
            .iter()
            .map(|&i| (graph.closure(i, false), graph.closure(i, true)))
            .collect();
        // Pure TRUE presence assertions cannot change namespace when there is
        // no tombstone. If tombstones exist, retain rank extrema per unbranched
        // TRUE segment and topology/false-revision causal relationship signature.
        // This preserves possible boolean winners without counting every save.
        for entity in &members {
            let Some(versions) = alive.get(entity.as_str()) else {
                continue;
            };
            let false_ids: Vec<_> = versions
                .iter()
                .filter(|(_, c)| c.value == false)
                .map(|(i, _)| *i)
                .collect();
            if false_ids.is_empty() {
                continue;
            }
            let register = FactGraph::register(events, versions);
            let false_context: Vec<_> = false_ids
                .iter()
                .map(|&i| (register.closure(i, false), register.closure(i, true)))
                .collect();
            let segments = register.segments();
            let mut groups: BTreeMap<(usize, Vec<u8>), (usize, usize)> = BTreeMap::new();
            for &(i, c) in versions {
                if c.value != true || relevant.contains(&i) {
                    continue;
                }
                let signature = primary
                    .iter()
                    .chain(&false_context)
                    .map(|(anc, desc)| {
                        if anc.contains(&i) {
                            1
                        } else if desc.contains(&i) {
                            2
                        } else {
                            0
                        }
                    })
                    .collect();
                let range = groups.entry((segments[i], signature)).or_insert((i, i));
                let rank = |j: usize| (&events[j].device, &events[j].id);
                if rank(i) < rank(range.0) {
                    range.0 = i;
                }
                if rank(i) > rank(range.1) {
                    range.1 = i;
                }
            }
            for (min, max) in groups.values() {
                relevant.extend([*min, *max]);
            }
        }
        let relevant: Vec<_> = relevant.into_iter().collect();
        let ancestors: Vec<_> = relevant.iter().map(|&i| graph.closure(i, false)).collect();
        let mut antichains: Vec<Vec<usize>> = vec![vec![]];
        let mut competing_cuts = 0;
        for (i, &event) in relevant.iter().enumerate() {
            let before = antichains.len();
            for index in 0..before {
                crate::startup::progress("replaying namespace cuts");
                if !antichains[index].iter().all(|&j| {
                    !ancestors[i].contains(&relevant[j]) && !ancestors[j].contains(&event)
                }) {
                    continue;
                }
                let mut selected = antichains[index].clone();
                selected.push(i);
                if selected.len() > 1 {
                    competing_cuts += 1;
                    ensure!(
                        competing_cuts <= MAX_NAMESPACE_CUTS,
                        "NAMESPACE_HISTORY_LIMIT: more than {MAX_NAMESPACE_CUTS} competing topology cuts in one dependency cone; commit refused without discarding conflicts"
                    );
                }
                let mut ids = BTreeSet::new();
                for &j in &selected {
                    ids.extend(&ancestors[j]);
                    ids.insert(relevant[j]);
                }
                let cut: Vec<_> = ids.into_iter().map(|j| events[j].clone()).collect();
                for c in project(branch, &cut)?.conflicts.into_iter().filter(|c| {
                    c.field == "namespace" && c.entity.split(',').any(|e| members.contains(e))
                }) {
                    conflicts.insert(c.id.clone(), c);
                }
                antichains.push(selected);
            }
        }
    }
    Ok(conflicts.into_values().collect())
}
struct FactGraph {
    index: BTreeMap<String, usize>,
    parents: Vec<Vec<usize>>,
    children: Vec<Vec<usize>>,
}
impl FactGraph {
    fn events(events: &[Event]) -> Self {
        let index: BTreeMap<_, _> = events
            .iter()
            .enumerate()
            .map(|(i, e)| (e.id.clone(), i))
            .collect();
        let parents: Vec<_> = events
            .iter()
            .map(|e| {
                e.dependencies
                    .iter()
                    .chain(e.changes.iter().flat_map(|c| c.parents.iter()))
                    .filter_map(|p| index.get(p).copied())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect()
            })
            .collect();
        Self::from_parents(index, parents)
    }
    fn register(events: &[Event], versions: &[(usize, &Change)]) -> Self {
        let index: BTreeMap<_, _> = versions
            .iter()
            .map(|(i, _)| (events[*i].id.clone(), *i))
            .collect();
        let mut parents = vec![vec![]; events.len()];
        for &(i, c) in versions {
            parents[i] = c
                .parents
                .iter()
                .filter_map(|p| index.get(p).copied())
                .collect();
        }
        let graph = Self::from_parents(index.clone(), parents);
        // Explicit review can list many ancestors as parents. Such redundant
        // edges are evidence, not new branches in an unchanged-TRUE segment.
        // Reduce only this analysis graph; immutable event parents stay intact.
        let mut degree: Vec<_> = graph.parents.iter().map(Vec::len).collect();
        let mut ready: Vec<_> = (0..degree.len()).filter(|&i| degree[i] == 0).collect();
        let mut rank = vec![0; degree.len()];
        let mut sequence = 0;
        while let Some(i) = ready.pop() {
            rank[i] = sequence;
            sequence += 1;
            for &child in &graph.children[i] {
                degree[child] -= 1;
                if degree[child] == 0 {
                    ready.push(child);
                }
            }
        }
        let mut reduced = graph.parents.clone();
        for ps in &mut reduced {
            if ps.len() < 2 {
                continue;
            }
            ps.sort_by_key(|&p| std::cmp::Reverse(rank[p]));
            let mut covered = BTreeSet::new();
            ps.retain(|&p| {
                if covered.contains(&p) {
                    false
                } else {
                    covered.extend(graph.closure(p, false));
                    true
                }
            });
        }
        Self::from_parents(index, reduced)
    }
    fn from_parents(index: BTreeMap<String, usize>, parents: Vec<Vec<usize>>) -> Self {
        let mut children = vec![vec![]; parents.len()];
        for (i, ps) in parents.iter().enumerate() {
            for &p in ps {
                children[p].push(i);
            }
        }
        Self {
            index,
            parents,
            children,
        }
    }
    fn closure(&self, id: usize, forward: bool) -> BTreeSet<usize> {
        let edges = if forward {
            &self.children
        } else {
            &self.parents
        };
        let mut found = BTreeSet::new();
        let mut todo = edges[id].clone();
        while let Some(i) = todo.pop() {
            if found.insert(i) {
                todo.extend(&edges[i]);
            }
        }
        found
    }
    fn segments(&self) -> Vec<usize> {
        let mut degree: Vec<_> = self.parents.iter().map(Vec::len).collect();
        let mut ready: Vec<_> = (0..degree.len()).filter(|&i| degree[i] == 0).collect();
        let mut segment: Vec<_> = (0..degree.len()).collect();
        while let Some(i) = ready.pop() {
            if self.parents[i].len() == 1 && self.children[self.parents[i][0]].len() == 1 {
                segment[i] = segment[self.parents[i][0]];
            }
            for &child in &self.children[i] {
                degree[child] -= 1;
                if degree[child] == 0 {
                    ready.push(child);
                }
            }
        }
        segment
    }
}
struct NamespaceCones {
    parents: BTreeMap<String, BTreeSet<String>>,
    entity_slots: BTreeMap<String, BTreeSet<(String, String)>>,
    slots: BTreeMap<(String, String), BTreeSet<String>>,
}
impl NamespaceCones {
    fn new(events: &[Event]) -> Result<Self> {
        let mut result = Self {
            parents: BTreeMap::new(),
            entity_slots: BTreeMap::new(),
            slots: BTreeMap::new(),
        };
        for e in events {
            for c in &e.changes {
                result.parents.entry(c.entity.clone()).or_default();
                if c.field == "location" {
                    let parent = c.value["parent"].as_str().unwrap();
                    if parent != ROOT {
                        result
                            .parents
                            .entry(c.entity.clone())
                            .or_default()
                            .insert(parent.to_owned());
                        result.parents.entry(parent.to_owned()).or_default();
                    }
                    let key = (
                        parent.to_owned(),
                        name_key(c.value["name"].as_str().unwrap())?,
                    );
                    result
                        .entity_slots
                        .entry(c.entity.clone())
                        .or_default()
                        .insert(key.clone());
                    result
                        .slots
                        .entry(key)
                        .or_default()
                        .insert(c.entity.clone());
                }
            }
        }
        Ok(result)
    }
    fn members(&self) -> BTreeSet<BTreeSet<String>> {
        let mut cones = BTreeSet::new();
        for entity in self.parents.keys() {
            crate::startup::progress("analyzing namespace dependencies");
            let mut members = BTreeSet::new();
            let mut visited_slots = BTreeSet::new();
            let mut pending = vec![entity.clone()];
            while let Some(next) = pending.pop() {
                if !members.insert(next.clone()) {
                    continue;
                }
                pending.extend(self.parents[&next].iter().cloned());
                if let Some(slots) = self.entity_slots.get(&next) {
                    for slot in slots {
                        if visited_slots.insert(slot.clone()) {
                            pending.extend(self.slots[slot].iter().cloned());
                        }
                    }
                }
            }
            // Ancestors and slot competitors can affect this entry's visibility.
            // Sharing an ancestor alone does not make sibling histories interact.
            cones.insert(members);
        }
        cones
    }
}

/// Complete topological activation followed by per-register ancestry. Unknown
/// parents keep the entire atomic event pending; child-before-parent is durable.
pub fn project(branch: &str, events: &[Event]) -> Result<Projection> {
    let by_id: BTreeMap<_, _> = events.iter().map(|e| (e.id.clone(), e)).collect();
    for e in events {
        for c in &e.changes {
            for parent in &c.parents {
                if let Some(parent) = by_id.get(parent) {
                    ensure!(
                        parent
                            .changes
                            .iter()
                            .any(|pc| pc.entity == c.entity && pc.field == c.field),
                        "INVALID_CAUSAL_PARENT"
                    );
                }
            }
        }
    }
    let active = activated_ids(events);
    // A known closed dependency cycle is malformed, rather than permanently pending.
    let pending: Vec<_> = by_id
        .keys()
        .filter(|e| !active.contains(*e))
        .cloned()
        .collect();
    if !pending.is_empty() {
        let has_missing = events.iter().any(|e| {
            e.dependencies.iter().any(|p| !by_id.contains_key(p))
                || e.changes
                    .iter()
                    .any(|c| c.parents.iter().any(|p| !by_id.contains_key(p)))
        });
        ensure!(has_missing, "CAUSAL_CYCLE");
    }
    let mut registers: BTreeMap<String, Vec<Version>> = BTreeMap::new();
    for e in events.iter().filter(|e| active.contains(&e.id)) {
        for c in &e.changes {
            registers
                .entry(register(&c.entity, &c.field))
                .or_default()
                .push(Version {
                    revision: e.id.clone(),
                    device: e.device.clone(),
                    created_ms: e.created_ms,
                    parents: c.parents.clone(),
                    value: c.value.clone(),
                });
        }
    }
    let mut p = Projection {
        pending,
        ..Default::default()
    };
    for (key, mut versions) in registers {
        crate::startup::progress("projecting registers");
        // All ancestors are dominated before the stable concurrent rank is used.
        let dominated: BTreeSet<_> = versions
            .iter()
            .flat_map(|v| v.parents.iter().cloned())
            .collect();
        let mut heads: Vec<_> = versions
            .iter()
            .filter(|v| !dominated.contains(&v.revision))
            .cloned()
            .collect();
        heads.sort_by(|a, b| (&a.device, &a.revision).cmp(&(&b.device, &b.revision)));
        let winner = heads.last().unwrap();
        let (entity, field) = key.rsplit_once('/').unwrap();
        let entry = p.entries.entry(entity.into()).or_insert_with(|| Entry {
            id: entity.into(),
            ..Default::default()
        });
        entry
            .revisions
            .insert(field.into(), winner.revision.clone());
        if field != "basic" {
            entry.modified_ms = entry.modified_ms.max(winner.created_ms);
        }
        match field {
            "kind" => {
                entry.kind = winner.value.as_str().unwrap().into();
                entry.created_ms = winner.created_ms;
            }
            "location" => {
                entry.parent = winner.value["parent"].as_str().unwrap().into();
                entry.name = winner.value["name"].as_str().unwrap().into();
            }
            "alive" => entry.alive = winner.value.as_bool().unwrap(),
            "content" => entry.content = winner.value.as_str().map(str::to_owned),
            "basic" => entry.basic = Some(serde_json::from_value(winner.value.clone())?),
            _ => {}
        }
        if heads.len() > 1
            && heads
                .iter()
                .map(|v| serde_json::to_string(&v.value).unwrap())
                .collect::<BTreeSet<_>>()
                .len()
                > 1
        {
            versions.sort_by(|a, b| a.revision.cmp(&b.revision));
            // Pair records have stable identity even when a third competitor is
            // learned later. No delivery-dependent subset record is persisted.
            for (i, a) in heads.iter().enumerate() {
                for b in &heads[i + 1..] {
                    if a.value != b.value {
                        let aa = ancestors(a, &versions);
                        let ba = ancestors(b, &versions);
                        let bases = versions
                            .iter()
                            .filter(|v| aa.contains(&v.revision) && ba.contains(&v.revision))
                            .cloned()
                            .collect();
                        p.conflicts.push(make_conflict(
                            branch,
                            entity,
                            field,
                            "concurrent revisions",
                            vec![a.clone(), b.clone()],
                            bases,
                        ));
                    }
                }
            }
        }
        p.heads.insert(key, heads);
    }
    // Make a deterministic, acyclic visible namespace, retaining suppressed
    // entities and their complete registers in conflicts/current-state.
    let ids: Vec<_> = p.entries.keys().cloned().collect();
    let mut slots: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    for entity in &ids {
        let e = &p.entries[entity];
        if e.alive {
            slots
                .entry((e.parent.clone(), name_key(&e.name)?))
                .or_default()
                .push(entity.clone());
        }
    }
    for members in slots.values().filter(|m| m.len() > 1) {
        let mut alternatives = vec![];
        for entity in members {
            alternatives.extend(p.heads[&register(entity, "location")].clone());
        }
        let winner = members.iter().max().unwrap();
        for entity in members {
            if entity != winner {
                p.entries.get_mut(entity).unwrap().alive = false;
            }
        }
        for (i, a) in members.iter().enumerate() {
            for b in &members[i + 1..] {
                let pair = [
                    p.heads[&register(a, "location")].clone(),
                    p.heads[&register(b, "location")].clone(),
                ]
                .concat();
                p.conflicts.push(make_conflict(
                    branch,
                    &format!("{a},{b}"),
                    "namespace",
                    "same name / file-directory collision",
                    pair,
                    vec![],
                ));
            }
        }
    }
    for entity in &ids {
        let mut cursor = entity.clone();
        let mut visited = BTreeSet::new();
        let mut invalid = false;
        while cursor != ROOT {
            if !visited.insert(cursor.clone()) {
                invalid = true;
                break;
            }
            if let Some(e) = p.entries.get(&cursor) {
                if !e.alive || (cursor != *entity && e.kind != "directory") {
                    invalid = true;
                    break;
                }
                cursor = e.parent.clone();
            } else {
                invalid = true;
                break;
            }
        }
        if invalid && p.entries[entity].alive {
            p.entries.get_mut(entity).unwrap().alive = false;
            let alternatives = p
                .heads
                .get(&register(entity, "location"))
                .cloned()
                .unwrap_or_default();
            p.conflicts.push(make_conflict(
                branch,
                entity,
                "namespace",
                "deleted/missing parent or directory cycle",
                alternatives,
                vec![],
            ));
        }
    }
    p.conflicts.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(p)
}
fn make_conflict(
    branch: &str,
    entity: &str,
    field: &str,
    reason: &str,
    mut alternatives: Vec<Version>,
    bases: Vec<Version>,
) -> Conflict {
    alternatives.sort_by(|a, b| a.revision.cmp(&b.revision));
    let revs: Vec<_> = alternatives.iter().map(|v| &v.revision).collect();
    let conflict_id = hash(&serde_json::to_vec(&(branch, entity, field, reason, revs)).unwrap());
    Conflict {
        id: conflict_id,
        branch: branch.into(),
        entity: entity.into(),
        field: field.into(),
        reason: reason.into(),
        alternatives,
        bases,
        resolved_by: None,
    }
}
