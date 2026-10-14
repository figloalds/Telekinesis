//! Differential oracle for derived state. The journal is authoritative; the
//! projection, conflict rows (including `resolved_by`) and the causal frontier
//! are derived. `verify_derived_state` re-derives them from scratch inside a
//! transaction with the same repair path startup uses (`rebuild_frontiers`,
//! then `rebuild` per branch, starting from an empty conflict table), compares
//! the parsed results and rolls everything back.
use super::*;

/// Differences between the stored derived state and a rebuild from the journal.
/// Every entry names the branch first. Empty means the stored state is exactly
/// what a rebuild would produce.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedDiff {
    /// `"<branch>: entry <id>"`, `"<branch>: heads <register>"`,
    /// `"<branch>: conflicts"`, `"<branch>: pending"` or a missing projection.
    pub projection_mismatch: Vec<String>,
    /// `"<branch>/<conflict id>"`: a rebuild derives it, the store lacks it.
    pub conflicts_missing: Vec<String>,
    /// `"<branch>/<conflict id>"`: stored but not derived by a rebuild, or
    /// stored with a different payload (suffix `": payload differs"`).
    pub conflicts_extra: Vec<String>,
    /// `"<branch>/<conflict id>: stored <a> rebuilt <b>"`.
    pub resolved_by_mismatch: Vec<String>,
    /// `"<branch>/<event>: missing"` or `"<branch>/<event>: extra"`.
    pub frontier_mismatch: Vec<String>,
}
impl DerivedDiff {
    pub fn is_empty(&self) -> bool {
        self.projection_mismatch.is_empty()
            && self.conflicts_missing.is_empty()
            && self.conflicts_extra.is_empty()
            && self.resolved_by_mismatch.is_empty()
            && self.frontier_mismatch.is_empty()
    }
}

/// One capture of the derived tables, parsed.
struct Derived {
    projections: BTreeMap<String, Option<Projection>>,
    /// (branch, id) -> (payload with `resolved_by` cleared, `resolved_by` column).
    conflicts: BTreeMap<(String, String), (Conflict, Option<String>)>,
    frontier: BTreeSet<(String, String)>,
}

fn capture(db: &Connection, branches: &[Branch]) -> Result<Derived> {
    let mut projections = BTreeMap::new();
    for branch in branches {
        let present: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM current_state WHERE branch=?)",
            [&branch.id],
            |r| r.get(0),
        )?;
        let projection = if present {
            Some(snapshot::load_projection(db, &branch.id)?)
        } else {
            None
        };
        projections.insert(branch.id.clone(), projection);
    }
    let mut conflicts = BTreeMap::new();
    let mut statement = db.prepare("SELECT branch,id,payload,resolved_by FROM conflicts")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        // Payload JSON may differ textually (a snapshot resume stores the
        // conflict with its `resolved_by`); compare parsed records instead.
        let mut conflict: Conflict = serde_json::from_str(&row.get::<_, String>(2)?)?;
        conflict.resolved_by = None;
        conflicts.insert((row.get(0)?, row.get(1)?), (conflict, row.get(3)?));
    }
    let frontier = db
        .prepare("SELECT branch,event FROM event_frontier")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(Derived {
        projections,
        conflicts,
        frontier,
    })
}

fn compare_projection(
    branch: &str,
    stored: Option<&Projection>,
    rebuilt: Option<&Projection>,
    out: &mut Vec<String>,
) {
    let (stored, rebuilt) = match (stored, rebuilt) {
        (Some(stored), Some(rebuilt)) => (stored, rebuilt),
        (None, None) => return,
        (None, Some(_)) => return out.push(format!("{branch}: stored projection missing")),
        (Some(_), None) => return out.push(format!("{branch}: rebuilt projection missing")),
    };
    let entities: BTreeSet<_> = stored
        .entries
        .keys()
        .chain(rebuilt.entries.keys())
        .collect();
    for entity in entities {
        if stored.entries.get(entity) != rebuilt.entries.get(entity) {
            out.push(format!("{branch}: entry {entity}"));
        }
    }
    let registers: BTreeSet<_> = stored.heads.keys().chain(rebuilt.heads.keys()).collect();
    for register in registers {
        if stored.heads.get(register) != rebuilt.heads.get(register) {
            out.push(format!("{branch}: heads {register}"));
        }
    }
    if stored.conflicts != rebuilt.conflicts {
        out.push(format!("{branch}: conflicts"));
    }
    if stored.pending != rebuilt.pending {
        out.push(format!("{branch}: pending"));
    }
}

fn compare(stored: &Derived, rebuilt: &Derived) -> DerivedDiff {
    let mut diff = DerivedDiff::default();
    let branches: BTreeSet<_> = stored
        .projections
        .keys()
        .chain(rebuilt.projections.keys())
        .collect();
    for branch in branches {
        compare_projection(
            branch,
            stored.projections.get(branch).and_then(Option::as_ref),
            rebuilt.projections.get(branch).and_then(Option::as_ref),
            &mut diff.projection_mismatch,
        );
    }
    for ((branch, id), (conflict, resolved)) in &stored.conflicts {
        match rebuilt.conflicts.get(&(branch.clone(), id.clone())) {
            None => diff.conflicts_extra.push(format!("{branch}/{id}")),
            Some((derived, derived_resolved)) => {
                if conflict != derived {
                    diff.conflicts_extra
                        .push(format!("{branch}/{id}: payload differs"));
                }
                if resolved != derived_resolved {
                    diff.resolved_by_mismatch.push(format!(
                        "{branch}/{id}: stored {resolved:?} rebuilt {derived_resolved:?}"
                    ));
                }
            }
        }
    }
    for (branch, id) in rebuilt.conflicts.keys() {
        if !stored.conflicts.contains_key(&(branch.clone(), id.clone())) {
            diff.conflicts_missing.push(format!("{branch}/{id}"));
        }
    }
    for (branch, event) in rebuilt.frontier.difference(&stored.frontier) {
        diff.frontier_mismatch
            .push(format!("{branch}/{event}: missing"));
    }
    for (branch, event) in stored.frontier.difference(&rebuilt.frontier) {
        diff.frontier_mismatch
            .push(format!("{branch}/{event}: extra"));
    }
    diff
}

impl Store {
    /// Compare the stored derived state with a rebuild from the journal,
    /// without side effects: everything runs inside one transaction (or
    /// `SAVEPOINT oracle` inside a caller's transaction) that is rolled back,
    /// then the view cache is cleared. A rebuild refusal (for example
    /// `NAMESPACE_HISTORY_LIMIT`) is returned as the error: accepted history
    /// must always rebuild.
    ///
    /// The rebuild starts from no conflict rows at all: every row and every
    /// `resolved_by` must be re-derived from the journal. So excess derived
    /// state is reported too: a fabricated or stale row (register or
    /// namespace) is `conflicts_extra`, and a `resolved_by` that no review in
    /// the journal sets (or sets lower) is `resolved_by_mismatch`.
    pub fn verify_derived_state(&mut self) -> Result<DerivedDiff> {
        let branches = self.branches()?;
        // Outside a transaction, take the write lock before the first read:
        // a deferred transaction that reads and then writes fails with
        // SQLITE_BUSY_SNAPSHOT when another connection (a live worker's
        // secondary handle) commits in between.
        let outer = self.db.is_autocommit();
        self.db.execute_batch(if outer {
            "BEGIN IMMEDIATE"
        } else {
            "SAVEPOINT oracle"
        })?;
        let result = (|| -> Result<(Derived, Derived)> {
            let stored = capture(&self.db, &branches)?;
            // Conflict rows are derived state. Without this, the rebuild's
            // upsert would keep stored rows it does not derive (it deletes
            // only namespace rows) and its `resolved_by` maximum would keep a
            // stored value larger than any review's.
            self.db.execute("DELETE FROM conflicts", [])?;
            self.rebuild_frontiers()?;
            for branch in &branches {
                self.rebuild(&branch.id)?;
            }
            Ok((stored, capture(&self.db, &branches)?))
        })();
        let rollback = self.db.execute_batch(if outer {
            "ROLLBACK"
        } else {
            "ROLLBACK TO oracle; RELEASE oracle"
        });
        self.view_cache.borrow_mut().take();
        // The rebuild's own failure is the finding; a failed rollback after it
        // (for example because SQLite already rolled back) must not hide it.
        let (stored, rebuilt) = result?;
        rollback?;
        Ok(compare(&stored, &rebuilt))
    }
}
