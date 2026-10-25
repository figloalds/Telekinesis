//! Bounded namespace invalidation history. Publication starts conservatively;
//! only a successful projection update may narrow it to affected subtrees.
use std::{
    collections::{BTreeSet, VecDeque},
    sync::{
        RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

const RECORD_LIMIT: usize = 1024;
const BYTE_LIMIT: usize = 1024 * 1024;

#[derive(Default)]
pub(crate) struct Scope {
    roots: BTreeSet<String>,
    bytes: usize,
}
impl Scope {
    pub fn add(&mut self, id: &str) -> bool {
        if self.roots.contains(id) {
            return true;
        }
        // Account conservatively for the owned string and tree node.
        let bytes = id.len().saturating_add(96);
        if bytes > BYTE_LIMIT.saturating_sub(self.bytes) {
            return false;
        }
        self.roots.insert(id.to_owned());
        self.bytes += bytes;
        true
    }
}

struct Change {
    epoch: u64,
    scope: Option<Scope>,
}
impl Change {
    fn bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.scope.as_ref().map_or(0, |scope| scope.bytes)
    }
}
#[derive(Default)]
struct History {
    changes: VecDeque<Change>,
    bytes: usize,
}
impl History {
    fn trim(&mut self) {
        while self.changes.len() > RECORD_LIMIT || self.bytes > BYTE_LIMIT {
            self.bytes -= self.changes.pop_front().unwrap().bytes();
        }
    }
}

pub(crate) struct NamespaceChanges {
    epoch: AtomicU64,
    history: RwLock<History>,
}
impl Default for NamespaceChanges {
    fn default() -> Self {
        Self {
            epoch: AtomicU64::new(1),
            history: RwLock::new(History::default()),
        }
    }
}
impl NamespaceChanges {
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }
    /// Store serializes writers. Publish before updating projections so errors
    /// and readers racing an incomplete update always require Store's lock.
    pub fn begin_global(&self) -> u64 {
        let mut history = self.history.write().unwrap();
        let epoch = self
            .epoch()
            .checked_add(1)
            .expect("namespace epoch exhausted");
        let change = Change { epoch, scope: None };
        history.bytes += change.bytes();
        history.changes.push_back(change);
        history.trim();
        self.epoch.store(epoch, Ordering::Release);
        epoch
    }
    pub fn refine(&self, epoch: u64, scope: Scope) {
        let mut history = self.history.write().unwrap();
        if let Some(change) = history.changes.back_mut()
            && change.epoch == epoch
            && change.scope.is_none()
        {
            let bytes = scope.bytes;
            change.scope = Some(scope);
            history.bytes += bytes;
            history.trim();
        }
    }
    pub fn unchanged(
        &self,
        from: u64,
        to: u64,
        entity: &str,
        ancestors: Option<&[String]>,
    ) -> bool {
        if from == to {
            return true;
        }
        let Some(ancestors) = ancestors else {
            return false;
        };
        let history = self.history.read().unwrap();
        if from > to
            || to != self.epoch()
            || history
                .changes
                .front()
                .is_none_or(|first| first.epoch > from + 1)
        {
            return false;
        }
        history
            .changes
            .iter()
            .filter(|change| change.epoch > from && change.epoch <= to)
            .all(|change| {
                change.scope.as_ref().is_some_and(|scope| {
                    !scope.roots.contains(entity)
                        && ancestors.iter().all(|id| !scope.roots.contains(id))
                })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_changes_invalidate_entities_and_descendants_with_global_fallback() {
        let changes = NamespaceChanges::default();
        let old = changes.epoch();
        let next = changes.begin_global();
        let ancestors = ["sources".into(), "root".into()];
        // A failed or unfinished update must keep the conservative record.
        assert!(!changes.unchanged(old, next, "file", Some(&ancestors)));
        let mut scope = Scope::default();
        assert!(scope.add(".git"));
        changes.refine(next, scope);
        assert!(changes.unchanged(old, next, "file", Some(&ancestors)));
        assert!(!changes.unchanged(old, next, ".git", Some(&["root".into()])));
        assert!(!changes.unchanged(old, next, "lock", Some(&[".git".into(), "root".into()])));
        assert!(!changes.unchanged(old, next, "file", None));
        let global = changes.begin_global();
        assert!(!changes.unchanged(old, global, "file", Some(&ancestors)));
        let mut stale = Scope::default();
        stale.add("unrelated");
        changes.refine(next, stale);
        assert!(!changes.unchanged(next, global, "file", Some(&ancestors)));
    }

    #[test]
    fn history_gaps_and_oversized_scopes_require_authoritative_lookup() {
        let changes = NamespaceChanges::default();
        let old = changes.epoch();
        for _ in 0..=RECORD_LIMIT {
            let epoch = changes.begin_global();
            let mut scope = Scope::default();
            scope.add("unrelated");
            changes.refine(epoch, scope);
        }
        assert!(!changes.unchanged(old, changes.epoch(), "file", Some(&[])));
        assert!(changes.unchanged(changes.epoch() - 1, changes.epoch(), "file", Some(&[])));
        let mut scope = Scope::default();
        assert!(!scope.add(&"x".repeat(BYTE_LIMIT)));
        let old = changes.epoch();
        let next = changes.begin_global();
        // Oversized builder is discarded by the caller, leaving Global.
        assert!(!changes.unchanged(old, next, "file", Some(&[])));
        let history = changes.history.read().unwrap();
        assert!(history.bytes <= BYTE_LIMIT && history.changes.len() <= RECORD_LIMIT);
    }
}
