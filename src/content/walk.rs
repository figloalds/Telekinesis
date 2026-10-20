//! Incremental reachability. Missing/corrupt pages leave the frontier intact;
//! callers must retain unknown reachability. No data chunks are read by a walk.
use super::{
    PinnedContent,
    local::{ObjectKey, ObjectKind},
};
use crate::model::content::{ContentKind, FANOUT, MAX_DEPTH, ManifestNode};
use anyhow::{Result, ensure};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ContentEdge {
    pub key: ObjectKey,
    pub offset: u64,
    pub len: u64,
    /// None for data, including legacy whole-file objects.
    pub level: Option<u8>,
}

impl ContentEdge {
    pub(crate) fn root(pin: &PinnedContent) -> Result<Self> {
        let reference = pin.reference();
        reference.validate()?;
        Ok(Self {
            key: ObjectKey::root(reference),
            offset: 0,
            len: reference.total_len,
            level: if reference.kind == ContentKind::Manifest {
                Some(ManifestNode::root_level(reference.total_len)?)
            } else {
                None
            },
        })
    }
}

/// One page and at most 256 authenticated outgoing edges. Byte work per batch
/// is at most one encoded node; memory is independent of file size.
#[derive(Clone, Debug)]
pub struct WalkBatch {
    pub visited: ContentEdge,
    pub children: Vec<ContentEdge>,
    pub complete: bool,
}

pub struct ContentWalk {
    pub(crate) pin: PinnedContent,
    pub(crate) frontier: Vec<ContentEdge>,
}

impl ContentWalk {
    pub(crate) fn new(pin: &PinnedContent) -> Result<Self> {
        Ok(Self {
            pin: pin.clone(),
            frontier: vec![ContentEdge::root(pin)?],
        })
    }

    pub fn reference(&self) -> &crate::model::content::FileContentRef {
        self.pin.reference()
    }

    pub fn complete(&self) -> bool {
        self.frontier.is_empty()
    }
    pub fn pending_pages(&self) -> usize {
        self.frontier.len()
    }

    pub(crate) fn advance(&mut self, children: &[ContentEdge]) -> Result<()> {
        let pages = children
            .iter()
            .filter(|edge| edge.key.kind == ObjectKind::Manifest);
        ensure!(
            self.frontier.len() - 1 + pages.clone().count() <= MAX_DEPTH as usize * FANOUT,
            "MANIFEST_FRONTIER_LIMIT"
        );
        self.frontier.pop();
        self.frontier.extend(pages.rev().cloned());
        Ok(())
    }
}
