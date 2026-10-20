//! Immutable content foundation. No runtime feature activation is performed here.
pub mod local;
pub mod provider;
pub mod receipt;
pub mod scheduler;
pub mod walk;
pub mod work;

pub use local::LocalContent;
pub use provider::{Availability, ContentProvider, PinnedContent, ReadContext, SnapshotRootHandle};
pub use walk::{ContentEdge, ContentWalk, WalkBatch};
