//! Folder lifecycle and networking belong to one daemon. Workers never dial peers.
mod api;
// The legacy `Supervisor` (`supervisor.rs`) and its catalog SQL
// (`catalog_db.rs`) were deleted by WP2.INT (REVAMP_PLAN §14 WP2.INT task 1);
// the daemon runs the Wave 1 actors since WP2.1.
// WP3.1 deleted the legacy (v2) network: `legacy_bridge.rs`, `lanes.rs`,
// `peering.rs` and `status.rs` (REVAMP_PLAN §14 WP3.1); the v3 peer stack
// (`peer`, `scheduler`, `meta_exchange`, `objects`, `inbound`) is the only
// network path.
mod config;
mod run;

// Seam modules landed by WP0.INT (REVAMP_PLAN §14 WP0.INT); each file names
// its owner in its first line.
pub mod catalog;
pub mod compat;
pub mod ctx;
pub mod diagnostics;
pub mod folder;
pub mod hub;
pub mod hub_rules;
pub mod inbound;
pub mod inbound_objects;
pub mod manage;
pub mod manage_v2;
pub mod meta_exchange;
pub mod objects;
pub mod peer;
pub mod process;
pub mod published;
pub mod registry;
pub mod scheduler;
pub mod shutdown;
pub mod worker_client;

pub use api::{Action, Project, client};
// CCR-WP2.2-1 (applied by WP2.INT): the client side of the management API
// where §14 names it (`orchestrator::ControlClient`, `api::format_status`).
pub use api::{ControlClient, default_deadline, format_folder, format_status, is_read_only};
pub use config::{Config, Network};
pub use run::{run, serve, serve_with};

#[cfg(unix)]
use anyhow::ensure;
use anyhow::{Context, Result};
use std::{
    fs::{File, OpenOptions},
    path::Path,
};

pub fn lock(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.share_mode(0);
    }
    let file = options
        .open(path)
        .context("ALREADY_RUNNING_OR_PATH_UNAVAILABLE")?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        ensure!(
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
            "ALREADY_RUNNING"
        );
    }
    Ok(file)
}
pub fn lock_state(path: &Path) -> Result<File> {
    lock(&path.join("owner.lock"))
}
