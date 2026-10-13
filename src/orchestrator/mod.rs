//! Folder lifecycle and networking belong to one daemon. Workers never dial peers.
mod api;
mod catalog_db;
mod config;
mod lanes;
mod peering;
mod run;
mod status;
mod supervisor;

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
pub mod legacy_bridge;
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

pub use api::{Action, Project, client, manage};
pub use config::{Config, Network};
pub use run::run;
pub use supervisor::{Shared, Supervisor};

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
