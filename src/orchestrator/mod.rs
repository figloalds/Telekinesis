//! Folder lifecycle and networking belong to one daemon. Workers never dial peers.
mod api;
mod catalog_db;
mod config;
mod lanes;
mod peering;
mod run;
mod status;
mod supervisor;

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
