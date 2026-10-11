pub mod backend;
pub mod core;
#[cfg(windows)]
pub mod desktop;
#[cfg(windows)]
pub mod desktop_branch;
#[cfg(windows)]
pub mod desktop_pairing;
#[cfg(windows)]
pub mod local_ipc;
#[cfg(target_os = "linux")]
#[path = "local_ipc_linux.rs"]
pub mod local_ipc;
pub mod managed_pairing;
#[cfg(windows)]
pub mod mount;
#[cfg(target_os = "linux")]
#[path = "fuse_linux.rs"]
pub mod mount;
pub mod onboarding;
pub mod orchestrator;
pub mod paired_sync;
pub mod pairing;
mod pairing_bootstrap;
pub mod pairing_code;
pub mod pairing_service;
pub mod pairing_setup;
pub mod private_storage;
pub mod protected_secret;
pub mod runtime;
pub mod staging;
mod ws_tunnel;
mod wss_transport;
