//! The orchestrator (`hub`: one per data directory, supervising a worker per
//! folder, serving `manage` on its control `endpoint`) and the folder
//! workers. A worker owns one folder's store (and mount) and serves it to
//! local clients over a token-authenticated loopback session; it never
//! listens for peers.
pub mod catalog;
pub mod config;
pub mod control;
pub mod demand;
pub mod endpoint;
pub mod folder;
pub mod hub;
pub mod manage;
pub mod process;
pub mod settings;
pub mod signals;
pub mod sync;
pub mod worker;
pub mod worker_client;

pub use worker::run;
