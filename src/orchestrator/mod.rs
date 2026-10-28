//! Folder workers. A worker owns one folder's store (and mount) and serves it
//! to local clients over a token-authenticated loopback session; it never
//! listens for peers.
pub mod catalog;
pub mod config;
pub mod control;
pub mod demand;
pub mod settings;
pub mod sync;
pub mod worker;

pub use worker::run;
