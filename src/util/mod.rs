//! Owned by WP0.4. Frozen in Wave 0: small, real, tested building blocks shared
//! by every later package. Changes go through a CCR (REVAMP_PLAN §13).
//!
//! | module | provides |
//! |---|---|
//! | [`snapshot`] | `Snapshot<T>`: `RwLock<Arc<T>>` + generation + condvar |
//! | [`notify`] | `Notify`: a poke counter with timed waits |
//! | [`cancel`] | `Cancel`: cloneable, interruptible sleeps, kill hooks, children |
//! | [`backoff`] | `Backoff`: exponential, capped, jittered, with healthy reset |
//! | [`lock`] | poison policy, poison-tolerant locks, `Tracked<T>` hold timing |
//! | [`log`] | `event!` JSON-line logging with per-code rate limits |
//! | [`time`] | `Clock`, `SystemClock`, `ManualClock` |
//! | [`fault`] | env-driven fault injection (`TKFS_FAULT_POINT` and friends) |
//! | [`hmac`] | RFC 2104 HMAC-SHA256 and constant-time comparison |
pub mod backoff;
pub mod cancel;
pub mod fault;
pub mod hmac;
pub mod lock;
pub mod log;
pub mod notify;
pub mod snapshot;
pub mod time;

pub use backoff::Backoff;
pub use cancel::Cancel;
pub use notify::Notify;
pub use snapshot::Snapshot;
pub use time::{Clock, ManualClock, SystemClock, now_ms};
