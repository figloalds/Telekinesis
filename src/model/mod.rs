//! Owned by WP0.4. Frozen cross-package contracts (REVAMP_PLAN §3.1, §6, §7.1,
//! §8): types, traits, wire/IPC/status shapes and timing constants. A change
//! needs a CCR applied by the wave integrator (§13).
//!
//! Serde conventions: unit enums are snake_case strings; data enums are
//! internally tagged (`kind`, `state`, `event` or `cmd`), except the
//! outcome-like `ObjectOutcome`, `PageStatus` and `ProcessEvent`, which are
//! externally tagged (`"completed"`, `{"progressed": {…}}`); `MountState` is
//! flattened into `MountStatus`; every `Option` field is `#[serde(default)]`,
//! and `retry_after_ms` and the WP4.1 `token` fields are omitted when `None`.
//! Golden fixtures for every wire, IPC and status shape live in
//! `tests/fixtures/contracts/` and are checked by `tests/contracts.rs`.
pub mod content;
pub mod content_wire;
pub mod errors;
pub mod health;
pub mod ipc;
pub mod lease;
pub mod orchestrator;
pub mod protocol;
pub mod status;
pub mod workspace;

pub use errors::{ErrorClass, ErrorRecord, ErrorWire, Scope, classify, code_of};
