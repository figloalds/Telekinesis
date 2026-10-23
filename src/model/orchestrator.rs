//! Session call errors shared by transport and callers.
use crate::model::errors::{ErrorClass, ErrorWire, classify, code_in};
use std::fmt;
pub mod limits {
    pub const CONTROL_HANDLERS: usize = 4;
    pub const BULK_HANDLERS: usize = 2;
    pub const HANDLER_QUEUE: usize = 16;
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallError {
    /// A complete error reply; the connection stays usable (invariant 17).
    Remote(ErrorWire),
    Timeout {
        after_ms: u64,
    },
    ConnectionLost(String),
    /// Could not reach the peer or worker (nothing was sent).
    Unavailable(String),
    /// Refused locally because a bounded queue is full (nothing was sent).
    Busy,
    Cancelled,
}

impl CallError {
    /// `Timeout | ConnectionLost`.
    pub fn is_uncertain(&self) -> bool {
        matches!(
            self,
            CallError::Timeout { .. } | CallError::ConnectionLost(_)
        )
    }

    /// The error code: the remote code; `REQUEST_TIMEOUT`; `CONNECTION_LOST`;
    /// the first code in an `Unavailable` message or `LOCAL_WORKER_UNAVAILABLE`;
    /// `BUSY`; `CANCELLED`.
    pub fn code(&self) -> String {
        match self {
            CallError::Remote(error) => error.code.clone(),
            CallError::Timeout { .. } => "REQUEST_TIMEOUT".into(),
            CallError::ConnectionLost(_) => "CONNECTION_LOST".into(),
            CallError::Unavailable(message) => code_in(message)
                .unwrap_or("LOCAL_WORKER_UNAVAILABLE")
                .to_owned(),
            CallError::Busy => "BUSY".into(),
            CallError::Cancelled => "CANCELLED".into(),
        }
    }

    pub fn class(&self) -> ErrorClass {
        match self {
            CallError::Remote(error) => error.class,
            other => classify(&other.code()),
        }
    }

    pub fn to_wire(&self) -> ErrorWire {
        match self {
            CallError::Remote(error) => error.clone(),
            other => ErrorWire::new(other.code(), other.to_string()),
        }
    }
}

impl fmt::Display for CallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallError::Remote(error) => write!(f, "{error}"),
            CallError::Timeout { after_ms } => {
                write!(f, "REQUEST_TIMEOUT: no reply after {after_ms} ms")
            }
            CallError::ConnectionLost(detail) => write!(f, "CONNECTION_LOST: {detail}"),
            CallError::Unavailable(detail) => {
                crate::model::errors::write_coded(f, &self.code(), detail)
            }
            CallError::Busy => f.write_str("BUSY: request queue full"),
            CallError::Cancelled => f.write_str("CANCELLED: call cancelled"),
        }
    }
}

impl std::error::Error for CallError {}

impl From<ErrorWire> for CallError {
    fn from(error: ErrorWire) -> Self {
        CallError::Remote(error)
    }
}
