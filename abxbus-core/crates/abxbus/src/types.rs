use serde::{Deserialize, Serialize};
use std::fmt;

/// Event lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EventStatus {
    Pending,
    Started,
    Completed,
}

impl fmt::Display for EventStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EventStatus::Pending => write!(f, "pending"),
            EventStatus::Started => write!(f, "started"),
            EventStatus::Completed => write!(f, "completed"),
        }
    }
}

/// Handler result status (superset of EventStatus with error).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResultStatus {
    Pending,
    Started,
    Completed,
    Error,
}

impl fmt::Display for ResultStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResultStatus::Pending => write!(f, "pending"),
            ResultStatus::Started => write!(f, "started"),
            ResultStatus::Completed => write!(f, "completed"),
            ResultStatus::Error => write!(f, "error"),
        }
    }
}

/// Event-level concurrency mode controlling how events are scheduled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventConcurrencyMode {
    /// All buses share one global lock — strict global FIFO.
    #[serde(rename = "global-serial")]
    GlobalSerial,
    /// Each bus has its own lock — strict per-bus FIFO (default).
    #[serde(rename = "bus-serial")]
    BusSerial,
    /// No locking — events process concurrently.
    #[serde(rename = "parallel")]
    Parallel,
}

impl Default for EventConcurrencyMode {
    fn default() -> Self {
        EventConcurrencyMode::BusSerial
    }
}

impl fmt::Display for EventConcurrencyMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EventConcurrencyMode::GlobalSerial => write!(f, "global-serial"),
            EventConcurrencyMode::BusSerial => write!(f, "bus-serial"),
            EventConcurrencyMode::Parallel => write!(f, "parallel"),
        }
    }
}

/// Handler-level concurrency mode controlling how handlers for one event run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EventHandlerConcurrencyMode {
    /// Handlers run one at a time per event (default).
    Serial,
    /// All handlers run concurrently per event.
    Parallel,
}

impl Default for EventHandlerConcurrencyMode {
    fn default() -> Self {
        EventHandlerConcurrencyMode::Serial
    }
}

impl fmt::Display for EventHandlerConcurrencyMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EventHandlerConcurrencyMode::Serial => write!(f, "serial"),
            EventHandlerConcurrencyMode::Parallel => write!(f, "parallel"),
        }
    }
}

/// Handler completion mode controlling when an event is considered done.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EventHandlerCompletionMode {
    /// Wait for all handlers to complete (default).
    All,
    /// Return first successful non-None result, cancel others.
    First,
}

impl Default for EventHandlerCompletionMode {
    fn default() -> Self {
        EventHandlerCompletionMode::All
    }
}

impl fmt::Display for EventHandlerCompletionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EventHandlerCompletionMode::All => write!(f, "all"),
            EventHandlerCompletionMode::First => write!(f, "first"),
        }
    }
}

/// Serialized error representation for JSON transport.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedError {
    #[serde(rename = "type")]
    pub error_type: String,
    pub message: String,
}

impl SerializedError {
    pub fn new(error_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error_type: error_type.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for SerializedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", self.error_type, self.message)
    }
}

/// Reserved field names that cannot be set in event payloads.
pub const RESERVED_USER_EVENT_FIELDS: &[&str] = &["bus", "first", "toString", "toJSON", "fromJSON"];

/// Handler ID namespace for UUID v5 deterministic generation.
pub static HANDLER_ID_NAMESPACE: std::sync::LazyLock<uuid::Uuid> =
    std::sync::LazyLock::new(|| uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_DNS, b"abxbus-handler"));
