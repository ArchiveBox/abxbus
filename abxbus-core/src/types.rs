use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type BusId = String;
pub type EventId = String;
pub type HandlerId = String;
pub type InvocationId = String;
pub type ResultId = String;
pub type RouteId = String;
pub type Timestamp = String;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EventConcurrency {
    GlobalSerial,
    BusSerial,
    Parallel,
}

impl Default for EventConcurrency {
    fn default() -> Self {
        Self::BusSerial
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HandlerConcurrency {
    Serial,
    Parallel,
}

impl Default for HandlerConcurrency {
    fn default() -> Self {
        Self::Serial
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HandlerCompletion {
    All,
    First,
}

impl Default for HandlerCompletion {
    fn default() -> Self {
        Self::All
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EventStatus {
    #[default]
    Pending,
    Started,
    Completed,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RouteStatus {
    #[default]
    Pending,
    Started,
    Completed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResultStatus {
    #[default]
    Pending,
    Started,
    Completed,
    Error,
    Cancelled,
}

impl ResultStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Error | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CancelReason {
    HandlerTimeout,
    EventTimeout,
    ParentTimeout,
    FirstResultWon,
    BusShutdown,
    HostDisconnected,
    LeaseExpired,
    ExplicitCancel,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoreErrorKind {
    HandlerTimeout,
    EventTimeout,
    HandlerCancelled,
    HandlerAborted,
    HostError,
    StaleWrite,
    LeaseLost,
    SchemaError,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CoreError {
    pub kind: CoreErrorKind,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl CoreError {
    pub fn new(kind: CoreErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            details: None,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CoreErrorState {
    #[error("missing bus: {0}")]
    MissingBus(BusId),
    #[error("missing event: {0}")]
    MissingEvent(EventId),
    #[error("missing handler: {0}")]
    MissingHandler(HandlerId),
    #[error("missing result: {0}")]
    MissingResult(ResultId),
    #[error("missing route: {0}")]
    MissingRoute(RouteId),
    #[error("stale invocation outcome")]
    StaleInvocationOutcome,
    #[error("lock lease mismatch")]
    LockLeaseMismatch,
    #[error("invalid envelope: {0}")]
    InvalidEnvelope(String),
    #[error("history limit reached ({current}/{max}); set event_history.max_history_drop=true")]
    HistoryLimitReached { current: usize, max: usize },
}
