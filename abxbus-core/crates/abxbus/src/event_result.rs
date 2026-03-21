use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::event::BaseEvent;
use crate::event_handler::EventHandler;
use crate::monotonic_dt::monotonic_datetime;
use crate::types::*;
use crate::uuid_gen::uuid7str;

/// Individual result from a single handler execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventResult {
    pub id: String,
    pub status: ResultStatus,
    pub event_id: String,
    pub handler: EventHandler,
    pub timeout: Option<f64>,
    pub started_at: Option<String>,

    // Result fields
    pub result: Option<serde_json::Value>,
    pub error: Option<SerializedError>,
    pub completed_at: Option<String>,

    // Child events emitted during handler execution
    #[serde(skip)]
    pub event_children: Vec<Arc<BaseEvent>>,

    // Completion signal
    #[serde(skip)]
    pub completed_signal: Arc<Notify>,
}

impl EventResult {
    /// Create a new pending result for a handler.
    pub fn new_pending(event_id: &str, handler: EventHandler, timeout: Option<f64>) -> Self {
        Self {
            id: uuid7str(),
            status: ResultStatus::Pending,
            event_id: event_id.to_string(),
            handler,
            timeout,
            started_at: None,
            result: None,
            error: None,
            completed_at: None,
            event_children: Vec::new(),
            completed_signal: Arc::new(Notify::new()),
        }
    }

    // Computed properties matching Python's computed_field

    pub fn handler_id(&self) -> &str {
        &self.handler.id
    }

    pub fn handler_name(&self) -> &str {
        &self.handler.handler_name
    }

    pub fn eventbus_id(&self) -> &str {
        &self.handler.eventbus_id
    }

    pub fn eventbus_name(&self) -> &str {
        &self.handler.eventbus_name
    }

    pub fn eventbus_label(&self) -> String {
        self.handler.eventbus_label()
    }

    /// Mark this result as started.
    pub fn mark_started(&mut self) {
        self.status = ResultStatus::Started;
        self.started_at = Some(monotonic_datetime(None).unwrap_or_default());
    }

    /// Mark this result as completed with a value.
    pub fn mark_completed(&mut self, result: Option<serde_json::Value>) {
        self.status = ResultStatus::Completed;
        self.result = result;
        self.completed_at = Some(monotonic_datetime(None).unwrap_or_default());
        self.completed_signal.notify_waiters();
    }

    /// Mark this result as errored.
    pub fn mark_error(&mut self, error: SerializedError) {
        self.status = ResultStatus::Error;
        self.error = Some(error);
        self.completed_at = Some(monotonic_datetime(None).unwrap_or_default());
        self.completed_signal.notify_waiters();
    }

    /// Update the result with provided fields (mirrors Python's `update(**kwargs)`).
    pub fn update(
        &mut self,
        status: Option<ResultStatus>,
        result: Option<serde_json::Value>,
        error: Option<SerializedError>,
    ) {
        // If result is set, mark as completed
        if let Some(ref r) = result {
            self.result = Some(r.clone());
            self.status = ResultStatus::Completed;
        }

        // If error is set, mark as error
        if let Some(ref e) = error {
            self.error = Some(e.clone());
            self.status = ResultStatus::Error;
            self.result = None;
        }

        // Explicit status override
        if let Some(s) = status {
            self.status = s;
        }

        // Timestamp bookkeeping
        if self.status != ResultStatus::Pending && self.started_at.is_none() {
            self.started_at = Some(monotonic_datetime(None).unwrap_or_default());
        }
        if (self.status == ResultStatus::Completed || self.status == ResultStatus::Error)
            && self.completed_at.is_none()
        {
            self.completed_at = Some(monotonic_datetime(None).unwrap_or_default());
            self.completed_signal.notify_waiters();
        }
    }

    /// Is this result in a terminal state?
    pub fn is_terminal(&self) -> bool {
        self.status == ResultStatus::Completed || self.status == ResultStatus::Error
    }

    /// Serialize to JSON dict matching Python's flat format.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id,
            "status": self.status,
            "event_id": self.event_id,
            "handler_id": self.handler.id,
            "handler_name": self.handler.handler_name,
            "handler_file_path": self.handler.handler_file_path,
            "handler_timeout": self.handler.handler_timeout,
            "handler_slow_timeout": self.handler.handler_slow_timeout,
            "handler_registered_at": self.handler.handler_registered_at,
            "handler_event_pattern": self.handler.event_pattern,
            "eventbus_id": self.handler.eventbus_id,
            "eventbus_name": self.handler.eventbus_name,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "result": self.result,
            "error": self.error,
            "event_children": self.event_children.iter().map(|c| serde_json::Value::String(c.event_id.clone())).collect::<Vec<_>>(),
        })
    }
}

impl fmt::Display for EventResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let handler_qualname = format!("{}.{}", self.eventbus_label(), self.handler_name());
        let outcome = match self.status {
            ResultStatus::Pending => "pending".to_string(),
            ResultStatus::Started => "started".to_string(),
            ResultStatus::Error => {
                if let Some(ref e) = self.error {
                    format!("error:{}", e.error_type)
                } else {
                    "error".to_string()
                }
            }
            ResultStatus::Completed => {
                if self.result.is_none() {
                    "result:none".to_string()
                } else {
                    "result:value".to_string()
                }
            }
        };
        write!(f, "{}() -> {} ({})", handler_qualname, outcome, self.status)
    }
}

impl PartialEq for EventResult {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for EventResult {}
