use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::event::BaseEvent;
use crate::monotonic_dt::monotonic_datetime;
use crate::uuid_gen::compute_handler_id;

/// The return type from a handler: an optional JSON value.
pub type HandlerReturn = Option<serde_json::Value>;

/// A boxed async handler function.
/// Takes a reference to the event and returns an optional JSON value.
pub type HandlerFn = Arc<
    dyn Fn(Arc<BaseEvent>) -> Pin<Box<dyn Future<Output = HandlerReturn> + Send>>
        + Send
        + Sync,
>;

/// Serializable metadata for a registered event handler.
#[derive(Clone, Serialize, Deserialize)]
pub struct EventHandler {
    pub id: String,
    pub handler_name: String,
    pub handler_file_path: Option<String>,
    pub handler_timeout: Option<f64>,
    pub handler_slow_timeout: Option<f64>,
    pub handler_registered_at: String,
    pub event_pattern: String,
    pub eventbus_name: String,
    pub eventbus_id: String,

    /// The actual callable — excluded from serialization.
    #[serde(skip)]
    pub handler: Option<HandlerFn>,
}

impl EventHandler {
    /// Create a new handler with computed ID.
    pub fn new(
        handler_name: impl Into<String>,
        handler_file_path: Option<String>,
        event_pattern: impl Into<String>,
        eventbus_name: impl Into<String>,
        eventbus_id: impl Into<String>,
        handler: Option<HandlerFn>,
    ) -> Self {
        let handler_name = handler_name.into();
        let event_pattern = event_pattern.into();
        let eventbus_name = eventbus_name.into();
        let eventbus_id = eventbus_id.into();
        let handler_registered_at = monotonic_datetime(None).unwrap_or_default();
        let file_path_str = handler_file_path.as_deref().unwrap_or("unknown");

        let id = compute_handler_id(
            &eventbus_id,
            &handler_name,
            file_path_str,
            &handler_registered_at,
            &event_pattern,
        );

        Self {
            id,
            handler_name,
            handler_file_path: handler_file_path,
            handler_timeout: None,
            handler_slow_timeout: None,
            handler_registered_at,
            event_pattern,
            eventbus_name,
            eventbus_id,
            handler: handler,
        }
    }

    /// Create a handler with an explicit ID (for deserialization).
    pub fn with_id(
        id: impl Into<String>,
        handler_name: impl Into<String>,
        event_pattern: impl Into<String>,
        eventbus_name: impl Into<String>,
        eventbus_id: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            handler_name: handler_name.into(),
            handler_file_path: None,
            handler_timeout: None,
            handler_slow_timeout: None,
            handler_registered_at: monotonic_datetime(None).unwrap_or_default(),
            event_pattern: event_pattern.into(),
            eventbus_name: eventbus_name.into(),
            eventbus_id: eventbus_id.into(),
            handler: None,
        }
    }

    /// Short label for logging: `handler_name#abcd`.
    pub fn label(&self) -> String {
        format!("{}#{}", self.handler_name, &self.id[self.id.len().saturating_sub(4)..])
    }

    /// Bus label: `BusName#abcd`.
    pub fn eventbus_label(&self) -> String {
        format!(
            "{}#{}",
            self.eventbus_name,
            &self.eventbus_id[self.eventbus_id.len().saturating_sub(4)..]
        )
    }

    /// Recompute the handler ID from current fields.
    pub fn recompute_id(&mut self) {
        let file_path = self.handler_file_path.as_deref().unwrap_or("unknown");
        self.id = compute_handler_id(
            &self.eventbus_id,
            &self.handler_name,
            file_path,
            &self.handler_registered_at,
            &self.event_pattern,
        );
    }

    /// Serialize to JSON dict (excluding handler callable).
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id,
            "handler_name": self.handler_name,
            "handler_file_path": self.handler_file_path,
            "handler_timeout": self.handler_timeout,
            "handler_slow_timeout": self.handler_slow_timeout,
            "handler_registered_at": self.handler_registered_at,
            "event_pattern": self.event_pattern,
            "eventbus_name": self.eventbus_name,
            "eventbus_id": self.eventbus_id,
        })
    }
}

impl fmt::Display for EventHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref path) = self.handler_file_path {
            write!(f, "{}() @ {}", self.handler_name, path)
        } else {
            write!(f, "{}()", self.handler_name)
        }
    }
}

impl std::fmt::Debug for EventHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventHandler")
            .field("id", &self.id)
            .field("handler_name", &self.handler_name)
            .field("handler_file_path", &self.handler_file_path)
            .field("event_pattern", &self.event_pattern)
            .field("eventbus_name", &self.eventbus_name)
            .field("eventbus_id", &self.eventbus_id)
            .field("handler", &self.handler.as_ref().map(|_| "<fn>"))
            .finish()
    }
}

impl PartialEq for EventHandler {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for EventHandler {}
