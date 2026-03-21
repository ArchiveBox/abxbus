use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::event_handler::EventHandler;
use crate::event_result::EventResult;
use crate::monotonic_dt::monotonic_datetime;
use crate::types::*;
use crate::uuid_gen::uuid7str;

/// The base event model — equivalent to Python's BaseEvent and TS's BaseEvent.
///
/// All user-defined fields are stored in `extra_fields` as dynamic JSON values.
/// Core metadata fields are strongly typed for performance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseEvent {
    // Identity
    pub event_type: String,
    pub event_version: String,
    pub event_id: String,

    // Lifecycle
    pub event_status: EventStatus,
    pub event_created_at: String,
    pub event_started_at: Option<String>,
    pub event_completed_at: Option<String>,

    // Routing
    pub event_path: Vec<String>,
    pub event_parent_id: Option<String>,
    pub event_emitted_by_handler_id: Option<String>,
    pub event_pending_bus_count: i32,

    // Concurrency configuration (None = defer to bus default)
    pub event_timeout: Option<f64>,
    pub event_slow_timeout: Option<f64>,
    pub event_concurrency: Option<EventConcurrencyMode>,
    pub event_handler_timeout: Option<f64>,
    pub event_handler_slow_timeout: Option<f64>,
    pub event_handler_concurrency: Option<EventHandlerConcurrencyMode>,
    pub event_handler_completion: Option<EventHandlerCompletionMode>,

    /// JSON Schema for handler result validation (serialized).
    pub event_result_type: Option<serde_json::Value>,

    /// User-defined payload fields.
    #[serde(flatten)]
    pub extra_fields: HashMap<String, serde_json::Value>,

    /// Handler results indexed by handler ID.
    #[serde(skip)]
    pub event_results: IndexMap<String, EventResult>,

    /// Completion signal — notified when all handlers finish.
    #[serde(skip)]
    pub completed_signal: Arc<Notify>,

    /// Flag indicating event is fully complete.
    #[serde(skip)]
    pub is_complete: bool,
}

impl BaseEvent {
    /// Create a new event with the given type and default metadata.
    pub fn new(event_type: impl Into<String>) -> Self {
        Self {
            event_type: event_type.into(),
            event_version: "0.0.1".to_string(),
            event_id: uuid7str(),
            event_status: EventStatus::Pending,
            event_created_at: monotonic_datetime(None).unwrap_or_default(),
            event_started_at: None,
            event_completed_at: None,
            event_path: Vec::new(),
            event_parent_id: None,
            event_emitted_by_handler_id: None,
            event_pending_bus_count: 0,
            event_timeout: None,
            event_slow_timeout: None,
            event_concurrency: None,
            event_handler_timeout: None,
            event_handler_slow_timeout: None,
            event_handler_concurrency: None,
            event_handler_completion: None,
            event_result_type: None,
            extra_fields: HashMap::new(),
            event_results: IndexMap::new(),
            completed_signal: Arc::new(Notify::new()),
            is_complete: false,
        }
    }

    /// Create a new event with user-defined payload fields.
    pub fn with_fields(event_type: impl Into<String>, fields: HashMap<String, serde_json::Value>) -> Self {
        let mut event = Self::new(event_type);
        event.extra_fields = fields;
        event
    }

    /// Short ID suffix for logging.
    pub fn short_id(&self) -> &str {
        &self.event_id[self.event_id.len().saturating_sub(4)..]
    }

    /// Mark the event as started, preserving the earliest start timestamp.
    pub fn mark_started(&mut self, started_at: Option<&str>) {
        if self.event_status == EventStatus::Completed {
            return;
        }
        let resolved = started_at
            .map(|s| monotonic_datetime(Some(s)).unwrap_or_else(|_| monotonic_datetime(None).unwrap()))
            .unwrap_or_else(|| monotonic_datetime(None).unwrap());

        if self.event_started_at.is_none() || self.event_started_at.as_deref().is_some_and(|s| resolved.as_str() < s) {
            self.event_started_at = Some(resolved);
        }
        if self.event_status == EventStatus::Pending {
            self.event_status = EventStatus::Started;
        }
        self.event_completed_at = None;
        self.is_complete = false;
    }

    /// Check if all handler results are terminal (completed or error).
    pub fn all_results_terminal(&self) -> bool {
        if self.event_results.is_empty() {
            return false;
        }
        self.event_results
            .values()
            .all(|r| r.status == ResultStatus::Completed || r.status == ResultStatus::Error)
    }

    /// Mark the event as completed if all results are terminal.
    /// Returns true if the event transitioned to completed.
    pub fn mark_completed_if_ready(&mut self) -> bool {
        if self.is_complete || self.event_status == EventStatus::Completed {
            return false;
        }
        if !self.all_results_terminal() {
            return false;
        }
        // Check child events are all complete too.
        for result in self.event_results.values() {
            for child in &result.event_children {
                if child.event_status != EventStatus::Completed {
                    return false;
                }
            }
        }
        self.event_status = EventStatus::Completed;
        self.event_completed_at = Some(monotonic_datetime(None).unwrap_or_default());
        self.is_complete = true;
        self.completed_signal.notify_waiters();
        true
    }

    /// Force-complete the event (used for timeout/cancellation paths).
    pub fn force_complete(&mut self) {
        if !self.is_complete {
            self.event_status = EventStatus::Completed;
            self.event_completed_at = Some(monotonic_datetime(None).unwrap_or_default());
            self.is_complete = true;
            self.completed_signal.notify_waiters();
        }
    }

    /// Create pending EventResult placeholders for the given handlers.
    pub fn create_pending_results(
        &mut self,
        handlers: &IndexMap<String, EventHandler>,
        timeout: Option<f64>,
    ) {
        self.is_complete = false;
        self.event_completed_at = None;
        if self.event_status == EventStatus::Completed {
            self.event_status = EventStatus::Pending;
            self.event_started_at = None;
        }
        for (handler_id, handler) in handlers {
            let result = EventResult::new_pending(
                &self.event_id,
                handler.clone(),
                timeout.or(self.event_timeout),
            );
            self.event_results.insert(handler_id.clone(), result);
        }
    }

    /// Get all child events across all handler results.
    pub fn event_children(&self) -> Vec<Arc<BaseEvent>> {
        let mut children = Vec::new();
        for result in self.event_results.values() {
            children.extend(result.event_children.iter().cloned());
        }
        children
    }

    /// Get the first handler result value (if any completed successfully).
    pub fn first_result(&self) -> Option<&serde_json::Value> {
        self.event_results.values().find_map(|r| {
            if r.status == ResultStatus::Completed {
                r.result.as_ref()
            } else {
                None
            }
        })
    }

    /// Serialize to JSON dict matching Python's `model_dump(mode='json')`.
    pub fn to_json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();

        map.insert("event_type".into(), serde_json::Value::String(self.event_type.clone()));
        map.insert("event_version".into(), serde_json::Value::String(self.event_version.clone()));
        map.insert("event_id".into(), serde_json::Value::String(self.event_id.clone()));
        map.insert("event_status".into(), serde_json::Value::String(self.event_status.to_string()));
        map.insert("event_created_at".into(), serde_json::Value::String(self.event_created_at.clone()));
        map.insert(
            "event_started_at".into(),
            self.event_started_at.as_ref().map_or(serde_json::Value::Null, |s| serde_json::Value::String(s.clone())),
        );
        map.insert(
            "event_completed_at".into(),
            self.event_completed_at.as_ref().map_or(serde_json::Value::Null, |s| serde_json::Value::String(s.clone())),
        );
        map.insert(
            "event_path".into(),
            serde_json::Value::Array(self.event_path.iter().map(|s| serde_json::Value::String(s.clone())).collect()),
        );
        map.insert(
            "event_parent_id".into(),
            self.event_parent_id.as_ref().map_or(serde_json::Value::Null, |s| serde_json::Value::String(s.clone())),
        );
        map.insert(
            "event_emitted_by_handler_id".into(),
            self.event_emitted_by_handler_id
                .as_ref()
                .map_or(serde_json::Value::Null, |s| serde_json::Value::String(s.clone())),
        );
        map.insert("event_pending_bus_count".into(), serde_json::json!(self.event_pending_bus_count));

        if let Some(t) = self.event_timeout {
            map.insert("event_timeout".into(), serde_json::json!(t));
        }
        if let Some(t) = self.event_slow_timeout {
            map.insert("event_slow_timeout".into(), serde_json::json!(t));
        }
        if let Some(ref c) = self.event_concurrency {
            map.insert("event_concurrency".into(), serde_json::json!(c));
        }
        if let Some(t) = self.event_handler_timeout {
            map.insert("event_handler_timeout".into(), serde_json::json!(t));
        }
        if let Some(t) = self.event_handler_slow_timeout {
            map.insert("event_handler_slow_timeout".into(), serde_json::json!(t));
        }
        if let Some(ref c) = self.event_handler_concurrency {
            map.insert("event_handler_concurrency".into(), serde_json::json!(c));
        }
        if let Some(ref c) = self.event_handler_completion {
            map.insert("event_handler_completion".into(), serde_json::json!(c));
        }
        if let Some(ref rt) = self.event_result_type {
            map.insert("event_result_type".into(), rt.clone());
        }

        // User-defined fields
        for (key, value) in &self.extra_fields {
            map.insert(key.clone(), value.clone());
        }

        serde_json::Value::Object(map)
    }

    /// Serialize to JSON dict including event_results.
    pub fn to_json_with_results(&self) -> serde_json::Value {
        let mut json = self.to_json();
        if let serde_json::Value::Object(ref mut map) = json {
            let results: Vec<serde_json::Value> =
                self.event_results.values().map(|r| r.to_json()).collect();
            map.insert("event_results".into(), serde_json::Value::Array(results));
        }
        json
    }

    /// Create a BaseEvent from a JSON payload.
    pub fn from_json(payload: serde_json::Value) -> Result<Self, String> {
        let obj = payload
            .as_object()
            .ok_or("BaseEvent JSON must be an object")?;

        let event_type = obj
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or("UndefinedEvent")
            .to_string();

        let mut event = Self::new(event_type);

        if let Some(v) = obj.get("event_version").and_then(|v| v.as_str()) {
            event.event_version = v.to_string();
        }
        if let Some(v) = obj.get("event_id").and_then(|v| v.as_str()) {
            event.event_id = v.to_string();
        }
        if let Some(v) = obj.get("event_status").and_then(|v| v.as_str()) {
            event.event_status = match v {
                "pending" => EventStatus::Pending,
                "started" => EventStatus::Started,
                "completed" => EventStatus::Completed,
                _ => EventStatus::Pending,
            };
        }
        if let Some(v) = obj.get("event_created_at").and_then(|v| v.as_str()) {
            event.event_created_at = monotonic_datetime(Some(v)).unwrap_or_else(|_| event.event_created_at.clone());
        }
        if let Some(v) = obj.get("event_started_at").and_then(|v| v.as_str()) {
            event.event_started_at = Some(monotonic_datetime(Some(v)).unwrap_or_else(|_| v.to_string()));
        }
        if let Some(v) = obj.get("event_completed_at").and_then(|v| v.as_str()) {
            event.event_completed_at = Some(monotonic_datetime(Some(v)).unwrap_or_else(|_| v.to_string()));
        }
        if let Some(v) = obj.get("event_parent_id").and_then(|v| v.as_str()) {
            event.event_parent_id = Some(v.to_string());
        }
        if let Some(v) = obj.get("event_emitted_by_handler_id").and_then(|v| v.as_str()) {
            event.event_emitted_by_handler_id = Some(v.to_string());
        }
        if let Some(v) = obj.get("event_pending_bus_count").and_then(|v| v.as_i64()) {
            event.event_pending_bus_count = v as i32;
        }
        if let Some(arr) = obj.get("event_path").and_then(|v| v.as_array()) {
            event.event_path = arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
        }
        if let Some(v) = obj.get("event_timeout").and_then(|v| v.as_f64()) {
            event.event_timeout = Some(v);
        }
        if let Some(v) = obj.get("event_slow_timeout").and_then(|v| v.as_f64()) {
            event.event_slow_timeout = Some(v);
        }
        if let Some(v) = obj.get("event_handler_timeout").and_then(|v| v.as_f64()) {
            event.event_handler_timeout = Some(v);
        }
        if let Some(v) = obj.get("event_handler_slow_timeout").and_then(|v| v.as_f64()) {
            event.event_handler_slow_timeout = Some(v);
        }
        if let Some(v) = obj.get("event_result_type") {
            if !v.is_null() {
                event.event_result_type = Some(v.clone());
            }
        }

        // Concurrency modes
        if let Some(v) = obj.get("event_concurrency").and_then(|v| v.as_str()) {
            event.event_concurrency = match v {
                "global-serial" => Some(EventConcurrencyMode::GlobalSerial),
                "bus-serial" => Some(EventConcurrencyMode::BusSerial),
                "parallel" => Some(EventConcurrencyMode::Parallel),
                _ => None,
            };
        }
        if let Some(v) = obj.get("event_handler_concurrency").and_then(|v| v.as_str()) {
            event.event_handler_concurrency = match v {
                "serial" => Some(EventHandlerConcurrencyMode::Serial),
                "parallel" => Some(EventHandlerConcurrencyMode::Parallel),
                _ => None,
            };
        }
        if let Some(v) = obj.get("event_handler_completion").and_then(|v| v.as_str()) {
            event.event_handler_completion = match v {
                "all" => Some(EventHandlerCompletionMode::All),
                "first" => Some(EventHandlerCompletionMode::First),
                _ => None,
            };
        }

        // Collect extra fields (anything not in the known set).
        let known_fields: &[&str] = &[
            "event_type", "event_version", "event_id", "event_status",
            "event_created_at", "event_started_at", "event_completed_at",
            "event_path", "event_parent_id", "event_emitted_by_handler_id",
            "event_pending_bus_count", "event_timeout", "event_slow_timeout",
            "event_concurrency", "event_handler_timeout", "event_handler_slow_timeout",
            "event_handler_concurrency", "event_handler_completion", "event_result_type",
            "event_results",
        ];
        for (key, value) in obj {
            if !known_fields.contains(&key.as_str()) {
                event.extra_fields.insert(key.clone(), value.clone());
            }
        }

        Ok(event)
    }
}

impl fmt::Display for BaseEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let icon = if self.is_complete || self.event_status == EventStatus::Completed {
            "✅"
        } else if self.event_status == EventStatus::Started {
            "🏃"
        } else {
            "⏳"
        };
        let bus_hint = self
            .event_path
            .last()
            .map(|s| s.as_str())
            .unwrap_or("?");
        write!(f, "{}▶ {}#{} {}", bus_hint, self.event_type, self.short_id(), icon)
    }
}

impl PartialEq for BaseEvent {
    fn eq(&self, other: &Self) -> bool {
        self.event_id == other.event_id
    }
}

impl Eq for BaseEvent {}

impl std::hash::Hash for BaseEvent {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.event_id.hash(state);
    }
}
