use std::{collections::HashMap, sync::Arc};

use event_listener::Event;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Map, Value};

use crate::{
    event_result::EventResult,
    id::uuid_v7_string,
    types::{
        EventConcurrencyMode, EventHandlerCompletionMode, EventHandlerConcurrencyMode, EventStatus,
    },
};

pub fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

#[derive(Clone, Serialize, Deserialize)]
pub struct BaseEventData {
    pub event_type: String,
    pub event_version: String,
    pub event_timeout: Option<f64>,
    pub event_slow_timeout: Option<f64>,
    pub event_concurrency: Option<EventConcurrencyMode>,
    pub event_handler_timeout: Option<f64>,
    pub event_handler_slow_timeout: Option<f64>,
    pub event_handler_concurrency: Option<EventHandlerConcurrencyMode>,
    pub event_handler_completion: Option<EventHandlerCompletionMode>,
    pub event_blocks_parent_completion: bool,
    pub event_result_type: Option<Value>,
    pub event_id: String,
    pub event_path: Vec<String>,
    pub event_parent_id: Option<String>,
    pub event_emitted_by_handler_id: Option<String>,
    pub event_pending_bus_count: usize,
    pub event_created_at: String,
    pub event_status: EventStatus,
    pub event_started_at: Option<String>,
    pub event_completed_at: Option<String>,
    pub event_results: HashMap<String, EventResult>,
    #[serde(flatten)]
    pub payload: Map<String, Value>,
}

pub struct BaseEvent {
    pub inner: Mutex<BaseEventData>,
    pub completed: Event,
}

impl Serialize for BaseEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_json_value().serialize(serializer)
    }
}

impl BaseEvent {
    pub fn new(event_type: impl Into<String>, payload: Map<String, Value>) -> Arc<Self> {
        Self::try_new(event_type, payload).expect("invalid base event")
    }

    pub fn try_new(
        event_type: impl Into<String>,
        payload: Map<String, Value>,
    ) -> Result<Arc<Self>, String> {
        let event_type = event_type.into();
        Self::validate_event_type(&event_type)?;
        Self::validate_payload_fields(&payload)?;

        Ok(Arc::new(Self {
            inner: Mutex::new(BaseEventData {
                event_type,
                event_version: "0.0.1".to_string(),
                event_timeout: None,
                event_slow_timeout: None,
                event_concurrency: None,
                event_handler_timeout: None,
                event_handler_slow_timeout: None,
                event_handler_concurrency: None,
                event_handler_completion: None,
                event_blocks_parent_completion: false,
                event_result_type: None,
                event_id: uuid_v7_string(),
                event_path: vec![],
                event_parent_id: None,
                event_emitted_by_handler_id: None,
                event_pending_bus_count: 0,
                event_created_at: now_iso(),
                event_status: EventStatus::Pending,
                event_started_at: None,
                event_completed_at: None,
                event_results: HashMap::new(),
                payload,
            }),
            completed: Event::new(),
        }))
    }

    fn validate_event_type(event_type: &str) -> Result<(), String> {
        let mut chars = event_type.chars();
        let Some(first) = chars.next() else {
            return Err("Invalid event name: empty".to_string());
        };
        if first == '_' || !(first == '_' || first.is_ascii_alphabetic()) {
            return Err(format!("Invalid event name: {event_type}"));
        }
        if !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()) {
            return Err(format!("Invalid event name: {event_type}"));
        }
        Ok(())
    }

    fn validate_payload_fields(payload: &Map<String, Value>) -> Result<(), String> {
        const RESERVED_USER_EVENT_FIELDS: &[&str] =
            &["bus", "emit", "first", "toString", "toJSON", "fromJSON"];
        for key in payload.keys() {
            if RESERVED_USER_EVENT_FIELDS.contains(&key.as_str()) {
                return Err(format!(
                    "Reserved runtime field is not allowed in payload: {key}"
                ));
            }
            if key.starts_with("event_") {
                return Err(format!(
                    "Unknown event-prefixed field is not allowed in payload: {key}"
                ));
            }
            if key.starts_with("model_") {
                return Err(format!(
                    "Model-prefixed field is not allowed in payload: {key}"
                ));
            }
        }
        Ok(())
    }

    pub async fn wait_completed(self: &Arc<Self>) {
        crate::event_bus::EventBus::queue_jump_if_waited(self.clone());
        loop {
            let listener = self.completed.listen();
            {
                let event = self.inner.lock();
                if event.event_status == EventStatus::Completed {
                    return;
                }
            }
            listener.await;
        }
    }

    pub fn mark_started(&self) {
        let mut event = self.inner.lock();
        if event.event_started_at.is_none() {
            event.event_started_at = Some(now_iso());
        }
        event.event_status = EventStatus::Started;
    }

    pub fn mark_completed(&self) {
        let mut event = self.inner.lock();
        event.event_status = EventStatus::Completed;
        if event.event_completed_at.is_none() {
            event.event_completed_at = Some(now_iso());
        }
        self.completed.notify(usize::MAX);
    }

    pub fn to_json_value(&self) -> Value {
        let event = self.inner.lock();
        let mut value = serde_json::to_value(&*event).unwrap_or(Value::Null);
        if let Value::Object(ref mut object) = value {
            if event.event_results.is_empty() {
                object.remove("event_results");
            } else {
                let mut results: Vec<_> = event.event_results.iter().collect();
                results.sort_by(|left, right| {
                    left.1
                        .started_at
                        .cmp(&right.1.started_at)
                        .then_with(|| left.1.handler.id.cmp(&right.1.handler.id))
                });
                let results = results
                    .into_iter()
                    .map(|(handler_id, result)| (handler_id.clone(), result.to_flat_json_value()))
                    .collect();
                object.insert("event_results".to_string(), Value::Object(results));
            }
        }
        value
    }

    pub fn event_errors(&self) -> Vec<String> {
        self.inner
            .lock()
            .event_results
            .values()
            .filter_map(|result| result.error.clone())
            .collect()
    }

    pub fn from_json_value(mut value: Value) -> Arc<Self> {
        if let Value::Object(ref mut object) = value {
            if !object.contains_key("event_version") {
                object.insert(
                    "event_version".to_string(),
                    Value::String("0.0.1".to_string()),
                );
            }
            for key in [
                "event_timeout",
                "event_slow_timeout",
                "event_concurrency",
                "event_handler_timeout",
                "event_handler_slow_timeout",
                "event_handler_concurrency",
                "event_handler_completion",
                "event_result_type",
                "event_parent_id",
                "event_emitted_by_handler_id",
                "event_started_at",
                "event_completed_at",
            ] {
                if !object.contains_key(key) {
                    object.insert(key.to_string(), Value::Null);
                }
            }
            if !object.contains_key("event_blocks_parent_completion") {
                object.insert(
                    "event_blocks_parent_completion".to_string(),
                    Value::Bool(false),
                );
            }
            if !object.contains_key("event_path") {
                object.insert("event_path".to_string(), Value::Array(Vec::new()));
            }
            if !object.contains_key("event_pending_bus_count") {
                object.insert("event_pending_bus_count".to_string(), Value::from(0));
            }
            if !object.contains_key("event_status") {
                object.insert(
                    "event_status".to_string(),
                    Value::String("pending".to_string()),
                );
            }

            if let Some(Value::Array(raw_results)) = object.get("event_results").cloned() {
                let fallback_event_id = object
                    .get("event_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let fallback_event_type = object
                    .get("event_type")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let fallback_event_created_at = object
                    .get("event_created_at")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let mut normalized_results = Map::new();
                for raw_result in raw_results {
                    let Some(result) = EventResult::from_flat_json_value(
                        raw_result,
                        &fallback_event_id,
                        &fallback_event_type,
                        &fallback_event_created_at,
                    ) else {
                        continue;
                    };
                    normalized_results.insert(
                        result.handler.id.clone(),
                        serde_json::to_value(result).expect("event result serialization failed"),
                    );
                }
                object.insert(
                    "event_results".to_string(),
                    Value::Object(normalized_results),
                );
            } else if !object.contains_key("event_results") {
                object.insert("event_results".to_string(), Value::Object(Map::new()));
            }
        }

        let parsed: BaseEventData = serde_json::from_value(value).expect("invalid base_event json");
        Arc::new(Self {
            inner: Mutex::new(parsed),
            completed: Event::new(),
        })
    }
}
