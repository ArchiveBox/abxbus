use serde::{ser::SerializeStruct, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{json, Value};

use crate::{event_handler::EventHandler, id::uuid_v7_string};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventResultStatus {
    Pending,
    Started,
    Completed,
    Error,
}

#[derive(Clone)]
pub struct EventResult {
    pub id: String,
    pub status: EventResultStatus,
    pub event_id: String,
    pub handler: EventHandler,
    pub timeout: Option<f64>,
    pub started_at: Option<String>,
    pub result: Option<Value>,
    pub error: Option<String>,
    pub completed_at: Option<String>,
    pub event_children: Vec<String>,
}

impl Serialize for EventResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("EventResult", 17)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("status", &self.status)?;
        state.serialize_field("event_id", &self.event_id)?;
        state.serialize_field("handler_id", &self.handler.id)?;
        state.serialize_field("handler_name", &self.handler.handler_name)?;
        state.serialize_field("handler_file_path", &self.handler.handler_file_path)?;
        state.serialize_field("handler_timeout", &self.handler.handler_timeout)?;
        state.serialize_field("handler_slow_timeout", &self.handler.handler_slow_timeout)?;
        state.serialize_field("handler_registered_at", &self.handler.handler_registered_at)?;
        state.serialize_field("handler_event_pattern", &self.handler.event_pattern)?;
        state.serialize_field("eventbus_name", &self.handler.eventbus_name)?;
        state.serialize_field("eventbus_id", &self.handler.eventbus_id)?;
        state.serialize_field("started_at", &self.started_at)?;
        state.serialize_field("completed_at", &self.completed_at)?;
        state.serialize_field("result", &self.result)?;
        state.serialize_field("error", &self.error)?;
        state.serialize_field("event_children", &self.event_children)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for EventResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        if value.get("handler").is_some() {
            #[derive(Deserialize)]
            struct NestedEventResult {
                id: String,
                status: EventResultStatus,
                event_id: String,
                handler: EventHandler,
                timeout: Option<f64>,
                started_at: Option<String>,
                result: Option<Value>,
                error: Option<String>,
                completed_at: Option<String>,
                event_children: Vec<String>,
            }

            let nested: NestedEventResult =
                serde_json::from_value(value).map_err(serde::de::Error::custom)?;
            return Ok(Self {
                id: nested.id,
                status: nested.status,
                event_id: nested.event_id,
                handler: nested.handler,
                timeout: nested.timeout,
                started_at: nested.started_at,
                result: nested.result,
                error: nested.error,
                completed_at: nested.completed_at,
                event_children: nested.event_children,
            });
        }

        Self::from_flat_json_value(value, "", "", "")
            .ok_or_else(|| serde::de::Error::custom("invalid event_result json"))
    }
}

impl EventResult {
    pub fn new(event_id: String, handler: EventHandler, timeout: Option<f64>) -> Self {
        Self {
            id: uuid_v7_string(),
            status: EventResultStatus::Pending,
            event_id,
            handler,
            timeout,
            started_at: None,
            result: None,
            error: None,
            completed_at: None,
            event_children: vec![],
        }
    }

    pub fn to_flat_json_value(&self) -> Value {
        json!({
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
            "eventbus_name": self.handler.eventbus_name,
            "eventbus_id": self.handler.eventbus_id,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "result": self.result,
            "error": self.error,
            "event_children": self.event_children,
        })
    }

    pub fn from_flat_json_value(
        value: Value,
        fallback_event_id: &str,
        fallback_event_type: &str,
        fallback_event_created_at: &str,
    ) -> Option<Self> {
        let Value::Object(record) = value else {
            return None;
        };

        let handler_id = record
            .get("handler_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if handler_id.is_empty() {
            return None;
        }

        let event_id = record
            .get("event_id")
            .and_then(Value::as_str)
            .unwrap_or(fallback_event_id)
            .to_string();
        let handler = EventHandler {
            id: handler_id,
            event_pattern: record
                .get("handler_event_pattern")
                .and_then(Value::as_str)
                .unwrap_or(fallback_event_type)
                .to_string(),
            handler_name: record
                .get("handler_name")
                .and_then(Value::as_str)
                .unwrap_or("anonymous")
                .to_string(),
            handler_file_path: record
                .get("handler_file_path")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            handler_timeout: record.get("handler_timeout").and_then(Value::as_f64),
            handler_slow_timeout: record.get("handler_slow_timeout").and_then(Value::as_f64),
            handler_registered_at: record
                .get("handler_registered_at")
                .and_then(Value::as_str)
                .unwrap_or(fallback_event_created_at)
                .to_string(),
            eventbus_name: record
                .get("eventbus_name")
                .and_then(Value::as_str)
                .unwrap_or("EventBus")
                .to_string(),
            eventbus_id: record
                .get("eventbus_id")
                .and_then(Value::as_str)
                .unwrap_or("00000000-0000-0000-0000-000000000000")
                .to_string(),
            callable: None,
        };

        let status = record
            .get("status")
            .cloned()
            .and_then(|status| serde_json::from_value(status).ok())
            .unwrap_or(EventResultStatus::Pending);
        let event_children = record
            .get("event_children")
            .and_then(Value::as_array)
            .map(|children| {
                children
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToString::to_string)
                    .collect()
            })
            .unwrap_or_default();

        Some(Self {
            id: record
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_else(|| "")
                .to_string(),
            status,
            event_id,
            handler,
            timeout: record.get("timeout").and_then(Value::as_f64),
            started_at: record
                .get("started_at")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            result: record.get("result").cloned(),
            error: record.get("error").and_then(|error| match error {
                Value::String(message) => Some(message.clone()),
                Value::Object(object) => object
                    .get("message")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
                    .or_else(|| Some(error.to_string())),
                Value::Null => None,
                other => Some(other.to_string()),
            }),
            completed_at: record
                .get("completed_at")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            event_children,
        })
    }
}
