use std::{sync::Arc, time::Duration};

use futures::future::{select, Either, FutureExt};
use futures_timer::Delay;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{json, Value};

use crate::{
    base_event::{now_iso, BaseEvent},
    event_handler::EventHandler,
    id::uuid_v7_string,
};

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
        #[derive(Serialize)]
        struct EventResultSnapshot<'a> {
            id: &'a str,
            status: EventResultStatus,
            event_id: &'a str,
            handler_id: &'a str,
            handler_name: &'a str,
            handler_file_path: &'a Option<String>,
            handler_timeout: &'a Option<f64>,
            handler_slow_timeout: &'a Option<f64>,
            handler_registered_at: &'a str,
            handler_event_pattern: &'a str,
            eventbus_name: &'a str,
            eventbus_id: &'a str,
            started_at: &'a Option<String>,
            completed_at: &'a Option<String>,
            result: &'a Option<Value>,
            error: Option<Value>,
            event_children: &'a Vec<String>,
        }

        let error = self.error_json_value();
        EventResultSnapshot {
            id: &self.id,
            status: self.status,
            event_id: &self.event_id,
            handler_id: &self.handler.id,
            handler_name: &self.handler.handler_name,
            handler_file_path: &self.handler.handler_file_path,
            handler_timeout: &self.handler.handler_timeout,
            handler_slow_timeout: &self.handler.handler_slow_timeout,
            handler_registered_at: &self.handler.handler_registered_at,
            handler_event_pattern: &self.handler.event_pattern,
            eventbus_name: &self.handler.eventbus_name,
            eventbus_id: &self.handler.eventbus_id,
            started_at: &self.started_at,
            completed_at: &self.completed_at,
            result: &self.result,
            error,
            event_children: &self.event_children,
        }
        .serialize(serializer)
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

    pub fn construct_pending_handler_result(
        event_id: String,
        handler: EventHandler,
        status: EventResultStatus,
        timeout: Option<f64>,
    ) -> Self {
        let mut result = Self::new(event_id, handler, timeout);
        result.status = status;
        result
    }

    pub fn update(
        &mut self,
        status: Option<EventResultStatus>,
        result: Option<Option<Value>>,
        error: Option<Option<String>>,
    ) -> &mut Self {
        if let Some(result) = result {
            self.result = result;
            self.status = EventResultStatus::Completed;
        }
        if let Some(error) = error {
            self.error = error;
            self.status = EventResultStatus::Error;
        }
        if let Some(status) = status {
            self.status = status;
        }
        if self.status != EventResultStatus::Pending && self.started_at.is_none() {
            self.started_at = Some(now_iso());
        }
        if matches!(
            self.status,
            EventResultStatus::Completed | EventResultStatus::Error
        ) && self.completed_at.is_none()
        {
            self.completed_at = Some(now_iso());
        }
        self
    }

    pub async fn run_handler(
        &mut self,
        event: Arc<BaseEvent>,
        timeout: Option<f64>,
    ) -> Result<Value, String> {
        if self.status != EventResultStatus::Pending {
            if self.status == EventResultStatus::Error {
                return Err(self.error.clone().unwrap_or_else(|| {
                    "EventHandlerError: handler result already errored".to_string()
                }));
            }
            return Ok(self.result.clone().unwrap_or(Value::Null));
        }

        self.status = EventResultStatus::Started;
        self.started_at = Some(now_iso());

        let Some(callable) = self.handler.callable.as_ref().cloned() else {
            let error = "EventHandlerError: handler callable missing".to_string();
            self.status = EventResultStatus::Error;
            self.error = Some(error.clone());
            self.completed_at = Some(now_iso());
            return Err(error);
        };

        let call_result = if let Some(timeout_secs) = timeout {
            let timeout_duration = Duration::from_secs_f64(timeout_secs.max(0.0));
            let timeout_future = Delay::new(timeout_duration)
                .map(|_| Err::<Value, String>("EventHandlerTimeoutError: timeout".to_string()));
            match select(callable(event.clone()), timeout_future.boxed()).await {
                Either::Left((result, _timeout_future)) => result,
                Either::Right((_timeout_result, _handler_future)) => {
                    let error = "EventHandlerTimeoutError: timeout".to_string();
                    self.status = EventResultStatus::Error;
                    self.error = Some(error.clone());
                    self.completed_at = Some(now_iso());
                    return Err(error);
                }
            }
        } else {
            callable(event.clone()).await
        };

        self.completed_at = Some(now_iso());
        match call_result {
            Ok(value) => match event.validate_result_value(value) {
                Ok(value) => {
                    self.status = EventResultStatus::Completed;
                    self.result = Some(value.clone());
                    Ok(value)
                }
                Err(error) => {
                    self.status = EventResultStatus::Error;
                    self.result = None;
                    self.error = Some(error.clone());
                    Err(error)
                }
            },
            Err(error) => {
                self.status = EventResultStatus::Error;
                self.error = Some(error.clone());
                Err(error)
            }
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
            "error": self.error_json_value(),
            "event_children": self.event_children,
        })
    }

    pub fn result_type_json(&self, event: &BaseEvent) -> Option<Value> {
        event.inner.lock().event_result_type.clone()
    }

    fn error_json_value(&self) -> Option<Value> {
        self.error.as_ref().map(|raw_message| {
            let (error_type, message) = Self::error_type_and_message(raw_message);
            json!({
                "type": error_type,
                "message": message,
            })
        })
    }

    pub fn error_metadata_json(&self, event: &BaseEvent) -> Option<Value> {
        self.error.as_ref().map(|raw_message| {
            let (error_type, message) = Self::error_type_and_message(raw_message);
            let event_data = event.inner.lock();
            json!({
                "type": error_type,
                "message": message,
                "event_result_id": self.id,
                "event_id": self.event_id,
                "event_type": event_data.event_type,
                "event_timeout": event_data.event_timeout,
                "timeout_seconds": self.timeout.or(event_data.event_timeout),
                "handler_id": self.handler.id,
                "handler_name": self.handler.handler_name,
                "cause": Self::error_cause_json_value(error_type, message),
            })
        })
    }

    fn error_cause_json_value(error_type: &str, message: &str) -> Value {
        if error_type == "EventHandlerTimeoutError" {
            return json!({
                "type": "TimeoutError",
                "message": message,
            });
        }
        if error_type == "EventHandlerAbortedError" && message == "timeout" {
            return json!({
                "type": "EventHandlerTimeoutError",
                "message": message,
            });
        }
        if let Some(reason) = message
            .strip_prefix("Aborted running handler due to parent error: ")
            .or_else(|| message.strip_prefix("Cancelled pending handler due to parent error: "))
        {
            let cause_type = if reason.contains("timed out") {
                "EventHandlerTimeoutError"
            } else {
                "Error"
            };
            return json!({
                "type": cause_type,
                "message": reason,
            });
        }
        if message.contains("first() resolved") {
            return json!({
                "type": "Error",
                "message": "first() resolved",
            });
        }
        Value::Null
    }

    fn error_type_and_message(raw_message: &str) -> (&str, &str) {
        for error_type in [
            "EventHandlerAbortedError",
            "EventHandlerCancelledError",
            "EventHandlerTimeoutError",
            "EventHandlerResultSchemaError",
            "Exception",
        ] {
            let prefix = format!("{error_type}: ");
            if let Some(message) = raw_message.strip_prefix(&prefix) {
                return (error_type, message);
            }
        }
        ("Error", raw_message)
    }

    fn error_from_json_value(error: &Value) -> Option<String> {
        match error {
            Value::String(message) => Some(message.clone()),
            Value::Object(object) => {
                let message = object
                    .get("message")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
                    .unwrap_or_else(|| error.to_string());
                if let Some(error_type) = object.get("type").and_then(Value::as_str) {
                    if !error_type.is_empty() {
                        return Some(format!("{error_type}: {message}"));
                    }
                }
                Some(message)
            }
            Value::Null => None,
            other => Some(other.to_string()),
        }
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
                .unwrap_or("")
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
            error: record.get("error").and_then(Self::error_from_json_value),
            completed_at: record
                .get("completed_at")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            event_children,
        })
    }
}
