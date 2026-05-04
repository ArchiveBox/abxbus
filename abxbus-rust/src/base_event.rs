use std::{collections::HashMap, sync::Arc};

use event_listener::Event;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Map, Value};

use crate::{
    event_handler::EventHandler,
    event_result::{EventResult, EventResultStatus},
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
    runtime_eventbus_id: Mutex<Option<String>>,
}

#[derive(Clone, Debug)]
pub struct EventResultsOptions {
    pub raise_if_any: bool,
    pub raise_if_none: bool,
}

impl Default for EventResultsOptions {
    fn default() -> Self {
        Self {
            raise_if_any: true,
            raise_if_none: true,
        }
    }
}

fn short_id(id: &str) -> String {
    let start = id.len().saturating_sub(4);
    id[start..].to_string()
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
            runtime_eventbus_id: Mutex::new(None),
        }))
    }

    pub fn event_bus(self: &Arc<Self>) -> Option<Arc<crate::event_bus::EventBus>> {
        crate::event_bus::EventBus::event_bus_for_event(self)
    }

    pub(crate) fn set_runtime_eventbus_id(&self, eventbus_id: Option<String>) {
        *self.runtime_eventbus_id.lock() = eventbus_id;
    }

    pub(crate) fn runtime_eventbus_id(&self) -> Option<String> {
        self.runtime_eventbus_id.lock().clone()
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
        self.event_completed().await;
    }

    pub async fn event_completed(self: &Arc<Self>) {
        crate::event_bus::EventBus::mark_blocks_parent_completion_if_awaited(self.clone());
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

    pub async fn event_result(
        self: &Arc<Self>,
        options: EventResultsOptions,
    ) -> Result<Option<Value>, String> {
        self.event_result_with_filter(options, Self::default_result_include)
            .await
    }

    pub async fn event_result_with_filter<F>(
        self: &Arc<Self>,
        options: EventResultsOptions,
        include: F,
    ) -> Result<Option<Value>, String>
    where
        F: Fn(&EventResult) -> bool,
    {
        let results = self
            .event_results_list_with_filter(options, include)
            .await?;
        Ok(results.into_iter().next())
    }

    pub async fn event_results_list(
        self: &Arc<Self>,
        options: EventResultsOptions,
    ) -> Result<Vec<Value>, String> {
        self.event_results_list_with_filter(options, Self::default_result_include)
            .await
    }

    pub async fn event_results_list_with_filter<F>(
        self: &Arc<Self>,
        options: EventResultsOptions,
        include: F,
    ) -> Result<Vec<Value>, String>
    where
        F: Fn(&EventResult) -> bool,
    {
        self.event_completed().await;
        let all_results = self.ordered_event_results();
        let error_results: Vec<String> = all_results
            .iter()
            .filter_map(|event_result| event_result.error.clone())
            .collect();

        if options.raise_if_any && !error_results.is_empty() {
            if error_results.len() == 1 {
                return Err(error_results[0].clone());
            }
            let event = self.inner.lock();
            return Err(format!(
                "Event {}#{} had {} handler error(s): {}",
                event.event_type,
                short_id(&event.event_id),
                error_results.len(),
                error_results.join("; ")
            ));
        }

        let values: Vec<Value> = all_results
            .iter()
            .filter(|result| include(result))
            .filter_map(|result| result.result.clone())
            .collect();

        if options.raise_if_none && values.is_empty() {
            let event = self.inner.lock();
            return Err(format!(
                "Expected at least one handler to return a non-null result, but none did: {}#{}",
                event.event_type,
                short_id(&event.event_id)
            ));
        }

        Ok(values)
    }

    fn default_result_include(result: &EventResult) -> bool {
        result.status == EventResultStatus::Completed
            && result.error.is_none()
            && result
                .result
                .as_ref()
                .is_some_and(|value| !value.is_null() && !Self::is_base_event_json(value))
    }

    pub(crate) fn is_base_event_json(value: &Value) -> bool {
        value.as_object().is_some_and(|object| {
            object.contains_key("event_type") && object.contains_key("event_id")
        })
    }

    pub(crate) fn validate_result_value(&self, value: Value) -> Result<Value, String> {
        let schema = self.inner.lock().event_result_type.clone();
        let Some(schema) = schema else {
            return Ok(value);
        };
        if value.is_null() || Self::is_base_event_json(&value) {
            return Ok(value);
        }
        Self::validate_json_schema_value(&schema, &value, "$")
            .map(|_| value.clone())
            .map_err(|error| {
                let preview = serde_json::to_string(&value)
                    .unwrap_or_else(|_| value.to_string())
                    .chars()
                    .take(40)
                    .collect::<String>();
                format!(
                    "EventHandlerResultSchemaError: Event handler return value {preview}... did not match event_result_type: {error}"
                )
            })
    }

    fn validate_json_schema_value(schema: &Value, value: &Value, path: &str) -> Result<(), String> {
        if let Some(any_of) = schema.get("anyOf").and_then(Value::as_array) {
            if any_of
                .iter()
                .any(|branch| Self::validate_json_schema_value(branch, value, path).is_ok())
            {
                return Ok(());
            }
            return Err(format!("{path} did not match anyOf schema"));
        }

        let schema_type = schema.get("type");
        if let Some(Value::Array(types)) = schema_type {
            if types
                .iter()
                .any(|schema_type| Self::json_schema_type_matches(schema_type, value))
            {
                return Self::validate_json_schema_children(schema, value, path);
            }
            return Err(format!("{path} did not match any allowed type"));
        }

        if let Some(schema_type) = schema_type {
            if !Self::json_schema_type_matches(schema_type, value) {
                return Err(format!(
                    "{path} expected {}",
                    schema_type.as_str().unwrap_or("matching schema type")
                ));
            }
        } else if schema.get("properties").is_some()
            || schema.get("required").is_some()
            || schema.get("additionalProperties").is_some()
        {
            if !value.is_object() {
                return Err(format!("{path} expected object"));
            }
        }

        Self::validate_json_schema_children(schema, value, path)
    }

    fn json_schema_type_matches(schema_type: &Value, value: &Value) -> bool {
        match schema_type.as_str() {
            Some("string") => value.is_string(),
            Some("number") => value.is_number(),
            Some("integer") => value
                .as_f64()
                .is_some_and(|number| number.is_finite() && number.fract() == 0.0),
            Some("boolean") => value.is_boolean(),
            Some("null") => value.is_null(),
            Some("array") => value.is_array(),
            Some("object") => value.is_object(),
            _ => true,
        }
    }

    fn validate_json_schema_children(
        schema: &Value,
        value: &Value,
        path: &str,
    ) -> Result<(), String> {
        if let Some(items_schema) = schema.get("items") {
            if let Some(items) = value.as_array() {
                for (index, item) in items.iter().enumerate() {
                    Self::validate_json_schema_value(
                        items_schema,
                        item,
                        &format!("{path}[{index}]"),
                    )?;
                }
            }
        }

        let Some(object) = value.as_object() else {
            return Ok(());
        };

        if let Some(required) = schema.get("required").and_then(Value::as_array) {
            for key in required.iter().filter_map(Value::as_str) {
                if !object.contains_key(key) {
                    return Err(format!("{path}.{key} is required"));
                }
            }
        }

        let properties = schema.get("properties").and_then(Value::as_object);
        if let Some(properties) = properties {
            for (key, property_schema) in properties {
                if let Some(property_value) = object.get(key) {
                    Self::validate_json_schema_value(
                        property_schema,
                        property_value,
                        &format!("{path}.{key}"),
                    )?;
                }
            }
        }

        match schema.get("additionalProperties") {
            Some(Value::Bool(false)) => {
                if let Some(properties) = properties {
                    for key in object.keys() {
                        if !properties.contains_key(key) {
                            return Err(format!("{path}.{key} is not allowed"));
                        }
                    }
                }
            }
            Some(additional_schema @ Value::Object(_)) => {
                for (key, item) in object {
                    if properties.is_some_and(|props| props.contains_key(key)) {
                        continue;
                    }
                    Self::validate_json_schema_value(
                        additional_schema,
                        item,
                        &format!("{path}.{key}"),
                    )?;
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn ordered_event_results(&self) -> Vec<EventResult> {
        let mut results: Vec<EventResult> =
            self.inner.lock().event_results.values().cloned().collect();
        results.sort_by(|left, right| {
            left.handler
                .handler_registered_at
                .cmp(&right.handler.handler_registered_at)
                .then_with(|| left.started_at.cmp(&right.started_at))
                .then_with(|| left.handler.id.cmp(&right.handler.id))
        });
        results
    }

    pub fn mark_started(&self) {
        let mut event = self.inner.lock();
        if event.event_started_at.is_none() {
            event.event_started_at = Some(now_iso());
        }
        event.event_status = EventStatus::Started;
    }

    pub fn mark_completed(&self) {
        self.mark_completed_without_notify();
        self.notify_completed();
    }

    pub(crate) fn mark_completed_without_notify(&self) {
        let mut event = self.inner.lock();
        event.event_status = EventStatus::Completed;
        if event.event_completed_at.is_none() {
            event.event_completed_at = Some(now_iso());
        }
    }

    pub(crate) fn notify_completed(&self) {
        self.completed.notify(usize::MAX);
    }

    pub fn event_reset(&self) -> Arc<Self> {
        let mut data = self.inner.lock().clone();
        data.event_id = uuid_v7_string();
        data.event_status = EventStatus::Pending;
        data.event_started_at = None;
        data.event_completed_at = None;
        data.event_pending_bus_count = 0;
        data.event_results.clear();
        Arc::new(Self {
            inner: Mutex::new(data),
            completed: Event::new(),
            runtime_eventbus_id: Mutex::new(None),
        })
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

    pub fn event_result_update(
        &self,
        handler: &EventHandler,
        status: Option<EventResultStatus>,
        result: Option<Option<Value>>,
        error: Option<Option<String>>,
        timeout: Option<Option<f64>>,
    ) -> EventResult {
        let mut event = self.inner.lock();
        let handler_id = handler.id.clone();
        let event_id = event.event_id.clone();
        let initial_status = status.unwrap_or(EventResultStatus::Pending);
        let initial_timeout = timeout.unwrap_or(event.event_timeout);

        let (updated, updated_status, updated_started_at) = {
            let event_result = event.event_results.entry(handler_id).or_insert_with(|| {
                EventResult::construct_pending_handler_result(
                    event_id,
                    handler.clone(),
                    initial_status,
                    initial_timeout,
                )
            });
            event_result.handler = handler.clone();
            event_result.update(status, result, error);
            if let Some(timeout) = timeout {
                event_result.timeout = timeout;
            }
            (
                event_result.clone(),
                event_result.status,
                event_result.started_at.clone(),
            )
        };

        if updated_status == EventResultStatus::Started {
            if let Some(started_at) = updated_started_at {
                if event.event_started_at.is_none() {
                    event.event_started_at = Some(started_at);
                }
                event.event_status = EventStatus::Started;
            }
        }
        if matches!(
            updated_status,
            EventResultStatus::Pending | EventResultStatus::Started
        ) {
            event.event_completed_at = None;
        }

        updated
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
            runtime_eventbus_id: Mutex::new(None),
        })
    }
}
