use std::{collections::HashMap, fmt, sync::Arc, thread, time::Duration};

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
    #[serde(skip)]
    pub event_result_order: Vec<String>,
    #[serde(flatten)]
    pub payload: Map<String, Value>,
}

pub struct BaseEvent {
    pub inner: Mutex<BaseEventData>,
    pub completed: Event,
    runtime_eventbus_id: Mutex<Option<String>>,
}

pub type EventResultInclude = Arc<dyn Fn(Option<&Value>, &EventResult) -> bool + Send + Sync>;

#[derive(Clone, Debug, Default)]
pub struct EventWaitOptions {
    pub timeout: Option<f64>,
    pub first_result: bool,
}

pub struct EventResultOptions {
    pub raise_if_any: bool,
    pub raise_if_none: bool,
    pub include: Option<EventResultInclude>,
}

impl Clone for EventResultOptions {
    fn clone(&self) -> Self {
        Self {
            raise_if_any: self.raise_if_any,
            raise_if_none: self.raise_if_none,
            include: self.include.clone(),
        }
    }
}

impl fmt::Debug for EventResultOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventResultOptions")
            .field("raise_if_any", &self.raise_if_any)
            .field("raise_if_none", &self.raise_if_none)
            .field("include", &self.include.as_ref().map(|_| "<include>"))
            .finish()
    }
}

impl Default for EventResultOptions {
    fn default() -> Self {
        Self {
            raise_if_any: true,
            raise_if_none: false,
            include: None,
        }
    }
}

fn short_id(id: &str) -> String {
    let start = id.len().saturating_sub(4);
    id[start..].to_string()
}

fn take_from_value<T>(payload: &mut Map<String, Value>, key: &str) -> Result<Option<T>, String>
where
    T: for<'de> Deserialize<'de>,
{
    let Some(value) = payload.remove(key) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    serde_json::from_value(value)
        .map(Some)
        .map_err(|error| format!("Invalid {key}: {error}"))
}

fn take_option_from_value<T>(
    payload: &mut Map<String, Value>,
    key: &str,
) -> Result<Option<T>, String>
where
    T: for<'de> Deserialize<'de>,
{
    take_from_value(payload, key)
}

fn take_string(payload: &mut Map<String, Value>, key: &str) -> Result<Option<String>, String> {
    take_from_value(payload, key)
}

fn take_option_string(
    payload: &mut Map<String, Value>,
    key: &str,
) -> Result<Option<String>, String> {
    take_from_value(payload, key)
}

fn take_vec_string(
    payload: &mut Map<String, Value>,
    key: &str,
) -> Result<Option<Vec<String>>, String> {
    take_from_value(payload, key)
}

fn take_bool(payload: &mut Map<String, Value>, key: &str) -> Result<Option<bool>, String> {
    take_from_value(payload, key)
}

fn take_usize(payload: &mut Map<String, Value>, key: &str) -> Result<Option<usize>, String> {
    take_from_value(payload, key)
}

fn take_option_f64(payload: &mut Map<String, Value>, key: &str) -> Result<Option<f64>, String> {
    let value: Option<f64> = take_from_value(payload, key)?;
    if let Some(value) = value {
        if value < 0.0 {
            return Err(format!("Invalid {key}: must be >= 0 or null"));
        }
    }
    Ok(value)
}

fn take_option_value(payload: &mut Map<String, Value>, key: &str) -> Option<Value> {
    match payload.remove(key) {
        Some(Value::Null) | None => None,
        Some(value) => Some(value),
    }
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
        let mut payload = payload;
        let event_type = payload
            .remove("event_type")
            .and_then(|value| serde_json::from_value::<String>(value).ok())
            .unwrap_or_else(|| event_type.into());
        Self::validate_event_type(&event_type)?;

        let event_version =
            take_string(&mut payload, "event_version")?.unwrap_or_else(|| "0.0.1".to_string());
        let event_timeout = take_option_f64(&mut payload, "event_timeout")?;
        let event_slow_timeout = take_option_f64(&mut payload, "event_slow_timeout")?;
        let event_concurrency = take_option_from_value(&mut payload, "event_concurrency")?;
        let event_handler_timeout = take_option_f64(&mut payload, "event_handler_timeout")?;
        let event_handler_slow_timeout =
            take_option_f64(&mut payload, "event_handler_slow_timeout")?;
        let event_handler_concurrency =
            take_option_from_value(&mut payload, "event_handler_concurrency")?;
        let event_handler_completion =
            take_option_from_value(&mut payload, "event_handler_completion")?;
        let event_blocks_parent_completion =
            take_bool(&mut payload, "event_blocks_parent_completion")?.unwrap_or(false);
        let event_result_type = take_option_value(&mut payload, "event_result_type");
        let event_id = take_string(&mut payload, "event_id")?.unwrap_or_else(uuid_v7_string);
        let event_path = take_vec_string(&mut payload, "event_path")?.unwrap_or_default();
        let event_parent_id = take_option_string(&mut payload, "event_parent_id")?;
        let event_emitted_by_handler_id =
            take_option_string(&mut payload, "event_emitted_by_handler_id")?;
        let event_pending_bus_count =
            take_usize(&mut payload, "event_pending_bus_count")?.unwrap_or(0);
        let event_created_at =
            take_string(&mut payload, "event_created_at")?.unwrap_or_else(now_iso);
        let event_status =
            take_from_value(&mut payload, "event_status")?.unwrap_or(EventStatus::Pending);
        let event_started_at = take_option_string(&mut payload, "event_started_at")?;
        let event_completed_at = take_option_string(&mut payload, "event_completed_at")?;
        let event_results = take_from_value(&mut payload, "event_results")?.unwrap_or_default();
        Self::validate_payload_fields(&payload)?;

        Ok(Arc::new(Self {
            inner: Mutex::new(BaseEventData {
                event_type,
                event_version,
                event_timeout,
                event_slow_timeout,
                event_concurrency,
                event_handler_timeout,
                event_handler_slow_timeout,
                event_handler_concurrency,
                event_handler_completion,
                event_blocks_parent_completion,
                event_result_type,
                event_id,
                event_path,
                event_parent_id,
                event_emitted_by_handler_id,
                event_pending_bus_count,
                event_created_at,
                event_status,
                event_started_at,
                event_completed_at,
                event_results,
                event_result_order: Vec::new(),
                payload,
            }),
            completed: Event::new(),
            runtime_eventbus_id: Mutex::new(None),
        }))
    }

    pub fn event_bus(self: &Arc<Self>) -> Option<Arc<crate::event_bus::EventBus>> {
        crate::event_bus::EventBus::event_bus_for_event(self)
    }

    pub fn bus(self: &Arc<Self>) -> Option<Arc<crate::event_bus::EventBus>> {
        self.event_bus()
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
        if first == '_' || !first.is_ascii_alphabetic() {
            return Err(format!("Invalid event name: {event_type}"));
        }
        if !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()) {
            return Err(format!("Invalid event name: {event_type}"));
        }
        Ok(())
    }

    fn validate_payload_fields(payload: &Map<String, Value>) -> Result<(), String> {
        const RESERVED_USER_EVENT_FIELDS: &[&str] = &[
            "bus", "emit", "now", "wait", "toString", "toJSON", "fromJSON",
        ];
        for key in payload.keys() {
            if RESERVED_USER_EVENT_FIELDS.contains(&key.as_str()) {
                return Err(format!(
                    "Reserved runtime field is not allowed in payload: {key}"
                ));
            }
            if key.starts_with("event_") {
                return Err(format!(
                    "Field starts with event_ but is not a recognized BaseEvent field: {key}"
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

    pub async fn now(self: &Arc<Self>) -> Result<Arc<Self>, String> {
        self.now_with_options(EventWaitOptions::default()).await
    }

    pub async fn now_with_options(
        self: &Arc<Self>,
        options: EventWaitOptions,
    ) -> Result<Arc<Self>, String> {
        self.ensure_attached_or_completed()?;
        if options.timeout.is_some_and(|timeout| timeout < 0.0) {
            return Err("timeout must be >= 0 or None".to_string());
        }
        if options.first_result {
            if self.inner.lock().event_status == EventStatus::Completed {
                return Ok(self.clone());
            }
            let event_for_queue_jump = self.clone();
            let initiating_context =
                crate::event_bus::EventBus::mark_blocks_parent_completion_if_awaited(self.clone());
            thread::spawn(move || {
                futures::executor::block_on(
                    crate::event_bus::EventBus::queue_jump_if_waited_async_with_context(
                        event_for_queue_jump,
                        initiating_context,
                    ),
                );
            });
            if !self
                .first_result_or_completion_with_timeout(options.timeout)
                .await
            {
                let event = self.inner.lock();
                return Err(format!(
                    "Timed out waiting for {} first result after {:.3}s",
                    event.event_type,
                    options.timeout.unwrap_or_default()
                ));
            }
            return Ok(self.clone());
        }
        if crate::event_bus::EventBus::is_inside_handler_context() {
            crate::event_bus::EventBus::queue_jump_if_waited_from_handler(self.clone());
        } else {
            crate::event_bus::EventBus::queue_jump_if_waited(self.clone());
        }
        if self.event_completed_with_timeout(options.timeout).await {
            Ok(self.clone())
        } else {
            let event = self.inner.lock();
            Err(format!(
                "Timed out waiting for {} completion after {:.3}s",
                event.event_type,
                options.timeout.unwrap_or_default()
            ))
        }
    }

    fn ensure_attached_or_completed(&self) -> Result<(), String> {
        let event = self.inner.lock();
        if event.event_status == EventStatus::Pending
            && event.event_path.is_empty()
            && event.event_pending_bus_count == 0
            && event.event_results.is_empty()
        {
            return Err("event has no bus attached".to_string());
        }
        Ok(())
    }

    pub async fn wait(self: &Arc<Self>) -> Result<Arc<Self>, String> {
        self.wait_with_options(EventWaitOptions::default()).await
    }

    pub async fn wait_with_options(
        self: &Arc<Self>,
        options: EventWaitOptions,
    ) -> Result<Arc<Self>, String> {
        self.ensure_attached_or_completed()?;
        if options.timeout.is_some_and(|timeout| timeout < 0.0) {
            return Err("timeout must be >= 0 or None".to_string());
        }
        let completed = if options.first_result {
            self.first_result_or_completion_with_timeout(options.timeout)
                .await
        } else {
            self.event_completed_with_timeout(options.timeout).await
        };
        if completed {
            Ok(self.clone())
        } else {
            let event = self.inner.lock();
            Err(format!(
                "Timed out waiting for {} completion after {:.3}s",
                event.event_type,
                options.timeout.unwrap_or_default()
            ))
        }
    }

    async fn wait_for_event_completed(self: &Arc<Self>) {
        loop {
            {
                let event = self.inner.lock();
                if event.event_status == EventStatus::Completed {
                    return;
                }
            }
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

    async fn event_completed_with_timeout(self: &Arc<Self>, timeout: Option<f64>) -> bool {
        let Some(timeout) = timeout else {
            self.wait_for_event_completed().await;
            return true;
        };
        if timeout <= 0.0 {
            self.wait_for_event_completed().await;
            return true;
        }

        let deadline = std::time::Instant::now() + Duration::from_secs_f64(timeout);
        loop {
            {
                let event = self.inner.lock();
                if event.event_status == EventStatus::Completed {
                    return true;
                }
            }

            let now = std::time::Instant::now();
            if now >= deadline {
                return false;
            }

            let remaining = deadline.saturating_duration_since(now);
            thread::sleep(remaining.min(Duration::from_millis(1)));
        }
    }

    async fn first_result_or_completion_with_timeout(
        self: &Arc<Self>,
        timeout: Option<f64>,
    ) -> bool {
        let deadline = timeout.and_then(|seconds| {
            if seconds <= 0.0 {
                None
            } else {
                Some(std::time::Instant::now() + Duration::from_secs_f64(seconds))
            }
        });
        loop {
            if self.has_valid_result() || self.inner.lock().event_status == EventStatus::Completed {
                return true;
            }
            if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
                return false;
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    async fn ensure_results_ready(self: &Arc<Self>, first_result: bool) -> Result<(), String> {
        let status = self.inner.lock().event_status;
        if status == EventStatus::Completed {
            return Ok(());
        }
        if status == EventStatus::Pending {
            return self
                .now_with_options(EventWaitOptions {
                    timeout: None,
                    first_result,
                })
                .await
                .map(|_| ());
        }
        Ok(())
    }

    fn has_valid_result(&self) -> bool {
        self.ordered_event_results()
            .iter()
            .any(|result| Self::default_result_include(result.result.as_ref(), result))
    }

    pub async fn event_result(self: &Arc<Self>) -> Result<Option<Value>, String> {
        self.event_result_with_options(EventResultOptions::default())
            .await
    }

    pub async fn event_result_with_options(
        self: &Arc<Self>,
        options: EventResultOptions,
    ) -> Result<Option<Value>, String> {
        self.ensure_results_ready(true).await?;
        let include = options.include.clone();
        let results = if let Some(include) = include {
            self.collect_event_results(options, move |result, event_result| {
                include(result, event_result)
            })
            .await?
        } else {
            self.collect_event_results(options, Self::default_result_include)
                .await?
        };
        Ok(results.into_iter().next())
    }

    pub async fn event_results_list(self: &Arc<Self>) -> Result<Vec<Value>, String> {
        self.event_results_list_with_options(EventResultOptions::default())
            .await
    }

    pub async fn event_results_list_with_options(
        self: &Arc<Self>,
        options: EventResultOptions,
    ) -> Result<Vec<Value>, String> {
        self.ensure_results_ready(false).await?;
        let include = options.include.clone();
        if let Some(include) = include {
            self.collect_event_results(options, move |result, event_result| {
                include(result, event_result)
            })
            .await
        } else {
            self.collect_event_results(options, Self::default_result_include)
                .await
        }
    }

    async fn collect_event_results<F>(
        self: &Arc<Self>,
        options: EventResultOptions,
        include: F,
    ) -> Result<Vec<Value>, String>
    where
        F: Fn(Option<&Value>, &EventResult) -> bool,
    {
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
            .filter(|result| {
                let value = result.result.as_ref();
                include(value, result)
            })
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

    fn default_result_include(result: Option<&Value>, event_result: &EventResult) -> bool {
        event_result.status == EventResultStatus::Completed
            && event_result.error.is_none()
            && result.is_some_and(|value| !value.is_null() && !Self::is_base_event_json(value))
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
        Self::validate_json_schema_value(&schema, &schema, &value, "$")
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

    fn validate_json_schema_value(
        root_schema: &Value,
        schema: &Value,
        value: &Value,
        path: &str,
    ) -> Result<(), String> {
        if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
            let resolved = Self::resolve_json_schema_ref(root_schema, reference)
                .ok_or_else(|| format!("{path} unresolved schema reference {reference}"))?;
            Self::validate_json_schema_value(root_schema, resolved, value, path)?;
            if schema.as_object().is_some_and(|object| object.len() == 1) {
                return Ok(());
            }
        }

        if let Some(any_of) = schema.get("anyOf").and_then(Value::as_array) {
            if any_of.iter().any(|branch| {
                Self::validate_json_schema_value(root_schema, branch, value, path).is_ok()
            }) {
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
                return Self::validate_json_schema_children(root_schema, schema, value, path);
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
        } else if (schema.get("properties").is_some()
            || schema.get("required").is_some()
            || schema.get("additionalProperties").is_some())
            && !value.is_object()
        {
            return Err(format!("{path} expected object"));
        }

        Self::validate_json_schema_children(root_schema, schema, value, path)
    }

    fn resolve_json_schema_ref<'a>(root_schema: &'a Value, reference: &str) -> Option<&'a Value> {
        let pointer = reference.strip_prefix('#')?;
        if pointer.is_empty() {
            Some(root_schema)
        } else {
            root_schema.pointer(pointer)
        }
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
        root_schema: &Value,
        schema: &Value,
        value: &Value,
        path: &str,
    ) -> Result<(), String> {
        if let Some(items_schema) = schema.get("items") {
            if let Some(items) = value.as_array() {
                for (index, item) in items.iter().enumerate() {
                    Self::validate_json_schema_value(
                        root_schema,
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
                        root_schema,
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
                        root_schema,
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

    fn sort_fallback_event_results(results: &mut [EventResult]) {
        results.sort_by(|left, right| {
            left.handler
                .handler_registered_at
                .cmp(&right.handler.handler_registered_at)
                .then_with(|| left.started_at.cmp(&right.started_at))
                .then_with(|| left.handler.id.cmp(&right.handler.id))
        });
    }

    fn ordered_event_results_from_data(event: &BaseEventData) -> Vec<EventResult> {
        let mut results = Vec::with_capacity(event.event_results.len());
        for handler_id in &event.event_result_order {
            if let Some(event_result) = event.event_results.get(handler_id) {
                results.push(event_result.clone());
            }
        }
        let mut missing_results: Vec<EventResult> = event
            .event_results
            .iter()
            .filter(|(handler_id, _)| !event.event_result_order.contains(handler_id))
            .map(|(_, event_result)| event_result.clone())
            .collect();
        Self::sort_fallback_event_results(&mut missing_results);
        results.extend(missing_results);
        results
    }

    fn ordered_event_results(&self) -> Vec<EventResult> {
        let event = self.inner.lock();
        Self::ordered_event_results_from_data(&event)
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
        data.event_result_order.clear();
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
                let results = Self::ordered_event_results_from_data(&event)
                    .into_iter()
                    .map(|result| (result.handler.id.clone(), result.to_flat_json_value()))
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
        if !event.event_result_order.contains(&handler_id) {
            event.event_result_order.push(handler_id.clone());
        }

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
        let mut event_result_order = Vec::new();
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
                    event_result_order.push(result.handler.id.clone());
                    normalized_results.insert(
                        result.handler.id.clone(),
                        serde_json::to_value(result).expect("event result serialization failed"),
                    );
                }
                object.insert(
                    "event_results".to_string(),
                    Value::Object(normalized_results),
                );
            } else if let Some(Value::Object(raw_results)) = object.get("event_results").cloned() {
                let mut normalized_results = Map::new();
                for (handler_id, raw_result) in raw_results {
                    let resolved_handler_id = raw_result
                        .as_object()
                        .and_then(|result| result.get("handler_id"))
                        .and_then(Value::as_str)
                        .map(ToString::to_string)
                        .unwrap_or(handler_id);
                    normalized_results.insert(resolved_handler_id, raw_result);
                }
                object.insert(
                    "event_results".to_string(),
                    Value::Object(normalized_results),
                );
            } else if !object.contains_key("event_results") {
                object.insert("event_results".to_string(), Value::Object(Map::new()));
            }
        }

        let mut parsed: BaseEventData =
            serde_json::from_value(value).expect("invalid base_event json");
        parsed.event_result_order = event_result_order
            .into_iter()
            .filter(|handler_id| parsed.event_results.contains_key(handler_id))
            .collect();
        let mut missing_results: Vec<EventResult> = parsed
            .event_results
            .iter()
            .filter(|(handler_id, _)| !parsed.event_result_order.contains(handler_id))
            .map(|(_, result)| result.clone())
            .collect();
        Self::sort_fallback_event_results(&mut missing_results);
        for result in missing_results {
            parsed.event_result_order.push(result.handler.id);
        }
        Arc::new(Self {
            inner: Mutex::new(parsed),
            completed: Event::new(),
            runtime_eventbus_id: Mutex::new(None),
        })
    }
}
