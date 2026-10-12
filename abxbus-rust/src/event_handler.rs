use std::{future::Future, pin::Pin, sync::Arc};

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::{base_event::BaseEvent, id::compute_handler_id};

pub type HandlerFuture = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'static>>;
pub type EventHandlerCallable =
    Arc<dyn Fn(Arc<BaseEvent>) -> HandlerFuture + Send + Sync + 'static>;

#[derive(Clone, Debug, Default)]
pub struct EventHandlerOptions {
    pub id: Option<String>,
    pub handler_file_path: Option<String>,
    pub handler_timeout: Option<f64>,
    pub handler_slow_timeout: Option<f64>,
    pub handler_result_ttl: Option<f64>,
    pub handler_registered_at: Option<String>,
    pub detect_handler_file_path: Option<bool>,
}

pub(crate) fn validate_optional_seconds_at_least_minus_one(
    name: &str,
    value: Option<f64>,
) -> Result<(), String> {
    if value.is_some_and(|ttl| !ttl.is_finite() || ttl < -1.0) {
        return Err(format!("{name} must be finite and >= -1 or None"));
    }
    Ok(())
}

pub(crate) fn validate_optional_seconds_nonnegative(
    name: &str,
    value: Option<f64>,
) -> Result<(), String> {
    if value.is_some_and(|timeout| !timeout.is_finite() || timeout < 0.0) {
        return Err(format!("{name} must be finite and >= 0 or None"));
    }
    Ok(())
}

fn deserialize_handler_timeout<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<f64>::deserialize(deserializer)?;
    validate_optional_seconds_nonnegative("handler_timeout", value)
        .map_err(serde::de::Error::custom)?;
    Ok(value)
}

fn deserialize_handler_slow_timeout<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<f64>::deserialize(deserializer)?;
    validate_optional_seconds_nonnegative("handler_slow_timeout", value)
        .map_err(serde::de::Error::custom)?;
    Ok(value)
}

fn deserialize_handler_result_ttl<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<f64>::deserialize(deserializer)?;
    validate_optional_seconds_at_least_minus_one("handler_result_ttl", value)
        .map_err(serde::de::Error::custom)?;
    Ok(value)
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EventHandler {
    pub id: String,
    pub event_pattern: String,
    pub handler_name: String,
    pub handler_file_path: Option<String>,
    #[serde(default, deserialize_with = "deserialize_handler_timeout")]
    pub handler_timeout: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_handler_slow_timeout")]
    pub handler_slow_timeout: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_handler_result_ttl")]
    pub handler_result_ttl: Option<f64>,
    pub handler_registered_at: String,
    pub eventbus_name: String,
    pub eventbus_id: String,
    #[serde(skip)]
    pub callable: Option<EventHandlerCallable>,
}

impl EventHandler {
    #[track_caller]
    pub fn from_callable(
        event_pattern: String,
        handler_name: String,
        eventbus_name: String,
        eventbus_id: String,
        callable: EventHandlerCallable,
    ) -> Self {
        Self::from_callable_with_options(
            event_pattern,
            handler_name,
            eventbus_name,
            eventbus_id,
            callable,
            EventHandlerOptions::default(),
        )
    }

    #[track_caller]
    pub fn from_callable_with_options(
        event_pattern: String,
        handler_name: String,
        eventbus_name: String,
        eventbus_id: String,
        callable: EventHandlerCallable,
        options: EventHandlerOptions,
    ) -> Self {
        let mut handler_file_path = options.handler_file_path;
        if handler_file_path.is_none() && options.detect_handler_file_path.unwrap_or(true) {
            let caller = std::panic::Location::caller();
            handler_file_path = Some(format!("{}:{}", caller.file(), caller.line()));
        }
        let handler_registered_at = options
            .handler_registered_at
            .unwrap_or_else(crate::base_event::now_iso);
        let id = options.id.unwrap_or_else(|| {
            compute_handler_id(
                &eventbus_id,
                &handler_name,
                handler_file_path.as_deref(),
                &handler_registered_at,
                &event_pattern,
            )
        });
        Self {
            id,
            event_pattern,
            handler_name,
            handler_file_path,
            handler_timeout: options.handler_timeout,
            handler_slow_timeout: options.handler_slow_timeout,
            handler_result_ttl: options.handler_result_ttl,
            handler_registered_at,
            eventbus_name,
            eventbus_id,
            callable: Some(callable),
        }
    }

    pub fn to_json_value(&self) -> Value {
        serde_json::to_value(self).expect("event handler serialization failed")
    }

    pub fn from_json_value(value: Value) -> Self {
        serde_json::from_value(value).expect("invalid event_handler json")
    }
}
