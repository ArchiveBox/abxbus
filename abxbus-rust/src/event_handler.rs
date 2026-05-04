use std::{future::Future, pin::Pin, sync::Arc};

use serde::{Deserialize, Serialize};
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
    pub handler_registered_at: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EventHandler {
    pub id: String,
    pub event_pattern: String,
    pub handler_name: String,
    pub handler_file_path: Option<String>,
    pub handler_timeout: Option<f64>,
    pub handler_slow_timeout: Option<f64>,
    pub handler_registered_at: String,
    pub eventbus_name: String,
    pub eventbus_id: String,
    #[serde(skip)]
    pub callable: Option<EventHandlerCallable>,
}

impl EventHandler {
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

    pub fn from_callable_with_options(
        event_pattern: String,
        handler_name: String,
        eventbus_name: String,
        eventbus_id: String,
        callable: EventHandlerCallable,
        options: EventHandlerOptions,
    ) -> Self {
        let handler_registered_at = options
            .handler_registered_at
            .unwrap_or_else(crate::base_event::now_iso);
        let id = options.id.unwrap_or_else(|| {
            compute_handler_id(
                &eventbus_id,
                &handler_name,
                options.handler_file_path.as_deref(),
                &handler_registered_at,
                &event_pattern,
            )
        });
        Self {
            id,
            event_pattern,
            handler_name,
            handler_file_path: options.handler_file_path,
            handler_timeout: options.handler_timeout,
            handler_slow_timeout: options.handler_slow_timeout,
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
