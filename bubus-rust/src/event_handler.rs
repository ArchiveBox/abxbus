use std::{future::Future, pin::Pin, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{base_event::BaseEvent, id::compute_handler_id};

pub type HandlerFuture = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'static>>;
pub type EventHandlerCallable =
    Arc<dyn Fn(Arc<BaseEvent>) -> HandlerFuture + Send + Sync + 'static>;

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
        let handler_registered_at = crate::base_event::now_iso();
        let id = compute_handler_id(
            &eventbus_id,
            &handler_name,
            None,
            &handler_registered_at,
            &event_pattern,
        );
        Self {
            id,
            event_pattern,
            handler_name,
            handler_file_path: None,
            handler_timeout: None,
            handler_slow_timeout: None,
            handler_registered_at,
            eventbus_name,
            eventbus_id,
            callable: Some(callable),
        }
    }
}
