use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use serde::{de::DeserializeOwned, Serialize};
use serde_json::{Map, Value};

use crate::types::{EventConcurrencyMode, EventHandlerCompletionMode, EventHandlerConcurrencyMode};
use crate::{
    base_event::BaseEvent,
    event_bus::EventBus,
    event_handler::{EventHandler, EventHandlerOptions},
};

pub trait EventSpec: Send + Sync + 'static {
    type Payload: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;
    type Result: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;

    const EVENT_TYPE: &'static str;
    const EVENT_VERSION: &'static str = "0.0.1";
    const EVENT_TIMEOUT: Option<f64> = None;
    const EVENT_SLOW_TIMEOUT: Option<f64> = None;
    const EVENT_CONCURRENCY: Option<EventConcurrencyMode> = None;
    const EVENT_HANDLER_TIMEOUT: Option<f64> = None;
    const EVENT_HANDLER_SLOW_TIMEOUT: Option<f64> = None;
    const EVENT_HANDLER_CONCURRENCY: Option<EventHandlerConcurrencyMode> = None;
    const EVENT_HANDLER_COMPLETION: Option<EventHandlerCompletionMode> = None;
    const EVENT_BLOCKS_PARENT_COMPLETION: bool = false;
}

#[derive(Clone)]
pub struct TypedEvent<E: EventSpec> {
    pub inner: Arc<BaseEvent>,
    marker: PhantomData<E>,
}

impl<E: EventSpec> TypedEvent<E> {
    pub fn new(payload: E::Payload) -> Self {
        let value = serde_json::to_value(payload).expect("typed payload serialization failed");
        let Value::Object(payload_map) = value else {
            panic!("typed payload must serialize to a JSON object");
        };

        let inner = BaseEvent::new(E::EVENT_TYPE, payload_map);
        {
            let mut event = inner.inner.lock();
            event.event_version = E::EVENT_VERSION.to_string();
            event.event_timeout = E::EVENT_TIMEOUT;
            event.event_slow_timeout = E::EVENT_SLOW_TIMEOUT;
            event.event_concurrency = E::EVENT_CONCURRENCY;
            event.event_handler_timeout = E::EVENT_HANDLER_TIMEOUT;
            event.event_handler_slow_timeout = E::EVENT_HANDLER_SLOW_TIMEOUT;
            event.event_handler_concurrency = E::EVENT_HANDLER_CONCURRENCY;
            event.event_handler_completion = E::EVENT_HANDLER_COMPLETION;
            event.event_blocks_parent_completion = E::EVENT_BLOCKS_PARENT_COMPLETION;
        }

        Self {
            inner,
            marker: PhantomData,
        }
    }

    pub fn from_base_event(event: Arc<BaseEvent>) -> Self {
        Self {
            inner: event,
            marker: PhantomData,
        }
    }

    pub fn payload(&self) -> E::Payload {
        let payload = self.inner.inner.lock().payload.clone();
        let value = Value::Object(payload);
        serde_json::from_value(value).expect("typed payload decode failed")
    }

    pub async fn wait_completed(&self) {
        self.inner.wait_completed().await;
    }

    pub async fn event_completed(&self) {
        self.inner.event_completed().await;
    }

    pub async fn first(&self) -> Option<E::Result> {
        self.inner.inner.lock().event_handler_completion = Some(EventHandlerCompletionMode::First);
        self.wait_completed().await;
        self.first_result()
    }

    pub fn first_result(&self) -> Option<E::Result> {
        let results: HashMap<String, crate::event_result::EventResult> =
            self.inner.inner.lock().event_results.clone();
        let mut ordered_handler_ids: Vec<String> = results.keys().cloned().collect();
        ordered_handler_ids.sort();
        for handler_id in ordered_handler_ids {
            let Some(result) = results.get(&handler_id) else {
                continue;
            };
            if result.error.is_none() {
                if let Some(value) = &result.result {
                    if value.is_null() {
                        continue;
                    }
                    let decoded: E::Result =
                        serde_json::from_value(value.clone()).expect("typed result decode failed");
                    return Some(decoded);
                }
            }
        }
        None
    }
}

impl EventBus {
    pub fn emit<E: EventSpec>(&self, event: TypedEvent<E>) -> TypedEvent<E> {
        let emitted = self.enqueue_base(event.inner.clone());
        TypedEvent::from_base_event(emitted)
    }

    pub fn emit_with_options<E: EventSpec>(
        &self,
        event: TypedEvent<E>,
        queue_jump: bool,
    ) -> TypedEvent<E> {
        let emitted = self.enqueue_base_with_options(event.inner.clone(), queue_jump);
        TypedEvent::from_base_event(emitted)
    }

    pub fn emit_child<E: EventSpec>(&self, event: TypedEvent<E>) -> TypedEvent<E> {
        let emitted = self.enqueue_child_base(event.inner.clone());
        TypedEvent::from_base_event(emitted)
    }

    pub fn emit_child_with_options<E: EventSpec>(
        &self,
        event: TypedEvent<E>,
        queue_jump: bool,
    ) -> TypedEvent<E> {
        let emitted = self.enqueue_child_base_with_options(event.inner.clone(), queue_jump);
        TypedEvent::from_base_event(emitted)
    }

    pub fn on_typed<E, F, Fut>(&self, handler_name: &str, handler_fn: F) -> EventHandler
    where
        E: EventSpec,
        F: Fn(TypedEvent<E>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<E::Result, String>> + Send + 'static,
    {
        self.on_typed_with_options::<E, _, _>(
            handler_name,
            EventHandlerOptions::default(),
            handler_fn,
        )
    }

    pub fn on_typed_with_options<E, F, Fut>(
        &self,
        handler_name: &str,
        options: EventHandlerOptions,
        handler_fn: F,
    ) -> EventHandler
    where
        E: EventSpec,
        F: Fn(TypedEvent<E>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<E::Result, String>> + Send + 'static,
    {
        self.on_with_options(E::EVENT_TYPE, handler_name, options, move |event| {
            let typed = TypedEvent::<E>::from_base_event(event);
            let fut = handler_fn(typed);
            async move {
                let result = fut.await?;
                serde_json::to_value(result).map_err(|error| error.to_string())
            }
        })
    }

    pub async fn find_typed<E: EventSpec>(
        &self,
        past: bool,
        future: Option<f64>,
    ) -> Option<TypedEvent<E>> {
        let found = self.find(E::EVENT_TYPE, past, future, None).await?;
        Some(TypedEvent::from_base_event(found))
    }
}

pub fn payload_map_from_value(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(map) => map,
        _ => panic!("typed payload must be a JSON object"),
    }
}
