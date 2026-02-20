use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use serde::{de::DeserializeOwned, Serialize};
use serde_json::{Map, Value};

use crate::{base_event::BaseEvent, event_bus::EventBus};

pub trait EventSpec: Send + Sync + 'static {
    type Payload: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;
    type Result: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;

    const EVENT_TYPE: &'static str;
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

        Self {
            inner: BaseEvent::new(E::EVENT_TYPE, payload_map),
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

    pub fn first_result(&self) -> Option<E::Result> {
        let results: HashMap<String, crate::event_result::EventResult> =
            self.inner.inner.lock().event_results.clone();
        for result in results.values() {
            if result.error.is_none() {
                if let Some(value) = &result.result {
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
    pub fn emit<E: EventSpec>(&self, payload: E::Payload) -> TypedEvent<E> {
        let typed_event = TypedEvent::<E>::new(payload);
        let emitted = self.emit_raw(typed_event.inner.clone());
        TypedEvent::from_base_event(emitted)
    }

    pub fn on_typed<E, F, Fut>(
        &self,
        handler_name: &str,
        handler_fn: F,
    ) -> crate::event_handler::EventHandler
    where
        E: EventSpec,
        F: Fn(TypedEvent<E>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<E::Result, String>> + Send + 'static,
    {
        self.on(E::EVENT_TYPE, handler_name, move |event| {
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
