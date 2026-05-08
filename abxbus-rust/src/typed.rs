use std::{any::TypeId, collections::HashMap, marker::PhantomData, sync::Arc};

use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Map, Value};

use crate::types::{EventConcurrencyMode, EventHandlerCompletionMode, EventHandlerConcurrencyMode};
use crate::{
    base_event::{BaseEvent as RawBaseEvent, EventResultsOptions},
    event_bus::EventBus,
    event_handler::{EventHandler, EventHandlerOptions},
};

#[allow(non_camel_case_types, non_upper_case_globals)]
pub trait EventSpec: Send + Sync + 'static {
    type payload: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;
    type event_result_type: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;

    const event_type: &'static str;
    const event_version: &'static str = "0.0.1";
    const event_timeout: Option<f64> = None;
    const event_slow_timeout: Option<f64> = None;
    const event_concurrency: Option<EventConcurrencyMode> = None;
    const event_handler_timeout: Option<f64> = None;
    const event_handler_slow_timeout: Option<f64> = None;
    const event_handler_concurrency: Option<EventHandlerConcurrencyMode> = None;
    const event_handler_completion: Option<EventHandlerCompletionMode> = None;
    const event_blocks_parent_completion: bool = false;
    const event_result_type_schema: Option<&'static str> = None;

    fn event_result_type_json() -> Option<Value> {
        if let Some(schema) = Self::event_result_type_schema {
            return Some(
                serde_json::from_str(schema)
                    .expect("event_result_type_schema must be valid JSON Schema JSON"),
            );
        }
        primitive_result_type_schema::<Self::event_result_type>()
    }
}

fn primitive_result_type_schema<T: 'static>() -> Option<Value> {
    let type_id = TypeId::of::<T>();
    if type_id == TypeId::of::<String>() {
        Some(json!({"type": "string"}))
    } else if type_id == TypeId::of::<bool>() {
        Some(json!({"type": "boolean"}))
    } else if type_id == TypeId::of::<i8>()
        || type_id == TypeId::of::<i16>()
        || type_id == TypeId::of::<i32>()
        || type_id == TypeId::of::<i64>()
        || type_id == TypeId::of::<isize>()
        || type_id == TypeId::of::<u8>()
        || type_id == TypeId::of::<u16>()
        || type_id == TypeId::of::<u32>()
        || type_id == TypeId::of::<u64>()
        || type_id == TypeId::of::<usize>()
    {
        Some(json!({"type": "integer"}))
    } else if type_id == TypeId::of::<f32>() || type_id == TypeId::of::<f64>() {
        Some(json!({"type": "number"}))
    } else {
        None
    }
}

#[derive(Clone)]
pub struct BaseEventHandle<E: EventSpec> {
    pub inner: Arc<RawBaseEvent>,
    marker: PhantomData<E>,
}

pub trait IntoBaseEventHandle {
    type Event: EventSpec;

    fn into_base_event_handle(self) -> BaseEventHandle<Self::Event>;
}

impl<E> IntoBaseEventHandle for E
where
    E: EventSpec<payload = E>,
{
    type Event = E;

    fn into_base_event_handle(self) -> BaseEventHandle<Self::Event> {
        BaseEventHandle::new(self)
    }
}

impl<E: EventSpec> IntoBaseEventHandle for BaseEventHandle<E> {
    type Event = E;

    fn into_base_event_handle(self) -> BaseEventHandle<Self::Event> {
        self
    }
}

impl<E: EventSpec> BaseEventHandle<E> {
    pub fn new(payload: E::payload) -> Self {
        let value = serde_json::to_value(payload).expect("event payload serialization failed");
        let Value::Object(payload_map) = value else {
            panic!("event payload must serialize to a JSON object");
        };
        let has_event_version = payload_map.contains_key("event_version");
        let has_event_timeout = payload_map.contains_key("event_timeout");
        let has_event_slow_timeout = payload_map.contains_key("event_slow_timeout");
        let has_event_concurrency = payload_map.contains_key("event_concurrency");
        let has_event_handler_timeout = payload_map.contains_key("event_handler_timeout");
        let has_event_handler_slow_timeout = payload_map.contains_key("event_handler_slow_timeout");
        let has_event_handler_concurrency = payload_map.contains_key("event_handler_concurrency");
        let has_event_handler_completion = payload_map.contains_key("event_handler_completion");
        let has_event_blocks_parent_completion =
            payload_map.contains_key("event_blocks_parent_completion");
        let has_event_result_type = payload_map.contains_key("event_result_type");

        let inner = RawBaseEvent::new(E::event_type, payload_map);
        {
            let mut event = inner.inner.lock();
            if !has_event_version {
                event.event_version = E::event_version.to_string();
            }
            if !has_event_timeout {
                event.event_timeout = E::event_timeout;
            }
            if !has_event_slow_timeout {
                event.event_slow_timeout = E::event_slow_timeout;
            }
            if !has_event_concurrency {
                event.event_concurrency = E::event_concurrency;
            }
            if !has_event_handler_timeout {
                event.event_handler_timeout = E::event_handler_timeout;
            }
            if !has_event_handler_slow_timeout {
                event.event_handler_slow_timeout = E::event_handler_slow_timeout;
            }
            if !has_event_handler_concurrency {
                event.event_handler_concurrency = E::event_handler_concurrency;
            }
            if !has_event_handler_completion {
                event.event_handler_completion = E::event_handler_completion;
            }
            if !has_event_blocks_parent_completion {
                event.event_blocks_parent_completion = E::event_blocks_parent_completion;
            }
            if !has_event_result_type {
                event.event_result_type = E::event_result_type_json();
            }
        }

        Self {
            inner,
            marker: PhantomData,
        }
    }

    pub fn from_base_event(event: Arc<RawBaseEvent>) -> Self {
        Self {
            inner: event,
            marker: PhantomData,
        }
    }

    pub fn event_payload(&self) -> E::payload {
        let payload = self.inner.inner.lock().payload.clone();
        let value = Value::Object(payload);
        serde_json::from_value(value).expect("event payload decode failed")
    }

    pub fn event_bus(&self) -> Option<Arc<EventBus>> {
        self.inner.event_bus()
    }

    pub fn bus(&self) -> Option<Arc<EventBus>> {
        self.inner.bus()
    }

    pub fn to_json_value(&self) -> Value {
        self.inner.to_json_value()
    }

    pub async fn wait_completed(&self) {
        self.inner.wait_completed().await;
    }

    pub async fn event_completed(&self) {
        self.inner.event_completed().await;
    }

    pub async fn first(&self) -> Result<Option<E::event_result_type>, String> {
        {
            let event = self.inner.inner.lock();
            if event.event_status == crate::types::EventStatus::Pending
                && event.event_path.is_empty()
                && event.event_pending_bus_count == 0
                && event.event_results.is_empty()
            {
                return Err("event has no bus attached".to_string());
            }
        }
        self.inner.inner.lock().event_handler_completion = Some(EventHandlerCompletionMode::First);
        self.wait_completed().await;
        self.first_result_or_error()
    }

    pub async fn event_result(
        &self,
        options: EventResultsOptions,
    ) -> Result<Option<E::event_result_type>, String> {
        self.inner
            .event_result(options)
            .await?
            .map(Self::decode_result_value)
            .transpose()
    }

    pub async fn event_results_list(
        &self,
        options: EventResultsOptions,
    ) -> Result<Vec<E::event_result_type>, String> {
        self.inner
            .event_results_list(options)
            .await?
            .into_iter()
            .map(Self::decode_result_value)
            .collect()
    }

    pub fn first_result(&self) -> Option<E::event_result_type> {
        self.first_result_from_completed().ok().flatten()
    }

    pub fn first_result_or_error(&self) -> Result<Option<E::event_result_type>, String> {
        let results: HashMap<String, crate::event_result::EventResult> =
            self.inner.inner.lock().event_results.clone();
        let mut error_results: Vec<_> = results
            .values()
            .filter(|result| {
                result.status == crate::event_result::EventResultStatus::Error
                    && !result
                        .error
                        .as_deref()
                        .unwrap_or_default()
                        .contains("first() resolved")
            })
            .collect();
        error_results.sort_by(|left, right| {
            left.completed_at
                .cmp(&right.completed_at)
                .then_with(|| left.started_at.cmp(&right.started_at))
                .then_with(|| {
                    left.handler
                        .handler_registered_at
                        .cmp(&right.handler.handler_registered_at)
                })
                .then_with(|| left.handler.id.cmp(&right.handler.id))
        });
        if let Some(error) = error_results
            .into_iter()
            .filter_map(|result| result.error.clone())
            .next()
        {
            return Err(error);
        }
        self.first_result_from_completed()
    }

    fn first_result_from_completed(&self) -> Result<Option<E::event_result_type>, String> {
        let results: HashMap<String, crate::event_result::EventResult> =
            self.inner.inner.lock().event_results.clone();
        let mut ordered_results: Vec<_> = results
            .values()
            .filter(|result| result.status == crate::event_result::EventResultStatus::Completed)
            .collect();
        ordered_results.sort_by(|left, right| {
            left.completed_at
                .cmp(&right.completed_at)
                .then_with(|| left.started_at.cmp(&right.started_at))
                .then_with(|| {
                    left.handler
                        .handler_registered_at
                        .cmp(&right.handler.handler_registered_at)
                })
                .then_with(|| left.handler.id.cmp(&right.handler.id))
        });
        for result in ordered_results {
            if result.error.is_none() {
                if let Some(value) = &result.result {
                    if value.is_null() || RawBaseEvent::is_base_event_json(value) {
                        continue;
                    }
                    let decoded: E::event_result_type =
                        serde_json::from_value(value.clone()).map_err(|error| error.to_string())?;
                    return Ok(Some(decoded));
                }
            }
        }
        Ok(None)
    }

    fn decode_result_value(value: Value) -> Result<E::event_result_type, String> {
        serde_json::from_value(value).map_err(|error| error.to_string())
    }
}

impl EventBus {
    pub fn emit<I: IntoBaseEventHandle>(&self, event: I) -> BaseEventHandle<I::Event> {
        let event = event.into_base_event_handle();
        let emitted = self.enqueue_base(event.inner.clone());
        BaseEventHandle::from_base_event(emitted)
    }

    pub fn emit_with_options<I: IntoBaseEventHandle>(
        &self,
        event: I,
        queue_jump: bool,
    ) -> BaseEventHandle<I::Event> {
        let event = event.into_base_event_handle();
        let emitted = self.enqueue_base_with_options(event.inner.clone(), queue_jump);
        BaseEventHandle::from_base_event(emitted)
    }

    pub fn emit_child<I: IntoBaseEventHandle>(&self, event: I) -> BaseEventHandle<I::Event> {
        let event = event.into_base_event_handle();
        let emitted = self.enqueue_child_base(event.inner.clone());
        BaseEventHandle::from_base_event(emitted)
    }

    pub fn emit_child_with_options<I: IntoBaseEventHandle>(
        &self,
        event: I,
        queue_jump: bool,
    ) -> BaseEventHandle<I::Event> {
        let event = event.into_base_event_handle();
        let emitted = self.enqueue_child_base_with_options(event.inner.clone(), queue_jump);
        BaseEventHandle::from_base_event(emitted)
    }

    pub fn on<E, F, Fut>(&self, handler_name: &str, handler_fn: F) -> EventHandler
    where
        E: EventSpec,
        F: Fn(BaseEventHandle<E>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<E::event_result_type, String>> + Send + 'static,
    {
        self.on_with_options::<E, _, _>(handler_name, EventHandlerOptions::default(), handler_fn)
    }

    pub fn on_with_options<E, F, Fut>(
        &self,
        handler_name: &str,
        options: EventHandlerOptions,
        handler_fn: F,
    ) -> EventHandler
    where
        E: EventSpec,
        F: Fn(BaseEventHandle<E>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<E::event_result_type, String>> + Send + 'static,
    {
        self.on_raw_with_options(E::event_type, handler_name, options, move |event| {
            let typed = BaseEventHandle::<E>::from_base_event(event);
            let fut = handler_fn(typed);
            async move {
                let result = fut.await?;
                serde_json::to_value(result).map_err(|error| error.to_string())
            }
        })
    }
}

pub fn payload_map_from_value(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(map) => map,
        _ => panic!("typed payload must be a JSON object"),
    }
}

#[macro_export]
macro_rules! event {
    ($(#[$attr:meta])* $vis:vis struct $name:ident { $($body:tt)* }) => {
        $crate::__abxbus_event_parse! {
            @parse
            [$(#[$attr])*] [$vis] [$name]
            payload[]
            result[]
            event_type[]
            event_version[]
            event_timeout[]
            event_slow_timeout[]
            event_concurrency[]
            event_handler_timeout[]
            event_handler_slow_timeout[]
            event_handler_concurrency[]
            event_handler_completion[]
            event_blocks_parent_completion[]
            event_result_schema[]
            $($body)* ,
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __abxbus_event_parse {
    (@parse
        [$($attr:tt)*] [$vis:vis] [$name:ident]
        payload[$($payload:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        ,
        $($rest:tt)*
    ) => {
        $crate::__abxbus_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_handler_concurrency[$($event_handler_concurrency)*]
            event_handler_completion[$($event_handler_completion)*]
            event_blocks_parent_completion[$($event_blocks_parent_completion)*]
            event_result_schema[$($event_result_schema)*]
            $($rest)*
        }
    };
    (@parse
        [$($attr:tt)*] [$vis:vis] [$name:ident]
        payload[$($payload:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_result_type: $next_result:ty,
        $($rest:tt)*
    ) => {
        $crate::__abxbus_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            result[$next_result]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_handler_concurrency[$($event_handler_concurrency)*]
            event_handler_completion[$($event_handler_completion)*]
            event_blocks_parent_completion[$($event_blocks_parent_completion)*]
            event_result_schema[$($event_result_schema)*]
            $($rest)*
        }
    };
    (@parse
        [$($attr:tt)*] [$vis:vis] [$name:ident]
        payload[$($payload:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_type: $next_event_type:literal,
        $($rest:tt)*
    ) => {
        $crate::__abxbus_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            result[$($result)*]
            event_type[$next_event_type]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_handler_concurrency[$($event_handler_concurrency)*]
            event_handler_completion[$($event_handler_completion)*]
            event_blocks_parent_completion[$($event_blocks_parent_completion)*]
            event_result_schema[$($event_result_schema)*]
            $($rest)*
        }
    };
    (@parse
        [$($attr:tt)*] [$vis:vis] [$name:ident]
        payload[$($payload:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_version: $next_event_version:literal,
        $($rest:tt)*
    ) => {
        $crate::__abxbus_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$next_event_version]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_handler_concurrency[$($event_handler_concurrency)*]
            event_handler_completion[$($event_handler_completion)*]
            event_blocks_parent_completion[$($event_blocks_parent_completion)*]
            event_result_schema[$($event_result_schema)*]
            $($rest)*
        }
    };
    (@parse
        [$($attr:tt)*] [$vis:vis] [$name:ident]
        payload[$($payload:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_timeout: $next_timeout:literal,
        $($rest:tt)*
    ) => {
        $crate::__abxbus_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$next_timeout]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_handler_concurrency[$($event_handler_concurrency)*]
            event_handler_completion[$($event_handler_completion)*]
            event_blocks_parent_completion[$($event_blocks_parent_completion)*]
            event_result_schema[$($event_result_schema)*]
            $($rest)*
        }
    };
    (@parse
        [$($attr:tt)*] [$vis:vis] [$name:ident]
        payload[$($payload:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_slow_timeout: $next_timeout:literal,
        $($rest:tt)*
    ) => {
        $crate::__abxbus_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$next_timeout]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_handler_concurrency[$($event_handler_concurrency)*]
            event_handler_completion[$($event_handler_completion)*]
            event_blocks_parent_completion[$($event_blocks_parent_completion)*]
            event_result_schema[$($event_result_schema)*]
            $($rest)*
        }
    };
    (@parse
        [$($attr:tt)*] [$vis:vis] [$name:ident]
        payload[$($payload:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_concurrency: $next_mode:tt,
        $($rest:tt)*
    ) => {
        $crate::__abxbus_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$next_mode]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_handler_concurrency[$($event_handler_concurrency)*]
            event_handler_completion[$($event_handler_completion)*]
            event_blocks_parent_completion[$($event_blocks_parent_completion)*]
            event_result_schema[$($event_result_schema)*]
            $($rest)*
        }
    };
    (@parse
        [$($attr:tt)*] [$vis:vis] [$name:ident]
        payload[$($payload:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_handler_timeout: $next_timeout:literal,
        $($rest:tt)*
    ) => {
        $crate::__abxbus_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$next_timeout]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_handler_concurrency[$($event_handler_concurrency)*]
            event_handler_completion[$($event_handler_completion)*]
            event_blocks_parent_completion[$($event_blocks_parent_completion)*]
            event_result_schema[$($event_result_schema)*]
            $($rest)*
        }
    };
    (@parse
        [$($attr:tt)*] [$vis:vis] [$name:ident]
        payload[$($payload:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_handler_slow_timeout: $next_timeout:literal,
        $($rest:tt)*
    ) => {
        $crate::__abxbus_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$next_timeout]
            event_handler_concurrency[$($event_handler_concurrency)*]
            event_handler_completion[$($event_handler_completion)*]
            event_blocks_parent_completion[$($event_blocks_parent_completion)*]
            event_result_schema[$($event_result_schema)*]
            $($rest)*
        }
    };
    (@parse
        [$($attr:tt)*] [$vis:vis] [$name:ident]
        payload[$($payload:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_handler_concurrency: $next_mode:tt,
        $($rest:tt)*
    ) => {
        $crate::__abxbus_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_handler_concurrency[$next_mode]
            event_handler_completion[$($event_handler_completion)*]
            event_blocks_parent_completion[$($event_blocks_parent_completion)*]
            event_result_schema[$($event_result_schema)*]
            $($rest)*
        }
    };
    (@parse
        [$($attr:tt)*] [$vis:vis] [$name:ident]
        payload[$($payload:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_handler_completion: $next_mode:tt,
        $($rest:tt)*
    ) => {
        $crate::__abxbus_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_handler_concurrency[$($event_handler_concurrency)*]
            event_handler_completion[$next_mode]
            event_blocks_parent_completion[$($event_blocks_parent_completion)*]
            event_result_schema[$($event_result_schema)*]
            $($rest)*
        }
    };
    (@parse
        [$($attr:tt)*] [$vis:vis] [$name:ident]
        payload[$($payload:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_blocks_parent_completion: $next_blocks:literal,
        $($rest:tt)*
    ) => {
        $crate::__abxbus_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_handler_concurrency[$($event_handler_concurrency)*]
            event_handler_completion[$($event_handler_completion)*]
            event_blocks_parent_completion[$next_blocks]
            event_result_schema[$($event_result_schema)*]
            $($rest)*
        }
    };
    (@parse
        [$($attr:tt)*] [$vis:vis] [$name:ident]
        payload[$($payload:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_result_schema: $next_schema:literal,
        $($rest:tt)*
    ) => {
        $crate::__abxbus_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_handler_concurrency[$($event_handler_concurrency)*]
            event_handler_completion[$($event_handler_completion)*]
            event_blocks_parent_completion[$($event_blocks_parent_completion)*]
            event_result_schema[$next_schema]
            $($rest)*
        }
    };
    (@parse
        [$($attr:tt)*] [$vis:vis] [$name:ident]
        payload[$($payload:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        $field_vis:vis $field:ident : $field_ty:ty,
        $($rest:tt)*
    ) => {
        $crate::__abxbus_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)* $field_vis $field: $field_ty,]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_handler_concurrency[$($event_handler_concurrency)*]
            event_handler_completion[$($event_handler_completion)*]
            event_blocks_parent_completion[$($event_blocks_parent_completion)*]
            event_result_schema[$($event_result_schema)*]
            $($rest)*
        }
    };
    (@parse
        [$($attr:tt)*] [$vis:vis] [$name:ident]
        payload[$($payload:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
    ) => {
        #[derive(Clone, Debug, PartialEq, $crate::serde::Serialize, $crate::serde::Deserialize)]
        $($attr)*
        $vis struct $name {
            $($payload)*
        }

        #[allow(non_camel_case_types, non_upper_case_globals)]
        impl $crate::typed::EventSpec for $name {
            type payload = $name;
            type event_result_type = $crate::__abxbus_event_result_type!($($result)*);

            const event_type: &'static str = $crate::__abxbus_event_type!($name; $($event_type)*);
            const event_version: &'static str = $crate::__abxbus_event_version!($($event_version)*);
            const event_timeout: Option<f64> = $crate::__abxbus_event_optional_f64!($($event_timeout)*);
            const event_slow_timeout: Option<f64> = $crate::__abxbus_event_optional_f64!($($event_slow_timeout)*);
            const event_concurrency: Option<$crate::types::EventConcurrencyMode> =
                $crate::__abxbus_event_concurrency!($($event_concurrency)*);
            const event_handler_timeout: Option<f64> =
                $crate::__abxbus_event_optional_f64!($($event_handler_timeout)*);
            const event_handler_slow_timeout: Option<f64> =
                $crate::__abxbus_event_optional_f64!($($event_handler_slow_timeout)*);
            const event_handler_concurrency: Option<$crate::types::EventHandlerConcurrencyMode> =
                $crate::__abxbus_event_handler_concurrency!($($event_handler_concurrency)*);
            const event_handler_completion: Option<$crate::types::EventHandlerCompletionMode> =
                $crate::__abxbus_event_handler_completion!($($event_handler_completion)*);
            const event_blocks_parent_completion: bool =
                $crate::__abxbus_event_bool_false!($($event_blocks_parent_completion)*);
            const event_result_type_schema: Option<&'static str> =
                $crate::__abxbus_event_optional_str!($($event_result_schema)*);
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __abxbus_event_result_type {
    () => {
        $crate::serde_json::Value
    };
    ($result:ty) => {
        $result
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __abxbus_event_type {
    ($name:ident;) => {
        stringify!($name)
    };
    ($name:ident; $event_type:literal) => {
        $event_type
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __abxbus_event_version {
    () => {
        "0.0.1"
    };
    ($version:literal) => {
        $version
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __abxbus_event_optional_f64 {
    () => {
        None
    };
    ($value:literal) => {
        Some($value as f64)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __abxbus_event_optional_str {
    () => {
        None
    };
    ($value:literal) => {
        Some($value)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __abxbus_event_bool_false {
    () => {
        false
    };
    ($value:literal) => {
        $value
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __abxbus_event_concurrency {
    () => {
        None
    };
    (global_serial) => {
        Some($crate::types::EventConcurrencyMode::GlobalSerial)
    };
    ("global-serial") => {
        Some($crate::types::EventConcurrencyMode::GlobalSerial)
    };
    ("global_serial") => {
        Some($crate::types::EventConcurrencyMode::GlobalSerial)
    };
    (bus_serial) => {
        Some($crate::types::EventConcurrencyMode::BusSerial)
    };
    ("bus-serial") => {
        Some($crate::types::EventConcurrencyMode::BusSerial)
    };
    ("bus_serial") => {
        Some($crate::types::EventConcurrencyMode::BusSerial)
    };
    (parallel) => {
        Some($crate::types::EventConcurrencyMode::Parallel)
    };
    ("parallel") => {
        Some($crate::types::EventConcurrencyMode::Parallel)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __abxbus_event_handler_concurrency {
    () => {
        None
    };
    (serial) => {
        Some($crate::types::EventHandlerConcurrencyMode::Serial)
    };
    ("serial") => {
        Some($crate::types::EventHandlerConcurrencyMode::Serial)
    };
    (parallel) => {
        Some($crate::types::EventHandlerConcurrencyMode::Parallel)
    };
    ("parallel") => {
        Some($crate::types::EventHandlerConcurrencyMode::Parallel)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __abxbus_event_handler_completion {
    () => {
        None
    };
    (all) => {
        Some($crate::types::EventHandlerCompletionMode::All)
    };
    ("all") => {
        Some($crate::types::EventHandlerCompletionMode::All)
    };
    (first) => {
        Some($crate::types::EventHandlerCompletionMode::First)
    };
    ("first") => {
        Some($crate::types::EventHandlerCompletionMode::First)
    };
}
