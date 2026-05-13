use std::{any::TypeId, fmt, future::Future, marker::PhantomData, ops::Deref, sync::Arc};

use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Map, Value};

use crate::types::{EventConcurrencyMode, EventHandlerCompletionMode, EventHandlerConcurrencyMode};
use crate::{
    base_event::{BaseEvent as RawBaseEvent, EventResultOptions, EventWaitOptions},
    event_bus::EventBus,
    event_handler::{EventHandler, EventHandlerOptions},
};

#[allow(clippy::ptr_arg)]
pub fn is_string_empty(value: &String) -> bool {
    value.is_empty()
}

#[allow(clippy::ptr_arg)]
pub fn is_vec_empty<T>(value: &Vec<T>) -> bool {
    value.is_empty()
}

pub fn is_hashmap_empty<K, V, S>(value: &std::collections::HashMap<K, V, S>) -> bool {
    value.is_empty()
}

pub fn is_false(value: &bool) -> bool {
    !*value
}

pub fn is_zero_usize(value: &usize) -> bool {
    *value == 0
}

pub fn is_event_status_pending(value: &crate::types::EventStatus) -> bool {
    *value == crate::types::EventStatus::Pending
}

pub struct EventType<E: EventSpec>(PhantomData<E>);

impl<E: EventSpec> Clone for EventType<E> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<E: EventSpec> Copy for EventType<E> {}

impl<E: EventSpec> EventType<E> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<E: EventSpec> Default for EventType<E> {
    fn default() -> Self {
        Self::new()
    }
}

pub trait EventMarker: Send + Sync + 'static {
    type Event: EventSpec;
}

impl<E: EventSpec> EventMarker for EventType<E> {
    type Event = E;
}

impl<E: EventSpec> EventMarker for E {
    type Event = E;
}

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
    payload: Arc<E::payload>,
    marker: PhantomData<E>,
}

impl<E: EventSpec> fmt::Debug for BaseEventHandle<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BaseEventHandle")
            .field("event_type", &E::event_type)
            .field("event_id", &self.inner.inner.lock().event_id)
            .finish()
    }
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
        let typed_payload = Arc::new(payload.clone());
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
            payload: typed_payload,
            marker: PhantomData,
        }
    }

    pub fn from_base_event(event: Arc<RawBaseEvent>) -> Self {
        let payload = Arc::new(Self::decode_payload_from_base_event(&event));
        Self {
            inner: event,
            payload,
            marker: PhantomData,
        }
    }

    fn decode_payload_from_base_event(event: &Arc<RawBaseEvent>) -> E::payload {
        let event = event.inner.lock();
        let mut payload = event.payload.clone();
        payload.insert("event_type".to_string(), json!(event.event_type));
        payload.insert("event_version".to_string(), json!(event.event_version));
        payload.insert("event_timeout".to_string(), json!(event.event_timeout));
        payload.insert(
            "event_slow_timeout".to_string(),
            json!(event.event_slow_timeout),
        );
        payload.insert(
            "event_concurrency".to_string(),
            json!(event.event_concurrency),
        );
        payload.insert(
            "event_handler_timeout".to_string(),
            json!(event.event_handler_timeout),
        );
        payload.insert(
            "event_handler_slow_timeout".to_string(),
            json!(event.event_handler_slow_timeout),
        );
        payload.insert(
            "event_handler_concurrency".to_string(),
            json!(event.event_handler_concurrency),
        );
        payload.insert(
            "event_handler_completion".to_string(),
            json!(event.event_handler_completion),
        );
        payload.insert(
            "event_blocks_parent_completion".to_string(),
            json!(event.event_blocks_parent_completion),
        );
        payload.insert(
            "event_result_type".to_string(),
            json!(event.event_result_type),
        );
        payload.insert("event_id".to_string(), json!(event.event_id));
        payload.insert("event_path".to_string(), json!(event.event_path));
        payload.insert("event_parent_id".to_string(), json!(event.event_parent_id));
        payload.insert(
            "event_emitted_by_handler_id".to_string(),
            json!(event.event_emitted_by_handler_id),
        );
        payload.insert(
            "event_pending_bus_count".to_string(),
            json!(event.event_pending_bus_count),
        );
        payload.insert(
            "event_created_at".to_string(),
            json!(event.event_created_at),
        );
        payload.insert("event_status".to_string(), json!(event.event_status));
        payload.insert(
            "event_started_at".to_string(),
            json!(event.event_started_at),
        );
        payload.insert(
            "event_completed_at".to_string(),
            json!(event.event_completed_at),
        );
        payload.insert("event_results".to_string(), json!(event.event_results));
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

    pub async fn now(&self) -> Result<Self, String> {
        self.inner.now().await.map(|_| Self {
            inner: self.inner.clone(),
            payload: self.payload.clone(),
            marker: PhantomData,
        })
    }

    pub async fn now_with_options(&self, options: EventWaitOptions) -> Result<Self, String> {
        self.inner.now_with_options(options).await.map(|_| Self {
            inner: self.inner.clone(),
            payload: self.payload.clone(),
            marker: PhantomData,
        })
    }

    pub async fn wait(&self) -> Result<Self, String> {
        self.wait_with_options(EventWaitOptions::default()).await
    }

    pub async fn wait_with_options(&self, options: EventWaitOptions) -> Result<Self, String> {
        self.inner.wait_with_options(options).await.map(|_| Self {
            inner: self.inner.clone(),
            payload: self.payload.clone(),
            marker: PhantomData,
        })
    }

    pub async fn event_result(
        &self,
        options: EventResultOptions,
    ) -> Result<Option<E::event_result_type>, String> {
        self.inner
            .event_result(options)
            .await?
            .map(Self::decode_result_value)
            .transpose()
    }

    pub async fn event_results_list(
        &self,
        options: EventResultOptions,
    ) -> Result<Vec<E::event_result_type>, String> {
        self.inner
            .event_results_list(options)
            .await?
            .into_iter()
            .map(Self::decode_result_value)
            .collect()
    }

    fn decode_result_value(value: Value) -> Result<E::event_result_type, String> {
        serde_json::from_value(value).map_err(|error| error.to_string())
    }
}

impl<E: EventSpec> Deref for BaseEventHandle<E> {
    type Target = E::payload;

    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

pub trait TypedEventHandler<E: EventSpec>: Send + Sync + 'static {
    type Future: Future<Output = Result<E::event_result_type, String>> + Send + 'static;

    fn call(&self, event: E::payload) -> Self::Future;
}

impl<E, F, Fut> TypedEventHandler<E> for F
where
    E: EventSpec,
    F: Fn(E::payload) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<E::event_result_type, String>> + Send + 'static,
{
    type Future = Fut;

    fn call(&self, event: E::payload) -> Self::Future {
        self(event)
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

    #[track_caller]
    pub fn on<M>(
        &self,
        _event_type: M,
        handler_fn: impl TypedEventHandler<M::Event>,
    ) -> EventHandler
    where
        M: EventMarker,
    {
        self.on_with_options(
            _event_type,
            &format!("on_{}", M::Event::event_type),
            EventHandlerOptions::default(),
            handler_fn,
        )
    }

    #[track_caller]
    pub fn on_with_options<M>(
        &self,
        _event_type: M,
        handler_name: &str,
        options: EventHandlerOptions,
        handler_fn: impl TypedEventHandler<M::Event>,
    ) -> EventHandler
    where
        M: EventMarker,
    {
        self.on_raw_with_options(M::Event::event_type, handler_name, options, move |event| {
            let typed = BaseEventHandle::<M::Event>::from_base_event(event);
            let fut = handler_fn.call((*typed.payload).clone());
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
        #[derive(Clone, Default, $crate::serde::Serialize, $crate::serde::Deserialize)]
        $($attr)*
        $vis struct $name {
            $($payload)*
            #[serde(default, skip_serializing_if = "abxbus_rust::typed::is_string_empty")]
            pub event_type: String,
            #[serde(default, skip_serializing_if = "abxbus_rust::typed::is_string_empty")]
            pub event_version: String,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub event_timeout: Option<f64>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub event_slow_timeout: Option<f64>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub event_concurrency: Option<$crate::types::EventConcurrencyMode>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub event_handler_timeout: Option<f64>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub event_handler_slow_timeout: Option<f64>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub event_handler_concurrency: Option<$crate::types::EventHandlerConcurrencyMode>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub event_handler_completion: Option<$crate::types::EventHandlerCompletionMode>,
            #[serde(default, skip_serializing_if = "abxbus_rust::typed::is_false")]
            pub event_blocks_parent_completion: bool,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub event_result_type: Option<$crate::serde_json::Value>,
            #[serde(default, skip_serializing_if = "abxbus_rust::typed::is_string_empty")]
            pub event_id: String,
            #[serde(default, skip_serializing_if = "abxbus_rust::typed::is_vec_empty")]
            pub event_path: Vec<String>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub event_parent_id: Option<String>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub event_emitted_by_handler_id: Option<String>,
            #[serde(default, skip_serializing_if = "abxbus_rust::typed::is_zero_usize")]
            pub event_pending_bus_count: usize,
            #[serde(default, skip_serializing_if = "abxbus_rust::typed::is_string_empty")]
            pub event_created_at: String,
            #[serde(default, skip_serializing_if = "abxbus_rust::typed::is_event_status_pending")]
            pub event_status: $crate::types::EventStatus,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub event_started_at: Option<String>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub event_completed_at: Option<String>,
            #[serde(default, skip_serializing_if = "abxbus_rust::typed::is_hashmap_empty")]
            pub event_results: std::collections::HashMap<String, $crate::event_result::EventResult>,
        }

        #[allow(non_upper_case_globals)]
        $vis const $name: $crate::typed::EventType<$name> = $crate::typed::EventType::new();

        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match $crate::serde_json::to_value(self) {
                    Ok(value) => write!(f, "{}({value})", stringify!($name)),
                    Err(_) => write!(f, "{}(<unserializable>)", stringify!($name)),
                }
            }
        }

        impl PartialEq for $name {
            fn eq(&self, other: &Self) -> bool {
                $crate::serde_json::to_value(self).ok() == $crate::serde_json::to_value(other).ok()
            }
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
