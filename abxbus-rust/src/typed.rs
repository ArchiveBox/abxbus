use std::{any::TypeId, fmt, future::Future, marker::PhantomData, sync::Arc};

use parking_lot::Mutex;
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{json, Map, Value};

use crate::types::{EventConcurrencyMode, EventHandlerCompletionMode, EventHandlerConcurrencyMode};
use crate::{
    base_event::{BaseEvent as RawBaseEvent, BaseEventData},
    event_bus::EventBus,
    event_handler::{
        validate_optional_seconds_at_least_minus_one, validate_optional_seconds_nonnegative,
        EventHandler, EventHandlerOptions,
    },
    event_result::EventResult,
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

#[derive(Clone)]
pub struct Live<T>
where
    T: Clone + Default + Send + Sync + 'static,
{
    fallback: Arc<Mutex<T>>,
    source: Option<Arc<RawBaseEvent>>,
    getter: Option<fn(&BaseEventData) -> T>,
    setter: Option<fn(&mut BaseEventData, T)>,
}

impl<T> Live<T>
where
    T: Clone + Default + Send + Sync + 'static,
{
    pub fn new(value: T) -> Self {
        Self {
            fallback: Arc::new(Mutex::new(value)),
            source: None,
            getter: None,
            setter: None,
        }
    }

    pub fn from_event(
        event: Arc<RawBaseEvent>,
        getter: fn(&BaseEventData) -> T,
        setter: fn(&mut BaseEventData, T),
    ) -> Self {
        Self {
            fallback: Arc::new(Mutex::new(T::default())),
            source: Some(event),
            getter: Some(getter),
            setter: Some(setter),
        }
    }

    pub fn read(&self) -> T {
        if let (Some(source), Some(getter)) = (&self.source, self.getter) {
            return getter(&source.inner.lock());
        }
        self.fallback.lock().clone()
    }

    pub fn set(&self, value: T) {
        if let (Some(source), Some(setter)) = (&self.source, self.setter) {
            setter(&mut source.inner.lock(), value);
            return;
        }
        *self.fallback.lock() = value;
    }
}

impl<T> Default for Live<T>
where
    T: Clone + Default + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> fmt::Debug for Live<T>
where
    T: Clone + Default + fmt::Debug + Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.read().fmt(formatter)
    }
}

impl<T> PartialEq<T> for Live<T>
where
    T: Clone + Default + PartialEq + Send + Sync + 'static,
{
    fn eq(&self, other: &T) -> bool {
        self.read() == *other
    }
}

impl<T> PartialEq for Live<T>
where
    T: Clone + Default + PartialEq + Send + Sync + 'static,
{
    fn eq(&self, other: &Self) -> bool {
        self.read() == other.read()
    }
}

impl<T> Serialize for Live<T>
where
    T: Clone + Default + Serialize + Send + Sync + 'static,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.read().serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for Live<T>
where
    T: Clone + Default + Deserialize<'de> + Send + Sync + 'static,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Self::new)
    }
}

pub fn is_live_vec_string_empty(value: &Live<Vec<String>>) -> bool {
    value.read().is_empty()
}

pub fn is_live_usize_zero(value: &Live<usize>) -> bool {
    value.read() == 0
}

pub fn is_live_event_status_pending(value: &Live<crate::types::EventStatus>) -> bool {
    value.read() == crate::types::EventStatus::Pending
}

pub fn is_live_option_string_none(value: &Live<Option<String>>) -> bool {
    value.read().is_none()
}

pub fn is_live_event_results_empty(
    value: &Live<std::collections::HashMap<String, EventResult>>,
) -> bool {
    value.read().is_empty()
}

pub struct EventType<E: EventSpec>(PhantomData<E>);

#[allow(non_snake_case)]
pub struct ModelField<T, D = ()> {
    pub name: &'static str,
    pub Type: Value,
    pub Default: D,
    _marker: PhantomData<fn() -> T>,
}

impl<T: 'static, D> ModelField<T, D> {
    pub fn new(name: &'static str, default: D) -> Self {
        Self {
            name,
            Type: json_schema_for_type::<T>(),
            Default: default,
            _marker: PhantomData,
        }
    }
}

impl<T, D> Clone for ModelField<T, D>
where
    D: Clone,
{
    fn clone(&self) -> Self {
        Self {
            name: self.name,
            Type: self.Type.clone(),
            Default: self.Default.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T, D> fmt::Debug for ModelField<T, D>
where
    D: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelField")
            .field("name", &self.name)
            .field("Type", &self.Type)
            .field("Default", &self.Default)
            .finish()
    }
}

impl<T, D> PartialEq for ModelField<T, D>
where
    D: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.Type == other.Type && self.Default == other.Default
    }
}

impl<T, D> Eq for ModelField<T, D> where D: Eq {}

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

impl<E> EventType<E>
where
    E: EventSpec + EventModelFields,
{
    pub fn model_fields(&self) -> E::ModelFields {
        E::model_fields()
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
    const event_ttl: Option<f64> = None;
    const event_result_ttl: Option<f64> = None;
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

pub trait EventModelFields: EventSpec {
    type ModelFields;

    fn model_fields() -> Self::ModelFields;
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

pub fn json_schema_for_type<T: 'static>() -> Value {
    primitive_result_type_schema::<T>().unwrap_or_else(|| json!({}))
}

pub trait TypedEventObject:
    EventSpec<payload = Self> + Serialize + DeserializeOwned + Clone
{
    #[doc(hidden)]
    fn _from_inner_event(event: Arc<RawBaseEvent>) -> Self;

    #[doc(hidden)]
    fn _inner_event(&self) -> Arc<RawBaseEvent> {
        let value = serde_json::to_value(self).expect("event payload serialization failed");
        let Value::Object(payload_map) = value else {
            panic!("event payload must serialize to a JSON object");
        };
        if let Some(event) = Self::attached_inner_event_from_payload(&payload_map) {
            sync_inner_event_from_payload::<Self>(&event, payload_map);
            return event;
        }
        build_inner_event_from_payload::<Self>(payload_map)
    }

    #[doc(hidden)]
    fn _attached_inner_event(&self) -> Option<Arc<RawBaseEvent>> {
        let value = serde_json::to_value(self).ok()?;
        let event_id = value
            .get("event_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        EventBus::event_for_event_id(event_id)
    }

    #[doc(hidden)]
    fn attached_inner_event_from_payload(
        payload_map: &Map<String, Value>,
    ) -> Option<Arc<RawBaseEvent>> {
        let event_id = payload_map
            .get("event_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        EventBus::event_for_event_id(event_id)
    }

    fn decode_result_value(value: Value) -> Result<Self::event_result_type, String> {
        serde_json::from_value(value).map_err(|error| error.to_string())
    }
}

#[doc(hidden)]
pub fn build_inner_event_from_payload<E>(payload_map: Map<String, Value>) -> Arc<RawBaseEvent>
where
    E: EventSpec,
{
    let has_event_version = payload_map.contains_key("event_version");
    let has_event_timeout = payload_map.contains_key("event_timeout");
    let has_event_slow_timeout = payload_map.contains_key("event_slow_timeout");
    let has_event_concurrency = payload_map.contains_key("event_concurrency");
    let has_event_handler_timeout = payload_map.contains_key("event_handler_timeout");
    let has_event_handler_slow_timeout = payload_map.contains_key("event_handler_slow_timeout");
    let has_event_ttl = payload_map.contains_key("event_ttl");
    let has_event_result_ttl = payload_map.contains_key("event_result_ttl");
    let has_event_handler_concurrency = payload_map.contains_key("event_handler_concurrency");
    let has_event_handler_completion = payload_map.contains_key("event_handler_completion");
    let has_event_blocks_parent_completion =
        payload_map.contains_key("event_blocks_parent_completion");
    let has_event_result_type = payload_map.contains_key("event_result_type");

    validate_optional_seconds_at_least_minus_one("event_ttl", E::event_ttl)
        .expect("event_ttl must be finite and >= -1 or None");
    validate_optional_seconds_at_least_minus_one("event_result_ttl", E::event_result_ttl)
        .expect("event_result_ttl must be finite and >= -1 or None");
    validate_optional_seconds_nonnegative("event_timeout", E::event_timeout)
        .expect("event_timeout must be finite and >= 0 or None");
    validate_optional_seconds_nonnegative("event_slow_timeout", E::event_slow_timeout)
        .expect("event_slow_timeout must be finite and >= 0 or None");
    validate_optional_seconds_nonnegative("event_handler_timeout", E::event_handler_timeout)
        .expect("event_handler_timeout must be finite and >= 0 or None");
    validate_optional_seconds_nonnegative(
        "event_handler_slow_timeout",
        E::event_handler_slow_timeout,
    )
    .expect("event_handler_slow_timeout must be finite and >= 0 or None");

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
        if !has_event_ttl {
            event.event_ttl = E::event_ttl;
        }
        if !has_event_result_ttl {
            event.event_result_ttl = E::event_result_ttl;
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
    inner
}

#[doc(hidden)]
pub fn sync_inner_event_from_payload<E>(event: &Arc<RawBaseEvent>, payload_map: Map<String, Value>)
where
    E: EventSpec,
{
    let updated = build_inner_event_from_payload::<E>(payload_map);
    let updated = updated.inner.lock();
    let mut current = event.inner.lock();
    current.event_type = updated.event_type.clone();
    current.event_version = updated.event_version.clone();
    current.event_timeout = updated.event_timeout;
    current.event_slow_timeout = updated.event_slow_timeout;
    current.event_concurrency = updated.event_concurrency;
    current.event_handler_timeout = updated.event_handler_timeout;
    current.event_handler_slow_timeout = updated.event_handler_slow_timeout;
    current.event_ttl = updated.event_ttl;
    current.event_result_ttl = updated.event_result_ttl;
    current.event_handler_concurrency = updated.event_handler_concurrency;
    current.event_handler_completion = updated.event_handler_completion;
    current.event_blocks_parent_completion = updated.event_blocks_parent_completion;
    current.event_result_type = updated.event_result_type.clone();
    current.event_parent_id = updated.event_parent_id.clone();
    current.event_emitted_by_handler_id = updated.event_emitted_by_handler_id.clone();
    current.event_created_at = updated.event_created_at.clone();
    current.event_extra_payload = updated.event_extra_payload.clone();
}

#[doc(hidden)]
pub fn payload_value_from_inner_event(event: &Arc<RawBaseEvent>) -> Value {
    let event = event.inner.lock();
    let mut payload = event.event_extra_payload.clone();
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
    payload.insert("event_ttl".to_string(), json!(event.event_ttl));
    payload.insert(
        "event_result_ttl".to_string(),
        json!(event.event_result_ttl),
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
    Value::Object(payload)
}

pub trait TypedEventHandler<E: TypedEventObject>: Send + Sync + 'static {
    type Future: Future<Output = Result<E::event_result_type, String>> + Send + 'static;

    fn call(&self, event: E) -> Self::Future;
}

impl<E, F, Fut> TypedEventHandler<E> for F
where
    E: TypedEventObject,
    F: Fn(E) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<E::event_result_type, String>> + Send + 'static,
{
    type Future = Fut;

    fn call(&self, event: E) -> Self::Future {
        self(event)
    }
}

impl EventBus {
    pub fn emit<E: TypedEventObject>(&self, event: E) -> E {
        self.raise_if_terminal_destroyed();
        let emitted = self.enqueue_base(<E as TypedEventObject>::_inner_event(&event));
        E::_from_inner_event(emitted)
    }

    pub fn emit_with_options<E: TypedEventObject>(&self, event: E, queue_jump: bool) -> E {
        self.raise_if_terminal_destroyed();
        let emitted = self
            .enqueue_base_with_options(<E as TypedEventObject>::_inner_event(&event), queue_jump);
        E::_from_inner_event(emitted)
    }

    pub fn emit_child<E: TypedEventObject>(&self, event: E) -> E {
        self.raise_if_terminal_destroyed();
        let emitted = self.enqueue_child_base(<E as TypedEventObject>::_inner_event(&event));
        E::_from_inner_event(emitted)
    }

    pub fn emit_child_with_options<E: TypedEventObject>(&self, event: E, queue_jump: bool) -> E {
        self.raise_if_terminal_destroyed();
        let emitted = self.enqueue_child_base_with_options(
            <E as TypedEventObject>::_inner_event(&event),
            queue_jump,
        );
        E::_from_inner_event(emitted)
    }

    #[track_caller]
    pub fn on<M>(
        &self,
        _event_type: M,
        handler_fn: impl TypedEventHandler<M::Event>,
    ) -> EventHandler
    where
        M: EventMarker,
        M::Event: TypedEventObject,
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
        M::Event: TypedEventObject,
    {
        self.on_raw_with_options(M::Event::event_type, handler_name, options, move |event| {
            let typed = M::Event::_from_inner_event(event);
            let fut = handler_fn.call(typed);
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
        $crate::_inner_event_parse! {
            @parse
            [$(#[$attr])*] [$vis] [$name]
            payload[]
            model_fields[]
            defaults[]
            default_methods[]
            result[]
            event_type[]
            event_version[]
            event_timeout[]
            event_slow_timeout[]
            event_concurrency[]
            event_handler_timeout[]
            event_handler_slow_timeout[]
            event_ttl[]
            event_result_ttl[]
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
macro_rules! _inner_event_parse {
    (@parse
        [$($attr:tt)*] [$vis:vis] [$name:ident]
        payload[$($payload:tt)*]
        model_fields[$($model_fields:tt)*]
        defaults[$($defaults:tt)*]
        default_methods[$($default_methods:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_ttl[$($event_ttl:tt)*]
        event_result_ttl[$($event_result_ttl:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        ,
        $($rest:tt)*
    ) => {
        $crate::_inner_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            model_fields[$($model_fields)*]
            defaults[$($defaults)*]
            default_methods[$($default_methods)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_ttl[$($event_ttl)*]
            event_result_ttl[$($event_result_ttl)*]
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
        model_fields[$($model_fields:tt)*]
        defaults[$($defaults:tt)*]
        default_methods[$($default_methods:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_ttl[$($event_ttl:tt)*]
        event_result_ttl[$($event_result_ttl:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_result_type: $next_result:ty,
        $($rest:tt)*
    ) => {
        $crate::_inner_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            model_fields[$($model_fields)*]
            defaults[$($defaults)*]
            default_methods[$($default_methods)*]
            result[$next_result]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_ttl[$($event_ttl)*]
            event_result_ttl[$($event_result_ttl)*]
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
        model_fields[$($model_fields:tt)*]
        defaults[$($defaults:tt)*]
        default_methods[$($default_methods:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_ttl[$($event_ttl:tt)*]
        event_result_ttl[$($event_result_ttl:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_type: $next_event_type:literal,
        $($rest:tt)*
    ) => {
        $crate::_inner_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            model_fields[$($model_fields)*]
            defaults[$($defaults)*]
            default_methods[$($default_methods)*]
            result[$($result)*]
            event_type[$next_event_type]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_ttl[$($event_ttl)*]
            event_result_ttl[$($event_result_ttl)*]
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
        model_fields[$($model_fields:tt)*]
        defaults[$($defaults:tt)*]
        default_methods[$($default_methods:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_ttl[$($event_ttl:tt)*]
        event_result_ttl[$($event_result_ttl:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_version: $next_event_version:literal,
        $($rest:tt)*
    ) => {
        $crate::_inner_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            model_fields[$($model_fields)*]
            defaults[$($defaults)*]
            default_methods[$($default_methods)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$next_event_version]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_ttl[$($event_ttl)*]
            event_result_ttl[$($event_result_ttl)*]
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
        model_fields[$($model_fields:tt)*]
        defaults[$($defaults:tt)*]
        default_methods[$($default_methods:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_ttl[$($event_ttl:tt)*]
        event_result_ttl[$($event_result_ttl:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_timeout: $next_timeout:literal,
        $($rest:tt)*
    ) => {
        $crate::_inner_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            model_fields[$($model_fields)*]
            defaults[$($defaults)*]
            default_methods[$($default_methods)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$next_timeout]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_ttl[$($event_ttl)*]
            event_result_ttl[$($event_result_ttl)*]
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
        model_fields[$($model_fields:tt)*]
        defaults[$($defaults:tt)*]
        default_methods[$($default_methods:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_ttl[$($event_ttl:tt)*]
        event_result_ttl[$($event_result_ttl:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_slow_timeout: $next_timeout:literal,
        $($rest:tt)*
    ) => {
        $crate::_inner_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            model_fields[$($model_fields)*]
            defaults[$($defaults)*]
            default_methods[$($default_methods)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$next_timeout]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_ttl[$($event_ttl)*]
            event_result_ttl[$($event_result_ttl)*]
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
        model_fields[$($model_fields:tt)*]
        defaults[$($defaults:tt)*]
        default_methods[$($default_methods:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_ttl[$($event_ttl:tt)*]
        event_result_ttl[$($event_result_ttl:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_concurrency: $next_mode:tt,
        $($rest:tt)*
    ) => {
        $crate::_inner_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            model_fields[$($model_fields)*]
            defaults[$($defaults)*]
            default_methods[$($default_methods)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$next_mode]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_ttl[$($event_ttl)*]
            event_result_ttl[$($event_result_ttl)*]
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
        model_fields[$($model_fields:tt)*]
        defaults[$($defaults:tt)*]
        default_methods[$($default_methods:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_ttl[$($event_ttl:tt)*]
        event_result_ttl[$($event_result_ttl:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_handler_timeout: $next_timeout:literal,
        $($rest:tt)*
    ) => {
        $crate::_inner_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            model_fields[$($model_fields)*]
            defaults[$($defaults)*]
            default_methods[$($default_methods)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$next_timeout]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_ttl[$($event_ttl)*]
            event_result_ttl[$($event_result_ttl)*]
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
        model_fields[$($model_fields:tt)*]
        defaults[$($defaults:tt)*]
        default_methods[$($default_methods:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_ttl[$($event_ttl:tt)*]
        event_result_ttl[$($event_result_ttl:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_handler_slow_timeout: $next_timeout:literal,
        $($rest:tt)*
    ) => {
        $crate::_inner_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            model_fields[$($model_fields)*]
            defaults[$($defaults)*]
            default_methods[$($default_methods)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$next_timeout]
            event_ttl[$($event_ttl)*]
            event_result_ttl[$($event_result_ttl)*]
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
        model_fields[$($model_fields:tt)*]
        defaults[$($defaults:tt)*]
        default_methods[$($default_methods:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_ttl[$($event_ttl:tt)*]
        event_result_ttl[$($event_result_ttl:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_ttl: $next_ttl:literal,
        $($rest:tt)*
    ) => {
        $crate::_inner_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            model_fields[$($model_fields)*]
            defaults[$($defaults)*]
            default_methods[$($default_methods)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_ttl[$next_ttl]
            event_result_ttl[$($event_result_ttl)*]
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
        model_fields[$($model_fields:tt)*]
        defaults[$($defaults:tt)*]
        default_methods[$($default_methods:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_ttl[$($event_ttl:tt)*]
        event_result_ttl[$($event_result_ttl:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_result_ttl: $next_ttl:literal,
        $($rest:tt)*
    ) => {
        $crate::_inner_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            model_fields[$($model_fields)*]
            defaults[$($defaults)*]
            default_methods[$($default_methods)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_ttl[$($event_ttl)*]
            event_result_ttl[$next_ttl]
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
        model_fields[$($model_fields:tt)*]
        defaults[$($defaults:tt)*]
        default_methods[$($default_methods:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_ttl[$($event_ttl:tt)*]
        event_result_ttl[$($event_result_ttl:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_handler_concurrency: $next_mode:tt,
        $($rest:tt)*
    ) => {
        $crate::_inner_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            model_fields[$($model_fields)*]
            defaults[$($defaults)*]
            default_methods[$($default_methods)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_ttl[$($event_ttl)*]
            event_result_ttl[$($event_result_ttl)*]
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
        model_fields[$($model_fields:tt)*]
        defaults[$($defaults:tt)*]
        default_methods[$($default_methods:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_ttl[$($event_ttl:tt)*]
        event_result_ttl[$($event_result_ttl:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_handler_completion: $next_mode:tt,
        $($rest:tt)*
    ) => {
        $crate::_inner_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            model_fields[$($model_fields)*]
            defaults[$($defaults)*]
            default_methods[$($default_methods)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_ttl[$($event_ttl)*]
            event_result_ttl[$($event_result_ttl)*]
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
        model_fields[$($model_fields:tt)*]
        defaults[$($defaults:tt)*]
        default_methods[$($default_methods:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_ttl[$($event_ttl:tt)*]
        event_result_ttl[$($event_result_ttl:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_blocks_parent_completion: $next_blocks:literal,
        $($rest:tt)*
    ) => {
        $crate::_inner_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            model_fields[$($model_fields)*]
            defaults[$($defaults)*]
            default_methods[$($default_methods)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_ttl[$($event_ttl)*]
            event_result_ttl[$($event_result_ttl)*]
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
        model_fields[$($model_fields:tt)*]
        defaults[$($defaults:tt)*]
        default_methods[$($default_methods:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_ttl[$($event_ttl:tt)*]
        event_result_ttl[$($event_result_ttl:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        event_result_schema: $next_schema:literal,
        $($rest:tt)*
    ) => {
        $crate::_inner_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)*]
            model_fields[$($model_fields)*]
            defaults[$($defaults)*]
            default_methods[$($default_methods)*]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_ttl[$($event_ttl)*]
            event_result_ttl[$($event_result_ttl)*]
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
        model_fields[$($model_fields:tt)*]
        defaults[$($defaults:tt)*]
        default_methods[$($default_methods:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_ttl[$($event_ttl:tt)*]
        event_result_ttl[$($event_result_ttl:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        $field_vis:vis $field:ident : $field_ty:ty = $field_default:expr,
        $($rest:tt)*
    ) => {
        $crate::_inner_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)* $field_vis $field: $field_ty,]
            model_fields[$($model_fields)* $field_vis $field: $crate::typed::ModelField<$field_ty, $field_ty>,]
            defaults[$($defaults)* $field: $field_default,]
            default_methods[$($default_methods)* $field: $crate::typed::ModelField::<$field_ty, $field_ty>::new(stringify!($field), $field_default),]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_ttl[$($event_ttl)*]
            event_result_ttl[$($event_result_ttl)*]
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
        model_fields[$($model_fields:tt)*]
        defaults[$($defaults:tt)*]
        default_methods[$($default_methods:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_ttl[$($event_ttl:tt)*]
        event_result_ttl[$($event_result_ttl:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
        $field_vis:vis $field:ident : $field_ty:ty,
        $($rest:tt)*
    ) => {
        $crate::_inner_event_parse! {
            @parse
            [$($attr)*] [$vis] [$name]
            payload[$($payload)* $field_vis $field: $field_ty,]
            model_fields[$($model_fields)* $field_vis $field: $crate::typed::ModelField<$field_ty, Option<$field_ty>>,]
            defaults[$($defaults)* $field: Default::default(),]
            default_methods[$($default_methods)* $field: $crate::typed::ModelField::<$field_ty, Option<$field_ty>>::new(stringify!($field), None),]
            result[$($result)*]
            event_type[$($event_type)*]
            event_version[$($event_version)*]
            event_timeout[$($event_timeout)*]
            event_slow_timeout[$($event_slow_timeout)*]
            event_concurrency[$($event_concurrency)*]
            event_handler_timeout[$($event_handler_timeout)*]
            event_handler_slow_timeout[$($event_handler_slow_timeout)*]
            event_ttl[$($event_ttl)*]
            event_result_ttl[$($event_result_ttl)*]
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
        model_fields[$($model_fields:tt)*]
        defaults[$($defaults:tt)*]
        default_methods[$($default_methods:tt)*]
        result[$($result:tt)*]
        event_type[$($event_type:tt)*]
        event_version[$($event_version:tt)*]
        event_timeout[$($event_timeout:tt)*]
        event_slow_timeout[$($event_slow_timeout:tt)*]
        event_concurrency[$($event_concurrency:tt)*]
        event_handler_timeout[$($event_handler_timeout:tt)*]
        event_handler_slow_timeout[$($event_handler_slow_timeout:tt)*]
        event_ttl[$($event_ttl:tt)*]
        event_result_ttl[$($event_result_ttl:tt)*]
        event_handler_concurrency[$($event_handler_concurrency:tt)*]
        event_handler_completion[$($event_handler_completion:tt)*]
        event_blocks_parent_completion[$($event_blocks_parent_completion:tt)*]
        event_result_schema[$($event_result_schema:tt)*]
    ) => {
        #[derive(Clone, $crate::serde::Serialize, $crate::serde::Deserialize)]
        $($attr)*
        $vis struct $name {
            $($payload)*
            #[serde(default, skip_serializing_if = "abxbus::typed::is_string_empty")]
            pub event_type: String,
            #[serde(default, skip_serializing_if = "abxbus::typed::is_string_empty")]
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
            pub event_ttl: Option<f64>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub event_result_ttl: Option<f64>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub event_handler_concurrency: Option<$crate::types::EventHandlerConcurrencyMode>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub event_handler_completion: Option<$crate::types::EventHandlerCompletionMode>,
            #[serde(default, skip_serializing_if = "abxbus::typed::is_false")]
            pub event_blocks_parent_completion: bool,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub event_result_type: Option<$crate::serde_json::Value>,
            #[serde(default, skip_serializing_if = "abxbus::typed::is_string_empty")]
            pub event_id: String,
            #[serde(default, skip_serializing_if = "abxbus::typed::is_live_vec_string_empty")]
            pub event_path: $crate::typed::Live<Vec<String>>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub event_parent_id: Option<String>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub event_emitted_by_handler_id: Option<String>,
            #[serde(default, skip_serializing_if = "abxbus::typed::is_live_usize_zero")]
            pub event_pending_bus_count: $crate::typed::Live<usize>,
            #[serde(default, skip_serializing_if = "abxbus::typed::is_string_empty")]
            pub event_created_at: String,
            #[serde(default, skip_serializing_if = "abxbus::typed::is_live_event_status_pending")]
            pub event_status: $crate::typed::Live<$crate::types::EventStatus>,
            #[serde(default, skip_serializing_if = "abxbus::typed::is_live_option_string_none")]
            pub event_started_at: $crate::typed::Live<Option<String>>,
            #[serde(default, skip_serializing_if = "abxbus::typed::is_live_option_string_none")]
            pub event_completed_at: $crate::typed::Live<Option<String>>,
            #[serde(default, skip_serializing_if = "abxbus::typed::is_live_event_results_empty")]
            pub event_results: $crate::typed::Live<std::collections::HashMap<String, $crate::event_result::EventResult>>,
            // Runtime-only storage for flat JSON fields not declared on this typed event.
            // Serde flattens it when dumping and fills it from unknown flat fields when
            // loading, so "event_extra_payload" never appears on the wire.
            #[serde(default, flatten)]
            pub event_extra_payload: $crate::serde_json::Map<String, $crate::serde_json::Value>,
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    $($defaults)*
                    event_type: String::new(),
                    event_version: String::new(),
                    event_timeout: None,
                    event_slow_timeout: None,
                    event_concurrency: None,
                    event_handler_timeout: None,
                    event_handler_slow_timeout: None,
                    event_ttl: None,
                    event_result_ttl: None,
                    event_handler_concurrency: None,
                    event_handler_completion: None,
                    event_blocks_parent_completion: false,
                    event_result_type: None,
                    event_id: String::new(),
                    event_path: Default::default(),
                    event_parent_id: None,
                    event_emitted_by_handler_id: None,
                    event_pending_bus_count: Default::default(),
                    event_created_at: String::new(),
                    event_status: Default::default(),
                    event_started_at: Default::default(),
                    event_completed_at: Default::default(),
                    event_results: Default::default(),
                    event_extra_payload: Default::default(),
                }
            }
        }

        #[allow(non_upper_case_globals)]
        $vis const $name: $crate::typed::EventType<$name> = $crate::typed::EventType::new();

        $crate::paste::paste! {
            #[derive(Clone, Debug, PartialEq)]
            $vis struct [<$name ModelFields>] {
                $($model_fields)*
                pub event_type: $crate::typed::ModelField<String, String>,
                pub event_version: $crate::typed::ModelField<String, String>,
                pub event_timeout: $crate::typed::ModelField<Option<f64>, Option<f64>>,
                pub event_slow_timeout: $crate::typed::ModelField<Option<f64>, Option<f64>>,
                pub event_concurrency: $crate::typed::ModelField<Option<$crate::types::EventConcurrencyMode>, Option<$crate::types::EventConcurrencyMode>>,
                pub event_handler_timeout: $crate::typed::ModelField<Option<f64>, Option<f64>>,
                pub event_handler_slow_timeout: $crate::typed::ModelField<Option<f64>, Option<f64>>,
                pub event_ttl: $crate::typed::ModelField<Option<f64>, Option<f64>>,
                pub event_result_ttl: $crate::typed::ModelField<Option<f64>, Option<f64>>,
                pub event_handler_concurrency: $crate::typed::ModelField<Option<$crate::types::EventHandlerConcurrencyMode>, Option<$crate::types::EventHandlerConcurrencyMode>>,
                pub event_handler_completion: $crate::typed::ModelField<Option<$crate::types::EventHandlerCompletionMode>, Option<$crate::types::EventHandlerCompletionMode>>,
                pub event_blocks_parent_completion: $crate::typed::ModelField<bool, bool>,
                pub event_result_type: $crate::typed::ModelField<<$name as $crate::typed::EventSpec>::event_result_type, Option<$crate::serde_json::Value>>,
            }

            impl $crate::typed::EventModelFields for $name {
                type ModelFields = [<$name ModelFields>];

                fn model_fields() -> Self::ModelFields {
                    [<$name ModelFields>] {
                        $($default_methods)*
                        event_type: $crate::typed::ModelField::new("event_type", <$name as $crate::typed::EventSpec>::event_type.to_string()),
                        event_version: $crate::typed::ModelField::new("event_version", <$name as $crate::typed::EventSpec>::event_version.to_string()),
                        event_timeout: $crate::typed::ModelField::new("event_timeout", <$name as $crate::typed::EventSpec>::event_timeout),
                        event_slow_timeout: $crate::typed::ModelField::new("event_slow_timeout", <$name as $crate::typed::EventSpec>::event_slow_timeout),
                        event_concurrency: $crate::typed::ModelField::new("event_concurrency", <$name as $crate::typed::EventSpec>::event_concurrency),
                        event_handler_timeout: $crate::typed::ModelField::new("event_handler_timeout", <$name as $crate::typed::EventSpec>::event_handler_timeout),
                        event_handler_slow_timeout: $crate::typed::ModelField::new("event_handler_slow_timeout", <$name as $crate::typed::EventSpec>::event_handler_slow_timeout),
                        event_ttl: $crate::typed::ModelField::new("event_ttl", <$name as $crate::typed::EventSpec>::event_ttl),
                        event_result_ttl: $crate::typed::ModelField::new("event_result_ttl", <$name as $crate::typed::EventSpec>::event_result_ttl),
                        event_handler_concurrency: $crate::typed::ModelField::new("event_handler_concurrency", <$name as $crate::typed::EventSpec>::event_handler_concurrency),
                        event_handler_completion: $crate::typed::ModelField::new("event_handler_completion", <$name as $crate::typed::EventSpec>::event_handler_completion),
                        event_blocks_parent_completion: $crate::typed::ModelField::new("event_blocks_parent_completion", <$name as $crate::typed::EventSpec>::event_blocks_parent_completion),
                        event_result_type: $crate::typed::ModelField::new("event_result_type", <$name as $crate::typed::EventSpec>::event_result_type_json()),
                    }
                }
            }
        }

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

        impl $name {
            pub fn event_bus(&self) -> Option<std::sync::Arc<$crate::event_bus::EventBus>> {
                $crate::event_bus::EventBus::event_bus_for_event_id(&self.event_id)
            }

            pub fn bus(&self) -> Option<std::sync::Arc<$crate::event_bus::EventBus>> {
                self.event_bus()
            }

            fn inner_event(&self) -> Result<std::sync::Arc<$crate::base_event::BaseEvent>, String> {
                let event = $crate::typed::TypedEventObject::_inner_event(self);
                event.ensure_attached_or_completed()?;
                Ok(event)
            }

            #[doc(hidden)]
            pub fn _inner_event(&self) -> std::sync::Arc<$crate::base_event::BaseEvent> {
                <Self as $crate::typed::TypedEventObject>::_inner_event(self)
            }

            pub fn to_json_value(&self) -> $crate::serde_json::Value {
                $crate::typed::TypedEventObject::_inner_event(self).to_json_value()
            }

            pub fn event_payload(&self) -> $crate::serde_json::Map<String, $crate::serde_json::Value> {
                $crate::typed::TypedEventObject::_inner_event(self).event_payload()
            }

            pub async fn now(&self) -> Result<Self, String> {
                self.now_with_options($crate::base_event::EventWaitOptions::default()).await
            }

            pub async fn now_with_options(&self, options: $crate::base_event::EventWaitOptions) -> Result<Self, String> {
                let event = $crate::typed::TypedEventObject::_inner_event(self);
                event.now_with_options(options).await?;
                Ok(<Self as $crate::typed::TypedEventObject>::_from_inner_event(event))
            }

            pub async fn wait(&self) -> Result<Self, String> {
                self.wait_with_options($crate::base_event::EventWaitOptions::default()).await
            }

            pub async fn wait_with_options(&self, options: $crate::base_event::EventWaitOptions) -> Result<Self, String> {
                if self.event_status.read() == $crate::types::EventStatus::Completed {
                    return Ok(self.clone());
                }
                let event = self.inner_event()?;
                event.wait_with_options(options).await?;
                Ok(<Self as $crate::typed::TypedEventObject>::_from_inner_event(event))
            }

            pub async fn event_result(&self) -> Result<Option<<Self as $crate::typed::EventSpec>::event_result_type>, String> {
                self.event_result_with_options($crate::base_event::EventResultOptions::default()).await
            }

            pub async fn event_result_with_options(
                &self,
                options: $crate::base_event::EventResultOptions,
            ) -> Result<Option<<Self as $crate::typed::EventSpec>::event_result_type>, String> {
                $crate::typed::TypedEventObject::_inner_event(self)
                    .event_result_with_options(options)
                    .await?
                    .map(<Self as $crate::typed::TypedEventObject>::decode_result_value)
                    .transpose()
            }

            pub async fn event_results_list(&self) -> Result<Vec<<Self as $crate::typed::EventSpec>::event_result_type>, String> {
                self.event_results_list_with_options($crate::base_event::EventResultOptions::default()).await
            }

            pub async fn event_results_list_with_options(
                &self,
                options: $crate::base_event::EventResultOptions,
            ) -> Result<Vec<<Self as $crate::typed::EventSpec>::event_result_type>, String> {
                $crate::typed::TypedEventObject::_inner_event(self)
                    .event_results_list_with_options(options)
                    .await?
                    .into_iter()
                    .map(<Self as $crate::typed::TypedEventObject>::decode_result_value)
                    .collect()
            }

            pub fn event_errors(&self) -> Vec<String> {
                $crate::typed::TypedEventObject::_inner_event(self).event_errors()
            }

            pub fn event_reset(&self) -> Self {
                self.event_reset_with_options($crate::base_event::EventResetOptions::default())
            }

            pub fn event_reset_with_options(&self, options: $crate::base_event::EventResetOptions) -> Self {
                <Self as $crate::typed::TypedEventObject>::_from_inner_event(
                    $crate::typed::TypedEventObject::_inner_event(self).event_reset_with_options(options)
                )
            }

            pub fn emit<E: $crate::typed::TypedEventObject>(
                &self,
                event: E,
            ) -> E {
                self.event_bus()
                    .expect("event.emit(...) requires an event attached to a running EventBus")
                    .emit_child(event)
            }

            pub fn emit_with_options<E: $crate::typed::TypedEventObject>(
                &self,
                event: E,
                queue_jump: bool,
            ) -> E {
                self.event_bus()
                    .expect("event.emit_with_options(...) requires an event attached to a running EventBus")
                    .emit_child_with_options(event, queue_jump)
            }
        }

        #[allow(non_camel_case_types, non_upper_case_globals)]
        impl $crate::typed::EventSpec for $name {
            type payload = $name;
            type event_result_type = $crate::_inner_event_result_type!($($result)*);

            const event_type: &'static str = $crate::_inner_event_type!($name; $($event_type)*);
            const event_version: &'static str = $crate::_inner_event_version!($($event_version)*);
            const event_timeout: Option<f64> = $crate::_inner_event_optional_f64!($($event_timeout)*);
            const event_slow_timeout: Option<f64> = $crate::_inner_event_optional_f64!($($event_slow_timeout)*);
            const event_concurrency: Option<$crate::types::EventConcurrencyMode> =
                $crate::_inner_event_concurrency!($($event_concurrency)*);
            const event_handler_timeout: Option<f64> =
                $crate::_inner_event_optional_f64!($($event_handler_timeout)*);
            const event_handler_slow_timeout: Option<f64> =
                $crate::_inner_event_optional_f64!($($event_handler_slow_timeout)*);
            const event_ttl: Option<f64> =
                $crate::_inner_event_optional_f64!($($event_ttl)*);
            const event_result_ttl: Option<f64> =
                $crate::_inner_event_optional_f64!($($event_result_ttl)*);
            const event_handler_concurrency: Option<$crate::types::EventHandlerConcurrencyMode> =
                $crate::_inner_event_handler_concurrency!($($event_handler_concurrency)*);
            const event_handler_completion: Option<$crate::types::EventHandlerCompletionMode> =
                $crate::_inner_event_handler_completion!($($event_handler_completion)*);
            const event_blocks_parent_completion: bool =
                $crate::_inner_event_bool_false!($($event_blocks_parent_completion)*);
            const event_result_type_schema: Option<&'static str> =
                $crate::_inner_event_optional_str!($($event_result_schema)*);
        }

        impl $crate::typed::TypedEventObject for $name {
            fn _from_inner_event(event: std::sync::Arc<$crate::base_event::BaseEvent>) -> Self {
                let mut typed: Self = $crate::serde_json::from_value(
                    $crate::typed::payload_value_from_inner_event(&event),
                )
                .expect("event payload decode failed");
                {
                    let inner = event.inner.lock();
                    typed.event_id = inner.event_id.clone();
                    typed.event_parent_id = inner.event_parent_id.clone();
                    typed.event_emitted_by_handler_id = inner.event_emitted_by_handler_id.clone();
                    typed.event_created_at = inner.event_created_at.clone();
                }
                typed.event_path = $crate::typed::Live::from_event(
                    event.clone(),
                    |event| event.event_path.clone(),
                    |event, value| event.event_path = value,
                );
                typed.event_pending_bus_count = $crate::typed::Live::from_event(
                    event.clone(),
                    |event| event.event_pending_bus_count,
                    |event, value| event.event_pending_bus_count = value,
                );
                typed.event_status = $crate::typed::Live::from_event(
                    event.clone(),
                    |event| event.event_status,
                    |event, value| event.event_status = value,
                );
                typed.event_started_at = $crate::typed::Live::from_event(
                    event.clone(),
                    |event| event.event_started_at.clone(),
                    |event, value| event.event_started_at = value,
                );
                typed.event_completed_at = $crate::typed::Live::from_event(
                    event.clone(),
                    |event| event.event_completed_at.clone(),
                    |event, value| event.event_completed_at = value,
                );
                typed.event_results = $crate::typed::Live::from_event(
                    event,
                    |event| event.event_results.clone(),
                    |event, value| event.event_results = value,
                );
                typed
            }
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! _inner_event_result_type {
    () => {
        $crate::serde_json::Value
    };
    ($result:ty) => {
        $result
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! _inner_event_type {
    ($name:ident;) => {
        stringify!($name)
    };
    ($name:ident; $event_type:literal) => {
        $event_type
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! _inner_event_version {
    () => {
        "0.0.1"
    };
    ($version:literal) => {
        $version
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! _inner_event_optional_f64 {
    () => {
        None
    };
    ($value:literal) => {
        Some($value as f64)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! _inner_event_optional_str {
    () => {
        None
    };
    ($value:literal) => {
        Some($value)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! _inner_event_bool_false {
    () => {
        false
    };
    ($value:literal) => {
        $value
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! _inner_event_concurrency {
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
macro_rules! _inner_event_handler_concurrency {
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
macro_rules! _inner_event_handler_completion {
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
