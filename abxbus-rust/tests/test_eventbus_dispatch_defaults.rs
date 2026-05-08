use abxbus_rust::{
    event_bus::{EventBus, EventBusOptions},
    typed::{BaseEventHandle, EventSpec},
    types::{EventConcurrencyMode, EventHandlerCompletionMode, EventHandlerConcurrencyMode},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Serialize, Deserialize)]
struct Payload {
    value: i64,
}
#[derive(Clone, Serialize, Deserialize)]
struct ResultT {
    value: String,
}
struct WorkEvent;
impl EventSpec for WorkEvent {
    type payload = Payload;
    type event_result_type = ResultT;
    const event_type: &'static str = "work";
}

struct ConcurrencyOverrideEvent;
impl EventSpec for ConcurrencyOverrideEvent {
    type payload = Payload;
    type event_result_type = ResultT;
    const event_type: &'static str = "ConcurrencyOverrideEvent";
    const event_concurrency: Option<EventConcurrencyMode> =
        Some(EventConcurrencyMode::GlobalSerial);
}

struct HandlerOverrideEvent;
impl EventSpec for HandlerOverrideEvent {
    type payload = Payload;
    type event_result_type = ResultT;
    const event_type: &'static str = "HandlerOverrideEvent";
    const event_handler_concurrency: Option<EventHandlerConcurrencyMode> =
        Some(EventHandlerConcurrencyMode::Serial);
    const event_handler_completion: Option<EventHandlerCompletionMode> =
        Some(EventHandlerCompletionMode::All);
}

struct ConfiguredEvent;
impl EventSpec for ConfiguredEvent {
    type payload = Payload;
    type event_result_type = ResultT;
    const event_type: &'static str = "ConfiguredEvent";
    const event_version: &'static str = "2.0.0";
    const event_timeout: Option<f64> = Some(12.0);
    const event_slow_timeout: Option<f64> = Some(30.0);
    const event_handler_timeout: Option<f64> = Some(3.0);
    const event_handler_slow_timeout: Option<f64> = Some(4.0);
    const event_blocks_parent_completion: bool = true;
}

#[test]
fn test_bus_default_handler_settings_are_applied() {
    let bus = EventBus::new(Some("BusDefaults".to_string()));

    bus.on_raw("work", "h1", |_event| async move { Ok(json!("ok")) });
    let event = BaseEventHandle::<WorkEvent>::new(Payload { value: 1 });
    {
        let mut inner = event.inner.inner.lock();
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Serial);
        inner.event_handler_completion = Some(EventHandlerCompletionMode::All);
    }
    let event = bus.emit(event);
    block_on(event.done());

    assert_eq!(event.inner.inner.lock().event_results.len(), 1);
    bus.stop();
}

#[test]
fn test_event_concurrency_remains_unset_on_dispatch_and_resolves_during_processing() {
    let bus = EventBus::new_with_options(
        Some("EventConcurrencyDefaultBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    bus.on_raw("work", "h1", |_event| async move { Ok(json!("ok")) });

    let implicit = BaseEventHandle::<WorkEvent>::new(Payload { value: 1 });
    let explicit_none = BaseEventHandle::<WorkEvent>::new(Payload { value: 2 });
    explicit_none.inner.inner.lock().event_concurrency = None;

    let implicit = bus.emit(implicit);
    let explicit_none = bus.emit(explicit_none);

    assert_eq!(implicit.inner.inner.lock().event_concurrency, None);
    assert_eq!(explicit_none.inner.inner.lock().event_concurrency, None);

    block_on(implicit.done());
    block_on(explicit_none.done());
    assert_eq!(implicit.inner.inner.lock().event_results.len(), 1);
    assert_eq!(explicit_none.inner.inner.lock().event_results.len(), 1);
    bus.stop();
}

#[test]
fn test_event_concurrency_class_override_beats_bus_default() {
    let bus = EventBus::new_with_options(
        Some("EventConcurrencyOverrideBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    bus.on_raw("ConcurrencyOverrideEvent", "h1", |_event| async move {
        Ok(json!("ok"))
    });

    let event = BaseEventHandle::<ConcurrencyOverrideEvent>::new(Payload { value: 1 });
    let event = bus.emit(event);

    assert_eq!(
        event.inner.inner.lock().event_concurrency,
        Some(EventConcurrencyMode::GlobalSerial)
    );
    block_on(event.done());
    assert_eq!(event.inner.inner.lock().event_results.len(), 1);
    bus.stop();
}

#[test]
fn test_handler_defaults_remain_unset_on_dispatch_and_resolve_during_processing() {
    let bus = EventBus::new_with_options(
        Some("HandlerDefaultsBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            event_handler_completion: EventHandlerCompletionMode::First,
            ..EventBusOptions::default()
        },
    );
    bus.on_raw("work", "h1", |_event| async move { Ok(json!("ok")) });

    let implicit = BaseEventHandle::<WorkEvent>::new(Payload { value: 1 });
    let explicit_none = BaseEventHandle::<WorkEvent>::new(Payload { value: 2 });
    {
        let mut inner = explicit_none.inner.inner.lock();
        inner.event_handler_concurrency = None;
        inner.event_handler_completion = None;
    }

    let implicit = bus.emit(implicit);
    let explicit_none = bus.emit(explicit_none);

    assert_eq!(implicit.inner.inner.lock().event_handler_concurrency, None);
    assert_eq!(implicit.inner.inner.lock().event_handler_completion, None);
    assert_eq!(
        explicit_none.inner.inner.lock().event_handler_concurrency,
        None
    );
    assert_eq!(
        explicit_none.inner.inner.lock().event_handler_completion,
        None
    );

    block_on(implicit.done());
    block_on(explicit_none.done());
    assert_eq!(implicit.inner.inner.lock().event_results.len(), 1);
    assert_eq!(explicit_none.inner.inner.lock().event_results.len(), 1);
    bus.stop();
}

#[test]
fn test_handler_class_override_beats_bus_defaults() {
    let bus = EventBus::new_with_options(
        Some("HandlerDefaultsOverrideBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            event_handler_completion: EventHandlerCompletionMode::First,
            ..EventBusOptions::default()
        },
    );
    bus.on_raw("HandlerOverrideEvent", "h1", |_event| async move {
        Ok(json!("ok"))
    });

    let event = BaseEventHandle::<HandlerOverrideEvent>::new(Payload { value: 1 });
    let event = bus.emit(event);

    assert_eq!(
        event.inner.inner.lock().event_handler_concurrency,
        Some(EventHandlerConcurrencyMode::Serial)
    );
    assert_eq!(
        event.inner.inner.lock().event_handler_completion,
        Some(EventHandlerCompletionMode::All)
    );
    block_on(event.done());
    assert_eq!(event.inner.inner.lock().event_results.len(), 1);
    bus.stop();
}

#[test]
fn test_handler_class_override_beats_bus_default() {
    test_handler_class_override_beats_bus_defaults();
}

#[test]
fn test_event_instance_override_beats_typed_event_defaults() {
    let event = BaseEventHandle::<ConcurrencyOverrideEvent>::new(Payload { value: 1 });
    assert_eq!(
        event.inner.inner.lock().event_concurrency,
        Some(EventConcurrencyMode::GlobalSerial)
    );

    event.inner.inner.lock().event_concurrency = Some(EventConcurrencyMode::Parallel);
    assert_eq!(
        event.inner.inner.lock().event_concurrency,
        Some(EventConcurrencyMode::Parallel)
    );
}

#[test]
fn test_typed_event_config_defaults_populate_base_event_fields() {
    let event = BaseEventHandle::<ConfiguredEvent>::new(Payload { value: 1 });
    let inner = event.inner.inner.lock();
    assert_eq!(inner.event_version, "2.0.0");
    assert_eq!(inner.event_timeout, Some(12.0));
    assert_eq!(inner.event_slow_timeout, Some(30.0));
    assert_eq!(inner.event_handler_timeout, Some(3.0));
    assert_eq!(inner.event_handler_slow_timeout, Some(4.0));
    assert!(inner.event_blocks_parent_completion);
}

#[test]
fn test_null_event_concurrency_null_resolves_to_bus_defaults() {
    test_event_concurrency_remains_unset_on_dispatch_and_resolves_during_processing();
}

#[test]
fn test_null_event_handler_concurrency_null_resolves_to_bus_defaults() {
    test_handler_defaults_remain_unset_on_dispatch_and_resolve_during_processing();
}
