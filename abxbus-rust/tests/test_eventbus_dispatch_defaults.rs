use abxbus_rust::event;
use abxbus_rust::{
    event_bus::{EventBus, EventBusOptions},
    types::{EventConcurrencyMode, EventHandlerCompletionMode, EventHandlerConcurrencyMode},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Serialize, Deserialize)]
struct ResultT {
    value: String,
}
event! {
    struct WorkEvent {
        value: i64,
        event_result_type: ResultT,
        event_type: "work",
    }
}
event! {
    struct ConcurrencyOverrideEvent {
        value: i64,
        event_result_type: ResultT,
        event_type: "ConcurrencyOverrideEvent",
        event_concurrency: global_serial,
    }
}
event! {
    struct HandlerOverrideEvent {
        value: i64,
        event_result_type: ResultT,
        event_type: "HandlerOverrideEvent",
        event_handler_concurrency: serial,
        event_handler_completion: all,
    }
}
event! {
    struct ConfiguredEvent {
        value: i64,
        event_result_type: ResultT,
        event_type: "ConfiguredEvent",
        event_version: "2.0.0",
        event_timeout: 12.0,
        event_slow_timeout: 30.0,
        event_handler_timeout: 3.0,
        event_handler_slow_timeout: 4.0,
        event_blocks_parent_completion: true,
    }
}
#[test]
fn test_bus_default_handler_settings_are_applied() {
    let bus = EventBus::new(Some("BusDefaults".to_string()));

    bus.on_raw("work", "h1", |_event| async move { Ok(json!("ok")) });
    let event = WorkEvent {
        value: 1,
        event_handler_concurrency: Some(EventHandlerConcurrencyMode::Serial),
        event_handler_completion: Some(EventHandlerCompletionMode::All),
        ..Default::default()
    };
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

    let implicit = WorkEvent {
        value: 1,
        ..Default::default()
    };
    let explicit_none = WorkEvent {
        value: 2,
        event_concurrency: None,
        ..Default::default()
    };
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

    let event = ConcurrencyOverrideEvent {
        value: 1,
        ..Default::default()
    };
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

    let implicit = WorkEvent {
        value: 1,
        ..Default::default()
    };
    let explicit_none = WorkEvent {
        value: 2,
        event_handler_concurrency: None,
        event_handler_completion: None,
        ..Default::default()
    };

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

    let event = HandlerOverrideEvent {
        value: 1,
        ..Default::default()
    };
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
    let bus = EventBus::new(Some("EventInstanceOverrideBus".to_string()));
    let class_default = bus.emit(ConcurrencyOverrideEvent {
        value: 1,
        ..Default::default()
    });
    assert_eq!(
        class_default.inner.inner.lock().event_concurrency,
        Some(EventConcurrencyMode::GlobalSerial)
    );

    let event = bus.emit(ConcurrencyOverrideEvent {
        value: 1,
        event_concurrency: Some(EventConcurrencyMode::Parallel),
        ..Default::default()
    });
    assert_eq!(
        event.inner.inner.lock().event_concurrency,
        Some(EventConcurrencyMode::Parallel)
    );
    bus.stop();
}

#[test]
fn test_typed_event_config_defaults_populate_base_event_fields() {
    let bus = EventBus::new(Some("ConfiguredEventDefaultsBus".to_string()));
    let event = bus.emit(ConfiguredEvent {
        value: 1,
        ..Default::default()
    });
    let inner = event.inner.inner.lock();
    assert_eq!(inner.event_version, "2.0.0");
    assert_eq!(inner.event_timeout, Some(12.0));
    assert_eq!(inner.event_slow_timeout, Some(30.0));
    assert_eq!(inner.event_handler_timeout, Some(3.0));
    assert_eq!(inner.event_handler_slow_timeout, Some(4.0));
    assert!(inner.event_blocks_parent_completion);
    drop(inner);
    bus.stop();
}

#[test]
fn test_null_event_concurrency_null_resolves_to_bus_defaults() {
    test_event_concurrency_remains_unset_on_dispatch_and_resolves_during_processing();
}

#[test]
fn test_null_event_handler_concurrency_null_resolves_to_bus_defaults() {
    test_handler_defaults_remain_unset_on_dispatch_and_resolve_during_processing();
}
