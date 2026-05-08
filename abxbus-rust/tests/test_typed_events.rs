use abxbus_rust::{
    base_event::EventResultsOptions,
    event,
    event_bus::{EventBus, FindOptions},
    typed::{BaseEventHandle, EventSpec},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, thread, time::Duration};

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
struct AddResult {
    sum: i64,
}

event! {
    struct AddEvent {
        a: i64,
        b: i64,
        event_result_type: AddResult,
    }
}

event! {
    struct TimeoutOverrideEvent {
        name: String,
        event_timeout: Option<f64>,
        event_handler_timeout: Option<f64>,
        event_result_type: serde_json::Value,
    }
}

#[test]
fn test_on_and_emit_typed_roundtrip() {
    let bus = EventBus::new(Some("TypedBus".to_string()));

    bus.on::<AddEvent, _, _>("add", |event: BaseEventHandle<AddEvent>| async move {
        let payload = event.event_payload();
        Ok(AddResult {
            sum: payload.a + payload.b,
        })
    });

    let event = bus.emit(AddEvent { a: 4, b: 9 });
    block_on(event.wait_completed());

    let first = event.first_result();
    assert_eq!(first, Some(AddResult { sum: 13 }));
    bus.stop();
}

#[test]
fn test_find_returns_typed_payload() {
    let bus = EventBus::new(Some("TypedFindBus".to_string()));

    let event = bus.emit(AddEvent { a: 7, b: 1 });
    block_on(event.wait_completed());

    let found = block_on(bus.find(AddEvent::event_type, true, None, None))
        .map(BaseEventHandle::<AddEvent>::from_base_event)
        .expect("expected typed event");
    assert_eq!(found.event_payload().a, 7);
    assert_eq!(found.event_payload().b, 1);
    bus.stop();
}

#[test]
fn test_find_type_inference() {
    let bus = EventBus::new(Some("expect_type_test_bus".to_string()));
    let bus_for_thread = bus.clone();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        bus_for_thread.emit(AddEvent { a: 57, b: 42 });
    });

    let found = block_on(bus.find(AddEvent::event_type, false, Some(1.0), None))
        .map(BaseEventHandle::<AddEvent>::from_base_event)
        .expect("expected future typed event");
    let payload = found.event_payload();
    assert_eq!(payload.a, 57);
    assert_eq!(payload.b, 42);

    let bus_for_filter = bus.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        bus_for_filter.emit(AddEvent { a: 32, b: 1 });
        bus_for_filter.emit(AddEvent { a: 51, b: 96 });
    });

    let filtered = block_on(bus.find_with_options(
        AddEvent::event_type,
        FindOptions {
            past: false,
            future: Some(1.0),
            where_predicate: Some(Arc::new(|event| {
                event
                    .inner
                    .lock()
                    .payload
                    .get("a")
                    .and_then(serde_json::Value::as_i64)
                    == Some(51)
            })),
            ..FindOptions::default()
        },
    ))
    .map(BaseEventHandle::<AddEvent>::from_base_event)
    .expect("expected filtered typed event");
    assert_eq!(filtered.event_payload().a, 51);
    assert_eq!(filtered.event_payload().b, 96);
    bus.stop();
}

#[test]
fn test_find_past_type_inference() {
    let bus = EventBus::new(Some("query_type_test_bus".to_string()));

    let event = bus.emit(AddEvent { a: 10, b: 20 });
    block_on(event.wait_completed());

    let found = block_on(bus.find(AddEvent::event_type, true, None, None))
        .map(BaseEventHandle::<AddEvent>::from_base_event)
        .expect("expected past typed event");
    let found_event_id = found.inner.inner.lock().event_id.clone();
    let emitted_event_id = event.inner.inner.lock().event_id.clone();
    assert_eq!(found_event_id, emitted_event_id);
    assert_eq!(found.event_payload(), AddEvent { a: 10, b: 20 });
    bus.stop();
}

#[test]
fn test_dispatch_type_inference() {
    let bus = EventBus::new(Some("type_inference_test_bus".to_string()));

    bus.on::<AddEvent, _, _>("add", |event: BaseEventHandle<AddEvent>| async move {
        let payload = event.event_payload();
        Ok(AddResult {
            sum: payload.a + payload.b,
        })
    });

    let dispatched_event: BaseEventHandle<AddEvent> = bus.emit(AddEvent { a: 4, b: 6 });
    assert_eq!(dispatched_event.event_payload(), AddEvent { a: 4, b: 6 });

    let result = block_on(dispatched_event.event_result(EventResultsOptions::default()))
        .expect("typed event result")
        .expect("handler result");
    assert_eq!(result, AddResult { sum: 10 });
    bus.stop();
}

#[test]
fn test_typed_event_result_accessors_decode_handler_values() {
    let bus = EventBus::new(Some("TypedResultAccessorsBus".to_string()));

    bus.on::<AddEvent, _, _>("first_add", |event: BaseEventHandle<AddEvent>| async move {
        let payload = event.event_payload();
        Ok(AddResult {
            sum: payload.a + payload.b,
        })
    });
    bus.on::<AddEvent, _, _>(
        "second_add",
        |event: BaseEventHandle<AddEvent>| async move {
            let payload = event.event_payload();
            Ok(AddResult {
                sum: payload.a * payload.b,
            })
        },
    );

    let event = bus.emit(AddEvent { a: 3, b: 5 });

    let first = block_on(event.event_result(EventResultsOptions {
        raise_if_any: false,
        raise_if_none: true,
        timeout: None,
    }))
    .expect("typed first result");
    assert_eq!(first, Some(AddResult { sum: 8 }));

    let values = block_on(event.event_results_list(EventResultsOptions {
        raise_if_any: false,
        raise_if_none: true,
        timeout: None,
    }))
    .expect("typed results list");
    assert_eq!(values, vec![AddResult { sum: 8 }, AddResult { sum: 15 }]);
    bus.stop();
}

#[test]
fn test_builtin_event_fields_in_payload_become_runtime_overrides() {
    let event = BaseEventHandle::<TimeoutOverrideEvent>::new(TimeoutOverrideEvent {
        name: "job".to_string(),
        event_timeout: Some(12.0),
        event_handler_timeout: Some(3.0),
    });
    let inner = event.inner.inner.lock();
    assert_eq!(inner.event_timeout, Some(12.0));
    assert_eq!(inner.event_handler_timeout, Some(3.0));
    assert_eq!(inner.payload.get("name"), Some(&serde_json::json!("job")));
    assert!(!inner.payload.contains_key("event_timeout"));
    assert!(!inner.payload.contains_key("event_handler_timeout"));
}
