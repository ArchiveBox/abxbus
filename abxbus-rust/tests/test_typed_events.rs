use abxbus_rust::{
    base_event::EventResultsOptions,
    event_bus::{EventBus, FindOptions},
    typed::{EventSpec, TypedEvent},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, thread, time::Duration};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct AddPayload {
    a: i64,
    b: i64,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
struct AddResult {
    sum: i64,
}

struct AddEvent;

impl EventSpec for AddEvent {
    type Payload = AddPayload;
    type Result = AddResult;

    const EVENT_TYPE: &'static str = "AddEvent";
}

#[test]
fn test_on_typed_and_emit_typed_roundtrip() {
    let bus = EventBus::new(Some("TypedBus".to_string()));

    bus.on_typed::<AddEvent, _, _>("add", |event: TypedEvent<AddEvent>| async move {
        let payload = event.payload();
        Ok(AddResult {
            sum: payload.a + payload.b,
        })
    });

    let event = bus.emit::<AddEvent>(TypedEvent::<AddEvent>::new(AddPayload { a: 4, b: 9 }));
    block_on(event.wait_completed());

    let first = event.first_result();
    assert_eq!(first, Some(AddResult { sum: 13 }));
    bus.stop();
}

#[test]
fn test_find_typed_returns_typed_payload() {
    let bus = EventBus::new(Some("TypedFindBus".to_string()));

    let event = bus.emit::<AddEvent>(TypedEvent::<AddEvent>::new(AddPayload { a: 7, b: 1 }));
    block_on(event.wait_completed());

    let found = block_on(bus.find_typed::<AddEvent>(true, None)).expect("expected typed event");
    assert_eq!(found.payload().a, 7);
    assert_eq!(found.payload().b, 1);
    bus.stop();
}

#[test]
fn test_find_type_inference() {
    let bus = EventBus::new(Some("expect_type_test_bus".to_string()));
    let bus_for_thread = bus.clone();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        bus_for_thread.emit::<AddEvent>(TypedEvent::<AddEvent>::new(AddPayload { a: 57, b: 42 }));
    });

    let found = block_on(bus.find_typed::<AddEvent>(false, Some(1.0)))
        .expect("expected future typed event");
    let payload: AddPayload = found.payload();
    assert_eq!(payload.a, 57);
    assert_eq!(payload.b, 42);

    let bus_for_filter = bus.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        bus_for_filter.emit::<AddEvent>(TypedEvent::<AddEvent>::new(AddPayload { a: 32, b: 1 }));
        bus_for_filter.emit::<AddEvent>(TypedEvent::<AddEvent>::new(AddPayload { a: 51, b: 96 }));
    });

    let filtered = block_on(bus.find_with_options(
        AddEvent::EVENT_TYPE,
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
    .map(TypedEvent::<AddEvent>::from_base_event)
    .expect("expected filtered typed event");
    assert_eq!(filtered.payload().a, 51);
    assert_eq!(filtered.payload().b, 96);
    bus.stop();
}

#[test]
fn test_find_past_type_inference() {
    let bus = EventBus::new(Some("query_type_test_bus".to_string()));

    let event = bus.emit::<AddEvent>(TypedEvent::<AddEvent>::new(AddPayload { a: 10, b: 20 }));
    block_on(event.wait_completed());

    let found =
        block_on(bus.find_typed::<AddEvent>(true, None)).expect("expected past typed event");
    let found_event_id = found.inner.inner.lock().event_id.clone();
    let emitted_event_id = event.inner.inner.lock().event_id.clone();
    assert_eq!(found_event_id, emitted_event_id);
    assert_eq!(found.payload(), AddPayload { a: 10, b: 20 });
    bus.stop();
}

#[test]
fn test_dispatch_type_inference() {
    let bus = EventBus::new(Some("type_inference_test_bus".to_string()));

    bus.on_typed::<AddEvent, _, _>("add", |event: TypedEvent<AddEvent>| async move {
        let payload = event.payload();
        Ok(AddResult {
            sum: payload.a + payload.b,
        })
    });

    let original_event = TypedEvent::<AddEvent>::new(AddPayload { a: 4, b: 6 });
    let original_event_id = original_event.inner.inner.lock().event_id.clone();
    let dispatched_event: TypedEvent<AddEvent> = bus.emit(original_event);
    assert_eq!(
        dispatched_event.inner.inner.lock().event_id,
        original_event_id
    );
    assert_eq!(dispatched_event.payload(), AddPayload { a: 4, b: 6 });

    let result = block_on(dispatched_event.event_result(EventResultsOptions::default()))
        .expect("typed event result")
        .expect("handler result");
    assert_eq!(result, AddResult { sum: 10 });
    bus.stop();
}

#[test]
fn test_typed_event_result_accessors_decode_handler_values() {
    let bus = EventBus::new(Some("TypedResultAccessorsBus".to_string()));

    bus.on_typed::<AddEvent, _, _>("first_add", |event: TypedEvent<AddEvent>| async move {
        let payload = event.payload();
        Ok(AddResult {
            sum: payload.a + payload.b,
        })
    });
    bus.on_typed::<AddEvent, _, _>("second_add", |event: TypedEvent<AddEvent>| async move {
        let payload = event.payload();
        Ok(AddResult {
            sum: payload.a * payload.b,
        })
    });

    let event = bus.emit::<AddEvent>(TypedEvent::<AddEvent>::new(AddPayload { a: 3, b: 5 }));

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
