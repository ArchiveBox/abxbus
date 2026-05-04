use abxbus_rust::{
    base_event::EventResultsOptions,
    event_bus::EventBus,
    typed::{EventSpec, TypedEvent},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
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
    }))
    .expect("typed first result");
    assert_eq!(first, Some(AddResult { sum: 8 }));

    let values = block_on(event.event_results_list(EventResultsOptions {
        raise_if_any: false,
        raise_if_none: true,
    }))
    .expect("typed results list");
    assert_eq!(values, vec![AddResult { sum: 8 }, AddResult { sum: 15 }]);
    bus.stop();
}
