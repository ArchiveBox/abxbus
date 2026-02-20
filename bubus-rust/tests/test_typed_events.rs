use bubus_rust::{
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
