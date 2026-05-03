use std::{thread, time::Duration};

use bubus_rust::{
    event_bus::EventBus,
    typed::{EventSpec, TypedEvent},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Serialize, Deserialize)]
struct EmptyPayload {}
#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}
struct WorkEvent;
impl EventSpec for WorkEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "work";
}
struct FutureEvent;
impl EventSpec for FutureEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "future_event";
}

#[test]
fn test_find_past_match_returns_event() {
    let bus = EventBus::new(Some("FindBus".to_string()));
    bus.on("work", "h1", |_event| async move { Ok(json!("ok")) });

    let event = bus.emit::<WorkEvent>(TypedEvent::<WorkEvent>::new(EmptyPayload {}));
    block_on(event.wait_completed());

    let found = block_on(bus.find("work", true, None, None));
    assert!(found.is_some());
    assert_eq!(found.expect("missing").inner.lock().event_type, "work");

    bus.stop();
}

#[test]
fn test_find_future_waits_for_new_event() {
    let bus = EventBus::new(Some("FindFutureBus".to_string()));
    let bus_for_emit = bus.clone();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(30));
        bus_for_emit.emit::<FutureEvent>(TypedEvent::<FutureEvent>::new(EmptyPayload {}));
    });

    let found = block_on(bus.find("future_event", false, Some(0.5), None));
    assert!(found.is_some());
    bus.stop();
}
