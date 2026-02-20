use std::{thread, time::Duration};

use bubus_rust::{base_event::BaseEvent, event_bus::EventBus};
use futures::executor::block_on;
use serde_json::{json, Map};

#[test]
fn test_find_past_match_returns_event() {
    let bus = EventBus::new(Some("FindBus".to_string()));
    bus.on("work", "h1", |_event| async move { Ok(json!("ok")) });

    let event = BaseEvent::new("work", Map::new());
    bus.emit_raw(event.clone());
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
        let event = BaseEvent::new("future_event", Map::new());
        bus_for_emit.emit_raw(event);
    });

    let found = block_on(bus.find("future_event", false, Some(0.5), None));
    assert!(found.is_some());
    bus.stop();
}
