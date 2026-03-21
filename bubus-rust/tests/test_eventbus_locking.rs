use std::{thread, time::Duration};

use bubus_rust::{base_event::BaseEvent, event_bus::EventBus, types::EventConcurrencyMode};
use futures::executor::block_on;
use serde_json::{json, Map, Value};

#[test]
fn test_queue_jump() {
    let bus = EventBus::new(Some("BusJump".to_string()));
    bus.on("q", "h", |event| async move {
        let value = event
            .inner
            .lock()
            .payload
            .get("idx")
            .cloned()
            .unwrap_or(Value::Null);
        Ok(value)
    });

    let mut p1 = Map::new();
    p1.insert("idx".into(), json!(1));
    let event1 = BaseEvent::new("q", p1);
    let mut p2 = Map::new();
    p2.insert("idx".into(), json!(2));
    let event2 = BaseEvent::new("q", p2);

    bus.emit(event1.clone());
    bus.emit_with_options(event2.clone(), true);

    block_on(async {
        event1.wait_completed().await;
        event2.wait_completed().await;
    });

    let event1_started = event1
        .inner
        .lock()
        .event_started_at
        .clone()
        .unwrap_or_default();
    let event2_started = event2
        .inner
        .lock()
        .event_started_at
        .clone()
        .unwrap_or_default();
    assert!(event2_started <= event1_started);
    bus.stop();
}

#[test]
fn test_bus_serial_processes_in_order() {
    let bus = EventBus::new(Some("BusSerial".to_string()));

    bus.on("work", "slow", |_event| async move {
        thread::sleep(Duration::from_millis(15));
        Ok(json!(1))
    });

    let event1 = BaseEvent::new("work", Map::new());
    let event2 = BaseEvent::new("work", Map::new());
    event1.inner.lock().event_concurrency = Some(EventConcurrencyMode::BusSerial);
    event2.inner.lock().event_concurrency = Some(EventConcurrencyMode::BusSerial);
    bus.emit(event1.clone());
    bus.emit(event2.clone());

    block_on(async {
        event1.wait_completed().await;
        event2.wait_completed().await;
    });

    let event1_started = event1
        .inner
        .lock()
        .event_started_at
        .clone()
        .unwrap_or_default();
    let event2_started = event2
        .inner
        .lock()
        .event_started_at
        .clone()
        .unwrap_or_default();
    assert!(event1_started <= event2_started);
    bus.stop();
}
