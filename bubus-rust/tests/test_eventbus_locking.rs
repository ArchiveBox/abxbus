use std::{thread, time::Duration};

use bubus_rust::{
    event_bus::EventBus,
    typed::{EventSpec, TypedEvent},
    types::EventConcurrencyMode,
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Serialize, Deserialize)]
struct QPayload {
    idx: i64,
}
#[derive(Clone, Serialize, Deserialize)]
struct EmptyPayload {}
#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}
struct QEvent;
impl EventSpec for QEvent {
    type Payload = QPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "q";
}
struct WorkEvent;
impl EventSpec for WorkEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "work";
}

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
            .unwrap_or(serde_json::Value::Null);
        Ok(value)
    });

    let event1 = bus.emit::<QEvent>(QPayload { idx: 1 });
    let event2 = bus.emit_with_options::<QEvent>(QPayload { idx: 2 }, true);

    block_on(async {
        event1.wait_completed().await;
        event2.wait_completed().await;
    });

    let event1_started = event1
        .inner
        .inner
        .lock()
        .event_started_at
        .clone()
        .unwrap_or_default();
    let event2_started = event2
        .inner
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

    let event1 = TypedEvent::<WorkEvent>::new(EmptyPayload {});
    let event2 = TypedEvent::<WorkEvent>::new(EmptyPayload {});
    event1.inner.inner.lock().event_concurrency = Some(EventConcurrencyMode::BusSerial);
    event2.inner.inner.lock().event_concurrency = Some(EventConcurrencyMode::BusSerial);
    let event1 = bus.emit_existing(event1);
    let event2 = bus.emit_existing(event2);

    block_on(async {
        event1.wait_completed().await;
        event2.wait_completed().await;
    });

    let event1_started = event1
        .inner
        .inner
        .lock()
        .event_started_at
        .clone()
        .unwrap_or_default();
    let event2_started = event2
        .inner
        .inner
        .lock()
        .event_started_at
        .clone()
        .unwrap_or_default();
    assert!(event1_started <= event2_started);
    bus.stop();
}
