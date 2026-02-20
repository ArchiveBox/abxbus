use std::{thread, time::Duration};

use bubus_rust::{
    event_bus::EventBus,
    typed::{EventSpec, TypedEvent},
    types::{EventConcurrencyMode, EventHandlerConcurrencyMode},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Serialize, Deserialize)]
struct WorkPayload {
    value: i64,
}

#[derive(Clone, Serialize, Deserialize)]
struct WorkResult {
    value: i64,
}

struct WorkEvent;
impl EventSpec for WorkEvent {
    type Payload = WorkPayload;
    type Result = WorkResult;
    const EVENT_TYPE: &'static str = "work";
}

#[test]
fn test_emit_and_handler_result() {
    let bus = EventBus::new(Some("BusA".to_string()));
    bus.on("work", "h1", |_event| async move { Ok(json!("ok")) });
    let event = bus.emit::<WorkEvent>(WorkPayload { value: 1 });
    block_on(event.wait_completed());

    let results = event.inner.inner.lock().event_results.clone();
    assert_eq!(results.len(), 1);
    let first = results.values().next().expect("missing first result");
    assert_eq!(first.result, Some(json!("ok")));
    bus.stop();
}

#[test]
fn test_parallel_handler_concurrency() {
    let bus = EventBus::new(Some("BusPar".to_string()));

    bus.on("work", "h1", |_event| async move {
        thread::sleep(Duration::from_millis(20));
        Ok(json!(1))
    });
    bus.on("work", "h2", |_event| async move {
        thread::sleep(Duration::from_millis(20));
        Ok(json!(2))
    });

    let event = TypedEvent::<WorkEvent>::new(WorkPayload { value: 1 });
    {
        let mut inner = event.inner.inner.lock();
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Parallel);
        inner.event_concurrency = Some(EventConcurrencyMode::Parallel);
    }
    let emitted = bus.emit_existing(event);
    block_on(emitted.wait_completed());
    assert_eq!(emitted.inner.inner.lock().event_results.len(), 2);
    bus.stop();
}
