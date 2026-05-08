use abxbus_rust::event;
use std::{thread, time::Duration};

use abxbus_rust::{
    event_bus::EventBus,
    types::{EventConcurrencyMode, EventHandlerConcurrencyMode},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Serialize, Deserialize)]
struct WorkResult {
    value: i64,
}

event! {
    struct WorkEvent {
        value: i64,
        event_result_type: WorkResult,
        event_type: "work",
    }
}
#[test]
fn test_emit_and_handler_result() {
    let bus = EventBus::new(Some("BusA".to_string()));
    bus.on_raw("work", "h1", |_event| async move { Ok(json!("ok")) });
    let event = bus.emit(WorkEvent {
        value: 1,
        ..Default::default()
    });
    block_on(event.done());

    let results = event.inner.inner.lock().event_results.clone();
    assert_eq!(results.len(), 1);
    let first = results.values().next().expect("missing first result");
    assert_eq!(first.result, Some(json!("ok")));
    bus.stop();
}

#[test]
fn test_parallel_handler_concurrency() {
    let bus = EventBus::new(Some("BusPar".to_string()));

    bus.on_raw("work", "h1", |_event| async move {
        thread::sleep(Duration::from_millis(20));
        Ok(json!(1))
    });
    bus.on_raw("work", "h2", |_event| async move {
        thread::sleep(Duration::from_millis(20));
        Ok(json!(2))
    });

    let event = WorkEvent {
        value: 1,
        event_handler_concurrency: Some(EventHandlerConcurrencyMode::Parallel),
        event_concurrency: Some(EventConcurrencyMode::Parallel),
        ..Default::default()
    };
    let emitted = bus.emit(event);
    block_on(emitted.done());
    assert_eq!(emitted.inner.inner.lock().event_results.len(), 2);
    bus.stop();
}
