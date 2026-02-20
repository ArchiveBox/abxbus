use std::{sync::Arc, thread, time::Duration};

use bubus_rust::{
    base_event::BaseEvent,
    event_bus::EventBus,
    types::{EventConcurrencyMode, EventHandlerConcurrencyMode},
};
use futures::executor::block_on;
use serde_json::{json, Map};

fn mk_event(event_type: &str) -> Arc<BaseEvent> {
    let mut payload = Map::new();
    payload.insert("value".to_string(), json!(1));
    BaseEvent::new(event_type.to_string(), payload)
}

#[test]
fn test_emit_and_handler_result() {
    let bus = EventBus::new(Some("BusA".to_string()));
    bus.on("work", "h1", |_event| async move { Ok(json!("ok")) });
    let event = mk_event("work");
    bus.emit(event.clone());
    block_on(event.wait_completed());

    let results = event.inner.lock().event_results.clone();
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

    let event = mk_event("work");
    {
        let mut inner = event.inner.lock();
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Parallel);
        inner.event_concurrency = Some(EventConcurrencyMode::Parallel);
    }
    bus.emit(event.clone());
    block_on(event.wait_completed());
    assert_eq!(event.inner.lock().event_results.len(), 2);
    bus.stop();
}
