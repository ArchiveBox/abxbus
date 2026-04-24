use std::{sync::Arc, thread, time::Duration};

use bubus_rust::{
    base_event::BaseEvent,
    event_bus::EventBus,
    types::{EventHandlerCompletionMode, EventHandlerConcurrencyMode},
};
use futures::executor::block_on;
use serde_json::{json, Map};

fn mk_event(event_type: &str) -> Arc<BaseEvent> {
    BaseEvent::new(event_type.to_string(), Map::new())
}

#[test]
fn test_event_handler_first_serial_stops_after_first_success() {
    let bus = EventBus::new(Some("BusFirstSerial".to_string()));

    bus.on("work", "first", |_event| async move { Ok(json!("winner")) });
    bus.on("work", "second", |_event| async move {
        thread::sleep(Duration::from_millis(20));
        Ok(json!("late"))
    });

    let event = mk_event("work");
    {
        let mut inner = event.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Serial);
    }
    bus.emit(event.clone());
    block_on(event.wait_completed());

    let results = event.inner.lock().event_results.clone();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results.values().next().and_then(|r| r.result.clone()),
        Some(json!("winner"))
    );
    bus.stop();
}
