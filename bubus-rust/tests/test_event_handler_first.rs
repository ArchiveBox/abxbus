use std::{thread, time::Duration};

use bubus_rust::{
    event_bus::EventBus,
    typed::{EventSpec, TypedEvent},
    types::{EventHandlerCompletionMode, EventHandlerConcurrencyMode},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Serialize, Deserialize)]
struct EmptyPayload {}
#[derive(Clone, Serialize, Deserialize)]
struct WorkResult {
    value: String,
}
struct WorkEvent;
impl EventSpec for WorkEvent {
    type Payload = EmptyPayload;
    type Result = WorkResult;
    const EVENT_TYPE: &'static str = "work";
}

#[test]
fn test_event_handler_first_serial_stops_after_first_success() {
    let bus = EventBus::new(Some("BusFirstSerial".to_string()));

    bus.on("work", "first", |_event| async move { Ok(json!("winner")) });
    bus.on("work", "second", |_event| async move {
        thread::sleep(Duration::from_millis(20));
        Ok(json!("late"))
    });

    let event = TypedEvent::<WorkEvent>::new(EmptyPayload {});
    {
        let mut inner = event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Serial);
    }
    let emitted = bus.emit(event);
    block_on(emitted.wait_completed());

    let results = emitted.inner.inner.lock().event_results.clone();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results.values().next().and_then(|r| r.result.clone()),
        Some(json!("winner"))
    );
    bus.stop();
}
