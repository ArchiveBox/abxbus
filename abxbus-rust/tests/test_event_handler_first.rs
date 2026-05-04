use std::{thread, time::Duration};

use abxbus_rust::{
    event_bus::EventBus,
    typed::{EventSpec, TypedEvent},
    types::{EventHandlerCompletionMode, EventHandlerConcurrencyMode},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

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

struct ValueEvent;
impl EventSpec for ValueEvent {
    type Payload = EmptyPayload;
    type Result = Value;
    const EVENT_TYPE: &'static str = "value";
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

#[test]
fn test_event_first_skips_none_result_and_uses_next_winner() {
    let bus = EventBus::new(Some("BusFirstSkipsNull".to_string()));

    bus.on("value", "none", |_event| async move { Ok(Value::Null) });
    bus.on(
        "value",
        "winner",
        |_event| async move { Ok(json!("winner")) },
    );
    bus.on("value", "late", |_event| async move { Ok(json!("late")) });

    let event = TypedEvent::<ValueEvent>::new(EmptyPayload {});
    {
        let mut inner = event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Serial);
    }
    let emitted = bus.emit(event);
    block_on(emitted.wait_completed());

    let results = emitted.inner.inner.lock().event_results.clone();
    assert_eq!(results.len(), 2);
    assert_eq!(emitted.first_result(), Some(json!("winner")));
    assert!(results
        .values()
        .any(|result| result.result == Some(Value::Null)));
    assert!(results
        .values()
        .any(|result| result.result == Some(json!("winner"))));
    bus.stop();
}

#[test]
fn test_event_first_preserves_false_and_empty_string_results() {
    let false_bus = EventBus::new(Some("BusFirstFalse".to_string()));
    false_bus.on(
        "value",
        "false_winner",
        |_event| async move { Ok(json!(false)) },
    );
    false_bus.on("value", "late", |_event| async move { Ok(json!("late")) });

    let false_event = TypedEvent::<ValueEvent>::new(EmptyPayload {});
    {
        let mut inner = false_event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Serial);
    }
    let false_event = false_bus.emit(false_event);
    block_on(false_event.wait_completed());
    assert_eq!(false_event.first_result(), Some(json!(false)));
    assert_eq!(false_event.inner.inner.lock().event_results.len(), 1);
    false_bus.stop();

    let empty_bus = EventBus::new(Some("BusFirstEmptyString".to_string()));
    empty_bus.on(
        "value",
        "empty_winner",
        |_event| async move { Ok(json!("")) },
    );
    empty_bus.on("value", "late", |_event| async move { Ok(json!("late")) });

    let empty_event = TypedEvent::<ValueEvent>::new(EmptyPayload {});
    {
        let mut inner = empty_event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Serial);
    }
    let empty_event = empty_bus.emit(empty_event);
    block_on(empty_event.wait_completed());
    assert_eq!(empty_event.first_result(), Some(json!("")));
    assert_eq!(empty_event.inner.inner.lock().event_results.len(), 1);
    empty_bus.stop();
}
