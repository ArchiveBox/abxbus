use abxbus_rust::{
    event_bus::EventBus,
    event_result::EventResultStatus,
    typed::{EventSpec, TypedEvent},
    types::EventStatus,
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::{Arc, Mutex};

#[derive(Clone, Serialize, Deserialize)]
struct EmptyPayload {}

#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}

struct TestEvent;
impl EventSpec for TestEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "TestEvent";
}

struct Event1;
impl EventSpec for Event1 {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "Event1";
}

struct Event2;
impl EventSpec for Event2 {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "Event2";
}

struct OrphanEvent;
impl EventSpec for OrphanEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "OrphanEvent";
}

#[test]
fn test_handler_error_is_captured_and_does_not_prevent_other_handlers_from_running() {
    let bus = EventBus::new(Some("ErrorIsolationBus".to_string()));
    let results = Arc::new(Mutex::new(Vec::new()));
    let results_for_handler = results.clone();

    bus.on("TestEvent", "failing_handler", |_event| async move {
        Err("Expected to fail - testing error handling".to_string())
    });
    bus.on("TestEvent", "working_handler", move |_event| {
        let results = results_for_handler.clone();
        async move {
            results.lock().expect("results lock").push("success");
            Ok(json!("worked"))
        }
    });

    let event = bus.emit::<TestEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());

    let event_results = event.inner.inner.lock().event_results.clone();
    assert_eq!(event_results.len(), 2);

    let failing_result = event_results
        .values()
        .find(|result| result.handler.handler_name == "failing_handler")
        .expect("failing_handler result should exist");
    assert_eq!(failing_result.status, EventResultStatus::Error);
    assert!(failing_result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("Expected to fail"));

    let working_result = event_results
        .values()
        .find(|result| result.handler.handler_name == "working_handler")
        .expect("working_handler result should exist");
    assert_eq!(working_result.status, EventResultStatus::Completed);
    assert_eq!(working_result.result, Some(json!("worked")));
    assert_eq!(
        results.lock().expect("results lock").as_slice(),
        &["success"]
    );
    bus.stop();
}

#[test]
fn test_event_event_errors_collects_handler_errors() {
    let bus = EventBus::new(Some("ErrorCollectionBus".to_string()));

    bus.on("TestEvent", "handler_a", |_event| async move {
        Err("error_a".to_string())
    });
    bus.on("TestEvent", "handler_b", |_event| async move {
        Err("error_b".to_string())
    });
    bus.on(
        "TestEvent",
        "handler_c",
        |_event| async move { Ok(json!("ok")) },
    );

    let event = bus.emit::<TestEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());

    let mut errors = event.inner.event_errors();
    errors.sort();
    assert_eq!(errors.len(), 2);
    assert!(errors.iter().any(|error| error.contains("error_a")));
    assert!(errors.iter().any(|error| error.contains("error_b")));
    bus.stop();
}

#[test]
fn test_handler_error_does_not_prevent_event_completion() {
    let bus = EventBus::new(Some("ErrorCompletionBus".to_string()));

    bus.on("TestEvent", "failing_handler", |_event| async move {
        Err("handler failed".to_string())
    });

    let event = bus.emit::<TestEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());

    assert_eq!(
        event.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    assert!(event.inner.inner.lock().event_completed_at.is_some());
    assert_eq!(event.inner.event_errors().len(), 1);
    bus.stop();
}

#[test]
fn test_error_in_one_event_does_not_affect_subsequent_queued_events() {
    let bus = EventBus::new(Some("ErrorQueueBus".to_string()));

    bus.on("Event1", "event1_handler", |_event| async move {
        Err("event1 handler failed".to_string())
    });
    bus.on("Event2", "event2_handler", |_event| async move {
        Ok(json!("event2 ok"))
    });

    let event_1 = bus.emit::<Event1>(TypedEvent::new(EmptyPayload {}));
    let event_2 = bus.emit::<Event2>(TypedEvent::new(EmptyPayload {}));
    block_on(bus.wait_until_idle(None));

    assert_eq!(
        event_1.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    assert_eq!(event_1.inner.event_errors().len(), 1);
    assert_eq!(
        event_2.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    assert_eq!(event_2.inner.event_errors().len(), 0);
    let result = event_2
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("event2 result");
    assert_eq!(result.status, EventResultStatus::Completed);
    assert_eq!(result.result, Some(json!("event2 ok")));
    bus.stop();
}

#[test]
fn test_event_with_no_handlers_completes_without_errors() {
    let bus = EventBus::new(Some("NoHandlerBus".to_string()));

    let event = bus.emit::<OrphanEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());

    assert_eq!(
        event.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    assert_eq!(event.inner.inner.lock().event_results.len(), 0);
    assert_eq!(event.inner.event_errors().len(), 0);
    bus.stop();
}

#[test]
fn test_error_handler_result_fields_are_populated_correctly() {
    let bus = EventBus::new(Some("ErrorFieldsBus".to_string()));

    bus.on("TestEvent", "my_handler", |_event| async move {
        Err("RangeError: out of range".to_string())
    });

    let event = bus.emit::<TestEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());

    let result = event
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("handler result");
    assert_eq!(result.status, EventResultStatus::Error);
    assert_eq!(result.handler.handler_name, "my_handler");
    assert_eq!(result.handler.eventbus_name, "ErrorFieldsBus");
    assert!(result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("out of range"));
    let payload = result.to_flat_json_value();
    assert_eq!(payload["error"]["type"], "Error");
    assert_eq!(payload["error"]["message"], "RangeError: out of range");
    assert!(result.started_at.is_some());
    assert!(result.completed_at.is_some());
    bus.stop();
}
