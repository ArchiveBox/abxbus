use abxbus_rust::{
    base_event::BaseEvent,
    event_bus::{EventBus, EventBusOptions},
    event_result::EventResultStatus,
    typed::{BaseEventHandle, EventSpec},
    types::{EventHandlerCompletionMode, EventHandlerConcurrencyMode, EventStatus},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

#[derive(Clone, Serialize, Deserialize)]
struct EmptyPayload {}

#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}

struct TestEvent;
impl EventSpec for TestEvent {
    type payload = EmptyPayload;
    type event_result_type = EmptyResult;
    const event_type: &'static str = "TestEvent";
}

struct Event1;
impl EventSpec for Event1 {
    type payload = EmptyPayload;
    type event_result_type = EmptyResult;
    const event_type: &'static str = "Event1";
}

struct Event2;
impl EventSpec for Event2 {
    type payload = EmptyPayload;
    type event_result_type = EmptyResult;
    const event_type: &'static str = "Event2";
}

struct OrphanEvent;
impl EventSpec for OrphanEvent {
    type payload = EmptyPayload;
    type event_result_type = EmptyResult;
    const event_type: &'static str = "OrphanEvent";
}

struct ForwardEvent;
impl EventSpec for ForwardEvent {
    type payload = EmptyPayload;
    type event_result_type = EmptyResult;
    const event_type: &'static str = "ForwardEvent";
}

struct TimeoutTaxonomyEvent;
impl EventSpec for TimeoutTaxonomyEvent {
    type payload = EmptyPayload;
    type event_result_type = String;
    const event_type: &'static str = "TimeoutTaxonomyEvent";
    const event_timeout: Option<f64> = Some(0.2);
    const event_handler_timeout: Option<f64> = Some(0.01);
}

fn schema_event(event_type: &str, schema: Value) -> Arc<BaseEvent> {
    let event = BaseEvent::new(event_type, serde_json::Map::new());
    event.inner.lock().event_result_type = Some(schema);
    event
}

#[test]
fn test_handler_error_is_captured_and_does_not_prevent_other_handlers_from_running() {
    let bus = EventBus::new(Some("ErrorIsolationBus".to_string()));
    let results = Arc::new(Mutex::new(Vec::new()));
    let results_for_handler = results.clone();

    bus.on_raw("TestEvent", "failing_handler", |_event| async move {
        Err("Expected to fail - testing error handling".to_string())
    });
    bus.on_raw("TestEvent", "working_handler", move |_event| {
        let results = results_for_handler.clone();
        async move {
            results.lock().expect("results lock").push("success");
            Ok(json!("worked"))
        }
    });

    let event = bus.emit(BaseEventHandle::<TestEvent>::new(EmptyPayload {}));
    block_on(event.done());

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

    bus.on_raw("TestEvent", "handler_a", |_event| async move {
        Err("error_a".to_string())
    });
    bus.on_raw("TestEvent", "handler_b", |_event| async move {
        Err("error_b".to_string())
    });
    bus.on_raw(
        "TestEvent",
        "handler_c",
        |_event| async move { Ok(json!("ok")) },
    );

    let event = bus.emit(BaseEventHandle::<TestEvent>::new(EmptyPayload {}));
    block_on(event.done());

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

    bus.on_raw("TestEvent", "failing_handler", |_event| async move {
        Err("handler failed".to_string())
    });

    let event = bus.emit(BaseEventHandle::<TestEvent>::new(EmptyPayload {}));
    block_on(event.done());

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

    bus.on_raw("Event1", "event1_handler", |_event| async move {
        Err("event1 handler failed".to_string())
    });
    bus.on_raw("Event2", "event2_handler", |_event| async move {
        Ok(json!("event2 ok"))
    });

    let event_1 = bus.emit(BaseEventHandle::<Event1>::new(EmptyPayload {}));
    let event_2 = bus.emit(BaseEventHandle::<Event2>::new(EmptyPayload {}));
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
fn test_async_handler_rejection_is_captured_as_error() {
    let bus = EventBus::new(Some("AsyncErrorBus".to_string()));

    bus.on_raw("TestEvent", "async_failing_handler", |_event| async move {
        thread::sleep(Duration::from_millis(1));
        Err("async rejection".to_string())
    });

    let event = bus.emit(BaseEventHandle::<TestEvent>::new(EmptyPayload {}));
    block_on(event.done());

    assert_eq!(
        event.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    let errors = event.inner.event_errors();
    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("async rejection"));

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
    bus.stop();
}

#[test]
fn test_error_in_forwarded_event_handler_does_not_block_source_bus() {
    let bus_a = EventBus::new(Some("ErrorForwardA".to_string()));
    let bus_b = EventBus::new(Some("ErrorForwardB".to_string()));

    let bus_b_for_forward = bus_b.clone();
    bus_a.on_raw("*", "emit", move |event| {
        let bus_b = bus_b_for_forward.clone();
        async move {
            bus_b.emit_base(event);
            Ok(json!(null))
        }
    });

    bus_b.on_raw("ForwardEvent", "bus_b_handler", |_event| async move {
        Err("bus_b handler failed".to_string())
    });
    bus_a.on_raw("ForwardEvent", "bus_a_handler", |_event| async move {
        Ok(json!("bus_a ok"))
    });

    let event = bus_a.emit(BaseEventHandle::<ForwardEvent>::new(EmptyPayload {}));
    block_on(event.done());

    assert_eq!(
        event.inner.inner.lock().event_status,
        EventStatus::Completed
    );

    let event_results = event.inner.inner.lock().event_results.clone();
    let bus_a_result = event_results
        .values()
        .find(|result| {
            result.handler.eventbus_id == bus_a.id && result.handler.handler_name != "emit"
        })
        .expect("bus_a result should exist");
    assert_eq!(bus_a_result.status, EventResultStatus::Completed);
    assert_eq!(bus_a_result.result, Some(json!("bus_a ok")));

    let bus_b_result = event_results
        .values()
        .find(|result| {
            result.handler.eventbus_id == bus_b.id && result.handler.handler_name != "emit"
        })
        .expect("bus_b result should exist");
    assert_eq!(bus_b_result.status, EventResultStatus::Error);
    assert!(event.inner.event_errors().len() >= 1);
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_event_with_no_handlers_completes_without_errors() {
    let bus = EventBus::new(Some("NoHandlerBus".to_string()));

    let event = bus.emit(BaseEventHandle::<OrphanEvent>::new(EmptyPayload {}));
    block_on(event.done());

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

    bus.on_raw("TestEvent", "my_handler", |_event| async move {
        Err("RangeError: out of range".to_string())
    });

    let event = bus.emit(BaseEventHandle::<TestEvent>::new(EmptyPayload {}));
    block_on(event.done());

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

#[test]
fn test_result_schema_mismatch_uses_event_handler_result_schema_error() {
    let bus = EventBus::new(Some("TaxonomySchemaBus".to_string()));

    bus.on_raw("IntTaxonomyEvent", "wrong_type", |_event| async move {
        Ok(json!("not-an-int"))
    });

    let event = bus.emit_base(schema_event(
        "IntTaxonomyEvent",
        json!({
            "type": "integer"
        }),
    ));
    block_on(event.event_completed());

    let result = event
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("handler result");
    assert_eq!(result.status, EventResultStatus::Error);
    assert!(result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("EventHandlerResultSchemaError"));
    assert_eq!(
        result.to_flat_json_value()["error"]["type"],
        "EventHandlerResultSchemaError"
    );
    bus.stop();
}

#[test]
fn test_handler_timeout_uses_event_handler_timeout_error() {
    let bus = EventBus::new(Some("TaxonomyTimeoutBus".to_string()));

    bus.on_raw(
        "TimeoutTaxonomyEvent",
        "slow_handler",
        |_event| async move {
            thread::sleep(Duration::from_millis(50));
            Ok(json!("slow"))
        },
    );

    let event = bus.emit(BaseEventHandle::<TimeoutTaxonomyEvent>::new(
        EmptyPayload {},
    ));
    block_on(event.done());

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
    assert!(result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("EventHandlerTimeoutError"));
    assert_eq!(
        result.to_flat_json_value()["error"],
        json!({
            "type": "EventHandlerTimeoutError",
            "message": "timeout",
        })
    );
    bus.stop();
}

#[test]
fn test_first_mode_pending_non_winner_uses_cancelled_error_class() {
    let bus = EventBus::new_with_options(
        Some("TaxonomyFirstPendingBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_handler_completion: EventHandlerCompletionMode::First,
            ..EventBusOptions::default()
        },
    );

    bus.on_raw("TaxonomyEvent", "winner", |_event| async move {
        Ok(json!("winner"))
    });
    bus.on_raw("TaxonomyEvent", "never_runs", |_event| async move {
        thread::sleep(Duration::from_millis(100));
        Ok(json!("loser"))
    });

    let event = bus.emit_base(BaseEvent::new("TaxonomyEvent", serde_json::Map::new()));
    block_on(event.event_completed());

    let loser_result = event
        .inner
        .lock()
        .event_results
        .values()
        .find(|result| result.handler.handler_name == "never_runs")
        .cloned()
        .expect("never_runs result");
    assert_eq!(loser_result.status, EventResultStatus::Error);
    assert!(loser_result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("EventHandlerCancelledError"));
    assert_eq!(
        loser_result.to_flat_json_value()["error"]["type"],
        "EventHandlerCancelledError"
    );
    bus.stop();
}

#[test]
fn test_parallel_first_started_loser_uses_aborted_error_class() {
    let bus = EventBus::new_with_options(
        Some("TaxonomyFirstParallelBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            event_handler_completion: EventHandlerCompletionMode::First,
            ..EventBusOptions::default()
        },
    );
    let slow_started = Arc::new(AtomicBool::new(false));

    let slow_started_for_slow = slow_started.clone();
    bus.on_raw("TaxonomyEvent", "slow_loser", move |_event| {
        let slow_started = slow_started_for_slow.clone();
        async move {
            slow_started.store(true, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(200));
            Ok(json!("slow"))
        }
    });

    let slow_started_for_fast = slow_started.clone();
    bus.on_raw("TaxonomyEvent", "fast_winner", move |_event| {
        let slow_started = slow_started_for_fast.clone();
        async move {
            while !slow_started.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(1));
            }
            Ok(json!("winner"))
        }
    });

    let event = bus.emit_base(BaseEvent::new("TaxonomyEvent", serde_json::Map::new()));
    block_on(event.event_completed());

    let slow_result = event
        .inner
        .lock()
        .event_results
        .values()
        .find(|result| result.handler.handler_name == "slow_loser")
        .cloned()
        .expect("slow_loser result");
    assert_eq!(slow_result.status, EventResultStatus::Error);
    assert!(slow_result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("EventHandlerAbortedError"));
    assert_eq!(
        slow_result.to_flat_json_value()["error"]["type"],
        "EventHandlerAbortedError"
    );
    bus.stop();
}
