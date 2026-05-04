use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use abxbus_rust::{
    base_event::{BaseEvent, EventResultsOptions},
    event_bus::{EventBus, EventBusOptions},
    event_result::EventResultStatus,
    typed::{EventSpec, TypedEvent},
    types::{
        EventConcurrencyMode, EventHandlerCompletionMode, EventHandlerConcurrencyMode, EventStatus,
    },
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Serialize, Deserialize)]
struct EmptyPayload {}

#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}

struct LifecycleMethodInvocationEvent;
impl EventSpec for LifecycleMethodInvocationEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "LifecycleMethodInvocationEvent";
}

struct WaitForIdleTimeoutEvent;
impl EventSpec for WaitForIdleTimeoutEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "WaitForIdleTimeoutEvent";
}

struct UserActionEvent;
impl EventSpec for UserActionEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "UserActionEvent";
}

struct RuntimeSerializationEvent;
impl EventSpec for RuntimeSerializationEvent {
    type Payload = EmptyPayload;
    type Result = String;
    const EVENT_TYPE: &'static str = "RuntimeSerializationEvent";
}

fn object_keys(value: &Value) -> BTreeSet<String> {
    value
        .as_object()
        .expect("expected object")
        .keys()
        .cloned()
        .collect()
}

fn expected_base_event_json_keys(include_results: bool) -> BTreeSet<String> {
    let mut keys = BTreeSet::from([
        "event_completed_at".to_string(),
        "event_blocks_parent_completion".to_string(),
        "event_concurrency".to_string(),
        "event_created_at".to_string(),
        "event_emitted_by_handler_id".to_string(),
        "event_handler_completion".to_string(),
        "event_handler_concurrency".to_string(),
        "event_handler_slow_timeout".to_string(),
        "event_handler_timeout".to_string(),
        "event_id".to_string(),
        "event_parent_id".to_string(),
        "event_path".to_string(),
        "event_pending_bus_count".to_string(),
        "event_result_type".to_string(),
        "event_slow_timeout".to_string(),
        "event_started_at".to_string(),
        "event_status".to_string(),
        "event_timeout".to_string(),
        "event_type".to_string(),
        "event_version".to_string(),
    ]);
    if include_results {
        keys.insert("event_results".to_string());
    }
    keys
}

fn expected_event_handler_json_keys() -> BTreeSet<String> {
    BTreeSet::from([
        "event_pattern".to_string(),
        "eventbus_id".to_string(),
        "eventbus_name".to_string(),
        "handler_file_path".to_string(),
        "handler_name".to_string(),
        "handler_registered_at".to_string(),
        "handler_slow_timeout".to_string(),
        "handler_timeout".to_string(),
        "id".to_string(),
    ])
}

fn expected_event_result_json_keys() -> BTreeSet<String> {
    BTreeSet::from([
        "completed_at".to_string(),
        "error".to_string(),
        "event_children".to_string(),
        "event_id".to_string(),
        "eventbus_id".to_string(),
        "eventbus_name".to_string(),
        "handler_event_pattern".to_string(),
        "handler_file_path".to_string(),
        "handler_id".to_string(),
        "handler_name".to_string(),
        "handler_registered_at".to_string(),
        "handler_slow_timeout".to_string(),
        "handler_timeout".to_string(),
        "id".to_string(),
        "result".to_string(),
        "started_at".to_string(),
        "status".to_string(),
    ])
}

fn expected_event_bus_json_keys() -> BTreeSet<String> {
    BTreeSet::from([
        "event_concurrency".to_string(),
        "event_handler_completion".to_string(),
        "event_handler_concurrency".to_string(),
        "event_handler_detect_file_paths".to_string(),
        "event_handler_slow_timeout".to_string(),
        "event_history".to_string(),
        "event_slow_timeout".to_string(),
        "event_timeout".to_string(),
        "handlers".to_string(),
        "handlers_by_key".to_string(),
        "id".to_string(),
        "max_history_drop".to_string(),
        "max_history_size".to_string(),
        "name".to_string(),
        "pending_event_queue".to_string(),
    ])
}

fn base_event(event_type: &str, payload: Value) -> Arc<BaseEvent> {
    let Value::Object(payload) = payload else {
        panic!("test payload must be an object");
    };
    BaseEvent::new(event_type, payload)
}

#[test]
fn test_event_bus_initializes_with_correct_defaults() {
    let bus = EventBus::new(Some("DefaultsBus".to_string()));

    assert_eq!(bus.name, "DefaultsBus");
    assert_eq!(bus.max_history_size(), Some(100));
    assert!(!bus.max_history_drop());
    assert_eq!(bus.event_concurrency, EventConcurrencyMode::BusSerial);
    assert_eq!(
        bus.event_handler_concurrency,
        EventHandlerConcurrencyMode::Serial
    );
    assert_eq!(
        bus.event_handler_completion,
        EventHandlerCompletionMode::All
    );
    assert_eq!(bus.event_timeout, Some(60.0));
    assert_eq!(bus.event_history_size(), 0);
    assert!(EventBus::all_instances_contains(&bus));
    assert!(block_on(bus.wait_until_idle(None)));
    bus.stop();
}

#[test]
fn test_dispatch_returns_pending_event_with_correct_initial_state() {
    let bus = EventBus::new(Some("LifecycleBus".to_string()));
    let event = bus.emit_base(base_event("TestEvent", json!({"data": "hello"})));

    {
        let inner = event.inner.lock();
        assert_eq!(inner.event_type, "TestEvent");
        assert!(!inner.event_id.is_empty());
        assert!(!inner.event_created_at.is_empty());
        assert_eq!(inner.payload.get("data"), Some(&json!("hello")));
        assert!(inner.event_path.contains(&bus.label()));
    }

    assert!(block_on(bus.wait_until_idle(None)));
    bus.stop();
}

#[test]
fn test_event_transitions_through_pending_started_completed() {
    let bus = EventBus::new(Some("StatusBus".to_string()));
    let status_during_handler = Arc::new(Mutex::new(None));
    let status_for_handler = status_during_handler.clone();

    bus.on("StatusLifecycleEvent", "handler", move |event| {
        let status_for_handler = status_for_handler.clone();
        async move {
            *status_for_handler.lock().expect("status lock") =
                Some(event.inner.lock().event_status);
            Ok(json!("done"))
        }
    });

    let event = bus.emit_base(base_event("StatusLifecycleEvent", json!({})));
    block_on(event.event_completed());

    assert_eq!(
        *status_during_handler.lock().expect("status lock"),
        Some(EventStatus::Started)
    );
    let inner = event.inner.lock();
    assert_eq!(inner.event_status, EventStatus::Completed);
    assert!(inner.event_started_at.is_some());
    assert!(inner.event_completed_at.is_some());
    drop(inner);
    bus.stop();
}

#[test]
fn test_event_with_no_handlers_completes_immediately() {
    let bus = EventBus::new(Some("NoHandlerBus".to_string()));
    let event = bus.emit_base(base_event("OrphanEvent", json!({})));
    block_on(event.event_completed());

    let inner = event.inner.lock();
    assert_eq!(inner.event_status, EventStatus::Completed);
    assert_eq!(inner.event_results.len(), 0);
    drop(inner);
    bus.stop();
}

#[test]
fn test_dispatched_events_appear_in_event_history() {
    let bus = EventBus::new(Some("HistoryBus".to_string()));
    let event_a = bus.emit_base(base_event("EventA", json!({})));
    let event_b = bus.emit_base(base_event("EventB", json!({})));
    block_on(event_a.event_completed());
    block_on(event_b.event_completed());
    assert!(block_on(bus.wait_until_idle(None)));

    assert_eq!(bus.event_history_size(), 2);
    let history_ids = bus.event_history_ids();
    assert_eq!(history_ids.len(), 2);
    assert_eq!(history_ids[0], event_a.inner.lock().event_id);
    assert_eq!(history_ids[1], event_b.inner.lock().event_id);
    let runtime = bus.runtime_payload_for_test();
    assert_eq!(runtime[&history_ids[0]].inner.lock().event_type, "EventA");
    assert_eq!(runtime[&history_ids[1]].inner.lock().event_type, "EventB");
    bus.stop();
}

#[test]
fn test_history_is_trimmed_to_max_history_size_completed_events_removed_first() {
    let bus = EventBus::new_with_options(
        Some("TrimBus".to_string()),
        EventBusOptions {
            max_history_size: Some(5),
            max_history_drop: true,
            ..EventBusOptions::default()
        },
    );
    bus.on(
        "TrimEvent",
        "handler",
        |_event| async move { Ok(json!("ok")) },
    );

    for seq in 0..10 {
        let event = bus.emit_base(base_event("TrimEvent", json!({"seq": seq})));
        block_on(event.event_completed());
    }
    assert!(block_on(bus.wait_until_idle(None)));

    assert!(bus.event_history_size() <= 5);
    let runtime = bus.runtime_payload_for_test();
    let seqs: Vec<i64> = bus
        .event_history_ids()
        .iter()
        .map(|event_id| {
            runtime[event_id].inner.lock().payload["seq"]
                .as_i64()
                .expect("seq")
        })
        .collect();
    assert!(seqs.windows(2).all(|pair| pair[1] > pair[0]));
    assert_eq!(seqs.last().copied(), Some(9));
    bus.stop();
}

#[test]
fn test_unlimited_history_max_history_size_null_keeps_all_events() {
    let bus = EventBus::new_with_options(
        Some("UnlimitedHistBus".to_string()),
        EventBusOptions {
            max_history_size: None,
            ..EventBusOptions::default()
        },
    );
    bus.on(
        "PingEvent",
        "handler",
        |_event| async move { Ok(json!("pong")) },
    );

    for _ in 0..150 {
        let event = bus.emit_base(base_event("PingEvent", json!({})));
        block_on(event.event_completed());
    }
    assert!(block_on(bus.wait_until_idle(None)));

    assert_eq!(bus.event_history_size(), 150);
    assert!(bus
        .runtime_payload_for_test()
        .values()
        .all(|event| event.inner.lock().event_status == EventStatus::Completed));
    bus.stop();
}

#[test]
fn test_max_history_size_0_keeps_in_flight_events_and_drops_them_on_completion() {
    let bus = EventBus::new_with_options(
        Some("ZeroHistBus".to_string()),
        EventBusOptions {
            max_history_size: Some(0),
            ..EventBusOptions::default()
        },
    );
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));

    bus.on("SlowEvent", "handler", move |_event| {
        let started_tx = started_tx.clone();
        let release_rx = release_rx.clone();
        async move {
            let _ = started_tx.send(());
            release_rx
                .lock()
                .expect("release lock")
                .recv_timeout(Duration::from_secs(2))
                .expect("release signal");
            Ok(json!("ok"))
        }
    });

    let first = bus.emit_base(base_event("SlowEvent", json!({})));
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("first handler should start");
    let second = bus.emit_base(base_event("SlowEvent", json!({})));
    let first_id = first.inner.lock().event_id.clone();
    let second_id = second.inner.lock().event_id.clone();

    let runtime = bus.runtime_payload_for_test();
    assert!(runtime.contains_key(&first_id));
    assert!(runtime.contains_key(&second_id));

    release_tx.send(()).expect("release first");
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("second handler should start");
    release_tx.send(()).expect("release second");
    block_on(first.event_completed());
    block_on(second.event_completed());
    assert!(block_on(bus.wait_until_idle(None)));

    assert_eq!(bus.event_history_size(), 0);
    assert!(bus.runtime_payload_for_test().is_empty());
    bus.stop();
}

#[test]
fn test_handler_registration_by_string_matches_extend_name() {
    let bus = EventBus::new(Some("StringMatchBus".to_string()));
    let received = Arc::new(Mutex::new(Vec::new()));
    let received_for_handler = received.clone();

    bus.on("NamedEvent", "string_handler", move |_event| {
        let received = received_for_handler.clone();
        async move {
            received
                .lock()
                .expect("received lock")
                .push("string_handler".to_string());
            Ok(json!(null))
        }
    });

    let event = bus.emit_base(base_event("NamedEvent", json!({})));
    block_on(event.event_completed());

    assert_eq!(
        received.lock().expect("received lock").as_slice(),
        &["string_handler".to_string()]
    );
    bus.stop();
}

#[test]
fn test_wildcard_handler_receives_all_events() {
    let bus = EventBus::new(Some("WildcardBus".to_string()));
    let types = Arc::new(Mutex::new(Vec::new()));
    let types_for_handler = types.clone();

    bus.on("*", "wildcard", move |event| {
        let types = types_for_handler.clone();
        async move {
            types
                .lock()
                .expect("types lock")
                .push(event.inner.lock().event_type.clone());
            Ok(json!(null))
        }
    });

    let event_a = bus.emit_base(base_event("EventA", json!({})));
    let event_b = bus.emit_base(base_event("EventB", json!({})));
    block_on(event_a.event_completed());
    block_on(event_b.event_completed());

    assert_eq!(
        types.lock().expect("types lock").as_slice(),
        &["EventA".to_string(), "EventB".to_string()]
    );
    bus.stop();
}

#[test]
fn test_wait_for_result() {
    let bus = EventBus::new(Some("WaitForResultBus".to_string()));
    let completion_order = Arc::new(Mutex::new(Vec::new()));

    let order_for_handler = completion_order.clone();
    bus.on("UserActionEvent", "slow_handler", move |_event| {
        let completion_order = order_for_handler.clone();
        async move {
            thread::sleep(Duration::from_millis(50));
            completion_order
                .lock()
                .expect("completion order lock")
                .push("handler_done".to_string());
            Ok(json!("done"))
        }
    });

    let event = bus.emit::<UserActionEvent>(TypedEvent::new(EmptyPayload {}));
    completion_order
        .lock()
        .expect("completion order lock")
        .push("enqueue_done".to_string());

    block_on(event.wait_completed());
    completion_order
        .lock()
        .expect("completion order lock")
        .push("wait_done".to_string());

    assert_eq!(
        completion_order
            .lock()
            .expect("completion order lock")
            .as_slice(),
        &[
            "enqueue_done".to_string(),
            "handler_done".to_string(),
            "wait_done".to_string()
        ]
    );
    assert!(event.inner.inner.lock().event_completed_at.is_some());
    bus.stop();
}

#[test]
fn test_error_handling() {
    let bus = EventBus::new(Some("ErrorHandlingBus".to_string()));
    let results = Arc::new(Mutex::new(Vec::new()));

    bus.on("UserActionEvent", "failing_handler", |_event| async move {
        Err("Expected to fail - testing error handling in event handlers".to_string())
    });

    let results_for_handler = results.clone();
    bus.on("UserActionEvent", "working_handler", move |_event| {
        let results = results_for_handler.clone();
        async move {
            results
                .lock()
                .expect("results lock")
                .push("success".to_string());
            Ok(json!("worked"))
        }
    });

    let event = bus.emit::<UserActionEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());
    let event_results = event.inner.inner.lock().event_results.clone();

    let failing_result = event_results
        .values()
        .find(|result| result.handler.handler_name == "failing_handler")
        .expect("failing handler result");
    assert_eq!(failing_result.status, EventResultStatus::Error);
    assert!(failing_result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("Expected to fail"));

    let working_result = event_results
        .values()
        .find(|result| result.handler.handler_name == "working_handler")
        .expect("working handler result");
    assert_eq!(working_result.status, EventResultStatus::Completed);
    assert_eq!(working_result.result, Some(json!("worked")));
    assert_eq!(
        results.lock().expect("results lock").as_slice(),
        &["success".to_string()]
    );
    bus.stop();
}

#[test]
fn test_event_result_raises_exception_group_when_multiple_handlers_fail() {
    let bus = EventBus::new(Some("EventResultMultiErrorBus".to_string()));

    bus.on(
        "UserActionEvent",
        "failing_handler_one",
        |_event| async move { Err("ValueError: first failure".to_string()) },
    );
    bus.on(
        "UserActionEvent",
        "failing_handler_two",
        |_event| async move { Err("RuntimeError: second failure".to_string()) },
    );

    let event = bus.emit::<UserActionEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());

    let error = block_on(event.inner.event_result(EventResultsOptions::default()))
        .expect_err("multiple handler errors should be raised");
    assert!(error.contains("2 handler error(s)"), "{error}");
    assert!(error.contains("ValueError: first failure"), "{error}");
    assert!(error.contains("RuntimeError: second failure"), "{error}");
    bus.stop();
}

#[test]
fn test_event_result_single_handler_error_raises_original_exception() {
    let bus = EventBus::new(Some("EventResultSingleErrorBus".to_string()));

    bus.on("UserActionEvent", "failing_handler", |_event| async move {
        Err("ValueError: single failure".to_string())
    });

    let event = bus.emit::<UserActionEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());

    let error = block_on(event.inner.event_result(EventResultsOptions::default()))
        .expect_err("single handler error should be raised");
    assert_eq!(error, "ValueError: single failure");
    bus.stop();
}

#[test]
fn test_event_results_access() {
    let bus = EventBus::new(Some("EventResultsAccessBus".to_string()));

    bus.on("TestEvent", "early_handler", |_event| async move {
        Ok(json!("early"))
    });
    bus.on("TestEvent", "late_handler", |_event| async move {
        thread::sleep(Duration::from_millis(10));
        Ok(json!("late"))
    });

    let event = bus.emit_base(base_event("TestEvent", json!({})));
    block_on(event.event_completed());
    let event_results = event.inner.lock().event_results.clone();
    assert_eq!(event_results.len(), 2);
    assert_eq!(
        event_results
            .values()
            .find(|result| result.handler.handler_name == "early_handler")
            .and_then(|result| result.result.clone()),
        Some(json!("early"))
    );
    assert_eq!(
        event_results
            .values()
            .find(|result| result.handler.handler_name == "late_handler")
            .and_then(|result| result.result.clone()),
        Some(json!("late"))
    );

    let empty_event = bus.emit_base(base_event("EmptyEvent", json!({})));
    block_on(empty_event.event_completed());
    assert_eq!(empty_event.inner.lock().event_results.len(), 0);
    bus.stop();
}

#[test]
fn test_by_handler_name() {
    let bus = EventBus::new(Some("ByHandlerNameBus".to_string()));

    bus.on("TestEvent", "process_data", |_event| async move {
        Ok(json!("version1"))
    });
    bus.on("TestEvent", "process_data", |_event| async move {
        Ok(json!("version2"))
    });
    bus.on("TestEvent", "unique_handler", |_event| async move {
        Ok(json!("unique"))
    });

    let event = bus.emit_base(base_event("TestEvent", json!({})));
    block_on(event.event_completed());
    let event_results = event.inner.lock().event_results.clone();
    let process_results: Vec<Value> = event_results
        .values()
        .filter(|result| result.handler.handler_name == "process_data")
        .filter_map(|result| result.result.clone())
        .collect();
    assert_eq!(process_results.len(), 2);
    assert!(process_results.contains(&json!("version1")));
    assert!(process_results.contains(&json!("version2")));
    assert_eq!(
        event_results
            .values()
            .find(|result| result.handler.handler_name == "unique_handler")
            .and_then(|result| result.result.clone()),
        Some(json!("unique"))
    );
    bus.stop();
}

#[test]
fn test_by_handler_id() {
    let bus = EventBus::new(Some("ByHandlerIdBus".to_string()));

    bus.on(
        "TestEvent",
        "handler",
        |_event| async move { Ok(json!("v1")) },
    );
    bus.on(
        "TestEvent",
        "handler",
        |_event| async move { Ok(json!("v2")) },
    );

    let event = bus.emit_base(base_event("TestEvent", json!({})));
    block_on(event.event_completed());
    let event_results = event.inner.lock().event_results.clone();
    let ids: BTreeSet<String> = event_results.keys().cloned().collect();
    let values: Vec<Value> = event_results
        .values()
        .filter_map(|result| result.result.clone())
        .collect();
    assert_eq!(ids.len(), 2);
    assert_eq!(event_results.len(), 2);
    assert!(values.contains(&json!("v1")));
    assert!(values.contains(&json!("v2")));
    bus.stop();
}

#[test]
fn test_string_indexing() {
    let bus = EventBus::new(Some("StringIndexingBus".to_string()));

    bus.on("TestEvent", "my_handler", |_event| async move {
        Ok(json!("my_result"))
    });

    let event = bus.emit_base(base_event("TestEvent", json!({})));
    block_on(event.event_completed());
    let event_results = event.inner.lock().event_results.clone();
    let my_handler_result = event_results
        .values()
        .find(|result| result.handler.handler_name == "my_handler");
    assert_eq!(
        my_handler_result.and_then(|result| result.result.clone()),
        Some(json!("my_result"))
    );
    let missing_result = event_results
        .values()
        .find(|result| result.handler.handler_name == "missing");
    assert!(missing_result.is_none());
    bus.stop();
}

#[test]
fn test_emit_alias_dispatches_event() {
    let bus = EventBus::new(Some("EmitAliasBus".to_string()));
    let handled_event_ids = Arc::new(Mutex::new(Vec::new()));

    let handled_for_handler = handled_event_ids.clone();
    bus.on("UserActionEvent", "user_handler", move |event| {
        let handled_event_ids = handled_for_handler.clone();
        async move {
            handled_event_ids
                .lock()
                .expect("handled ids lock")
                .push(event.inner.lock().event_id.clone());
            Ok(json!("handled"))
        }
    });

    let event = bus.emit::<UserActionEvent>(TypedEvent::new(EmptyPayload {}));
    let event_id = event.inner.inner.lock().event_id.clone();
    block_on(event.wait_completed());

    assert_eq!(
        handled_event_ids
            .lock()
            .expect("handled ids lock")
            .as_slice(),
        &[event_id.clone()]
    );
    assert_eq!(
        event.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    assert!(event.inner.inner.lock().event_path.contains(&bus.label()));
    bus.stop();
}

#[test]
fn test_handler_registration() {
    let bus = EventBus::new(Some("HandlerRegistrationBus".to_string()));
    let specific = Arc::new(Mutex::new(Vec::new()));
    let model = Arc::new(Mutex::new(Vec::new()));
    let universal = Arc::new(Mutex::new(Vec::new()));

    let specific_for_handler = specific.clone();
    bus.on("UserActionEvent", "user_handler", move |event| {
        let specific = specific_for_handler.clone();
        async move {
            specific
                .lock()
                .expect("specific lock")
                .push(event.inner.lock().payload["action"].clone());
            Ok(json!("user_handled"))
        }
    });

    let model_for_handler = model.clone();
    bus.on_typed::<RuntimeSerializationEvent, _, _>("system_handler", move |_event| {
        let model = model_for_handler.clone();
        async move {
            model
                .lock()
                .expect("model lock")
                .push("startup".to_string());
            Ok("system_handled".to_string())
        }
    });

    let universal_for_handler = universal.clone();
    bus.on("*", "universal_handler", move |event| {
        let universal = universal_for_handler.clone();
        async move {
            universal
                .lock()
                .expect("universal lock")
                .push(event.inner.lock().event_type.clone());
            Ok(json!("universal"))
        }
    });

    let user = bus.emit_base(base_event("UserActionEvent", json!({"action": "login"})));
    let system = bus.emit::<RuntimeSerializationEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(async {
        user.event_completed().await;
        system.wait_completed().await;
        assert!(bus.wait_until_idle(Some(1.0)).await);
    });

    assert_eq!(
        specific.lock().expect("specific lock").as_slice(),
        &[json!("login")]
    );
    assert_eq!(
        model.lock().expect("model lock").as_slice(),
        &["startup".to_string()]
    );
    let universal_values = universal.lock().expect("universal lock").clone();
    assert!(universal_values.contains(&"UserActionEvent".to_string()));
    assert!(universal_values.contains(&"RuntimeSerializationEvent".to_string()));
    bus.stop();
}

#[test]
fn test_multiple_handlers_parallel() {
    let bus = EventBus::new_with_options(
        Some("MultipleHandlersParallelBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    let starts = Arc::new(Mutex::new(Vec::new()));
    let ends = Arc::new(Mutex::new(Vec::new()));

    for handler_name in ["slow_handler_1", "slow_handler_2"] {
        let starts = starts.clone();
        let ends = ends.clone();
        bus.on("UserActionEvent", handler_name, move |_event| {
            let starts = starts.clone();
            let ends = ends.clone();
            async move {
                starts
                    .lock()
                    .expect("starts lock")
                    .push((handler_name.to_string(), std::time::Instant::now()));
                thread::sleep(Duration::from_millis(100));
                ends.lock()
                    .expect("ends lock")
                    .push((handler_name.to_string(), std::time::Instant::now()));
                Ok(json!(handler_name))
            }
        });
    }

    let start = std::time::Instant::now();
    let event = bus.emit::<UserActionEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());
    let duration = start.elapsed();

    assert!(
        duration < Duration::from_millis(180),
        "duration={duration:?}"
    );
    assert_eq!(starts.lock().expect("starts lock").len(), 2);
    assert_eq!(ends.lock().expect("ends lock").len(), 2);
    let event_results = event.inner.inner.lock().event_results.clone();
    assert!(event_results.values().any(|result| {
        result.handler.handler_name == "slow_handler_1"
            && result.result == Some(json!("slow_handler_1"))
    }));
    assert!(event_results.values().any(|result| {
        result.handler.handler_name == "slow_handler_2"
            && result.result == Some(json!("slow_handler_2"))
    }));
    bus.stop();
}

#[test]
fn test_batch_emit_with_gather() {
    let bus = EventBus::new(Some("BatchEmitBus".to_string()));

    let events = [
        bus.emit_base(base_event("UserActionEvent", json!({"action": "login"}))),
        bus.emit_base(base_event("SystemEventModel", json!({"name": "startup"}))),
        bus.emit_base(base_event("UserActionEvent", json!({"action": "logout"}))),
    ];

    for event in &events {
        block_on(event.event_completed());
    }

    assert_eq!(events.len(), 3);
    assert!(events
        .iter()
        .all(|event| event.inner.lock().event_completed_at.is_some()));
    bus.stop();
}

#[test]
fn test_concurrent_emit_calls() {
    let bus = EventBus::new(Some("ConcurrentEmitCallsBus".to_string()));
    let mut events = Vec::new();

    for index in 0..100 {
        events.push(bus.emit_base(base_event(
            "UserActionEvent",
            json!({"action": format!("concurrent_{index}")}),
        )));
    }

    for event in &events {
        block_on(event.event_completed());
    }
    assert!(block_on(bus.wait_until_idle(Some(2.0))));
    assert_eq!(bus.event_history_size(), 100);
    bus.stop();
}

#[test]
fn test_mixed_delay_handlers_maintain_order() {
    let bus = EventBus::new(Some("MixedDelayOrderBus".to_string()));
    let collected_orders = Arc::new(Mutex::new(Vec::new()));
    let handler_start_orders = Arc::new(Mutex::new(Vec::new()));

    let collected_for_handler = collected_orders.clone();
    let starts_for_handler = handler_start_orders.clone();
    bus.on("UserActionEvent", "handler", move |event| {
        let collected_orders = collected_for_handler.clone();
        let handler_start_orders = starts_for_handler.clone();
        async move {
            let order = event.inner.lock().payload["order"]
                .as_i64()
                .expect("order payload");
            handler_start_orders
                .lock()
                .expect("handler start lock")
                .push(order);
            if order % 2 == 0 {
                thread::sleep(Duration::from_millis(10));
            } else {
                thread::sleep(Duration::from_millis(2));
            }
            collected_orders
                .lock()
                .expect("collected order lock")
                .push(order);
            Ok(json!(format!("handled_{order}")))
        }
    });

    for order in 0..20 {
        bus.emit_base(base_event("UserActionEvent", json!({"order": order})));
    }
    assert!(block_on(bus.wait_until_idle(Some(3.0))));

    let expected: Vec<i64> = (0..20).collect();
    assert_eq!(
        collected_orders
            .lock()
            .expect("collected order lock")
            .as_slice(),
        expected.as_slice()
    );
    assert_eq!(
        handler_start_orders
            .lock()
            .expect("handler start lock")
            .as_slice(),
        expected.as_slice()
    );
    bus.stop();
}

#[test]
fn test_event_with_complex_data() {
    let bus = EventBus::new(Some("ComplexDataBus".to_string()));
    let event = bus.emit_base(base_event(
        "SystemEventModel",
        json!({
            "name": "complex",
            "details": {
                "nested": {
                    "list": [1, 2, {"inner": "value"}],
                    "none": null
                }
            }
        }),
    ));
    block_on(event.event_completed());

    assert_eq!(
        event.inner.lock().payload["details"]["nested"]["list"][2]["inner"],
        json!("value")
    );
    bus.stop();
}

#[test]
fn test_zero_history_size_keeps_inflight_and_drops_on_completion() {
    test_max_history_size_0_keeps_in_flight_events_and_drops_them_on_completion();
}

#[test]
fn test_handler_error_is_captured_without_crashing_the_bus() {
    let bus = EventBus::new(Some("ErrorBus".to_string()));
    bus.on("ErrorEvent", "throws", |_event| async move {
        Err("handler blew up".to_string())
    });

    let event = bus.emit_base(base_event("ErrorEvent", json!({})));
    block_on(event.event_completed());

    assert_eq!(event.inner.lock().event_status, EventStatus::Completed);
    let errors = event.event_errors();
    assert_eq!(errors.len(), 1);
    let result = event
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("result");
    assert_eq!(result.status, EventResultStatus::Error);
    assert!(result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("handler blew up"));
    bus.stop();
}

#[test]
fn test_one_handler_error_does_not_prevent_other_handlers_from_running() {
    let bus = EventBus::new(Some("ErrorIsolationBus".to_string()));
    let second_ran = Arc::new(AtomicBool::new(false));
    let second_ran_for_handler = second_ran.clone();

    bus.on("ErrorIsolationEvent", "bad", |_event| async move {
        Err("bad handler".to_string())
    });
    bus.on("ErrorIsolationEvent", "good", move |_event| {
        let second_ran = second_ran_for_handler.clone();
        async move {
            second_ran.store(true, Ordering::SeqCst);
            Ok(json!("ok"))
        }
    });

    let event = bus.emit_base(base_event("ErrorIsolationEvent", json!({})));
    block_on(event.event_completed());

    assert!(second_ran.load(Ordering::SeqCst));
    let results = event.inner.lock().event_results.clone();
    assert_eq!(results.len(), 2);
    assert!(results
        .values()
        .any(|result| result.status == EventResultStatus::Error));
    assert!(results
        .values()
        .any(|result| result.status == EventResultStatus::Completed));
    bus.stop();
}

#[test]
fn test_many_events_dispatched_concurrently_all_complete() {
    let bus = EventBus::new_with_options(
        Some("ConcurrentDispatchBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    let count = Arc::new(AtomicUsize::new(0));
    let count_for_handler = count.clone();
    bus.on("ConcurrentEvent", "handler", move |_event| {
        let count = count_for_handler.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(json!("ok"))
        }
    });

    let mut joins = Vec::new();
    for i in 0..25 {
        let bus = bus.clone();
        joins.push(thread::spawn(move || {
            bus.emit_base(base_event("ConcurrentEvent", json!({"seq": i})))
        }));
    }
    let events: Vec<_> = joins
        .into_iter()
        .map(|join| join.join().expect("emit thread"))
        .collect();
    for event in &events {
        block_on(event.event_completed());
        assert_eq!(event.inner.lock().event_status, EventStatus::Completed);
    }
    assert_eq!(count.load(Ordering::SeqCst), 25);
    bus.stop();
}

#[test]
fn test_dispatch_leaves_event_timeout_unset_and_processing_uses_bus_timeout_default() {
    let bus = EventBus::new_with_options(
        Some("TimeoutDefaultDispatchBus".to_string()),
        EventBusOptions {
            event_timeout: Some(10.0),
            ..EventBusOptions::default()
        },
    );
    bus.on("TimeoutDefaultEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });

    let event = base_event("TimeoutDefaultEvent", json!({}));
    assert_eq!(event.inner.lock().event_timeout, None);
    let event = bus.emit_base(event);
    assert_eq!(event.inner.lock().event_timeout, None);
    block_on(event.event_completed());
    let result = event
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("result");
    assert_eq!(result.timeout, Some(10.0));
    bus.stop();
}

#[test]
fn test_event_with_explicit_timeout_is_not_overridden_by_bus_default() {
    let bus = EventBus::new_with_options(
        Some("ExplicitTimeoutDispatchBus".to_string()),
        EventBusOptions {
            event_timeout: Some(10.0),
            ..EventBusOptions::default()
        },
    );
    bus.on("ExplicitTimeoutEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });

    let event = base_event("ExplicitTimeoutEvent", json!({}));
    event.inner.lock().event_timeout = Some(2.0);
    let event = bus.emit_base(event);
    block_on(event.event_completed());
    let result = event
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("result");
    assert_eq!(event.inner.lock().event_timeout, Some(2.0));
    assert_eq!(result.timeout, Some(2.0));
    bus.stop();
}

#[test]
fn test_eventbus_all_instances_tracks_all_created_buses() {
    let bus_a = EventBus::new(Some("AllInstancesBusA".to_string()));
    let bus_b = EventBus::new(Some("AllInstancesBusB".to_string()));

    assert!(EventBus::all_instances_contains(&bus_a));
    assert!(EventBus::all_instances_contains(&bus_b));
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_reset_creates_a_fresh_pending_event_for_cross_bus_dispatch() {
    let bus_a = EventBus::new(Some("ResetBusA".to_string()));
    let bus_b = EventBus::new(Some("ResetBusB".to_string()));
    bus_a.on(
        "ResetEvent",
        "handler_a",
        |_event| async move { Ok(json!("a")) },
    );
    bus_b.on(
        "ResetEvent",
        "handler_b",
        |_event| async move { Ok(json!("b")) },
    );

    let completed = bus_a.emit_base(base_event("ResetEvent", json!({"label": "hello"})));
    block_on(completed.event_completed());
    assert_eq!(completed.inner.lock().event_status, EventStatus::Completed);
    assert_eq!(completed.inner.lock().event_results.len(), 1);

    let fresh = completed.event_reset();
    assert_ne!(fresh.inner.lock().event_id, completed.inner.lock().event_id);
    assert_eq!(fresh.inner.lock().event_status, EventStatus::Pending);
    assert!(fresh.inner.lock().event_started_at.is_none());
    assert!(fresh.inner.lock().event_completed_at.is_none());
    assert_eq!(fresh.inner.lock().event_results.len(), 0);

    let forwarded = bus_b.emit_base(fresh);
    block_on(forwarded.event_completed());
    assert_eq!(forwarded.inner.lock().event_status, EventStatus::Completed);
    assert_eq!(forwarded.inner.lock().event_results.len(), 1);
    let event_path = forwarded.inner.lock().event_path.clone();
    assert!(event_path.iter().any(|path| path.starts_with("ResetBusA#")));
    assert!(event_path.iter().any(|path| path.starts_with("ResetBusB#")));
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_max_history_size_0_prunes_previously_completed_events_on_later_dispatch() {
    let bus = EventBus::new_with_options(
        Some("ZeroHistPruneBus".to_string()),
        EventBusOptions {
            max_history_size: Some(0),
            ..EventBusOptions::default()
        },
    );
    bus.on("ZeroHistPruneEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });

    let first = bus.emit_base(base_event("ZeroHistPruneEvent", json!({"seq": 1})));
    block_on(first.event_completed());
    assert_eq!(bus.event_history_size(), 0);

    let second = bus.emit_base(base_event("ZeroHistPruneEvent", json!({"seq": 2})));
    block_on(second.event_completed());
    assert_eq!(bus.event_history_size(), 0);
    assert!(bus.runtime_payload_for_test().is_empty());
    bus.stop();
}

#[test]
fn test_base_event_to_json_from_json_roundtrips_runtime_fields_and_event_results() {
    let bus = EventBus::new(Some("RuntimeSerializationBus".to_string()));
    bus.on(
        "RuntimeSerializationEvent",
        "returns_ok",
        |_event| async move { Ok(json!("ok")) },
    );

    let event = bus.emit::<RuntimeSerializationEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());

    let serialized = event.inner.to_json_value();
    assert_eq!(
        serde_json::to_value(&*event.inner).expect("base event serde"),
        serialized
    );
    assert_eq!(
        object_keys(&serialized),
        expected_base_event_json_keys(true)
    );
    assert_eq!(serialized["event_status"], "completed");
    assert!(serialized["event_created_at"].is_string());
    assert!(serialized["event_started_at"].is_string());
    assert!(serialized["event_completed_at"].is_string());
    assert_eq!(serialized["event_pending_bus_count"], 0);
    assert!(serialized["event_results"].is_object());

    let json_results = serialized["event_results"].as_object().expect("object");
    assert_eq!(json_results.len(), 1);
    let handler_id = event
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .expect("event result")
        .handler
        .id
        .clone();
    let json_result = json_results.get(&handler_id).expect("handler keyed result");
    assert_eq!(object_keys(json_result), expected_event_result_json_keys());
    assert_eq!(json_result["status"], "completed");
    assert_eq!(json_result["result"], "ok");
    assert_eq!(json_result["handler_id"], handler_id);
    assert!(json_result.get("handler").is_none());

    let restored = abxbus_rust::base_event::BaseEvent::from_json_value(serialized);
    assert_eq!(restored.inner.lock().event_status, EventStatus::Completed);
    assert_eq!(restored.inner.lock().event_pending_bus_count, 0);
    assert_eq!(restored.inner.lock().event_results.len(), 1);
    let restored_result = restored
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("restored result");
    assert_eq!(restored_result.status, EventResultStatus::Completed);
    assert_eq!(restored_result.result, Some(json!("ok")));
    assert_eq!(restored_result.handler.handler_name, "returns_ok");
    bus.stop();
}

#[test]
fn test_event_handler_json_matches_python_typescript_shape() {
    let bus = EventBus::new_with_options(
        Some("HandlerJsonBus".to_string()),
        EventBusOptions {
            id: Some("018f8e40-1234-7000-8000-000000001234".to_string()),
            ..EventBusOptions::default()
        },
    );
    let handler = bus.on(
        "RuntimeSerializationEvent",
        "handler",
        |_event| async move { Ok(json!("ok")) },
    );

    let serialized = handler.to_json_value();
    assert_eq!(object_keys(&serialized), expected_event_handler_json_keys());
    assert_eq!(serialized["event_pattern"], "RuntimeSerializationEvent");
    assert_eq!(serialized["eventbus_name"], "HandlerJsonBus");
    assert_eq!(
        serialized["eventbus_id"],
        "018f8e40-1234-7000-8000-000000001234"
    );
    assert_eq!(serialized["handler_name"], "handler");
    assert!(serialized.get("handler").is_none());

    let restored = abxbus_rust::event_handler::EventHandler::from_json_value(serialized);
    assert_eq!(restored.id, handler.id);
    assert_eq!(restored.event_pattern, handler.event_pattern);
    assert_eq!(restored.eventbus_id, handler.eventbus_id);
    assert!(restored.callable.is_none());
    bus.stop();
}

#[test]
fn test_eventbus_model_dump_json_roundtrip_uses_id_keyed_structures() {
    let bus = EventBus::new_with_options(
        Some("SerializableBus".to_string()),
        EventBusOptions {
            id: Some("018f8e40-1234-7000-8000-000000001234".to_string()),
            max_history_size: Some(500),
            max_history_drop: false,
            event_concurrency: EventConcurrencyMode::Parallel,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            event_handler_completion: EventHandlerCompletionMode::First,
            event_timeout: None,
            event_slow_timeout: Some(34.0),
            event_handler_slow_timeout: Some(12.0),
            event_handler_detect_file_paths: false,
        },
    );
    let handler = bus.on(
        "RuntimeSerializationEvent",
        "handler",
        |_event| async move { Ok(json!("ok")) },
    );
    let event = bus.emit::<RuntimeSerializationEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());

    let payload = bus.to_json_value();
    assert_eq!(serde_json::to_value(&*bus).expect("bus serde"), payload);
    assert_eq!(object_keys(&payload), expected_event_bus_json_keys());
    assert_eq!(payload["id"], "018f8e40-1234-7000-8000-000000001234");
    assert_eq!(payload["name"], "SerializableBus");
    assert_eq!(payload["max_history_size"], 500);
    assert_eq!(payload["max_history_drop"], false);
    assert_eq!(payload["event_concurrency"], "parallel");
    assert_eq!(payload["event_timeout"], Value::Null);
    assert_eq!(payload["event_slow_timeout"], 34.0);
    assert_eq!(payload["event_handler_concurrency"], "parallel");
    assert_eq!(payload["event_handler_completion"], "first");
    assert_eq!(payload["event_handler_slow_timeout"], 12.0);
    assert_eq!(payload["event_handler_detect_file_paths"], false);

    let handlers = payload["handlers"].as_object().expect("handlers");
    assert_eq!(handlers.keys().cloned().collect::<BTreeSet<_>>(), {
        let mut keys = BTreeSet::new();
        keys.insert(handler.id.clone());
        keys
    });
    assert_eq!(
        object_keys(handlers.get(&handler.id).expect("handler json")),
        expected_event_handler_json_keys()
    );
    assert_eq!(
        payload["handlers_by_key"]["RuntimeSerializationEvent"],
        json!([handler.id.clone()])
    );

    let event_id = event.inner.inner.lock().event_id.clone();
    let event_history = payload["event_history"].as_object().expect("history");
    assert_eq!(event_history.keys().cloned().collect::<BTreeSet<_>>(), {
        let mut keys = BTreeSet::new();
        keys.insert(event_id.clone());
        keys
    });
    assert_eq!(
        object_keys(event_history.get(&event_id).expect("event json")),
        expected_base_event_json_keys(true)
    );
    assert_eq!(payload["pending_event_queue"], json!([]));

    let restored = EventBus::from_json_value(payload.clone());
    let restored_payload = restored.to_json_value();
    assert_eq!(restored_payload, payload);
    restored.stop();
    bus.stop();
}

#[test]
fn test_eventbus_validate_creates_missing_handler_entries_from_event_results() {
    let bus = EventBus::new_with_options(
        Some("SerializableBusMissingHandlers".to_string()),
        EventBusOptions {
            id: Some("018f8e40-1234-7000-8000-000000001235".to_string()),
            ..EventBusOptions::default()
        },
    );
    let handler = bus.on(
        "RuntimeSerializationEvent",
        "handler",
        |_event| async move { Ok(json!("ok")) },
    );
    let event = bus.emit::<RuntimeSerializationEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());

    let mut payload = bus.to_json_value();
    payload["handlers"] = json!({});
    payload["handlers_by_key"] = json!({});

    let restored = EventBus::from_json_value(payload);
    let restored_payload = restored.to_json_value();
    assert!(restored_payload["handlers"]
        .as_object()
        .expect("handlers")
        .contains_key(&handler.id));
    assert_eq!(
        restored_payload["handlers_by_key"]["RuntimeSerializationEvent"],
        json!([handler.id])
    );
    restored.stop();
    bus.stop();
}

#[test]
fn test_eventbus_model_dump_promotes_pending_events_into_event_history() {
    let bus = EventBus::new(Some("QueueOnlyBus".to_string()));
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let sent_started = Arc::new(AtomicBool::new(false));
    let sent_started_for_handler = sent_started.clone();

    bus.on(
        "RuntimeSerializationEvent",
        "blocking_handler",
        move |_event| {
            let started_tx = started_tx.clone();
            let sent_started = sent_started_for_handler.clone();
            async move {
                if !sent_started.swap(true, Ordering::SeqCst) {
                    let _ = started_tx.send(());
                }
                thread::sleep(Duration::from_millis(100));
                Ok(json!("ok"))
            }
        },
    );

    let first = bus.emit::<RuntimeSerializationEvent>(TypedEvent::new(EmptyPayload {}));
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("first handler should start");
    let second = bus.emit::<RuntimeSerializationEvent>(TypedEvent::new(EmptyPayload {}));

    let first_id = first.inner.inner.lock().event_id.clone();
    let second_id = second.inner.inner.lock().event_id.clone();
    let payload = bus.to_json_value();
    let event_history = payload["event_history"].as_object().expect("history");
    assert!(event_history.contains_key(&first_id));
    assert!(event_history.contains_key(&second_id));
    assert_eq!(payload["pending_event_queue"], json!([second_id]));

    block_on(first.wait_completed());
    block_on(second.wait_completed());
    bus.stop();
}

#[test]
fn test_eventbus_initialization() {
    let bus = EventBus::new(None);

    assert_eq!(bus.event_history_size(), 0);
    assert!(!bus.max_history_drop());
    assert_eq!(bus.event_history_ids().len(), 0);
    assert!(EventBus::all_instances_contains(&bus));
    bus.stop();
}

#[test]
fn test_wait_until_idle_timeout_returns_after_timeout_when_work_is_still_in_flight() {
    let bus = EventBus::new(Some("WaitForIdleTimeoutBus".to_string()));
    bus.on("WaitForIdleTimeoutEvent", "wait", |_event| async move {
        thread::sleep(Duration::from_millis(100));
        Ok(json!(null))
    });

    let event = bus.emit::<WaitForIdleTimeoutEvent>(TypedEvent::new(EmptyPayload {}));
    let started = std::time::Instant::now();
    let became_idle = block_on(bus.wait_until_idle(Some(0.02)));
    let elapsed = started.elapsed();

    assert!(!became_idle);
    assert!(elapsed >= Duration::from_millis(15));
    assert!(elapsed < Duration::from_secs(1));
    assert!(!bus.is_idle_and_queue_empty());

    block_on(event.wait_completed());
    assert!(block_on(bus.wait_until_idle(None)));
    bus.stop();
}

#[test]
fn test_event_bus_applies_custom_options() {
    let bus = EventBus::new_with_options(
        Some("CustomBus".to_string()),
        EventBusOptions {
            max_history_size: Some(500),
            max_history_drop: false,
            event_concurrency: EventConcurrencyMode::Parallel,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_handler_completion: EventHandlerCompletionMode::First,
            event_timeout: Some(30.0),
            ..EventBusOptions::default()
        },
    );

    assert_eq!(bus.max_history_size(), Some(500));
    assert!(!bus.max_history_drop());
    assert_eq!(bus.event_concurrency, EventConcurrencyMode::Parallel);
    assert_eq!(
        bus.event_handler_concurrency,
        EventHandlerConcurrencyMode::Serial
    );
    assert_eq!(
        bus.event_handler_completion,
        EventHandlerCompletionMode::First
    );
    assert_eq!(bus.event_timeout, Some(30.0));
    bus.stop();
}

#[test]
fn test_event_bus_with_null_max_history_size_means_unlimited() {
    let bus = EventBus::new_with_options(
        Some("UnlimitedBus".to_string()),
        EventBusOptions {
            max_history_size: None,
            ..EventBusOptions::default()
        },
    );

    assert_eq!(bus.max_history_size(), None);
    bus.stop();
}

#[test]
fn test_unbounded_history_disables_history_rejection() {
    let bus = EventBus::new_with_options(
        Some("NoLimitBus".to_string()),
        EventBusOptions {
            max_history_size: None,
            ..EventBusOptions::default()
        },
    );

    for _ in 0..150 {
        let event = bus.emit::<UserActionEvent>(TypedEvent::new(EmptyPayload {}));
        block_on(event.wait_completed());
    }

    assert_eq!(bus.event_history_size(), 150);
    bus.stop();
}

#[test]
fn test_event_bus_with_null_event_timeout_disables_timeouts() {
    let bus = EventBus::new_with_options(
        Some("NoTimeoutBus".to_string()),
        EventBusOptions {
            event_timeout: None,
            ..EventBusOptions::default()
        },
    );

    assert_eq!(bus.event_timeout, None);
    bus.stop();
}

#[test]
fn test_event_bus_auto_generates_name_when_not_provided() {
    let bus = EventBus::new(None);
    assert_eq!(bus.name, "EventBus");
    bus.stop();
}

#[test]
fn test_eventbus_accepts_custom_id() {
    let custom_id = "018f8e40-1234-7000-8000-000000001234".to_string();
    let bus = EventBus::new_with_options(
        None,
        EventBusOptions {
            id: Some(custom_id.clone()),
            ..EventBusOptions::default()
        },
    );

    assert_eq!(bus.id, custom_id);
    assert!(bus.label().ends_with("#1234"));
    bus.stop();
}

#[test]
fn test_base_event_lifecycle_methods_are_callable_and_preserve_lifecycle_behavior() {
    let bus = EventBus::new(Some("LifecycleMethodInvocationBus".to_string()));

    let standalone = TypedEvent::<LifecycleMethodInvocationEvent>::new(EmptyPayload {});
    standalone.inner.mark_started();
    assert_eq!(
        standalone.inner.inner.lock().event_status,
        EventStatus::Started
    );
    standalone.inner.mark_completed();
    assert_eq!(
        standalone.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    block_on(standalone.wait_completed());

    let dispatched = bus.emit::<LifecycleMethodInvocationEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(dispatched.wait_completed());
    assert_eq!(
        dispatched.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    bus.stop();
}
