use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use abxbus_rust::{
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
