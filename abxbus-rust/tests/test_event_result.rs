use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use abxbus_rust::{
    base_event::{BaseEvent, EventResultsOptions},
    event_bus::EventBus,
    event_handler::{EventHandler, EventHandlerOptions, HandlerFuture},
    event_result::{EventResult, EventResultStatus},
    id::compute_handler_id,
    typed::{EventSpec, TypedEvent},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

fn object_keys(value: &Value) -> BTreeSet<String> {
    value
        .as_object()
        .expect("expected object")
        .keys()
        .cloned()
        .collect()
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

#[derive(Clone, Serialize, Deserialize)]
struct EmptyPayload {}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct ScreenshotEventResult {
    screenshot_base64: Option<String>,
    error: Option<String>,
}

struct ScreenshotEvent;
impl EventSpec for ScreenshotEvent {
    type Payload = EmptyPayload;
    type Result = ScreenshotEventResult;
    const EVENT_TYPE: &'static str = "ScreenshotEvent";
}

struct AccessorEvent;
impl EventSpec for AccessorEvent {
    type Payload = EmptyPayload;
    type Result = Value;
    const EVENT_TYPE: &'static str = "AccessorEvent";
}

struct StringResultEvent;
impl EventSpec for StringResultEvent {
    type Payload = EmptyPayload;
    type Result = String;
    const EVENT_TYPE: &'static str = "StringResultEvent";
}

struct IntEvent;
impl EventSpec for IntEvent {
    type Payload = EmptyPayload;
    type Result = i64;
    const EVENT_TYPE: &'static str = "IntEvent";
}

struct NormalEvent;
impl EventSpec for NormalEvent {
    type Payload = EmptyPayload;
    type Result = Value;
    const EVENT_TYPE: &'static str = "NormalEvent";
}

struct ForwardingTypedEvent;
impl EventSpec for ForwardingTypedEvent {
    type Payload = EmptyPayload;
    type Result = i64;
    const EVENT_TYPE: &'static str = "ForwardingTypedEvent";
}

fn schema_event(event_type: &str, schema: Option<Value>) -> Arc<BaseEvent> {
    let event = BaseEvent::new(event_type, serde_json::Map::new());
    event.inner.lock().event_result_type = schema;
    event
}

fn first_result(event: &Arc<BaseEvent>) -> EventResult {
    event
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("expected one event result")
}

#[test]
fn test_pydantic_model_result_casting() {
    let bus = EventBus::new(Some("pydantic_test_bus".to_string()));

    bus.on(
        "ScreenshotEvent",
        "screenshot_handler",
        |_event| async move {
            Ok(json!({
                "screenshot_base64": "fake_screenshot_data",
                "error": null
            }))
        },
    );

    let event = bus.emit::<ScreenshotEvent>(TypedEvent::new(EmptyPayload {}));
    let result = block_on(event.event_result(EventResultsOptions::default()))
        .expect("typed event_result")
        .expect("handler result");

    assert_eq!(
        result,
        ScreenshotEventResult {
            screenshot_base64: Some("fake_screenshot_data".to_string()),
            error: None,
        }
    );
    bus.stop();
}

#[test]
fn test_builtin_type_casting() {
    let bus = EventBus::new(Some("builtin_test_bus".to_string()));

    bus.on("StringResultEvent", "string_handler", |_event| async move {
        Ok(json!("42"))
    });
    bus.on(
        "IntEvent",
        "int_handler",
        |_event| async move { Ok(json!(123)) },
    );

    let string_event = bus.emit::<StringResultEvent>(TypedEvent::new(EmptyPayload {}));
    let string_result = block_on(string_event.event_result(EventResultsOptions::default()))
        .expect("string result")
        .expect("string handler result");
    assert_eq!(string_result, "42");

    let int_event = bus.emit::<IntEvent>(TypedEvent::new(EmptyPayload {}));
    let int_result = block_on(int_event.event_result(EventResultsOptions::default()))
        .expect("int result")
        .expect("int handler result");
    assert_eq!(int_result, 123);
    bus.stop();
}

#[test]
fn test_casting_failure_handling() {
    let bus = EventBus::new(Some("failure_test_bus".to_string()));

    bus.on("IntEvent", "bad_handler", |_event| async move {
        Ok(json!("not_a_number"))
    });

    let event = bus.emit::<IntEvent>(TypedEvent::new(EmptyPayload {}));
    let typed_error = block_on(event.event_result(EventResultsOptions {
        raise_if_any: false,
        raise_if_none: false,
    }))
    .expect_err("typed accessor should reject invalid integer result");
    assert!(typed_error.contains("invalid type") || typed_error.contains("i64"));

    let stored_result = event
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("event result");
    assert_eq!(stored_result.status, EventResultStatus::Completed);
    assert_eq!(stored_result.error, None);
    assert_eq!(stored_result.result, Some(json!("not_a_number")));
    bus.stop();
}

#[test]
fn test_no_casting_when_no_result_type() {
    let bus = EventBus::new(Some("normal_test_bus".to_string()));

    bus.on("NormalEvent", "normal_handler", |_event| async move {
        Ok(json!({"raw": "data"}))
    });

    let event = bus.emit::<NormalEvent>(TypedEvent::new(EmptyPayload {}));
    let result = block_on(event.event_result(EventResultsOptions::default()))
        .expect("raw result")
        .expect("handler result");

    assert_eq!(result, json!({"raw": "data"}));
    assert_eq!(event.inner.inner.lock().event_result_type, None);
    bus.stop();
}

#[test]
fn test_typed_accessors_normalize_forwarded_event_results_to_none() {
    let bus = EventBus::new(Some("forwarded_result_normalization_bus".to_string()));

    bus.on(
        "ForwardingTypedEvent",
        "forward_handler",
        |_event| async move {
            Ok(BaseEvent::new("ForwardedEventFromHandler", serde_json::Map::new()).to_json_value())
        },
    );

    let event = bus.emit::<ForwardingTypedEvent>(TypedEvent::new(EmptyPayload {}));

    let result = block_on(event.event_result(EventResultsOptions {
        raise_if_any: false,
        raise_if_none: false,
    }))
    .expect("typed event_result");
    let results_list = block_on(event.event_results_list(EventResultsOptions {
        raise_if_any: false,
        raise_if_none: false,
    }))
    .expect("typed event_results_list");
    assert_eq!(result, None);
    assert!(results_list.is_empty());

    let raw_results = event.inner.inner.lock().event_results.clone();
    assert!(raw_results.values().any(|result| result
        .result
        .as_ref()
        .is_some_and(|value| value.get("event_type")
            == Some(&json!("ForwardedEventFromHandler"))
            && value.get("event_id").is_some())));
    bus.stop();
}

#[test]
fn test_event_result_defaults() {
    let handler = EventHandler {
        id: "h1".into(),
        event_pattern: "work".into(),
        handler_name: "handler".into(),
        handler_file_path: None,
        handler_timeout: None,
        handler_slow_timeout: None,
        handler_registered_at: "2026-01-01T00:00:00.000Z".into(),
        eventbus_name: "bus".into(),
        eventbus_id: "bus-id".into(),
        callable: None,
    };

    let result = EventResult::new("event-id".into(), handler, Some(5.0));
    assert_eq!(result.status, EventResultStatus::Pending);
    assert_eq!(result.timeout, Some(5.0));
}

#[test]
fn test_event_result_serializes_handler_metadata_and_derived_fields() {
    let handler = EventHandler {
        id: "h1".into(),
        event_pattern: "StandaloneEvent".into(),
        handler_name: "handler".into(),
        handler_file_path: Some("~/project/app.rs:123".into()),
        handler_timeout: Some(10.0),
        handler_slow_timeout: Some(2.0),
        handler_registered_at: "2026-01-01T00:00:00.000Z".into(),
        eventbus_name: "StandaloneBus".into(),
        eventbus_id: "018f8e40-1234-7000-8000-000000001234".into(),
        callable: None,
    };

    let mut result = EventResult::new("event-id".into(), handler.clone(), Some(5.0));
    result.status = EventResultStatus::Completed;
    result.started_at = Some("2026-01-01T00:00:01.000Z".into());
    result.completed_at = Some("2026-01-01T00:00:02.000Z".into());
    result.result = Some(json!("ok"));
    result.event_children = vec!["child-id".into()];

    let payload = serde_json::to_value(&result).expect("event result json");
    assert_eq!(object_keys(&payload), expected_event_result_json_keys());
    assert!(payload.get("handler").is_none());
    assert!(payload.get("timeout").is_none());
    assert_eq!(payload["handler_id"], handler.id);
    assert_eq!(payload["handler_name"], handler.handler_name);
    assert_eq!(payload["handler_event_pattern"], handler.event_pattern);
    assert_eq!(payload["eventbus_name"], handler.eventbus_name);
    assert_eq!(payload["eventbus_id"], handler.eventbus_id);
    assert_eq!(payload["result"], "ok");
    assert_eq!(payload["event_children"], json!(["child-id"]));

    let restored: EventResult =
        serde_json::from_value(payload).expect("flat event result should deserialize");
    assert_eq!(restored.handler.id, handler.id);
    assert_eq!(restored.handler.handler_name, handler.handler_name);
    assert_eq!(restored.status, EventResultStatus::Completed);
    assert_eq!(restored.result, Some(json!("ok")));
    assert_eq!(restored.event_children, vec!["child-id".to_string()]);
}

#[test]
fn test_event_results_capture_handler_return_values() {
    let bus = EventBus::new(Some("ResultCaptureBus".to_string()));
    bus.on("StringResultEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });

    let event = bus.emit::<StringResultEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());

    let results = event.inner.inner.lock().event_results.clone();
    assert_eq!(results.len(), 1);
    let result = results.values().next().expect("result");
    assert_eq!(result.status, EventResultStatus::Completed);
    assert_eq!(result.result, Some(json!("ok")));
    bus.stop();
}

#[test]
fn test_event_result_type_validates_handler_results() {
    let bus = EventBus::new(Some("ResultSchemaBus".to_string()));
    let schema = json!({
        "type": "object",
        "properties": {
            "value": {"type": "string"},
            "count": {"type": "number"}
        },
        "required": ["value", "count"]
    });

    bus.on("ObjectResultEvent", "handler", |_event| async move {
        Ok(json!({"value": "hello", "count": 2}))
    });

    let event = bus.emit_base(schema_event("ObjectResultEvent", Some(schema)));
    block_on(event.event_completed());

    let result = first_result(&event);
    assert_eq!(result.status, EventResultStatus::Completed);
    assert_eq!(result.result, Some(json!({"value": "hello", "count": 2})));
    bus.stop();
}

#[test]
fn test_event_result_type_allows_undefined_handler_return_values() {
    let bus = EventBus::new(Some("ResultSchemaUndefinedBus".to_string()));
    let schema = json!({
        "type": "object",
        "properties": {
            "value": {"type": "string"},
            "count": {"type": "number"}
        },
        "required": ["value", "count"]
    });

    bus.on("ObjectResultEvent", "handler", |_event| async move {
        Ok(Value::Null)
    });

    let event = bus.emit_base(schema_event("ObjectResultEvent", Some(schema)));
    block_on(event.event_completed());

    let result = first_result(&event);
    assert_eq!(result.status, EventResultStatus::Completed);
    assert_eq!(result.result, Some(Value::Null));
    bus.stop();
}

#[test]
fn test_invalid_result_marks_handler_error() {
    let bus = EventBus::new(Some("ResultSchemaErrorBus".to_string()));
    let schema = json!({
        "type": "object",
        "properties": {
            "value": {"type": "string"},
            "count": {"type": "number"}
        },
        "required": ["value", "count"]
    });

    bus.on("ObjectResultEvent", "handler", |_event| async move {
        Ok(json!({"value": "bad", "count": "nope"}))
    });

    let event = bus.emit_base(schema_event("ObjectResultEvent", Some(schema)));
    block_on(event.event_completed());

    let result = first_result(&event);
    assert_eq!(result.status, EventResultStatus::Error);
    assert!(result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("EventHandlerResultSchemaError"));
    assert!(!event.event_errors().is_empty());
    bus.stop();
}

#[test]
fn test_event_with_no_result_schema_stores_raw_values() {
    let bus = EventBus::new(Some("NoSchemaBus".to_string()));

    bus.on("NoResultSchemaEvent", "handler", |_event| async move {
        Ok(json!({"raw": true}))
    });

    let event = bus.emit_base(schema_event("NoResultSchemaEvent", None));
    block_on(event.event_completed());

    let result = first_result(&event);
    assert_eq!(result.status, EventResultStatus::Completed);
    assert_eq!(result.result, Some(json!({"raw": true})));
    bus.stop();
}

#[test]
fn test_event_result_json_omits_result_type_and_derives_from_parent_event() {
    let bus = EventBus::new(Some("ResultTypeDeriveBus".to_string()));
    bus.on("StringResultEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });

    let event = bus.emit::<StringResultEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());

    let result = event
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("result");
    let payload = serde_json::to_value(&result).expect("event result json");

    assert!(!payload
        .as_object()
        .expect("object")
        .contains_key("result_type"));
    assert!(payload.get("handler").is_none());
    assert!(payload["handler_id"].is_string());
    assert!(payload["handler_name"].is_string());
    assert!(payload["handler_event_pattern"].is_string());
    assert!(payload["eventbus_name"].is_string());
    assert!(payload["eventbus_id"].is_string());
    assert!(payload["handler_registered_at"].is_string());
    assert_eq!(event.inner.inner.lock().event_result_type, None);
    bus.stop();
}

#[test]
fn test_eventhandler_json_roundtrips_handler_metadata() {
    let handler = EventHandler {
        id: "h1".into(),
        event_pattern: "StandaloneEvent".into(),
        handler_name: "pkg.module.handler".into(),
        handler_file_path: Some("~/project/app.rs:123".into()),
        handler_timeout: None,
        handler_slow_timeout: None,
        handler_registered_at: "2025-01-02T03:04:05.678Z".into(),
        eventbus_name: "StandaloneBus".into(),
        eventbus_id: "018f8e40-1234-7000-8000-000000001234".into(),
        callable: None,
    };

    let dumped = handler.to_json_value();
    let loaded = EventHandler::from_json_value(dumped);

    assert_eq!(loaded.id, handler.id);
    assert_eq!(loaded.event_pattern, "StandaloneEvent");
    assert_eq!(loaded.eventbus_name, "StandaloneBus");
    assert_eq!(loaded.eventbus_id, "018f8e40-1234-7000-8000-000000001234");
    assert_eq!(loaded.handler_name, "pkg.module.handler");
    assert_eq!(
        loaded.handler_file_path.as_deref(),
        Some("~/project/app.rs:123")
    );
}

#[test]
fn test_eventhandler_computehandlerid_matches_uuidv5_seed_algorithm() {
    let expected_seed =
        "018f8e40-1234-7000-8000-000000001234|pkg.module.handler|~/project/app.py:123|2025-01-02T03:04:05.678901000Z|StandaloneEvent";
    let expected_id = "19ea9fe8-cfbe-541e-8a35-2579e4e9efff";

    let eventbus_id = "018f8e40-1234-7000-8000-000000001234";
    let handler_name = "pkg.module.handler";
    let handler_file_path = Some("~/project/app.py:123");
    let handler_registered_at = "2025-01-02T03:04:05.678901000Z";
    let event_pattern = "StandaloneEvent";
    let actual_seed = format!(
        "{eventbus_id}|{handler_name}|{}|{handler_registered_at}|{event_pattern}",
        handler_file_path.expect("handler path")
    );
    let computed_id = compute_handler_id(
        eventbus_id,
        handler_name,
        handler_file_path,
        handler_registered_at,
        event_pattern,
    );

    assert_eq!(actual_seed, expected_seed);
    assert_eq!(computed_id, expected_id);
}

#[test]
fn test_eventhandler_fromcallable_supports_id_override_and_detect_handler_file_path_toggle() {
    let explicit_id = "018f8e40-1234-7000-8000-000000009999";
    let callable = Arc::new(|_event| -> HandlerFuture { Box::pin(async { Ok(json!("ok")) }) });

    let explicit = EventHandler::from_callable_with_options(
        "StandaloneEvent".to_string(),
        "handler".to_string(),
        "StandaloneBus".to_string(),
        "018f8e40-1234-7000-8000-000000001234".to_string(),
        callable.clone(),
        EventHandlerOptions {
            id: Some(explicit_id.to_string()),
            detect_handler_file_path: Some(false),
            ..EventHandlerOptions::default()
        },
    );
    assert_eq!(explicit.id, explicit_id);

    let no_detect = EventHandler::from_callable_with_options(
        "StandaloneEvent".to_string(),
        "handler".to_string(),
        "StandaloneBus".to_string(),
        "018f8e40-1234-7000-8000-000000001234".to_string(),
        callable,
        EventHandlerOptions {
            detect_handler_file_path: Some(false),
            ..EventHandlerOptions::default()
        },
    );
    assert_eq!(no_detect.handler_file_path, None);
}

#[test]
fn test_event_result_update_keeps_consistent_ordering_semantics_for_status_result_error() {
    let handler = EventHandler {
        id: "h1".into(),
        event_pattern: "StandaloneEvent".into(),
        handler_name: "handler".into(),
        handler_file_path: None,
        handler_timeout: None,
        handler_slow_timeout: None,
        handler_registered_at: "2026-01-01T00:00:00.000Z".into(),
        eventbus_name: "StandaloneBus".into(),
        eventbus_id: "018f8e40-1234-7000-8000-000000001234".into(),
        callable: None,
    };

    let mut result = EventResult::new("event-id".into(), handler, None);
    result.error = Some("RuntimeError: existing".to_string());

    result.update(Some(EventResultStatus::Completed), None, None);
    assert_eq!(result.status, EventResultStatus::Completed);
    assert_eq!(result.error.as_deref(), Some("RuntimeError: existing"));

    result.update(
        Some(EventResultStatus::Error),
        Some(Some(json!("seeded"))),
        None,
    );
    assert_eq!(result.result, Some(json!("seeded")));
    assert_eq!(result.status, EventResultStatus::Error);
}

#[test]
fn test_run_handler_is_a_no_op_for_already_settled_results() {
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let calls_for_handler = handler_calls.clone();
    let handler = EventHandler {
        id: "h1".into(),
        event_pattern: "RunHandlerSettledEvent".into(),
        handler_name: "handler".into(),
        handler_file_path: None,
        handler_timeout: None,
        handler_slow_timeout: None,
        handler_registered_at: "2026-01-01T00:00:00.000Z".into(),
        eventbus_name: "RunHandlerSettledBus".into(),
        eventbus_id: "018f8e40-1234-7000-8000-000000001234".into(),
        callable: Some(Arc::new(move |_event| -> HandlerFuture {
            let calls = calls_for_handler.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(json!("ok"))
            })
        })),
    };
    let event = schema_event("RunHandlerSettledEvent", None);
    let mut result = EventResult::new(event.inner.lock().event_id.clone(), handler, None);
    result.status = EventResultStatus::Completed;
    result.result = Some(json!("settled"));

    let value = block_on(result.run_handler(event, None)).expect("settled result");

    assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
    assert_eq!(result.status, EventResultStatus::Completed);
    assert_eq!(value, json!("settled"));
}

#[test]
fn test_handler_result_stays_pending_while_waiting_for_handler_lock_entry() {
    let bus = EventBus::new_with_options(
        Some("RunHandlerLockWaitBus".to_string()),
        abxbus_rust::event_bus::EventBusOptions {
            event_handler_concurrency: abxbus_rust::types::EventHandlerConcurrencyMode::Serial,
            ..abxbus_rust::event_bus::EventBusOptions::default()
        },
    );
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));

    let release_for_first = release_rx.clone();
    bus.on("RunHandlerLockWaitEvent", "first_handler", move |_event| {
        let started_tx = started_tx.clone();
        let release_rx = release_for_first.clone();
        async move {
            let _ = started_tx.send(());
            release_rx
                .lock()
                .expect("release lock")
                .recv_timeout(Duration::from_secs(2))
                .expect("release signal");
            Ok(json!("first"))
        }
    });
    bus.on(
        "RunHandlerLockWaitEvent",
        "second_handler",
        |_event| async move {
            thread::sleep(Duration::from_millis(1));
            Ok(json!("second"))
        },
    );

    let event = bus.emit_base(schema_event("RunHandlerLockWaitEvent", None));
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("first handler should start");

    let results = event.inner.lock().event_results.clone();
    assert_eq!(results.len(), 2);
    let first_result = results
        .values()
        .find(|result| result.handler.handler_name == "first_handler")
        .expect("first result");
    let second_result = results
        .values()
        .find(|result| result.handler.handler_name == "second_handler")
        .expect("second result");
    assert_eq!(first_result.status, EventResultStatus::Started);
    assert_eq!(second_result.status, EventResultStatus::Pending);

    thread::sleep(Duration::from_millis(20));
    let second_status = event
        .inner
        .lock()
        .event_results
        .values()
        .find(|result| result.handler.handler_name == "second_handler")
        .expect("second result")
        .status;
    assert_eq!(second_status, EventResultStatus::Pending);

    release_tx.send(()).expect("release send");
    block_on(event.event_completed());
    let completed_results = event.inner.lock().event_results.clone();
    assert_eq!(
        completed_results
            .values()
            .find(|result| result.handler.handler_name == "first_handler")
            .expect("first result")
            .status,
        EventResultStatus::Completed
    );
    assert_eq!(
        completed_results
            .values()
            .find(|result| result.handler.handler_name == "second_handler")
            .expect("second result")
            .status,
        EventResultStatus::Completed
    );
    bus.stop();
}

#[test]
fn test_slow_handler_warning_is_based_on_handler_runtime_after_lock_wait() {
    let bus = EventBus::new_with_options(
        Some("RunHandlerSlowAfterLockWaitBus".to_string()),
        abxbus_rust::event_bus::EventBusOptions {
            event_handler_concurrency: abxbus_rust::types::EventHandlerConcurrencyMode::Serial,
            event_handler_slow_timeout: Some(0.01),
            ..abxbus_rust::event_bus::EventBusOptions::default()
        },
    );
    let (first_started_tx, first_started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));

    let release_for_first = release_rx.clone();
    bus.on(
        "RunHandlerSlowAfterLockWaitEvent",
        "first_handler",
        move |_event| {
            let first_started_tx = first_started_tx.clone();
            let release_rx = release_for_first.clone();
            async move {
                let _ = first_started_tx.send(());
                release_rx
                    .lock()
                    .expect("release lock")
                    .recv_timeout(Duration::from_secs(2))
                    .expect("release signal");
                thread::sleep(Duration::from_millis(40));
                Ok(json!("first"))
            }
        },
    );
    bus.on_with_options(
        "RunHandlerSlowAfterLockWaitEvent",
        "second_handler",
        EventHandlerOptions {
            handler_slow_timeout: Some(0.01),
            ..EventHandlerOptions::default()
        },
        |_event| async move {
            thread::sleep(Duration::from_millis(30));
            Ok(json!("second"))
        },
    );

    let event = bus.emit_base(schema_event("RunHandlerSlowAfterLockWaitEvent", None));
    first_started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("first handler should start");

    while event.inner.lock().event_results.len() < 2 {
        thread::sleep(Duration::from_millis(1));
    }
    let second_status = || {
        event
            .inner
            .lock()
            .event_results
            .values()
            .find(|result| result.handler.handler_name == "second_handler")
            .expect("second result")
            .status
    };
    assert_eq!(second_status(), EventResultStatus::Pending);
    thread::sleep(Duration::from_millis(20));
    assert_eq!(second_status(), EventResultStatus::Pending);

    release_tx.send(()).expect("release send");
    block_on(event.event_completed());
    assert_eq!(second_status(), EventResultStatus::Completed);
    bus.stop();
}

#[test]
fn test_event_result_error_json_roundtrip_preserves_error_type_and_message() {
    let payload = json!({
        "id": "018f8e40-1234-7000-8000-000000009999",
        "status": "error",
        "event_id": "018f8e40-1234-7000-8000-000000001111",
        "handler_id": "h1",
        "handler_name": "handler",
        "handler_file_path": null,
        "handler_timeout": null,
        "handler_slow_timeout": null,
        "handler_registered_at": "2026-01-01T00:00:00.000Z",
        "handler_event_pattern": "StandaloneEvent",
        "eventbus_name": "StandaloneBus",
        "eventbus_id": "018f8e40-1234-7000-8000-000000001234",
        "started_at": "2026-01-01T00:00:01.000Z",
        "completed_at": "2026-01-01T00:00:02.000Z",
        "result": null,
        "error": {
            "type": "EventHandlerTimeoutError",
            "message": "handler exceeded 0.01s"
        },
        "event_children": []
    });

    let restored: EventResult =
        serde_json::from_value(payload.clone()).expect("event result json should deserialize");
    assert_eq!(
        restored.error.as_deref(),
        Some("EventHandlerTimeoutError: handler exceeded 0.01s")
    );
    assert_eq!(serde_json::to_value(&restored).expect("serialize"), payload);
}

#[test]
fn test_eventresultslist_returns_filtered_values_by_default_and_can_return_raw_values_with_include()
{
    let bus = EventBus::new(Some("EventResultsListBus".to_string()));

    bus.on("AccessorEvent", "first_handler", |_event| async move {
        Ok(json!("first"))
    });
    bus.on("AccessorEvent", "null_handler", |_event| async move {
        Ok(Value::Null)
    });
    bus.on("AccessorEvent", "second_handler", |_event| async move {
        Ok(json!("second"))
    });

    let event = bus.emit::<AccessorEvent>(TypedEvent::new(EmptyPayload {}));

    let default_values = block_on(event.inner.event_results_list(EventResultsOptions {
        raise_if_any: false,
        raise_if_none: true,
    }))
    .expect("default values");
    assert_eq!(default_values, vec![json!("first"), json!("second")]);

    let raw_values = block_on(event.inner.event_results_list_with_filter(
        EventResultsOptions {
            raise_if_any: false,
            raise_if_none: true,
        },
        |result| result.status == EventResultStatus::Completed && result.error.is_none(),
    ))
    .expect("raw values");
    assert_eq!(
        raw_values,
        vec![json!("first"), Value::Null, json!("second")]
    );

    bus.stop();
}

#[test]
fn test_event_result_returns_first_filtered_value_in_handler_registration_order() {
    let bus = EventBus::new(Some("EventResultFirstValueBus".to_string()));

    bus.on("AccessorEvent", "null_handler", |_event| async move {
        Ok(Value::Null)
    });
    bus.on("AccessorEvent", "winner_handler", |_event| async move {
        Ok(json!("winner"))
    });
    bus.on("AccessorEvent", "late_handler", |_event| async move {
        Ok(json!("late"))
    });

    let event = bus.emit::<AccessorEvent>(TypedEvent::new(EmptyPayload {}));
    let first_value = block_on(event.inner.event_result(EventResultsOptions {
        raise_if_any: false,
        raise_if_none: true,
    }))
    .expect("first result");

    assert_eq!(first_value, Some(json!("winner")));
    bus.stop();
}

#[test]
fn test_eventresultslist_supports_include_raise_if_any_raise_if_none_arguments() {
    let error_bus = EventBus::new(Some("EventResultsListErrorsBus".to_string()));
    error_bus.on("AccessorEvent", "failing_handler", |_event| async move {
        Err("boom".to_string())
    });
    error_bus.on("AccessorEvent", "working_handler", |_event| async move {
        Ok(json!("ok"))
    });

    let error_event = error_bus.emit::<AccessorEvent>(TypedEvent::new(EmptyPayload {}));

    let raised = block_on(
        error_event
            .inner
            .event_results_list(EventResultsOptions::default()),
    )
    .expect_err("raise_if_any should surface handler errors");
    assert!(raised.contains("boom"));

    let suppressed = block_on(error_event.inner.event_results_list(EventResultsOptions {
        raise_if_any: false,
        raise_if_none: true,
    }))
    .expect("raise_if_any false should return successful values");
    assert_eq!(suppressed, vec![json!("ok")]);
    error_bus.stop();

    let none_bus = EventBus::new(Some("EventResultsListNoneBus".to_string()));
    none_bus.on("AccessorEvent", "null_handler", |_event| async move {
        Ok(Value::Null)
    });
    let none_event = none_bus.emit::<AccessorEvent>(TypedEvent::new(EmptyPayload {}));

    let empty_error = block_on(none_event.inner.event_results_list(EventResultsOptions {
        raise_if_any: false,
        raise_if_none: true,
    }))
    .expect_err("raise_if_none should reject empty filtered results");
    assert!(empty_error.contains("Expected at least one handler"));

    let empty_values = block_on(none_event.inner.event_results_list(EventResultsOptions {
        raise_if_any: false,
        raise_if_none: false,
    }))
    .expect("raise_if_none false should allow empty filtered results");
    assert!(empty_values.is_empty());
    none_bus.stop();
}
