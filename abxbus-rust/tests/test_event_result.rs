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
    typed::{BaseEventHandle, EventSpec},
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
    type payload = EmptyPayload;
    type event_result_type = ScreenshotEventResult;
    const event_type: &'static str = "ScreenshotEvent";
}

struct AccessorEvent;
impl EventSpec for AccessorEvent {
    type payload = EmptyPayload;
    type event_result_type = Value;
    const event_type: &'static str = "AccessorEvent";
}

struct StringResultEvent;
impl EventSpec for StringResultEvent {
    type payload = EmptyPayload;
    type event_result_type = String;
    const event_type: &'static str = "StringResultEvent";
}

struct IntEvent;
impl EventSpec for IntEvent {
    type payload = EmptyPayload;
    type event_result_type = i64;
    const event_type: &'static str = "IntEvent";
}

struct NormalEvent;
impl EventSpec for NormalEvent {
    type payload = EmptyPayload;
    type event_result_type = Value;
    const event_type: &'static str = "NormalEvent";
}

struct ForwardingBaseEventHandle;
impl EventSpec for ForwardingBaseEventHandle {
    type payload = EmptyPayload;
    type event_result_type = i64;
    const event_type: &'static str = "ForwardingBaseEventHandle";
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

    bus.on_raw(
        "ScreenshotEvent",
        "screenshot_handler",
        |_event| async move {
            Ok(json!({
                "screenshot_base64": "fake_screenshot_data",
                "error": null
            }))
        },
    );

    let event = bus.emit(BaseEventHandle::<ScreenshotEvent>::new(EmptyPayload {}));
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

    bus.on_raw("StringResultEvent", "string_handler", |_event| async move {
        Ok(json!("42"))
    });
    bus.on_raw(
        "IntEvent",
        "int_handler",
        |_event| async move { Ok(json!(123)) },
    );

    let string_event = bus.emit(BaseEventHandle::<StringResultEvent>::new(EmptyPayload {}));
    let string_result = block_on(string_event.event_result(EventResultsOptions::default()))
        .expect("string result")
        .expect("string handler result");
    assert_eq!(string_result, "42");

    let int_event = bus.emit(BaseEventHandle::<IntEvent>::new(EmptyPayload {}));
    let int_result = block_on(int_event.event_result(EventResultsOptions::default()))
        .expect("int result")
        .expect("int handler result");
    assert_eq!(int_result, 123);
    bus.stop();
}

#[test]
fn test_casting_failure_handling() {
    let bus = EventBus::new(Some("failure_test_bus".to_string()));

    bus.on_raw("IntEvent", "bad_handler", |_event| async move {
        Ok(json!("not_a_number"))
    });

    let event = bus.emit(BaseEventHandle::<IntEvent>::new(EmptyPayload {}));
    let typed_result = block_on(event.event_result(EventResultsOptions {
        raise_if_any: false,
        raise_if_none: false,
        timeout: None,
    }))
    .expect("suppressed schema error");
    assert_eq!(typed_result, None);

    let stored_result = event
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("event result");
    assert_eq!(stored_result.status, EventResultStatus::Error);
    assert!(stored_result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("EventHandlerResultSchemaError"));
    assert_eq!(stored_result.result, None);
    bus.stop();
}

#[test]
fn test_no_casting_when_no_result_type() {
    let bus = EventBus::new(Some("normal_test_bus".to_string()));

    bus.on_raw("NormalEvent", "normal_handler", |_event| async move {
        Ok(json!({"raw": "data"}))
    });

    let event = bus.emit(BaseEventHandle::<NormalEvent>::new(EmptyPayload {}));
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

    bus.on_raw(
        "ForwardingBaseEventHandle",
        "forward_handler",
        |_event| async move {
            Ok(BaseEvent::new("ForwardedEventFromHandler", serde_json::Map::new()).to_json_value())
        },
    );

    let event = bus.emit(BaseEventHandle::<ForwardingBaseEventHandle>::new(
        EmptyPayload {},
    ));

    let result = block_on(event.event_result(EventResultsOptions {
        raise_if_any: false,
        raise_if_none: false,
        timeout: None,
    }))
    .expect("typed event_result");
    let results_list = block_on(event.event_results_list(EventResultsOptions {
        raise_if_any: false,
        raise_if_none: false,
        timeout: None,
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
    bus.on_raw("StringResultEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });

    let event = bus.emit(BaseEventHandle::<StringResultEvent>::new(EmptyPayload {}));
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

    bus.on_raw("ObjectResultEvent", "handler", |_event| async move {
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

    bus.on_raw("ObjectResultEvent", "handler", |_event| async move {
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

    bus.on_raw("ObjectResultEvent", "handler", |_event| async move {
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

    bus.on_raw("NoResultSchemaEvent", "handler", |_event| async move {
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
    bus.on_raw("StringResultEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });

    let event = bus.emit(BaseEventHandle::<StringResultEvent>::new(EmptyPayload {}));
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
    assert_eq!(
        event.inner.inner.lock().event_result_type,
        Some(json!({"type": "string"}))
    );
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
fn test_event_handler_from_callable_supports_id_override_and_detect_handler_file_path_toggle() {
    test_eventhandler_fromcallable_supports_id_override_and_detect_handler_file_path_toggle();
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
    bus.on_raw("RunHandlerLockWaitEvent", "first_handler", move |_event| {
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
    bus.on_raw(
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
    bus.on_raw(
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
    bus.on_raw_with_options(
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
fn test_run_handler_marks_started_after_handler_lock_entry() {
    test_handler_result_stays_pending_while_waiting_for_handler_lock_entry();
}

#[test]
fn test_run_handler_starts_slow_monitor_after_lock_wait() {
    test_slow_handler_warning_is_based_on_handler_runtime_after_lock_wait();
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

    bus.on_raw("AccessorEvent", "first_handler", |_event| async move {
        Ok(json!("first"))
    });
    bus.on_raw("AccessorEvent", "null_handler", |_event| async move {
        Ok(Value::Null)
    });
    bus.on_raw("AccessorEvent", "second_handler", |_event| async move {
        Ok(json!("second"))
    });

    let event = bus.emit(BaseEventHandle::<AccessorEvent>::new(EmptyPayload {}));

    let default_values = block_on(event.inner.event_results_list(EventResultsOptions {
        raise_if_any: false,
        raise_if_none: true,
        timeout: None,
    }))
    .expect("default values");
    assert_eq!(default_values, vec![json!("first"), json!("second")]);

    let raw_values = block_on(event.inner.event_results_list_with_filter(
        EventResultsOptions {
            raise_if_any: false,
            raise_if_none: true,
            timeout: None,
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
fn test_eventresult_update_keeps_consistent_ordering_semantics_for_status_result_error() {
    test_event_result_update_keeps_consistent_ordering_semantics_for_status_result_error();
}

#[test]
fn test_runhandler_is_a_no_op_for_already_settled_results() {
    test_run_handler_is_a_no_op_for_already_settled_results();
}

#[test]
fn test_event_result_returns_first_filtered_value_in_handler_registration_order() {
    let bus = EventBus::new_with_options(
        Some("EventResultFirstValueBus".to_string()),
        abxbus_rust::event_bus::EventBusOptions {
            event_handler_concurrency: abxbus_rust::types::EventHandlerConcurrencyMode::Parallel,
            ..abxbus_rust::event_bus::EventBusOptions::default()
        },
    );
    let completed_order = Arc::new(Mutex::new(Vec::<String>::new()));
    let registered_at = "2026-01-01T00:00:00.000Z".to_string();

    let null_order = completed_order.clone();
    bus.on_raw_sync_with_options(
        "AccessorEvent",
        "null_handler",
        EventHandlerOptions {
            id: Some("00000000-0000-5000-8000-00000000000b".to_string()),
            handler_registered_at: Some(registered_at.clone()),
            ..EventHandlerOptions::default()
        },
        move |_event| {
            thread::sleep(Duration::from_millis(30));
            null_order.lock().unwrap().push("null".to_string());
            Ok(Value::Null)
        },
    );
    let winner_order = completed_order.clone();
    bus.on_raw_sync_with_options(
        "AccessorEvent",
        "winner_handler",
        EventHandlerOptions {
            id: Some("00000000-0000-5000-8000-00000000000c".to_string()),
            handler_registered_at: Some(registered_at.clone()),
            ..EventHandlerOptions::default()
        },
        move |_event| {
            thread::sleep(Duration::from_millis(20));
            winner_order.lock().unwrap().push("winner".to_string());
            Ok(json!("winner"))
        },
    );
    let late_order = completed_order.clone();
    bus.on_raw_sync_with_options(
        "AccessorEvent",
        "late_handler",
        EventHandlerOptions {
            id: Some("00000000-0000-5000-8000-00000000000a".to_string()),
            handler_registered_at: Some(registered_at),
            ..EventHandlerOptions::default()
        },
        move |_event| {
            late_order.lock().unwrap().push("late".to_string());
            Ok(json!("late"))
        },
    );

    let event = bus.emit(BaseEventHandle::<AccessorEvent>::new(EmptyPayload {}));
    let first_value = block_on(event.inner.event_result(EventResultsOptions {
        raise_if_any: false,
        raise_if_none: true,
        timeout: None,
    }))
    .expect("first result");

    assert_eq!(first_value, Some(json!("winner")));
    let raw_values = block_on(event.inner.event_results_list_with_filter(
        EventResultsOptions {
            raise_if_any: false,
            raise_if_none: false,
            timeout: None,
        },
        |_| true,
    ))
    .expect("raw values");
    assert_eq!(
        raw_values,
        vec![Value::Null, json!("winner"), json!("late")]
    );
    assert_eq!(
        completed_order.lock().unwrap().clone(),
        vec!["late".to_string(), "winner".to_string(), "null".to_string()]
    );
    bus.stop();
}

#[test]
fn test_base_event_from_json_preserves_event_results_object_registration_order() {
    let event_id = "018f8e40-1234-7000-8000-000000001260";
    let bus_id = "018f8e40-1234-7000-8000-000000001261";
    let registered_at = "2026-01-01T00:00:00.000Z";
    let started_at = "2026-01-01T00:00:01.000Z";
    let completed_at = "2026-01-01T00:00:02.000Z";

    let result_payload = |handler_id: &str, handler_name: &str, result: Value| {
        json!({
            "id": format!("018f8e40-1234-7000-8000-{}", &handler_id[handler_id.len() - 12..]),
            "status": "completed",
            "event_id": event_id,
            "handler_id": handler_id,
            "handler_name": handler_name,
            "handler_file_path": null,
            "handler_timeout": null,
            "handler_slow_timeout": null,
            "handler_registered_at": registered_at,
            "handler_event_pattern": "AccessorEvent",
            "eventbus_name": "RestoredOrderBus",
            "eventbus_id": bus_id,
            "timeout": null,
            "started_at": started_at,
            "completed_at": completed_at,
            "result": result,
            "error": null,
            "event_children": [],
        })
    };

    let null_id = "00000000-0000-5000-8000-00000000000b";
    let winner_id = "00000000-0000-5000-8000-00000000000c";
    let late_id = "00000000-0000-5000-8000-00000000000a";
    let mut event_results = serde_json::Map::new();
    event_results.insert(
        null_id.to_string(),
        result_payload(null_id, "null_handler", Value::Null),
    );
    event_results.insert(
        winner_id.to_string(),
        result_payload(winner_id, "winner_handler", json!("winner")),
    );
    event_results.insert(
        late_id.to_string(),
        result_payload(late_id, "late_handler", json!("late")),
    );
    let mut event_payload = json!({
        "event_type": "AccessorEvent",
        "event_version": "0.0.1",
        "event_id": event_id,
        "event_created_at": "2026-01-01T00:00:00.000Z",
        "event_status": "completed",
        "event_started_at": started_at,
        "event_completed_at": completed_at,
    });
    event_payload["event_results"] = Value::Object(event_results);
    let event = BaseEvent::from_json_value(event_payload);

    let raw_values = block_on(event.event_results_list_with_filter(
        EventResultsOptions {
            raise_if_any: false,
            raise_if_none: false,
            timeout: None,
        },
        |_| true,
    ))
    .expect("raw values");
    assert_eq!(
        raw_values,
        vec![Value::Null, json!("winner"), json!("late")]
    );

    let filtered_values = block_on(event.event_results_list(EventResultsOptions {
        raise_if_any: false,
        raise_if_none: true,
        timeout: None,
    }))
    .expect("filtered values");
    assert_eq!(filtered_values, vec![json!("winner"), json!("late")]);

    let serialized = event.to_json_value();
    let serialized_order: Vec<String> = serialized["event_results"]
        .as_object()
        .expect("event_results object")
        .keys()
        .cloned()
        .collect();
    assert_eq!(
        serialized_order,
        vec![
            null_id.to_string(),
            winner_id.to_string(),
            late_id.to_string()
        ]
    );
}

#[test]
fn test_eventresultslist_supports_include_raise_if_any_raise_if_none_arguments() {
    let error_bus = EventBus::new(Some("EventResultsListErrorsBus".to_string()));
    error_bus.on_raw("AccessorEvent", "failing_handler", |_event| async move {
        Err("boom".to_string())
    });
    error_bus.on_raw("AccessorEvent", "working_handler", |_event| async move {
        Ok(json!("ok"))
    });

    let error_event = error_bus.emit(BaseEventHandle::<AccessorEvent>::new(EmptyPayload {}));

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
        timeout: None,
    }))
    .expect("raise_if_any false should return successful values");
    assert_eq!(suppressed, vec![json!("ok")]);
    error_bus.stop();

    let none_bus = EventBus::new(Some("EventResultsListNoneBus".to_string()));
    none_bus.on_raw("AccessorEvent", "null_handler", |_event| async move {
        Ok(Value::Null)
    });
    let none_event = none_bus.emit(BaseEventHandle::<AccessorEvent>::new(EmptyPayload {}));

    let empty_error = block_on(none_event.inner.event_results_list(EventResultsOptions {
        raise_if_any: false,
        raise_if_none: true,
        timeout: None,
    }))
    .expect_err("raise_if_none should reject empty filtered results");
    assert!(empty_error.contains("Expected at least one handler"));

    let empty_values = block_on(none_event.inner.event_results_list(EventResultsOptions {
        raise_if_any: false,
        raise_if_none: false,
        timeout: None,
    }))
    .expect("raise_if_none false should allow empty filtered results");
    assert!(empty_values.is_empty());
    none_bus.stop();
}

#[test]
fn test_eventresultslist_supports_timeout_include_raise_if_any_raise_if_none_arguments() {
    test_eventresultslist_supports_include_raise_if_any_raise_if_none_arguments();

    let include_bus = EventBus::new(Some("EventResultsListIncludeBus".to_string()));
    include_bus.on_raw("IncludeEvent", "keep_handler", |_event| async move {
        Ok(json!("keep"))
    });
    include_bus.on_raw("IncludeEvent", "drop_handler", |_event| async move {
        Ok(json!("drop"))
    });
    let include_event =
        include_bus.emit_base(BaseEvent::new("IncludeEvent", serde_json::Map::new()));
    let filtered_values = block_on(include_event.event_results_list_with_filter(
        EventResultsOptions {
            raise_if_any: false,
            raise_if_none: true,
            timeout: None,
        },
        |result| result.result.as_ref() == Some(&json!("keep")),
    ))
    .expect("filtered values");
    assert_eq!(filtered_values, vec![json!("keep")]);
    include_bus.stop();

    let timeout_bus = EventBus::new(Some("EventResultsListTimeoutBus".to_string()));
    timeout_bus.on_raw("TimeoutEvent", "slow_handler", |_event| async move {
        thread::sleep(Duration::from_millis(50));
        Ok(json!("late"))
    });
    let timeout_event =
        timeout_bus.emit_base(BaseEvent::new("TimeoutEvent", serde_json::Map::new()));
    let timeout_error = block_on(timeout_event.event_results_list(EventResultsOptions {
        raise_if_any: false,
        raise_if_none: false,
        timeout: Some(0.01),
    }))
    .expect_err("timeout should reject before the slow handler completes");
    assert!(
        timeout_error.contains("Timed out waiting"),
        "{timeout_error}"
    );
    block_on(timeout_event.event_completed());
    timeout_bus.stop();
}
