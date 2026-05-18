use abxbus::event;
use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard, OnceLock,
    },
    thread,
    time::Duration,
};

use abxbus::{
    base_event::{BaseEvent, EventResultOptions, EventWaitOptions},
    event_bus::EventBus,
    event_handler::{EventHandler, EventHandlerOptions, HandlerFuture},
    event_result::{EventResult, EventResultStatus},
    id::compute_handler_id,
    types::EventConcurrencyMode,
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

fn event_result_test_guard() -> MutexGuard<'static, ()> {
    static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("event result test lock")
}

fn load_json_schema_common_shapes_fixture() -> Value {
    let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../tests/fixtures/jsonschema_common_shapes.json");
    let fixture = std::fs::read_to_string(fixture_path).expect("json schema fixture");
    serde_json::from_str(&fixture).expect("json schema fixture json")
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct ScreenshotEventResult {
    screenshot_base64: Option<String>,
    error: Option<String>,
}

event! {
    struct ScreenshotEvent {
        event_result_type: ScreenshotEventResult,
        event_type: "ScreenshotEvent",
    }
}
event! {
    struct AccessorEvent {
        event_result_type: Value,
        event_type: "AccessorEvent",
    }
}
event! {
    struct StringResultEvent {
        event_result_type: String,
        event_type: "StringResultEvent",
    }
}
event! {
    struct IntEvent {
        event_result_type: i64,
        event_type: "IntEvent",
    }
}
event! {
    struct NormalEvent {
        event_result_type: Value,
        event_type: "NormalEvent",
    }
}
event! {
    struct ForwardingBaseEventResult {
        event_result_type: i64,
        event_type: "ForwardingBaseEventResult",
    }
}
fn schema_event(event_type: &str, schema: Option<Value>) -> Arc<BaseEvent> {
    let event = BaseEvent::new(event_type, serde_json::Map::new());
    event.inner.lock().event_result_type = schema;
    event
}

fn first_event_result_record(event: &Arc<BaseEvent>) -> EventResult {
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
fn test_no_args_event_result_accessors_use_default_options() {
    let _guard = event_result_test_guard();
    let bus = EventBus::new(Some("NoArgsEventResultAccessorsBus".to_string()));
    bus.on_raw("AccessorEvent", "first", |_event| async move {
        Ok(json!("first"))
    });
    bus.on_raw("AccessorEvent", "second", |_event| async move {
        Ok(json!("second"))
    });

    let event = block_on(
        bus.emit(AccessorEvent {
            ..Default::default()
        })
        .now(),
    )
    .expect("typed event should complete");

    let typed_first = block_on(event.event_result())
        .expect("typed first result")
        .expect("typed first result value");
    let typed_values = block_on(event.event_results_list()).expect("typed result list");
    let raw_first = block_on(event.event_result())
        .expect("raw first result")
        .expect("raw first result value");
    let raw_values = block_on(event.event_results_list()).expect("raw result list");

    assert_eq!(typed_first, json!("first"));
    assert_eq!(typed_values, vec![json!("first"), json!("second")]);
    assert_eq!(raw_first, json!("first"));
    assert_eq!(raw_values, vec![json!("first"), json!("second")]);

    bus.destroy();
}

#[test]
fn test_typed_result_schema_validates_handler_result() {
    let _guard = event_result_test_guard();
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

    let event = bus.emit(ScreenshotEvent {
        ..Default::default()
    });
    block_on(event.now()).expect("complete screenshot event");
    let result = block_on(event.event_result_with_options(EventResultOptions::default()))
        .expect("typed event_result")
        .expect("handler result");

    assert_eq!(
        result,
        ScreenshotEventResult {
            screenshot_base64: Some("fake_screenshot_data".to_string()),
            error: None,
        }
    );
    bus.destroy();
}

#[test]
fn test_builtin_result_schema_validates_handler_results() {
    let _guard = event_result_test_guard();
    let bus = EventBus::new(Some("builtin_test_bus".to_string()));

    bus.on_raw("StringResultEvent", "string_handler", |_event| async move {
        Ok(json!("42"))
    });
    bus.on_raw(
        "IntEvent",
        "int_handler",
        |_event| async move { Ok(json!(123)) },
    );

    let string_event = bus.emit(StringResultEvent {
        ..Default::default()
    });
    block_on(string_event.now()).expect("complete string event");
    let string_result =
        block_on(string_event.event_result_with_options(EventResultOptions::default()))
            .expect("string result")
            .expect("string handler result");
    assert_eq!(string_result, "42");

    let int_event = bus.emit(IntEvent {
        ..Default::default()
    });
    block_on(int_event.now()).expect("complete int event");
    let int_result = block_on(int_event.event_result_with_options(EventResultOptions::default()))
        .expect("int result")
        .expect("int handler result");
    assert_eq!(int_result, 123);
    bus.destroy();
}

#[test]
fn test_invalid_handler_result_marks_error_when_schema_is_defined() {
    let _guard = event_result_test_guard();
    let bus = EventBus::new(Some("failure_test_bus".to_string()));

    bus.on_raw("IntEvent", "bad_handler", |_event| async move {
        Ok(json!("not_a_number"))
    });

    let event = bus.emit(IntEvent {
        ..Default::default()
    });
    let executed = block_on(event.now()).expect("event execution should complete");
    let typed_result = block_on(executed.event_result_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: false,
        include: None,
    }))
    .expect("suppressed schema error");
    assert_eq!(typed_result, None);

    let stored_result = event
        ._inner_event()
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
    bus.destroy();
}

#[test]
fn test_no_schema_leaves_raw_handler_result_untouched() {
    let _guard = event_result_test_guard();
    let bus = EventBus::new(Some("normal_test_bus".to_string()));

    bus.on_raw("NormalEvent", "normal_handler", |_event| async move {
        Ok(json!({"raw": "data"}))
    });

    let event = bus.emit(NormalEvent {
        ..Default::default()
    });
    block_on(event.now()).expect("complete normal event");
    let result = block_on(event.event_result_with_options(EventResultOptions::default()))
        .expect("raw result")
        .expect("handler result");

    assert_eq!(result, json!({"raw": "data"}));
    assert_eq!(event.event_result_type, None);
    bus.destroy();
}

#[test]
fn test_typed_accessors_normalize_forwarded_event_results_to_none() {
    let _guard = event_result_test_guard();
    let bus = EventBus::new(Some("forwarded_result_normalization_bus".to_string()));

    bus.on_raw(
        "ForwardingBaseEventResult",
        "forward_handler",
        |_event| async move {
            Ok(BaseEvent::new("ForwardedEventFromHandler", serde_json::Map::new()).to_json_value())
        },
    );

    let event = bus.emit(ForwardingBaseEventResult {
        ..Default::default()
    });
    block_on(event.now()).expect("complete forwarded result event");

    let result = block_on(event.event_result_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: false,
        include: None,
    }))
    .expect("typed event_result");
    let results_list = block_on(event.event_results_list_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: false,
        include: None,
    }))
    .expect("typed event_results_list");
    assert_eq!(result, None);
    assert!(results_list.is_empty());

    let raw_results = event.event_results.read();
    assert!(raw_results.values().any(|result| {
        result.result.as_ref().is_some_and(|value| {
            value.get("event_type") == Some(&json!("ForwardedEventFromHandler"))
                && value.get("event_id").is_some()
        })
    }));
    bus.destroy();
}

#[test]
fn test_event_result_defaults() {
    let _guard = event_result_test_guard();
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
    let _guard = event_result_test_guard();
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
    let _guard = event_result_test_guard();
    let bus = EventBus::new(Some("ResultCaptureBus".to_string()));
    bus.on_raw("StringResultEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });

    let event = bus.emit(StringResultEvent {
        ..Default::default()
    });
    let _ = block_on(event.now());

    let results = event.event_results.read();
    assert_eq!(results.len(), 1);
    let result = results.values().next().expect("result");
    assert_eq!(result.status, EventResultStatus::Completed);
    assert_eq!(result.result, Some(json!("ok")));
    bus.destroy();
}

#[test]
fn test_event_result_type_validates_handler_results() {
    let _guard = event_result_test_guard();
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
    let _ = block_on(event.wait());

    let result = first_event_result_record(&event);
    assert_eq!(result.status, EventResultStatus::Completed);
    assert_eq!(result.result, Some(json!({"value": "hello", "count": 2})));
    bus.destroy();
}

#[test]
fn test_event_result_type_allows_undefined_handler_return_values() {
    let _guard = event_result_test_guard();
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
    let _ = block_on(event.wait());

    let result = first_event_result_record(&event);
    assert_eq!(result.status, EventResultStatus::Completed);
    assert_eq!(result.result, Some(Value::Null));
    bus.destroy();
}

#[test]
fn test_invalid_result_marks_handler_error() {
    let _guard = event_result_test_guard();
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
    let _ = block_on(event.wait());

    let result = first_event_result_record(&event);
    assert_eq!(result.status, EventResultStatus::Error);
    assert!(result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("EventHandlerResultSchemaError"));
    assert!(!event.event_errors().is_empty());
    bus.destroy();
}

#[test]
fn test_event_with_no_result_schema_stores_raw_values() {
    let _guard = event_result_test_guard();
    let bus = EventBus::new(Some("NoSchemaBus".to_string()));

    bus.on_raw("NoResultSchemaEvent", "handler", |_event| async move {
        Ok(json!({"raw": true}))
    });

    let event = bus.emit_base(schema_event("NoResultSchemaEvent", None));
    let _ = block_on(event.wait());

    let result = first_event_result_record(&event);
    assert_eq!(result.status, EventResultStatus::Completed);
    assert_eq!(result.result, Some(json!({"raw": true})));
    bus.destroy();
}

#[test]
fn test_event_result_json_omits_result_type_and_derives_from_parent_event() {
    let _guard = event_result_test_guard();
    let bus = EventBus::new(Some("ResultTypeDeriveBus".to_string()));
    bus.on_raw("StringResultEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });

    let event = bus.emit(StringResultEvent {
        ..Default::default()
    });
    let _ = block_on(event.now());

    let result = event
        ._inner_event()
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
    assert_eq!(event.event_result_type, Some(json!({"type": "string"})));
    bus.destroy();
}

#[test]
fn test_eventhandler_json_roundtrips_handler_metadata() {
    let _guard = event_result_test_guard();
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
    let _guard = event_result_test_guard();
    let expected_seed = "018f8e40-1234-7000-8000-000000001234|pkg.module.handler|~/project/app.py:123|2025-01-02T03:04:05.678901000Z|StandaloneEvent";
    let expected_id = "19ea9fe8-cfbe-541e-8a35-2579e4e9efff";

    let eventbus_id = "018f8e40-1234-7000-8000-000000001234";
    let handler_name = "pkg.module.handler";
    let handler_file_path = "~/project/app.py:123";
    let handler_registered_at = "2025-01-02T03:04:05.678901000Z";
    let event_pattern = "StandaloneEvent";
    let actual_seed = format!(
        "{eventbus_id}|{handler_name}|{}|{handler_registered_at}|{event_pattern}",
        handler_file_path
    );
    let computed_id = compute_handler_id(
        eventbus_id,
        handler_name,
        Some(handler_file_path),
        handler_registered_at,
        event_pattern,
    );

    assert_eq!(actual_seed, expected_seed);
    assert_eq!(computed_id, expected_id);
}

#[test]
fn test_eventhandler_fromcallable_supports_id_override_and_detect_handler_file_path_toggle() {
    let _guard = event_result_test_guard();
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
    let _guard = event_result_test_guard();
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
    let _guard = event_result_test_guard();
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
    let _guard = event_result_test_guard();
    let bus = EventBus::new_with_options(
        Some("RunHandlerLockWaitBus".to_string()),
        abxbus::event_bus::EventBusOptions {
            event_handler_concurrency: abxbus::types::EventHandlerConcurrencyMode::Serial,
            ..abxbus::event_bus::EventBusOptions::default()
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
    let _ = block_on(event.wait());
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
    bus.destroy();
}

#[test]
fn test_slow_handler_warning_is_based_on_handler_runtime_after_lock_wait() {
    let _guard = event_result_test_guard();
    let bus = EventBus::new_with_options(
        Some("RunHandlerSlowAfterLockWaitBus".to_string()),
        abxbus::event_bus::EventBusOptions {
            event_handler_concurrency: abxbus::types::EventHandlerConcurrencyMode::Serial,
            event_handler_slow_timeout: Some(0.01),
            ..abxbus::event_bus::EventBusOptions::default()
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
    let _ = block_on(event.wait());
    assert_eq!(second_status(), EventResultStatus::Completed);
    bus.destroy();
}

#[test]
fn test_event_result_error_json_roundtrip_preserves_error_type_and_message() {
    let _guard = event_result_test_guard();
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
    let _guard = event_result_test_guard();
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

    let event = bus.emit(AccessorEvent {
        ..Default::default()
    });
    block_on(event.now()).expect("complete accessor event");

    let default_values = block_on(event.event_results_list_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: true,
        include: None,
    }))
    .expect("default values");
    assert_eq!(default_values, vec![json!("first"), json!("second")]);

    let raw_values = block_on(event.event_results_list_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: true,
        include: Some(Arc::new(|_result, event_result| {
            event_result.status == EventResultStatus::Completed && event_result.error.is_none()
        })),
    }))
    .expect("raw values");
    assert_eq!(
        raw_values,
        vec![json!("first"), Value::Null, json!("second")]
    );

    bus.destroy();
}

#[test]
fn test_event_result_and_results_list_use_registration_order_for_current_result_subset() {
    let _guard = event_result_test_guard();
    let bus = EventBus::new_with_options(
        Some("EventResultFirstValueBus".to_string()),
        abxbus::event_bus::EventBusOptions {
            event_handler_concurrency: abxbus::types::EventHandlerConcurrencyMode::Parallel,
            ..abxbus::event_bus::EventBusOptions::default()
        },
    );
    let completed_order = Arc::new(Mutex::new(Vec::<String>::new()));
    let null_order = completed_order.clone();
    bus.on_raw_sync_with_options(
        "AccessorEvent",
        "null_handler",
        EventHandlerOptions {
            id: Some("00000000-0000-5000-8000-00000000000b".to_string()),
            handler_registered_at: Some("2026-01-01T00:00:00.001Z".to_string()),
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
            handler_registered_at: Some("2026-01-01T00:00:00.002Z".to_string()),
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
            handler_registered_at: Some("2026-01-01T00:00:00.003Z".to_string()),
            ..EventHandlerOptions::default()
        },
        move |_event| {
            late_order.lock().unwrap().push("late".to_string());
            Ok(json!("late"))
        },
    );

    let event = bus.emit(AccessorEvent {
        ..Default::default()
    });
    block_on(event.now()).expect("complete accessor event");
    let first_value = block_on(event.event_result_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: true,
        include: None,
    }))
    .expect("first result");

    assert_eq!(first_value, Some(json!("winner")));
    let _ = block_on(event.wait());
    block_on(bus.wait_until_idle(Some(2.0)));
    for _ in 0..100 {
        if event.event_results.read().len() >= 3 {
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    let raw_values = block_on(event.event_results_list_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: false,
        include: Some(Arc::new(|_result, _event_result| true)),
    }))
    .expect("raw values");
    assert_eq!(
        raw_values,
        vec![Value::Null, json!("winner"), json!("late")]
    );
    assert_eq!(
        *completed_order.lock().unwrap(),
        vec!["late".to_string(), "winner".to_string(), "null".to_string()]
    );
    bus.destroy();
}

#[test]
fn test_base_event_from_json_preserves_event_results_object_registration_order() {
    let _guard = event_result_test_guard();
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

    let raw_values = block_on(event.event_results_list_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: false,
        include: Some(Arc::new(|_result, _event_result| true)),
    }))
    .expect("raw values");
    assert_eq!(
        raw_values,
        vec![json!("late"), Value::Null, json!("winner")]
    );

    let filtered_values = block_on(event.event_results_list_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: true,
        include: None,
    }))
    .expect("filtered values");
    assert_eq!(filtered_values, vec![json!("late"), json!("winner")]);

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
            late_id.to_string(),
            null_id.to_string(),
            winner_id.to_string()
        ]
    );
}

#[test]
fn test_eventresultslist_supports_include_raise_if_any_raise_if_none_arguments() {
    let _guard = event_result_test_guard();
    eventresultslist_supports_include_raise_if_any_raise_if_none_arguments();
}

fn eventresultslist_supports_include_raise_if_any_raise_if_none_arguments() {
    let error_bus = EventBus::new(Some("EventResultsListErrorsBus".to_string()));
    error_bus.on_raw("AccessorEvent", "failing_handler", |_event| async move {
        Err("boom".to_string())
    });
    error_bus.on_raw("AccessorEvent", "working_handler", |_event| async move {
        Ok(json!("ok"))
    });

    let error_event = error_bus.emit(AccessorEvent {
        ..Default::default()
    });
    block_on(error_event.now()).expect("error event completed");

    let raised =
        block_on(error_event.event_results_list_with_options(EventResultOptions::default()))
            .expect_err("raise_if_any should surface handler errors");
    assert!(raised.contains("boom"));

    let suppressed_event = error_bus.emit(AccessorEvent {
        ..Default::default()
    });
    block_on(suppressed_event.now()).expect("suppressed event completed");
    let suppressed = block_on(suppressed_event.event_results_list_with_options(
        EventResultOptions {
            raise_if_any: false,
            raise_if_none: true,
            include: None,
        },
    ))
    .expect("raise_if_any false should return successful values");
    assert_eq!(suppressed, vec![json!("ok")]);
    error_bus.destroy();

    let none_bus = EventBus::new(Some("EventResultsListNoneBus".to_string()));
    none_bus.on_raw("AccessorEvent", "null_handler", |_event| async move {
        Ok(Value::Null)
    });
    let none_event = none_bus.emit(AccessorEvent {
        ..Default::default()
    });
    block_on(none_event.now()).expect("none event completed");

    let empty_error = block_on(
        none_event.event_results_list_with_options(EventResultOptions {
            raise_if_any: false,
            raise_if_none: true,
            include: None,
        }),
    )
    .expect_err("raise_if_none should reject empty filtered results");
    assert!(empty_error.contains("Expected at least one handler"));

    let empty_values = block_on(
        none_event.event_results_list_with_options(EventResultOptions {
            raise_if_any: false,
            raise_if_none: false,
            include: None,
        }),
    )
    .expect("raise_if_none false should allow empty filtered results");
    assert!(empty_values.is_empty());
    none_bus.destroy();
}

#[test]
fn test_event_result_error_shapes_use_single_exception_or_group() {
    let _guard = event_result_test_guard();
    let bus = EventBus::new(Some("EventResultErrorShapeBus".to_string()));
    bus.on_raw("SingleErrorEvent", "single", |_event| async move {
        Err("single shape failure".to_string())
    });
    bus.on_raw("MultiErrorEvent", "first", |_event| async move {
        Err("first shape failure".to_string())
    });
    bus.on_raw("MultiErrorEvent", "second", |_event| async move {
        Err("second shape failure".to_string())
    });

    let single = bus.emit_base(BaseEvent::new("SingleErrorEvent", serde_json::Map::new()));
    block_on(single.now()).expect("single error event should complete");
    let single_error = block_on(single.event_result()).expect_err("single error should raise");
    assert_eq!(single_error, "single shape failure");

    let multi = bus.emit_base(BaseEvent::new("MultiErrorEvent", serde_json::Map::new()));
    block_on(multi.now()).expect("multi error event should complete");
    let multi_error = block_on(multi.event_result()).expect_err("multi error should raise");
    assert!(
        multi_error.contains("Event MultiErrorEvent#"),
        "{multi_error}"
    );
    assert!(
        multi_error.contains("had 2 handler error(s)"),
        "{multi_error}"
    );
    assert!(multi_error.contains("first shape failure"), "{multi_error}");
    assert!(
        multi_error.contains("second shape failure"),
        "{multi_error}"
    );

    bus.destroy();
}

#[test]
fn test_event_result_all_error_options_contract() {
    let _guard = event_result_test_guard();
    let bus = EventBus::new(Some("AllErrorResultOptionsBus".to_string()));
    bus.on_raw("AccessorEvent", "first_handler", |_event| async move {
        Err("first failure".to_string())
    });
    bus.on_raw("AccessorEvent", "second_handler", |_event| async move {
        Err("second failure".to_string())
    });

    let event = bus.emit(AccessorEvent {
        ..Default::default()
    });
    block_on(event.now()).expect("event completed");

    let default_error = block_on(event.event_result_with_options(EventResultOptions::default()))
        .expect_err("default event_result should surface handler errors");
    assert!(default_error.contains("first failure"), "{default_error}");
    assert!(default_error.contains("second failure"), "{default_error}");
    let default_list_error =
        block_on(event.event_results_list_with_options(EventResultOptions::default()))
            .expect_err("default event_results_list should surface handler errors");
    assert!(
        default_list_error.contains("first failure"),
        "{default_list_error}"
    );
    assert!(
        default_list_error.contains("second failure"),
        "{default_list_error}"
    );

    let value = block_on(event.event_result_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: false,
        include: None,
    }))
    .expect("false/false event_result should not raise");
    assert_eq!(value, None);
    let values = block_on(event.event_results_list_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: false,
        include: None,
    }))
    .expect("false/false event_results_list should not raise");
    assert!(values.is_empty());

    let none_error = block_on(event.event_result_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: true,
        include: None,
    }))
    .expect_err("false/true event_result should raise no-result error");
    assert!(
        none_error.contains("Expected at least one handler"),
        "{none_error}"
    );
    let none_list_error = block_on(event.event_results_list_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: true,
        include: None,
    }))
    .expect_err("false/true event_results_list should raise no-result error");
    assert!(
        none_list_error.contains("Expected at least one handler"),
        "{none_list_error}"
    );

    for options in [
        EventResultOptions {
            raise_if_any: true,
            raise_if_none: false,
            include: None,
        },
        EventResultOptions {
            raise_if_any: true,
            raise_if_none: true,
            include: None,
        },
    ] {
        let error = block_on(event.event_result_with_options(options.clone()))
            .expect_err("raise_if_any=true event_result should surface handler errors");
        assert!(error.contains("first failure"), "{error}");
        let list_error = block_on(event.event_results_list_with_options(options))
            .expect_err("raise_if_any=true event_results_list should surface handler errors");
        assert!(list_error.contains("first failure"), "{list_error}");
    }

    bus.destroy();
}

#[test]
fn test_event_result_default_options_contract() {
    let _guard = event_result_test_guard();
    let error_bus = EventBus::new(Some("EventResultDefaultErrorOptionsBus".to_string()));
    error_bus.on_raw("DefaultErrorOptionsEvent", "boom", |_event| async move {
        Err("default boom".to_string())
    });

    let error_event = error_bus.emit_base(BaseEvent::new(
        "DefaultErrorOptionsEvent",
        serde_json::Map::new(),
    ));
    block_on(error_event.now()).expect("error event should complete");

    let error =
        block_on(error_event.event_result()).expect_err("default event_result should raise");
    assert_eq!(error, "default boom");
    let list_error = block_on(error_event.event_results_list())
        .expect_err("default event_results_list should raise");
    assert_eq!(list_error, "default boom");

    let suppressed = block_on(error_event.event_result_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: false,
        include: None,
    }))
    .expect("explicit raise_if_any=false should suppress handler errors");
    assert_eq!(suppressed, None);
    let suppressed_list = block_on(error_event.event_results_list_with_options(
        EventResultOptions {
            raise_if_any: false,
            raise_if_none: false,
            include: None,
        },
    ))
    .expect("explicit raise_if_any=false should suppress list handler errors");
    assert!(suppressed_list.is_empty());
    error_bus.destroy();

    let empty_bus = EventBus::new(Some("EventResultDefaultNoneOptionsBus".to_string()));
    let empty_event = empty_bus.emit_base(BaseEvent::new(
        "DefaultNoneOptionsEvent",
        serde_json::Map::new(),
    ));
    block_on(empty_event.now()).expect("empty event should complete");

    let value = block_on(empty_event.event_result())
        .expect("default raise_if_none=false should not reject empty result");
    assert_eq!(value, None);
    let values = block_on(empty_event.event_results_list())
        .expect("default raise_if_none=false should not reject empty result list");
    assert!(values.is_empty());

    let none_error = block_on(empty_event.event_result_with_options(EventResultOptions {
        raise_if_any: true,
        raise_if_none: true,
        include: None,
    }))
    .expect_err("raise_if_none=true should reject empty results");
    assert!(
        none_error.contains("Expected at least one handler"),
        "{none_error}"
    );
    let none_list_error = block_on(empty_event.event_results_list_with_options(
        EventResultOptions {
            raise_if_any: true,
            raise_if_none: true,
            include: None,
        },
    ))
    .expect_err("raise_if_none=true should reject empty result list");
    assert!(
        none_list_error.contains("Expected at least one handler"),
        "{none_list_error}"
    );
    empty_bus.destroy();
}

#[test]
fn test_eventresultslist_supports_timeout_include_raise_if_any_raise_if_none_arguments() {
    let _guard = event_result_test_guard();
    eventresultslist_supports_include_raise_if_any_raise_if_none_arguments();

    let include_bus = EventBus::new(Some("EventResultsListIncludeBus".to_string()));
    include_bus.on_raw("IncludeEvent", "keep_handler", |_event| async move {
        Ok(json!("keep"))
    });
    include_bus.on_raw("IncludeEvent", "drop_handler", |_event| async move {
        Ok(json!("drop"))
    });
    let include_event =
        include_bus.emit_base(BaseEvent::new("IncludeEvent", serde_json::Map::new()));
    block_on(include_event.now()).expect("include event should complete");
    let filtered_values = block_on(include_event.event_results_list_with_options(
        EventResultOptions {
            raise_if_any: false,
            raise_if_none: true,
            include: Some(Arc::new(|result, _event_result| {
                result == Some(&json!("keep"))
            })),
        },
    ))
    .expect("filtered values");
    assert_eq!(filtered_values, vec![json!("keep")]);
    include_bus.destroy();

    let timeout_bus = EventBus::new(Some("EventResultsListTimeoutBus".to_string()));
    timeout_bus.on_raw("TimeoutEvent", "slow_handler", |_event| async move {
        thread::sleep(Duration::from_millis(50));
        Ok(json!("late"))
    });
    let timeout_target = BaseEvent::new("TimeoutEvent", serde_json::Map::new());
    timeout_target.inner.lock().event_concurrency = Some(EventConcurrencyMode::Parallel);
    let timeout_event = timeout_bus.emit_base(timeout_target);
    let timeout_error = match block_on(timeout_event.wait_with_options(EventWaitOptions {
        timeout: Some(0.01),
        first_result: false,
    })) {
        Ok(_) => panic!("timeout should reject before the slow handler completes"),
        Err(error) => error,
    };
    assert!(
        timeout_error.contains("Timed out waiting"),
        "{timeout_error}"
    );
    let _ = block_on(timeout_event.wait());
    timeout_bus.destroy();
}

// Folded from test_event_result_handler_metadata.rs to keep test layout class-based.
mod folded_test_event_result_handler_metadata {
    use std::sync::Arc;

    use abxbus::{
        base_event::BaseEvent,
        event_handler::{EventHandler, EventHandlerCallable, EventHandlerOptions},
        event_result::{EventResult, EventResultStatus},
        id::compute_handler_id,
    };
    use futures::executor::block_on;
    use serde_json::{json, Map, Value};

    fn standalone_event(data: &str) -> Arc<BaseEvent> {
        let mut payload = Map::new();
        payload.insert("data".to_string(), json!(data));
        BaseEvent::new("StandaloneEvent", payload)
    }

    fn data_handler() -> EventHandlerCallable {
        Arc::new(|event| {
            Box::pin(async move {
                let value = event
                    .inner
                    .lock()
                    .payload
                    .get("data")
                    .cloned()
                    .unwrap_or(Value::Null);
                Ok(value)
            })
        })
    }

    fn uppercase_handler() -> EventHandlerCallable {
        Arc::new(|event| {
            Box::pin(async move {
                let value = event
                    .inner
                    .lock()
                    .payload
                    .get("data")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_uppercase();
                Ok(json!(value))
            })
        })
    }

    fn handler_entry(handler_name: &str, callable: EventHandlerCallable) -> EventHandler {
        EventHandler::from_callable_with_options(
            "StandaloneEvent".to_string(),
            handler_name.to_string(),
            "StandaloneBus".to_string(),
            "018f8e40-1234-7000-8000-000000001234".to_string(),
            callable,
            EventHandlerOptions {
                detect_handler_file_path: Some(false),
                handler_registered_at: Some("2025-01-02T03:04:05.678901000Z".to_string()),
                ..EventHandlerOptions::default()
            },
        )
    }

    #[test]
    fn test_event_result_run_handler_with_base_event() {
        let event = standalone_event("ok");
        let handler = handler_entry("handler", data_handler());
        let event_id = event.inner.lock().event_id.clone();
        let mut event_result = EventResult::new(event_id, handler, None);

        let result_value =
            block_on(event_result.run_handler(event, None)).expect("handler should complete");

        assert_eq!(result_value, json!("ok"));
        assert_eq!(event_result.status, EventResultStatus::Completed);
        assert_eq!(event_result.result, Some(json!("ok")));
    }

    #[test]
    fn test_event_and_result_without_eventbus() {
        let event = standalone_event("message");
        let handler = handler_entry("handler", uppercase_handler());
        let handler_id = handler.id.clone();
        let event_id = event.inner.lock().event_id.clone();
        let mut event_result = EventResult::new(event_id, handler, None);

        let value = block_on(event_result.run_handler(event.clone(), None))
            .expect("handler should complete");
        event
            .inner
            .lock()
            .event_results
            .insert(handler_id.clone(), event_result.clone());

        assert_eq!(value, json!("MESSAGE"));
        assert_eq!(event_result.status, EventResultStatus::Completed);
        assert_eq!(
            event.inner.lock().event_results[&handler_id].status,
            EventResultStatus::Completed
        );
        assert_eq!(
            event.inner.lock().event_results[&handler_id].result,
            Some(json!("MESSAGE"))
        );
    }

    #[test]
    fn test_event_handler_model_is_serializable() {
        let entry = handler_entry("pkg.module.handler", data_handler());

        let dumped = entry.to_json_value();
        assert_eq!(dumped["event_pattern"], "StandaloneEvent");
        assert_eq!(dumped["eventbus_name"], "StandaloneBus");
        assert!(dumped.get("callable").is_none());

        let loaded = EventHandler::from_json_value(dumped);
        assert_eq!(loaded.id, entry.id);
        assert_eq!(loaded.event_pattern, entry.event_pattern);
        assert!(loaded.callable.is_none());
    }

    #[test]
    fn test_event_handler_id_matches_typescript_uuidv5_algorithm() {
        let expected_seed = "018f8e40-1234-7000-8000-000000001234|pkg.module.handler|~/project/app.py:123|2025-01-02T03:04:05.678901000Z|StandaloneEvent";
        let expected_id = "19ea9fe8-cfbe-541e-8a35-2579e4e9efff";

        let computed_id = compute_handler_id(
            "018f8e40-1234-7000-8000-000000001234",
            "pkg.module.handler",
            Some("~/project/app.py:123"),
            "2025-01-02T03:04:05.678901000Z",
            "StandaloneEvent",
        );
        let actual_seed = format!(
            "{}|{}|{}|{}|{}",
            "018f8e40-1234-7000-8000-000000001234",
            "pkg.module.handler",
            "~/project/app.py:123",
            "2025-01-02T03:04:05.678901000Z",
            "StandaloneEvent"
        );

        assert_eq!(actual_seed, expected_seed);
        assert_eq!(computed_id, expected_id);
    }

    #[test]
    fn test_event_handler_model_detects_handler_file_path() {
        let entry = EventHandler::from_callable(
            "StandaloneEvent".to_string(),
            "handler".to_string(),
            "StandaloneBus".to_string(),
            "018f8e40-1234-7000-8000-000000001234".to_string(),
            data_handler(),
        );

        let handler_file_path = entry
            .handler_file_path
            .as_deref()
            .expect("handler file path should be detected");
        assert!(
            handler_file_path.contains("test_event_result.rs"),
            "unexpected handler file path: {handler_file_path}"
        );
    }

    #[test]
    fn test_event_handler_from_callable_supports_id_override_and_detect_file_path_toggle() {
        let explicit_id = "018f8e40-1234-7000-8000-000000009999";

        let explicit = EventHandler::from_callable_with_options(
            "StandaloneEvent".to_string(),
            "handler".to_string(),
            "StandaloneBus".to_string(),
            "018f8e40-1234-7000-8000-000000001234".to_string(),
            data_handler(),
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
            data_handler(),
            EventHandlerOptions {
                detect_handler_file_path: Some(false),
                ..EventHandlerOptions::default()
            },
        );
        assert_eq!(no_detect.handler_file_path, None);
    }

    #[test]
    fn test_event_result_update_keeps_consistent_ordering_semantics_for_status_result_error() {
        let handler = handler_entry("handler", data_handler());
        let mut event_result = EventResult::new(
            "018f8e40-1234-7000-8000-000000009998".to_string(),
            handler,
            None,
        );

        event_result.error = Some("RuntimeError: existing".to_string());
        event_result.update(Some(EventResultStatus::Completed), None, None);
        assert_eq!(event_result.status, EventResultStatus::Completed);
        assert_eq!(
            event_result.error.as_deref(),
            Some("RuntimeError: existing")
        );

        event_result.update(
            Some(EventResultStatus::Error),
            Some(Some(json!("seeded"))),
            None,
        );
        assert_eq!(event_result.result, Some(json!("seeded")));
        assert_eq!(event_result.status, EventResultStatus::Error);
    }

    #[test]
    fn test_construct_pending_handler_result_matches_constructor() {
        let handler = handler_entry("handler", data_handler());
        let event_id = "018f8e40-1234-7000-8000-000000009997".to_string();
        let fast_result = EventResult::construct_pending_handler_result(
            event_id.clone(),
            handler.clone(),
            EventResultStatus::Pending,
            Some(1.25),
        );
        let mut validated_result = EventResult::new(event_id, handler, Some(1.25));
        validated_result.id = fast_result.id.clone();

        assert_eq!(
            serde_json::to_value(&fast_result).expect("fast result json"),
            serde_json::to_value(&validated_result).expect("validated result json")
        );
        assert_eq!(fast_result.handler.id, validated_result.handler.id);
        assert_eq!(fast_result.status, EventResultStatus::Pending);
    }

    #[test]
    fn test_event_result_serializes_handler_metadata_and_derived_fields() {
        let entry = handler_entry("handler", data_handler());
        let result = EventResult::new(
            "018f8e40-1234-7000-8000-000000009996".to_string(),
            entry.clone(),
            None,
        );
        let payload = serde_json::to_value(&result).expect("event result json");

        assert!(payload.get("handler").is_none());
        assert!(payload.get("result_type").is_none());
        assert_eq!(payload["handler_id"], entry.id);
        assert_eq!(payload["handler_name"], entry.handler_name);
        assert_eq!(payload["handler_event_pattern"], entry.event_pattern);
        assert_eq!(payload["eventbus_id"], entry.eventbus_id);
        assert_eq!(payload["eventbus_name"], entry.eventbus_name);
    }
}

// Folded from test_event_result_typed_results.rs to keep test layout class-based.
mod folded_test_event_result_typed_results {
    use std::sync::Arc;

    use abxbus::event;
    use abxbus::{
        base_event::BaseEvent,
        event_bus::EventBus,
        event_result::{EventResult, EventResultStatus},
        jsonschema::{normalize_json_schema, validate_json_schema_value},
    };
    use futures::executor::block_on;
    use serde::{Deserialize, Serialize};
    use serde_json::{json, Map, Value};

    fn schema_event(event_type: &str, schema: Option<Value>) -> Arc<BaseEvent> {
        let event = BaseEvent::new(event_type, Map::new());
        event.inner.lock().event_result_type = schema;
        event
    }

    fn first_event_result_record(event: &Arc<BaseEvent>) -> EventResult {
        event
            .inner
            .lock()
            .event_results
            .values()
            .next()
            .cloned()
            .expect("expected one event result")
    }

    fn assert_schema_roundtrips(schema: Value) {
        let normalized_schema = normalize_json_schema(schema);
        let original = schema_event("SchemaEvent", Some(normalized_schema.clone()));
        let restored = BaseEvent::from_json_value(original.to_json_value());
        assert_eq!(
            restored.inner.lock().event_result_type,
            Some(normalized_schema)
        );
    }

    fn wait(event: &Arc<BaseEvent>) {
        let _ = block_on(event.wait());
    }

    #[test]
    fn test_typed_result_schema_validates_and_parses_handler_result() {
        let bus = EventBus::new(Some("TypedResultBus".to_string()));
        let schema = json!({
            "type": "object",
            "properties": {
                "value": {"type": "string"},
                "count": {"type": "number"}
            },
            "required": ["value", "count"]
        });

        bus.on_raw("TypedResultEvent", "handler", |_event| async move {
            Ok(json!({"value": "hello", "count": 42}))
        });

        let event = bus.emit_base(schema_event("TypedResultEvent", Some(schema)));
        wait(&event);

        let result = first_event_result_record(&event);
        assert_eq!(
            result.status,
            abxbus::event_result::EventResultStatus::Completed
        );
        assert_eq!(result.result, Some(json!({"value": "hello", "count": 42})));
        bus.destroy();
    }

    #[test]
    fn test_result_type_stored_in_event_result() {
        let bus = EventBus::new(Some("storage_test_bus".to_string()));
        let schema = json!({"type": "string"});

        bus.on_raw("StringEvent", "handler", |_event| async move {
            Ok(json!("123"))
        });

        let event = bus.emit_base(schema_event("StringEvent", Some(schema.clone())));
        wait(&event);

        let result = first_event_result_record(&event);
        assert_eq!(result.status, EventResultStatus::Completed);
        assert_eq!(result.result_type_json(&event), Some(schema));
        assert!(
            !result
                .to_flat_json_value()
                .as_object()
                .expect("result json object")
                .contains_key("result_type"),
            "EventResult JSON must not duplicate the parent event result schema"
        );
        bus.destroy();
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct SimpleResult {
        value: String,
        count: i64,
    }

    #[test]
    fn test_simple_typed_result_model_roundtrip_and_status() {
        let bus = EventBus::new(Some("typed_result_simple_bus".to_string()));
        let schema = json!({
            "type": "object",
            "properties": {
                "value": {"type": "string"},
                "count": {"type": "integer"},
            },
            "required": ["value", "count"],
            "additionalProperties": false,
        });

        bus.on_raw("SimpleBaseEventResult", "handler", |_event| async move {
            Ok(json!({"value": "hello", "count": 42}))
        });

        let event = bus.emit_base(schema_event("SimpleBaseEventResult", Some(schema)));
        wait(&event);

        assert_eq!(
            event.inner.lock().event_status,
            abxbus::types::EventStatus::Completed
        );

        let result = first_event_result_record(&event);
        assert_eq!(result.status, EventResultStatus::Completed);
        assert!(result.error.is_none());
        assert_eq!(result.result, Some(json!({"value": "hello", "count": 42})));

        let typed_result: SimpleResult =
            serde_json::from_value(result.result.expect("handler result")).expect("typed result");
        assert_eq!(
            typed_result,
            SimpleResult {
                value: "hello".to_string(),
                count: 42
            }
        );
        bus.destroy();
    }

    event! {
        struct BuiltinStringEvent {
            event_result_type: String,
            event_type: "BuiltinStringEvent",
        }
    }

    event! {
        struct BuiltinIntEvent {
            event_result_type: i64,
            event_type: "BuiltinIntEvent",
        }
    }

    event! {
        struct BuiltinFloatEvent {
            event_result_type: f64,
            event_type: "BuiltinFloatEvent",
        }
    }

    event! {
        struct PlainSchemaEvent {
            event_result_type: Value,
            event_type: "PlainSchemaEvent",
        }
    }

    event! {
        struct NoneSchemaEvent {
            event_result_type: (),
            event_type: "NoneSchemaEvent",
        }
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct ModuleLevelResult {
        result_id: String,
        data: Map<String, Value>,
        success: bool,
    }

    event! {
        struct RuntimeSchemaEvent {
            event_result_type: ModuleLevelResult,
            event_type: "RuntimeSchemaEvent",
            event_result_schema: r#"{
            "type": "object",
            "properties": {
                "result_id": {"type": "string"},
                "data": {"type": "object"},
                "success": {"type": "boolean"}
            },
            "required": ["result_id", "data", "success"],
            "additionalProperties": false
        }"#,
        }
    }

    struct DictIntSchemaEvent;
    impl abxbus::typed::EventSpec for DictIntSchemaEvent {
        type payload = Map<String, Value>;
        type event_result_type = Map<String, Value>;
        const event_type: &'static str = "DictIntSchemaEvent";
        const event_result_type_schema: Option<&'static str> =
            Some(r#"{"type": "object", "additionalProperties": {"type": "integer"}}"#);
    }

    struct DictModuleSchemaEvent;
    impl abxbus::typed::EventSpec for DictModuleSchemaEvent {
        type payload = Map<String, Value>;
        type event_result_type = Map<String, Value>;
        const event_type: &'static str = "DictModuleSchemaEvent";
        const event_result_type_schema: Option<&'static str> = Some(
            r#"{
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "properties": {
                    "subject": {"type": "string"},
                    "body": {"type": "string"},
                    "recipients": {"type": "array", "items": {"type": "string"}}
                },
                "required": ["subject", "body", "recipients"],
                "additionalProperties": false
            }
        }"#,
        );
    }

    struct DictLocalSchemaEvent;
    impl abxbus::typed::EventSpec for DictLocalSchemaEvent {
        type payload = Map<String, Value>;
        type event_result_type = Map<String, Value>;
        const event_type: &'static str = "DictLocalSchemaEvent";
        const event_result_type_schema: Option<&'static str> = Some(
            r#"{
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "properties": {
                    "filename": {"type": "string"},
                    "content": {"type": "string", "contentEncoding": "base64"},
                    "mime_type": {"type": "string"}
                },
                "required": ["filename", "content", "mime_type"],
                "additionalProperties": false
            }
        }"#,
        );
    }

    struct SpecificUserEvent;
    impl abxbus::typed::EventSpec for SpecificUserEvent {
        type payload = Map<String, Value>;
        type event_result_type = ModuleLevelResult;
        const event_type: &'static str = "SpecificUserEvent";
        const event_result_type_schema: Option<&'static str> =
            <RuntimeSchemaEvent as abxbus::typed::EventSpec>::event_result_type_schema;
    }

    #[test]
    fn test_builtin_types_auto_extraction() {
        let bus = EventBus::new(Some("BuiltinResultSchemaBus".to_string()));
        let string_event = bus.emit(BuiltinStringEvent {
            ..Default::default()
        });
        let int_event = bus.emit(BuiltinIntEvent {
            ..Default::default()
        });
        let float_event = bus.emit(BuiltinFloatEvent {
            ..Default::default()
        });

        assert_eq!(
            string_event.event_result_type,
            Some(json!({"type": "string"}))
        );
        assert_eq!(
            int_event.event_result_type,
            Some(json!({"type": "integer"}))
        );
        assert_eq!(
            float_event.event_result_type,
            Some(json!({"type": "number"}))
        );
        bus.destroy();
    }

    #[test]
    fn test_no_generic_parameter() {
        let bus = EventBus::new(Some("PlainResultSchemaBus".to_string()));
        let plain_event = bus.emit(PlainSchemaEvent {
            ..Default::default()
        });

        assert_eq!(plain_event.event_result_type, None);
        bus.destroy();
    }

    #[test]
    fn test_none_generic_parameter() {
        let bus = EventBus::new(Some("NoneResultSchemaBus".to_string()));
        let none_event = bus.emit(NoneSchemaEvent {
            ..Default::default()
        });

        assert_eq!(none_event.event_result_type, None);
        bus.destroy();
    }

    #[test]
    fn test_eventspec_result_schema_runtime_enforcement() {
        let bus = EventBus::new(Some("runtime_test_bus".to_string()));

        bus.on_raw(
            "RuntimeSchemaEvent",
            "correct_handler",
            |_event| async move {
                Ok(json!({
                    "result_id": "e1bb315c-472f-7bd1-8e72-c8502e1a9a36",
                    "data": {"key": "value"},
                    "success": true
                }))
            },
        );

        let event = bus.emit(RuntimeSchemaEvent {
            ..Default::default()
        });
        wait(&event._inner_event());
        let result = first_event_result_record(&event._inner_event());
        assert_eq!(result.status, EventResultStatus::Completed);
        let typed: ModuleLevelResult =
            serde_json::from_value(result.result.expect("result")).expect("typed result");
        assert_eq!(typed.result_id, "e1bb315c-472f-7bd1-8e72-c8502e1a9a36");
        assert_eq!(typed.data.get("key"), Some(&json!("value")));
        assert!(typed.success);

        bus.off("RuntimeSchemaEvent", None);
        bus.on_raw(
            "RuntimeSchemaEvent",
            "incorrect_handler",
            |_event| async move { Ok(json!({"wrong": "format"})) },
        );

        let invalid_event = bus.emit(RuntimeSchemaEvent {
            ..Default::default()
        });
        wait(&invalid_event._inner_event());
        let invalid_result = first_event_result_record(&invalid_event._inner_event());
        assert_eq!(invalid_result.status, EventResultStatus::Error);
        assert!(invalid_result
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("EventHandlerResultSchemaError"));
        bus.destroy();
    }

    #[test]
    fn test_nested_inheritance() {
        assert_eq!(
            <SpecificUserEvent as abxbus::typed::EventSpec>::event_result_type_json(),
            <RuntimeSchemaEvent as abxbus::typed::EventSpec>::event_result_type_json()
        );
    }

    #[test]
    fn test_module_level_types_auto_extraction() {
        let schema = <RuntimeSchemaEvent as abxbus::typed::EventSpec>::event_result_type_json()
            .expect("module-level schema");
        assert_eq!(schema["type"], "object");
        assert!(schema["properties"].get("result_id").is_some());
        assert!(schema["properties"].get("data").is_some());
        assert!(schema["properties"].get("success").is_some());
    }

    #[test]
    fn test_complex_module_level_generics() {
        for schema in [
            json!({
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "result_id": {"type": "string"},
                        "data": {"type": "object"},
                        "success": {"type": "boolean"}
                    },
                    "required": ["result_id", "data", "success"]
                }
            }),
            json!({
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "properties": {
                        "items": {"type": "array", "items": {"type": "string"}},
                        "metadata": {"type": "object", "additionalProperties": {"type": "integer"}}
                    },
                    "required": ["items", "metadata"]
                }
            }),
        ] {
            assert_schema_roundtrips(schema);
        }
    }

    #[test]
    fn test_extract_basemodel_generic_arg_basic() {
        assert_eq!(
            <BuiltinIntEvent as abxbus::typed::EventSpec>::event_result_type_json(),
            Some(json!({"type": "integer"}))
        );
    }

    #[test]
    fn test_extract_basemodel_generic_arg_dict() {
        assert_eq!(
            <DictIntSchemaEvent as abxbus::typed::EventSpec>::event_result_type_json(),
            Some(json!({"type": "object", "additionalProperties": {"type": "integer"}}))
        );
    }

    #[test]
    fn test_extract_basemodel_generic_arg_dict_with_module_type() {
        let schema = <DictModuleSchemaEvent as abxbus::typed::EventSpec>::event_result_type_json()
            .expect("module dict schema");
        assert_eq!(schema["type"], "object");
        assert_eq!(
            schema["additionalProperties"]["properties"]["recipients"]["items"]["type"],
            "string"
        );
    }

    #[test]
    fn test_extract_basemodel_generic_arg_dict_with_local_type() {
        let schema = <DictLocalSchemaEvent as abxbus::typed::EventSpec>::event_result_type_json()
            .expect("local dict schema");
        assert_eq!(schema["type"], "object");
        assert_eq!(
            schema["additionalProperties"]["properties"]["mime_type"]["type"],
            "string"
        );
    }

    #[test]
    fn test_extract_basemodel_generic_arg_no_generic() {
        assert_eq!(
            <PlainSchemaEvent as abxbus::typed::EventSpec>::event_result_type_json(),
            None
        );
    }

    #[test]
    fn test_built_in_result_schemas_validate_handler_results() {
        let bus = EventBus::new(Some("BuiltinResultBus".to_string()));

        bus.on_raw("StringResultEvent", "string_handler", |_event| async move {
            Ok(json!("42"))
        });
        bus.on_raw("NumberResultEvent", "number_handler", |_event| async move {
            Ok(json!(123))
        });

        let string_event = bus.emit_base(schema_event(
            "StringResultEvent",
            Some(json!({"type": "string"})),
        ));
        let number_event = bus.emit_base(schema_event(
            "NumberResultEvent",
            Some(json!({"type": "number"})),
        ));
        wait(&string_event);
        wait(&number_event);

        let string_result = first_event_result_record(&string_event);
        let number_result = first_event_result_record(&number_event);
        assert_eq!(
            string_result.status,
            abxbus::event_result::EventResultStatus::Completed
        );
        assert_eq!(string_result.result, Some(json!("42")));
        assert_eq!(
            number_result.status,
            abxbus::event_result::EventResultStatus::Completed
        );
        assert_eq!(number_result.result, Some(json!(123)));
        bus.destroy();
    }

    #[test]
    fn test_event_result_type_supports_constructor_shorthands_and_enforces_them() {
        let bus = EventBus::new(Some("ConstructorResultTypeBus".to_string()));

        for (event_type, result) in [
            ("ConstructorStringResultEvent", json!("ok")),
            ("ConstructorNumberResultEvent", json!(123)),
            ("ConstructorBooleanResultEvent", json!(true)),
            ("ConstructorArrayResultEvent", json!([1, "two", false])),
            ("ConstructorObjectResultEvent", json!({"id": 1, "ok": true})),
        ] {
            bus.on_raw(event_type, "handler", move |_event| {
                let result = result.clone();
                async move { Ok(result) }
            });
        }

        let cases = [
            ("ConstructorStringResultEvent", json!({"type": "string"})),
            ("ConstructorNumberResultEvent", json!({"type": "number"})),
            ("ConstructorBooleanResultEvent", json!({"type": "boolean"})),
            ("ConstructorArrayResultEvent", json!({"type": "array"})),
            ("ConstructorObjectResultEvent", json!({"type": "object"})),
        ];
        for (event_type, schema) in cases {
            let event = bus.emit_base(schema_event(event_type, Some(schema)));
            wait(&event);
            assert_eq!(
                first_event_result_record(&event).status,
                abxbus::event_result::EventResultStatus::Completed
            );
        }

        bus.on_raw(
            "ConstructorNumberResultEventInvalid",
            "invalid_handler",
            |_event| async move { Ok(json!("not-a-number")) },
        );
        let invalid = bus.emit_base(schema_event(
            "ConstructorNumberResultEventInvalid",
            Some(json!({"type": "number"})),
        ));
        wait(&invalid);
        let invalid_result = first_event_result_record(&invalid);
        assert_eq!(
            invalid_result.status,
            abxbus::event_result::EventResultStatus::Error
        );
        assert!(invalid_result
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("EventHandlerResultSchemaError"));
        assert_eq!(invalid.event_errors().len(), 1);
        bus.destroy();
    }

    #[test]
    fn test_runtime_schema_rejects_invalid_handler_result() {
        let bus = EventBus::new(Some("ResultValidationErrorBus".to_string()));

        bus.on_raw("NumberResultEvent", "handler", |_event| async move {
            Ok(json!("not-a-number"))
        });

        let event = bus.emit_base(schema_event(
            "NumberResultEvent",
            Some(json!({"type": "number"})),
        ));
        wait(&event);

        let result = first_event_result_record(&event);
        assert_eq!(
            result.status,
            abxbus::event_result::EventResultStatus::Error
        );
        assert!(result
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("EventHandlerResultSchemaError"));
        assert!(!event.event_errors().is_empty());
        bus.destroy();
    }

    #[test]
    fn test_separate_no_schema_event_stores_raw_handler_result() {
        let bus = EventBus::new(Some("NoSchemaResultBus".to_string()));

        bus.on_raw("NoSchemaEvent", "handler", |_event| async move {
            Ok(json!({"raw": true}))
        });

        let event = bus.emit_base(schema_event("NoSchemaEvent", None));
        wait(&event);

        let result = first_event_result_record(&event);
        assert_eq!(
            result.status,
            abxbus::event_result::EventResultStatus::Completed
        );
        assert_eq!(result.result, Some(json!({"raw": true})));
        bus.destroy();
    }

    #[test]
    fn test_complex_result_schema_validates_nested_data() {
        let bus = EventBus::new(Some("ComplexResultBus".to_string()));
        let schema = json!({
            "type": "object",
            "properties": {
                "items": {"type": "array", "items": {"type": "string"}},
                "metadata": {"type": "object", "additionalProperties": {"type": "number"}}
            },
            "required": ["items", "metadata"]
        });

        bus.on_raw("ComplexResultEvent", "handler", |_event| async move {
            Ok(json!({"items": ["a", "b"], "metadata": {"a": 1, "b": 2}}))
        });

        let event = bus.emit_base(schema_event("ComplexResultEvent", Some(schema)));
        wait(&event);

        let result = first_event_result_record(&event);
        assert_eq!(
            result.status,
            abxbus::event_result::EventResultStatus::Completed
        );
        assert_eq!(
            result.result,
            Some(json!({"items": ["a", "b"], "metadata": {"a": 1, "b": 2}}))
        );
        bus.destroy();
    }

    #[test]
    fn test_from_json_converts_event_result_type_into_schema() {
        let bus = EventBus::new(Some("FromJsonResultBus".to_string()));
        let schema = json!({
            "type": "object",
            "properties": {
                "value": {"type": "string"},
                "count": {"type": "number"}
            },
            "required": ["value", "count"]
        });
        let restored = BaseEvent::from_json_value(
            schema_event("TypedResultEvent", Some(schema)).to_json_value(),
        );

        assert!(restored.inner.lock().event_result_type.is_some());

        bus.on_raw("TypedResultEvent", "handler", |_event| async move {
            Ok(json!({"value": "from-json", "count": 7}))
        });

        let dispatched = bus.emit_base(restored);
        wait(&dispatched);

        let result = first_event_result_record(&dispatched);
        assert_eq!(
            result.status,
            abxbus::event_result::EventResultStatus::Completed
        );
        assert_eq!(
            result.result,
            Some(json!({"value": "from-json", "count": 7}))
        );
        bus.destroy();
    }

    #[test]
    fn test_fromjson_deserializes_event_result_type_and_tojson_reserializes_schema() {
        let schema = json!({"type": "integer"});
        let normalized_schema = normalize_json_schema(schema.clone());
        let event = BaseEvent::from_json_value(json!({
            "event_id": "018f8e40-1234-7000-8000-000000001235",
            "event_created_at": "2025-01-01T00:00:01.000Z",
            "event_type": "RawSchemaEvent",
            "event_timeout": null,
            "event_result_type": schema,
        }));

        assert_eq!(
            event.inner.lock().event_result_type,
            Some(normalized_schema.clone())
        );
        assert_eq!(
            event.to_json_value()["event_result_type"],
            normalized_schema
        );
    }

    #[test]
    fn test_from_json_reconstructs_primitive_json_schema() {
        let bus = EventBus::new(Some("PrimitiveFromJsonBus".to_string()));
        let restored = BaseEvent::from_json_value(
            schema_event("PrimitiveResultEvent", Some(json!({"type": "boolean"}))).to_json_value(),
        );

        assert!(restored.inner.lock().event_result_type.is_some());

        bus.on_raw("PrimitiveResultEvent", "handler", |_event| async move {
            Ok(json!(true))
        });

        let dispatched = bus.emit_base(restored);
        wait(&dispatched);

        let result = first_event_result_record(&dispatched);
        assert_eq!(
            result.status,
            abxbus::event_result::EventResultStatus::Completed
        );
        assert_eq!(result.result, Some(json!(true)));
        bus.destroy();
    }

    #[test]
    fn test_fromjson_reconstructs_integer_and_null_schemas_for_runtime_validation() {
        let bus = EventBus::new(Some("SchemaPrimitiveRuntimeBus".to_string()));

        bus.on_raw("RawIntegerEvent", "int_handler", |_event| async move {
            Ok(json!(123))
        });
        let int_event = bus.emit_base(schema_event(
            "RawIntegerEvent",
            Some(json!({"type": "integer"})),
        ));
        wait(&int_event);
        assert_eq!(
            first_event_result_record(&int_event).status,
            EventResultStatus::Completed
        );

        bus.on_raw(
            "RawIntegerEventBad",
            "int_bad_handler",
            |_event| async move { Ok(json!(1.5)) },
        );
        let int_bad_event = bus.emit_base(schema_event(
            "RawIntegerEventBad",
            Some(json!({"type": "integer"})),
        ));
        wait(&int_bad_event);
        assert_eq!(
            first_event_result_record(&int_bad_event).status,
            EventResultStatus::Error
        );

        bus.on_raw("RawNullEvent", "null_handler", |_event| async move {
            Ok(Value::Null)
        });
        let null_event = bus.emit_base(schema_event("RawNullEvent", Some(json!({"type": "null"}))));
        wait(&null_event);
        assert_eq!(
            first_event_result_record(&null_event).status,
            EventResultStatus::Completed
        );
        bus.destroy();
    }

    #[test]
    fn test_json_schema_primitive_deserialization() {
        for schema in [
            json!({"type": "string"}),
            json!({"type": "number"}),
            json!({"type": "integer"}),
            json!({"type": "boolean"}),
            json!({"type": "null"}),
        ] {
            assert_schema_roundtrips(schema);
        }
    }

    #[test]
    fn test_json_schema_null_unions_normalize_to_standard_anyof() {
        let event = schema_event(
            "NullableSchemaEvent",
            Some(json!({"anyOf": [{"type": "string"}, {"type": "null"}]})),
        );
        let schema = event.to_json_value()["event_result_type"].clone();

        assert_eq!(
            schema["anyOf"],
            json!([{"type": "string"}, {"type": "null"}])
        );
        assert!(schema.get("nullable").is_none());
        assert!(schema.get("oneOf").is_none());
    }

    #[test]
    fn test_json_schema_type_null_union_validates_the_same_as_anyof_null_union() {
        let bus = EventBus::new(Some("StandardNullUnionSchemaBus".to_string()));
        bus.on_raw(
            "StandardNullUnionSchemaEvent",
            "handler",
            |_event| async move { Ok(json!("ok")) },
        );
        let event = bus.emit_base(schema_event(
            "StandardNullUnionSchemaEvent",
            Some(json!({"type": ["string", "null"]})),
        ));
        wait(&event);

        let result = first_event_result_record(&event);
        assert_eq!(result.status, EventResultStatus::Completed);
        assert_eq!(result.result, Some(json!("ok")));
        let bus_json = bus.to_json_value();
        let history = bus_json["event_history"]
            .as_object()
            .expect("event history");
        let event_json = history.values().next().expect("event history event");
        assert_eq!(
            event_json["event_result_type"]["anyOf"],
            json!([{"type": "string"}, {"type": "null"}])
        );
        assert!(event_json["event_result_type"].get("nullable").is_none());
        bus.destroy();
    }

    #[test]
    fn test_json_schema_oneof_semantics_survive_normalization() {
        let schema = normalize_json_schema(json!({"oneOf": [{}, {"type": "null"}]}));

        assert!(schema.get("oneOf").is_some());
        assert!(schema.get("anyOf").is_none());
        validate_json_schema_value(&schema, &schema, &json!("ok"), "$")
            .expect("oneOf non-null value should validate");
        assert!(
            validate_json_schema_value(&schema, &schema, &Value::Null, "$").is_err(),
            "oneOf null value should fail because it matches both branches"
        );
    }

    #[test]
    fn test_json_schema_allof_semantics_survive_rehydration() {
        let schema = json!({"allOf": [{"type": "string", "minLength": 2}, {"pattern": "^a"}]});

        validate_json_schema_value(&schema, &schema, &json!("ab"), "$")
            .expect("allOf valid value should validate");
        assert!(
            validate_json_schema_value(&schema, &schema, &json!("b"), "$").is_err(),
            "allOf should reject values that fail a branch"
        );
        assert!(
            validate_json_schema_value(&schema, &schema, &json!("a"), "$").is_err(),
            "allOf should reject values that fail sibling constraints"
        );
    }

    #[test]
    fn test_json_schema_null_enum_semantics_survive_rehydration() {
        let schema = json!({"enum": ["queued", null]});

        validate_json_schema_value(&schema, &schema, &json!("queued"), "$")
            .expect("enum string value should validate");
        validate_json_schema_value(&schema, &schema, &Value::Null, "$")
            .expect("enum null value should validate");
        assert!(
            validate_json_schema_value(&schema, &schema, &json!("done"), "$").is_err(),
            "enum should reject values outside the enum"
        );
    }

    #[test]
    fn test_json_schema_tuple_prefix_items_only_apply_items_to_remaining_values() {
        let schema = json!({
            "type": "array",
            "prefixItems": [{"type": "string"}, {"type": "integer"}],
            "items": {"type": "boolean"}
        });

        validate_json_schema_value(&schema, &schema, &json!(["ok", 1, true, false]), "$")
            .expect("tuple prefix plus remaining items should validate");
        assert!(
            validate_json_schema_value(&schema, &schema, &json!(["ok", 1, "not-boolean"]), "$")
                .is_err(),
            "items should validate only values after prefixItems"
        );
        assert!(
            validate_json_schema_value(&schema, &schema, &json!(["ok", "not-integer", true]), "$")
                .is_err(),
            "prefixItems should still validate tuple-prefix values"
        );
    }

    #[test]
    fn test_json_schema_object_without_properties_rejects_additional_properties() {
        let schema = json!({"type": "object", "additionalProperties": false});

        validate_json_schema_value(&schema, &schema, &json!({}), "$")
            .expect("empty object should validate");
        assert!(
            validate_json_schema_value(&schema, &schema, &json!({"extra": true}), "$").is_err(),
            "additionalProperties false should reject keys even without properties"
        );
    }

    #[test]
    fn test_json_schema_recursive_null_refs_serialize_without_infinite_expansion() {
        let schema = json!({
            "$defs": {
                "Node": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "child": {
                            "anyOf": [
                                {"$ref": "#/$defs/Node"},
                                {"type": "null"}
                            ]
                        }
                    },
                    "required": ["name"]
                }
            },
            "$ref": "#/$defs/Node"
        });
        let event = schema_event("RecursiveNullableSchemaEvent", Some(schema));
        let event_schema = event.to_json_value()["event_result_type"].clone();
        let child_schema = event_schema["properties"]["child"]
            .as_object()
            .expect("child schema");

        assert_eq!(
            child_schema.get("anyOf"),
            Some(&json!([{"$ref": "#"}, {"type": "null"}]))
        );
        assert_eq!(event_schema.get("title"), Some(&json!("Node")));
        assert!(child_schema.get("nullable").is_none());
        assert!(child_schema.get("allOf").is_none());
        assert!(child_schema.get("oneOf").is_none());

        let normalized_schema = normalize_json_schema(json!({
            "$defs": {
                "Node": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "child": {
                            "anyOf": [
                                {"$ref": "#/$defs/Node"},
                                {"type": "null"}
                            ]
                        }
                    },
                    "required": ["name"]
                }
            },
            "$ref": "#/$defs/Node"
        }));
        let normalized_child_schema = normalized_schema["properties"]["child"]
            .as_object()
            .expect("normalized child schema");
        assert_eq!(normalized_schema.get("title"), Some(&json!("Node")));
        assert!(normalized_schema.get("$defs").is_none());
        assert_eq!(
            normalized_child_schema.get("anyOf"),
            Some(&json!([{"$ref": "#"}, {"type": "null"}]))
        );
        assert!(normalized_child_schema.get("nullable").is_none());
        assert!(normalized_child_schema.get("allOf").is_none());
        assert!(normalized_child_schema.get("oneOf").is_none());
    }

    #[test]
    fn test_json_schema_common_shapes_normalize_as_stable_roundtrip_fixtures() {
        let fixture = crate::load_json_schema_common_shapes_fixture();
        let mut raw_fixtures = fixture["raw_schemas"]
            .as_array()
            .expect("raw schemas")
            .clone();
        let common_complex_schema = fixture["common_complex_schema"].clone();
        let common_complex_payload = fixture["common_complex_payload"].clone();
        let common_complex_invalid_payloads = fixture["common_complex_invalid_payloads"]
            .as_array()
            .expect("invalid payloads")
            .clone();
        raw_fixtures.push(common_complex_schema.clone());

        for schema in &raw_fixtures {
            let normalized = normalize_json_schema(schema.clone());
            assert_eq!(normalize_json_schema(normalized.clone()), normalized);
            assert_eq!(
                normalized.get("$schema"),
                Some(&json!("https://json-schema.org/draft/2020-12/schema"))
            );
        }

        let nullable_string = normalize_json_schema(raw_fixtures[0].clone());
        assert_eq!(
            nullable_string.get("anyOf"),
            Some(&json!([{"type": "string"}, {"type": "null"}]))
        );
        assert!(nullable_string.get("nullable").is_none());

        let recursive = normalize_json_schema(raw_fixtures[1].clone());
        assert!(recursive.get("$defs").is_none());
        assert_eq!(recursive.get("title"), Some(&json!("CommonNodeResult")));
        let recursive_child = &recursive["properties"]["child"];
        assert_eq!(
            recursive_child.get("anyOf"),
            Some(&json!([{"$ref": "#"}, {"type": "null"}]))
        );
        assert!(recursive_child.get("nullable").is_none());

        let object_union = normalize_json_schema(raw_fixtures[2].clone());
        assert_eq!(
            object_union.get("required"),
            Some(&json!(["count", "value"]))
        );

        let normalized_complex = normalize_json_schema(common_complex_schema.clone());
        assert_eq!(normalized_complex, common_complex_schema);
        assert_eq!(
            normalized_complex["properties"]["id"]["pattern"],
            json!("^[a-z][a-z0-9-]*$")
        );
        assert_eq!(
            normalized_complex["properties"]["mode"]["const"],
            json!("standard")
        );
        assert_eq!(
            normalized_complex["properties"]["category"]["enum"],
            json!(["alpha", "beta"])
        );
        assert_eq!(
            normalized_complex["properties"]["status"]["anyOf"][1]["minimum"],
            json!(1)
        );
        assert_eq!(
            normalized_complex["properties"]["status"]["anyOf"][1]["maximum"],
            json!(3)
        );
        assert_eq!(
            normalized_complex["properties"]["score"]["multipleOf"],
            json!(5)
        );
        assert_eq!(
            normalized_complex["properties"]["confidence"]["exclusiveMaximum"],
            json!(1)
        );
        assert_eq!(
            normalized_complex["properties"]["score"]["default"],
            json!(0)
        );
        assert_eq!(
            normalized_complex["properties"]["owner"]["anyOf"][1],
            json!({"type": "null"})
        );
        assert_eq!(
            normalized_complex["properties"]["owner"]["anyOf"][0]["properties"]["tier"]["default"],
            json!(1)
        );
        assert_eq!(
            normalized_complex["properties"]["tags"]["maxItems"],
            json!(4)
        );
        assert_eq!(
            normalized_complex["properties"]["metrics"]["additionalProperties"]["properties"]
                ["count"]["maximum"],
            json!(9007199254740991i64)
        );
        assert_eq!(
            normalized_complex["properties"]["metrics"]["additionalProperties"]["properties"]
                ["note"]["anyOf"][0]["maxLength"],
            json!(20)
        );
        assert_eq!(
            normalized_complex["properties"]["metrics"]["additionalProperties"]["properties"]
                ["samples"]["items"]["multipleOf"],
            json!(0.25)
        );
        assert_eq!(
            normalized_complex["properties"]["regions"]["items"]["properties"]["window"]
                ["prefixItems"][1]["maximum"],
            json!(10)
        );
        assert_eq!(
            normalized_complex["properties"]["regions"]["items"]["properties"]["visible"]
                ["default"],
            json!(true)
        );
        validate_json_schema_value(
            &normalized_complex,
            &normalized_complex,
            &common_complex_payload,
            "$",
        )
        .expect("complex payload should validate");
        for invalid_case in common_complex_invalid_payloads {
            let invalid_payload = &invalid_case["payload"];
            assert!(
                validate_json_schema_value(
                    &normalized_complex,
                    &normalized_complex,
                    invalid_payload,
                    "$"
                )
                .is_err(),
                "complex invalid payload should fail validation: {invalid_case}"
            );
        }
    }

    #[test]
    fn test_custom_pydantic_models_auto_extraction() {
        let bus = EventBus::new(Some("RuntimeSchemaExtractionBus".to_string()));
        let event = bus.emit(RuntimeSchemaEvent {
            ..Default::default()
        });
        assert_eq!(
            event.event_result_type,
            Some(
                <RuntimeSchemaEvent as abxbus::typed::EventSpec>::event_result_type_json()
                    .expect("runtime schema")
            )
        );
        bus.destroy();
    }

    #[test]
    fn test_complex_generic_types_auto_extraction() {
        for schema in [
            json!({"type": "array", "items": {"type": "string"}}),
            json!({"type": "object", "additionalProperties": {"type": "integer"}}),
            json!({"type": "array", "uniqueItems": true, "items": {"type": "integer"}}),
        ] {
            assert_schema_roundtrips(schema);
        }
    }

    #[test]
    fn test_json_schema_list_of_models_deserialization() {
        let bus = EventBus::new(Some("SchemaListOfModelsBus".to_string()));
        let schema = json!({
            "type": "array",
            "items": {"$ref": "#/$defs/UserData"},
            "$defs": {
                "UserData": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "age": {"type": "integer"}
                    },
                    "required": ["name", "age"],
                    "additionalProperties": false
                }
            }
        });
        assert_schema_roundtrips(schema.clone());

        bus.on_raw("ListOfModelsValidEvent", "handler", |_event| async move {
            Ok(json!([{"name": "alice", "age": 33}]))
        });
        let valid_event =
            bus.emit_base(schema_event("ListOfModelsValidEvent", Some(schema.clone())));
        wait(&valid_event);
        let valid_result = first_event_result_record(&valid_event);
        assert_eq!(valid_result.status, EventResultStatus::Completed);
        assert_eq!(
            valid_result.result,
            Some(json!([{"name": "alice", "age": 33}]))
        );

        bus.on_raw("ListOfModelsInvalidEvent", "handler", |_event| async move {
            Ok(json!([{"name": "alice", "age": "bad"}]))
        });
        let invalid_event = bus.emit_base(schema_event("ListOfModelsInvalidEvent", Some(schema)));
        wait(&invalid_event);
        assert_eq!(
            first_event_result_record(&invalid_event).status,
            EventResultStatus::Error
        );
        bus.destroy();
    }

    #[test]
    fn test_json_schema_nested_object_collection_deserialization() {
        let bus = EventBus::new(Some("SchemaNestedObjectCollectionBus".to_string()));
        let schema = json!({
            "type": "object",
            "additionalProperties": {
                "type": "array",
                "items": {"$ref": "#/$defs/TaskResult"}
            },
            "$defs": {
                "TaskResult": {
                    "type": "object",
                    "properties": {
                        "task_id": {"type": "string"},
                        "status": {"type": "string"}
                    },
                    "required": ["task_id", "status"],
                    "additionalProperties": false
                }
            }
        });
        assert_schema_roundtrips(schema.clone());

        bus.on_raw("NestedObjectValidEvent", "handler", |_event| async move {
        Ok(json!({"batch_a": [{"task_id": "6b2e9266-87c4-7d4a-81e5-a6026165e14b", "status": "ok"}]}))
    });
        let valid_event =
            bus.emit_base(schema_event("NestedObjectValidEvent", Some(schema.clone())));
        wait(&valid_event);
        assert_eq!(
            first_event_result_record(&valid_event).status,
            EventResultStatus::Completed
        );

        bus.on_raw("NestedObjectInvalidEvent", "handler", |_event| async move {
        Ok(json!({"batch_a": [{"task_id": "6b2e9266-87c4-7d4a-81e5-a6026165e14b", "status": 404}]}))
    });
        let invalid_event = bus.emit_base(schema_event("NestedObjectInvalidEvent", Some(schema)));
        wait(&invalid_event);
        assert_eq!(
            first_event_result_record(&invalid_event).status,
            EventResultStatus::Error
        );
        bus.destroy();
    }

    #[test]
    fn test_type_adapter_validation() {
        let bus = EventBus::new(Some("TypeAdapterValidationBus".to_string()));
        let schema = json!({"type": "object", "additionalProperties": {"type": "integer"}});

        bus.on_raw(
            "TypeAdapterValidEvent",
            "valid_handler",
            |_event| async move { Ok(json!({"abc": 123, "def": 456})) },
        );
        let valid_event =
            bus.emit_base(schema_event("TypeAdapterValidEvent", Some(schema.clone())));
        wait(&valid_event);
        assert_eq!(
            first_event_result_record(&valid_event).status,
            EventResultStatus::Completed
        );

        bus.on_raw(
            "TypeAdapterInvalidEvent",
            "invalid_handler",
            |_event| async move { Ok(json!({"abc": "badvalue"})) },
        );
        let invalid_event = bus.emit_base(schema_event("TypeAdapterInvalidEvent", Some(schema)));
        wait(&invalid_event);
        let invalid_result = first_event_result_record(&invalid_event);
        assert_eq!(invalid_result.status, EventResultStatus::Error);
        assert!(invalid_result
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("integer"));
        bus.destroy();
    }

    #[test]
    fn test_json_schema_top_level_shape_deserialization_matrix() {
        for schema in [
            json!({"type": "array", "items": {"type": "string"}}),
            json!({"type": "array", "items": {"type": "integer"}}),
            json!({"type": "object", "additionalProperties": {"type": "integer"}}),
            json!({
                "type": "object",
                "properties": {"scores": {"type": "array", "items": {"type": "integer"}}},
                "required": ["scores"]
            }),
        ] {
            assert_schema_roundtrips(schema);
        }
    }

    #[test]
    fn test_json_schema_typed_dict_rehydrates_to_pydantic_model() {
        let bus = EventBus::new(Some("TypedDictSchemaBus".to_string()));
        let schema = json!({
            "type": "object",
            "properties": {
                "user_id": {"type": "string"},
                "active": {"type": "boolean"},
                "score": {"type": "integer"}
            },
            "required": ["user_id", "active", "score"],
            "additionalProperties": false
        });

        bus.on_raw("TypedDictValidEvent", "handler", |_event| async move {
        Ok(json!({"user_id": "e692b6cb-ae63-773b-8557-3218f7ce5ced", "active": true, "score": 9}))
    });
        let event = bus.emit_base(schema_event("TypedDictValidEvent", Some(schema)));
        wait(&event);
        assert_eq!(
            first_event_result_record(&event).status,
            EventResultStatus::Completed
        );
        bus.destroy();
    }

    #[test]
    fn test_json_schema_optional_typed_dict_is_lax_on_missing_fields() {
        let bus = EventBus::new(Some("OptionalSchemaBus".to_string()));
        let optional_schema = json!({
            "type": "object",
            "properties": {
                "nickname": {"type": "string"},
                "age": {"type": "integer"}
            }
        });

        for (event_type, result) in [
            ("OptionalSchemaEmptyEvent", json!({})),
            ("OptionalSchemaPartialEvent", json!({"nickname": "squash"})),
        ] {
            bus.on_raw(event_type, "handler", move |_event| {
                let result = result.clone();
                async move { Ok(result) }
            });
            let event = bus.emit_base(schema_event(event_type, Some(optional_schema.clone())));
            wait(&event);
            assert_eq!(
                first_event_result_record(&event).status,
                EventResultStatus::Completed
            );
        }
        bus.destroy();
    }

    #[test]
    fn test_json_schema_dataclass_rehydrates_to_pydantic_model() {
        let bus = EventBus::new(Some("DataclassSchemaBus".to_string()));
        let schema = json!({
            "type": "object",
            "properties": {
                "task_id": {"type": "string"},
                "priority": {"type": "integer"}
            },
            "required": ["task_id", "priority"],
            "additionalProperties": false
        });

        bus.on_raw("DataclassValidEvent", "handler", |_event| async move {
            Ok(json!({"task_id": "16272e4a-6936-7e87-872b-0eadeb911f9d", "priority": 2}))
        });
        let event = bus.emit_base(schema_event("DataclassValidEvent", Some(schema)));
        wait(&event);
        assert_eq!(
            first_event_result_record(&event).status,
            EventResultStatus::Completed
        );
        bus.destroy();
    }

    #[test]
    fn test_json_schema_list_of_dataclass_rehydrates_to_list_of_models() {
        let bus = EventBus::new(Some("DataclassListSchemaBus".to_string()));
        let schema = json!({
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "task_id": {"type": "string"},
                    "priority": {"type": "integer"}
                },
                "required": ["task_id", "priority"],
                "additionalProperties": false
            }
        });

        bus.on_raw("DataclassListValidEvent", "handler", |_event| async move {
            Ok(json!([{"task_id": "78cfaa39-d697-7ef5-8e62-19b94b2cb48e", "priority": 5}]))
        });
        let event = bus.emit_base(schema_event("DataclassListValidEvent", Some(schema)));
        wait(&event);
        assert_eq!(
            first_event_result_record(&event).status,
            EventResultStatus::Completed
        );
        bus.destroy();
    }

    #[test]
    fn test_json_schema_nested_object_and_array_runtime_enforcement() {
        let bus = EventBus::new(Some("NestedSchemaRuntimeBus".to_string()));
        let nested_schema = json!({
            "type": "object",
            "properties": {
                "items": {"type": "array", "items": {"type": "integer"}},
                "meta": {"type": "object", "additionalProperties": {"type": "boolean"}}
            },
            "required": ["items", "meta"]
        });

        bus.on_raw(
            "NestedSchemaValidEvent",
            "valid_handler",
            |_event| async move {
                Ok(json!({"items": [1, 2, 3], "meta": {"ok": true, "cached": false}}))
            },
        );
        let valid_event = bus.emit_base(schema_event(
            "NestedSchemaValidEvent",
            Some(nested_schema.clone()),
        ));
        wait(&valid_event);
        let valid_result = first_event_result_record(&valid_event);
        assert_eq!(valid_result.status, EventResultStatus::Completed);
        assert_eq!(
            valid_result.result,
            Some(json!({"items": [1, 2, 3], "meta": {"ok": true, "cached": false}}))
        );

        bus.on_raw(
            "NestedSchemaInvalidEvent",
            "invalid_handler",
            |_event| async move { Ok(json!({"items": ["not-an-int"], "meta": {"ok": "yes"}})) },
        );
        let invalid_event = bus.emit_base(schema_event(
            "NestedSchemaInvalidEvent",
            Some(nested_schema),
        ));
        wait(&invalid_event);
        let invalid_result = first_event_result_record(&invalid_event);
        assert_eq!(invalid_result.status, EventResultStatus::Error);
        assert!(invalid_result
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("EventHandlerResultSchemaError"));
        bus.destroy();
    }

    #[test]
    fn test_module_level_runtime_enforcement() {
        let bus = EventBus::new(Some("ModuleLevelRuntimeBus".to_string()));
        let module_schema = json!({
            "type": "object",
            "properties": {
                "result_id": {"type": "string"},
                "data": {"type": "object"},
                "success": {"type": "boolean"}
            },
            "required": ["result_id", "data", "success"],
            "additionalProperties": false
        });

        bus.on_raw(
            "RuntimeValidEvent",
            "correct_handler",
            |_event| async move {
                Ok(json!({
                    "result_id": "e1bb315c-472f-7bd1-8e72-c8502e1a9a36",
                    "data": {"key": "value"},
                    "success": true
                }))
            },
        );
        let valid_event = bus.emit_base(schema_event(
            "RuntimeValidEvent",
            Some(module_schema.clone()),
        ));
        wait(&valid_event);
        assert_eq!(
            first_event_result_record(&valid_event).status,
            EventResultStatus::Completed
        );

        bus.on_raw(
            "RuntimeInvalidEvent",
            "incorrect_handler",
            |_event| async move { Ok(json!({"wrong": "format"})) },
        );
        let invalid_event = bus.emit_base(schema_event("RuntimeInvalidEvent", Some(module_schema)));
        wait(&invalid_event);
        let invalid_result = first_event_result_record(&invalid_event);
        assert_eq!(invalid_result.status, EventResultStatus::Error);
        assert!(invalid_result
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("required"));
        bus.destroy();
    }

    #[test]
    fn test_roundtrip_preserves_complex_result_schema_types() {
        let bus = EventBus::new(Some("RoundtripSchemaBus".to_string()));
        let complex_schema = json!({
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "count": {"type": "number"},
                "flags": {"type": "array", "items": {"type": "boolean"}},
                "active": {"type": "boolean"},
                "meta": {
                    "type": "object",
                    "properties": {
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "rating": {"type": "number"}
                    },
                    "required": ["tags", "rating"]
                }
            },
            "required": ["title", "count", "flags", "active", "meta"]
        });
        let normalized_complex_schema = normalize_json_schema(complex_schema.clone());
        let original = schema_event("ComplexRoundtripEvent", Some(complex_schema.clone()));
        let roundtripped = BaseEvent::from_json_value(original.to_json_value());

        assert_eq!(
            roundtripped.inner.lock().event_result_type,
            Some(normalized_complex_schema)
        );

        bus.on_raw("ComplexRoundtripEvent", "handler", |_event| async move {
            Ok(json!({
                "title": "ok",
                "count": 3,
                "flags": [true, false, true],
                "active": false,
                "meta": {"tags": ["a", "b"], "rating": 4}
            }))
        });

        let dispatched = bus.emit_base(roundtripped);
        wait(&dispatched);

        let result = first_event_result_record(&dispatched);
        assert_eq!(
            result.status,
            abxbus::event_result::EventResultStatus::Completed
        );
        assert_eq!(
            result.result,
            Some(json!({
                "title": "ok",
                "count": 3,
                "flags": [true, false, true],
                "active": false,
                "meta": {"tags": ["a", "b"], "rating": 4}
            }))
        );
        bus.destroy();
    }
}

// Folded from test_typed_events.rs to keep test layout class-based.
mod folded_test_typed_events {
    use abxbus::{
        base_event::EventResultOptions,
        event,
        event_bus::{EventBus, FindOptions},
        typed::EventSpec,
    };
    use futures::executor::block_on;
    use serde::{Deserialize, Serialize};
    use std::{sync::Arc, thread, time::Duration};

    #[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
    struct AddResult {
        sum: i64,
    }

    event! {
        struct AddEvent {
            a: i64,
            b: i64,
            event_result_type: AddResult,
        }
    }

    event! {
        struct TimeoutOverrideEvent {
            name: String,
            event_result_type: serde_json::Value,
        }
    }

    #[test]
    fn test_on_and_emit_typed_roundtrip() {
        let bus = EventBus::new(Some("TypedBus".to_string()));

        bus.on(AddEvent, |event: AddEvent| async move {
            Ok(AddResult {
                sum: event.a + event.b,
            })
        });

        let event = bus.emit(AddEvent {
            a: 4,
            b: 9,
            ..Default::default()
        });
        let _ = block_on(event.now());

        let first = block_on(event.event_result_with_options(EventResultOptions::default()))
            .expect("first result");
        assert_eq!(first, Some(AddResult { sum: 13 }));
        bus.destroy();
    }

    #[test]
    fn test_find_returns_typed_payload() {
        let bus = EventBus::new(Some("TypedFindBus".to_string()));

        let event = bus.emit(AddEvent {
            a: 7,
            b: 1,
            ..Default::default()
        });
        let _ = block_on(event.now());

        let found = block_on(bus.find(AddEvent::event_type, true, None, None))
            .map(<AddEvent as abxbus::typed::TypedEventObject>::_from_inner_event)
            .expect("expected typed event");
        assert_eq!(found.a, 7);
        assert_eq!(found.b, 1);
        bus.destroy();
    }

    #[test]
    fn test_find_type_inference() {
        let bus = EventBus::new(Some("expect_type_test_bus".to_string()));
        let bus_for_thread = bus.clone();

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            bus_for_thread.emit(AddEvent {
                a: 57,
                b: 42,
                ..Default::default()
            });
        });

        let found = block_on(bus.find(AddEvent::event_type, false, Some(1.0), None))
            .map(<AddEvent as abxbus::typed::TypedEventObject>::_from_inner_event)
            .expect("expected future typed event");
        assert_eq!(found.a, 57);
        assert_eq!(found.b, 42);

        let bus_for_filter = bus.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            bus_for_filter.emit(AddEvent {
                a: 32,
                b: 1,
                ..Default::default()
            });
            bus_for_filter.emit(AddEvent {
                a: 51,
                b: 96,
                ..Default::default()
            });
        });

        let filtered = block_on(bus.find_with_options(
            AddEvent::event_type,
            FindOptions {
                past: false,
                future: Some(1.0),
                where_predicate: Some(Arc::new(|event| {
                    event
                        .inner
                        .lock()
                        .payload
                        .get("a")
                        .and_then(serde_json::Value::as_i64)
                        == Some(51)
                })),
                ..FindOptions::default()
            },
        ))
        .map(<AddEvent as abxbus::typed::TypedEventObject>::_from_inner_event)
        .expect("expected filtered typed event");
        assert_eq!(filtered.a, 51);
        assert_eq!(filtered.b, 96);
        bus.destroy();
    }

    #[test]
    fn test_find_past_type_inference() {
        let bus = EventBus::new(Some("query_type_test_bus".to_string()));

        let event = bus.emit(AddEvent {
            a: 10,
            b: 20,
            ..Default::default()
        });
        let _ = block_on(event.now());

        let found = block_on(bus.find(AddEvent::event_type, true, None, None))
            .map(<AddEvent as abxbus::typed::TypedEventObject>::_from_inner_event)
            .expect("expected past typed event");
        let found_event_id = found.event_id.clone();
        let emitted_event_id = event.event_id.clone();
        assert_eq!(found_event_id, emitted_event_id);
        assert_eq!(found.a, 10);
        assert_eq!(found.b, 20);
        assert_eq!(found.event_type, "AddEvent");
        bus.destroy();
    }

    #[test]
    fn test_dispatch_type_inference() {
        let bus = EventBus::new(Some("type_inference_test_bus".to_string()));

        bus.on(AddEvent, |event: AddEvent| async move {
            Ok(AddResult {
                sum: event.a + event.b,
            })
        });

        let dispatched_event: AddEvent = bus.emit(AddEvent {
            a: 4,
            b: 6,
            ..Default::default()
        });
        assert_eq!(dispatched_event.a, 4);
        assert_eq!(dispatched_event.b, 6);
        assert_eq!(dispatched_event.event_type, "AddEvent");

        let _ = block_on(dispatched_event.now());
        let result =
            block_on(dispatched_event.event_result_with_options(EventResultOptions::default()))
                .expect("typed event result")
                .expect("handler result");
        assert_eq!(result, AddResult { sum: 10 });
        bus.destroy();
    }

    #[test]
    fn test_typed_event_result_accessors_decode_handler_values() {
        let bus = EventBus::new(Some("TypedResultAccessorsBus".to_string()));

        bus.on(AddEvent, |event: AddEvent| async move {
            Ok(AddResult {
                sum: event.a + event.b,
            })
        });
        bus.on(AddEvent, |event: AddEvent| async move {
            Ok(AddResult {
                sum: event.a * event.b,
            })
        });

        let event = bus.emit(AddEvent {
            a: 3,
            b: 5,
            ..Default::default()
        });
        let _ = block_on(event.now());

        let first = block_on(event.event_result_with_options(EventResultOptions {
            raise_if_any: false,
            raise_if_none: true,
            include: None,
        }))
        .expect("typed first result");
        assert_eq!(first, Some(AddResult { sum: 8 }));

        let values = block_on(event.event_results_list_with_options(EventResultOptions {
            raise_if_any: false,
            raise_if_none: true,
            include: None,
        }))
        .expect("typed results list");
        assert_eq!(values, vec![AddResult { sum: 8 }, AddResult { sum: 15 }]);
        bus.destroy();
    }

    #[test]
    fn test_builtin_event_fields_in_payload_become_runtime_overrides() {
        let bus = EventBus::new(Some("TypedRuntimeOverrideBus".to_string()));
        let event = bus.emit(TimeoutOverrideEvent {
            name: "job".to_string(),
            event_timeout: Some(12.0),
            event_handler_timeout: Some(3.0),
            ..Default::default()
        });
        let base = event._inner_event();
        let inner = base.inner.lock();
        assert_eq!(inner.event_timeout, Some(12.0));
        assert_eq!(inner.event_handler_timeout, Some(3.0));
        assert_eq!(inner.payload.get("name"), Some(&serde_json::json!("job")));
        assert!(!inner.payload.contains_key("event_timeout"));
        assert!(!inner.payload.contains_key("event_handler_timeout"));
        drop(inner);
        bus.destroy();
    }
}
