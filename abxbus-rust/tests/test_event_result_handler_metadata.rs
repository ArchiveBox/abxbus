use std::sync::Arc;

use abxbus_rust::{
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

    let value =
        block_on(event_result.run_handler(event.clone(), None)).expect("handler should complete");
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
        handler_file_path.contains("test_event_result_handler_metadata.rs"),
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
fn test_construct_pending_handler_result_matches_pydantic_constructor() {
    test_construct_pending_handler_result_matches_constructor();
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
