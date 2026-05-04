use std::collections::BTreeSet;

use abxbus_rust::{
    base_event::EventResultsOptions,
    event_bus::EventBus,
    event_handler::EventHandler,
    event_result::{EventResult, EventResultStatus},
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

struct AccessorEvent;
impl EventSpec for AccessorEvent {
    type Payload = EmptyPayload;
    type Result = Value;
    const EVENT_TYPE: &'static str = "AccessorEvent";
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
