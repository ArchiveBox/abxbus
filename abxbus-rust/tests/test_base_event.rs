use std::sync::Arc;

use abxbus_rust::{
    base_event::{now_iso, BaseEvent},
    types::EventStatus,
};
use futures::executor::block_on;
use serde_json::{json, Map, Value};

fn mk_event(event_type: &str) -> Arc<BaseEvent> {
    let mut payload = Map::new();
    payload.insert("value".to_string(), json!(1));
    BaseEvent::new(event_type.to_string(), payload)
}

fn unwrap_event_error(result: Result<Arc<BaseEvent>, String>) -> String {
    match result {
        Ok(_) => panic!("expected BaseEvent construction to fail"),
        Err(error) => error,
    }
}

#[test]
fn test_baseevent_lifecycle_transitions_are_explicit_and_awaitable() {
    let event = mk_event("BaseEventLifecycleTestEvent");
    assert_eq!(event.inner.lock().event_status, EventStatus::Pending);
    assert!(event.inner.lock().event_started_at.is_none());
    assert!(event.inner.lock().event_completed_at.is_none());

    event.mark_started();
    assert_eq!(event.inner.lock().event_status, EventStatus::Started);
    assert!(event.inner.lock().event_started_at.is_some());

    event.mark_completed();
    assert_eq!(event.inner.lock().event_status, EventStatus::Completed);
    assert!(event.inner.lock().event_completed_at.is_some());
    block_on(event.event_completed());
}

#[test]
fn test_base_event_json_roundtrip() {
    let event = mk_event("test_event");
    let json_value = event.to_json_value();
    let deserialized = BaseEvent::from_json_value(json_value.clone());
    assert_eq!(json_value, deserialized.to_json_value());
}

#[test]
fn test_base_event_runtime_state_transitions() {
    let event = mk_event("runtime_event");
    assert_eq!(event.inner.lock().event_status, EventStatus::Pending);
    event.mark_started();
    assert_eq!(event.inner.lock().event_status, EventStatus::Started);
    event.mark_completed();
    assert_eq!(event.inner.lock().event_status, EventStatus::Completed);
    block_on(event.wait_completed());
}

#[test]
fn test_monotonicdatetime_emits_parseable_monotonic_iso_timestamps() {
    let first = now_iso();
    let second = now_iso();

    assert!(chrono::DateTime::parse_from_rfc3339(&first).is_ok());
    assert!(chrono::DateTime::parse_from_rfc3339(&second).is_ok());
    assert!(second > first || second == first);
}

#[test]
fn test_python_serialized_at_fields_are_strings() {
    let timestamp = now_iso();
    assert!(timestamp.contains('T'));
    assert!(timestamp.ends_with('Z'));
    assert!(timestamp.contains('-'));
    assert!(timestamp.contains(':'));

    let event = mk_event("TimestampEvent");
    let serialized = event.to_json_value();
    assert!(serialized["event_created_at"].is_string());
    assert!(serialized["event_created_at"]
        .as_str()
        .expect("created_at string")
        .contains('T'));
}

#[test]
fn test_baseevent_reset_returns_a_fresh_pending_event_that_can_be_redispatched() {
    let event = mk_event("BaseEventResetEvent");
    event.mark_started();
    event.mark_completed();

    let reset = event.event_reset();

    assert_ne!(reset.inner.lock().event_id, event.inner.lock().event_id);
    assert_eq!(reset.inner.lock().event_status, EventStatus::Pending);
    assert!(reset.inner.lock().event_started_at.is_none());
    assert!(reset.inner.lock().event_completed_at.is_none());
    assert_eq!(reset.inner.lock().event_pending_bus_count, 0);
    assert!(reset.inner.lock().event_results.is_empty());
    assert_eq!(reset.inner.lock().event_type, "BaseEventResetEvent");
    assert_eq!(reset.inner.lock().payload.get("value"), Some(&json!(1)));
}

#[test]
fn test_event_at_fields_are_recognized() {
    let event = BaseEvent::from_json_value(json!({
        "event_id": "018f8e40-1234-7000-8000-000000001240",
        "event_created_at": "2025-01-02T03:04:05.678901234Z",
        "event_started_at": "2025-01-02T03:04:06.100000000Z",
        "event_completed_at": "2025-01-02T03:04:07.200000000Z",
        "event_type": "AtFieldRecognitionEvent",
        "event_timeout": null,
        "event_slow_timeout": 1.5,
        "event_emitted_by_handler_id": "018f8e40-1234-7000-8000-000000000301",
        "event_pending_bus_count": 2
    }));

    let serialized = event.to_json_value();
    assert_eq!(
        serialized["event_created_at"],
        "2025-01-02T03:04:05.678901234Z"
    );
    assert_eq!(
        serialized["event_started_at"],
        "2025-01-02T03:04:06.100000000Z"
    );
    assert_eq!(
        serialized["event_completed_at"],
        "2025-01-02T03:04:07.200000000Z"
    );
    assert_eq!(serialized["event_slow_timeout"], 1.5);
    assert_eq!(
        serialized["event_emitted_by_handler_id"],
        "018f8e40-1234-7000-8000-000000000301"
    );
    assert_eq!(serialized["event_pending_bus_count"], 2);
}

#[test]
fn test_baseevent_fromjson_preserves_nullable_parent_emitted_metadata() {
    let event = BaseEvent::from_json_value(json!({
        "event_id": "018f8e40-1234-7000-8000-000000001234",
        "event_created_at": "2025-01-01T00:00:00.000Z",
        "event_type": "NullableMetadataEvent",
        "event_parent_id": null,
        "event_emitted_by_handler_id": null,
        "event_timeout": null
    }));

    assert_eq!(event.inner.lock().event_parent_id, None);
    assert_eq!(event.inner.lock().event_emitted_by_handler_id, None);
    let payload = event.to_json_value();
    assert_eq!(payload["event_parent_id"], Value::Null);
    assert_eq!(payload["event_emitted_by_handler_id"], Value::Null);
}

#[test]
fn test_event_status_is_serialized_and_stateful() {
    let event = mk_event("SerializeStatusEvent");

    let pending_payload = event.to_json_value();
    assert_eq!(pending_payload["event_status"], "pending");

    event.mark_started();
    let started_payload = event.to_json_value();
    assert_eq!(started_payload["event_status"], "started");
    assert!(started_payload["event_started_at"].is_string());

    event.mark_completed();
    let completed_payload = event.to_json_value();
    assert_eq!(completed_payload["event_status"], "completed");
    assert!(completed_payload["event_completed_at"].is_string());
}

#[test]
fn test_reserved_runtime_fields_are_rejected() {
    for field in ["bus", "emit", "first", "toString", "toJSON", "fromJSON"] {
        let mut payload = Map::new();
        payload.insert(field.to_string(), json!(true));
        let error = unwrap_event_error(BaseEvent::try_new("ReservedFieldEvent", payload));
        assert!(error.contains(field));
    }
}

#[test]
fn test_unknown_event_prefixed_field_rejected_in_payload() {
    let mut payload = Map::new();
    payload.insert("event_unknown".to_string(), json!("bad"));

    let error = unwrap_event_error(BaseEvent::try_new("UnknownEventField", payload));
    assert!(error.contains("event_unknown"));
}

#[test]
fn test_model_prefixed_field_rejected_in_payload() {
    let mut payload = Map::new();
    payload.insert("model_config".to_string(), json!("bad"));

    let error = unwrap_event_error(BaseEvent::try_new("ModelFieldEvent", payload));
    assert!(error.contains("model_config"));
}

#[test]
fn test_from_json_accepts_event_parent_id_null_and_preserves_it_in_to_json_output() {
    let event = BaseEvent::from_json_value(json!({
        "event_id": "018f8e40-1234-7000-8000-000000001234",
        "event_created_at": "2025-01-01T00:00:00.000Z",
        "event_type": "NullParentIdEvent",
        "event_parent_id": null,
        "event_timeout": null
    }));

    assert_eq!(event.inner.lock().event_parent_id, None);
    assert_eq!(event.to_json_value()["event_parent_id"], Value::Null);

    let missing_field_event = BaseEvent::from_json_value(json!({
        "event_id": "018f8e40-1234-7000-8000-000000001233",
        "event_created_at": "2025-01-01T00:00:00.000Z",
        "event_type": "MissingParentIdEvent",
        "event_timeout": null
    }));

    assert_eq!(missing_field_event.inner.lock().event_parent_id, None);
    assert_eq!(
        missing_field_event.to_json_value()["event_parent_id"],
        Value::Null
    );
}

#[test]
fn test_event_emitted_by_handler_id_defaults_to_null_and_accepts_null_in_from_json() {
    let fresh_event = mk_event("NullEmittedByDefaultEvent");
    assert_eq!(fresh_event.inner.lock().event_emitted_by_handler_id, None);

    let missing_field_event = BaseEvent::from_json_value(json!({
        "event_id": "018f8e40-1234-7000-8000-000000001239",
        "event_created_at": "2025-01-01T00:00:00.000Z",
        "event_type": "MissingEmittedByIdEvent",
        "event_timeout": null
    }));
    assert_eq!(
        missing_field_event.inner.lock().event_emitted_by_handler_id,
        None
    );
    assert_eq!(
        missing_field_event.to_json_value()["event_emitted_by_handler_id"],
        Value::Null
    );

    let json_event = BaseEvent::from_json_value(json!({
        "event_id": "018f8e40-1234-7000-8000-00000000123a",
        "event_created_at": "2025-01-01T00:00:00.000Z",
        "event_type": "NullEmittedByIdEvent",
        "event_emitted_by_handler_id": null,
        "event_timeout": null
    }));
    assert_eq!(json_event.inner.lock().event_emitted_by_handler_id, None);
    assert_eq!(
        json_event.to_json_value()["event_emitted_by_handler_id"],
        Value::Null
    );
}
