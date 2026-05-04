use std::sync::Arc;

use abxbus_rust::{base_event::BaseEvent, event_bus::EventBus};
use futures::executor::block_on;
use serde_json::{json, Map, Value};

fn schema_event(event_type: &str, schema: Option<Value>) -> Arc<BaseEvent> {
    let event = BaseEvent::new(event_type, Map::new());
    event.inner.lock().event_result_type = schema;
    event
}

fn first_result(event: &Arc<BaseEvent>) -> abxbus_rust::event_result::EventResult {
    event
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("expected one event result")
}

fn wait(event: &Arc<BaseEvent>) {
    block_on(event.event_completed());
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

    bus.on("TypedResultEvent", "handler", |_event| async move {
        Ok(json!({"value": "hello", "count": 42}))
    });

    let event = bus.emit_base(schema_event("TypedResultEvent", Some(schema)));
    wait(&event);

    let result = first_result(&event);
    assert_eq!(
        result.status,
        abxbus_rust::event_result::EventResultStatus::Completed
    );
    assert_eq!(result.result, Some(json!({"value": "hello", "count": 42})));
    bus.stop();
}

#[test]
fn test_built_in_result_schemas_validate_handler_results() {
    let bus = EventBus::new(Some("BuiltinResultBus".to_string()));

    bus.on("StringResultEvent", "string_handler", |_event| async move {
        Ok(json!("42"))
    });
    bus.on("NumberResultEvent", "number_handler", |_event| async move {
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

    let string_result = first_result(&string_event);
    let number_result = first_result(&number_event);
    assert_eq!(
        string_result.status,
        abxbus_rust::event_result::EventResultStatus::Completed
    );
    assert_eq!(string_result.result, Some(json!("42")));
    assert_eq!(
        number_result.status,
        abxbus_rust::event_result::EventResultStatus::Completed
    );
    assert_eq!(number_result.result, Some(json!(123)));
    bus.stop();
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
        bus.on(event_type, "handler", move |_event| {
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
            first_result(&event).status,
            abxbus_rust::event_result::EventResultStatus::Completed
        );
    }

    bus.on(
        "ConstructorNumberResultEventInvalid",
        "invalid_handler",
        |_event| async move { Ok(json!("not-a-number")) },
    );
    let invalid = bus.emit_base(schema_event(
        "ConstructorNumberResultEventInvalid",
        Some(json!({"type": "number"})),
    ));
    wait(&invalid);
    let invalid_result = first_result(&invalid);
    assert_eq!(
        invalid_result.status,
        abxbus_rust::event_result::EventResultStatus::Error
    );
    assert!(invalid_result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("EventHandlerResultSchemaError"));
    assert_eq!(invalid.event_errors().len(), 1);
    bus.stop();
}

#[test]
fn test_invalid_handler_result_marks_error_when_schema_is_defined() {
    let bus = EventBus::new(Some("ResultValidationErrorBus".to_string()));

    bus.on("NumberResultEvent", "handler", |_event| async move {
        Ok(json!("not-a-number"))
    });

    let event = bus.emit_base(schema_event(
        "NumberResultEvent",
        Some(json!({"type": "number"})),
    ));
    wait(&event);

    let result = first_result(&event);
    assert_eq!(
        result.status,
        abxbus_rust::event_result::EventResultStatus::Error
    );
    assert!(result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("EventHandlerResultSchemaError"));
    assert!(!event.event_errors().is_empty());
    bus.stop();
}

#[test]
fn test_no_schema_leaves_raw_handler_result_untouched() {
    let bus = EventBus::new(Some("NoSchemaResultBus".to_string()));

    bus.on("NoSchemaEvent", "handler", |_event| async move {
        Ok(json!({"raw": true}))
    });

    let event = bus.emit_base(schema_event("NoSchemaEvent", None));
    wait(&event);

    let result = first_result(&event);
    assert_eq!(
        result.status,
        abxbus_rust::event_result::EventResultStatus::Completed
    );
    assert_eq!(result.result, Some(json!({"raw": true})));
    bus.stop();
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

    bus.on("ComplexResultEvent", "handler", |_event| async move {
        Ok(json!({"items": ["a", "b"], "metadata": {"a": 1, "b": 2}}))
    });

    let event = bus.emit_base(schema_event("ComplexResultEvent", Some(schema)));
    wait(&event);

    let result = first_result(&event);
    assert_eq!(
        result.status,
        abxbus_rust::event_result::EventResultStatus::Completed
    );
    assert_eq!(
        result.result,
        Some(json!({"items": ["a", "b"], "metadata": {"a": 1, "b": 2}}))
    );
    bus.stop();
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
    let restored =
        BaseEvent::from_json_value(schema_event("TypedResultEvent", Some(schema)).to_json_value());

    assert!(restored.inner.lock().event_result_type.is_some());

    bus.on("TypedResultEvent", "handler", |_event| async move {
        Ok(json!({"value": "from-json", "count": 7}))
    });

    let dispatched = bus.emit_base(restored);
    wait(&dispatched);

    let result = first_result(&dispatched);
    assert_eq!(
        result.status,
        abxbus_rust::event_result::EventResultStatus::Completed
    );
    assert_eq!(
        result.result,
        Some(json!({"value": "from-json", "count": 7}))
    );
    bus.stop();
}

#[test]
fn test_from_json_reconstructs_primitive_json_schema() {
    let bus = EventBus::new(Some("PrimitiveFromJsonBus".to_string()));
    let restored = BaseEvent::from_json_value(
        schema_event("PrimitiveResultEvent", Some(json!({"type": "boolean"}))).to_json_value(),
    );

    assert!(restored.inner.lock().event_result_type.is_some());

    bus.on("PrimitiveResultEvent", "handler", |_event| async move {
        Ok(json!(true))
    });

    let dispatched = bus.emit_base(restored);
    wait(&dispatched);

    let result = first_result(&dispatched);
    assert_eq!(
        result.status,
        abxbus_rust::event_result::EventResultStatus::Completed
    );
    assert_eq!(result.result, Some(json!(true)));
    bus.stop();
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
    let original = schema_event("ComplexRoundtripEvent", Some(complex_schema.clone()));
    let roundtripped = BaseEvent::from_json_value(original.to_json_value());

    assert_eq!(
        roundtripped.inner.lock().event_result_type,
        Some(complex_schema)
    );

    bus.on("ComplexRoundtripEvent", "handler", |_event| async move {
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

    let result = first_result(&dispatched);
    assert_eq!(
        result.status,
        abxbus_rust::event_result::EventResultStatus::Completed
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
    bus.stop();
}
