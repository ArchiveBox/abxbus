use std::sync::Arc;

use abxbus_rust::event;
use abxbus_rust::{
    base_event::BaseEvent,
    event_bus::EventBus,
    event_result::{EventResult, EventResultStatus},
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
    let original = schema_event("SchemaEvent", Some(schema.clone()));
    let restored = BaseEvent::from_json_value(original.to_json_value());
    assert_eq!(restored.inner.lock().event_result_type, Some(schema));
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
        abxbus_rust::event_result::EventResultStatus::Completed
    );
    assert_eq!(result.result, Some(json!({"value": "hello", "count": 42})));
    bus.stop();
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
    bus.stop();
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

    bus.on_raw("SimpleBaseEventHandle", "handler", |_event| async move {
        Ok(json!({"value": "hello", "count": 42}))
    });

    let event = bus.emit_base(schema_event("SimpleBaseEventHandle", Some(schema)));
    wait(&event);

    assert_eq!(
        event.inner.lock().event_status,
        abxbus_rust::types::EventStatus::Completed
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
    bus.stop();
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
impl abxbus_rust::typed::EventSpec for DictIntSchemaEvent {
    type payload = Map<String, Value>;
    type event_result_type = Map<String, Value>;
    const event_type: &'static str = "DictIntSchemaEvent";
    const event_result_type_schema: Option<&'static str> =
        Some(r#"{"type": "object", "additionalProperties": {"type": "integer"}}"#);
}

struct DictModuleSchemaEvent;
impl abxbus_rust::typed::EventSpec for DictModuleSchemaEvent {
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
impl abxbus_rust::typed::EventSpec for DictLocalSchemaEvent {
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
impl abxbus_rust::typed::EventSpec for SpecificUserEvent {
    type payload = Map<String, Value>;
    type event_result_type = ModuleLevelResult;
    const event_type: &'static str = "SpecificUserEvent";
    const event_result_type_schema: Option<&'static str> =
        <RuntimeSchemaEvent as abxbus_rust::typed::EventSpec>::event_result_type_schema;
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
        string_event.inner.inner.lock().event_result_type,
        Some(json!({"type": "string"}))
    );
    assert_eq!(
        int_event.inner.inner.lock().event_result_type,
        Some(json!({"type": "integer"}))
    );
    assert_eq!(
        float_event.inner.inner.lock().event_result_type,
        Some(json!({"type": "number"}))
    );
    bus.stop();
}

#[test]
fn test_no_generic_parameter() {
    let bus = EventBus::new(Some("PlainResultSchemaBus".to_string()));
    let plain_event = bus.emit(PlainSchemaEvent {
        ..Default::default()
    });

    assert_eq!(plain_event.inner.inner.lock().event_result_type, None);
    bus.stop();
}

#[test]
fn test_none_generic_parameter() {
    let bus = EventBus::new(Some("NoneResultSchemaBus".to_string()));
    let none_event = bus.emit(NoneSchemaEvent {
        ..Default::default()
    });

    assert_eq!(none_event.inner.inner.lock().event_result_type, None);
    bus.stop();
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
    wait(&event.inner);
    let result = first_event_result_record(&event.inner);
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
    wait(&invalid_event.inner);
    let invalid_result = first_event_result_record(&invalid_event.inner);
    assert_eq!(invalid_result.status, EventResultStatus::Error);
    assert!(invalid_result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("EventHandlerResultSchemaError"));
    bus.stop();
}

#[test]
fn test_nested_inheritance() {
    assert_eq!(
        <SpecificUserEvent as abxbus_rust::typed::EventSpec>::event_result_type_json(),
        <RuntimeSchemaEvent as abxbus_rust::typed::EventSpec>::event_result_type_json()
    );
}

#[test]
fn test_module_level_types_auto_extraction() {
    let schema = <RuntimeSchemaEvent as abxbus_rust::typed::EventSpec>::event_result_type_json()
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
        <BuiltinIntEvent as abxbus_rust::typed::EventSpec>::event_result_type_json(),
        Some(json!({"type": "integer"}))
    );
}

#[test]
fn test_extract_basemodel_generic_arg_dict() {
    assert_eq!(
        <DictIntSchemaEvent as abxbus_rust::typed::EventSpec>::event_result_type_json(),
        Some(json!({"type": "object", "additionalProperties": {"type": "integer"}}))
    );
}

#[test]
fn test_extract_basemodel_generic_arg_dict_with_module_type() {
    let schema = <DictModuleSchemaEvent as abxbus_rust::typed::EventSpec>::event_result_type_json()
        .expect("module dict schema");
    assert_eq!(schema["type"], "object");
    assert_eq!(
        schema["additionalProperties"]["properties"]["recipients"]["items"]["type"],
        "string"
    );
}

#[test]
fn test_extract_basemodel_generic_arg_dict_with_local_type() {
    let schema = <DictLocalSchemaEvent as abxbus_rust::typed::EventSpec>::event_result_type_json()
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
        <PlainSchemaEvent as abxbus_rust::typed::EventSpec>::event_result_type_json(),
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
            abxbus_rust::event_result::EventResultStatus::Completed
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

    bus.on_raw("NoSchemaEvent", "handler", |_event| async move {
        Ok(json!({"raw": true}))
    });

    let event = bus.emit_base(schema_event("NoSchemaEvent", None));
    wait(&event);

    let result = first_event_result_record(&event);
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

    bus.on_raw("ComplexResultEvent", "handler", |_event| async move {
        Ok(json!({"items": ["a", "b"], "metadata": {"a": 1, "b": 2}}))
    });

    let event = bus.emit_base(schema_event("ComplexResultEvent", Some(schema)));
    wait(&event);

    let result = first_event_result_record(&event);
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

    bus.on_raw("TypedResultEvent", "handler", |_event| async move {
        Ok(json!({"value": "from-json", "count": 7}))
    });

    let dispatched = bus.emit_base(restored);
    wait(&dispatched);

    let result = first_event_result_record(&dispatched);
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
fn test_fromjson_deserializes_event_result_type_and_tojson_reserializes_schema() {
    let schema = json!({"type": "integer"});
    let event = BaseEvent::from_json_value(json!({
        "event_id": "018f8e40-1234-7000-8000-000000001235",
        "event_created_at": "2025-01-01T00:00:01.000Z",
        "event_type": "RawSchemaEvent",
        "event_timeout": null,
        "event_result_type": schema,
    }));

    assert_eq!(event.inner.lock().event_result_type, Some(schema.clone()));
    assert_eq!(event.to_json_value()["event_result_type"], schema);
}

#[test]
fn test_fromjson_converts_event_result_type_into_zod_schema() {
    test_from_json_converts_event_result_type_into_schema();
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
        abxbus_rust::event_result::EventResultStatus::Completed
    );
    assert_eq!(result.result, Some(json!(true)));
    bus.stop();
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
    bus.stop();
}

#[test]
fn test_fromjson_reconstructs_primitive_json_schema() {
    test_from_json_reconstructs_primitive_json_schema();
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
fn test_custom_pydantic_models_auto_extraction() {
    let bus = EventBus::new(Some("RuntimeSchemaExtractionBus".to_string()));
    let event = bus.emit(RuntimeSchemaEvent {
        ..Default::default()
    });
    assert_eq!(
        event.inner.inner.lock().event_result_type,
        Some(
            <RuntimeSchemaEvent as abxbus_rust::typed::EventSpec>::event_result_type_json()
                .expect("runtime schema")
        )
    );
    bus.stop();
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
    let valid_event = bus.emit_base(schema_event("ListOfModelsValidEvent", Some(schema.clone())));
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
    bus.stop();
}

#[test]
fn test_complex_generic_with_custom_types() {
    test_json_schema_list_of_models_deserialization();
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
    let valid_event = bus.emit_base(schema_event("NestedObjectValidEvent", Some(schema.clone())));
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
    bus.stop();
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
    let valid_event = bus.emit_base(schema_event("TypeAdapterValidEvent", Some(schema.clone())));
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
    bus.stop();
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
    bus.stop();
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
    bus.stop();
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
    bus.stop();
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
    bus.stop();
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

    bus.on_raw("NestedSchemaValidEvent", "valid_handler", |_event| async move {
        Ok(json!({"items": [1, 2, 3], "meta": {"ok": true, "cached": false}}))
    });
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
    bus.stop();
}

#[test]
fn test_fromjson_reconstructs_nested_object_array_result_schemas() {
    test_json_schema_nested_object_and_array_runtime_enforcement();
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
