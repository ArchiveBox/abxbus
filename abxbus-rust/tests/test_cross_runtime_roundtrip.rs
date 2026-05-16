use abxbus::event;
use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::PathBuf,
    process::Command,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use abxbus::{base_event::BaseEvent, event_bus::EventBus, event_handler::EventHandlerOptions};
use futures::executor::block_on;
use serde_json::{json, Value};

event! {
    struct CrossRuntimeResumeEvent {
        label: String,
        event_result_type: Value,
        event_type: "CrossRuntimeResumeEvent",
    }
}
fn assert_original_fields_survive(original: &Value, roundtripped: &Value) {
    let original = original.as_object().expect("original object");
    let roundtripped = roundtripped.as_object().expect("roundtripped object");
    for (key, value) in original {
        assert!(
            roundtripped.contains_key(key),
            "missing key after roundtrip: {key}"
        );
        if let (Some(left), Some(right)) = (roundtripped[key].as_f64(), value.as_f64()) {
            assert!((left - right).abs() < f64::EPSILON, "field changed: {key}");
            continue;
        }
        assert_eq!(&roundtripped[key], value, "field changed: {key}");
    }
}

fn run_go_roundtrip(mode: &str, payload: &Value) -> Value {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir.parent().expect("repo root");
    let go_root = repo_root.join("abxbus-go");
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let temp_dir = env::temp_dir().join(format!("abxbus-rust-go-roundtrip-{mode}-{unique}"));
    fs::create_dir_all(&temp_dir).expect("create temp roundtrip dir");
    let input_path = temp_dir.join("input.json");
    let output_path = temp_dir.join("output.json");
    fs::write(
        &input_path,
        serde_json::to_vec_pretty(payload).expect("encode input payload"),
    )
    .expect("write input payload");

    let output = Command::new("go")
        .args([
            "run",
            "./tests/roundtrip_cli",
            mode,
            input_path.to_str().expect("input path utf8"),
            output_path.to_str().expect("output path utf8"),
        ])
        .current_dir(&go_root)
        .output()
        .expect("run Go roundtrip helper");
    if !output.status.success() {
        panic!(
            "go {mode} roundtrip failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let output_payload = fs::read(&output_path).expect("read output payload");
    let _ = fs::remove_dir_all(&temp_dir);
    serde_json::from_slice(&output_payload).expect("decode output payload")
}

fn event_result_fixture(
    id: &str,
    event_id: &str,
    handler_id: &str,
    handler_name: &str,
    status: &str,
    result: Value,
) -> Value {
    json!({
        "id": id,
        "status": status,
        "event_id": event_id,
        "handler_id": handler_id,
        "handler_name": handler_name,
        "handler_file_path": null,
        "handler_timeout": null,
        "handler_slow_timeout": null,
        "handler_registered_at": if handler_id == "handler-one" {
            "2025-01-02T03:04:05.000000000Z"
        } else {
            "2025-01-02T03:04:06.000000000Z"
        },
        "handler_event_pattern": "CrossRuntimeResumeEvent",
        "eventbus_name": "CrossRuntimeBus",
        "eventbus_id": "018f8e40-1234-7000-8000-00000000cc33",
        "started_at": null,
        "completed_at": null,
        "result": result,
        "error": null,
        "event_children": []
    })
}

fn event_fixture(event_id: &str, label: &str, event_results: BTreeMap<String, Value>) -> Value {
    let mut event = json!({
        "event_type": "CrossRuntimeResumeEvent",
        "event_version": "0.0.1",
        "event_timeout": null,
        "event_slow_timeout": null,
        "event_concurrency": null,
        "event_handler_timeout": null,
        "event_handler_slow_timeout": null,
        "event_handler_concurrency": null,
        "event_handler_completion": null,
        "event_blocks_parent_completion": false,
        "event_result_type": {"type": "string"},
        "event_id": event_id,
        "event_path": ["PyBus#aaaa", "TsBridge#bbbb"],
        "event_parent_id": null,
        "event_emitted_by_handler_id": null,
        "event_pending_bus_count": 0,
        "event_created_at": "2025-01-02T03:04:04.000000000Z",
        "event_status": "pending",
        "event_started_at": null,
        "event_completed_at": null,
        "label": label
    });
    if !event_results.is_empty() {
        event["event_results"] = json!(event_results);
    }
    event
}

fn cross_runtime_bus_fixture() -> Value {
    let mut event_one_results = BTreeMap::new();
    event_one_results.insert(
        "handler-one".to_string(),
        event_result_fixture(
            "result-one-h1",
            "018f8e40-1234-7000-8000-00000000e001",
            "handler-one",
            "handler_one",
            "completed",
            json!("seeded"),
        ),
    );
    event_one_results.insert(
        "handler-two".to_string(),
        event_result_fixture(
            "result-one-h2",
            "018f8e40-1234-7000-8000-00000000e001",
            "handler-two",
            "handler_two",
            "pending",
            Value::Null,
        ),
    );

    json!({
        "id": "018f8e40-1234-7000-8000-00000000cc33",
        "name": "CrossRuntimeBus",
        "max_history_size": 100,
        "max_history_drop": false,
        "event_concurrency": "bus-serial",
        "event_timeout": 60.0,
        "event_slow_timeout": 300.0,
        "event_handler_concurrency": "serial",
        "event_handler_completion": "all",
        "event_handler_slow_timeout": 30.0,
        "event_handler_detect_file_paths": false,
        "handlers": {
            "handler-one": {
                "id": "handler-one",
                "event_pattern": "CrossRuntimeResumeEvent",
                "handler_name": "handler_one",
                "handler_file_path": null,
                "handler_timeout": null,
                "handler_slow_timeout": null,
                "handler_registered_at": "2025-01-02T03:04:05.000000000Z",
                "eventbus_name": "CrossRuntimeBus",
                "eventbus_id": "018f8e40-1234-7000-8000-00000000cc33"
            },
            "handler-two": {
                "id": "handler-two",
                "event_pattern": "CrossRuntimeResumeEvent",
                "handler_name": "handler_two",
                "handler_file_path": null,
                "handler_timeout": null,
                "handler_slow_timeout": null,
                "handler_registered_at": "2025-01-02T03:04:06.000000000Z",
                "eventbus_name": "CrossRuntimeBus",
                "eventbus_id": "018f8e40-1234-7000-8000-00000000cc33"
            }
        },
        "handlers_by_key": {
            "CrossRuntimeResumeEvent": ["handler-one", "handler-two"]
        },
        "event_history": {
            "018f8e40-1234-7000-8000-00000000e001": event_fixture(
                "018f8e40-1234-7000-8000-00000000e001",
                "e1",
                event_one_results,
            ),
            "018f8e40-1234-7000-8000-00000000e002": event_fixture(
                "018f8e40-1234-7000-8000-00000000e002",
                "e2",
                BTreeMap::new(),
            )
        },
        "pending_event_queue": [
            "018f8e40-1234-7000-8000-00000000e001",
            "018f8e40-1234-7000-8000-00000000e002"
        ]
    })
}

fn go_rust_event_fixture() -> Value {
    json!({
        "event_type": "GoRustRoundtripEvent",
        "event_version": "0.0.1",
        "event_timeout": null,
        "event_slow_timeout": null,
        "event_concurrency": null,
        "event_handler_timeout": null,
        "event_handler_slow_timeout": null,
        "event_handler_concurrency": null,
        "event_handler_completion": null,
        "event_blocks_parent_completion": false,
        "event_result_type": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "kind": {"const": "ok"},
                "scores": {
                    "type": "array",
                    "minItems": 1,
                    "items": {"type": "number", "minimum": 0}
                }
            },
            "required": ["kind", "scores"],
            "additionalProperties": false
        },
        "event_id": "018f8e40-1234-7000-8000-00000000d001",
        "event_path": ["RustBus#aaaa", "GoBridge#bbbb"],
        "event_parent_id": "018f8e40-1234-7000-8000-00000000d000",
        "event_emitted_by_handler_id": "handler-parent",
        "event_pending_bus_count": 0,
        "event_created_at": "2025-01-02T03:04:07.678901000Z",
        "event_status": "pending",
        "event_started_at": null,
        "event_completed_at": null,
        "label": "rust-go"
    })
}

#[test]
fn test_python_to_rust_roundtrip_preserves_event_fields_and_result_type_schema() {
    let original = json!({
        "event_type": "PyTsScreenshotEvent",
        "event_version": "0.0.1",
        "event_timeout": 33.0,
        "event_slow_timeout": null,
        "event_concurrency": null,
        "event_handler_timeout": null,
        "event_handler_slow_timeout": null,
        "event_handler_concurrency": null,
        "event_handler_completion": null,
        "event_blocks_parent_completion": false,
        "event_result_type": {
            "type": "object",
            "properties": {
                "image_url": {"type": "string"},
                "width": {"type": "integer"},
                "height": {"type": "integer"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "is_animated": {"type": "boolean"},
                "confidence_scores": {"type": "array", "items": {"type": "number"}},
                "metadata": {"type": "object", "additionalProperties": {"type": "number"}},
                "regions": {"type": "array", "items": {"type": "object"}}
            },
            "required": ["image_url", "width", "height", "tags", "is_animated"]
        },
        "event_id": "018f8e40-1234-7000-8000-00000000a001",
        "event_path": ["PyBus#aaaa", "TsBridge#bbbb"],
        "event_parent_id": "018f8e40-1234-7000-8000-00000000a000",
        "event_emitted_by_handler_id": null,
        "event_pending_bus_count": 0,
        "event_created_at": "2025-01-02T03:04:05.678901000Z",
        "event_status": "pending",
        "event_started_at": null,
        "event_completed_at": null,
        "target_id": "0c1ccf21-65c0-7390-8b64-9182e985740e",
        "quality": "high"
    });

    let restored = BaseEvent::from_json_value(original.clone());
    let roundtripped = restored.to_json_value();

    assert_original_fields_survive(&original, &roundtripped);
}

#[test]
fn test_ts_to_rust_roundtrip_preserves_event_fields_and_result_type_schema() {
    let original = json!({
        "event_type": "TsPy_RecordResultEvent",
        "event_version": "0.0.1",
        "event_timeout": null,
        "event_slow_timeout": null,
        "event_concurrency": null,
        "event_handler_timeout": null,
        "event_handler_slow_timeout": null,
        "event_handler_concurrency": null,
        "event_handler_completion": null,
        "event_blocks_parent_completion": false,
        "event_result_type": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "additionalProperties": {
                "type": "array",
                "items": {"type": "number"}
            }
        },
        "event_id": "018f8e40-1234-7000-8000-00000000b001",
        "event_path": ["TsBus#aaaa"],
        "event_parent_id": null,
        "event_emitted_by_handler_id": null,
        "event_pending_bus_count": 0,
        "event_created_at": "2025-01-02T03:04:06.678901000Z",
        "event_status": "pending",
        "event_started_at": null,
        "event_completed_at": null,
        "id": "ba1a8735-0955-737f-8b4d-7337d2169a3c"
    });

    let restored = BaseEvent::from_json_value(original.clone());
    let roundtripped = restored.to_json_value();

    assert_original_fields_survive(&original, &roundtripped);
}

#[test]
fn test_go_to_rust_roundtrip_preserves_event_fields_and_result_type_schema() {
    let original = go_rust_event_fixture();
    let go_roundtripped = run_go_roundtrip("events", &json!([original.clone()]));
    let go_event = go_roundtripped
        .as_array()
        .and_then(|events| events.first())
        .expect("go roundtripped event");
    assert_original_fields_survive(&original, go_event);

    let restored = BaseEvent::from_json_value(go_event.clone());
    let rust_roundtripped = restored.to_json_value();
    assert_original_fields_survive(&original, &rust_roundtripped);
}

#[test]
fn test_rust_to_go_roundtrip_preserves_event_fields_and_result_type_schema() {
    let original = go_rust_event_fixture();
    let rust_serialized = BaseEvent::from_json_value(original).to_json_value();
    let go_roundtripped = run_go_roundtrip("events", &json!([rust_serialized.clone()]));
    let go_event = go_roundtripped
        .as_array()
        .and_then(|events| events.first())
        .expect("go roundtripped event");
    assert_original_fields_survive(&rust_serialized, go_event);
}

#[test]
fn test_cross_runtime_bus_roundtrip_rehydrates_and_resumes_pending_queue() {
    assert_bus_roundtrip_rehydrates_and_resumes_pending_queue(cross_runtime_bus_fixture());
}

#[test]
fn test_go_to_rust_bus_roundtrip_rehydrates_and_resumes_pending_queue() {
    let fixture = cross_runtime_bus_fixture();
    let go_roundtripped = run_go_roundtrip("bus", &fixture);
    assert_original_fields_survive(&fixture, &go_roundtripped);
    assert_bus_roundtrip_rehydrates_and_resumes_pending_queue(go_roundtripped);
}

fn assert_bus_roundtrip_rehydrates_and_resumes_pending_queue(payload: Value) {
    let bus = EventBus::from_json_value(payload);
    let run_order = Arc::new(Mutex::new(Vec::<String>::new()));

    let run_order_for_h1 = run_order.clone();
    bus.on_raw_with_options(
        "CrossRuntimeResumeEvent",
        "handler_one",
        EventHandlerOptions {
            id: Some("handler-one".to_string()),
            detect_handler_file_path: Some(false),
            handler_registered_at: Some("2025-01-02T03:04:05.000000000Z".to_string()),
            ..EventHandlerOptions::default()
        },
        move |event| {
            let run_order = run_order_for_h1.clone();
            async move {
                let label = event
                    .inner
                    .lock()
                    .payload
                    .get("label")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let value = format!("h1:{label}");
                run_order.lock().expect("run order").push(value.clone());
                Ok(json!(value))
            }
        },
    );

    let run_order_for_h2 = run_order.clone();
    bus.on_raw_with_options(
        "CrossRuntimeResumeEvent",
        "handler_two",
        EventHandlerOptions {
            id: Some("handler-two".to_string()),
            detect_handler_file_path: Some(false),
            handler_registered_at: Some("2025-01-02T03:04:06.000000000Z".to_string()),
            ..EventHandlerOptions::default()
        },
        move |event| {
            let run_order = run_order_for_h2.clone();
            async move {
                let label = event
                    .inner
                    .lock()
                    .payload
                    .get("label")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let value = format!("h2:{label}");
                run_order.lock().expect("run order").push(value.clone());
                Ok(json!(value))
            }
        },
    );

    let trigger = bus.emit(CrossRuntimeResumeEvent {
        label: "e3".to_string(),
        ..Default::default()
    });
    let _ = block_on(trigger.wait());
    assert!(block_on(bus.wait_until_idle(Some(2.0))));

    let payload = bus.to_json_value();
    assert_eq!(payload["pending_event_queue"], json!([]));
    let event_history = payload["event_history"]
        .as_object()
        .expect("event history object");
    let event_ids: BTreeSet<_> = event_history.keys().cloned().collect();
    assert!(event_ids.contains("018f8e40-1234-7000-8000-00000000e001"));
    assert!(event_ids.contains("018f8e40-1234-7000-8000-00000000e002"));
    assert!(event_ids.contains(&trigger._inner_event().inner.lock().event_id));

    let done_one = &event_history["018f8e40-1234-7000-8000-00000000e001"];
    let done_two = &event_history["018f8e40-1234-7000-8000-00000000e002"];
    assert_eq!(done_one["event_status"], "completed");
    assert_eq!(done_two["event_status"], "completed");
    assert_eq!(
        done_one["event_results"]["handler-one"]["result"],
        json!("seeded")
    );
    assert_eq!(
        done_one["event_results"]["handler-two"]["result"],
        json!("h2:e1")
    );
    assert_eq!(
        done_two["event_results"]["handler-one"]["result"],
        json!("h1:e2")
    );
    assert_eq!(
        done_two["event_results"]["handler-two"]["result"],
        json!("h2:e2")
    );

    assert_eq!(
        *run_order.lock().expect("run order"),
        vec!["h2:e1", "h1:e2", "h2:e2", "h1:e3", "h2:e3"]
    );

    bus.destroy();
}
