use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use abxbus_rust::{
    base_event::BaseEvent,
    event_bus::{EventBus, EventBusOptions},
    typed::{EventSpec, TypedEvent},
    types::{EventConcurrencyMode, EventHandlerCompletionMode, EventHandlerConcurrencyMode},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Serialize, Deserialize)]
struct EmptyPayload {}

#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}

struct SerializableEvent;
impl EventSpec for SerializableEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "SerializableEvent";
}

fn assert_eventbus_json_roundtrip_uses_id_keyed_structures() {
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

    let handler = bus.on("SerializableEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });
    let event = bus.emit::<SerializableEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());

    let payload = bus.to_json_value();
    assert_eq!(payload["id"], "018f8e40-1234-7000-8000-000000001234");
    assert_eq!(payload["name"], "SerializableBus");
    assert_eq!(payload["max_history_size"], 500);
    assert_eq!(payload["max_history_drop"], false);
    assert_eq!(payload["event_concurrency"], "parallel");
    assert_eq!(payload["event_handler_concurrency"], "parallel");
    assert_eq!(payload["event_handler_completion"], "first");
    assert_eq!(payload["event_timeout"], serde_json::Value::Null);
    assert_eq!(payload["event_slow_timeout"], 34.0);
    assert_eq!(payload["event_handler_slow_timeout"], 12.0);
    assert_eq!(payload["event_handler_detect_file_paths"], false);
    assert_eq!(payload["handlers"].as_object().expect("handlers").len(), 1);
    assert!(payload["handlers"]
        .as_object()
        .expect("handlers")
        .contains_key(&handler.id));
    assert_eq!(
        payload["handlers_by_key"]["SerializableEvent"],
        json!([handler.id.clone()])
    );

    let event_id = event.inner.inner.lock().event_id.clone();
    assert!(payload["event_history"]
        .as_object()
        .expect("history")
        .contains_key(&event_id));
    assert_eq!(payload["pending_event_queue"], json!([]));

    let restored = EventBus::from_json_value(payload.clone());
    assert_eq!(restored.id, "018f8e40-1234-7000-8000-000000001234");
    assert_eq!(restored.name, "SerializableBus");
    assert_eq!(restored.to_json_value(), payload);
    restored.stop();
    bus.stop();
}

#[test]
fn test_eventbus_model_dump_json_roundtrip_uses_id_keyed_structures() {
    assert_eventbus_json_roundtrip_uses_id_keyed_structures();
}

#[test]
fn test_eventbus_to_json_from_json_roundtrip_uses_id_keyed_structures() {
    assert_eventbus_json_roundtrip_uses_id_keyed_structures();
}

#[test]
fn test_baseevent_model_validate_roundtrips_runtime_json_shape() {
    let bus = EventBus::new_with_options(
        Some("SerializableBaseEventBus".to_string()),
        EventBusOptions {
            event_handler_detect_file_paths: false,
            ..EventBusOptions::default()
        },
    );

    bus.on("SerializableEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });
    let event = bus.emit::<SerializableEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());

    let payload = event.inner.to_json_value();
    let restored_payload = BaseEvent::from_json_value(payload.clone()).to_json_value();
    assert_eq!(restored_payload, payload);
    bus.stop();
}

fn assert_eventbus_recreates_missing_handler_entries_from_event_result_metadata() {
    let bus = EventBus::new_with_options(
        Some("MissingHandlerHydrationBus".to_string()),
        EventBusOptions {
            event_handler_detect_file_paths: false,
            ..EventBusOptions::default()
        },
    );

    let handler = bus.on("SerializableEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });
    let event = bus.emit::<SerializableEvent>(TypedEvent::new(EmptyPayload {}));
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
        restored_payload["handlers_by_key"]["SerializableEvent"],
        json!([handler.id])
    );
    restored.stop();
    bus.stop();
}

#[test]
fn test_eventbus_validate_creates_missing_handler_entries_from_event_results() {
    assert_eventbus_recreates_missing_handler_entries_from_event_result_metadata();
}

#[test]
fn test_eventbus_from_json_recreates_missing_handler_entries_from_event_result_metadata() {
    assert_eventbus_recreates_missing_handler_entries_from_event_result_metadata();
}

fn assert_eventbus_promotes_pending_events_into_event_history() {
    let bus = EventBus::new(Some("ModelDumpPendingBus".to_string()));
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let sent_started = Arc::new(AtomicBool::new(false));
    let sent_started_for_handler = sent_started.clone();

    bus.on("SerializableEvent", "blocking_handler", move |_event| {
        let started_tx = started_tx.clone();
        let sent_started = sent_started_for_handler.clone();
        async move {
            if !sent_started.swap(true, Ordering::SeqCst) {
                let _ = started_tx.send(());
            }
            thread::sleep(Duration::from_millis(100));
            Ok(json!("ok"))
        }
    });

    let first = bus.emit::<SerializableEvent>(TypedEvent::new(EmptyPayload {}));
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("first handler should start");
    let pending = bus.emit::<SerializableEvent>(TypedEvent::new(EmptyPayload {}));

    let first_id = first.inner.inner.lock().event_id.clone();
    let pending_id = pending.inner.inner.lock().event_id.clone();
    let payload = bus.to_json_value();
    let event_history = payload["event_history"].as_object().expect("history");
    assert!(event_history.contains_key(&first_id));
    assert!(event_history.contains_key(&pending_id));
    assert_eq!(payload["pending_event_queue"], json!([pending_id]));

    block_on(first.wait_completed());
    block_on(pending.wait_completed());
    bus.stop();
}

#[test]
fn test_eventbus_model_dump_promotes_pending_events_into_event_history() {
    assert_eventbus_promotes_pending_events_into_event_history();
}

#[test]
fn test_eventbus_to_json_promotes_pending_events_into_event_history_snapshot() {
    assert_eventbus_promotes_pending_events_into_event_history();
}
