use abxbus_rust::event;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use abxbus_rust::{
    base_event::BaseEvent,
    event_bus::{EventBus, EventBusOptions},
    event_handler::EventHandlerOptions,
    types::{EventConcurrencyMode, EventHandlerCompletionMode, EventHandlerConcurrencyMode},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}

event! {
    struct SerializableEvent {
        event_result_type: EmptyResult,
        event_type: "SerializableEvent",
    }
}
event! {
    struct HandlerOrderEvent {
        event_result_type: EmptyResult,
        event_type: "HandlerOrderEvent",
    }
}
fn json_object_keys(value: &Value, key: &str) -> Vec<String> {
    value[key].as_object().expect(key).keys().cloned().collect()
}

fn assert_eventbus_json_roundtrip_uses_id_keyed_structures(bus_name: &str, bus_id: &str) {
    let bus = EventBus::new_with_options(
        Some(bus_name.to_string()),
        EventBusOptions {
            id: Some(bus_id.to_string()),
            max_history_size: Some(500),
            max_history_drop: false,
            event_concurrency: EventConcurrencyMode::Parallel,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            event_handler_completion: EventHandlerCompletionMode::First,
            event_timeout: Some(0.0),
            event_slow_timeout: Some(34.0),
            event_handler_slow_timeout: Some(12.0),
            event_handler_detect_file_paths: false,
            max_handler_recursion_depth: 2,
        },
    );

    let handler = bus.on_raw("SerializableEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });
    let event = bus.emit(SerializableEvent {
        ..Default::default()
    });
    let _ = block_on(event.now());

    let payload = bus.to_json_value();
    assert_eq!(payload["id"], bus_id);
    assert_eq!(payload["name"], bus_name);
    assert_eq!(payload["max_history_size"], 500);
    assert_eq!(payload["max_history_drop"], false);
    assert_eq!(payload["event_concurrency"], "parallel");
    assert_eq!(payload["event_handler_concurrency"], "parallel");
    assert_eq!(payload["event_handler_completion"], "first");
    assert_eq!(payload["event_timeout"], 0.0);
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

    let event_id = event.event_id.clone();
    assert!(payload["event_history"]
        .as_object()
        .expect("history")
        .contains_key(&event_id));
    assert_eq!(payload["pending_event_queue"], json!([]));

    let restored = EventBus::from_json_value(payload.clone());
    assert_eq!(restored.id, bus_id);
    assert_eq!(restored.name, bus_name);
    assert_eq!(restored.to_json_value(), payload);
    restored.destroy();
    bus.destroy();
}

#[test]
fn test_eventbus_to_json_from_json_roundtrip_uses_id_keyed_structures() {
    assert_eventbus_json_roundtrip_uses_id_keyed_structures(
        "SerializableBusToJson",
        "018f8e40-1234-7000-8000-000000001235",
    );
}

#[test]
fn test_eventbus_serialization_preserves_unbounded_history_null() {
    let bus = EventBus::new_with_options(
        Some("UnlimitedSerBus".to_string()),
        EventBusOptions {
            max_history_size: None,
            max_history_drop: false,
            ..EventBusOptions::default()
        },
    );
    let payload = bus.to_json_value();
    assert_eq!(payload["max_history_size"], Value::Null);

    let restored = EventBus::from_json_value(payload);
    assert_eq!(restored.to_json_value()["max_history_size"], Value::Null);
    restored.destroy();
    bus.destroy();
}

#[test]
fn test_eventbus_from_json_null_event_timeout_uses_default() {
    let bus = EventBus::new(Some("TimeoutNullBus".to_string()));
    let mut payload = bus.to_json_value();
    payload["event_timeout"] = Value::Null;

    let restored = EventBus::from_json_value(payload);
    assert_eq!(restored.to_json_value()["event_timeout"], json!(60.0));
    restored.destroy();
    bus.destroy();
}

#[test]
fn test_eventbus_from_json_defaults_missing_handler_maps() {
    let bus = EventBus::new(Some("MissingHandlerMaps".to_string()));
    let mut payload = bus.to_json_value();
    payload.as_object_mut().expect("payload").remove("handlers");
    payload
        .as_object_mut()
        .expect("payload")
        .remove("handlers_by_key");

    let restored = EventBus::from_json_value(payload);
    restored.on_raw("SerializableEvent", "handler", |_event| async move {
        Ok(json!({}))
    });
    let event = restored.emit(SerializableEvent {
        ..Default::default()
    });
    let _ = block_on(event.now());
    assert_eq!(event.event_results.read().len(), 1);
    restored.destroy();
    bus.destroy();
}

#[test]
fn test_eventbus_preserves_handler_registration_order_through_json_and_restore() {
    let bus = EventBus::new_with_options(
        Some("HandlerOrderSourceBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_handler_completion: EventHandlerCompletionMode::All,
            event_handler_detect_file_paths: false,
            ..EventBusOptions::default()
        },
    );
    let original_order = Arc::new(Mutex::new(Vec::<String>::new()));

    let order = original_order.clone();
    let first = bus.on_raw_sync_with_options(
        "HandlerOrderEvent",
        "first",
        EventHandlerOptions {
            handler_registered_at: Some("2025-01-02T03:04:05.000000000Z".to_string()),
            detect_handler_file_path: Some(false),
            ..EventHandlerOptions::default()
        },
        move |_event| {
            order.lock().expect("order").push("first".to_string());
            Ok(json!("first"))
        },
    );
    let order = original_order.clone();
    let second = bus.on_raw_sync_with_options(
        "HandlerOrderEvent",
        "second",
        EventHandlerOptions {
            handler_registered_at: Some("2025-01-02T03:04:06.000000000Z".to_string()),
            detect_handler_file_path: Some(false),
            ..EventHandlerOptions::default()
        },
        move |_event| {
            order.lock().expect("order").push("second".to_string());
            Ok(json!("second"))
        },
    );
    let expected_ids = vec![first.id.clone(), second.id.clone()];

    let payload = bus.to_json_value();
    assert_eq!(json_object_keys(&payload, "handlers"), expected_ids);
    assert_eq!(
        payload["handlers_by_key"]["HandlerOrderEvent"],
        json!(expected_ids.clone())
    );

    let event = bus.emit(HandlerOrderEvent {
        ..Default::default()
    });
    let _ = block_on(event.wait());
    assert_eq!(
        original_order.lock().expect("order").clone(),
        vec!["first".to_string(), "second".to_string()]
    );

    let restored = EventBus::from_json_value(payload);
    let restored_payload = restored.to_json_value();
    assert_eq!(
        json_object_keys(&restored_payload, "handlers"),
        expected_ids
    );
    assert_eq!(
        restored_payload["handlers_by_key"]["HandlerOrderEvent"],
        json!(expected_ids.clone())
    );

    let restored_order = Arc::new(Mutex::new(Vec::<String>::new()));
    let order = restored_order.clone();
    restored.on_raw_sync_with_options(
        "HandlerOrderEvent",
        "first",
        EventHandlerOptions {
            id: Some(first.id.clone()),
            handler_registered_at: Some(first.handler_registered_at.clone()),
            detect_handler_file_path: Some(false),
            ..EventHandlerOptions::default()
        },
        move |_event| {
            order.lock().expect("order").push("first".to_string());
            Ok(json!("first"))
        },
    );
    let order = restored_order.clone();
    restored.on_raw_sync_with_options(
        "HandlerOrderEvent",
        "second",
        EventHandlerOptions {
            id: Some(second.id.clone()),
            handler_registered_at: Some(second.handler_registered_at.clone()),
            detect_handler_file_path: Some(false),
            ..EventHandlerOptions::default()
        },
        move |_event| {
            order.lock().expect("order").push("second".to_string());
            Ok(json!("second"))
        },
    );

    let restored_event = restored.emit(HandlerOrderEvent {
        ..Default::default()
    });
    let _ = block_on(restored_event.wait());
    assert_eq!(
        restored_order.lock().expect("order").clone(),
        vec!["first".to_string(), "second".to_string()]
    );

    restored.destroy();
    bus.destroy();
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

    bus.on_raw("SerializableEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });
    let event = bus.emit(SerializableEvent {
        ..Default::default()
    });
    let _ = block_on(event.now());

    let payload = event.to_json_value();
    let restored_payload = BaseEvent::from_json_value(payload.clone()).to_json_value();
    assert_eq!(restored_payload, payload);
    bus.destroy();
}

fn assert_eventbus_recreates_missing_handler_entries_from_event_result_metadata() {
    let bus = EventBus::new_with_options(
        Some("MissingHandlerHydrationBus".to_string()),
        EventBusOptions {
            event_handler_detect_file_paths: false,
            ..EventBusOptions::default()
        },
    );

    let handler = bus.on_raw("SerializableEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });
    let event = bus.emit(SerializableEvent {
        ..Default::default()
    });
    let _ = block_on(event.now());

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
    restored.destroy();
    bus.destroy();
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

    bus.on_raw("SerializableEvent", "blocking_handler", move |_event| {
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

    let first = bus.emit(SerializableEvent {
        ..Default::default()
    });
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("first handler should start");
    let pending = bus.emit(SerializableEvent {
        ..Default::default()
    });

    let first_id = first.event_id.clone();
    let pending_id = pending.event_id.clone();
    let payload = bus.to_json_value();
    let event_history = payload["event_history"].as_object().expect("history");
    assert!(event_history.contains_key(&first_id));
    assert!(event_history.contains_key(&pending_id));
    assert_eq!(payload["pending_event_queue"], json!([pending_id]));

    let _ = block_on(first.now());
    let _ = block_on(pending.now());
    bus.destroy();
}

#[test]
fn test_eventbus_to_json_promotes_pending_events_into_event_history_snapshot() {
    assert_eventbus_promotes_pending_events_into_event_history();
}

#[test]
fn test_eventbus_from_json_preserves_event_history_object_order() {
    let bus = EventBus::new(Some("HistoryOrderBus".to_string()));
    let first = bus.emit(SerializableEvent {
        ..Default::default()
    });
    let second = bus.emit(SerializableEvent {
        ..Default::default()
    });
    let _ = block_on(first.now());
    let _ = block_on(second.now());
    let first_id = first.event_id.clone();
    let second_id = second.event_id.clone();

    let payload = bus.to_json_value();
    assert_eq!(
        json_object_keys(&payload, "event_history"),
        vec![first_id.clone(), second_id.clone()]
    );

    let restored = EventBus::from_json_value(payload);
    assert_eq!(
        json_object_keys(&restored.to_json_value(), "event_history"),
        vec![first_id, second_id]
    );
    restored.destroy();
    bus.destroy();
}
