use std::{
    sync::{mpsc, Arc, Mutex},
    thread,
    time::Duration,
};

use abxbus_rust::{
    base_event::{now_iso, BaseEvent},
    event_bus::EventBus,
    event_result::EventResultStatus,
    typed::{BaseEventHandle, EventSpec},
    types::EventStatus,
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

#[derive(Clone, Serialize, Deserialize)]
struct RuntimePayload {
    data: String,
}

struct RuntimeSampleEvent;
impl EventSpec for RuntimeSampleEvent {
    type payload = RuntimePayload;
    type event_result_type = String;
    const event_type: &'static str = "RuntimeSampleEvent";
}

struct RuntimeSeededEvent;
impl EventSpec for RuntimeSeededEvent {
    type payload = RuntimePayload;
    type event_result_type = String;
    const event_type: &'static str = "RuntimeSeededEvent";
}

fn sample_event(data: &str) -> Arc<BaseEvent> {
    let mut payload = Map::new();
    payload.insert("data".to_string(), json!(data));
    BaseEvent::new("RuntimeSampleEvent", payload)
}

#[test]
fn test_event_started_at_with_deserialized_event() {
    let event = sample_event("original");
    let event_dict = event.to_json_value();

    let deserialized_event = BaseEvent::from_json_value(event_dict);
    let deserialized = deserialized_event.inner.lock();

    assert_eq!(deserialized.event_started_at, None);
    assert_eq!(deserialized.event_completed_at, None);
}

#[test]
fn test_event_started_at_with_json_deserialization() {
    let event = sample_event("json_test");
    let json_str = serde_json::to_string(&event.to_json_value()).expect("event json");
    let json_value: Value = serde_json::from_str(&json_str).expect("event json value");

    let deserialized_event = BaseEvent::from_json_value(json_value);
    let deserialized = deserialized_event.inner.lock();

    assert_eq!(deserialized.event_started_at, None);
    assert_eq!(deserialized.event_completed_at, None);
}

#[test]
fn test_event_started_at_after_processing() {
    let bus = EventBus::new(Some("RuntimeStateProcessingBus".to_string()));
    bus.on_raw("RuntimeSampleEvent", "handler", |_event| async {
        thread::sleep(Duration::from_millis(10));
        Ok(json!("done"))
    });

    let event = bus.emit(BaseEventHandle::<RuntimeSampleEvent>::new(RuntimePayload {
        data: "processing_test".to_string(),
    }));
    block_on(event.wait_completed());

    let event = event.inner.inner.lock();
    assert!(event.event_started_at.is_some());
    assert!(event.event_completed_at.is_some());
    assert_eq!(event.event_status, EventStatus::Completed);
    bus.stop();
}

#[test]
fn test_event_without_handlers_completes_and_serializes_runtime_state() {
    let event = BaseEventHandle::<RuntimeSampleEvent>::new(RuntimePayload {
        data: "no_handlers".to_string(),
    });
    let bus = EventBus::new(Some("RuntimeStateNoHandlersBus".to_string()));

    assert_eq!(event.inner.inner.lock().event_started_at, None);
    assert_eq!(event.inner.inner.lock().event_completed_at, None);

    let processed_event = bus.emit(event);
    block_on(processed_event.wait_completed());

    let processed = processed_event.inner.inner.lock();
    assert_eq!(processed.event_status, EventStatus::Completed);
    assert_eq!(processed.event_pending_bus_count, 0);
    assert!(processed.event_results.is_empty());
    assert!(processed.event_started_at.is_some());
    assert!(processed.event_completed_at.is_some());
    bus.stop();
}

#[test]
fn test_event_without_handlers() {
    test_event_without_handlers_completes_and_serializes_runtime_state();
}

#[test]
fn test_event_with_manually_set_completed_at_reconciles_through_dispatch() {
    let event = sample_event("manual");
    let bus = EventBus::new(Some("RuntimeStateManualCompletedAtBus".to_string()));
    event.inner.lock().event_completed_at = Some(now_iso());

    {
        let event = event.inner.lock();
        assert_eq!(event.event_started_at, None);
        assert_eq!(event.event_status, EventStatus::Pending);
        assert!(event.event_completed_at.is_some());
    }

    let processed_event = bus.emit_base(event);
    block_on(processed_event.wait_completed());

    {
        let processed = processed_event.inner.lock();
        assert_eq!(processed.event_status, EventStatus::Completed);
        assert!(processed.event_started_at.is_some());
        assert!(processed.event_completed_at.is_some());
    }

    let seeded_event = BaseEventHandle::<RuntimeSeededEvent>::new(RuntimePayload {
        data: "manual_seeded_result".to_string(),
    });
    let handler_entry = bus.on_raw("RuntimeSeededEvent", "handler", |_event| async {
        Ok(json!("done"))
    });
    let seeded_result = seeded_event.inner.event_result_update(
        &handler_entry,
        Some(EventResultStatus::Started),
        None,
        None,
        None,
    );
    assert_eq!(
        seeded_event.inner.inner.lock().event_status,
        EventStatus::Started
    );
    assert_eq!(seeded_event.inner.inner.lock().event_completed_at, None);
    seeded_event
        .inner
        .inner
        .lock()
        .event_results
        .get_mut(&seeded_result.handler.id)
        .expect("seeded result")
        .update(
            Some(EventResultStatus::Completed),
            Some(Some(json!("done"))),
            None,
        );
    assert_eq!(seeded_event.inner.inner.lock().event_completed_at, None);

    let reconciled = bus.emit(seeded_event);
    block_on(reconciled.wait_completed());
    let reconciled = reconciled.inner.inner.lock();
    assert_eq!(reconciled.event_status, EventStatus::Completed);
    assert!(reconciled.event_started_at.is_some());
    assert!(reconciled.event_completed_at.is_some());
    bus.stop();
}

#[test]
fn test_event_with_manually_set_completed_at() {
    test_event_with_manually_set_completed_at_reconciles_through_dispatch();
}

#[test]
fn test_event_copy_preserves_runtime_attrs() {
    let event = sample_event("copy_test");
    let copied_event = BaseEvent::from_json_value(event.to_json_value());

    assert_eq!(copied_event.inner.lock().event_started_at, None);
    assert_eq!(copied_event.inner.lock().event_completed_at, None);
}

#[test]
fn test_event_copy_preserves_private_attrs() {
    test_event_copy_preserves_runtime_attrs();
}

#[test]
fn test_event_started_at_is_serialized_and_stateful() {
    let bus = EventBus::new(Some("RuntimeStateStartedAtBus".to_string()));
    let event = BaseEventHandle::<RuntimeSampleEvent>::new(RuntimePayload {
        data: "serialize_started_at".to_string(),
    });
    let pending_payload = event.inner.to_json_value();
    assert!(pending_payload
        .as_object()
        .unwrap()
        .contains_key("event_started_at"));
    assert_eq!(pending_payload["event_started_at"], Value::Null);

    let handler_entry = bus.on_raw("RuntimeSampleEvent", "handler", |_event| async {
        Ok(json!("ok"))
    });
    event.inner.event_result_update(
        &handler_entry,
        Some(EventResultStatus::Started),
        None,
        None,
        None,
    );
    let first_started_at = event.inner.to_json_value()["event_started_at"]
        .as_str()
        .expect("started at")
        .to_string();

    let forced_started_at = "2020-01-01T00:00:00.000000000Z".to_string();
    event
        .inner
        .inner
        .lock()
        .event_results
        .get_mut(&handler_entry.id)
        .expect("handler result")
        .started_at = Some(forced_started_at.clone());

    let second_started_at = event.inner.to_json_value()["event_started_at"]
        .as_str()
        .expect("started at")
        .to_string();
    assert_eq!(second_started_at, first_started_at);
    assert_ne!(second_started_at, forced_started_at);
    bus.stop();
}

#[test]
fn test_event_result_update_started_marks_event_started_and_clears_completion() {
    let bus = EventBus::new(Some("RuntimeStateResultUpdateStartedBus".to_string()));
    let event = BaseEventHandle::<RuntimeSampleEvent>::new(RuntimePayload {
        data: "result_update_started".to_string(),
    });
    event.inner.inner.lock().event_completed_at = Some(now_iso());
    let handler_entry = bus.on_raw("RuntimeSampleEvent", "handler", |_event| async {
        Ok(json!("ok"))
    });

    let result = event.inner.event_result_update(
        &handler_entry,
        Some(EventResultStatus::Started),
        None,
        None,
        None,
    );

    assert_eq!(result.status, EventResultStatus::Started);
    let event = event.inner.inner.lock();
    assert_eq!(event.event_status, EventStatus::Started);
    assert!(event.event_started_at.is_some());
    assert_eq!(event.event_completed_at, None);
    bus.stop();
}

#[test]
fn test_event_status_is_serialized_and_stateful() {
    let bus = EventBus::new(Some("RuntimeStateStatusBus".to_string()));
    let event = BaseEventHandle::<RuntimeSampleEvent>::new(RuntimePayload {
        data: "serialize_status".to_string(),
    });
    assert_eq!(event.inner.to_json_value()["event_status"], "pending");

    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    bus.on_raw("RuntimeSampleEvent", "slow_handler", move |_event| {
        let entered_tx = entered_tx.clone();
        let release_rx = release_rx.clone();
        async move {
            let _ = entered_tx.send(());
            release_rx
                .lock()
                .expect("release lock")
                .recv_timeout(Duration::from_secs(2))
                .expect("release handler");
            Ok(json!("ok"))
        }
    });

    let processing_event = bus.emit(event);
    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("handler entered");
    assert_eq!(
        processing_event.inner.to_json_value()["event_status"],
        "started"
    );

    release_tx.send(()).expect("release send");
    block_on(processing_event.wait_completed());
    assert_eq!(
        processing_event.inner.to_json_value()["event_status"],
        "completed"
    );
    bus.stop();
}
