use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
    time::Duration,
};

use abxbus_rust::{
    base_event::{now_iso, BaseEvent, EventResultsOptions},
    event_bus::{EventBus, EventBusOptions},
    event_result::EventResultStatus,
    typed::{BaseEventHandle, EventSpec},
    types::{EventConcurrencyMode, EventHandlerConcurrencyMode, EventStatus},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

#[derive(Clone, Serialize, Deserialize)]
struct EmptyPayload {}

struct BaseEventDoneRaisesFirstErrorEvent;
impl EventSpec for BaseEventDoneRaisesFirstErrorEvent {
    type payload = EmptyPayload;
    type event_result_type = String;
    const event_type: &'static str = "BaseEventDoneRaisesFirstErrorEvent";
}

struct BaseEventEventResultUpdateEvent;
impl EventSpec for BaseEventEventResultUpdateEvent {
    type payload = EmptyPayload;
    type event_result_type = String;
    const event_type: &'static str = "BaseEventEventResultUpdateEvent";
}

struct BaseEventEventResultUpdateStatusOnlyEvent;
impl EventSpec for BaseEventEventResultUpdateStatusOnlyEvent {
    type payload = EmptyPayload;
    type event_result_type = String;
    const event_type: &'static str = "BaseEventEventResultUpdateStatusOnlyEvent";
}

struct BaseEventAllowedEventConfigEvent;
impl EventSpec for BaseEventAllowedEventConfigEvent {
    type payload = EmptyPayload;
    type event_result_type = String;
    const event_type: &'static str = "BaseEventAllowedEventConfigEvent";
    const event_timeout: Option<f64> = Some(123.0);
    const event_slow_timeout: Option<f64> = Some(9.0);
    const event_handler_timeout: Option<f64> = Some(45.0);
}

struct BaseEventImmediateParentEvent;
impl EventSpec for BaseEventImmediateParentEvent {
    type payload = EmptyPayload;
    type event_result_type = String;
    const event_type: &'static str = "BaseEventImmediateParentEvent";
}

struct BaseEventImmediateChildEvent;
impl EventSpec for BaseEventImmediateChildEvent {
    type payload = EmptyPayload;
    type event_result_type = String;
    const event_type: &'static str = "BaseEventImmediateChildEvent";
}

struct BaseEventImmediateSiblingEvent;
impl EventSpec for BaseEventImmediateSiblingEvent {
    type payload = EmptyPayload;
    type event_result_type = String;
    const event_type: &'static str = "BaseEventImmediateSiblingEvent";
}

struct BaseEventParallelImmediateParentEvent;
impl EventSpec for BaseEventParallelImmediateParentEvent {
    type payload = EmptyPayload;
    type event_result_type = String;
    const event_type: &'static str = "BaseEventParallelImmediateParentEvent";
}

struct BaseEventParallelImmediateChildEvent1;
impl EventSpec for BaseEventParallelImmediateChildEvent1 {
    type payload = EmptyPayload;
    type event_result_type = String;
    const event_type: &'static str = "BaseEventParallelImmediateChildEvent1";
}

struct BaseEventParallelImmediateChildEvent2;
impl EventSpec for BaseEventParallelImmediateChildEvent2 {
    type payload = EmptyPayload;
    type event_result_type = String;
    const event_type: &'static str = "BaseEventParallelImmediateChildEvent2";
}

struct BaseEventParallelImmediateChildEvent3;
impl EventSpec for BaseEventParallelImmediateChildEvent3 {
    type payload = EmptyPayload;
    type event_result_type = String;
    const event_type: &'static str = "BaseEventParallelImmediateChildEvent3";
}

struct BaseEventQueuedParentEvent;
impl EventSpec for BaseEventQueuedParentEvent {
    type payload = EmptyPayload;
    type event_result_type = String;
    const event_type: &'static str = "BaseEventQueuedParentEvent";
}

struct BaseEventQueuedChildEvent;
impl EventSpec for BaseEventQueuedChildEvent {
    type payload = EmptyPayload;
    type event_result_type = String;
    const event_type: &'static str = "BaseEventQueuedChildEvent";
}

struct BaseEventQueuedSiblingEvent;
impl EventSpec for BaseEventQueuedSiblingEvent {
    type payload = EmptyPayload;
    type event_result_type = String;
    const event_type: &'static str = "BaseEventQueuedSiblingEvent";
}

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

fn push(order: &Arc<Mutex<Vec<String>>>, value: &str) {
    order.lock().expect("order lock").push(value.to_string());
}

fn index_of(order: &[String], value: &str) -> usize {
    order
        .iter()
        .position(|entry| entry == value)
        .unwrap_or_else(|| panic!("missing {value} in {order:?}"))
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
fn test_event_result_re_raises_first_processing_exception_after_completion() {
    let bus = EventBus::new_with_options(
        Some("BaseEventDoneRaisesFirstErrorBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            event_timeout: None,
            ..EventBusOptions::default()
        },
    );

    bus.on_raw(
        "BaseEventDoneRaisesFirstErrorEvent",
        "first_failure",
        |_event| async {
            thread::sleep(Duration::from_millis(1));
            Err("first failure".to_string())
        },
    );
    bus.on_raw(
        "BaseEventDoneRaisesFirstErrorEvent",
        "second_failure",
        |_event| async {
            thread::sleep(Duration::from_millis(10));
            Err("second failure".to_string())
        },
    );

    let event = bus.emit(BaseEventHandle::<BaseEventDoneRaisesFirstErrorEvent>::new(
        EmptyPayload {},
    ));
    let error = block_on(event.inner.event_result(EventResultsOptions::default()))
        .expect_err("handler error should be surfaced");

    assert!(error.contains("first failure"));
    assert_eq!(
        event.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    let results = event.inner.inner.lock().event_results.clone();
    assert_eq!(results.len(), 2);
    assert!(results
        .values()
        .all(|result| result.status == EventResultStatus::Error));
    bus.stop();
}

#[test]
fn test_event_result_update_creates_and_updates_typed_handler_results() {
    let bus = EventBus::new(Some("BaseEventEventResultUpdateBus".to_string()));
    let event = BaseEventHandle::<BaseEventEventResultUpdateEvent>::new(EmptyPayload {});
    let handler_entry = bus.on_raw(
        "BaseEventEventResultUpdateEvent",
        "handler",
        |_event| async { Ok(json!("ok")) },
    );

    let pending = event.inner.event_result_update(
        &handler_entry,
        Some(EventResultStatus::Pending),
        None,
        None,
        None,
    );
    assert_eq!(
        event
            .inner
            .inner
            .lock()
            .event_results
            .get(&handler_entry.id)
            .expect("pending result")
            .id,
        pending.id
    );
    assert_eq!(pending.status, EventResultStatus::Pending);

    let completed = event.inner.event_result_update(
        &handler_entry,
        Some(EventResultStatus::Completed),
        Some(Some(json!("seeded"))),
        None,
        None,
    );
    assert_eq!(completed.id, pending.id);
    assert_eq!(completed.status, EventResultStatus::Completed);
    assert_eq!(completed.result, Some(json!("seeded")));
    assert!(completed.started_at.is_some());
    assert!(completed.completed_at.is_some());
    bus.stop();
}

#[test]
fn test_event_result_update_status_only_preserves_existing_error_and_result() {
    let bus = EventBus::new(Some("BaseEventEventResultUpdateStatusOnlyBus".to_string()));
    let event = BaseEventHandle::<BaseEventEventResultUpdateStatusOnlyEvent>::new(EmptyPayload {});
    let handler_entry = bus.on_raw(
        "BaseEventEventResultUpdateStatusOnlyEvent",
        "handler",
        |_event| async { Ok(json!("ok")) },
    );

    let errored = event.inner.event_result_update(
        &handler_entry,
        None,
        None,
        Some(Some("RuntimeError: seeded error".to_string())),
        None,
    );
    assert_eq!(errored.status, EventResultStatus::Error);
    assert_eq!(errored.error.as_deref(), Some("RuntimeError: seeded error"));

    let status_only = event.inner.event_result_update(
        &handler_entry,
        Some(EventResultStatus::Pending),
        None,
        None,
        None,
    );
    assert_eq!(status_only.status, EventResultStatus::Pending);
    assert_eq!(
        status_only.error.as_deref(),
        Some("RuntimeError: seeded error")
    );
    assert_eq!(status_only.result, None);
    bus.stop();
}

#[test]
fn test_await_event_queue_jumps_inside_handler() {
    let bus = EventBus::new_with_options(
        Some("BaseEventImmediateQueueJumpBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));

    let bus_for_parent = bus.clone();
    let order_for_parent = order.clone();
    bus.on_raw("BaseEventImmediateParentEvent", "parent", move |_event| {
        let bus = bus_for_parent.clone();
        let order = order_for_parent.clone();
        async move {
            push(&order, "parent_start");
            bus.emit(BaseEventHandle::<BaseEventImmediateSiblingEvent>::new(
                EmptyPayload {},
            ));
            let child = bus.emit_child(BaseEventHandle::<BaseEventImmediateChildEvent>::new(
                EmptyPayload {},
            ));
            child.done().await;
            push(&order, "parent_end");
            Ok(json!("parent"))
        }
    });

    let order_for_child = order.clone();
    bus.on_raw("BaseEventImmediateChildEvent", "child", move |_event| {
        let order = order_for_child.clone();
        async move {
            push(&order, "child");
            Ok(json!("child"))
        }
    });

    let order_for_sibling = order.clone();
    bus.on_raw("BaseEventImmediateSiblingEvent", "sibling", move |_event| {
        let order = order_for_sibling.clone();
        async move {
            push(&order, "sibling");
            Ok(json!("sibling"))
        }
    });

    let parent = bus.emit(BaseEventHandle::<BaseEventImmediateParentEvent>::new(
        EmptyPayload {},
    ));
    block_on(parent.done());
    block_on(bus.wait_until_idle(Some(2.0)));

    assert_eq!(
        order.lock().expect("order lock").as_slice(),
        ["parent_start", "child", "parent_end", "sibling"]
    );
    bus.stop();
}

#[test]
fn test_parallel_event_concurrency_plus_immediate_execution_races_child_events_inside_handlers() {
    let bus = EventBus::new_with_options(
        Some("BaseEventParallelImmediateRaceBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::Parallel,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));
    let release = Arc::new(AtomicBool::new(false));
    let in_flight = Arc::new(AtomicUsize::new(0));
    let max_in_flight = Arc::new(AtomicUsize::new(0));
    let (all_started_tx, all_started_rx) = mpsc::channel();

    let track_child = move |label: &'static str,
                            order: Arc<Mutex<Vec<String>>>,
                            release: Arc<AtomicBool>,
                            in_flight: Arc<AtomicUsize>,
                            max_in_flight: Arc<AtomicUsize>,
                            all_started_tx: mpsc::Sender<()>| async move {
        push(&order, &format!("{label}_start"));
        let active = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        max_in_flight.fetch_max(active, Ordering::SeqCst);
        if active == 3 {
            let _ = all_started_tx.send(());
        }
        while !release.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(1));
        }
        push(&order, &format!("{label}_end"));
        in_flight.fetch_sub(1, Ordering::SeqCst);
        Ok(json!(label))
    };

    let bus_for_parent = bus.clone();
    let order_for_parent = order.clone();
    bus.on_raw(
        "BaseEventParallelImmediateParentEvent",
        "parent",
        move |_event| {
            let bus = bus_for_parent.clone();
            let order = order_for_parent.clone();
            async move {
                push(&order, "parent_start");
                let child1 = bus.emit_child(
                    BaseEventHandle::<BaseEventParallelImmediateChildEvent1>::new(EmptyPayload {}),
                );
                let child2 = bus.emit_child(
                    BaseEventHandle::<BaseEventParallelImmediateChildEvent2>::new(EmptyPayload {}),
                );
                let child3 = bus.emit_child(
                    BaseEventHandle::<BaseEventParallelImmediateChildEvent3>::new(EmptyPayload {}),
                );
                child1.done().await;
                child2.done().await;
                child3.done().await;
                push(&order, "parent_end");
                Ok(json!("parent"))
            }
        },
    );

    for (event_type, label) in [
        ("BaseEventParallelImmediateChildEvent1", "child1"),
        ("BaseEventParallelImmediateChildEvent2", "child2"),
        ("BaseEventParallelImmediateChildEvent3", "child3"),
    ] {
        let order = order.clone();
        let release = release.clone();
        let in_flight = in_flight.clone();
        let max_in_flight = max_in_flight.clone();
        let all_started_tx = all_started_tx.clone();
        bus.on_raw(event_type, label, move |_event| {
            track_child(
                label,
                order.clone(),
                release.clone(),
                in_flight.clone(),
                max_in_flight.clone(),
                all_started_tx.clone(),
            )
        });
    }

    let parent =
        bus.emit(BaseEventHandle::<BaseEventParallelImmediateParentEvent>::new(EmptyPayload {}));
    all_started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("all child handlers should start before release");
    assert!(max_in_flight.load(Ordering::SeqCst) >= 3);
    assert!(!order
        .lock()
        .expect("order lock")
        .contains(&"parent_end".to_string()));

    release.store(true, Ordering::SeqCst);
    block_on(parent.done());
    block_on(bus.wait_until_idle(Some(2.0)));

    let order = order.lock().expect("order lock").clone();
    let parent_end_index = index_of(&order, "parent_end");
    for label in ["child1", "child2", "child3"] {
        assert!(index_of(&order, &format!("{label}_start")) < parent_end_index);
        assert!(index_of(&order, &format!("{label}_end")) < parent_end_index);
    }
    bus.stop();
}

#[test]
fn test_event_completed_waits_in_queue_order_inside_handler() {
    let bus = EventBus::new_with_options(
        Some("BaseEventQueueOrderBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::Parallel,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));

    let bus_for_parent = bus.clone();
    let order_for_parent = order.clone();
    bus.on_raw("BaseEventQueuedParentEvent", "parent", move |_event| {
        let bus = bus_for_parent.clone();
        let order = order_for_parent.clone();
        async move {
            push(&order, "parent_start");
            bus.emit(BaseEventHandle::<BaseEventQueuedSiblingEvent>::new(
                EmptyPayload {},
            ));
            let child = bus.emit_child(BaseEventHandle::<BaseEventQueuedChildEvent>::new(
                EmptyPayload {},
            ));
            child.event_completed().await;
            push(&order, "parent_end");
            Ok(json!("parent"))
        }
    });

    let order_for_child = order.clone();
    bus.on_raw("BaseEventQueuedChildEvent", "child", move |_event| {
        let order = order_for_child.clone();
        async move {
            push(&order, "child_start");
            thread::sleep(Duration::from_millis(5));
            push(&order, "child_end");
            Ok(json!("child"))
        }
    });

    let order_for_sibling = order.clone();
    bus.on_raw("BaseEventQueuedSiblingEvent", "sibling", move |_event| {
        let order = order_for_sibling.clone();
        async move {
            push(&order, "sibling_start");
            thread::sleep(Duration::from_millis(5));
            push(&order, "sibling_end");
            Ok(json!("sibling"))
        }
    });

    let parent = bus.emit(BaseEventHandle::<BaseEventQueuedParentEvent>::new(
        EmptyPayload {},
    ));
    block_on(parent.done());
    block_on(bus.wait_until_idle(Some(2.0)));

    let order = order.lock().expect("order lock").clone();
    assert!(index_of(&order, "sibling_start") < index_of(&order, "child_start"));
    assert!(index_of(&order, "child_end") < index_of(&order, "parent_end"));
    bus.stop();
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
    block_on(event.done());
}

#[test]
fn test_monotonicdatetime_emits_parseable_monotonic_iso_timestamps() {
    let first = now_iso();
    let second = now_iso();

    assert!(chrono::DateTime::parse_from_rfc3339(&first).is_ok());
    assert!(chrono::DateTime::parse_from_rfc3339(&second).is_ok());
    assert!(second >= first);
}

#[test]
fn test_monotonic_datetime_emits_parseable_monotonic_iso_timestamps() {
    test_monotonicdatetime_emits_parseable_monotonic_iso_timestamps();
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
fn test_builtin_event_prefixed_override_is_allowed() {
    let event = BaseEventHandle::<BaseEventAllowedEventConfigEvent>::new(EmptyPayload {});
    let event = event.inner.inner.lock();

    assert_eq!(event.event_timeout, Some(123.0));
    assert_eq!(event.event_slow_timeout, Some(9.0));
    assert_eq!(event.event_handler_timeout, Some(45.0));
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

#[test]
fn test_fromjson_accepts_event_parent_id_null_and_preserves_it_in_tojson_output() {
    test_from_json_accepts_event_parent_id_null_and_preserves_it_in_to_json_output();
}

#[test]
fn test_event_emitted_by_handler_id_defaults_to_null_and_accepts_null_in_fromjson() {
    test_event_emitted_by_handler_id_defaults_to_null_and_accepts_null_in_from_json();
}

#[test]
fn test_done_re_raises_the_first_processing_exception_after_completion() {
    test_event_result_re_raises_first_processing_exception_after_completion();
}

#[test]
fn test_baseevent_eventresultupdate_creates_and_updates_typed_handler_results() {
    test_event_result_update_creates_and_updates_typed_handler_results();
}

#[test]
fn test_baseevent_eventresultupdate_status_only_update_does_not_implicitly_pass_undefined_result_error_keys(
) {
    test_event_result_update_status_only_preserves_existing_error_and_result();
}

#[test]
fn test_base_event_event_result_update_status_only_update_does_not_implicitly_pass_undefined_result_error_keys(
) {
    test_event_result_update_status_only_preserves_existing_error_and_result();
}

#[test]
fn test_await_event_done_queue_jumps_child_processing_inside_handlers() {
    test_await_event_queue_jumps_inside_handler();
}

#[test]
fn test_await_event_eventcompleted_preserves_normal_queue_order_inside_handlers() {
    test_event_completed_waits_in_queue_order_inside_handler();
}

#[test]
fn test_baseevent_rejects_reserved_runtime_fields_in_payload_and_event_shape() {
    test_reserved_runtime_fields_are_rejected();
}

#[test]
fn test_baseevent_rejects_unknown_event_fields_while_allowing_known_event_overrides() {
    test_unknown_event_prefixed_field_rejected_in_payload();
    test_builtin_event_prefixed_override_is_allowed();
}

#[test]
fn test_baseevent_rejects_model_fields_in_payload_and_event_shape() {
    test_model_prefixed_field_rejected_in_payload();
}

#[test]
fn test_baseevent_tojson_fromjson_roundtrips_runtime_fields_and_event_results() {
    test_base_event_json_roundtrip();
}

#[test]
fn test_baseevent_event_at_fields_are_recognized_and_normalized() {
    test_event_at_fields_are_recognized();
}
