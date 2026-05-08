use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
    thread,
    time::{Duration, Instant},
};

use abxbus_rust::{
    base_event::{BaseEvent, EventResultsOptions},
    event_bus::{EventBus, EventBusOptions},
    event_result::{EventResult, EventResultStatus},
    typed::{EventSpec, TypedEvent},
    types::{
        EventConcurrencyMode, EventHandlerCompletionMode, EventHandlerConcurrencyMode, EventStatus,
    },
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Serialize, Deserialize)]
struct EmptyPayload {}

#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}

struct LifecycleMethodInvocationEvent;
impl EventSpec for LifecycleMethodInvocationEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "LifecycleMethodInvocationEvent";
}

struct WaitForIdleTimeoutEvent;
impl EventSpec for WaitForIdleTimeoutEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "WaitForIdleTimeoutEvent";
}

fn wait_for_eventbus_weak_refs_to_drop(refs: &[Weak<EventBus>]) -> bool {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        EventBus::all_instances_len();
        if refs.iter().all(|weak_ref| weak_ref.upgrade().is_none()) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

struct UserActionEvent;
impl EventSpec for UserActionEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "UserActionEvent";
}

#[test]
fn test_eventbus_exposes_locks_api_surface() {
    let bus = EventBus::new(Some("GateSurfaceBus".to_string()));

    let mut pause = bus.locks.request_runloop_pause();
    assert!(bus.locks.is_paused());
    pause.release();
    assert!(!bus.locks.is_paused());

    assert!(bus
        .locks
        .wait_for_idle(Some(Duration::from_millis(20)), || true));

    let event = BaseEvent::new("GateSurfaceEvent", serde_json::Map::new());
    assert!(bus.locks.get_lock_for_event(&bus, &event).is_some());
    bus.stop();
}

#[test]
fn test_eventbus_locks_methods_are_callable_and_preserve_lock_resolution_behavior() {
    let bus = EventBus::new_with_options(
        Some("GateInvocationBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );

    let mut release_pause = bus.locks.request_runloop_pause();
    assert!(bus.locks.is_paused());
    let resumed = Arc::new(AtomicBool::new(false));
    let resumed_for_thread = resumed.clone();
    let locks_for_waiter = bus.locks.clone();
    let waiter = thread::spawn(move || {
        locks_for_waiter.wait_until_runloop_resumed();
        resumed_for_thread.store(true, Ordering::SeqCst);
    });
    thread::sleep(Duration::from_millis(20));
    assert!(!resumed.load(Ordering::SeqCst));
    release_pause.release();
    waiter.join().expect("pause waiter joins");
    assert!(resumed.load(Ordering::SeqCst));
    assert!(!bus.locks.is_paused());

    let event_with_global = BaseEvent::new("GateInvocationEvent", serde_json::Map::new());
    {
        let mut inner = event_with_global.inner.lock();
        inner.event_concurrency = Some(EventConcurrencyMode::GlobalSerial);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Serial);
    }
    let global_lock = bus
        .locks
        .get_lock_for_event(&bus, &event_with_global)
        .expect("global lock");
    assert!(Arc::ptr_eq(&global_lock, &EventBus::global_serial_lock()));
    let handler = bus.on("GateInvocationEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });
    let result = EventResult::new(
        event_with_global.inner.lock().event_id.clone(),
        handler.clone(),
        None,
    );
    let handler_lock = bus
        .locks
        .get_lock_for_event_handler(&bus, &event_with_global, &result)
        .expect("handler lock");
    let same_event_handler_lock = bus
        .locks
        .get_lock_for_event_handler(&bus, &event_with_global, &result)
        .expect("same handler lock");
    assert!(Arc::ptr_eq(&handler_lock, &same_event_handler_lock));

    let event_with_parallel = BaseEvent::new("GateInvocationEvent", serde_json::Map::new());
    {
        let mut inner = event_with_parallel.inner.lock();
        inner.event_concurrency = Some(EventConcurrencyMode::Parallel);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Parallel);
    }
    let parallel_result = EventResult::new(
        event_with_parallel.inner.lock().event_id.clone(),
        handler.clone(),
        None,
    );
    assert!(bus
        .locks
        .get_lock_for_event(&bus, &event_with_parallel)
        .is_none());
    assert!(bus
        .locks
        .get_lock_for_event_handler(&bus, &event_with_parallel, &parallel_result)
        .is_none());

    let another_serial_event = BaseEvent::new("GateInvocationEvent", serde_json::Map::new());
    let another_result = EventResult::new(
        another_serial_event.inner.lock().event_id.clone(),
        handler,
        None,
    );
    let another_handler_lock = bus
        .locks
        .get_lock_for_event_handler(&bus, &another_serial_event, &another_result)
        .expect("another handler lock");
    assert!(!Arc::ptr_eq(&handler_lock, &another_handler_lock));

    let emitted = bus.emit_base(BaseEvent::new(
        "GateInvocationEvent",
        serde_json::Map::new(),
    ));
    block_on(emitted.wait_completed());
    assert!(bus.locks.wait_for_idle(Some(Duration::from_secs(1)), || bus
        .is_idle_and_queue_empty()));
    bus.stop();
}

#[derive(Clone, Serialize, Deserialize)]
struct VersionPayload {
    data: String,
}

struct VersionedEvent;
impl EventSpec for VersionedEvent {
    type Payload = VersionPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "VersionedEvent";
    const EVENT_VERSION: &'static str = "1.2.3";
}

#[derive(Clone, Serialize, Deserialize)]
struct CreateAgentTaskPayload {
    user_id: String,
    agent_session_id: String,
    llm_model: String,
    task: String,
}

struct CreateAgentTaskEvent;
impl EventSpec for CreateAgentTaskEvent {
    type Payload = CreateAgentTaskPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "CreateAgentTaskEvent";
}

struct ExplicitOverrideEvent;
impl EventSpec for ExplicitOverrideEvent {
    type Payload = VersionPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "CustomEventType";
}

struct RuntimeSerializationEvent;
impl EventSpec for RuntimeSerializationEvent {
    type Payload = EmptyPayload;
    type Result = String;
    const EVENT_TYPE: &'static str = "RuntimeSerializationEvent";
}

fn object_keys(value: &Value) -> BTreeSet<String> {
    value
        .as_object()
        .expect("expected object")
        .keys()
        .cloned()
        .collect()
}

fn expected_base_event_json_keys(include_results: bool) -> BTreeSet<String> {
    let mut keys = BTreeSet::from([
        "event_completed_at".to_string(),
        "event_blocks_parent_completion".to_string(),
        "event_concurrency".to_string(),
        "event_created_at".to_string(),
        "event_emitted_by_handler_id".to_string(),
        "event_handler_completion".to_string(),
        "event_handler_concurrency".to_string(),
        "event_handler_slow_timeout".to_string(),
        "event_handler_timeout".to_string(),
        "event_id".to_string(),
        "event_parent_id".to_string(),
        "event_path".to_string(),
        "event_pending_bus_count".to_string(),
        "event_result_type".to_string(),
        "event_slow_timeout".to_string(),
        "event_started_at".to_string(),
        "event_status".to_string(),
        "event_timeout".to_string(),
        "event_type".to_string(),
        "event_version".to_string(),
    ]);
    if include_results {
        keys.insert("event_results".to_string());
    }
    keys
}

fn expected_event_handler_json_keys() -> BTreeSet<String> {
    BTreeSet::from([
        "event_pattern".to_string(),
        "eventbus_id".to_string(),
        "eventbus_name".to_string(),
        "handler_file_path".to_string(),
        "handler_name".to_string(),
        "handler_registered_at".to_string(),
        "handler_slow_timeout".to_string(),
        "handler_timeout".to_string(),
        "id".to_string(),
    ])
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

fn expected_event_bus_json_keys() -> BTreeSet<String> {
    BTreeSet::from([
        "event_concurrency".to_string(),
        "event_handler_completion".to_string(),
        "event_handler_concurrency".to_string(),
        "event_handler_detect_file_paths".to_string(),
        "event_handler_slow_timeout".to_string(),
        "event_history".to_string(),
        "event_slow_timeout".to_string(),
        "event_timeout".to_string(),
        "handlers".to_string(),
        "handlers_by_key".to_string(),
        "id".to_string(),
        "max_history_drop".to_string(),
        "max_history_size".to_string(),
        "name".to_string(),
        "pending_event_queue".to_string(),
    ])
}

fn base_event(event_type: &str, payload: Value) -> Arc<BaseEvent> {
    let Value::Object(payload) = payload else {
        panic!("test payload must be an object");
    };
    BaseEvent::new(event_type, payload)
}

#[test]
fn test_event_bus_initializes_with_correct_defaults() {
    let bus = EventBus::new(Some("DefaultsBus".to_string()));

    assert_eq!(bus.name, "DefaultsBus");
    assert_eq!(bus.max_history_size(), Some(100));
    assert!(!bus.max_history_drop());
    assert_eq!(bus.event_concurrency, EventConcurrencyMode::BusSerial);
    assert_eq!(
        bus.event_handler_concurrency,
        EventHandlerConcurrencyMode::Serial
    );
    assert_eq!(
        bus.event_handler_completion,
        EventHandlerCompletionMode::All
    );
    assert_eq!(bus.event_timeout, Some(60.0));
    assert_eq!(bus.event_history_size(), 0);
    assert!(EventBus::all_instances_contains(&bus));
    assert!(block_on(bus.wait_until_idle(None)));
    bus.stop();
}

#[test]
fn test_dispatch_returns_pending_event_with_correct_initial_state() {
    let bus = EventBus::new(Some("LifecycleBus".to_string()));
    let event = bus.emit_base(base_event("TestEvent", json!({"data": "hello"})));

    {
        let inner = event.inner.lock();
        assert_eq!(inner.event_type, "TestEvent");
        assert!(!inner.event_id.is_empty());
        assert!(!inner.event_created_at.is_empty());
        assert_eq!(inner.payload.get("data"), Some(&json!("hello")));
        assert!(inner.event_path.contains(&bus.label()));
    }

    assert!(block_on(bus.wait_until_idle(None)));
    bus.stop();
}

#[test]
fn test_event_transitions_through_pending_started_completed() {
    let bus = EventBus::new(Some("StatusBus".to_string()));
    let status_during_handler = Arc::new(Mutex::new(None));
    let status_for_handler = status_during_handler.clone();

    bus.on("StatusLifecycleEvent", "handler", move |event| {
        let status_for_handler = status_for_handler.clone();
        async move {
            *status_for_handler.lock().expect("status lock") =
                Some(event.inner.lock().event_status);
            Ok(json!("done"))
        }
    });

    let event = bus.emit_base(base_event("StatusLifecycleEvent", json!({})));
    block_on(event.event_completed());

    assert_eq!(
        *status_during_handler.lock().expect("status lock"),
        Some(EventStatus::Started)
    );
    let inner = event.inner.lock();
    assert_eq!(inner.event_status, EventStatus::Completed);
    assert!(inner.event_started_at.is_some());
    assert!(inner.event_completed_at.is_some());
    drop(inner);
    bus.stop();
}

#[test]
fn test_event_with_no_handlers_completes_immediately() {
    let bus = EventBus::new(Some("NoHandlerBus".to_string()));
    let event = bus.emit_base(base_event("OrphanEvent", json!({})));
    block_on(event.event_completed());

    let inner = event.inner.lock();
    assert_eq!(inner.event_status, EventStatus::Completed);
    assert_eq!(inner.event_results.len(), 0);
    drop(inner);
    bus.stop();
}

#[test]
fn test_auto_start_and_stop() {
    let bus = EventBus::new(Some("AutoStartStopBus".to_string()));
    assert!(!bus.is_running_for_test());

    let event = bus.emit::<UserActionEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());
    assert!(block_on(bus.wait_until_idle(Some(1.0))));
    assert!(bus.is_running_for_test());

    bus.stop();
    assert!(!bus.is_running_for_test());
    assert!(bus.is_stopped_for_test());
}

#[test]
fn test_wait_until_idle_recovers_when_idle_flag_was_cleared() {
    let bus = EventBus::new(Some("IdleRecoveryBus".to_string()));

    bus.on("UserActionEvent", "handler", |_event| async move {
        Ok(json!(null))
    });

    let event = bus.emit::<UserActionEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());
    assert!(block_on(bus.wait_until_idle(Some(1.0))));

    assert!(block_on(bus.wait_until_idle(Some(1.0))));
    bus.stop();
    bus.stop();
    assert!(bus.is_stopped_for_test());
}

#[test]
fn test_stop_with_pending_events() {
    let bus = EventBus::new(Some("StopPendingBus".to_string()));
    bus.on("*", "slow_handler", |_event| async move {
        thread::sleep(Duration::from_millis(100));
        Ok(json!("done"))
    });

    for action in 0..5 {
        bus.emit_base(base_event(
            "UserActionEvent",
            json!({"action": format!("action_{action}")}),
        ));
    }

    bus.stop();
    assert!(!bus.is_running_for_test());
    assert!(bus.is_stopped_for_test());
}

#[test]
fn test_emit_and_result() {
    let bus = EventBus::new(Some("EmitAndResultBus".to_string()));
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));

    bus.on("UserActionEvent", "user_action_handler", move |_event| {
        let started_tx = started_tx.clone();
        let release_rx = release_rx.clone();
        async move {
            let _ = started_tx.send(());
            release_rx
                .lock()
                .expect("release lock")
                .recv_timeout(Duration::from_secs(2))
                .expect("release signal");
            Ok(json!("handled"))
        }
    });

    let event = base_event(
        "UserActionEvent",
        json!({
            "action": "login",
            "user_id": "50d357df-e68c-7111-8a6c-7018569514b0"
        }),
    );
    event.inner.lock().event_timeout = Some(1.0);
    let queued = bus.emit_base(event.clone());

    assert!(Arc::ptr_eq(&queued, &event));
    {
        let inner = queued.inner.lock();
        assert_eq!(inner.event_type, "UserActionEvent");
        assert_eq!(inner.event_version, "0.0.1");
        assert_eq!(inner.payload["action"], json!("login"));
        assert_eq!(
            inner.payload["user_id"],
            json!("50d357df-e68c-7111-8a6c-7018569514b0")
        );
        assert!(!inner.event_id.is_empty());
        assert!(!inner.event_created_at.is_empty());
        assert!(inner.event_completed_at.is_none());
        assert_eq!(inner.event_timeout, Some(1.0));
    }

    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("handler should start");
    {
        let inner = queued.inner.lock();
        assert_eq!(inner.event_status, EventStatus::Started);
        assert!(inner.event_started_at.is_some());
        assert!(inner.event_completed_at.is_none());
    }

    release_tx.send(()).expect("release handler");
    block_on(queued.event_completed());

    let inner = queued.inner.lock();
    assert_eq!(inner.event_status, EventStatus::Completed);
    assert!(inner.event_started_at.is_some());
    assert!(inner.event_completed_at.is_some());
    assert_eq!(inner.event_results.len(), 1);
    drop(inner);
    assert_eq!(bus.event_history_size(), 1);
    bus.stop();
}

#[test]
fn test_dispatched_events_appear_in_event_history() {
    let bus = EventBus::new(Some("HistoryBus".to_string()));
    let event_a = bus.emit_base(base_event("EventA", json!({})));
    let event_b = bus.emit_base(base_event("EventB", json!({})));
    block_on(event_a.event_completed());
    block_on(event_b.event_completed());
    assert!(block_on(bus.wait_until_idle(None)));

    assert_eq!(bus.event_history_size(), 2);
    let history_ids = bus.event_history_ids();
    assert_eq!(history_ids.len(), 2);
    assert_eq!(history_ids[0], event_a.inner.lock().event_id);
    assert_eq!(history_ids[1], event_b.inner.lock().event_id);
    let runtime = bus.runtime_payload_for_test();
    assert_eq!(runtime[&history_ids[0]].inner.lock().event_type, "EventA");
    assert_eq!(runtime[&history_ids[1]].inner.lock().event_type, "EventB");
    bus.stop();
}

#[test]
fn test_write_ahead_log_captures_all_events() {
    let bus = EventBus::new(Some("WriteAheadLogBus".to_string()));
    bus.on("UserActionEvent", "handler", |_event| async move {
        Ok(json!("done"))
    });

    for action in 0..5 {
        bus.emit_base(base_event(
            "UserActionEvent",
            json!({"action": format!("action_{action}")}),
        ));
    }

    assert!(block_on(bus.wait_until_idle(Some(2.0))));

    let history_ids = bus.event_history_ids();
    let runtime = bus.runtime_payload_for_test();
    assert_eq!(history_ids.len(), 5);

    let mut completed = 0;
    let mut pending = 0;
    let mut started = 0;
    for (index, event_id) in history_ids.iter().enumerate() {
        let event = runtime.get(event_id).expect("history event");
        let inner = event.inner.lock();
        assert_eq!(inner.event_type, "UserActionEvent");
        assert_eq!(inner.payload["action"], json!(format!("action_{index}")));
        match inner.event_status {
            EventStatus::Completed => completed += 1,
            EventStatus::Pending => pending += 1,
            EventStatus::Started => started += 1,
        }
    }

    assert_eq!(completed + pending + started, 5);
    assert_eq!(completed, 5);
    assert_eq!(pending, 0);
    assert_eq!(started, 0);
    bus.stop();
}

#[test]
fn test_history_is_trimmed_to_max_history_size_completed_events_removed_first() {
    let bus = EventBus::new_with_options(
        Some("TrimBus".to_string()),
        EventBusOptions {
            max_history_size: Some(5),
            max_history_drop: true,
            ..EventBusOptions::default()
        },
    );
    bus.on(
        "TrimEvent",
        "handler",
        |_event| async move { Ok(json!("ok")) },
    );

    for seq in 0..10 {
        let event = bus.emit_base(base_event("TrimEvent", json!({"seq": seq})));
        block_on(event.event_completed());
    }
    assert!(block_on(bus.wait_until_idle(None)));

    assert!(bus.event_history_size() <= 5);
    let runtime = bus.runtime_payload_for_test();
    let seqs: Vec<i64> = bus
        .event_history_ids()
        .iter()
        .map(|event_id| {
            runtime[event_id].inner.lock().payload["seq"]
                .as_i64()
                .expect("seq")
        })
        .collect();
    assert!(seqs.windows(2).all(|pair| pair[1] > pair[0]));
    assert_eq!(seqs.last().copied(), Some(9));
    bus.stop();
}

#[test]
fn test_unlimited_history_max_history_size_null_keeps_all_events() {
    let bus = EventBus::new_with_options(
        Some("UnlimitedHistBus".to_string()),
        EventBusOptions {
            max_history_size: None,
            ..EventBusOptions::default()
        },
    );
    bus.on(
        "PingEvent",
        "handler",
        |_event| async move { Ok(json!("pong")) },
    );

    for _ in 0..150 {
        let event = bus.emit_base(base_event("PingEvent", json!({})));
        block_on(event.event_completed());
    }
    assert!(block_on(bus.wait_until_idle(None)));

    assert_eq!(bus.event_history_size(), 150);
    assert!(bus
        .runtime_payload_for_test()
        .values()
        .all(|event| event.inner.lock().event_status == EventStatus::Completed));
    bus.stop();
}

#[test]
fn test_max_history_drop_false_rejects_new_dispatch_when_history_is_full() {
    let bus = EventBus::new_with_options(
        Some("NoDropHistBus".to_string()),
        EventBusOptions {
            max_history_size: Some(2),
            max_history_drop: false,
            ..EventBusOptions::default()
        },
    );
    bus.on(
        "NoDropEvent",
        "handler",
        |_event| async move { Ok(json!("ok")) },
    );

    for seq in 1..=2 {
        let event = bus.emit_base(base_event("NoDropEvent", json!({"seq": seq})));
        block_on(event.event_completed());
    }

    assert_eq!(bus.event_history_size(), 2);
    let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        bus.emit_base(base_event("NoDropEvent", json!({"seq": 3})));
    }))
    .expect_err("full history should reject the third dispatch");
    let panic_message = rejected
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| rejected.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        panic_message.contains("history limit reached"),
        "{panic_message}"
    );
    assert_eq!(bus.event_history_size(), 2);
    assert_eq!(
        bus.to_json_value()["pending_event_queue"],
        json!([]),
        "rejected dispatch must not enqueue a pending event"
    );
    bus.stop();
}

#[test]
fn test_max_history_size_0_keeps_in_flight_events_and_drops_them_on_completion() {
    let bus = EventBus::new_with_options(
        Some("ZeroHistBus".to_string()),
        EventBusOptions {
            max_history_size: Some(0),
            ..EventBusOptions::default()
        },
    );
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));

    bus.on("SlowEvent", "handler", move |_event| {
        let started_tx = started_tx.clone();
        let release_rx = release_rx.clone();
        async move {
            let _ = started_tx.send(());
            release_rx
                .lock()
                .expect("release lock")
                .recv_timeout(Duration::from_secs(2))
                .expect("release signal");
            Ok(json!("ok"))
        }
    });

    let first = bus.emit_base(base_event("SlowEvent", json!({})));
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("first handler should start");
    let second = bus.emit_base(base_event("SlowEvent", json!({})));
    let first_id = first.inner.lock().event_id.clone();
    let second_id = second.inner.lock().event_id.clone();

    let runtime = bus.runtime_payload_for_test();
    assert!(runtime.contains_key(&first_id));
    assert!(runtime.contains_key(&second_id));

    release_tx.send(()).expect("release first");
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("second handler should start");
    release_tx.send(()).expect("release second");
    block_on(first.event_completed());
    block_on(second.event_completed());
    assert!(block_on(bus.wait_until_idle(None)));

    assert_eq!(bus.event_history_size(), 0);
    assert!(bus.runtime_payload_for_test().is_empty());
    bus.stop();
}

#[test]
fn test_max_history_size_0_with_max_history_drop_false_still_allows_unbounded_queueing_and_drops_completed_events(
) {
    let bus = EventBus::new_with_options(
        Some("ZeroHistNoDropBus".to_string()),
        EventBusOptions {
            max_history_size: Some(0),
            max_history_drop: false,
            ..EventBusOptions::default()
        },
    );
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));

    bus.on("BurstEvent", "handler", move |_event| {
        let started_tx = started_tx.clone();
        let release_rx = release_rx.clone();
        async move {
            let _ = started_tx.send(());
            release_rx
                .lock()
                .expect("release lock")
                .recv_timeout(Duration::from_secs(2))
                .expect("release signal");
            Ok(json!("ok"))
        }
    });

    let mut events = Vec::new();
    events.push(bus.emit_base(base_event("BurstEvent", json!({"seq": 0}))));
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("first handler should start");
    for seq in 1..25 {
        events.push(bus.emit_base(base_event("BurstEvent", json!({"seq": seq}))));
    }

    assert!(
        bus.to_json_value()["pending_event_queue"]
            .as_array()
            .expect("pending_event_queue array")
            .len()
            > 1
    );
    assert!(bus.event_history_size() >= 1);

    for _ in 0..25 {
        release_tx.send(()).expect("release event");
    }
    for event in &events {
        block_on(event.event_completed());
    }
    assert!(block_on(bus.wait_until_idle(None)));

    assert_eq!(bus.event_history_size(), 0);
    assert_eq!(bus.to_json_value()["pending_event_queue"], json!([]));
    bus.stop();
}

#[test]
fn test_handler_registration_by_string_matches_extend_name() {
    let bus = EventBus::new(Some("StringMatchBus".to_string()));
    let received = Arc::new(Mutex::new(Vec::new()));
    let received_for_handler = received.clone();

    bus.on("NamedEvent", "string_handler", move |_event| {
        let received = received_for_handler.clone();
        async move {
            received
                .lock()
                .expect("received lock")
                .push("string_handler".to_string());
            Ok(json!(null))
        }
    });

    let event = bus.emit_base(base_event("NamedEvent", json!({})));
    block_on(event.event_completed());

    assert_eq!(
        received.lock().expect("received lock").as_slice(),
        &["string_handler".to_string()]
    );
    bus.stop();
}

#[test]
fn test_class_matcher_matches_generic_base_event_by_event_type() {
    let bus = EventBus::new(Some("GenericClassMatcherBus".to_string()));
    let seen = Arc::new(Mutex::new(Vec::new()));

    for (handler_name, prefix) in [("class_handler", "class"), ("string_handler", "string")] {
        let seen = seen.clone();
        bus.on("DifferentNameFromClass", handler_name, move |event| {
            let seen = seen.clone();
            async move {
                seen.lock()
                    .expect("seen lock")
                    .push(format!("{prefix}:{}", event.inner.lock().event_type));
                Ok(json!(null))
            }
        });
    }

    let seen_for_wildcard = seen.clone();
    bus.on("*", "wildcard_handler", move |event| {
        let seen = seen_for_wildcard.clone();
        async move {
            seen.lock()
                .expect("seen lock")
                .push(format!("wildcard:{}", event.inner.lock().event_type));
            Ok(json!(null))
        }
    });

    let event = bus.emit_base(base_event("DifferentNameFromClass", json!({})));
    block_on(event.event_completed());

    assert_eq!(
        seen.lock().expect("seen lock").as_slice(),
        &[
            "class:DifferentNameFromClass".to_string(),
            "string:DifferentNameFromClass".to_string(),
            "wildcard:DifferentNameFromClass".to_string(),
        ]
    );
    assert_eq!(
        bus.to_json_value()["handlers_by_key"]["DifferentNameFromClass"]
            .as_array()
            .expect("handler ids")
            .len(),
        2
    );
    bus.stop();
}

#[test]
fn test_wildcard_handler_receives_all_events() {
    let bus = EventBus::new(Some("WildcardBus".to_string()));
    let types = Arc::new(Mutex::new(Vec::new()));
    let types_for_handler = types.clone();

    bus.on("*", "wildcard", move |event| {
        let types = types_for_handler.clone();
        async move {
            types
                .lock()
                .expect("types lock")
                .push(event.inner.lock().event_type.clone());
            Ok(json!(null))
        }
    });

    let event_a = bus.emit_base(base_event("EventA", json!({})));
    let event_b = bus.emit_base(base_event("EventB", json!({})));
    block_on(event_a.event_completed());
    block_on(event_b.event_completed());

    assert_eq!(
        types.lock().expect("types lock").as_slice(),
        &["EventA".to_string(), "EventB".to_string()]
    );
    bus.stop();
}

#[test]
fn test_wait_for_result() {
    let bus = EventBus::new(Some("WaitForResultBus".to_string()));
    let completion_order = Arc::new(Mutex::new(Vec::new()));

    let order_for_handler = completion_order.clone();
    bus.on("UserActionEvent", "slow_handler", move |_event| {
        let completion_order = order_for_handler.clone();
        async move {
            thread::sleep(Duration::from_millis(50));
            completion_order
                .lock()
                .expect("completion order lock")
                .push("handler_done".to_string());
            Ok(json!("done"))
        }
    });

    let event = bus.emit::<UserActionEvent>(TypedEvent::new(EmptyPayload {}));
    completion_order
        .lock()
        .expect("completion order lock")
        .push("enqueue_done".to_string());

    block_on(event.wait_completed());
    completion_order
        .lock()
        .expect("completion order lock")
        .push("wait_done".to_string());

    assert_eq!(
        completion_order
            .lock()
            .expect("completion order lock")
            .as_slice(),
        &[
            "enqueue_done".to_string(),
            "handler_done".to_string(),
            "wait_done".to_string()
        ]
    );
    assert!(event.inner.inner.lock().event_completed_at.is_some());
    bus.stop();
}

#[test]
fn test_error_handling() {
    let bus = EventBus::new(Some("ErrorHandlingBus".to_string()));
    let results = Arc::new(Mutex::new(Vec::new()));

    bus.on("UserActionEvent", "failing_handler", |_event| async move {
        Err("Expected to fail - testing error handling in event handlers".to_string())
    });

    let results_for_handler = results.clone();
    bus.on("UserActionEvent", "working_handler", move |_event| {
        let results = results_for_handler.clone();
        async move {
            results
                .lock()
                .expect("results lock")
                .push("success".to_string());
            Ok(json!("worked"))
        }
    });

    let event = bus.emit::<UserActionEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());
    let event_results = event.inner.inner.lock().event_results.clone();

    let failing_result = event_results
        .values()
        .find(|result| result.handler.handler_name == "failing_handler")
        .expect("failing handler result");
    assert_eq!(failing_result.status, EventResultStatus::Error);
    assert!(failing_result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("Expected to fail"));

    let working_result = event_results
        .values()
        .find(|result| result.handler.handler_name == "working_handler")
        .expect("working handler result");
    assert_eq!(working_result.status, EventResultStatus::Completed);
    assert_eq!(working_result.result, Some(json!("worked")));
    assert_eq!(
        results.lock().expect("results lock").as_slice(),
        &["success".to_string()]
    );
    bus.stop();
}

#[test]
fn test_event_result_raises_exception_group_when_multiple_handlers_fail() {
    let bus = EventBus::new(Some("EventResultMultiErrorBus".to_string()));

    bus.on(
        "UserActionEvent",
        "failing_handler_one",
        |_event| async move { Err("ValueError: first failure".to_string()) },
    );
    bus.on(
        "UserActionEvent",
        "failing_handler_two",
        |_event| async move { Err("RuntimeError: second failure".to_string()) },
    );

    let event = bus.emit::<UserActionEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());

    let error = block_on(event.inner.event_result(EventResultsOptions::default()))
        .expect_err("multiple handler errors should be raised");
    assert!(error.contains("2 handler error(s)"), "{error}");
    assert!(error.contains("ValueError: first failure"), "{error}");
    assert!(error.contains("RuntimeError: second failure"), "{error}");
    bus.stop();
}

#[test]
fn test_event_result_single_handler_error_raises_original_exception() {
    let bus = EventBus::new(Some("EventResultSingleErrorBus".to_string()));

    bus.on("UserActionEvent", "failing_handler", |_event| async move {
        Err("ValueError: single failure".to_string())
    });

    let event = bus.emit::<UserActionEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());

    let error = block_on(event.inner.event_result(EventResultsOptions::default()))
        .expect_err("single handler error should be raised");
    assert_eq!(error, "ValueError: single failure");
    bus.stop();
}

#[test]
fn test_event_results_access() {
    let bus = EventBus::new(Some("EventResultsAccessBus".to_string()));

    bus.on("TestEvent", "early_handler", |_event| async move {
        Ok(json!("early"))
    });
    bus.on("TestEvent", "late_handler", |_event| async move {
        thread::sleep(Duration::from_millis(10));
        Ok(json!("late"))
    });

    let event = bus.emit_base(base_event("TestEvent", json!({})));
    block_on(event.event_completed());
    let event_results = event.inner.lock().event_results.clone();
    assert_eq!(event_results.len(), 2);
    assert_eq!(
        event_results
            .values()
            .find(|result| result.handler.handler_name == "early_handler")
            .and_then(|result| result.result.clone()),
        Some(json!("early"))
    );
    assert_eq!(
        event_results
            .values()
            .find(|result| result.handler.handler_name == "late_handler")
            .and_then(|result| result.result.clone()),
        Some(json!("late"))
    );

    let empty_event = bus.emit_base(base_event("EmptyEvent", json!({})));
    block_on(empty_event.event_completed());
    assert_eq!(empty_event.inner.lock().event_results.len(), 0);
    bus.stop();
}

#[test]
fn test_by_handler_name() {
    let bus = EventBus::new(Some("ByHandlerNameBus".to_string()));

    bus.on("TestEvent", "process_data", |_event| async move {
        Ok(json!("version1"))
    });
    bus.on("TestEvent", "process_data", |_event| async move {
        Ok(json!("version2"))
    });
    bus.on("TestEvent", "unique_handler", |_event| async move {
        Ok(json!("unique"))
    });

    let event = bus.emit_base(base_event("TestEvent", json!({})));
    block_on(event.event_completed());
    let event_results = event.inner.lock().event_results.clone();
    let process_results: Vec<Value> = event_results
        .values()
        .filter(|result| result.handler.handler_name == "process_data")
        .filter_map(|result| result.result.clone())
        .collect();
    assert_eq!(process_results.len(), 2);
    assert!(process_results.contains(&json!("version1")));
    assert!(process_results.contains(&json!("version2")));
    assert_eq!(
        event_results
            .values()
            .find(|result| result.handler.handler_name == "unique_handler")
            .and_then(|result| result.result.clone()),
        Some(json!("unique"))
    );
    bus.stop();
}

#[test]
fn test_by_handler_id() {
    let bus = EventBus::new(Some("ByHandlerIdBus".to_string()));

    bus.on(
        "TestEvent",
        "handler",
        |_event| async move { Ok(json!("v1")) },
    );
    bus.on(
        "TestEvent",
        "handler",
        |_event| async move { Ok(json!("v2")) },
    );

    let event = bus.emit_base(base_event("TestEvent", json!({})));
    block_on(event.event_completed());
    let event_results = event.inner.lock().event_results.clone();
    let ids: BTreeSet<String> = event_results.keys().cloned().collect();
    let values: Vec<Value> = event_results
        .values()
        .filter_map(|result| result.result.clone())
        .collect();
    assert_eq!(ids.len(), 2);
    assert_eq!(event_results.len(), 2);
    assert!(values.contains(&json!("v1")));
    assert!(values.contains(&json!("v2")));
    bus.stop();
}

#[test]
fn test_string_indexing() {
    let bus = EventBus::new(Some("StringIndexingBus".to_string()));

    bus.on("TestEvent", "my_handler", |_event| async move {
        Ok(json!("my_result"))
    });

    let event = bus.emit_base(base_event("TestEvent", json!({})));
    block_on(event.event_completed());
    let event_results = event.inner.lock().event_results.clone();
    let my_handler_result = event_results
        .values()
        .find(|result| result.handler.handler_name == "my_handler");
    assert_eq!(
        my_handler_result.and_then(|result| result.result.clone()),
        Some(json!("my_result"))
    );
    let missing_result = event_results
        .values()
        .find(|result| result.handler.handler_name == "missing");
    assert!(missing_result.is_none());
    bus.stop();
}

#[test]
fn test_emit_alias_dispatches_event() {
    let bus = EventBus::new(Some("EmitAliasBus".to_string()));
    let handled_event_ids = Arc::new(Mutex::new(Vec::new()));

    let handled_for_handler = handled_event_ids.clone();
    bus.on("UserActionEvent", "user_handler", move |event| {
        let handled_event_ids = handled_for_handler.clone();
        async move {
            handled_event_ids
                .lock()
                .expect("handled ids lock")
                .push(event.inner.lock().event_id.clone());
            Ok(json!("handled"))
        }
    });

    let event = bus.emit::<UserActionEvent>(TypedEvent::new(EmptyPayload {}));
    let event_id = event.inner.inner.lock().event_id.clone();
    block_on(event.wait_completed());

    assert_eq!(
        handled_event_ids
            .lock()
            .expect("handled ids lock")
            .as_slice(),
        &[event_id.clone()]
    );
    assert_eq!(
        event.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    assert!(event.inner.inner.lock().event_path.contains(&bus.label()));
    bus.stop();
}

#[test]
fn test_handler_registration() {
    let bus = EventBus::new(Some("HandlerRegistrationBus".to_string()));
    let specific = Arc::new(Mutex::new(Vec::new()));
    let model = Arc::new(Mutex::new(Vec::new()));
    let universal = Arc::new(Mutex::new(Vec::new()));

    let specific_for_handler = specific.clone();
    bus.on("UserActionEvent", "user_handler", move |event| {
        let specific = specific_for_handler.clone();
        async move {
            specific
                .lock()
                .expect("specific lock")
                .push(event.inner.lock().payload["action"].clone());
            Ok(json!("user_handled"))
        }
    });

    let model_for_handler = model.clone();
    bus.on_typed::<RuntimeSerializationEvent, _, _>("system_handler", move |_event| {
        let model = model_for_handler.clone();
        async move {
            model
                .lock()
                .expect("model lock")
                .push("startup".to_string());
            Ok("system_handled".to_string())
        }
    });

    let universal_for_handler = universal.clone();
    bus.on("*", "universal_handler", move |event| {
        let universal = universal_for_handler.clone();
        async move {
            universal
                .lock()
                .expect("universal lock")
                .push(event.inner.lock().event_type.clone());
            Ok(json!("universal"))
        }
    });

    let user = bus.emit_base(base_event("UserActionEvent", json!({"action": "login"})));
    let system = bus.emit::<RuntimeSerializationEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(async {
        user.event_completed().await;
        system.wait_completed().await;
        assert!(bus.wait_until_idle(Some(1.0)).await);
    });

    assert_eq!(
        specific.lock().expect("specific lock").as_slice(),
        &[json!("login")]
    );
    assert_eq!(
        model.lock().expect("model lock").as_slice(),
        &["startup".to_string()]
    );
    let universal_values = universal.lock().expect("universal lock").clone();
    assert!(universal_values.contains(&"UserActionEvent".to_string()));
    assert!(universal_values.contains(&"RuntimeSerializationEvent".to_string()));
    bus.stop();
}

#[test]
fn test_event_subclass_type() {
    let bus = EventBus::new(Some("EventSubclassTypeBus".to_string()));
    let event = TypedEvent::<CreateAgentTaskEvent>::new(CreateAgentTaskPayload {
        user_id: "371bbd3c-5231-7ff0-8aef-e63732a8d40f".to_string(),
        agent_session_id: "12345678-1234-5678-1234-567812345678".to_string(),
        llm_model: "test-model".to_string(),
        task: "test task".to_string(),
    });

    let result = bus.emit(event);
    assert_eq!(result.inner.inner.lock().event_type, "CreateAgentTaskEvent");
    block_on(result.wait_completed());
    bus.stop();
}

#[test]
fn test_event_type_and_version_identity_fields() {
    let bus = EventBus::new(Some("IdentityFieldsBus".to_string()));

    let base = base_event("TestEvent", json!({}));
    assert_eq!(base.inner.lock().event_type, "TestEvent");
    assert_eq!(base.inner.lock().event_version, "0.0.1");

    let task = TypedEvent::<CreateAgentTaskEvent>::new(CreateAgentTaskPayload {
        user_id: "371bbd3c-5231-7ff0-8aef-e63732a8d40f".to_string(),
        agent_session_id: "12345678-1234-5678-1234-567812345678".to_string(),
        llm_model: "test-model".to_string(),
        task: "test task".to_string(),
    });
    assert_eq!(task.inner.inner.lock().event_type, "CreateAgentTaskEvent");
    assert_eq!(task.inner.inner.lock().event_version, "0.0.1");

    let expected_type = task.inner.inner.lock().event_type.clone();
    let expected_version = task.inner.inner.lock().event_version.clone();
    let emitted = bus.emit(task);
    assert_eq!(emitted.inner.inner.lock().event_type, expected_type);
    assert_eq!(emitted.inner.inner.lock().event_version, expected_version);
    block_on(emitted.wait_completed());
    bus.stop();
}

#[test]
fn test_event_version_defaults_and_overrides() {
    let bus = EventBus::new(Some("VersionFieldsBus".to_string()));

    let base = base_event("TestVersionEvent", json!({}));
    assert_eq!(base.inner.lock().event_version, "0.0.1");

    let class_default = TypedEvent::<VersionedEvent>::new(VersionPayload {
        data: "x".to_string(),
    });
    assert_eq!(class_default.inner.inner.lock().event_version, "1.2.3");

    let runtime_override = TypedEvent::<VersionedEvent>::new(VersionPayload {
        data: "x".to_string(),
    });
    runtime_override.inner.inner.lock().event_version = "9.9.9".to_string();
    assert_eq!(runtime_override.inner.inner.lock().event_version, "9.9.9");

    let dispatched = bus.emit(TypedEvent::<VersionedEvent>::new(VersionPayload {
        data: "queued".to_string(),
    }));
    assert_eq!(dispatched.inner.inner.lock().event_version, "1.2.3");
    block_on(dispatched.wait_completed());

    let restored = BaseEvent::from_json_value(dispatched.inner.to_json_value());
    assert_eq!(restored.inner.lock().event_version, "1.2.3");
    assert_eq!(restored.inner.lock().event_type, "VersionedEvent");
    assert_eq!(restored.inner.lock().payload["data"], json!("queued"));
    bus.stop();
}

#[test]
fn test_event_version_supports_defaults_extend_time_defaults_runtime_override_and_json_roundtrip() {
    test_event_version_defaults_and_overrides();
}

#[test]
fn test_automatic_event_type_derivation() {
    let bus = EventBus::new(Some("AutomaticEventTypeBus".to_string()));
    let received = Arc::new(Mutex::new(Vec::new()));

    let user = TypedEvent::<UserActionEvent>::new(EmptyPayload {});
    assert_eq!(user.inner.inner.lock().event_type, "UserActionEvent");
    let system = TypedEvent::<RuntimeSerializationEvent>::new(EmptyPayload {});
    assert_eq!(
        system.inner.inner.lock().event_type,
        "RuntimeSerializationEvent"
    );

    let received_for_user = received.clone();
    bus.on("UserActionEvent", "user_handler", move |event| {
        let received = received_for_user.clone();
        async move {
            received
                .lock()
                .expect("received lock")
                .push(event.inner.lock().event_type.clone());
            Ok(json!(null))
        }
    });
    let received_for_system = received.clone();
    bus.on(
        "RuntimeSerializationEvent",
        "system_handler",
        move |event| {
            let received = received_for_system.clone();
            async move {
                received
                    .lock()
                    .expect("received lock")
                    .push(event.inner.lock().event_type.clone());
                Ok(json!(null))
            }
        },
    );

    let user = bus.emit(user);
    let system = bus.emit(system);
    block_on(async {
        user.wait_completed().await;
        system.wait_completed().await;
        assert!(bus.wait_until_idle(Some(1.0)).await);
    });

    assert_eq!(
        received.lock().expect("received lock").as_slice(),
        &[
            "UserActionEvent".to_string(),
            "RuntimeSerializationEvent".to_string(),
        ]
    );
    bus.stop();
}

#[test]
fn test_event_type_is_derived_from_extend_name_argument() {
    test_automatic_event_type_derivation();
}

#[test]
fn test_explicit_event_type_override() {
    let bus = EventBus::new(Some("ExplicitEventTypeBus".to_string()));
    let received = Arc::new(Mutex::new(Vec::new()));

    let received_for_custom = received.clone();
    bus.on("CustomEventType", "custom_handler", move |event| {
        let received = received_for_custom.clone();
        async move {
            received
                .lock()
                .expect("received lock")
                .push(event.inner.lock().event_type.clone());
            Ok(json!(null))
        }
    });
    let received_for_default = received.clone();
    bus.on("ExplicitOverrideEvent", "default_handler", move |event| {
        let received = received_for_default.clone();
        async move {
            received
                .lock()
                .expect("received lock")
                .push(event.inner.lock().event_type.clone());
            Ok(json!(null))
        }
    });

    let event = TypedEvent::<ExplicitOverrideEvent>::new(VersionPayload {
        data: "test".to_string(),
    });
    assert_eq!(event.inner.inner.lock().event_type, "CustomEventType");
    let event = bus.emit(event);
    block_on(event.wait_completed());

    assert_eq!(
        received.lock().expect("received lock").as_slice(),
        &["CustomEventType".to_string()]
    );
    bus.stop();
}

#[test]
fn test_event_type_can_be_overridden_at_instantiation() {
    test_explicit_event_type_override();
}

#[test]
fn test_multiple_handlers_parallel() {
    let bus = EventBus::new_with_options(
        Some("MultipleHandlersParallelBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    let starts = Arc::new(Mutex::new(Vec::new()));
    let ends = Arc::new(Mutex::new(Vec::new()));

    for handler_name in ["slow_handler_1", "slow_handler_2"] {
        let starts = starts.clone();
        let ends = ends.clone();
        bus.on("UserActionEvent", handler_name, move |_event| {
            let starts = starts.clone();
            let ends = ends.clone();
            async move {
                starts
                    .lock()
                    .expect("starts lock")
                    .push((handler_name.to_string(), std::time::Instant::now()));
                thread::sleep(Duration::from_millis(100));
                ends.lock()
                    .expect("ends lock")
                    .push((handler_name.to_string(), std::time::Instant::now()));
                Ok(json!(handler_name))
            }
        });
    }

    let start = std::time::Instant::now();
    let event = bus.emit::<UserActionEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());
    let duration = start.elapsed();

    assert!(
        duration < Duration::from_millis(180),
        "duration={duration:?}"
    );
    assert_eq!(starts.lock().expect("starts lock").len(), 2);
    assert_eq!(ends.lock().expect("ends lock").len(), 2);
    let event_results = event.inner.inner.lock().event_results.clone();
    assert!(event_results.values().any(|result| {
        result.handler.handler_name == "slow_handler_1"
            && result.result == Some(json!("slow_handler_1"))
    }));
    assert!(event_results.values().any(|result| {
        result.handler.handler_name == "slow_handler_2"
            && result.result == Some(json!("slow_handler_2"))
    }));
    bus.stop();
}

#[test]
fn test_handler_can_be_sync_or_async() {
    let bus = EventBus::new(Some("SyncAsyncHandlersBus".to_string()));

    bus.on_sync("TestEvent", "sync_handler", |_event| Ok(json!("sync")));
    bus.on("TestEvent", "async_handler", |_event| async move {
        Ok(json!("async"))
    });

    assert_eq!(
        bus.to_json_value()["handlers_by_key"]["TestEvent"]
            .as_array()
            .expect("handler ids")
            .len(),
        2
    );

    let event = bus.emit_base(base_event("TestEvent", json!({})));
    block_on(event.event_completed());
    let results: Vec<Value> = event
        .inner
        .lock()
        .event_results
        .values()
        .filter_map(|result| result.result.clone())
        .collect();
    assert!(results.contains(&json!("sync")));
    assert!(results.contains(&json!("async")));
    bus.stop();
}

#[test]
fn test_class_and_instance_method_handlers() {
    struct EventProcessor {
        name: String,
        value: i64,
    }

    impl EventProcessor {
        fn sync_method_handler(&self, event: Arc<BaseEvent>) -> Result<Value, String> {
            Ok(json!({
                "processor": self.name,
                "value": self.value,
                "action": event.inner.lock().payload["action"].clone(),
            }))
        }

        async fn async_method_handler(&self, event: Arc<BaseEvent>) -> Result<Value, String> {
            thread::sleep(Duration::from_millis(10));
            Ok(json!({
                "processor": self.name,
                "value": self.value * 2,
                "action": event.inner.lock().payload["action"].clone(),
            }))
        }

        fn class_method_handler(_event: Arc<BaseEvent>) -> Result<Value, String> {
            Ok(json!("Handled by EventProcessor"))
        }

        fn static_method_handler(_event: Arc<BaseEvent>) -> Result<Value, String> {
            Ok(json!("Handled by static method"))
        }
    }

    let bus = EventBus::new(Some("ClassAndInstanceHandlersBus".to_string()));
    let results_seen = Arc::new(Mutex::new(Vec::new()));
    let processor1 = Arc::new(EventProcessor {
        name: "Processor1".to_string(),
        value: 10,
    });
    let processor2 = Arc::new(EventProcessor {
        name: "Processor2".to_string(),
        value: 20,
    });

    let seen = results_seen.clone();
    let processor = processor1.clone();
    bus.on_sync(
        "UserActionEvent",
        "Processor1.sync_method_handler",
        move |event| {
            seen.lock()
                .expect("results seen lock")
                .push("Processor1_sync".to_string());
            processor.sync_method_handler(event)
        },
    );

    let seen = results_seen.clone();
    let processor = processor1.clone();
    bus.on(
        "UserActionEvent",
        "Processor1.async_method_handler",
        move |event| {
            let seen = seen.clone();
            let processor = processor.clone();
            async move {
                seen.lock()
                    .expect("results seen lock")
                    .push("Processor1_async".to_string());
                processor.async_method_handler(event).await
            }
        },
    );

    let seen = results_seen.clone();
    let processor = processor2.clone();
    bus.on_sync(
        "UserActionEvent",
        "Processor2.sync_method_handler",
        move |event| {
            seen.lock()
                .expect("results seen lock")
                .push("Processor2_sync".to_string());
            processor.sync_method_handler(event)
        },
    );

    let seen = results_seen.clone();
    bus.on_sync(
        "UserActionEvent",
        "EventProcessor.class_method_handler",
        move |event| {
            seen.lock()
                .expect("results seen lock")
                .push("classmethod".to_string());
            EventProcessor::class_method_handler(event)
        },
    );

    let seen = results_seen.clone();
    bus.on_sync(
        "UserActionEvent",
        "EventProcessor.static_method_handler",
        move |event| {
            seen.lock()
                .expect("results seen lock")
                .push("staticmethod".to_string());
            EventProcessor::static_method_handler(event)
        },
    );

    let event = bus.emit_base(base_event(
        "UserActionEvent",
        json!({
            "action": "test_methods",
            "user_id": "dab45f48-9e3a-7042-80f8-ac8f07b6cfe3"
        }),
    ));
    block_on(event.event_completed());

    let seen = results_seen.lock().expect("results seen lock").clone();
    assert_eq!(seen.len(), 5);
    for expected in [
        "Processor1_sync",
        "Processor1_async",
        "Processor2_sync",
        "classmethod",
        "staticmethod",
    ] {
        assert!(seen.contains(&expected.to_string()));
    }

    let results: Vec<Value> = event
        .inner
        .lock()
        .event_results
        .values()
        .filter_map(|result| result.result.clone())
        .collect();
    assert!(results.iter().any(|result| {
        result["processor"] == "Processor1"
            && result["value"] == 10
            && result["action"] == "test_methods"
    }));
    assert!(results.iter().any(|result| {
        result["processor"] == "Processor1"
            && result["value"] == 20
            && result["action"] == "test_methods"
    }));
    assert!(results.iter().any(|result| {
        result["processor"] == "Processor2"
            && result["value"] == 20
            && result["action"] == "test_methods"
    }));
    assert!(results.contains(&json!("Handled by EventProcessor")));
    assert!(results.contains(&json!("Handled by static method")));
    bus.stop();
}

#[test]
fn test_batch_emit_with_gather() {
    let bus = EventBus::new(Some("BatchEmitBus".to_string()));

    let events = [
        bus.emit_base(base_event("UserActionEvent", json!({"action": "login"}))),
        bus.emit_base(base_event("SystemEventModel", json!({"name": "startup"}))),
        bus.emit_base(base_event("UserActionEvent", json!({"action": "logout"}))),
    ];

    for event in &events {
        block_on(event.event_completed());
    }

    assert_eq!(events.len(), 3);
    assert!(events
        .iter()
        .all(|event| event.inner.lock().event_completed_at.is_some()));
    bus.stop();
}

#[test]
fn test_concurrent_emit_calls() {
    let bus = EventBus::new(Some("ConcurrentEmitCallsBus".to_string()));
    let mut events = Vec::new();

    for index in 0..100 {
        events.push(bus.emit_base(base_event(
            "UserActionEvent",
            json!({"action": format!("concurrent_{index}")}),
        )));
    }

    for event in &events {
        block_on(event.event_completed());
    }
    assert!(block_on(bus.wait_until_idle(Some(2.0))));
    assert_eq!(bus.event_history_size(), 100);
    bus.stop();
}

fn assert_mixed_delay_handlers_maintain_order() {
    let bus = EventBus::new(Some("MixedDelayOrderBus".to_string()));
    let collected_orders = Arc::new(Mutex::new(Vec::new()));
    let handler_start_orders = Arc::new(Mutex::new(Vec::new()));

    let collected_for_handler = collected_orders.clone();
    let starts_for_handler = handler_start_orders.clone();
    bus.on("UserActionEvent", "handler", move |event| {
        let collected_orders = collected_for_handler.clone();
        let handler_start_orders = starts_for_handler.clone();
        async move {
            let order = event.inner.lock().payload["order"]
                .as_i64()
                .expect("order payload");
            handler_start_orders
                .lock()
                .expect("handler start lock")
                .push(order);
            if order % 2 == 0 {
                thread::sleep(Duration::from_millis(10));
            } else {
                thread::sleep(Duration::from_millis(2));
            }
            collected_orders
                .lock()
                .expect("collected order lock")
                .push(order);
            Ok(json!(format!("handled_{order}")))
        }
    });

    for order in 0..20 {
        bus.emit_base(base_event("UserActionEvent", json!({"order": order})));
    }
    assert!(block_on(bus.wait_until_idle(Some(3.0))));

    let expected: Vec<i64> = (0..20).collect();
    assert_eq!(
        collected_orders
            .lock()
            .expect("collected order lock")
            .as_slice(),
        expected.as_slice()
    );
    assert_eq!(
        handler_start_orders
            .lock()
            .expect("handler start lock")
            .as_slice(),
        expected.as_slice()
    );
    bus.stop();
}

#[test]
fn test_fifo_with_varying_handler_delays() {
    assert_mixed_delay_handlers_maintain_order();
}

#[test]
fn test_mixed_delay_handlers_maintain_order() {
    assert_mixed_delay_handlers_maintain_order();
}

#[test]
fn test_event_with_complex_data() {
    let bus = EventBus::new(Some("ComplexDataBus".to_string()));
    let event = bus.emit_base(base_event(
        "SystemEventModel",
        json!({
            "name": "complex",
            "details": {
                "nested": {
                    "list": [1, 2, {"inner": "value"}],
                    "none": null
                }
            }
        }),
    ));
    block_on(event.event_completed());

    assert_eq!(
        event.inner.lock().payload["details"]["nested"]["list"][2]["inner"],
        json!("value")
    );
    bus.stop();
}

#[test]
fn test_zero_history_size_keeps_inflight_and_drops_on_completion() {
    test_max_history_size_0_keeps_in_flight_events_and_drops_them_on_completion();
}

#[test]
fn test_dispatch_returns_event_results() {
    let bus = EventBus::new(Some("DispatchReturnsEventResultsBus".to_string()));

    bus.on("UserActionEvent", "test_handler", |_event| async move {
        Ok(json!({"result": "test_result"}))
    });

    let event = bus.emit::<UserActionEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());
    let all_results = block_on(
        event
            .inner
            .event_results_list(EventResultsOptions::default()),
    )
    .expect("event results list");

    assert_eq!(all_results, vec![json!({"result": "test_result"})]);

    let result_no_handlers = bus.emit_base(base_event("NoHandlersEvent", json!({})));
    block_on(result_no_handlers.event_completed());
    assert_eq!(result_no_handlers.inner.lock().event_results.len(), 0);
    bus.stop();
}

#[test]
fn test_handler() {
    let bus = EventBus::new(Some("HandlerResultBus".to_string()));

    bus.on("TestEvent", "test_handler", |_event| async move {
        Ok(json!({"result": "test_result"}))
    });

    let event = bus.emit_base(base_event("TestEvent", json!({})));
    block_on(event.event_completed());

    let all_results = block_on(event.event_results_list(EventResultsOptions::default()))
        .expect("event results list");
    assert_eq!(all_results, vec![json!({"result": "test_result"})]);

    let no_handlers = bus.emit_base(base_event("NoHandlersEvent", json!({})));
    block_on(no_handlers.event_completed());
    assert_eq!(no_handlers.inner.lock().event_results.len(), 0);
    bus.stop();
}

#[test]
fn test_event_results_indexing() {
    let bus = EventBus::new(Some("EventResultsIndexingBus".to_string()));
    let order = Arc::new(Mutex::new(Vec::new()));

    for (handler_name, value, index) in [
        ("handler1", "first", 1),
        ("handler2", "second", 2),
        ("handler3", "third", 3),
    ] {
        let order = order.clone();
        bus.on("TestEvent", handler_name, move |_event| {
            let order = order.clone();
            async move {
                order.lock().expect("order lock").push(index);
                Ok(json!(value))
            }
        });
    }

    let event = bus.emit_base(base_event("TestEvent", json!({})));
    block_on(event.event_completed());
    let event_results = event.inner.lock().event_results.clone();

    assert_eq!(
        event_results
            .values()
            .find(|result| result.handler.handler_name == "handler1")
            .and_then(|result| result.result.clone()),
        Some(json!("first"))
    );
    assert_eq!(
        event_results
            .values()
            .find(|result| result.handler.handler_name == "handler2")
            .and_then(|result| result.result.clone()),
        Some(json!("second"))
    );
    assert_eq!(
        event_results
            .values()
            .find(|result| result.handler.handler_name == "handler3")
            .and_then(|result| result.result.clone()),
        Some(json!("third"))
    );
    assert_eq!(order.lock().expect("order lock").as_slice(), &[1, 2, 3]);
    bus.stop();
}

#[test]
fn test_manual_dict_merge() {
    let bus = EventBus::new(Some("ManualDictMergeBus".to_string()));

    bus.on("GetConfig", "config_base", |_event| async move {
        Ok(json!({"debug": false, "port": 8080, "name": "base"}))
    });
    bus.on("GetConfig", "config_override", |_event| async move {
        Ok(json!({"debug": true, "timeout": 30, "name": "override"}))
    });

    let event = bus.emit_base(base_event("GetConfig", json!({})));
    block_on(event.event_completed());
    let dict_results = block_on(event.event_results_list_with_filter(
        EventResultsOptions {
            raise_if_any: false,
            raise_if_none: true,
            timeout: None,
        },
        |result| result.result.as_ref().is_some_and(Value::is_object),
    ))
    .expect("dict results");
    let mut merged = serde_json::Map::new();
    for result in dict_results {
        merged.extend(result.as_object().expect("dict result").clone());
    }
    assert_eq!(
        Value::Object(merged),
        json!({"debug": true, "port": 8080, "timeout": 30, "name": "override"})
    );

    bus.on("BadConfig", "bad_handler", |_event| async move {
        Ok(json!("not a dict"))
    });
    let event_bad = bus.emit_base(base_event("BadConfig", json!({})));
    block_on(event_bad.event_completed());
    let merged_bad = block_on(event_bad.event_results_list_with_filter(
        EventResultsOptions {
            raise_if_any: false,
            raise_if_none: false,
            timeout: None,
        },
        |result| result.result.as_ref().is_some_and(Value::is_object),
    ))
    .expect("empty dict results");
    assert!(merged_bad.is_empty());
    bus.stop();
}

#[test]
fn test_manual_dict_merge_conflicts_last_write_wins() {
    let bus = EventBus::new(Some("ManualDictConflictBus".to_string()));

    bus.on("ConflictEvent", "handler_one", |_event| async move {
        Ok(json!({"shared": 1, "unique1": "a"}))
    });
    bus.on("ConflictEvent", "handler_two", |_event| async move {
        Ok(json!({"shared": 2, "unique2": "b"}))
    });

    let event = bus.emit_base(base_event("ConflictEvent", json!({})));
    block_on(event.event_completed());
    let dict_results = block_on(event.event_results_list_with_filter(
        EventResultsOptions {
            raise_if_any: false,
            raise_if_none: true,
            timeout: None,
        },
        |result| result.result.as_ref().is_some_and(Value::is_object),
    ))
    .expect("dict results");
    let mut merged = serde_json::Map::new();
    for result in dict_results {
        merged.extend(result.as_object().expect("dict result").clone());
    }

    assert_eq!(merged.get("shared"), Some(&json!(2)));
    assert_eq!(merged.get("unique1"), Some(&json!("a")));
    assert_eq!(merged.get("unique2"), Some(&json!("b")));
    bus.stop();
}

#[test]
fn test_manual_list_flatten() {
    let bus = EventBus::new(Some("ManualListFlattenBus".to_string()));

    bus.on("GetErrors", "errors1", |_event| async move {
        Ok(json!(["error1", "error2"]))
    });
    bus.on("GetErrors", "errors2", |_event| async move {
        Ok(json!(["error3"]))
    });
    bus.on("GetErrors", "errors3", |_event| async move {
        Ok(json!(["error4", "error5"]))
    });

    let event = bus.emit_base(base_event("GetErrors", json!({})));
    block_on(event.event_completed());
    let list_results = block_on(event.event_results_list_with_filter(
        EventResultsOptions {
            raise_if_any: false,
            raise_if_none: true,
            timeout: None,
        },
        |result| result.result.as_ref().is_some_and(Value::is_array),
    ))
    .expect("list results");
    let flattened: Vec<Value> = list_results
        .iter()
        .flat_map(|result| result.as_array().expect("list result").iter().cloned())
        .collect();
    assert_eq!(
        flattened,
        vec![
            json!("error1"),
            json!("error2"),
            json!("error3"),
            json!("error4"),
            json!("error5")
        ]
    );

    bus.on("GetSingle", "single_value", |_event| async move {
        Ok(json!("single"))
    });
    let event_single = bus.emit_base(base_event("GetSingle", json!({})));
    block_on(event_single.event_completed());
    let single_lists = block_on(event_single.event_results_list_with_filter(
        EventResultsOptions {
            raise_if_any: false,
            raise_if_none: false,
            timeout: None,
        },
        |result| result.result.as_ref().is_some_and(Value::is_array),
    ))
    .expect("empty list results");
    assert!(single_lists.is_empty());
    bus.stop();
}

#[test]
fn test_by_handler_name_access() {
    let bus = EventBus::new(Some("ByHandlerNameAccessBus".to_string()));

    bus.on("TestEvent", "handler_a", |_event| async move {
        Ok(json!("result_a"))
    });
    bus.on("TestEvent", "handler_b", |_event| async move {
        Ok(json!("result_b"))
    });

    let event = bus.emit_base(base_event("TestEvent", json!({})));
    block_on(event.event_completed());
    let event_results = event.inner.lock().event_results.clone();

    assert_eq!(
        event_results
            .values()
            .find(|result| result.handler.handler_name == "handler_a")
            .and_then(|result| result.result.clone()),
        Some(json!("result_a"))
    );
    assert_eq!(
        event_results
            .values()
            .find(|result| result.handler.handler_name == "handler_b")
            .and_then(|result| result.result.clone()),
        Some(json!("result_b"))
    );
    bus.stop();
}

#[test]
fn test_forwarding_flattens_results() {
    let bus1 = EventBus::new(Some("Bus1".to_string()));
    let bus2 = EventBus::new(Some("Bus2".to_string()));
    let bus3 = EventBus::new(Some("Bus3".to_string()));
    let execution_order = Arc::new(Mutex::new(Vec::new()));

    let order = execution_order.clone();
    bus1.on("TestEvent", "bus1_handler", move |_event| {
        let order = order.clone();
        async move {
            order.lock().expect("order lock").push("bus1".to_string());
            Ok(json!("from_bus1"))
        }
    });
    let order = execution_order.clone();
    bus2.on("TestEvent", "bus2_handler", move |_event| {
        let order = order.clone();
        async move {
            order.lock().expect("order lock").push("bus2".to_string());
            Ok(json!("from_bus2"))
        }
    });
    let order = execution_order.clone();
    bus3.on("TestEvent", "bus3_handler", move |_event| {
        let order = order.clone();
        async move {
            order.lock().expect("order lock").push("bus3".to_string());
            Ok(json!("from_bus3"))
        }
    });

    let bus2_for_forward = bus2.clone();
    bus1.on("*", "forward_to_bus2", move |event| {
        let bus2 = bus2_for_forward.clone();
        async move {
            bus2.emit_base(event);
            Ok(json!(null))
        }
    });
    let bus3_for_forward = bus3.clone();
    bus2.on("*", "forward_to_bus3", move |event| {
        let bus3 = bus3_for_forward.clone();
        async move {
            bus3.emit_base(event);
            Ok(json!(null))
        }
    });

    let event = bus1.emit_base(base_event("TestEvent", json!({})));
    block_on(event.event_completed());
    block_on(bus1.wait_until_idle(None));
    block_on(bus2.wait_until_idle(None));
    block_on(bus3.wait_until_idle(None));

    let event_results = event.inner.lock().event_results.clone();
    for (handler_name, expected_result) in [
        ("bus1_handler", json!("from_bus1")),
        ("bus2_handler", json!("from_bus2")),
        ("bus3_handler", json!("from_bus3")),
    ] {
        let result = event_results
            .values()
            .find(|result| result.handler.handler_name == handler_name)
            .expect("forwarded handler result");
        assert_eq!(result.status, EventResultStatus::Completed);
        assert_eq!(result.result, Some(expected_result));
    }
    assert_eq!(
        execution_order.lock().expect("order lock").as_slice(),
        &["bus1".to_string(), "bus2".to_string(), "bus3".to_string()]
    );
    assert_eq!(
        event.inner.lock().event_path,
        vec![bus1.label(), bus2.label(), bus3.label()]
    );
    bus1.stop();
    bus2.stop();
    bus3.stop();
}

#[test]
fn test_by_eventbus_id_and_path() {
    let bus1 = EventBus::new(Some("MainBus".to_string()));
    let bus2 = EventBus::new(Some("PluginBus".to_string()));

    bus1.on("DataEvent", "main_handler", |_event| async move {
        Ok(json!("main_result"))
    });
    bus2.on("DataEvent", "plugin_handler1", |_event| async move {
        Ok(json!("plugin_result1"))
    });
    bus2.on("DataEvent", "plugin_handler2", |_event| async move {
        Ok(json!("plugin_result2"))
    });

    let bus2_for_forward = bus2.clone();
    bus1.on("*", "forward_to_plugin", move |event| {
        let bus2 = bus2_for_forward.clone();
        async move {
            bus2.emit_base(event);
            Ok(json!(null))
        }
    });

    let event = bus1.emit_base(base_event("DataEvent", json!({})));
    block_on(event.event_completed());
    block_on(bus1.wait_until_idle(None));
    block_on(bus2.wait_until_idle(None));

    let event_results = event.inner.lock().event_results.clone();
    let main_results: Vec<_> = event_results
        .values()
        .filter(|result| {
            result.handler.eventbus_id == bus1.id
                && result.result.as_ref().is_some_and(|value| !value.is_null())
        })
        .collect();
    let plugin_results: Vec<_> = event_results
        .values()
        .filter(|result| {
            result.handler.eventbus_id == bus2.id
                && result.result.as_ref().is_some_and(|value| !value.is_null())
        })
        .collect();

    assert_eq!(main_results.len(), 1);
    assert_eq!(main_results[0].result, Some(json!("main_result")));
    assert_eq!(plugin_results.len(), 2);
    assert!(plugin_results
        .iter()
        .any(|result| result.result == Some(json!("plugin_result1"))));
    assert!(plugin_results
        .iter()
        .any(|result| result.result == Some(json!("plugin_result2"))));
    assert_eq!(
        event.inner.lock().event_path,
        vec![bus1.label(), bus2.label()]
    );
    bus1.stop();
    bus2.stop();
}

#[test]
fn test_complex_multi_bus_scenario() {
    let app_bus = EventBus::new(Some("AppBus".to_string()));
    let auth_bus = EventBus::new(Some("AuthBus".to_string()));
    let data_bus = EventBus::new(Some("DataBus".to_string()));

    app_bus.on("ValidationRequest", "validate", |_event| async move {
        Ok(json!({"app_valid": true, "timestamp": 1000}))
    });
    auth_bus.on("ValidationRequest", "validate", |_event| async move {
        Ok(json!({"auth_valid": true, "user": "alice"}))
    });
    auth_bus.on("ValidationRequest", "process", |_event| async move {
        Ok(json!(["auth_log_1", "auth_log_2"]))
    });
    data_bus.on("ValidationRequest", "validate", |_event| async move {
        Ok(json!({"data_valid": true, "schema": "v2"}))
    });
    data_bus.on("ValidationRequest", "process", |_event| async move {
        Ok(json!(["data_log_1", "data_log_2", "data_log_3"]))
    });

    let auth_for_forward = auth_bus.clone();
    app_bus.on("*", "forward_to_auth", move |event| {
        let auth_bus = auth_for_forward.clone();
        async move {
            auth_bus.emit_base(event);
            Ok(json!(null))
        }
    });
    let data_for_forward = data_bus.clone();
    auth_bus.on("*", "forward_to_data", move |event| {
        let data_bus = data_for_forward.clone();
        async move {
            data_bus.emit_base(event);
            Ok(json!(null))
        }
    });

    let event = app_bus.emit_base(base_event("ValidationRequest", json!({})));
    block_on(event.event_completed());
    block_on(app_bus.wait_until_idle(None));
    block_on(auth_bus.wait_until_idle(None));
    block_on(data_bus.wait_until_idle(None));

    let results = event.inner.lock().event_results.clone();
    let validate_results: Vec<_> = results
        .values()
        .filter(|result| result.handler.handler_name == "validate")
        .collect();
    let process_results: Vec<_> = results
        .values()
        .filter(|result| result.handler.handler_name == "process")
        .collect();
    assert_eq!(validate_results.len(), 3);
    assert_eq!(process_results.len(), 2);
    assert_eq!(
        event.inner.lock().event_path,
        vec![app_bus.label(), auth_bus.label(), data_bus.label()]
    );

    let dict_results = block_on(event.event_results_list_with_filter(
        EventResultsOptions {
            raise_if_any: false,
            raise_if_none: true,
            timeout: None,
        },
        |result| result.result.as_ref().is_some_and(Value::is_object),
    ))
    .expect("dict results");
    let mut merged = serde_json::Map::new();
    for result in dict_results {
        merged.extend(result.as_object().expect("dict result").clone());
    }
    assert_eq!(merged.get("app_valid"), Some(&json!(true)));
    assert_eq!(merged.get("auth_valid"), Some(&json!(true)));
    assert_eq!(merged.get("data_valid"), Some(&json!(true)));

    let list_results = block_on(event.event_results_list_with_filter(
        EventResultsOptions {
            raise_if_any: false,
            raise_if_none: true,
            timeout: None,
        },
        |result| result.result.as_ref().is_some_and(Value::is_array),
    ))
    .expect("list results");
    let flattened: Vec<Value> = list_results
        .iter()
        .flat_map(|result| result.as_array().expect("list result").iter().cloned())
        .collect();
    assert_eq!(
        flattened,
        vec![
            json!("auth_log_1"),
            json!("auth_log_2"),
            json!("data_log_1"),
            json!("data_log_2"),
            json!("data_log_3")
        ]
    );
    app_bus.stop();
    auth_bus.stop();
    data_bus.stop();
}

#[test]
fn test_event_result_type_enforcement_with_dict() {
    let bus = EventBus::new(Some("DictResultTypeBus".to_string()));

    for (handler_name, value) in [
        ("dict_handler1", json!({"key1": "value1"})),
        ("dict_handler2", json!({"key2": "value2"})),
        ("string_handler", json!("this is a string, not a dict")),
        ("int_handler", json!(42)),
        ("list_handler", json!([1, 2, 3])),
    ] {
        bus.on("DictResultEvent", handler_name, move |_event| {
            let value = value.clone();
            async move { Ok(value) }
        });
    }

    let event = base_event("DictResultEvent", json!({}));
    event.inner.lock().event_result_type = Some(json!({"type": "object"}));
    let event = bus.emit_base(event);
    block_on(event.event_completed());
    let event_results = event.inner.lock().event_results.clone();

    for handler_name in ["dict_handler1", "dict_handler2"] {
        let result = event_results
            .values()
            .find(|result| result.handler.handler_name == handler_name)
            .expect("dict handler result");
        assert_eq!(result.status, EventResultStatus::Completed);
        assert!(result.result.as_ref().is_some_and(Value::is_object));
    }
    for handler_name in ["string_handler", "int_handler", "list_handler"] {
        let result = event_results
            .values()
            .find(|result| result.handler.handler_name == handler_name)
            .expect("wrong type handler result");
        assert_eq!(result.status, EventResultStatus::Error);
        let error = result.error.as_deref().unwrap_or_default();
        assert!(error.contains("did not match event_result_type"), "{error}");
        assert!(error.contains("expected object"), "{error}");
    }
    bus.stop();
}

#[test]
fn test_event_result_type_enforcement_with_list() {
    let bus = EventBus::new(Some("ListResultTypeBus".to_string()));

    for (handler_name, value) in [
        ("list_handler1", json!([1, 2, 3])),
        ("list_handler2", json!(["a", "b", "c"])),
        ("dict_handler", json!({"key": "value"})),
        ("string_handler", json!("not a list")),
        ("int_handler", json!(99)),
    ] {
        bus.on("ListResultEvent", handler_name, move |_event| {
            let value = value.clone();
            async move { Ok(value) }
        });
    }

    let event = base_event("ListResultEvent", json!({}));
    event.inner.lock().event_result_type = Some(json!({"type": "array"}));
    let event = bus.emit_base(event);
    block_on(event.event_completed());
    let event_results = event.inner.lock().event_results.clone();

    for handler_name in ["list_handler1", "list_handler2"] {
        let result = event_results
            .values()
            .find(|result| result.handler.handler_name == handler_name)
            .expect("list handler result");
        assert_eq!(result.status, EventResultStatus::Completed);
        assert!(result.result.as_ref().is_some_and(Value::is_array));
    }
    for handler_name in ["dict_handler", "string_handler", "int_handler"] {
        let result = event_results
            .values()
            .find(|result| result.handler.handler_name == handler_name)
            .expect("wrong type handler result");
        assert_eq!(result.status, EventResultStatus::Error);
        let error = result.error.as_deref().unwrap_or_default();
        assert!(error.contains("did not match event_result_type"), "{error}");
        assert!(error.contains("expected array"), "{error}");
    }

    let list_results = block_on(event.event_results_list_with_filter(
        EventResultsOptions {
            raise_if_any: false,
            raise_if_none: false,
            timeout: None,
        },
        |result| result.result.as_ref().is_some_and(Value::is_array),
    ))
    .expect("list results");
    let flattened: Vec<Value> = list_results
        .iter()
        .flat_map(|result| result.as_array().expect("list result").iter().cloned())
        .collect();
    assert_eq!(
        flattened,
        vec![
            json!(1),
            json!(2),
            json!(3),
            json!("a"),
            json!("b"),
            json!("c")
        ]
    );
    bus.stop();
}

#[test]
fn test_handler_error_is_captured_without_crashing_the_bus() {
    let bus = EventBus::new(Some("ErrorBus".to_string()));
    bus.on("ErrorEvent", "throws", |_event| async move {
        Err("handler blew up".to_string())
    });

    let event = bus.emit_base(base_event("ErrorEvent", json!({})));
    block_on(event.event_completed());

    assert_eq!(event.inner.lock().event_status, EventStatus::Completed);
    let errors = event.event_errors();
    assert_eq!(errors.len(), 1);
    let result = event
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("result");
    assert_eq!(result.status, EventResultStatus::Error);
    assert!(result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("handler blew up"));
    bus.stop();
}

#[test]
fn test_one_handler_error_does_not_prevent_other_handlers_from_running() {
    let bus = EventBus::new(Some("ErrorIsolationBus".to_string()));
    let second_ran = Arc::new(AtomicBool::new(false));
    let second_ran_for_handler = second_ran.clone();

    bus.on("ErrorIsolationEvent", "bad", |_event| async move {
        Err("bad handler".to_string())
    });
    bus.on("ErrorIsolationEvent", "good", move |_event| {
        let second_ran = second_ran_for_handler.clone();
        async move {
            second_ran.store(true, Ordering::SeqCst);
            Ok(json!("ok"))
        }
    });

    let event = bus.emit_base(base_event("ErrorIsolationEvent", json!({})));
    block_on(event.event_completed());

    assert!(second_ran.load(Ordering::SeqCst));
    let results = event.inner.lock().event_results.clone();
    assert_eq!(results.len(), 2);
    assert!(results
        .values()
        .any(|result| result.status == EventResultStatus::Error));
    assert!(results
        .values()
        .any(|result| result.status == EventResultStatus::Completed));
    bus.stop();
}

#[test]
fn test_many_events_dispatched_concurrently_all_complete() {
    let bus = EventBus::new_with_options(
        Some("ConcurrentDispatchBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    let count = Arc::new(AtomicUsize::new(0));
    let count_for_handler = count.clone();
    bus.on("ConcurrentEvent", "handler", move |_event| {
        let count = count_for_handler.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(json!("ok"))
        }
    });

    let mut joins = Vec::new();
    for i in 0..25 {
        let bus = bus.clone();
        joins.push(thread::spawn(move || {
            bus.emit_base(base_event("ConcurrentEvent", json!({"seq": i})))
        }));
    }
    let events: Vec<_> = joins
        .into_iter()
        .map(|join| join.join().expect("emit thread"))
        .collect();
    for event in &events {
        block_on(event.event_completed());
        assert_eq!(event.inner.lock().event_status, EventStatus::Completed);
    }
    assert_eq!(count.load(Ordering::SeqCst), 25);
    bus.stop();
}

#[test]
fn test_dispatch_leaves_event_timeout_unset_and_processing_uses_bus_timeout_default() {
    let bus = EventBus::new_with_options(
        Some("TimeoutDefaultDispatchBus".to_string()),
        EventBusOptions {
            event_timeout: Some(10.0),
            ..EventBusOptions::default()
        },
    );
    bus.on("TimeoutDefaultEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });

    let event = base_event("TimeoutDefaultEvent", json!({}));
    assert_eq!(event.inner.lock().event_timeout, None);
    let event = bus.emit_base(event);
    assert_eq!(event.inner.lock().event_timeout, None);
    block_on(event.event_completed());
    let result = event
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("result");
    assert_eq!(result.timeout, Some(10.0));
    bus.stop();
}

#[test]
fn test_event_with_explicit_timeout_is_not_overridden_by_bus_default() {
    let bus = EventBus::new_with_options(
        Some("ExplicitTimeoutDispatchBus".to_string()),
        EventBusOptions {
            event_timeout: Some(10.0),
            ..EventBusOptions::default()
        },
    );
    bus.on("ExplicitTimeoutEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });

    let event = base_event("ExplicitTimeoutEvent", json!({}));
    event.inner.lock().event_timeout = Some(2.0);
    let event = bus.emit_base(event);
    block_on(event.event_completed());
    let result = event
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("result");
    assert_eq!(event.inner.lock().event_timeout, Some(2.0));
    assert_eq!(result.timeout, Some(2.0));
    bus.stop();
}

#[test]
fn test_eventbus_all_instances_tracks_all_created_buses() {
    let bus_a = EventBus::new(Some("AllInstancesBusA".to_string()));
    let bus_b = EventBus::new(Some("AllInstancesBusB".to_string()));

    assert!(EventBus::all_instances_contains(&bus_a));
    assert!(EventBus::all_instances_contains(&bus_b));
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_unreferenced_eventbus_can_be_garbage_collected_not_retained_by_all_instances() {
    let (bus_id, weak_ref) = {
        let bus = EventBus::new(Some("GCTestBus".to_string()));
        let bus_id = bus.id.clone();
        let weak_ref = Arc::downgrade(&bus);
        assert!(EventBus::all_instances_contains(&bus));
        assert!(EventBus::live_instance_by_id(&bus_id).is_some());
        (bus_id, weak_ref)
    };

    assert!(
        wait_for_eventbus_weak_refs_to_drop(&[weak_ref]),
        "all_instances must not hold a strong reference to an unreferenced bus"
    );
    assert!(
        EventBus::live_instance_by_id(&bus_id).is_none(),
        "dead EventBus weak refs should be purged from all_instances"
    );
}

#[test]
fn test_unreferenced_buses_with_event_history_are_garbage_collected_without_destroy() {
    let mut refs = Vec::new();
    let mut bus_ids = Vec::new();

    for index in 0..5 {
        let bus = EventBus::new_with_options(
            Some(format!("GCNoDestroyBus{index}")),
            EventBusOptions {
                max_history_size: Some(20),
                ..EventBusOptions::default()
            },
        );
        bus.on("UserActionEvent", "history_handler", |_event| async move {
            Ok(json!("ok"))
        });
        for _ in 0..10 {
            let event = bus.emit::<UserActionEvent>(TypedEvent::new(EmptyPayload {}));
            block_on(event.wait_completed());
        }
        block_on(bus.wait_until_idle(Some(2.0)));
        assert_eq!(bus.event_history_size(), 10);
        bus_ids.push(bus.id.clone());
        refs.push(Arc::downgrade(&bus));
    }

    assert!(
        wait_for_eventbus_weak_refs_to_drop(&refs),
        "all_instances must not retain buses after their last Arc handle is dropped"
    );
    assert!(
        bus_ids
            .iter()
            .all(|bus_id| EventBus::live_instance_by_id(bus_id).is_none()),
        "dead EventBus weak refs should be purged after history-bearing buses are dropped"
    );
}

#[test]
fn test_reset_creates_a_fresh_pending_event_for_cross_bus_dispatch() {
    let bus_a = EventBus::new(Some("ResetBusA".to_string()));
    let bus_b = EventBus::new(Some("ResetBusB".to_string()));
    bus_a.on(
        "ResetEvent",
        "handler_a",
        |_event| async move { Ok(json!("a")) },
    );
    bus_b.on(
        "ResetEvent",
        "handler_b",
        |_event| async move { Ok(json!("b")) },
    );

    let completed = bus_a.emit_base(base_event("ResetEvent", json!({"label": "hello"})));
    block_on(completed.event_completed());
    assert_eq!(completed.inner.lock().event_status, EventStatus::Completed);
    assert_eq!(completed.inner.lock().event_results.len(), 1);

    let fresh = completed.event_reset();
    assert_ne!(fresh.inner.lock().event_id, completed.inner.lock().event_id);
    assert_eq!(fresh.inner.lock().event_status, EventStatus::Pending);
    assert!(fresh.inner.lock().event_started_at.is_none());
    assert!(fresh.inner.lock().event_completed_at.is_none());
    assert_eq!(fresh.inner.lock().event_results.len(), 0);

    let forwarded = bus_b.emit_base(fresh);
    block_on(forwarded.event_completed());
    assert_eq!(forwarded.inner.lock().event_status, EventStatus::Completed);
    assert_eq!(forwarded.inner.lock().event_results.len(), 1);
    let event_path = forwarded.inner.lock().event_path.clone();
    assert!(event_path.iter().any(|path| path.starts_with("ResetBusA#")));
    assert!(event_path.iter().any(|path| path.starts_with("ResetBusB#")));
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_max_history_size_0_prunes_previously_completed_events_on_later_dispatch() {
    let bus = EventBus::new_with_options(
        Some("ZeroHistPruneBus".to_string()),
        EventBusOptions {
            max_history_size: Some(0),
            ..EventBusOptions::default()
        },
    );
    bus.on("ZeroHistPruneEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });

    let first = bus.emit_base(base_event("ZeroHistPruneEvent", json!({"seq": 1})));
    block_on(first.event_completed());
    assert_eq!(bus.event_history_size(), 0);

    let second = bus.emit_base(base_event("ZeroHistPruneEvent", json!({"seq": 2})));
    block_on(second.event_completed());
    assert_eq!(bus.event_history_size(), 0);
    assert!(bus.runtime_payload_for_test().is_empty());
    bus.stop();
}

#[test]
fn test_base_event_to_json_from_json_roundtrips_runtime_fields_and_event_results() {
    let bus = EventBus::new(Some("RuntimeSerializationBus".to_string()));
    bus.on(
        "RuntimeSerializationEvent",
        "returns_ok",
        |_event| async move { Ok(json!("ok")) },
    );

    let event = bus.emit::<RuntimeSerializationEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());

    let serialized = event.inner.to_json_value();
    assert_eq!(
        serde_json::to_value(&*event.inner).expect("base event serde"),
        serialized
    );
    assert_eq!(
        object_keys(&serialized),
        expected_base_event_json_keys(true)
    );
    assert_eq!(serialized["event_status"], "completed");
    assert!(serialized["event_created_at"].is_string());
    assert!(serialized["event_started_at"].is_string());
    assert!(serialized["event_completed_at"].is_string());
    assert_eq!(serialized["event_pending_bus_count"], 0);
    assert!(serialized["event_results"].is_object());

    let json_results = serialized["event_results"].as_object().expect("object");
    assert_eq!(json_results.len(), 1);
    let handler_id = event
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .expect("event result")
        .handler
        .id
        .clone();
    let json_result = json_results.get(&handler_id).expect("handler keyed result");
    assert_eq!(object_keys(json_result), expected_event_result_json_keys());
    assert_eq!(json_result["status"], "completed");
    assert_eq!(json_result["result"], "ok");
    assert_eq!(json_result["handler_id"], handler_id);
    assert!(json_result.get("handler").is_none());

    let restored = abxbus_rust::base_event::BaseEvent::from_json_value(serialized);
    assert_eq!(restored.inner.lock().event_status, EventStatus::Completed);
    assert_eq!(restored.inner.lock().event_pending_bus_count, 0);
    assert_eq!(restored.inner.lock().event_results.len(), 1);
    let restored_result = restored
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("restored result");
    assert_eq!(restored_result.status, EventResultStatus::Completed);
    assert_eq!(restored_result.result, Some(json!("ok")));
    assert_eq!(restored_result.handler.handler_name, "returns_ok");
    bus.stop();
}

#[test]
fn test_event_handler_json_matches_python_typescript_shape() {
    let bus = EventBus::new_with_options(
        Some("HandlerJsonBus".to_string()),
        EventBusOptions {
            id: Some("018f8e40-1234-7000-8000-000000001234".to_string()),
            ..EventBusOptions::default()
        },
    );
    let handler = bus.on(
        "RuntimeSerializationEvent",
        "handler",
        |_event| async move { Ok(json!("ok")) },
    );

    let serialized = handler.to_json_value();
    assert_eq!(object_keys(&serialized), expected_event_handler_json_keys());
    assert_eq!(serialized["event_pattern"], "RuntimeSerializationEvent");
    assert_eq!(serialized["eventbus_name"], "HandlerJsonBus");
    assert_eq!(
        serialized["eventbus_id"],
        "018f8e40-1234-7000-8000-000000001234"
    );
    assert_eq!(serialized["handler_name"], "handler");
    assert!(serialized.get("handler").is_none());

    let restored = abxbus_rust::event_handler::EventHandler::from_json_value(serialized);
    assert_eq!(restored.id, handler.id);
    assert_eq!(restored.event_pattern, handler.event_pattern);
    assert_eq!(restored.eventbus_id, handler.eventbus_id);
    assert!(restored.callable.is_none());
    bus.stop();
}

#[test]
fn test_eventbus_model_dump_json_roundtrip_uses_id_keyed_structures() {
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
            max_handler_recursion_depth: 2,
        },
    );
    let handler = bus.on(
        "RuntimeSerializationEvent",
        "handler",
        |_event| async move { Ok(json!("ok")) },
    );
    let event = bus.emit::<RuntimeSerializationEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());

    let payload = bus.to_json_value();
    assert_eq!(serde_json::to_value(&*bus).expect("bus serde"), payload);
    assert_eq!(object_keys(&payload), expected_event_bus_json_keys());
    assert_eq!(payload["id"], "018f8e40-1234-7000-8000-000000001234");
    assert_eq!(payload["name"], "SerializableBus");
    assert_eq!(payload["max_history_size"], 500);
    assert_eq!(payload["max_history_drop"], false);
    assert_eq!(payload["event_concurrency"], "parallel");
    assert_eq!(payload["event_timeout"], Value::Null);
    assert_eq!(payload["event_slow_timeout"], 34.0);
    assert_eq!(payload["event_handler_concurrency"], "parallel");
    assert_eq!(payload["event_handler_completion"], "first");
    assert_eq!(payload["event_handler_slow_timeout"], 12.0);
    assert_eq!(payload["event_handler_detect_file_paths"], false);

    let handlers = payload["handlers"].as_object().expect("handlers");
    assert_eq!(handlers.keys().cloned().collect::<BTreeSet<_>>(), {
        let mut keys = BTreeSet::new();
        keys.insert(handler.id.clone());
        keys
    });
    assert_eq!(
        object_keys(handlers.get(&handler.id).expect("handler json")),
        expected_event_handler_json_keys()
    );
    assert_eq!(
        payload["handlers_by_key"]["RuntimeSerializationEvent"],
        json!([handler.id.clone()])
    );

    let event_id = event.inner.inner.lock().event_id.clone();
    let event_history = payload["event_history"].as_object().expect("history");
    assert_eq!(event_history.keys().cloned().collect::<BTreeSet<_>>(), {
        let mut keys = BTreeSet::new();
        keys.insert(event_id.clone());
        keys
    });
    assert_eq!(
        object_keys(event_history.get(&event_id).expect("event json")),
        expected_base_event_json_keys(true)
    );
    assert_eq!(payload["pending_event_queue"], json!([]));

    let restored = EventBus::from_json_value(payload.clone());
    let restored_payload = restored.to_json_value();
    assert_eq!(restored_payload, payload);
    restored.stop();
    bus.stop();
}

#[test]
fn test_eventbus_validate_creates_missing_handler_entries_from_event_results() {
    let bus = EventBus::new_with_options(
        Some("SerializableBusMissingHandlers".to_string()),
        EventBusOptions {
            id: Some("018f8e40-1234-7000-8000-000000001235".to_string()),
            ..EventBusOptions::default()
        },
    );
    let handler = bus.on(
        "RuntimeSerializationEvent",
        "handler",
        |_event| async move { Ok(json!("ok")) },
    );
    let event = bus.emit::<RuntimeSerializationEvent>(TypedEvent::new(EmptyPayload {}));
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
        restored_payload["handlers_by_key"]["RuntimeSerializationEvent"],
        json!([handler.id])
    );
    restored.stop();
    bus.stop();
}

#[test]
fn test_eventbus_model_dump_promotes_pending_events_into_event_history() {
    let bus = EventBus::new(Some("QueueOnlyBus".to_string()));
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let sent_started = Arc::new(AtomicBool::new(false));
    let sent_started_for_handler = sent_started.clone();

    bus.on(
        "RuntimeSerializationEvent",
        "blocking_handler",
        move |_event| {
            let started_tx = started_tx.clone();
            let sent_started = sent_started_for_handler.clone();
            async move {
                if !sent_started.swap(true, Ordering::SeqCst) {
                    let _ = started_tx.send(());
                }
                thread::sleep(Duration::from_millis(100));
                Ok(json!("ok"))
            }
        },
    );

    let first = bus.emit::<RuntimeSerializationEvent>(TypedEvent::new(EmptyPayload {}));
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("first handler should start");
    let second = bus.emit::<RuntimeSerializationEvent>(TypedEvent::new(EmptyPayload {}));

    let first_id = first.inner.inner.lock().event_id.clone();
    let second_id = second.inner.inner.lock().event_id.clone();
    let payload = bus.to_json_value();
    let event_history = payload["event_history"].as_object().expect("history");
    assert!(event_history.contains_key(&first_id));
    assert!(event_history.contains_key(&second_id));
    assert_eq!(payload["pending_event_queue"], json!([second_id]));

    block_on(first.wait_completed());
    block_on(second.wait_completed());
    bus.stop();
}

#[test]
fn test_eventbus_initialization() {
    let bus = EventBus::new(None);

    assert_eq!(bus.event_history_size(), 0);
    assert!(!bus.max_history_drop());
    assert_eq!(bus.event_history_ids().len(), 0);
    assert!(EventBus::all_instances_contains(&bus));
    bus.stop();
}

#[test]
fn test_wait_until_idle_timeout_returns_after_timeout_when_work_is_still_in_flight() {
    let bus = EventBus::new(Some("WaitForIdleTimeoutBus".to_string()));
    bus.on("WaitForIdleTimeoutEvent", "wait", |_event| async move {
        thread::sleep(Duration::from_millis(100));
        Ok(json!(null))
    });

    let event = bus.emit::<WaitForIdleTimeoutEvent>(TypedEvent::new(EmptyPayload {}));
    let started = std::time::Instant::now();
    let became_idle = block_on(bus.wait_until_idle(Some(0.02)));
    let elapsed = started.elapsed();

    assert!(!became_idle);
    assert!(elapsed >= Duration::from_millis(15));
    assert!(elapsed < Duration::from_secs(1));
    assert!(!bus.is_idle_and_queue_empty());

    block_on(event.wait_completed());
    assert!(block_on(bus.wait_until_idle(None)));
    bus.stop();
}

#[test]
fn test_event_bus_applies_custom_options() {
    let bus = EventBus::new_with_options(
        Some("CustomBus".to_string()),
        EventBusOptions {
            max_history_size: Some(500),
            max_history_drop: false,
            event_concurrency: EventConcurrencyMode::Parallel,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_handler_completion: EventHandlerCompletionMode::First,
            event_timeout: Some(30.0),
            ..EventBusOptions::default()
        },
    );

    assert_eq!(bus.max_history_size(), Some(500));
    assert!(!bus.max_history_drop());
    assert_eq!(bus.event_concurrency, EventConcurrencyMode::Parallel);
    assert_eq!(
        bus.event_handler_concurrency,
        EventHandlerConcurrencyMode::Serial
    );
    assert_eq!(
        bus.event_handler_completion,
        EventHandlerCompletionMode::First
    );
    assert_eq!(bus.event_timeout, Some(30.0));
    bus.stop();
}

#[test]
fn test_event_bus_with_null_max_history_size_means_unlimited() {
    let bus = EventBus::new_with_options(
        Some("UnlimitedBus".to_string()),
        EventBusOptions {
            max_history_size: None,
            ..EventBusOptions::default()
        },
    );

    assert_eq!(bus.max_history_size(), None);
    bus.stop();
}

#[test]
fn test_unbounded_history_disables_history_rejection() {
    let bus = EventBus::new_with_options(
        Some("NoLimitBus".to_string()),
        EventBusOptions {
            max_history_size: None,
            ..EventBusOptions::default()
        },
    );

    for _ in 0..150 {
        let event = bus.emit::<UserActionEvent>(TypedEvent::new(EmptyPayload {}));
        block_on(event.wait_completed());
    }

    assert_eq!(bus.event_history_size(), 150);
    bus.stop();
}

#[test]
fn test_event_bus_with_null_event_timeout_disables_timeouts() {
    let bus = EventBus::new_with_options(
        Some("NoTimeoutBus".to_string()),
        EventBusOptions {
            event_timeout: None,
            ..EventBusOptions::default()
        },
    );

    assert_eq!(bus.event_timeout, None);
    bus.stop();
}

#[test]
fn test_event_bus_auto_generates_name_when_not_provided() {
    let bus = EventBus::new(None);
    assert!(bus.name.starts_with("EventBus_"));
    assert_eq!(
        bus.name,
        format!("EventBus_{}", &bus.id[bus.id.len() - 8..])
    );
    bus.stop();
}

#[test]
fn test_eventbus_initializes_with_correct_defaults() {
    test_event_bus_initializes_with_correct_defaults();
}

#[test]
fn test_waituntilidle_timeout_returns_after_timeout_when_work_is_still_in_flight() {
    test_wait_until_idle_timeout_returns_after_timeout_when_work_is_still_in_flight();
}

#[test]
fn test_eventbus_applies_custom_options() {
    test_event_bus_applies_custom_options();
}

#[test]
fn test_eventbus_with_null_max_history_size_means_unlimited() {
    test_event_bus_with_null_max_history_size_means_unlimited();
}

#[test]
fn test_eventbus_with_null_event_timeout_disables_timeouts() {
    test_event_bus_with_null_event_timeout_disables_timeouts();
}

#[test]
fn test_eventbus_auto_generates_name_when_not_provided() {
    test_event_bus_auto_generates_name_when_not_provided();
}

#[test]
fn test_baseevent_lifecycle_methods_are_callable_and_preserve_lifecycle_behavior() {
    test_base_event_lifecycle_methods_are_callable_and_preserve_lifecycle_behavior();
}

#[test]
fn test_baseevent_tojson_fromjson_roundtrips_runtime_fields_and_event_results() {
    test_base_event_to_json_from_json_roundtrips_runtime_fields_and_event_results();
}

#[test]
fn test_eventbus_accepts_custom_id() {
    let custom_id = "018f8e40-1234-7000-8000-000000001234".to_string();
    let bus = EventBus::new_with_options(
        None,
        EventBusOptions {
            id: Some(custom_id.clone()),
            ..EventBusOptions::default()
        },
    );

    assert_eq!(bus.id, custom_id);
    assert!(bus.label().ends_with("#1234"));
    bus.stop();
}

#[test]
fn test_eventbus_accepts_custom_handler_recursion_depth() {
    let bus = EventBus::new_with_options(
        Some("CustomRecursionConfigBus".to_string()),
        EventBusOptions {
            max_handler_recursion_depth: 5,
            ..EventBusOptions::default()
        },
    );

    assert_eq!(bus.max_handler_recursion_depth, 5);
    bus.stop();
}

#[test]
fn test_handler_registration_via_string_class_and_wildcard() {
    test_handler_registration_by_string_matches_extend_name();
    test_class_matcher_matches_generic_base_event_by_event_type();
    test_wildcard_handler_receives_all_events();
}

#[test]
fn test_handlers_can_be_sync_or_async() {
    test_handler_can_be_sync_or_async();
}

#[test]
fn test_class_matcher_falls_back_to_class_name_and_matches_generic_baseevent_event_type() {
    test_class_matcher_matches_generic_base_event_by_event_type();
}

#[test]
fn test_instance_class_and_static_method_handlers() {
    test_class_and_instance_method_handlers();
}

#[test]
fn test_custom_handler_recursion_depth_allows_deeper_nested_handlers() {
    let bus = EventBus::new_with_options(
        Some("CustomRecursionDepthBus".to_string()),
        EventBusOptions {
            max_handler_recursion_depth: 5,
            ..EventBusOptions::default()
        },
    );
    let seen_levels = Arc::new(Mutex::new(Vec::new()));
    let bus_for_handler = bus.clone();
    let seen_for_handler = seen_levels.clone();

    bus.on("RecursiveEvent", "recursive_handler", move |event| {
        let bus = bus_for_handler.clone();
        let seen_levels = seen_for_handler.clone();
        async move {
            let payload = event.inner.lock().payload.clone();
            let level = payload["level"].as_i64().expect("level");
            let max_level = payload["max_level"].as_i64().expect("max_level");
            seen_levels.lock().expect("seen levels lock").push(level);
            if level < max_level {
                let child = bus.emit_child_base(base_event(
                    "RecursiveEvent",
                    json!({"level": level + 1, "max_level": max_level}),
                ));
                child.wait_completed().await;
            }
            Ok(json!(null))
        }
    });

    let event = bus.emit_base(base_event(
        "RecursiveEvent",
        json!({"level": 0, "max_level": 5}),
    ));
    block_on(event.event_completed());
    assert_eq!(
        seen_levels.lock().expect("seen levels lock").as_slice(),
        &[0, 1, 2, 3, 4, 5]
    );
    bus.stop();
}

#[test]
fn test_default_handler_recursion_depth_still_catches_runaway_loops() {
    let bus = EventBus::new(Some("DefaultRecursionDepthBus".to_string()));
    let bus_for_handler = bus.clone();

    bus.on("RecursiveEvent", "recursive_handler", move |event| {
        let bus = bus_for_handler.clone();
        async move {
            let payload = event.inner.lock().payload.clone();
            let level = payload["level"].as_i64().expect("level");
            let max_level = payload["max_level"].as_i64().expect("max_level");
            if level < max_level {
                let child = bus.emit_child_base(base_event(
                    "RecursiveEvent",
                    json!({"level": level + 1, "max_level": max_level}),
                ));
                child.wait_completed().await;
            }
            Ok(json!(null))
        }
    });

    let event = bus.emit_base(base_event(
        "RecursiveEvent",
        json!({"level": 0, "max_level": 3}),
    ));
    block_on(event.event_completed());

    let has_recursion_error = bus
        .runtime_payload_for_test()
        .values()
        .flat_map(|event| {
            event
                .inner
                .lock()
                .event_results
                .values()
                .cloned()
                .collect::<Vec<_>>()
        })
        .any(|result| {
            result.status == EventResultStatus::Error
                && result
                    .error
                    .as_deref()
                    .is_some_and(|error| error.contains("Infinite loop detected"))
        });
    assert!(has_recursion_error);
    bus.stop();
}

#[test]
fn test_base_event_lifecycle_methods_are_callable_and_preserve_lifecycle_behavior() {
    let bus = EventBus::new(Some("LifecycleMethodInvocationBus".to_string()));

    let standalone = TypedEvent::<LifecycleMethodInvocationEvent>::new(EmptyPayload {});
    standalone.inner.mark_started();
    assert_eq!(
        standalone.inner.inner.lock().event_status,
        EventStatus::Started
    );
    standalone.inner.mark_completed();
    assert_eq!(
        standalone.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    block_on(standalone.wait_completed());

    let dispatched = bus.emit::<LifecycleMethodInvocationEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(dispatched.wait_completed());
    assert_eq!(
        dispatched.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    bus.stop();
}
