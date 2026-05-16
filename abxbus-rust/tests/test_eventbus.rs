use abxbus_rust::event;
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
    base_event::{BaseEvent, EventResultOptions, EventWaitOptions},
    event_bus::{DestroyOptions, EventBus, EventBusOptions},
    event_result::{EventResult, EventResultStatus},
    types::{
        EventConcurrencyMode, EventHandlerCompletionMode, EventHandlerConcurrencyMode, EventStatus,
    },
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EmptyResult {}

event! {
    struct LifecycleMethodInvocationEvent {
        event_result_type: EmptyResult,
        event_type: "LifecycleMethodInvocationEvent",
    }
}
event! {
    struct WaitForIdleTimeoutEvent {
        event_result_type: EmptyResult,
        event_type: "WaitForIdleTimeoutEvent",
    }
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

fn panic_message(result: std::thread::Result<()>) -> String {
    let payload = result.expect_err("operation should panic");
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_string();
    }
    "<non-string panic>".to_string()
}

event! {
    struct UserActionEvent {
        event_result_type: Value,
        event_type: "UserActionEvent",
    }
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
    bus.destroy();
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
    let handler = bus.on_raw("GateInvocationEvent", "handler", |_event| async move {
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
    let _ = block_on(emitted.now());
    assert!(bus.locks.wait_for_idle(Some(Duration::from_secs(1)), || bus
        .is_idle_and_queue_empty()));
    bus.destroy();
}

event! {
    struct VersionedEvent {
        data: String,
        event_result_type: EmptyResult,
        event_type: "VersionedEvent",
        event_version: "1.2.3",
    }
}
event! {
    struct CreateAgentTaskEvent {
        user_id: String,
        agent_session_id: String,
        llm_model: String,
        task: String,
        event_result_type: EmptyResult,
        event_type: "CreateAgentTaskEvent",
    }
}
event! {
    struct ExplicitOverrideEvent {
        data: String,
        event_result_type: EmptyResult,
        event_type: "CustomEventType",
    }
}
event! {
    struct RuntimeSerializationEvent {
        event_result_type: String,
        event_type: "RuntimeSerializationEvent",
    }
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
        "event_handler_timeout".to_string(),
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
    bus.destroy();
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
    bus.destroy();
}

#[test]
fn test_event_transitions_through_pending_started_completed() {
    let bus = EventBus::new(Some("StatusBus".to_string()));
    let status_during_handler = Arc::new(Mutex::new(None));
    let status_for_handler = status_during_handler.clone();

    bus.on_raw("StatusLifecycleEvent", "handler", move |event| {
        let status_for_handler = status_for_handler.clone();
        async move {
            *status_for_handler.lock().expect("status lock") =
                Some(event.inner.lock().event_status);
            Ok(json!("done"))
        }
    });

    let event = bus.emit_base(base_event("StatusLifecycleEvent", json!({})));
    let _ = block_on(event.wait());

    assert_eq!(
        *status_during_handler.lock().expect("status lock"),
        Some(EventStatus::Started)
    );
    let inner = event.inner.lock();
    assert_eq!(inner.event_status, EventStatus::Completed);
    assert!(inner.event_started_at.is_some());
    assert!(inner.event_completed_at.is_some());
    drop(inner);
    bus.destroy();
}

#[test]
fn test_event_with_no_handlers_completes_immediately() {
    let bus = EventBus::new(Some("NoHandlerBus".to_string()));
    let event = bus.emit_base(base_event("OrphanEvent", json!({})));
    let _ = block_on(event.wait());

    let inner = event.inner.lock();
    assert_eq!(inner.event_status, EventStatus::Completed);
    assert_eq!(inner.event_results.len(), 0);
    drop(inner);
    bus.destroy();
}

#[test]
fn test_auto_start_and_destroy() {
    let bus = EventBus::new(Some("AutoStartDestroyBus".to_string()));
    assert!(!bus.is_running_for_test());

    let event = bus.emit(UserActionEvent {
        ..Default::default()
    });
    block_on(event.now_with_options(EventWaitOptions::default())).expect("now should complete");
    assert!(block_on(bus.wait_until_idle(Some(1.0))));
    assert!(bus.is_running_for_test());

    bus.destroy();
    assert!(!bus.is_running_for_test());
    assert!(bus.is_destroyed_for_test());
}

#[test]
fn test_wait_until_idle_recovers_when_idle_flag_was_cleared() {
    let bus = EventBus::new(Some("IdleRecoveryBus".to_string()));

    bus.on_raw("UserActionEvent", "handler", |_event| async move {
        Ok(json!(null))
    });

    let event = bus.emit(UserActionEvent {
        ..Default::default()
    });
    block_on(event.now_with_options(EventWaitOptions::default())).expect("now should complete");
    assert!(block_on(bus.wait_until_idle(Some(1.0))));

    assert!(block_on(bus.wait_until_idle(Some(1.0))));
    bus.destroy();
    bus.destroy();
    assert!(bus.is_destroyed_for_test());
}

#[test]
fn test_destroy_with_pending_events() {
    let bus = EventBus::new(Some("DestroyPendingBus".to_string()));
    bus.on_raw("*", "slow_handler", |_event| async move {
        thread::sleep(Duration::from_millis(100));
        Ok(json!("done"))
    });

    for action in 0..5 {
        bus.emit_base(base_event(
            "UserActionEvent",
            json!({"action": format!("action_{action}")}),
        ));
    }

    bus.destroy();
    assert!(!bus.is_running_for_test());
    assert!(bus.is_destroyed_for_test());
}

#[test]
fn test_destroy_clear_false_preserves_handlers_and_history_resolves_waiters_and_is_terminal() {
    let bus = EventBus::new(Some("DestroyClearFalseTerminalBus".to_string()));
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_handler = calls.clone();
    bus.on_raw("UserActionEvent", "handler", move |_event| {
        let calls = calls_for_handler.clone();
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(json!(null))
        }
    });

    let first = bus.emit(UserActionEvent {
        ..Default::default()
    });
    block_on(first.now()).expect("first event should complete");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(bus.event_history_size(), 1);

    let bus_for_waiter = bus.clone();
    let (waiter_tx, waiter_rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let found = block_on(bus_for_waiter.find("NeverHappens", false, Some(5.0), None));
        waiter_tx.send(found.is_none()).expect("send waiter result");
    });
    thread::sleep(Duration::from_millis(20));

    bus.destroy_with_options(DestroyOptions { clear: false });

    assert!(waiter_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("destroy should resolve future waiter"));
    assert!(bus.is_destroyed_for_test());
    assert_eq!(bus.event_history_size(), 1);
    assert!(bus
        .to_json_value()
        .get("handlers")
        .and_then(Value::as_object)
        .is_some_and(|handlers| !handlers.is_empty()));

    let emit_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        bus.emit(UserActionEvent {
            ..Default::default()
        });
    }));
    assert!(panic_message(emit_result).contains("has been destroyed"));

    let on_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        bus.on_raw("UserActionEvent", "after_destroy", |_event| async move {
            Ok(json!(null))
        });
    }));
    assert!(panic_message(on_result).contains("has been destroyed"));

    let find_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = block_on(bus.find("UserActionEvent", true, None, None));
    }));
    assert!(panic_message(find_result).contains("has been destroyed"));
}

#[test]
fn test_destroy_is_immediate_and_rejects_late_handler_emits() {
    let bus = EventBus::new(Some("DestroyImmediateBus".to_string()));
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (late_tx, late_rx) = std::sync::mpsc::channel();
    let release_handler = Arc::new(AtomicBool::new(false));
    let release_for_handler = release_handler.clone();
    let bus_for_handler = bus.clone();
    bus.on_raw("UserActionEvent", "slow_handler", move |_event| {
        let started_tx = started_tx.clone();
        let late_tx = late_tx.clone();
        let release_handler = release_for_handler.clone();
        let bus = bus_for_handler.clone();
        async move {
            let _ = started_tx.send(());
            while !release_handler.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(1));
            }
            let late_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                bus.emit(UserActionEvent {
                    ..Default::default()
                });
            }));
            let _ = late_tx.send(late_result.is_err());
            Ok(json!(null))
        }
    });

    bus.emit(UserActionEvent {
        ..Default::default()
    });
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("handler should start");

    let start = Instant::now();
    bus.destroy_with_options(DestroyOptions { clear: false });

    assert!(
        start.elapsed() < Duration::from_millis(50),
        "Destroy should be immediate, elapsed={:?}",
        start.elapsed()
    );
    assert_eq!(bus.event_history_size(), 1);
    assert!(bus.is_destroyed_for_test());

    let outside_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        bus.emit(UserActionEvent {
            ..Default::default()
        });
    }));
    assert!(panic_message(outside_result).contains("has been destroyed"));

    release_handler.store(true, Ordering::SeqCst);
    assert!(late_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("late handler emit should report rejection"));
}

#[test]
fn test_destroy_default_clear_is_terminal_and_frees_bus_state() {
    let bus = EventBus::new(Some("TerminalDestroyBus".to_string()));
    bus.on_raw("UserActionEvent", "handler", |_event| async move {
        Ok(json!(null))
    });
    let event = bus.emit(UserActionEvent {
        ..Default::default()
    });
    block_on(event.now()).expect("event should complete before destroy");
    assert_eq!(bus.event_history_size(), 1);
    assert!(!bus.runtime_payload_for_test().is_empty());

    let label = bus.label();
    bus.destroy();

    assert!(bus.is_destroyed_for_test());
    assert_eq!(bus.event_history_size(), 0);
    assert!(bus.runtime_payload_for_test().is_empty());
    assert!(bus
        .to_json_value()
        .get("handlers")
        .and_then(Value::as_object)
        .is_some_and(|handlers| handlers.is_empty()));
    assert!(!EventBus::all_instances_contains(&bus));

    for message in [
        panic_message(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || {
                bus.emit(UserActionEvent {
                    ..Default::default()
                });
            },
        ))),
        panic_message(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || {
                bus.on_raw("UserActionEvent", "late_handler", |_event| async move {
                    Ok(json!(null))
                });
            },
        ))),
        panic_message(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || {
                let _ = block_on(bus.find("UserActionEvent", true, None, None));
            },
        ))),
    ] {
        assert!(
            message.contains("has been destroyed and cannot be used again"),
            "{message}"
        );
        assert!(message.contains(&label), "{message}");
    }
}

#[test]
fn test_destroying_one_bus_does_not_break_shared_handlers_or_forward_targets() {
    let source = EventBus::new(Some("DestroySharedSourceBus".to_string()));
    let target = EventBus::new(Some("DestroySharedTargetBus".to_string()));
    let seen = Arc::new(AtomicUsize::new(0));
    let seen_for_source = seen.clone();
    source.on_raw("UserActionEvent", "shared", move |_event| {
        let seen = seen_for_source.clone();
        async move {
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(json!("shared"))
        }
    });
    let target_for_forwarding = target.clone();
    source.on_raw("*", "forward", move |event| {
        let target = target_for_forwarding.clone();
        async move {
            target.emit_base(event);
            Ok(json!(null))
        }
    });
    let seen_for_target = seen.clone();
    target.on_raw("UserActionEvent", "shared", move |_event| {
        let seen = seen_for_target.clone();
        async move {
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(json!("shared"))
        }
    });

    let forwarded = source.emit(UserActionEvent {
        ..Default::default()
    });
    block_on(forwarded.now()).expect("forwarded event should complete");
    assert_eq!(seen.load(Ordering::SeqCst), 2);

    source.destroy();

    let independent_event = target.emit(UserActionEvent {
        ..Default::default()
    });
    block_on(independent_event.now()).expect("target bus should still run");
    assert_eq!(target.event_history_size(), 2);
    assert_eq!(seen.load(Ordering::SeqCst), 3);

    target.destroy();
}

#[test]
fn test_emit_and_result() {
    let bus = EventBus::new(Some("EmitAndResultBus".to_string()));
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));

    bus.on_raw("UserActionEvent", "user_action_handler", move |_event| {
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
    let _ = block_on(queued.wait());

    let inner = queued.inner.lock();
    assert_eq!(inner.event_status, EventStatus::Completed);
    assert!(inner.event_started_at.is_some());
    assert!(inner.event_completed_at.is_some());
    assert_eq!(inner.event_results.len(), 1);
    drop(inner);
    assert_eq!(bus.event_history_size(), 1);
    bus.destroy();
}

#[test]
fn test_dispatched_events_appear_in_event_history() {
    let bus = EventBus::new(Some("HistoryBus".to_string()));
    let event_a = bus.emit_base(base_event("EventA", json!({})));
    let event_b = bus.emit_base(base_event("EventB", json!({})));
    let _ = block_on(event_a.wait());
    let _ = block_on(event_b.wait());
    assert!(block_on(bus.wait_until_idle(None)));

    assert_eq!(bus.event_history_size(), 2);
    let history_ids = bus.event_history_ids();
    assert_eq!(history_ids.len(), 2);
    assert_eq!(history_ids[0], event_a.inner.lock().event_id);
    assert_eq!(history_ids[1], event_b.inner.lock().event_id);
    let runtime = bus.runtime_payload_for_test();
    assert_eq!(runtime[&history_ids[0]].inner.lock().event_type, "EventA");
    assert_eq!(runtime[&history_ids[1]].inner.lock().event_type, "EventB");
    bus.destroy();
}

#[test]
fn test_write_ahead_log_captures_all_events() {
    let bus = EventBus::new(Some("WriteAheadLogBus".to_string()));
    bus.on_raw("UserActionEvent", "handler", |_event| async move {
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
    bus.destroy();
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
    bus.on_raw(
        "TrimEvent",
        "handler",
        |_event| async move { Ok(json!("ok")) },
    );

    for seq in 0..10 {
        let event = bus.emit_base(base_event("TrimEvent", json!({"seq": seq})));
        let _ = block_on(event.wait());
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
    bus.destroy();
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
    bus.on_raw(
        "PingEvent",
        "handler",
        |_event| async move { Ok(json!("pong")) },
    );

    for _ in 0..150 {
        let event = bus.emit_base(base_event("PingEvent", json!({})));
        let _ = block_on(event.wait());
    }
    assert!(block_on(bus.wait_until_idle(None)));

    assert_eq!(bus.event_history_size(), 150);
    assert!(bus
        .runtime_payload_for_test()
        .values()
        .all(|event| event.inner.lock().event_status == EventStatus::Completed));
    bus.destroy();
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
    bus.on_raw(
        "NoDropEvent",
        "handler",
        |_event| async move { Ok(json!("ok")) },
    );

    for seq in 1..=2 {
        let event = bus.emit_base(base_event("NoDropEvent", json!({"seq": seq})));
        let _ = block_on(event.wait());
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
    bus.destroy();
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

    bus.on_raw("SlowEvent", "handler", move |_event| {
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
    let _ = block_on(first.wait());
    let _ = block_on(second.wait());
    assert!(block_on(bus.wait_until_idle(None)));

    assert_eq!(bus.event_history_size(), 0);
    assert!(bus.runtime_payload_for_test().is_empty());
    bus.destroy();
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

    bus.on_raw("BurstEvent", "handler", move |_event| {
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
        let _ = block_on(event.wait());
    }
    assert!(block_on(bus.wait_until_idle(None)));

    assert_eq!(bus.event_history_size(), 0);
    assert_eq!(bus.to_json_value()["pending_event_queue"], json!([]));
    bus.destroy();
}

#[test]
fn test_handler_registration_by_string_matches_extend_name() {
    let bus = EventBus::new(Some("StringMatchBus".to_string()));
    let received = Arc::new(Mutex::new(Vec::new()));
    let received_for_handler = received.clone();

    bus.on_raw("NamedEvent", "string_handler", move |_event| {
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
    let _ = block_on(event.wait());

    assert_eq!(
        received.lock().expect("received lock").as_slice(),
        &["string_handler".to_string()]
    );
    bus.destroy();
}

#[test]
fn test_class_matcher_matches_generic_base_event_by_event_type() {
    let bus = EventBus::new(Some("GenericClassMatcherBus".to_string()));
    let seen = Arc::new(Mutex::new(Vec::new()));

    for (handler_name, prefix) in [("class_handler", "class"), ("string_handler", "string")] {
        let seen = seen.clone();
        bus.on_raw("DifferentNameFromClass", handler_name, move |event| {
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
    bus.on_raw("*", "wildcard_handler", move |event| {
        let seen = seen_for_wildcard.clone();
        async move {
            seen.lock()
                .expect("seen lock")
                .push(format!("wildcard:{}", event.inner.lock().event_type));
            Ok(json!(null))
        }
    });

    let event = bus.emit_base(base_event("DifferentNameFromClass", json!({})));
    let _ = block_on(event.wait());

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
    bus.destroy();
}

#[test]
fn test_wildcard_handler_receives_all_events() {
    let bus = EventBus::new(Some("WildcardBus".to_string()));
    let types = Arc::new(Mutex::new(Vec::new()));
    let types_for_handler = types.clone();

    bus.on_raw("*", "wildcard", move |event| {
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
    let _ = block_on(event_a.wait());
    let _ = block_on(event_b.wait());

    assert_eq!(
        types.lock().expect("types lock").as_slice(),
        &["EventA".to_string(), "EventB".to_string()]
    );
    bus.destroy();
}

#[test]
fn test_wait_for_result() {
    let bus = EventBus::new(Some("WaitForResultBus".to_string()));
    let completion_order = Arc::new(Mutex::new(Vec::new()));

    let order_for_handler = completion_order.clone();
    bus.on_raw("UserActionEvent", "slow_handler", move |_event| {
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

    let event = bus.emit(UserActionEvent {
        ..Default::default()
    });
    completion_order
        .lock()
        .expect("completion order lock")
        .push("enqueue_done".to_string());

    let _ = block_on(event.now());
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
    assert!(event.event_completed_at.read().is_some());
    bus.destroy();
}

#[test]
fn test_error_handling() {
    let bus = EventBus::new(Some("ErrorHandlingBus".to_string()));
    let results = Arc::new(Mutex::new(Vec::new()));

    bus.on_raw("UserActionEvent", "failing_handler", |_event| async move {
        Err("Expected to fail - testing error handling in event handlers".to_string())
    });

    let results_for_handler = results.clone();
    bus.on_raw("UserActionEvent", "working_handler", move |_event| {
        let results = results_for_handler.clone();
        async move {
            results
                .lock()
                .expect("results lock")
                .push("success".to_string());
            Ok(json!("worked"))
        }
    });

    let event = bus.emit(UserActionEvent {
        ..Default::default()
    });
    let _ = block_on(event.now());
    let event_results = event.event_results.read();

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
    bus.destroy();
}

#[test]
fn test_event_result_raises_exception_group_when_multiple_handlers_fail() {
    let bus = EventBus::new(Some("EventResultMultiErrorBus".to_string()));

    bus.on_raw(
        "UserActionEvent",
        "failing_handler_one",
        |_event| async move { Err("ValueError: first failure".to_string()) },
    );
    bus.on_raw(
        "UserActionEvent",
        "failing_handler_two",
        |_event| async move { Err("RuntimeError: second failure".to_string()) },
    );

    let event = bus.emit(UserActionEvent {
        ..Default::default()
    });
    let _ = block_on(event.now());

    let error = block_on(event.event_result_with_options(EventResultOptions::default()))
        .expect_err("multiple handler errors should be raised");
    assert!(error.contains("2 handler error(s)"), "{error}");
    assert!(error.contains("ValueError: first failure"), "{error}");
    assert!(error.contains("RuntimeError: second failure"), "{error}");
    bus.destroy();
}

#[test]
fn test_event_result_single_handler_error_raises_original_exception() {
    let bus = EventBus::new(Some("EventResultSingleErrorBus".to_string()));

    bus.on_raw("UserActionEvent", "failing_handler", |_event| async move {
        Err("ValueError: single failure".to_string())
    });

    let event = bus.emit(UserActionEvent {
        ..Default::default()
    });
    let _ = block_on(event.now());

    let error = block_on(event.event_result_with_options(EventResultOptions::default()))
        .expect_err("single handler error should be raised");
    assert_eq!(error, "ValueError: single failure");
    bus.destroy();
}

#[test]
fn test_event_result_raise_if_any_options() {
    let bus = EventBus::new(Some("NowRaiseIfAnyBus".to_string()));

    bus.on_raw("UserActionEvent", "failing_handler", |_event| async move {
        Err("ValueError: handler failure".to_string())
    });

    let event = bus.emit(UserActionEvent {
        ..Default::default()
    });

    block_on(event.now_with_options(EventWaitOptions::default())).expect("now should complete");
    let error = match block_on(event.event_result_with_options(EventResultOptions::default())) {
        Ok(_) => panic!("event_result should raise handler errors by default"),
        Err(error) => error,
    };
    assert_eq!(error, "ValueError: handler failure");

    block_on(event.event_result_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: false,
        include: None,
    }))
    .expect("raise_if_any=false should only inspect results");
    bus.destroy();
}

#[test]
fn test_event_results_list_defaults_filter_empty_values_raise_errors_and_options_override() {
    let bus = EventBus::new(Some("EventResultsOptionsBus".to_string()));

    bus.on_raw("ResultOptionsDefaultEvent", "ok", |_event| async move {
        Ok(json!("ok"))
    });
    bus.on_raw("ResultOptionsDefaultEvent", "null", |_event| async move {
        Ok(Value::Null)
    });
    bus.on_raw(
        "ResultOptionsDefaultEvent",
        "forwarded",
        |_event| async move { Ok(base_event("ForwardedResultEvent", json!({})).to_json_value()) },
    );
    let default_event = bus.emit_base(base_event("ResultOptionsDefaultEvent", json!({})));
    let _ = block_on(default_event.wait());
    let default_values =
        block_on(default_event.event_results_list()).expect("default result values");
    assert_eq!(default_values, vec![json!("ok")]);

    bus.on_raw("ResultOptionsErrorEvent", "ok", |_event| async move {
        Ok(json!("ok"))
    });
    bus.on_raw("ResultOptionsErrorEvent", "boom", |_event| async move {
        Err("boom".to_string())
    });
    let error_event = bus.emit_base(base_event("ResultOptionsErrorEvent", json!({})));
    let _ = block_on(error_event.wait());
    let error = block_on(error_event.event_results_list())
        .expect_err("default raise_if_any should surface handler errors");
    assert!(error.contains("boom"), "{error}");

    let values_without_errors = block_on(error_event.event_results_list_with_options(
        EventResultOptions {
            raise_if_any: false,
            raise_if_none: true,
            include: None,
        },
    ))
    .expect("raise_if_any=false should keep valid values");
    assert_eq!(values_without_errors, vec![json!("ok")]);

    bus.on_raw("ResultOptionsEmptyEvent", "null", |_event| async move {
        Ok(Value::Null)
    });
    let empty_event = bus.emit_base(base_event("ResultOptionsEmptyEvent", json!({})));
    let _ = block_on(empty_event.wait());
    let empty_error = block_on(
        empty_event.event_results_list_with_options(EventResultOptions {
            raise_if_any: true,
            raise_if_none: true,
            include: None,
        }),
    )
    .expect_err("raise_if_none=true should reject an empty filtered list");
    assert!(
        empty_error.contains("Expected at least one handler"),
        "{empty_error}"
    );
    let empty_values = block_on(
        empty_event.event_results_list_with_options(EventResultOptions {
            raise_if_any: false,
            raise_if_none: false,
            include: None,
        }),
    )
    .expect("raise_if_none=false should allow an empty list");
    assert!(empty_values.is_empty());

    bus.on_raw("ResultOptionsIncludeEvent", "keep", |_event| async move {
        Ok(json!("keep"))
    });
    bus.on_raw("ResultOptionsIncludeEvent", "drop", |_event| async move {
        Ok(json!("drop"))
    });
    let seen_handler_names = Arc::new(Mutex::new(Vec::new()));
    let seen_for_include = seen_handler_names.clone();
    let include_event = bus.emit_base(base_event("ResultOptionsIncludeEvent", json!({})));
    let _ = block_on(include_event.wait());
    let filtered_values = block_on(include_event.event_results_list_with_options(
        EventResultOptions {
            raise_if_any: false,
            raise_if_none: true,
            include: Some(Arc::new(move |result, event_result| {
                seen_for_include
                    .lock()
                    .expect("seen handler names lock")
                    .push(event_result.handler.handler_name.clone());
                matches!(result, Some(Value::String(value)) if value == "keep")
            })),
        },
    ))
    .expect("include filter should return matching values");
    assert_eq!(filtered_values, vec![json!("keep")]);
    assert_eq!(
        seen_handler_names
            .lock()
            .expect("seen handler names lock")
            .len(),
        2
    );

    bus.destroy();
}

#[test]
fn test_event_results_access() {
    let bus = EventBus::new(Some("EventResultsAccessBus".to_string()));

    bus.on_raw("TestEvent", "early_handler", |_event| async move {
        Ok(json!("early"))
    });
    bus.on_raw("TestEvent", "late_handler", |_event| async move {
        thread::sleep(Duration::from_millis(10));
        Ok(json!("late"))
    });

    let event = bus.emit_base(base_event("TestEvent", json!({})));
    let _ = block_on(event.wait());
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
    let _ = block_on(empty_event.wait());
    assert_eq!(empty_event.inner.lock().event_results.len(), 0);
    bus.destroy();
}

#[test]
fn test_by_handler_name() {
    let bus = EventBus::new(Some("ByHandlerNameBus".to_string()));

    bus.on_raw("TestEvent", "process_data", |_event| async move {
        Ok(json!("version1"))
    });
    bus.on_raw("TestEvent", "process_data", |_event| async move {
        Ok(json!("version2"))
    });
    bus.on_raw("TestEvent", "unique_handler", |_event| async move {
        Ok(json!("unique"))
    });

    let event = bus.emit_base(base_event("TestEvent", json!({})));
    let _ = block_on(event.wait());
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
    bus.destroy();
}

#[test]
fn test_by_handler_id() {
    let bus = EventBus::new(Some("ByHandlerIdBus".to_string()));

    bus.on_raw(
        "TestEvent",
        "handler",
        |_event| async move { Ok(json!("v1")) },
    );
    bus.on_raw(
        "TestEvent",
        "handler",
        |_event| async move { Ok(json!("v2")) },
    );

    let event = bus.emit_base(base_event("TestEvent", json!({})));
    let _ = block_on(event.wait());
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
    bus.destroy();
}

#[test]
fn test_string_indexing() {
    let bus = EventBus::new(Some("StringIndexingBus".to_string()));

    bus.on_raw("TestEvent", "my_handler", |_event| async move {
        Ok(json!("my_result"))
    });

    let event = bus.emit_base(base_event("TestEvent", json!({})));
    let _ = block_on(event.wait());
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
    bus.destroy();
}

#[test]
fn test_emit_alias_dispatches_event() {
    let bus = EventBus::new(Some("EmitAliasBus".to_string()));
    let handled_event_ids = Arc::new(Mutex::new(Vec::new()));

    let handled_for_handler = handled_event_ids.clone();
    bus.on_raw("UserActionEvent", "user_handler", move |event| {
        let handled_event_ids = handled_for_handler.clone();
        async move {
            handled_event_ids
                .lock()
                .expect("handled ids lock")
                .push(event.inner.lock().event_id.clone());
            Ok(json!("handled"))
        }
    });

    let event = bus.emit(UserActionEvent {
        ..Default::default()
    });
    let event_id = event.event_id.clone();
    let _ = block_on(event.now());

    assert_eq!(
        handled_event_ids
            .lock()
            .expect("handled ids lock")
            .as_slice(),
        std::slice::from_ref(&event_id)
    );
    assert_eq!(event.event_status.read(), EventStatus::Completed);
    assert!(event.event_path.read().contains(&bus.label()));
    bus.destroy();
}

#[test]
fn test_handler_registration() {
    let bus = EventBus::new(Some("HandlerRegistrationBus".to_string()));
    let specific = Arc::new(Mutex::new(Vec::new()));
    let model = Arc::new(Mutex::new(Vec::new()));
    let universal = Arc::new(Mutex::new(Vec::new()));

    let specific_for_handler = specific.clone();
    bus.on_raw("UserActionEvent", "user_handler", move |event| {
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
    bus.on(
        RuntimeSerializationEvent,
        move |_event: RuntimeSerializationEvent| {
            let model = model_for_handler.clone();
            async move {
                model
                    .lock()
                    .expect("model lock")
                    .push("startup".to_string());
                Ok("system_handled".to_string())
            }
        },
    );

    let universal_for_handler = universal.clone();
    bus.on_raw("*", "universal_handler", move |event| {
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
    let system = bus.emit(RuntimeSerializationEvent {
        ..Default::default()
    });
    block_on(async {
        let _ = user.wait().await;
        let _ = system.now().await;
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
    bus.destroy();
}

#[test]
fn test_event_subclass_type() {
    let bus = EventBus::new(Some("EventSubclassTypeBus".to_string()));
    let event = CreateAgentTaskEvent {
        user_id: "371bbd3c-5231-7ff0-8aef-e63732a8d40f".to_string(),
        agent_session_id: "12345678-1234-5678-1234-567812345678".to_string(),
        llm_model: "test-model".to_string(),
        task: "test task".to_string(),
        ..Default::default()
    };

    let result = bus.emit(event);
    assert_eq!(result.event_type, "CreateAgentTaskEvent");
    let _ = block_on(result.now());
    bus.destroy();
}

#[test]
fn test_event_type_and_version_identity_fields() {
    let bus = EventBus::new(Some("IdentityFieldsBus".to_string()));

    let base = base_event("TestEvent", json!({}));
    assert_eq!(base.inner.lock().event_type, "TestEvent");
    assert_eq!(base.inner.lock().event_version, "0.0.1");

    let task = CreateAgentTaskEvent {
        user_id: "371bbd3c-5231-7ff0-8aef-e63732a8d40f".to_string(),
        agent_session_id: "12345678-1234-5678-1234-567812345678".to_string(),
        llm_model: "test-model".to_string(),
        task: "test task".to_string(),
        ..Default::default()
    };
    let emitted = bus.emit(task);
    assert_eq!(emitted.event_type, "CreateAgentTaskEvent");
    assert_eq!(emitted.event_version, "0.0.1");
    let _ = block_on(emitted.now());
    bus.destroy();
}

#[test]
fn test_event_version_defaults_and_overrides() {
    let bus = EventBus::new(Some("VersionFieldsBus".to_string()));

    let base = base_event("TestVersionEvent", json!({}));
    assert_eq!(base.inner.lock().event_version, "0.0.1");

    let class_default = bus.emit(VersionedEvent {
        data: "x".to_string(),
        ..Default::default()
    });
    assert_eq!(class_default.event_version, "1.2.3");

    let mut runtime_override = VersionedEvent {
        data: "x".to_string(),
        ..Default::default()
    };
    runtime_override.event_version = "9.9.9".to_string();
    let runtime_override = bus.emit(runtime_override);
    assert_eq!(runtime_override.event_version, "9.9.9");

    let dispatched = bus.emit(VersionedEvent {
        data: "queued".to_string(),
        ..Default::default()
    });
    assert_eq!(dispatched.event_version, "1.2.3");
    let _ = block_on(dispatched.now());

    let restored = BaseEvent::from_json_value(dispatched.to_json_value());
    assert_eq!(restored.inner.lock().event_version, "1.2.3");
    assert_eq!(restored.inner.lock().event_type, "VersionedEvent");
    assert_eq!(restored.inner.lock().payload["data"], json!("queued"));
    bus.destroy();
}

#[test]
fn test_automatic_event_type_derivation() {
    let bus = EventBus::new(Some("AutomaticEventTypeBus".to_string()));
    let received = Arc::new(Mutex::new(Vec::new()));

    let user = UserActionEvent {
        ..Default::default()
    };
    assert_eq!(
        <UserActionEvent as abxbus_rust::typed::EventSpec>::event_type,
        "UserActionEvent"
    );
    let system = RuntimeSerializationEvent {
        ..Default::default()
    };
    assert_eq!(
        <RuntimeSerializationEvent as abxbus_rust::typed::EventSpec>::event_type,
        "RuntimeSerializationEvent"
    );

    let received_for_user = received.clone();
    bus.on_raw("UserActionEvent", "user_handler", move |event| {
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
    bus.on_raw(
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
        let _ = user.wait().await;
        let _ = system.wait().await;
        assert!(bus.wait_until_idle(Some(1.0)).await);
    });

    assert_eq!(
        received.lock().expect("received lock").as_slice(),
        &[
            "UserActionEvent".to_string(),
            "RuntimeSerializationEvent".to_string(),
        ]
    );
    bus.destroy();
}

#[test]
fn test_explicit_event_type_override() {
    let bus = EventBus::new(Some("ExplicitEventTypeBus".to_string()));
    let received = Arc::new(Mutex::new(Vec::new()));

    let received_for_custom = received.clone();
    bus.on_raw("CustomEventType", "custom_handler", move |event| {
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
    bus.on_raw("ExplicitOverrideEvent", "default_handler", move |event| {
        let received = received_for_default.clone();
        async move {
            received
                .lock()
                .expect("received lock")
                .push(event.inner.lock().event_type.clone());
            Ok(json!(null))
        }
    });

    let event = ExplicitOverrideEvent {
        data: "test".to_string(),
        ..Default::default()
    };
    let event = bus.emit(event);
    assert_eq!(event.event_type, "CustomEventType");
    let _ = block_on(event.now());

    assert_eq!(
        received.lock().expect("received lock").as_slice(),
        &["CustomEventType".to_string()]
    );
    bus.destroy();
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
        bus.on_raw("UserActionEvent", handler_name, move |_event| {
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
    let event = bus.emit(UserActionEvent {
        ..Default::default()
    });
    let _ = block_on(event.now());
    let duration = start.elapsed();

    assert!(
        duration < Duration::from_millis(180),
        "duration={duration:?}"
    );
    assert_eq!(starts.lock().expect("starts lock").len(), 2);
    assert_eq!(ends.lock().expect("ends lock").len(), 2);
    let event_results = event.event_results.read();
    assert!(event_results.values().any(|result| {
        result.handler.handler_name == "slow_handler_1"
            && result.result == Some(json!("slow_handler_1"))
    }));
    assert!(event_results.values().any(|result| {
        result.handler.handler_name == "slow_handler_2"
            && result.result == Some(json!("slow_handler_2"))
    }));
    bus.destroy();
}

#[test]
fn test_handler_can_be_sync_or_async() {
    let bus = EventBus::new(Some("SyncAsyncHandlersBus".to_string()));

    bus.on_raw_sync("TestEvent", "sync_handler", |_event| Ok(json!("sync")));
    bus.on_raw("TestEvent", "async_handler", |_event| async move {
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
    let _ = block_on(event.wait());
    let results: Vec<Value> = event
        .inner
        .lock()
        .event_results
        .values()
        .filter_map(|result| result.result.clone())
        .collect();
    assert!(results.contains(&json!("sync")));
    assert!(results.contains(&json!("async")));
    bus.destroy();
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
    bus.on_raw_sync(
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
    bus.on_raw(
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
    bus.on_raw_sync(
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
    bus.on_raw_sync(
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
    bus.on_raw_sync(
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
    let _ = block_on(event.wait());

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
    bus.destroy();
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
        let _ = block_on(event.wait());
    }

    assert_eq!(events.len(), 3);
    assert!(events
        .iter()
        .all(|event| event.inner.lock().event_completed_at.is_some()));
    bus.destroy();
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
        let _ = block_on(event.wait());
    }
    assert!(block_on(bus.wait_until_idle(Some(2.0))));
    assert_eq!(bus.event_history_size(), 100);
    bus.destroy();
}

fn assert_mixed_delay_handlers_maintain_order() {
    let bus = EventBus::new(Some("MixedDelayOrderBus".to_string()));
    let collected_orders = Arc::new(Mutex::new(Vec::new()));
    let handler_start_orders = Arc::new(Mutex::new(Vec::new()));

    let collected_for_handler = collected_orders.clone();
    let starts_for_handler = handler_start_orders.clone();
    bus.on_raw("UserActionEvent", "handler", move |event| {
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
    bus.destroy();
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
    let _ = block_on(event.wait());

    assert_eq!(
        event.inner.lock().payload["details"]["nested"]["list"][2]["inner"],
        json!("value")
    );
    bus.destroy();
}

#[test]
fn test_dispatch_returns_event_results() {
    let bus = EventBus::new(Some("DispatchReturnsEventResultsBus".to_string()));

    bus.on_raw("UserActionEvent", "test_handler", |_event| async move {
        Ok(json!({"result": "test_result"}))
    });

    let event = bus.emit(UserActionEvent {
        ..Default::default()
    });
    let _ = block_on(event.now());
    let all_results =
        block_on(event.event_results_list_with_options(EventResultOptions::default()))
            .expect("event results list");

    assert_eq!(all_results, vec![json!({"result": "test_result"})]);

    let result_no_handlers = bus.emit_base(base_event("NoHandlersEvent", json!({})));
    let _ = block_on(result_no_handlers.wait());
    assert_eq!(result_no_handlers.inner.lock().event_results.len(), 0);
    bus.destroy();
}

#[test]
fn test_handler() {
    let bus = EventBus::new(Some("HandlerResultBus".to_string()));

    bus.on_raw("TestEvent", "test_handler", |_event| async move {
        Ok(json!({"result": "test_result"}))
    });

    let event = bus.emit_base(base_event("TestEvent", json!({})));
    let _ = block_on(event.wait());

    let all_results =
        block_on(event.event_results_list_with_options(EventResultOptions::default()))
            .expect("event results list");
    assert_eq!(all_results, vec![json!({"result": "test_result"})]);

    let no_handlers = bus.emit_base(base_event("NoHandlersEvent", json!({})));
    let _ = block_on(no_handlers.wait());
    assert_eq!(no_handlers.inner.lock().event_results.len(), 0);
    bus.destroy();
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
        bus.on_raw("TestEvent", handler_name, move |_event| {
            let order = order.clone();
            async move {
                order.lock().expect("order lock").push(index);
                Ok(json!(value))
            }
        });
    }

    let event = bus.emit_base(base_event("TestEvent", json!({})));
    let _ = block_on(event.wait());
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
    bus.destroy();
}

#[test]
fn test_manual_dict_merge() {
    let bus = EventBus::new(Some("ManualDictMergeBus".to_string()));

    bus.on_raw("GetConfig", "config_base", |_event| async move {
        Ok(json!({"debug": false, "port": 8080, "name": "base"}))
    });
    bus.on_raw("GetConfig", "config_override", |_event| async move {
        Ok(json!({"debug": true, "timeout": 30, "name": "override"}))
    });

    let event = bus.emit_base(base_event("GetConfig", json!({})));
    let _ = block_on(event.wait());
    let dict_results = block_on(event.event_results_list_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: true,
        include: Some(Arc::new(|result, _event_result| {
            result.is_some_and(Value::is_object)
        })),
    }))
    .expect("dict results");
    let mut merged = serde_json::Map::new();
    for result in dict_results {
        merged.extend(result.as_object().expect("dict result").clone());
    }
    assert_eq!(
        Value::Object(merged),
        json!({"debug": true, "port": 8080, "timeout": 30, "name": "override"})
    );

    bus.on_raw("BadConfig", "bad_handler", |_event| async move {
        Ok(json!("not a dict"))
    });
    let event_bad = bus.emit_base(base_event("BadConfig", json!({})));
    let _ = block_on(event_bad.wait());
    let merged_bad = block_on(
        event_bad.event_results_list_with_options(EventResultOptions {
            raise_if_any: false,
            raise_if_none: false,
            include: Some(Arc::new(|result, _event_result| {
                result.is_some_and(Value::is_object)
            })),
        }),
    )
    .expect("empty dict results");
    assert!(merged_bad.is_empty());
    bus.destroy();
}

#[test]
fn test_manual_dict_merge_conflicts_last_write_wins() {
    let bus = EventBus::new(Some("ManualDictConflictBus".to_string()));

    bus.on_raw("ConflictEvent", "handler_one", |_event| async move {
        Ok(json!({"shared": 1, "unique1": "a"}))
    });
    bus.on_raw("ConflictEvent", "handler_two", |_event| async move {
        Ok(json!({"shared": 2, "unique2": "b"}))
    });

    let event = bus.emit_base(base_event("ConflictEvent", json!({})));
    let _ = block_on(event.wait());
    let dict_results = block_on(event.event_results_list_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: true,
        include: Some(Arc::new(|result, _event_result| {
            result.is_some_and(Value::is_object)
        })),
    }))
    .expect("dict results");
    let mut merged = serde_json::Map::new();
    for result in dict_results {
        merged.extend(result.as_object().expect("dict result").clone());
    }

    assert_eq!(merged.get("shared"), Some(&json!(2)));
    assert_eq!(merged.get("unique1"), Some(&json!("a")));
    assert_eq!(merged.get("unique2"), Some(&json!("b")));
    bus.destroy();
}

#[test]
fn test_manual_list_flatten() {
    let bus = EventBus::new(Some("ManualListFlattenBus".to_string()));

    bus.on_raw("GetErrors", "errors1", |_event| async move {
        Ok(json!(["error1", "error2"]))
    });
    bus.on_raw("GetErrors", "errors2", |_event| async move {
        Ok(json!(["error3"]))
    });
    bus.on_raw("GetErrors", "errors3", |_event| async move {
        Ok(json!(["error4", "error5"]))
    });

    let event = bus.emit_base(base_event("GetErrors", json!({})));
    let _ = block_on(event.wait());
    let list_results = block_on(event.event_results_list_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: true,
        include: Some(Arc::new(|result, _event_result| {
            result.is_some_and(Value::is_array)
        })),
    }))
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

    bus.on_raw("GetSingle", "single_value", |_event| async move {
        Ok(json!("single"))
    });
    let event_single = bus.emit_base(base_event("GetSingle", json!({})));
    let _ = block_on(event_single.wait());
    let single_lists = block_on(
        event_single.event_results_list_with_options(EventResultOptions {
            raise_if_any: false,
            raise_if_none: false,
            include: Some(Arc::new(|result, _event_result| {
                result.is_some_and(Value::is_array)
            })),
        }),
    )
    .expect("empty list results");
    assert!(single_lists.is_empty());
    bus.destroy();
}

#[test]
fn test_by_handler_name_access() {
    let bus = EventBus::new(Some("ByHandlerNameAccessBus".to_string()));

    bus.on_raw("TestEvent", "handler_a", |_event| async move {
        Ok(json!("result_a"))
    });
    bus.on_raw("TestEvent", "handler_b", |_event| async move {
        Ok(json!("result_b"))
    });

    let event = bus.emit_base(base_event("TestEvent", json!({})));
    let _ = block_on(event.wait());
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
    bus.destroy();
}

#[test]
fn test_forwarding_flattens_results() {
    let bus1 = EventBus::new(Some("Bus1".to_string()));
    let bus2 = EventBus::new(Some("Bus2".to_string()));
    let bus3 = EventBus::new(Some("Bus3".to_string()));
    let execution_order = Arc::new(Mutex::new(Vec::new()));

    let order = execution_order.clone();
    bus1.on_raw("TestEvent", "bus1_handler", move |_event| {
        let order = order.clone();
        async move {
            order.lock().expect("order lock").push("bus1".to_string());
            Ok(json!("from_bus1"))
        }
    });
    let order = execution_order.clone();
    bus2.on_raw("TestEvent", "bus2_handler", move |_event| {
        let order = order.clone();
        async move {
            order.lock().expect("order lock").push("bus2".to_string());
            Ok(json!("from_bus2"))
        }
    });
    let order = execution_order.clone();
    bus3.on_raw("TestEvent", "bus3_handler", move |_event| {
        let order = order.clone();
        async move {
            order.lock().expect("order lock").push("bus3".to_string());
            Ok(json!("from_bus3"))
        }
    });

    let bus2_for_forward = bus2.clone();
    bus1.on_raw("*", "forward_to_bus2", move |event| {
        let bus2 = bus2_for_forward.clone();
        async move {
            bus2.emit_base(event);
            Ok(json!(null))
        }
    });
    let bus3_for_forward = bus3.clone();
    bus2.on_raw("*", "forward_to_bus3", move |event| {
        let bus3 = bus3_for_forward.clone();
        async move {
            bus3.emit_base(event);
            Ok(json!(null))
        }
    });

    let event = bus1.emit_base(base_event("TestEvent", json!({})));
    let _ = block_on(event.wait());
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
    bus1.destroy();
    bus2.destroy();
    bus3.destroy();
}

#[test]
fn test_by_eventbus_id_and_path() {
    let bus1 = EventBus::new(Some("MainBus".to_string()));
    let bus2 = EventBus::new(Some("PluginBus".to_string()));

    bus1.on_raw("DataEvent", "main_handler", |_event| async move {
        Ok(json!("main_result"))
    });
    bus2.on_raw("DataEvent", "plugin_handler1", |_event| async move {
        Ok(json!("plugin_result1"))
    });
    bus2.on_raw("DataEvent", "plugin_handler2", |_event| async move {
        Ok(json!("plugin_result2"))
    });

    let bus2_for_forward = bus2.clone();
    bus1.on_raw("*", "forward_to_plugin", move |event| {
        let bus2 = bus2_for_forward.clone();
        async move {
            bus2.emit_base(event);
            Ok(json!(null))
        }
    });

    let event = bus1.emit_base(base_event("DataEvent", json!({})));
    let _ = block_on(event.wait());
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
    bus1.destroy();
    bus2.destroy();
}

#[test]
fn test_complex_multi_bus_scenario() {
    let app_bus = EventBus::new(Some("AppBus".to_string()));
    let auth_bus = EventBus::new(Some("AuthBus".to_string()));
    let data_bus = EventBus::new(Some("DataBus".to_string()));

    app_bus.on_raw("ValidationRequest", "validate", |_event| async move {
        Ok(json!({"app_valid": true, "timestamp": 1000}))
    });
    auth_bus.on_raw("ValidationRequest", "validate", |_event| async move {
        Ok(json!({"auth_valid": true, "user": "alice"}))
    });
    auth_bus.on_raw("ValidationRequest", "process", |_event| async move {
        Ok(json!(["auth_log_1", "auth_log_2"]))
    });
    data_bus.on_raw("ValidationRequest", "validate", |_event| async move {
        Ok(json!({"data_valid": true, "schema": "v2"}))
    });
    data_bus.on_raw("ValidationRequest", "process", |_event| async move {
        Ok(json!(["data_log_1", "data_log_2", "data_log_3"]))
    });

    let auth_for_forward = auth_bus.clone();
    app_bus.on_raw("*", "forward_to_auth", move |event| {
        let auth_bus = auth_for_forward.clone();
        async move {
            auth_bus.emit_base(event);
            Ok(json!(null))
        }
    });
    let data_for_forward = data_bus.clone();
    auth_bus.on_raw("*", "forward_to_data", move |event| {
        let data_bus = data_for_forward.clone();
        async move {
            data_bus.emit_base(event);
            Ok(json!(null))
        }
    });

    let event = app_bus.emit_base(base_event("ValidationRequest", json!({})));
    let _ = block_on(event.wait());
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

    let dict_results = block_on(event.event_results_list_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: true,
        include: Some(Arc::new(|result, _event_result| {
            result.is_some_and(Value::is_object)
        })),
    }))
    .expect("dict results");
    let mut merged = serde_json::Map::new();
    for result in dict_results {
        merged.extend(result.as_object().expect("dict result").clone());
    }
    assert_eq!(merged.get("app_valid"), Some(&json!(true)));
    assert_eq!(merged.get("auth_valid"), Some(&json!(true)));
    assert_eq!(merged.get("data_valid"), Some(&json!(true)));

    let list_results = block_on(event.event_results_list_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: true,
        include: Some(Arc::new(|result, _event_result| {
            result.is_some_and(Value::is_array)
        })),
    }))
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
    app_bus.destroy();
    auth_bus.destroy();
    data_bus.destroy();
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
        bus.on_raw("DictResultEvent", handler_name, move |_event| {
            let value = value.clone();
            async move { Ok(value) }
        });
    }

    let event = base_event("DictResultEvent", json!({}));
    event.inner.lock().event_result_type = Some(json!({"type": "object"}));
    let event = bus.emit_base(event);
    let _ = block_on(event.wait());
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
    bus.destroy();
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
        bus.on_raw("ListResultEvent", handler_name, move |_event| {
            let value = value.clone();
            async move { Ok(value) }
        });
    }

    let event = base_event("ListResultEvent", json!({}));
    event.inner.lock().event_result_type = Some(json!({"type": "array"}));
    let event = bus.emit_base(event);
    let _ = block_on(event.wait());
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

    let list_results = block_on(event.event_results_list_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: false,
        include: Some(Arc::new(|result, _event_result| {
            result.is_some_and(Value::is_array)
        })),
    }))
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
    bus.destroy();
}

#[test]
fn test_handler_error_is_captured_without_crashing_the_bus() {
    let bus = EventBus::new(Some("ErrorBus".to_string()));
    bus.on_raw("ErrorEvent", "throws", |_event| async move {
        Err("handler blew up".to_string())
    });

    let event = bus.emit_base(base_event("ErrorEvent", json!({})));
    let _ = block_on(event.wait());

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
    bus.destroy();
}

#[test]
fn test_one_handler_error_does_not_prevent_other_handlers_from_running() {
    let bus = EventBus::new(Some("ErrorIsolationBus".to_string()));
    let second_ran = Arc::new(AtomicBool::new(false));
    let second_ran_for_handler = second_ran.clone();

    bus.on_raw("ErrorIsolationEvent", "bad", |_event| async move {
        Err("bad handler".to_string())
    });
    bus.on_raw("ErrorIsolationEvent", "good", move |_event| {
        let second_ran = second_ran_for_handler.clone();
        async move {
            second_ran.store(true, Ordering::SeqCst);
            Ok(json!("ok"))
        }
    });

    let event = bus.emit_base(base_event("ErrorIsolationEvent", json!({})));
    let _ = block_on(event.wait());

    assert!(second_ran.load(Ordering::SeqCst));
    let results = event.inner.lock().event_results.clone();
    assert_eq!(results.len(), 2);
    assert!(results
        .values()
        .any(|result| result.status == EventResultStatus::Error));
    assert!(results
        .values()
        .any(|result| result.status == EventResultStatus::Completed));
    bus.destroy();
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
    bus.on_raw("ConcurrentEvent", "handler", move |_event| {
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
        let _ = block_on(event.wait());
        assert_eq!(event.inner.lock().event_status, EventStatus::Completed);
    }
    assert_eq!(count.load(Ordering::SeqCst), 25);
    bus.destroy();
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
    bus.on_raw("TimeoutDefaultEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });

    let event = base_event("TimeoutDefaultEvent", json!({}));
    assert_eq!(event.inner.lock().event_timeout, None);
    let event = bus.emit_base(event);
    assert_eq!(event.inner.lock().event_timeout, None);
    let _ = block_on(event.wait());
    let result = event
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("result");
    assert_eq!(result.timeout, Some(10.0));
    bus.destroy();
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
    bus.on_raw("ExplicitTimeoutEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });

    let event = base_event("ExplicitTimeoutEvent", json!({}));
    event.inner.lock().event_timeout = Some(2.0);
    let event = bus.emit_base(event);
    let _ = block_on(event.wait());
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
    bus.destroy();
}

#[test]
fn test_eventbus_all_instances_tracks_all_created_buses() {
    let bus_a = EventBus::new(Some("AllInstancesBusA".to_string()));
    let bus_b = EventBus::new(Some("AllInstancesBusB".to_string()));

    assert!(EventBus::all_instances_contains(&bus_a));
    assert!(EventBus::all_instances_contains(&bus_b));
    bus_a.destroy();
    bus_b.destroy();
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
        bus.on_raw("UserActionEvent", "history_handler", |_event| async move {
            Ok(json!("ok"))
        });
        for _ in 0..10 {
            let event = bus.emit(UserActionEvent {
                ..Default::default()
            });
            let _ = block_on(event.now());
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
    bus_a.on_raw(
        "ResetEvent",
        "handler_a",
        |_event| async move { Ok(json!("a")) },
    );
    bus_b.on_raw(
        "ResetEvent",
        "handler_b",
        |_event| async move { Ok(json!("b")) },
    );

    let completed = bus_a.emit_base(base_event("ResetEvent", json!({"label": "hello"})));
    let _ = block_on(completed.wait());
    assert_eq!(completed.inner.lock().event_status, EventStatus::Completed);
    assert_eq!(completed.inner.lock().event_results.len(), 1);

    let fresh = completed.event_reset();
    assert_ne!(fresh.inner.lock().event_id, completed.inner.lock().event_id);
    assert_eq!(fresh.inner.lock().event_status, EventStatus::Pending);
    assert!(fresh.inner.lock().event_started_at.is_none());
    assert!(fresh.inner.lock().event_completed_at.is_none());
    assert_eq!(fresh.inner.lock().event_results.len(), 0);

    let forwarded = bus_b.emit_base(fresh);
    let _ = block_on(forwarded.wait());
    assert_eq!(forwarded.inner.lock().event_status, EventStatus::Completed);
    assert_eq!(forwarded.inner.lock().event_results.len(), 1);
    let event_path = forwarded.inner.lock().event_path.clone();
    assert!(event_path.iter().any(|path| path.starts_with("ResetBusA#")));
    assert!(event_path.iter().any(|path| path.starts_with("ResetBusB#")));
    bus_a.destroy();
    bus_b.destroy();
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
    bus.on_raw("ZeroHistPruneEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });

    let first = bus.emit_base(base_event("ZeroHistPruneEvent", json!({"seq": 1})));
    let _ = block_on(first.wait());
    assert_eq!(bus.event_history_size(), 0);

    let second = bus.emit_base(base_event("ZeroHistPruneEvent", json!({"seq": 2})));
    let _ = block_on(second.wait());
    assert_eq!(bus.event_history_size(), 0);
    assert!(bus.runtime_payload_for_test().is_empty());
    bus.destroy();
}

#[test]
fn test_base_event_to_json_from_json_roundtrips_runtime_fields_and_event_results() {
    let bus = EventBus::new(Some("RuntimeSerializationBus".to_string()));
    bus.on_raw(
        "RuntimeSerializationEvent",
        "returns_ok",
        |_event| async move { Ok(json!("ok")) },
    );

    let event = bus.emit(RuntimeSerializationEvent {
        ..Default::default()
    });
    let _ = block_on(event.now());

    let serialized = event.to_json_value();
    let base = event._inner_event();
    assert_eq!(
        serde_json::to_value(&*base).expect("base event serde"),
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
        ._inner_event()
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
    bus.destroy();
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
    let handler = bus.on_raw(
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
    bus.destroy();
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
            event_timeout: Some(0.0),
            event_slow_timeout: Some(34.0),
            event_handler_timeout: None,
            event_handler_slow_timeout: Some(12.0),
            event_handler_detect_file_paths: false,
            max_handler_recursion_depth: 2,
        },
    );
    let handler = bus.on_raw(
        "RuntimeSerializationEvent",
        "handler",
        |_event| async move { Ok(json!("ok")) },
    );
    let event = bus.emit(RuntimeSerializationEvent {
        ..Default::default()
    });
    let _ = block_on(event.now());

    let payload = bus.to_json_value();
    assert_eq!(serde_json::to_value(&*bus).expect("bus serde"), payload);
    assert_eq!(object_keys(&payload), expected_event_bus_json_keys());
    assert_eq!(payload["id"], "018f8e40-1234-7000-8000-000000001234");
    assert_eq!(payload["name"], "SerializableBus");
    assert_eq!(payload["max_history_size"], 500);
    assert_eq!(payload["max_history_drop"], false);
    assert_eq!(payload["event_concurrency"], "parallel");
    assert_eq!(payload["event_timeout"], 0.0);
    assert_eq!(payload["event_slow_timeout"], 34.0);
    assert_eq!(payload["event_handler_concurrency"], "parallel");
    assert_eq!(payload["event_handler_completion"], "first");
    assert_eq!(payload["event_handler_timeout"], Value::Null);
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

    let event_id = event.event_id.clone();
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
    restored.destroy();
    bus.destroy();
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
    let handler = bus.on_raw(
        "RuntimeSerializationEvent",
        "handler",
        |_event| async move { Ok(json!("ok")) },
    );
    let event = bus.emit(RuntimeSerializationEvent {
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
        restored_payload["handlers_by_key"]["RuntimeSerializationEvent"],
        json!([handler.id])
    );
    restored.destroy();
    bus.destroy();
}

#[test]
fn test_eventbus_model_dump_promotes_pending_events_into_event_history() {
    let bus = EventBus::new(Some("QueueOnlyBus".to_string()));
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let sent_started = Arc::new(AtomicBool::new(false));
    let sent_started_for_handler = sent_started.clone();

    bus.on_raw(
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

    let first = bus.emit(RuntimeSerializationEvent {
        ..Default::default()
    });
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("first handler should start");
    let second = bus.emit(RuntimeSerializationEvent {
        ..Default::default()
    });

    let first_id = first.event_id.clone();
    let second_id = second.event_id.clone();
    let payload = bus.to_json_value();
    let event_history = payload["event_history"].as_object().expect("history");
    assert!(event_history.contains_key(&first_id));
    assert!(event_history.contains_key(&second_id));
    assert_eq!(payload["pending_event_queue"], json!([second_id]));

    let _ = block_on(first.now());
    let _ = block_on(second.now());
    bus.destroy();
}

#[test]
fn test_eventbus_initialization() {
    let bus = EventBus::new(None);

    assert_eq!(bus.event_history_size(), 0);
    assert!(!bus.max_history_drop());
    assert_eq!(bus.event_history_ids().len(), 0);
    assert!(EventBus::all_instances_contains(&bus));
    bus.destroy();
}

#[test]
fn test_wait_until_idle_timeout_returns_after_timeout_when_work_is_still_in_flight() {
    let bus = EventBus::new(Some("WaitForIdleTimeoutBus".to_string()));
    bus.on_raw("WaitForIdleTimeoutEvent", "wait", |_event| async move {
        thread::sleep(Duration::from_millis(100));
        Ok(json!(null))
    });

    let event = bus.emit(WaitForIdleTimeoutEvent {
        ..Default::default()
    });
    let started = std::time::Instant::now();
    let became_idle = block_on(bus.wait_until_idle(Some(0.02)));
    let elapsed = started.elapsed();

    assert!(!became_idle);
    assert!(elapsed >= Duration::from_millis(15));
    assert!(elapsed < Duration::from_secs(1));
    assert!(!bus.is_idle_and_queue_empty());

    let _ = block_on(event.now());
    assert!(block_on(bus.wait_until_idle(None)));
    bus.destroy();
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
    bus.destroy();
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
    bus.destroy();
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
        let event = bus.emit(UserActionEvent {
            ..Default::default()
        });
        let _ = block_on(event.now());
    }

    assert_eq!(bus.event_history_size(), 150);
    bus.destroy();
}

#[test]
fn test_event_bus_with_zero_event_timeout_disables_timeouts() {
    let bus = EventBus::new_with_options(
        Some("NoTimeoutBus".to_string()),
        EventBusOptions {
            event_timeout: Some(0.0),
            ..EventBusOptions::default()
        },
    );

    assert_eq!(bus.event_timeout, Some(0.0));
    bus.destroy();
}

#[test]
fn test_event_bus_auto_generates_name_when_not_provided() {
    let bus = EventBus::new(None);
    assert!(bus.name.starts_with("EventBus_"));
    assert_eq!(
        bus.name,
        format!("EventBus_{}", &bus.id[bus.id.len() - 8..])
    );
    bus.destroy();
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
    bus.destroy();
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
    bus.destroy();
}

#[test]
fn test_handler_registration_via_string_class_and_wildcard() {
    test_handler_registration_by_string_matches_extend_name();
    test_class_matcher_matches_generic_base_event_by_event_type();
    test_wildcard_handler_receives_all_events();
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

    bus.on_raw("RecursiveEvent", "recursive_handler", move |event| {
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
                let _ = child.now().await;
            }
            Ok(json!(null))
        }
    });

    let event = bus.emit_base(base_event(
        "RecursiveEvent",
        json!({"level": 0, "max_level": 5}),
    ));
    let _ = block_on(event.wait());
    assert_eq!(
        seen_levels.lock().expect("seen levels lock").as_slice(),
        &[0, 1, 2, 3, 4, 5]
    );
    bus.destroy();
}

#[test]
fn test_default_handler_recursion_depth_still_catches_runaway_loops() {
    let bus = EventBus::new(Some("DefaultRecursionDepthBus".to_string()));
    let bus_for_handler = bus.clone();

    bus.on_raw("RecursiveEvent", "recursive_handler", move |event| {
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
                let _ = child.now().await;
            }
            Ok(json!(null))
        }
    });

    let event = bus.emit_base(base_event(
        "RecursiveEvent",
        json!({"level": 0, "max_level": 3}),
    ));
    let _ = block_on(event.wait());

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
    bus.destroy();
}

#[test]
fn test_base_event_lifecycle_methods_are_callable_and_preserve_lifecycle_behavior() {
    let bus = EventBus::new(Some("LifecycleMethodInvocationBus".to_string()));

    let standalone = BaseEvent::new("LifecycleMethodInvocationEvent", serde_json::Map::new());
    standalone.mark_started();
    assert_eq!(standalone.inner.lock().event_status, EventStatus::Started);
    standalone.mark_completed();
    assert_eq!(standalone.inner.lock().event_status, EventStatus::Completed);
    let _ = block_on(standalone.now());

    let dispatched = bus.emit(LifecycleMethodInvocationEvent {
        ..Default::default()
    });
    let _ = block_on(dispatched.now());
    assert_eq!(dispatched.event_status.read(), EventStatus::Completed);
    bus.destroy();
}

// Folded from legacy event_bus_tests.rs to keep the cross-language EventBus layout 1:1.
#[derive(Clone, Serialize, Deserialize)]
struct WorkResult {
    value: i64,
}

event! {
    struct WorkEvent {
        value: i64,
        event_result_type: WorkResult,
        event_type: "work",
    }
}
#[test]
fn test_emit_and_handler_result() {
    let bus = EventBus::new(Some("BusA".to_string()));
    bus.on_raw("work", "h1", |_event| async move { Ok(json!("ok")) });
    let event = bus.emit(WorkEvent {
        value: 1,
        ..Default::default()
    });
    let _ = block_on(event.now());

    let results = event.event_results.read();
    assert_eq!(results.len(), 1);
    let first = results.values().next().expect("missing first result");
    assert_eq!(first.result, Some(json!("ok")));
    bus.destroy();
}

#[test]
fn test_parallel_handler_concurrency() {
    let bus = EventBus::new(Some("BusPar".to_string()));

    bus.on_raw("work", "h1", |_event| async move {
        thread::sleep(Duration::from_millis(20));
        Ok(json!(1))
    });
    bus.on_raw("work", "h2", |_event| async move {
        thread::sleep(Duration::from_millis(20));
        Ok(json!(2))
    });

    let event = WorkEvent {
        value: 1,
        event_handler_concurrency: Some(EventHandlerConcurrencyMode::Parallel),
        event_concurrency: Some(EventConcurrencyMode::Parallel),
        ..Default::default()
    };
    let emitted = bus.emit(event);
    let _ = block_on(emitted.now());
    assert_eq!(emitted.event_results.read().len(), 2);
    bus.destroy();
}

// Folded from test_event_history_store.rs to keep test layout class-based.
mod folded_test_event_history_store {
    use abxbus_rust::event;
    use abxbus_rust::event_bus::EventBus;
    use futures::executor::block_on;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Serialize, Deserialize)]
    struct EmptyResult {}

    event! {
        struct HistoryEvent {
            event_result_type: EmptyResult,
            event_type: "history_event",
        }
    }
    #[test]
    fn test_max_history_drop_true_keeps_recent_entries() {
        let bus = EventBus::new_with_history(Some("HistoryDropBus".to_string()), Some(2), true);

        for _ in 0..3 {
            let event = bus.emit(HistoryEvent {
                ..Default::default()
            });
            let _ = block_on(event.now());
        }

        let history = bus.event_history_ids();
        assert_eq!(history.len(), 2);
        assert!(history.iter().any(|id| id.contains('-')));
        bus.destroy();
    }

    #[test]
    #[should_panic(expected = "history limit reached")]
    fn test_max_history_drop_false_rejects_new_emit_when_full() {
        let bus = EventBus::new_with_history(Some("HistoryRejectBus".to_string()), Some(1), false);

        let first = bus.emit(HistoryEvent {
            ..Default::default()
        });
        let _ = block_on(first.now());

        bus.emit(HistoryEvent {
            ..Default::default()
        });
    }
}

// Folded from test_eventbus_edge_cases.rs to keep test layout class-based.
mod folded_test_eventbus_edge_cases {
    use abxbus_rust::event;
    use abxbus_rust::{event_bus::EventBus, event_result::EventResultStatus, types::EventStatus};
    use futures::executor::block_on;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::{
        sync::{mpsc, Arc, Mutex},
        time::{Duration, Instant},
    };

    #[derive(Clone, Serialize, Deserialize)]
    struct EmptyResult {}

    event! {
        struct NothingEvent {
            event_result_type: EmptyResult,
            event_type: "nothing",
        }
    }
    event! {
        struct SpecificEvent {
            event_result_type: EmptyResult,
            event_type: "specific_event",
        }
    }
    event! {
        struct WorkEvent {
            event_result_type: EmptyResult,
            event_type: "work",
        }
    }
    event! {
        struct ResetCoverageEvent {
            label: String,
            event_result_type: EmptyResult,
            event_type: "ResetCoverageEvent",
        }
    }
    event! {
        struct IdleTimeoutCoverageEvent {
            event_result_type: EmptyResult,
            event_type: "IdleTimeoutCoverageEvent",
        }
    }
    event! {
        struct DestroyCoverageEvent {
            event_result_type: EmptyResult,
            event_type: "DestroyCoverageEvent",
        }
    }
    #[test]
    fn test_event_reset_creates_fresh_pending_event_for_cross_bus_dispatch() {
        let bus_a = EventBus::new(Some("ResetCoverageBusA".to_string()));
        let bus_b = EventBus::new(Some("ResetCoverageBusB".to_string()));
        let seen_a = Arc::new(Mutex::new(Vec::<String>::new()));
        let seen_b = Arc::new(Mutex::new(Vec::<String>::new()));
        let seen_a_for_handler = seen_a.clone();
        let seen_b_for_handler = seen_b.clone();

        bus_a.on_raw("ResetCoverageEvent", "record_a", move |event| {
            let seen_a = seen_a_for_handler.clone();
            async move {
                let label = event
                    .inner
                    .lock()
                    .payload
                    .get("label")
                    .and_then(|value| value.as_str())
                    .expect("label")
                    .to_string();
                seen_a.lock().expect("seen_a lock").push(label);
                Ok(json!(null))
            }
        });
        bus_b.on_raw("ResetCoverageEvent", "record_b", move |event| {
            let seen_b = seen_b_for_handler.clone();
            async move {
                let label = event
                    .inner
                    .lock()
                    .payload
                    .get("label")
                    .and_then(|value| value.as_str())
                    .expect("label")
                    .to_string();
                seen_b.lock().expect("seen_b lock").push(label);
                Ok(json!(null))
            }
        });

        let completed = bus_a.emit(ResetCoverageEvent {
            label: "hello".to_string(),
            ..Default::default()
        });
        let _ = block_on(completed.now());
        assert_eq!(completed.event_status.read(), EventStatus::Completed);
        assert_eq!(completed.event_results.read().len(), 1);

        let fresh = completed.event_reset();
        assert_ne!(
            fresh._inner_event().inner.lock().event_id,
            completed._inner_event().inner.lock().event_id
        );
        assert_eq!(fresh.event_status.read(), EventStatus::Pending);
        assert!(fresh.event_started_at.read().is_none());
        assert!(fresh.event_completed_at.read().is_none());
        assert_eq!(fresh.event_results.read().len(), 0);

        let forwarded = bus_b.emit(fresh);
        let _ = block_on(forwarded.now());

        assert_eq!(
            seen_a.lock().expect("seen_a lock").as_slice(),
            &["hello".to_string()]
        );
        assert_eq!(
            seen_b.lock().expect("seen_b lock").as_slice(),
            &["hello".to_string()]
        );
        let event_path = forwarded.event_path.read();
        assert!(event_path
            .iter()
            .any(|path| path.starts_with("ResetCoverageBusA#")));
        assert!(event_path
            .iter()
            .any(|path| path.starts_with("ResetCoverageBusB#")));
        bus_a.destroy();
        bus_b.destroy();
    }

    #[test]
    fn test_wait_until_idle_timeout_path_recovers_after_inflight_handler_finishes() {
        let bus = EventBus::new(Some("IdleTimeoutCoverageBus".to_string()));
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Arc::new(Mutex::new(release_rx));

        bus.on_raw("IdleTimeoutCoverageEvent", "slow_handler", move |_event| {
            let started_tx = started_tx.clone();
            let release_rx = release_rx.clone();
            async move {
                let _ = started_tx.send(());
                release_rx
                    .lock()
                    .expect("release lock")
                    .recv_timeout(Duration::from_secs(2))
                    .expect("release handler");
                Ok(json!(null))
            }
        });

        let pending = bus.emit(IdleTimeoutCoverageEvent {
            ..Default::default()
        });
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("handler should start");

        let start = Instant::now();
        let idle = block_on(bus.wait_until_idle(Some(0.01)));
        let elapsed = start.elapsed();
        assert!(!idle);
        assert!(elapsed < Duration::from_millis(500));
        assert_ne!(pending.event_status.read(), EventStatus::Completed);

        release_tx.send(()).expect("release handler");
        let _ = block_on(pending.now());
        assert!(block_on(bus.wait_until_idle(Some(1.0))));
        assert_eq!(pending.event_status.read(), EventStatus::Completed);
        bus.destroy();
    }

    #[test]
    fn test_destroy_clears_running_bus_and_releases_name() {
        let bus_name = "DestroyCoverageBus".to_string();
        let bus = EventBus::new(Some(bus_name.clone()));
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Arc::new(Mutex::new(release_rx));

        bus.on_raw("DestroyCoverageEvent", "slow_handler", move |_event| {
            let started_tx = started_tx.clone();
            let release_rx = release_rx.clone();
            async move {
                let _ = started_tx.send(());
                let _ = release_rx
                    .lock()
                    .expect("release lock")
                    .recv_timeout(Duration::from_millis(200));
                Ok(json!(null))
            }
        });

        let _pending = bus.emit(DestroyCoverageEvent {
            ..Default::default()
        });
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("handler should start");

        let start = Instant::now();
        bus.destroy();
        let elapsed = start.elapsed();
        assert!(elapsed < Duration::from_millis(500));
        assert!(bus.is_destroyed_for_test());
        assert!(!EventBus::all_instances_contains(&bus));

        release_tx.send(()).expect("release handler");

        let replacement = EventBus::new(Some(bus_name));
        replacement.on_raw("DestroyCoverageEvent", "handler", |_event| async move {
            Ok(json!(null))
        });
        let event = replacement.emit(DestroyCoverageEvent {
            ..Default::default()
        });
        let _ = block_on(event.now());
        assert_eq!(event.event_status.read(), EventStatus::Completed);
        replacement.destroy();
    }

    #[test]
    fn test_emit_with_no_handlers_completes_event() {
        let bus = EventBus::new(Some("NoHandlers".to_string()));
        let event = bus.emit(NothingEvent {
            ..Default::default()
        });

        let _ = block_on(event.now());

        let base = event._inner_event();
        let inner = base.inner.lock();
        assert_eq!(inner.event_results.len(), 0);
        assert_eq!(inner.event_pending_bus_count, 0);
        assert!(inner.event_started_at.is_some());
        assert!(inner.event_completed_at.is_some());
        drop(inner);
        bus.destroy();
    }

    #[test]
    fn test_wildcard_handler_runs_for_any_event_type() {
        let bus = EventBus::new(Some("WildcardBus".to_string()));
        bus.on_raw("*", "catch_all", |_event| async move { Ok(json!("all")) });
        let event = bus.emit(SpecificEvent {
            ..Default::default()
        });

        let _ = block_on(event.now());

        let results = event.event_results.read();
        assert_eq!(results.len(), 1);
        let only = results.values().next().expect("missing result");
        assert_eq!(only.result, Some(json!("all")));
        bus.destroy();
    }

    #[test]
    fn test_handler_error_populates_error_status() {
        let bus = EventBus::new(Some("ErrorBus".to_string()));
        bus.on_raw(
            "work",
            "bad",
            |_event| async move { Err("boom".to_string()) },
        );
        let event = bus.emit(WorkEvent {
            ..Default::default()
        });

        let _ = block_on(event.now());

        let results = event.event_results.read();
        assert_eq!(results.len(), 1);
        let only = results.values().next().expect("missing result");
        assert_eq!(only.status, EventResultStatus::Error);
        assert_eq!(only.error.as_deref(), Some("boom"));
        bus.destroy();
    }
}

// Folded from test_eventbus_name_conflict_gc.rs to keep test layout class-based.
mod folded_test_eventbus_name_conflict_gc {
    use abxbus_rust::event;
    use std::{
        collections::BTreeSet,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Barrier, Weak,
        },
        thread,
        time::{Duration, Instant},
    };

    use abxbus_rust::event_bus::{EventBus, EventBusOptions};
    use futures::executor::block_on;
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    #[derive(Clone, Serialize, Deserialize)]
    struct EmptyResult {}

    static NEXT_BUS_NAME: AtomicUsize = AtomicUsize::new(1);

    fn unique_bus_name(prefix: &str) -> String {
        format!("{prefix}_{}", NEXT_BUS_NAME.fetch_add(1, Ordering::Relaxed))
    }

    fn assert_eventually_collected(weak_ref: &Weak<EventBus>) {
        let deadline = Instant::now() + Duration::from_millis(500);
        while weak_ref.upgrade().is_some() && Instant::now() < deadline {
            thread::yield_now();
            thread::sleep(Duration::from_millis(1));
        }
        assert!(weak_ref.upgrade().is_none());
    }

    event! {
        struct GcHistoryEvent {
            event_result_type: EmptyResult,
            event_type: "GcHistoryEvent",
        }
    }
    event! {
        struct GcImplicitEvent {
            event_result_type: EmptyResult,
            event_type: "GcImplicitEvent",
        }
    }
    #[test]
    fn test_name_conflict_with_live_reference() {
        let requested_name = unique_bus_name("GCTestConflict");
        let bus1 = EventBus::new(Some(requested_name.clone()));
        let bus2 = EventBus::new(Some(requested_name.clone()));

        assert_eq!(bus1.name, requested_name);
        assert!(bus2.name.starts_with(&format!("{requested_name}_")));
        assert_ne!(bus2.name, requested_name);
        assert_eq!(bus2.name.len(), requested_name.len() + 1 + 8);
        bus1.destroy();
        bus2.destroy();
    }

    #[test]
    fn test_name_no_conflict_after_deletion() {
        let requested_name = unique_bus_name("GCTestBus1");
        let weak_ref = {
            let bus1 = EventBus::new(Some(requested_name.clone()));
            Arc::downgrade(&bus1)
        };
        assert_eventually_collected(&weak_ref);

        let bus2 = EventBus::new(Some(requested_name.clone()));
        assert_eq!(bus2.name, requested_name);
        bus2.destroy();
    }

    #[test]
    fn test_name_no_conflict_with_no_reference() {
        let requested_name = unique_bus_name("GCTestBus2");
        {
            let _ = EventBus::new(Some(requested_name.clone()));
        }

        let bus2 = EventBus::new(Some(requested_name.clone()));
        assert_eq!(bus2.name, requested_name);
        bus2.destroy();
    }

    #[test]
    fn test_name_conflict_with_weak_reference_only() {
        let requested_name = unique_bus_name("GCTestBus3");
        let weak_ref = {
            let bus1 = EventBus::new(Some(requested_name.clone()));
            let weak_ref = Arc::downgrade(&bus1);
            assert!(weak_ref.upgrade().is_some());
            weak_ref
        };

        assert_eventually_collected(&weak_ref);
        let bus2 = EventBus::new(Some(requested_name.clone()));
        assert_eq!(bus2.name, requested_name);
        bus2.destroy();
    }

    #[test]
    fn test_multiple_buses_with_gc() {
        let name1 = unique_bus_name("GCMulti1");
        let name2 = unique_bus_name("GCMulti2");
        let name3 = unique_bus_name("GCMulti3");
        let name4 = unique_bus_name("GCMulti4");
        let bus1 = EventBus::new(Some(name1.clone()));
        {
            let _ = EventBus::new(Some(name2.clone()));
        }
        let bus3 = EventBus::new(Some(name3.clone()));
        {
            let _ = EventBus::new(Some(name4.clone()));
        }

        let bus2_new = EventBus::new(Some(name2.clone()));
        let bus4_new = EventBus::new(Some(name4.clone()));
        assert_eq!(bus2_new.name, name2);
        assert_eq!(bus4_new.name, name4);

        let bus1_conflict = EventBus::new(Some(name1.clone()));
        assert!(bus1_conflict.name.starts_with(&format!("{name1}_")));
        assert_ne!(bus1_conflict.name, bus1.name);

        let bus3_conflict = EventBus::new(Some(name3.clone()));
        assert!(bus3_conflict.name.starts_with(&format!("{name3}_")));
        assert_ne!(bus3_conflict.name, bus3.name);

        bus1.destroy();
        bus2_new.destroy();
        bus3.destroy();
        bus4_new.destroy();
        bus1_conflict.destroy();
        bus3_conflict.destroy();
    }

    #[test]
    fn test_name_conflict_after_destroy_and_clear() {
        let requested_name = unique_bus_name("GCDestroyClear");
        let bus1 = EventBus::new(Some(requested_name.clone()));
        bus1.destroy();

        let bus2 = EventBus::new(Some(requested_name.clone()));
        assert_eq!(bus2.name, requested_name);
        bus2.destroy();
    }

    #[test]
    fn test_weakset_behavior() {
        let bus1 = EventBus::new(Some(unique_bus_name("WeakTest1")));
        let bus2 = EventBus::new(Some(unique_bus_name("WeakTest2")));
        let bus3 = EventBus::new(Some(unique_bus_name("WeakTest3")));
        let bus2_id = bus2.id.clone();
        let weak2 = Arc::downgrade(&bus2);

        assert!(EventBus::all_instances_contains(&bus1));
        assert!(EventBus::all_instances_contains(&bus2));
        assert!(EventBus::all_instances_contains(&bus3));

        drop(bus2);
        assert_eventually_collected(&weak2);
        EventBus::all_instances_len();
        assert!(EventBus::live_instance_by_id(&bus2_id).is_none());
        assert!(EventBus::all_instances_contains(&bus1));
        assert!(EventBus::all_instances_contains(&bus3));
        bus1.destroy();
        bus3.destroy();
    }

    #[test]
    fn test_eventbus_removed_from_weakset() {
        let requested_name = unique_bus_name("GCDeadBus");
        {
            let _ = EventBus::new(Some(requested_name.clone()));
        }

        let bus = EventBus::new(Some(requested_name.clone()));
        assert_eq!(bus.name, requested_name);
        assert!(EventBus::all_instances_contains(&bus));
        bus.destroy();
    }

    #[test]
    fn test_concurrent_name_creation() {
        let workers = 8;
        let requested_name = unique_bus_name("ConcurrentTest");
        let barrier = Arc::new(Barrier::new(workers));
        let handles = (0..workers)
            .map(|_| {
                let barrier = barrier.clone();
                let requested_name = requested_name.clone();
                thread::spawn(move || {
                    barrier.wait();
                    EventBus::new(Some(requested_name))
                })
            })
            .collect::<Vec<_>>();

        let buses = handles
            .into_iter()
            .map(|handle| handle.join().expect("worker creates bus"))
            .collect::<Vec<_>>();
        let names = buses.iter().map(|bus| bus.name.clone()).collect::<Vec<_>>();
        let unique_names = names.iter().cloned().collect::<BTreeSet<_>>();

        assert_eq!(unique_names.len(), workers);
        assert!(unique_names.contains(&requested_name));
        assert!(unique_names.iter().all(|name| {
            name == &requested_name
                || (name.starts_with(&format!("{requested_name}_"))
                    && name.len() == requested_name.len() + 1 + 8)
        }));

        for bus in buses {
            bus.destroy();
        }
    }

    #[test]
    fn test_unreferenced_buses_with_history_can_be_cleaned_without_instance_leak() {
        let prefix = unique_bus_name("GCNoDestroyBus");
        let mut refs = Vec::new();
        let mut ids = Vec::new();

        for index in 0..10 {
            let bus = EventBus::new_with_options(
                Some(format!("{prefix}_{index}")),
                EventBusOptions {
                    max_history_size: Some(40),
                    ..EventBusOptions::default()
                },
            );
            ids.push(bus.id.clone());
            bus.on_raw("GcHistoryEvent", "history_handler", |_event| async move {
                Ok(json!("ok"))
            });
            for _ in 0..20 {
                let event = bus.emit(GcHistoryEvent {
                    ..Default::default()
                });
                let _ = block_on(event.now());
            }
            block_on(bus.wait_until_idle(Some(1.0)));
            refs.push(Arc::downgrade(&bus));
            bus.destroy();
        }

        for weak_ref in &refs {
            assert_eventually_collected(weak_ref);
        }
        EventBus::all_instances_len();
        assert!(ids
            .iter()
            .all(|eventbus_id| EventBus::live_instance_by_id(eventbus_id).is_none()));
    }

    #[test]
    fn test_unreferenced_buses_with_history_are_collected_without_destroy() {
        let prefix = unique_bus_name("GCImplicitNoDestroy");
        let mut refs = Vec::new();
        let mut ids = Vec::new();

        for index in 0..10 {
            let bus = EventBus::new_with_options(
                Some(format!("{prefix}_{index}")),
                EventBusOptions {
                    max_history_size: Some(30),
                    ..EventBusOptions::default()
                },
            );
            ids.push(bus.id.clone());
            bus.on_raw("GcImplicitEvent", "implicit_handler", |_event| async move {
                Ok(json!("ok"))
            });
            for _ in 0..20 {
                let event = bus.emit(GcImplicitEvent {
                    ..Default::default()
                });
                let _ = block_on(event.now());
            }
            block_on(bus.wait_until_idle(Some(1.0)));
            refs.push(Arc::downgrade(&bus));
        }

        for weak_ref in &refs {
            assert_eventually_collected(weak_ref);
        }
        EventBus::all_instances_len();
        assert!(ids
            .iter()
            .all(|eventbus_id| EventBus::live_instance_by_id(eventbus_id).is_none()));
    }
}
