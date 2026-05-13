use abxbus_rust::event;
use std::{
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc, Mutex, MutexGuard, OnceLock,
    },
    thread,
    time::Duration,
};

use abxbus_rust::{
    base_event::{now_iso, BaseEvent, EventResultOptions, EventWaitOptions},
    event_bus::{EventBus, EventBusOptions},
    event_result::EventResultStatus,
    typed::IntoBaseEventHandle,
    types::{EventConcurrencyMode, EventHandlerConcurrencyMode, EventStatus},
};
use futures::executor::block_on;
use serde_json::{json, Map, Value};

fn test_guard() -> MutexGuard<'static, ()> {
    static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("base event test lock")
}

event! {
    struct BaseEventNowRaisesFirstErrorEvent {
        event_result_type: String,
        event_type: "BaseEventNowRaisesFirstErrorEvent",
    }
}
event! {
    struct DeferredEmitAfterCompletionParentEvent {
        event_result_type: String,
        event_type: "DeferredEmitAfterCompletionParentEvent",
    }
}
event! {
    struct DeferredEmitAfterCompletionChildEvent {
        event_result_type: String,
        event_type: "DeferredEmitAfterCompletionChildEvent",
    }
}
event! {
    struct BaseEventEventResultUpdateEvent {
        event_result_type: String,
        event_type: "BaseEventEventResultUpdateEvent",
    }
}
event! {
    struct BaseEventEventResultUpdateStatusOnlyEvent {
        event_result_type: String,
        event_type: "BaseEventEventResultUpdateStatusOnlyEvent",
    }
}
event! {
    struct BaseEventAllowedEventConfigEvent {
        event_result_type: String,
        event_type: "BaseEventAllowedEventConfigEvent",
        event_timeout: 123.0,
        event_slow_timeout: 9.0,
        event_handler_timeout: 45.0,
    }
}
event! {
    struct BaseEventImmediateParentEvent {
        event_result_type: String,
        event_type: "BaseEventImmediateParentEvent",
    }
}
event! {
    struct BaseEventImmediateChildEvent {
        event_result_type: String,
        event_type: "BaseEventImmediateChildEvent",
    }
}
event! {
    struct BaseEventImmediateSiblingEvent {
        event_result_type: String,
        event_type: "BaseEventImmediateSiblingEvent",
    }
}
event! {
    struct BaseEventParallelImmediateParentEvent {
        event_result_type: String,
        event_type: "BaseEventParallelImmediateParentEvent",
    }
}
event! {
    struct BaseEventParallelImmediateChildEvent1 {
        event_result_type: String,
        event_type: "BaseEventParallelImmediateChildEvent1",
    }
}
event! {
    struct BaseEventParallelImmediateChildEvent2 {
        event_result_type: String,
        event_type: "BaseEventParallelImmediateChildEvent2",
    }
}
event! {
    struct BaseEventParallelImmediateChildEvent3 {
        event_result_type: String,
        event_type: "BaseEventParallelImmediateChildEvent3",
    }
}
event! {
    struct BaseEventQueuedParentEvent {
        event_result_type: String,
        event_type: "BaseEventQueuedParentEvent",
    }
}
event! {
    struct BaseEventQueuedChildEvent {
        event_result_type: String,
        event_type: "BaseEventQueuedChildEvent",
    }
}
event! {
    struct BaseEventQueuedSiblingEvent {
        event_result_type: String,
        event_type: "BaseEventQueuedSiblingEvent",
    }
}
event! {
    struct PassiveSerialParentEvent {
        event_result_type: String,
        event_type: "PassiveSerialParentEvent",
    }
}
event! {
    struct PassiveSerialEmittedEvent {
        event_result_type: String,
        event_type: "PassiveSerialEmittedEvent",
    }
}
event! {
    struct PassiveSerialFoundEvent {
        event_result_type: String,
        event_type: "PassiveSerialFoundEvent",
    }
}
event! {
    struct EventCompletedSerialDeadlockWarningParentEvent {
        event_result_type: String,
        event_type: "EventCompletedSerialDeadlockWarningParentEvent",
    }
}
event! {
    struct EventCompletedSerialDeadlockWarningChildEvent {
        event_result_type: String,
        event_type: "EventCompletedSerialDeadlockWarningChildEvent",
    }
}
event! {
    struct PassiveParallelParentEvent {
        event_result_type: String,
        event_type: "PassiveParallelParentEvent",
    }
}
event! {
    struct PassiveParallelEmittedEvent {
        event_result_type: String,
        event_type: "PassiveParallelEmittedEvent",
    }
}
event! {
    struct PassiveParallelFoundEvent {
        event_result_type: String,
        event_type: "PassiveParallelFoundEvent",
    }
}
event! {
    struct FutureParallelSomeOtherEvent {
        event_result_type: String,
        event_type: "FutureParallelSomeOtherEvent",
    }
}
event! {
    struct FutureParallelEvent {
        event_result_type: String,
        event_type: "FutureParallelEvent",
    }
}
event! {
    struct WaitOutsideHandlerBlockerEvent {
        event_result_type: String,
        event_type: "WaitOutsideHandlerBlockerEvent",
    }
}
event! {
    struct WaitOutsideHandlerTargetEvent {
        event_result_type: String,
        event_type: "WaitOutsideHandlerTargetEvent",
    }
}
event! {
    struct WaitOutsideHandlerParallelBlockerEvent {
        event_result_type: String,
        event_type: "WaitOutsideHandlerParallelBlockerEvent",
    }
}
event! {
    struct WaitOutsideHandlerParallelTargetEvent {
        event_result_type: String,
        event_type: "WaitOutsideHandlerParallelTargetEvent",
    }
}
event! {
    struct EventCompletedTimeoutEvent {
        event_result_type: String,
        event_type: "EventCompletedTimeoutEvent",
    }
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

fn wait_until_bool(flag: &AtomicBool) {
    let started = std::time::Instant::now();
    while !flag.load(Ordering::SeqCst) {
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "timed out waiting for flag"
        );
        thread::sleep(Duration::from_millis(1));
    }
}

fn base_event_deadlock_warning_child_enabled() -> bool {
    std::env::var("ABXBUS_RUN_BASE_EVENT_DEADLOCK_WARNING_CHILD").as_deref() == Ok("1")
}

fn run_base_event_deadlock_warning_child(test_name: &str) -> String {
    let output = Command::new(std::env::current_exe().expect("current test binary"))
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .env("ABXBUS_RUN_BASE_EVENT_DEADLOCK_WARNING_CHILD", "1")
        .output()
        .expect("run base event deadlock warning child test");
    assert!(
        output.status.success(),
        "child test {test_name} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stderr).to_string()
}

#[test]
fn test_baseevent_lifecycle_transitions_are_explicit_and_awaitable() {
    let _guard = test_guard();
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
    let _ = block_on(event.wait());
}

#[test]
fn test_event_result_re_raises_first_processing_exception_after_completion() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("BaseEventNowRaisesFirstErrorBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            event_timeout: Some(0.0),
            ..EventBusOptions::default()
        },
    );

    bus.on_raw(
        "BaseEventNowRaisesFirstErrorEvent",
        "first_failure",
        |_event| async {
            thread::sleep(Duration::from_millis(1));
            Err("first failure".to_string())
        },
    );
    bus.on_raw(
        "BaseEventNowRaisesFirstErrorEvent",
        "second_failure",
        |_event| async {
            thread::sleep(Duration::from_millis(10));
            Err("second failure".to_string())
        },
    );

    let event = bus.emit(BaseEventNowRaisesFirstErrorEvent {
        ..Default::default()
    });
    block_on(event.inner.now()).expect("complete error event");
    let error = block_on(
        event
            .inner
            .event_result_with_options(EventResultOptions::default()),
    )
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
    bus.destroy();
}

#[test]
fn test_event_result_update_creates_and_updates_typed_handler_results() {
    let _guard = test_guard();
    let bus = EventBus::new(Some("BaseEventEventResultUpdateBus".to_string()));
    let event = BaseEvent::new("BaseEventEventResultUpdateEvent", Map::new());
    let handler_entry = bus.on_raw(
        "BaseEventEventResultUpdateEvent",
        "handler",
        |_event| async { Ok(json!("ok")) },
    );

    let pending = event.event_result_update(
        &handler_entry,
        Some(EventResultStatus::Pending),
        None,
        None,
        None,
    );
    assert_eq!(
        event
            .inner
            .lock()
            .event_results
            .get(&handler_entry.id)
            .expect("pending result")
            .id,
        pending.id
    );
    assert_eq!(pending.status, EventResultStatus::Pending);

    let completed = event.event_result_update(
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
    bus.destroy();
}

#[test]
fn test_event_result_update_status_only_preserves_existing_error_and_result() {
    let _guard = test_guard();
    let bus = EventBus::new(Some("BaseEventEventResultUpdateStatusOnlyBus".to_string()));
    let event = BaseEvent::new("BaseEventEventResultUpdateStatusOnlyEvent", Map::new());
    let handler_entry = bus.on_raw(
        "BaseEventEventResultUpdateStatusOnlyEvent",
        "handler",
        |_event| async { Ok(json!("ok")) },
    );

    let errored = event.event_result_update(
        &handler_entry,
        None,
        None,
        Some(Some("RuntimeError: seeded error".to_string())),
        None,
    );
    assert_eq!(errored.status, EventResultStatus::Error);
    assert_eq!(errored.error.as_deref(), Some("RuntimeError: seeded error"));

    let status_only = event.event_result_update(
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
    bus.destroy();
}

#[test]
fn test_base_event_now_inside_handler_no_args() {
    let _guard = test_guard();
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
            bus.emit(BaseEventImmediateSiblingEvent {
                ..Default::default()
            });
            let child = bus.emit_child(BaseEventImmediateChildEvent {
                ..Default::default()
            });
            let _ = child.now().await;
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

    let parent = bus.emit(BaseEventImmediateParentEvent {
        ..Default::default()
    });
    let _ = block_on(parent.now());
    block_on(bus.wait_until_idle(Some(2.0)));

    assert_eq!(
        order.lock().expect("order lock").as_slice(),
        ["parent_start", "child", "parent_end", "sibling"]
    );
    bus.destroy();
}

#[test]
fn test_base_event_now_inside_handler_with_args() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("BaseEventImmediateQueueJumpArgsBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));
    let child_failed = Arc::new(AtomicBool::new(false));

    let bus_for_parent = bus.clone();
    let order_for_parent = order.clone();
    bus.on_raw(
        "BaseEventImmediateParentEvent",
        "parent_with_args",
        move |_event| {
            let bus = bus_for_parent.clone();
            let order = order_for_parent.clone();
            async move {
                push(&order, "parent_start");
                bus.emit(BaseEventImmediateSiblingEvent {
                    ..Default::default()
                });
                let child = bus.emit_child(BaseEventImmediateChildEvent {
                    ..Default::default()
                });
                child
                    .now_with_options(EventWaitOptions {
                        timeout: Some(1.0),
                        ..EventWaitOptions::default()
                    })
                    .await?;
                push(&order, "parent_end");
                Ok(json!("parent"))
            }
        },
    );

    let order_for_child = order.clone();
    let child_failed_for_child = child_failed.clone();
    bus.on_raw(
        "BaseEventImmediateChildEvent",
        "child_error",
        move |_event| {
            let order = order_for_child.clone();
            let child_failed = child_failed_for_child.clone();
            async move {
                push(&order, "child");
                child_failed.store(true, Ordering::SeqCst);
                Err("child failure".to_string())
            }
        },
    );

    let order_for_sibling = order.clone();
    bus.on_raw("BaseEventImmediateSiblingEvent", "sibling", move |_event| {
        let order = order_for_sibling.clone();
        async move {
            push(&order, "sibling");
            Ok(json!("sibling"))
        }
    });

    let parent = bus.emit(BaseEventImmediateParentEvent {
        ..Default::default()
    });
    block_on(parent.now()).expect("parent should complete");
    block_on(bus.wait_until_idle(Some(2.0)));

    assert_eq!(
        order.lock().expect("order lock").as_slice(),
        ["parent_start", "child", "parent_end", "sibling"]
    );
    assert!(child_failed.load(Ordering::SeqCst));
    bus.destroy();
}

#[test]
fn test_base_event_now_outside_handler_no_args() {
    let _guard = test_guard();
    let bus = EventBus::new(Some("BaseEventNowOutsideNoArgsBus".to_string()));
    bus.on_raw(
        "BaseEventNowRaisesFirstErrorEvent",
        "failing_handler",
        |_event| async move { Err("outside failure".to_string()) },
    );

    let event = bus.emit(BaseEventNowRaisesFirstErrorEvent {
        ..Default::default()
    });
    block_on(event.now()).expect("now should wait for completion without raising handler errors");
    let error = block_on(event.event_result_with_options(EventResultOptions::default()))
        .expect_err("event_result should raise outside handler errors");
    assert_eq!(error, "outside failure");
    assert_eq!(
        event.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    bus.destroy();
}

#[test]
fn test_base_event_now_outside_handler_with_args() {
    let _guard = test_guard();
    let bus = EventBus::new(Some("BaseEventNowOutsideArgsBus".to_string()));
    bus.on_raw(
        "BaseEventNowRaisesFirstErrorEvent",
        "failing_handler",
        |_event| async move { Err("outside suppressed failure".to_string()) },
    );

    let event = bus.emit(BaseEventNowRaisesFirstErrorEvent {
        ..Default::default()
    });
    block_on(event.inner.now_with_options(EventWaitOptions {
        timeout: Some(1.0),
        ..EventWaitOptions::default()
    }))
    .expect("raise_if_any=false should only wait for completion");
    assert_eq!(
        event.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    bus.destroy();
}

#[test]
fn test_now_outside_handler_queue_jumps_queued_execution() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("WaitOutsideHandlerQueueOrderBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));
    let blocker_started = Arc::new(AtomicBool::new(false));
    let release_blocker = Arc::new(AtomicBool::new(false));

    let order_for_blocker = order.clone();
    let blocker_started_for_handler = blocker_started.clone();
    let release_blocker_for_handler = release_blocker.clone();
    bus.on_raw("WaitOutsideHandlerBlockerEvent", "blocker", move |_event| {
        let order = order_for_blocker.clone();
        let blocker_started = blocker_started_for_handler.clone();
        let release_blocker = release_blocker_for_handler.clone();
        async move {
            push(&order, "blocker_start");
            blocker_started.store(true, Ordering::SeqCst);
            while !release_blocker.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(1));
            }
            push(&order, "blocker_end");
            Ok(json!(null))
        }
    });

    let order_for_target = order.clone();
    bus.on_raw("WaitOutsideHandlerTargetEvent", "target", move |_event| {
        let order = order_for_target.clone();
        async move {
            push(&order, "target");
            Ok(json!(null))
        }
    });

    bus.emit(WaitOutsideHandlerBlockerEvent {
        ..Default::default()
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while !blocker_started.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }
    assert!(blocker_started.load(Ordering::SeqCst));

    let target = bus.emit(WaitOutsideHandlerTargetEvent {
        ..Default::default()
    });
    let target_for_wait = target.clone();
    let now_thread = thread::spawn(move || block_on(target_for_wait.now()));
    thread::sleep(Duration::from_millis(50));
    assert_eq!(
        order.lock().expect("order lock").as_slice(),
        ["blocker_start", "target"]
    );
    release_blocker.store(true, Ordering::SeqCst);
    assert!(now_thread
        .join()
        .expect("now thread")
        .map(|event| Arc::ptr_eq(&event.inner, &target.inner))
        .unwrap_or(false));
    block_on(bus.wait_until_idle(Some(2.0)));
    assert_eq!(
        order.lock().expect("order lock").as_slice(),
        ["blocker_start", "target", "blocker_end"]
    );
    bus.destroy();
}

#[test]
fn test_now_outside_handler_allows_normal_parallel_processing() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("WaitOutsideHandlerParallelQueueOrderBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));
    let blocker_started = Arc::new(AtomicBool::new(false));
    let release_blocker = Arc::new(AtomicBool::new(false));

    let order_for_blocker = order.clone();
    let blocker_started_for_handler = blocker_started.clone();
    let release_blocker_for_handler = release_blocker.clone();
    bus.on_raw(
        "WaitOutsideHandlerParallelBlockerEvent",
        "blocker",
        move |_event| {
            let order = order_for_blocker.clone();
            let blocker_started = blocker_started_for_handler.clone();
            let release_blocker = release_blocker_for_handler.clone();
            async move {
                push(&order, "blocker_start");
                blocker_started.store(true, Ordering::SeqCst);
                while !release_blocker.load(Ordering::SeqCst) {
                    thread::sleep(Duration::from_millis(1));
                }
                push(&order, "blocker_end");
                Ok(json!(null))
            }
        },
    );

    let order_for_target = order.clone();
    bus.on_raw(
        "WaitOutsideHandlerParallelTargetEvent",
        "target",
        move |_event| {
            let order = order_for_target.clone();
            async move {
                push(&order, "target");
                Ok(json!(null))
            }
        },
    );

    bus.emit(WaitOutsideHandlerParallelBlockerEvent {
        ..Default::default()
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while !blocker_started.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }
    assert!(blocker_started.load(Ordering::SeqCst));

    let target = bus.emit(WaitOutsideHandlerParallelTargetEvent {
        event_concurrency: Some(EventConcurrencyMode::Parallel),
        ..Default::default()
    });
    let target_for_wait = target.clone();
    let done_thread = thread::spawn(move || block_on(target_for_wait.now()));
    thread::sleep(Duration::from_millis(50));
    assert_eq!(
        order.lock().expect("order lock").as_slice(),
        ["blocker_start", "target"]
    );
    release_blocker.store(true, Ordering::SeqCst);
    assert!(done_thread
        .join()
        .expect("done thread")
        .map(|event| Arc::ptr_eq(&event.inner, &target.inner))
        .unwrap_or(false));
    block_on(bus.wait_until_idle(Some(2.0)));
    assert_eq!(
        order.lock().expect("order lock").as_slice(),
        ["blocker_start", "target", "blocker_end"]
    );
    bus.destroy();
}

#[test]
fn test_wait_returns_event_without_forcing_queued_execution() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("WaitPassiveQueueOrderBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));
    let blocker_started = Arc::new(AtomicBool::new(false));
    let release_blocker = Arc::new(AtomicBool::new(false));

    let order_for_blocker = order.clone();
    let blocker_started_for_handler = blocker_started.clone();
    let release_blocker_for_handler = release_blocker.clone();
    bus.on_raw("WaitPassiveBlockerEvent", "blocker", move |_event| {
        let order = order_for_blocker.clone();
        let blocker_started = blocker_started_for_handler.clone();
        let release_blocker = release_blocker_for_handler.clone();
        async move {
            push(&order, "blocker_start");
            blocker_started.store(true, Ordering::SeqCst);
            while !release_blocker.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(1));
            }
            push(&order, "blocker_end");
            Ok(json!(null))
        }
    });
    let order_for_target = order.clone();
    bus.on_raw("WaitPassiveTargetEvent", "target", move |_event| {
        let order = order_for_target.clone();
        async move {
            push(&order, "target");
            Ok(json!("target"))
        }
    });

    bus.emit_base(BaseEvent::new("WaitPassiveBlockerEvent", Map::new()));
    wait_until_bool(&blocker_started);
    let target = bus.emit_base(BaseEvent::new("WaitPassiveTargetEvent", Map::new()));
    let target_for_wait = target.clone();
    let wait_thread = thread::spawn(move || {
        block_on(target_for_wait.wait_with_options(EventWaitOptions {
            timeout: Some(1.0),
            first_result: false,
        }))
    });
    thread::sleep(Duration::from_millis(50));
    assert_eq!(
        order.lock().expect("order lock").as_slice(),
        ["blocker_start"]
    );
    release_blocker.store(true, Ordering::SeqCst);
    assert!(wait_thread
        .join()
        .expect("wait thread")
        .map(|event| Arc::ptr_eq(&event, &target))
        .unwrap_or(false));
    assert_eq!(
        order.lock().expect("order lock").as_slice(),
        ["blocker_start", "blocker_end", "target"]
    );
    bus.destroy();
}

#[test]
fn test_now_returns_event_and_queue_jumps_queued_execution() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("NowActiveQueueJumpBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));
    let blocker_started = Arc::new(AtomicBool::new(false));
    let release_blocker = Arc::new(AtomicBool::new(false));

    let order_for_blocker = order.clone();
    let blocker_started_for_handler = blocker_started.clone();
    let release_blocker_for_handler = release_blocker.clone();
    bus.on_raw("NowActiveBlockerEvent", "blocker", move |_event| {
        let order = order_for_blocker.clone();
        let blocker_started = blocker_started_for_handler.clone();
        let release_blocker = release_blocker_for_handler.clone();
        async move {
            push(&order, "blocker_start");
            blocker_started.store(true, Ordering::SeqCst);
            while !release_blocker.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(1));
            }
            push(&order, "blocker_end");
            Ok(json!(null))
        }
    });
    let order_for_target = order.clone();
    bus.on_raw("NowActiveTargetEvent", "target", move |_event| {
        let order = order_for_target.clone();
        async move {
            push(&order, "target");
            Ok(json!("target"))
        }
    });

    bus.emit_base(BaseEvent::new("NowActiveBlockerEvent", Map::new()));
    wait_until_bool(&blocker_started);
    let target = bus.emit_base(BaseEvent::new("NowActiveTargetEvent", Map::new()));
    let target_for_now = target.clone();
    let now_thread = thread::spawn(move || {
        block_on(target_for_now.now_with_options(EventWaitOptions {
            timeout: Some(1.0),
            first_result: false,
        }))
    });
    thread::sleep(Duration::from_millis(50));
    assert_eq!(
        order.lock().expect("order lock").as_slice(),
        ["blocker_start", "target"]
    );
    assert!(now_thread
        .join()
        .expect("now thread")
        .map(|event| Arc::ptr_eq(&event, &target))
        .unwrap_or(false));
    release_blocker.store(true, Ordering::SeqCst);
    block_on(bus.wait_until_idle(Some(2.0)));
    assert_eq!(
        order.lock().expect("order lock").as_slice(),
        ["blocker_start", "target", "blocker_end"]
    );
    bus.destroy();
}

#[test]
fn test_wait_first_result_returns_before_event_completion() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("WaitFirstResultBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::Parallel,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            event_timeout: Some(0.0),
            ..EventBusOptions::default()
        },
    );
    let slow_finished = Arc::new(AtomicBool::new(false));
    bus.on_raw("WaitFirstResultEvent", "medium", |_event| async {
        thread::sleep(Duration::from_millis(30));
        Ok(json!("medium"))
    });
    bus.on_raw("WaitFirstResultEvent", "fast", |_event| async {
        thread::sleep(Duration::from_millis(10));
        Ok(json!("fast"))
    });
    let slow_finished_for_handler = slow_finished.clone();
    bus.on_raw("WaitFirstResultEvent", "slow", move |_event| {
        let slow_finished = slow_finished_for_handler.clone();
        async move {
            thread::sleep(Duration::from_millis(250));
            slow_finished.store(true, Ordering::SeqCst);
            Ok(json!("slow"))
        }
    });

    let target = BaseEvent::new("WaitFirstResultEvent", Map::new());
    target.inner.lock().event_concurrency = Some(EventConcurrencyMode::Parallel);
    let event = bus.emit_base(target);
    let waited = block_on(event.wait_with_options(EventWaitOptions {
        timeout: Some(1.0),
        first_result: true,
    }))
    .expect("wait first_result");
    assert!(Arc::ptr_eq(&waited, &event));
    assert_eq!(
        block_on(event.event_result_with_options(EventResultOptions {
            raise_if_any: false,
            ..EventResultOptions::default()
        }))
        .expect("event result"),
        Some(json!("fast"))
    );
    thread::sleep(Duration::from_millis(50));
    assert_eq!(
        block_on(event.event_results_list_with_options(EventResultOptions {
            raise_if_any: false,
            ..EventResultOptions::default()
        }))
        .expect("event results list"),
        vec![json!("medium"), json!("fast")]
    );
    assert!(!slow_finished.load(Ordering::SeqCst));
    assert_ne!(event.inner.lock().event_status, EventStatus::Completed);
    wait_until_bool(&slow_finished);
    bus.destroy();
}

#[test]
fn test_now_first_result_returns_before_event_completion() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("NowFirstResultBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            event_timeout: Some(0.0),
            ..EventBusOptions::default()
        },
    );
    let slow_finished = Arc::new(AtomicBool::new(false));
    let release_slow = Arc::new(AtomicBool::new(false));
    bus.on_raw("NowFirstResultEvent", "medium", |_event| async {
        thread::sleep(Duration::from_millis(30));
        Ok(json!("medium"))
    });
    bus.on_raw("NowFirstResultEvent", "fast", |_event| async {
        thread::sleep(Duration::from_millis(10));
        Ok(json!("fast"))
    });
    let slow_finished_for_handler = slow_finished.clone();
    let release_slow_for_handler = release_slow.clone();
    bus.on_raw("NowFirstResultEvent", "slow", move |_event| {
        let slow_finished = slow_finished_for_handler.clone();
        let release_slow = release_slow_for_handler.clone();
        async move {
            while !release_slow.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(1));
            }
            slow_finished.store(true, Ordering::SeqCst);
            Ok(json!("slow"))
        }
    });

    let target = BaseEvent::new("NowFirstResultEvent", Map::new());
    target.inner.lock().event_concurrency = Some(EventConcurrencyMode::Parallel);
    let event = bus.emit_base(target);
    let waited = block_on(event.now_with_options(EventWaitOptions {
        timeout: Some(1.0),
        first_result: true,
    }))
    .expect("now first_result");
    assert!(Arc::ptr_eq(&waited, &event));
    assert_eq!(
        block_on(event.event_result_with_options(EventResultOptions {
            raise_if_any: false,
            ..EventResultOptions::default()
        }))
        .expect("event result"),
        Some(json!("fast"))
    );
    thread::sleep(Duration::from_millis(50));
    assert_eq!(
        block_on(event.event_results_list_with_options(EventResultOptions {
            raise_if_any: false,
            ..EventResultOptions::default()
        }))
        .expect("event results list"),
        vec![json!("medium"), json!("fast")]
    );
    assert!(!slow_finished.load(Ordering::SeqCst));
    assert_ne!(event.inner.lock().event_status, EventStatus::Completed);
    release_slow.store(true, Ordering::SeqCst);
    wait_until_bool(&slow_finished);
    block_on(event.wait()).expect("event completed after slow handler release");
    assert_eq!(event.inner.lock().event_status, EventStatus::Completed);
    bus.destroy();
}

#[test]
fn test_event_result_starts_never_started_event_and_returns_first_result() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("EventResultShortcutQueueJumpBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));
    let blocker_started = Arc::new(AtomicBool::new(false));
    let release_blocker = Arc::new(AtomicBool::new(false));
    let order_for_blocker = order.clone();
    let blocker_started_for_handler = blocker_started.clone();
    let release_blocker_for_handler = release_blocker.clone();
    bus.on_raw(
        "EventResultShortcutBlockerEvent",
        "blocker",
        move |_event| {
            let order = order_for_blocker.clone();
            let blocker_started = blocker_started_for_handler.clone();
            let release_blocker = release_blocker_for_handler.clone();
            async move {
                push(&order, "blocker_start");
                blocker_started.store(true, Ordering::SeqCst);
                while !release_blocker.load(Ordering::SeqCst) {
                    thread::sleep(Duration::from_millis(1));
                }
                push(&order, "blocker_end");
                Ok(json!(null))
            }
        },
    );
    let order_for_target = order.clone();
    bus.on_raw("EventResultShortcutTargetEvent", "target", move |_event| {
        let order = order_for_target.clone();
        async move {
            push(&order, "target");
            Ok(json!("target"))
        }
    });

    bus.emit_base(BaseEvent::new(
        "EventResultShortcutBlockerEvent",
        Map::new(),
    ));
    wait_until_bool(&blocker_started);
    let target = bus.emit_base(BaseEvent::new("EventResultShortcutTargetEvent", Map::new()));
    let target_for_result = target.clone();
    let result_thread = thread::spawn(move || {
        block_on(target_for_result.event_result_with_options(EventResultOptions::default()))
    });
    thread::sleep(Duration::from_millis(50));
    assert_eq!(
        order.lock().expect("order lock").as_slice(),
        ["blocker_start", "target"]
    );
    assert_eq!(
        result_thread
            .join()
            .expect("result thread")
            .expect("result"),
        Some(json!("target"))
    );
    release_blocker.store(true, Ordering::SeqCst);
    bus.destroy();
}

#[test]
fn test_event_results_list_starts_never_started_event_and_returns_all_results() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("EventResultsShortcutQueueJumpBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));
    let blocker_started = Arc::new(AtomicBool::new(false));
    let release_blocker = Arc::new(AtomicBool::new(false));
    let order_for_blocker = order.clone();
    let blocker_started_for_handler = blocker_started.clone();
    let release_blocker_for_handler = release_blocker.clone();
    bus.on_raw(
        "EventResultsShortcutBlockerEvent",
        "blocker",
        move |_event| {
            let order = order_for_blocker.clone();
            let blocker_started = blocker_started_for_handler.clone();
            let release_blocker = release_blocker_for_handler.clone();
            async move {
                push(&order, "blocker_start");
                blocker_started.store(true, Ordering::SeqCst);
                while !release_blocker.load(Ordering::SeqCst) {
                    thread::sleep(Duration::from_millis(1));
                }
                push(&order, "blocker_end");
                Ok(json!(null))
            }
        },
    );
    let order_for_first = order.clone();
    bus.on_raw("EventResultsShortcutTargetEvent", "first", move |_event| {
        let order = order_for_first.clone();
        async move {
            push(&order, "first");
            Ok(json!("first"))
        }
    });
    let order_for_second = order.clone();
    bus.on_raw("EventResultsShortcutTargetEvent", "second", move |_event| {
        let order = order_for_second.clone();
        async move {
            push(&order, "second");
            Ok(json!("second"))
        }
    });

    bus.emit_base(BaseEvent::new(
        "EventResultsShortcutBlockerEvent",
        Map::new(),
    ));
    wait_until_bool(&blocker_started);
    let target = bus.emit_base(BaseEvent::new(
        "EventResultsShortcutTargetEvent",
        Map::new(),
    ));
    let target_for_results = target.clone();
    let results_thread = thread::spawn(move || {
        block_on(target_for_results.event_results_list_with_options(EventResultOptions::default()))
    });
    thread::sleep(Duration::from_millis(50));
    assert_eq!(
        order.lock().expect("order lock").as_slice(),
        ["blocker_start", "first", "second"]
    );
    assert_eq!(
        results_thread
            .join()
            .expect("results thread")
            .expect("results"),
        vec![json!("first"), json!("second")]
    );
    let mut mapped_values: Vec<Value> = target
        .inner
        .lock()
        .event_results
        .values()
        .filter_map(|event_result| event_result.result.clone())
        .collect();
    mapped_values.sort_by_key(|value| value.as_str().unwrap_or_default().to_string());
    assert_eq!(mapped_values, vec![json!("first"), json!("second")]);
    release_blocker.store(true, Ordering::SeqCst);
    bus.destroy();
}

#[test]
fn test_event_result_helpers_do_not_wait_for_started_event() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("EventResultHelpersStartedBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::Parallel,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            event_timeout: Some(0.0),
            ..EventBusOptions::default()
        },
    );
    let handler_started = Arc::new(AtomicBool::new(false));
    let release_handler = Arc::new(AtomicBool::new(false));
    let handler_started_for_handler = handler_started.clone();
    let release_handler_for_handler = release_handler.clone();
    bus.on_raw("EventResultHelpersStartedEvent", "slow", move |_event| {
        let handler_started = handler_started_for_handler.clone();
        let release_handler = release_handler_for_handler.clone();
        async move {
            handler_started.store(true, Ordering::SeqCst);
            while !release_handler.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(1));
            }
            Ok(json!("late"))
        }
    });

    let event = bus.emit_base(BaseEvent::new("EventResultHelpersStartedEvent", Map::new()));
    wait_until_bool(&handler_started);
    assert_eq!(event.inner.lock().event_status, EventStatus::Started);

    let event_for_result = event.clone();
    let (result_tx, result_rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = result_tx.send(block_on(event_for_result.event_result_with_options(
            EventResultOptions {
                raise_if_any: true,
                raise_if_none: false,
                include: None,
            },
        )));
    });
    assert_eq!(
        result_rx
            .recv_timeout(Duration::from_millis(50))
            .expect("event_result should not wait for a started event")
            .expect("event_result"),
        None
    );

    let event_for_results = event.clone();
    let (results_tx, results_rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = results_tx.send(block_on(event_for_results.event_results_list_with_options(
            EventResultOptions {
                raise_if_any: true,
                raise_if_none: false,
                include: None,
            },
        )));
    });
    assert_eq!(
        results_rx
            .recv_timeout(Duration::from_millis(50))
            .expect("event_results_list should not wait for a started event")
            .expect("event_results_list"),
        Vec::<Value>::new()
    );
    assert_eq!(event.inner.lock().event_status, EventStatus::Started);

    release_handler.store(true, Ordering::SeqCst);
    assert!(block_on(bus.wait_until_idle(Some(1.0))));
    bus.destroy();
}

#[test]
fn test_now_on_already_executing_event_waits_without_duplicate_execution() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("NowAlreadyExecutingBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_timeout: Some(0.0),
            ..EventBusOptions::default()
        },
    );
    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let run_count = Arc::new(AtomicUsize::new(0));
    let started_for_handler = started.clone();
    let release_for_handler = release.clone();
    let run_count_for_handler = run_count.clone();
    bus.on_raw("NowAlreadyExecutingEvent", "handler", move |_event| {
        let started = started_for_handler.clone();
        let release = release_for_handler.clone();
        let run_count = run_count_for_handler.clone();
        async move {
            run_count.fetch_add(1, Ordering::SeqCst);
            started.store(true, Ordering::SeqCst);
            while !release.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(1));
            }
            Ok(json!("done"))
        }
    });

    let event = bus.emit_base(BaseEvent::new("NowAlreadyExecutingEvent", Map::new()));
    wait_until_bool(&started);
    let event_for_now = event.clone();
    let now_thread = thread::spawn(move || {
        block_on(event_for_now.now_with_options(EventWaitOptions {
            timeout: Some(1.0),
            first_result: false,
        }))
    });
    thread::sleep(Duration::from_millis(50));
    assert_eq!(run_count.load(Ordering::SeqCst), 1);
    release.store(true, Ordering::SeqCst);
    assert!(now_thread
        .join()
        .expect("now thread")
        .map(|completed| Arc::ptr_eq(&completed, &event))
        .unwrap_or(false));
    assert_eq!(
        block_on(event.event_result_with_options(EventResultOptions::default())).expect("result"),
        Some(json!("done"))
    );
    assert_eq!(run_count.load(Ordering::SeqCst), 1);
    bus.destroy();
}

#[test]
fn test_now_with_rapid_handler_churn_does_not_duplicate_execution() {
    let _guard = test_guard();
    let total_events = 200usize;
    let bus = EventBus::new_with_options(
        Some("NowRapidHandlerChurnBus".to_string()),
        EventBusOptions {
            event_timeout: Some(0.0),
            max_history_size: Some(512),
            max_history_drop: true,
            ..EventBusOptions::default()
        },
    );
    let run_count = Arc::new(AtomicUsize::new(0));

    for index in 0..total_events {
        let run_count_for_handler = run_count.clone();
        let handler_id = format!("now-rapid-handler-churn-{index}");
        let handler = bus.on_raw("NowRapidHandlerChurnEvent", &handler_id, move |_event| {
            let run_count = run_count_for_handler.clone();
            async move {
                run_count.fetch_add(1, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(1));
                Ok(json!("done"))
            }
        });
        let event = bus.emit_base(BaseEvent::new("NowRapidHandlerChurnEvent", Map::new()));
        let completed = block_on(event.now_with_options(EventWaitOptions {
            timeout: Some(1.0),
            first_result: false,
        }))
        .expect("now");
        assert!(Arc::ptr_eq(&completed, &event));
        thread::sleep(Duration::from_millis(1));
        assert!(block_on(bus.wait_until_idle(Some(1.0))));
        bus.off("NowRapidHandlerChurnEvent", Some(&handler.id));
    }

    assert_eq!(run_count.load(Ordering::SeqCst), total_events);
    bus.destroy();
}

#[test]
fn test_event_result_options_apply_to_current_results() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("EventResultOptionsCurrentResultsBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            event_timeout: Some(0.0),
            ..EventBusOptions::default()
        },
    );
    let release_slow = Arc::new(AtomicBool::new(false));
    bus.on_raw(
        "EventResultOptionsCurrentResultsEvent",
        "fail",
        |_event| async { Err("option boom".to_string()) },
    );
    bus.on_raw(
        "EventResultOptionsCurrentResultsEvent",
        "keep",
        |_event| async {
            thread::sleep(Duration::from_millis(10));
            Ok(json!("keep"))
        },
    );
    let release_slow_for_handler = release_slow.clone();
    bus.on_raw(
        "EventResultOptionsCurrentResultsEvent",
        "slow",
        move |_event| {
            let release_slow = release_slow_for_handler.clone();
            async move {
                while !release_slow.load(Ordering::SeqCst) {
                    thread::sleep(Duration::from_millis(1));
                }
                Ok(json!("late"))
            }
        },
    );

    let event = block_on(
        bus.emit_base(BaseEvent::new(
            "EventResultOptionsCurrentResultsEvent",
            Map::new(),
        ))
        .now_with_options(EventWaitOptions {
            timeout: Some(1.0),
            first_result: true,
        }),
    )
    .expect("now first_result");
    assert_eq!(
        block_on(event.event_result_with_options(EventResultOptions {
            raise_if_any: false,
            ..EventResultOptions::default()
        }))
        .expect("event result"),
        Some(json!("keep"))
    );
    assert!(
        block_on(event.event_result_with_options(EventResultOptions {
            raise_if_any: true,
            ..EventResultOptions::default()
        }))
        .expect_err("raise_if_any should surface current error")
        .contains("option boom")
    );
    assert_eq!(
        block_on(event.event_results_list_with_options(EventResultOptions {
            include: Some(Arc::new(
                |result, _event_result| result == Some(&json!("missing"))
            )),
            raise_if_any: false,
            raise_if_none: false,
        }))
        .expect("filtered results"),
        Vec::<Value>::new()
    );
    release_slow.store(true, Ordering::SeqCst);
    bus.destroy();
}

#[test]
fn test_parallel_event_concurrency_plus_immediate_execution_races_child_events_inside_handlers() {
    let _guard = test_guard();
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
                let child1 = bus.emit_child(BaseEventParallelImmediateChildEvent1 {
                    ..Default::default()
                });
                let child2 = bus.emit_child(BaseEventParallelImmediateChildEvent2 {
                    ..Default::default()
                });
                let child3 = bus.emit_child(BaseEventParallelImmediateChildEvent3 {
                    ..Default::default()
                });
                let _ = child1.now().await;
                let _ = child2.now().await;
                let _ = child3.now().await;
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

    let parent = bus.emit(BaseEventParallelImmediateParentEvent {
        ..Default::default()
    });
    all_started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("all child handlers should start before release");
    assert!(max_in_flight.load(Ordering::SeqCst) >= 3);
    assert!(!order
        .lock()
        .expect("order lock")
        .contains(&"parent_end".to_string()));

    release.store(true, Ordering::SeqCst);
    let _ = block_on(parent.now());
    block_on(bus.wait_until_idle(Some(2.0)));

    let order = order.lock().expect("order lock").clone();
    let parent_end_index = index_of(&order, "parent_end");
    for label in ["child1", "child2", "child3"] {
        assert!(index_of(&order, &format!("{label}_start")) < parent_end_index);
        assert!(index_of(&order, &format!("{label}_end")) < parent_end_index);
    }
    bus.destroy();
}

#[test]
fn test_wait_waits_in_queue_order_inside_handler() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("BaseEventQueueOrderBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::Parallel,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));
    let sibling_started = Arc::new(AtomicBool::new(false));

    let bus_for_parent = bus.clone();
    let order_for_parent = order.clone();
    let sibling_started_for_parent = sibling_started.clone();
    bus.on_raw("BaseEventQueuedParentEvent", "parent", move |_event| {
        let bus = bus_for_parent.clone();
        let order = order_for_parent.clone();
        let sibling_started = sibling_started_for_parent.clone();
        async move {
            push(&order, "parent_start");
            bus.emit(BaseEventQueuedSiblingEvent {
                ..Default::default()
            });
            let deadline = std::time::Instant::now() + Duration::from_millis(500);
            while !sibling_started.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
            let child = bus.emit_child(BaseEventQueuedChildEvent {
                ..Default::default()
            });
            let _ = child.wait().await;
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
    let sibling_started_for_sibling = sibling_started.clone();
    bus.on_raw("BaseEventQueuedSiblingEvent", "sibling", move |_event| {
        let order = order_for_sibling.clone();
        let sibling_started = sibling_started_for_sibling.clone();
        async move {
            push(&order, "sibling_start");
            sibling_started.store(true, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(5));
            push(&order, "sibling_end");
            Ok(json!("sibling"))
        }
    });

    let parent = bus.emit(BaseEventQueuedParentEvent {
        ..Default::default()
    });
    let _ = block_on(parent.now());
    block_on(bus.wait_until_idle(Some(2.0)));

    let order = order.lock().expect("order lock").clone();
    assert!(index_of(&order, "sibling_start") < index_of(&order, "child_start"));
    assert!(index_of(&order, "child_end") < index_of(&order, "parent_end"));
    bus.destroy();
}

#[test]
fn test_wait_is_passive_inside_handlers_and_times_out_for_serial_events() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("PassiveSerialEventCompletedBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));

    let bus_for_parent = bus.clone();
    let order_for_parent = order.clone();
    bus.on_raw("PassiveSerialParentEvent", "parent", move |_event| {
        let bus = bus_for_parent.clone();
        let order = order_for_parent.clone();
        async move {
            push(&order, "parent_start");
            let emitted = bus.emit_child(PassiveSerialEmittedEvent {
                ..Default::default()
            });
            let found_source = bus.emit_child(PassiveSerialFoundEvent {
                ..Default::default()
            });
            let found = bus
                .find("PassiveSerialFoundEvent", true, None, None)
                .await
                .expect("found queued serial event");
            let found_id = found.inner.lock().event_id.clone();
            let found_source_id = found_source.inner.inner.lock().event_id.clone();
            assert_eq!(found_id, found_source_id);

            let emitted_error = match emitted
                .wait_with_options(EventWaitOptions {
                    timeout: Some(0.02),
                    first_result: false,
                })
                .await
            {
                Ok(_) => panic!("emitted serial wait should time out"),
                Err(error) => error,
            };
            assert!(emitted_error.contains("Timed out waiting"));
            push(&order, "emitted_timeout");
            let found_error = match found
                .wait_with_options(EventWaitOptions {
                    timeout: Some(0.02),
                    first_result: false,
                })
                .await
            {
                Ok(_) => panic!("found serial wait should time out"),
                Err(error) => error,
            };
            assert!(found_error.contains("Timed out waiting"));
            push(&order, "found_timeout");

            let snapshot = order.lock().expect("order lock").clone();
            assert!(!snapshot.iter().any(|item| item == "emitted_start"));
            assert!(!snapshot.iter().any(|item| item == "found_start"));
            assert!(!emitted.inner.inner.lock().event_blocks_parent_completion);
            assert!(!found.inner.lock().event_blocks_parent_completion);
            push(&order, "parent_end");
            Ok(json!("parent"))
        }
    });
    let order_for_emitted = order.clone();
    bus.on_raw("PassiveSerialEmittedEvent", "emitted", move |_event| {
        let order = order_for_emitted.clone();
        async move {
            push(&order, "emitted_start");
            Ok(json!("emitted"))
        }
    });
    let order_for_found = order.clone();
    bus.on_raw("PassiveSerialFoundEvent", "found", move |_event| {
        let order = order_for_found.clone();
        async move {
            push(&order, "found_start");
            Ok(json!("found"))
        }
    });

    let parent = bus.emit(PassiveSerialParentEvent {
        ..Default::default()
    });
    let _ = block_on(parent.now());
    block_on(bus.wait_until_idle(Some(2.0)));
    assert_eq!(
        order.lock().expect("order lock").as_slice(),
        [
            "parent_start",
            "emitted_timeout",
            "found_timeout",
            "parent_end",
            "emitted_start",
            "found_start"
        ]
    );
    bus.destroy();
}

#[test]
fn test_wait_serial_wait_inside_handler_times_out_and_warns_about_slow_handler() {
    let _guard = test_guard();
    let stderr = run_base_event_deadlock_warning_child(
        "__abxbus_event_completed_serial_wait_deadlock_warning_child",
    );
    let slow_warning_index = stderr
        .to_lowercase()
        .find("slow event handler")
        .unwrap_or_else(|| panic!("expected slow handler warning in stderr, got: {stderr}"));
    let timeout_marker_index = stderr
        .find("serial event_completed timeout observed")
        .unwrap_or_else(|| panic!("expected timeout marker in stderr, got: {stderr}"));
    assert!(
        slow_warning_index < timeout_marker_index,
        "slow handler warning should be emitted while handler is still waiting, got: {stderr}"
    );
}

#[test]
fn __abxbus_event_completed_serial_wait_deadlock_warning_child() {
    if !base_event_deadlock_warning_child_enabled() {
        return;
    }
    let bus = EventBus::new_with_options(
        Some("EventCompletedSerialDeadlockWarningBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_slow_timeout: Some(0.0),
            event_handler_slow_timeout: Some(0.01),
            ..EventBusOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));

    let bus_for_parent = bus.clone();
    let order_for_parent = order.clone();
    bus.on_raw(
        "EventCompletedSerialDeadlockWarningParentEvent",
        "parent",
        move |_event| {
            let bus = bus_for_parent.clone();
            let order = order_for_parent.clone();
            async move {
                push(&order, "parent_start");
                let child = bus.emit_child(EventCompletedSerialDeadlockWarningChildEvent {
                    ..Default::default()
                });
                let found = bus
                    .find(
                        "EventCompletedSerialDeadlockWarningChildEvent",
                        true,
                        None,
                        None,
                    )
                    .await
                    .expect("expected to find queued serial child event");
                let found_id = found.inner.lock().event_id.clone();
                let child_id = child.inner.inner.lock().event_id.clone();
                assert_eq!(found_id, child_id);
                let error = match found
                    .wait_with_options(EventWaitOptions {
                        timeout: Some(0.05),
                        first_result: false,
                    })
                    .await
                {
                    Ok(_) => panic!("serial child wait should time out"),
                    Err(error) => error,
                };
                assert!(error.contains("Timed out waiting"));
                eprintln!(
                    "serial event_completed timeout observed while parent handler is still running"
                );
                push(&order, "child_timeout");
                assert!(!order
                    .lock()
                    .expect("order lock")
                    .iter()
                    .any(|item| item == "child_start"));
                assert!(!found.inner.lock().event_blocks_parent_completion);
                push(&order, "parent_end");
                Ok(json!("parent"))
            }
        },
    );
    let order_for_child = order.clone();
    bus.on_raw(
        "EventCompletedSerialDeadlockWarningChildEvent",
        "child",
        move |_event| {
            let order = order_for_child.clone();
            async move {
                push(&order, "child_start");
                Ok(json!("child"))
            }
        },
    );

    let parent = bus.emit(EventCompletedSerialDeadlockWarningParentEvent {
        ..Default::default()
    });
    let _ = block_on(parent.now());
    block_on(bus.wait_until_idle(Some(2.0)));
    assert_eq!(
        order.lock().expect("order lock").as_slice(),
        ["parent_start", "child_timeout", "parent_end", "child_start"]
    );
    bus.destroy();
}

#[test]
fn test_deferred_emit_after_handler_completion_is_accepted() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("DeferredEmitAfterCompletionBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));
    let (emitted_tx, emitted_rx) = mpsc::channel();

    let bus_for_parent = bus.clone();
    let order_for_parent = order.clone();
    bus.on_raw(
        "DeferredEmitAfterCompletionParentEvent",
        "parent",
        move |_event| {
            let bus = bus_for_parent.clone();
            let order = order_for_parent.clone();
            let emitted_tx = emitted_tx.clone();
            async move {
                push(&order, "parent_start");
                let order_for_thread = order.clone();
                thread::spawn(move || {
                    thread::sleep(Duration::from_millis(20));
                    push(&order_for_thread, "deferred_emit");
                    bus.emit_child(DeferredEmitAfterCompletionChildEvent {
                        ..Default::default()
                    });
                    emitted_tx.send(()).expect("send deferred emit signal");
                });
                push(&order, "parent_end");
                Ok(json!("parent"))
            }
        },
    );
    let order_for_child = order.clone();
    bus.on_raw(
        "DeferredEmitAfterCompletionChildEvent",
        "child",
        move |_event| {
            let order = order_for_child.clone();
            async move {
                push(&order, "child_start");
                Ok(json!("child"))
            }
        },
    );

    let parent = bus.emit(DeferredEmitAfterCompletionParentEvent {
        ..Default::default()
    });
    let _ = block_on(parent.now());
    emitted_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("timed out waiting for deferred emit");
    block_on(bus.wait_until_idle(Some(1.0)));
    assert_eq!(
        order.lock().expect("order lock").as_slice(),
        ["parent_start", "parent_end", "deferred_emit", "child_start"]
    );
    bus.destroy();
}

#[test]
fn test_wait_waits_for_normal_parallel_processing_inside_handlers() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("PassiveParallelEventCompletedBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));

    let bus_for_parent = bus.clone();
    let order_for_parent = order.clone();
    bus.on_raw("PassiveParallelParentEvent", "parent", move |_event| {
        let bus = bus_for_parent.clone();
        let order = order_for_parent.clone();
        async move {
            push(&order, "parent_start");
            let emitted = bus.emit_child(PassiveParallelEmittedEvent {
                event_concurrency: Some(EventConcurrencyMode::Parallel),
                ..Default::default()
            });
            let found_source = bus.emit_child(PassiveParallelFoundEvent {
                event_concurrency: Some(EventConcurrencyMode::Parallel),
                ..Default::default()
            });
            let found = bus
                .find("PassiveParallelFoundEvent", true, None, None)
                .await
                .expect("found queued parallel event");
            let found_id = found.inner.lock().event_id.clone();
            let found_source_id = found_source.inner.inner.lock().event_id.clone();
            assert_eq!(found_id, found_source_id);

            emitted
                .wait_with_options(EventWaitOptions {
                    timeout: Some(1.0),
                    first_result: false,
                })
                .await
                .expect("emitted parallel event should complete");
            push(&order, "emitted_completed");
            found
                .wait_with_options(EventWaitOptions {
                    timeout: Some(1.0),
                    first_result: false,
                })
                .await
                .expect("found parallel event should complete");
            push(&order, "found_completed");
            assert!(!emitted.inner.inner.lock().event_blocks_parent_completion);
            assert!(!found.inner.lock().event_blocks_parent_completion);
            push(&order, "parent_end");
            Ok(json!("parent"))
        }
    });
    let order_for_emitted = order.clone();
    bus.on_raw("PassiveParallelEmittedEvent", "emitted", move |_event| {
        let order = order_for_emitted.clone();
        async move {
            push(&order, "emitted_start");
            thread::sleep(Duration::from_millis(5));
            push(&order, "emitted_end");
            Ok(json!("emitted"))
        }
    });
    let order_for_found = order.clone();
    bus.on_raw("PassiveParallelFoundEvent", "found", move |_event| {
        let order = order_for_found.clone();
        async move {
            push(&order, "found_start");
            thread::sleep(Duration::from_millis(5));
            push(&order, "found_end");
            Ok(json!("found"))
        }
    });

    let parent = bus.emit(PassiveParallelParentEvent {
        ..Default::default()
    });
    let _ = block_on(parent.now());
    block_on(bus.wait_until_idle(Some(2.0)));
    let order = order.lock().expect("order lock").clone();
    assert!(index_of(&order, "emitted_end") < index_of(&order, "emitted_completed"));
    assert!(index_of(&order, "found_end") < index_of(&order, "found_completed"));
    assert_eq!(order.last().map(String::as_str), Some("parent_end"));
    bus.destroy();
}

#[test]
fn test_wait_waits_for_future_parallel_event_found_after_handler_starts() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("FutureParallelEventCompletedBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );
    let other_started = Arc::new(AtomicBool::new(false));
    let release_find = Arc::new(AtomicBool::new(false));
    let parallel_started = Arc::new(AtomicBool::new(false));
    let continued = Arc::new(AtomicBool::new(false));
    let waited_for = Arc::new(Mutex::new(None::<Duration>));

    let bus_for_other = bus.clone();
    let other_started_for_handler = other_started.clone();
    let release_find_for_handler = release_find.clone();
    let continued_for_handler = continued.clone();
    let waited_for_handler = waited_for.clone();
    bus.on_raw("FutureParallelSomeOtherEvent", "other", move |_event| {
        let bus = bus_for_other.clone();
        let other_started = other_started_for_handler.clone();
        let release_find = release_find_for_handler.clone();
        let continued = continued_for_handler.clone();
        let waited_for = waited_for_handler.clone();
        async move {
            other_started.store(true, Ordering::SeqCst);
            while !release_find.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(1));
            }
            let found = bus
                .find("FutureParallelEvent", true, None, None)
                .await
                .expect("expected to find pending parallel event");
            let started_at = std::time::Instant::now();
            found
                .wait_with_options(EventWaitOptions {
                    timeout: Some(1.0),
                    first_result: false,
                })
                .await
                .expect("parallel event should complete");
            *waited_for.lock().expect("waited_for lock") = Some(started_at.elapsed());
            continued.store(true, Ordering::SeqCst);
            Ok(json!("other"))
        }
    });

    let parallel_started_for_handler = parallel_started.clone();
    bus.on_raw("FutureParallelEvent", "parallel", move |_event| {
        let parallel_started = parallel_started_for_handler.clone();
        async move {
            parallel_started.store(true, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(250));
            Ok(json!("parallel"))
        }
    });

    let other = bus.emit(FutureParallelSomeOtherEvent {
        ..Default::default()
    });
    wait_until_bool(&other_started);
    bus.emit(FutureParallelEvent {
        event_concurrency: Some(EventConcurrencyMode::Parallel),
        ..Default::default()
    });
    wait_until_bool(&parallel_started);
    release_find.store(true, Ordering::SeqCst);
    wait_until_bool(&continued);
    let _ = block_on(other.now());
    block_on(bus.wait_until_idle(Some(2.0)));
    let waited = waited_for
        .lock()
        .expect("waited_for lock")
        .expect("waited duration");
    assert!(waited >= Duration::from_millis(150));
    bus.destroy();
}

#[test]
fn test_wait_returns_event_accepts_timeout_and_rejects_unattached_pending_event() {
    let _guard = test_guard();
    let pending = EventCompletedTimeoutEvent {
        ..Default::default()
    }
    .into_base_event_handle();
    let error = match block_on(pending.wait_with_options(EventWaitOptions {
        timeout: Some(0.01),
        first_result: false,
    })) {
        Ok(_) => panic!("pending event without bus should reject"),
        Err(error) => error,
    };
    assert_eq!(error, "event has no bus attached");

    let completed = EventCompletedTimeoutEvent {
        ..Default::default()
    }
    .into_base_event_handle();
    completed.inner.inner.lock().event_status = EventStatus::Completed;
    let returned = block_on(completed.wait_with_options(EventWaitOptions {
        timeout: Some(0.01),
        first_result: false,
    }))
    .expect("completed event should not require bus");
    assert!(Arc::ptr_eq(&returned.inner, &completed.inner));

    let bus = EventBus::new_with_options(
        Some("EventCompletedTimeoutBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );
    let release_handler = Arc::new(AtomicBool::new(false));
    let release_handler_for_handler = release_handler.clone();
    bus.on_raw("EventCompletedTimeoutEvent", "slow", move |_event| {
        let release_handler = release_handler_for_handler.clone();
        async move {
            while !release_handler.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(1));
            }
            Ok(json!(null))
        }
    });

    let event = bus.emit(EventCompletedTimeoutEvent {
        ..Default::default()
    });
    let error = match block_on(event.wait_with_options(EventWaitOptions {
        timeout: Some(0.01),
        first_result: false,
    })) {
        Ok(_) => panic!("wait should time out"),
        Err(error) => error,
    };
    assert!(error.contains("Timed out waiting"));

    release_handler.store(true, Ordering::SeqCst);
    let returned = block_on(event.wait_with_options(EventWaitOptions {
        timeout: Some(1.0),
        first_result: false,
    }))
    .expect("event should complete after release");
    assert!(Arc::ptr_eq(&returned.inner, &event.inner));
    bus.destroy();
}

#[test]
fn test_base_event_json_roundtrip() {
    let _guard = test_guard();
    let event = mk_event("test_event");
    let json_value = event.to_json_value();
    let deserialized = BaseEvent::from_json_value(json_value.clone());
    assert_eq!(json_value, deserialized.to_json_value());
}

#[test]
fn test_base_event_runtime_state_transitions() {
    let _guard = test_guard();
    let event = mk_event("runtime_event");
    assert_eq!(event.inner.lock().event_status, EventStatus::Pending);
    event.mark_started();
    assert_eq!(event.inner.lock().event_status, EventStatus::Started);
    event.mark_completed();
    assert_eq!(event.inner.lock().event_status, EventStatus::Completed);
    let _ = block_on(event.now());
}

#[test]
fn test_monotonicdatetime_emits_parseable_monotonic_iso_timestamps() {
    let _guard = test_guard();
    let first = now_iso();
    let second = now_iso();

    assert!(chrono::DateTime::parse_from_rfc3339(&first).is_ok());
    assert!(chrono::DateTime::parse_from_rfc3339(&second).is_ok());
    assert!(second >= first);
}

#[test]
fn test_python_serialized_at_fields_are_strings() {
    let _guard = test_guard();
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
    let _guard = test_guard();
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
    let _guard = test_guard();
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
    let _guard = test_guard();
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
    let _guard = test_guard();
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
    let _guard = test_guard();
    for field in [
        "bus", "emit", "now", "wait", "toString", "toJSON", "fromJSON",
    ] {
        let mut payload = Map::new();
        payload.insert(field.to_string(), json!(true));
        let error = unwrap_event_error(BaseEvent::try_new("ReservedFieldEvent", payload));
        assert!(error.contains(field));
    }
}

#[test]
fn test_unknown_event_prefixed_field_rejected_in_payload() {
    let _guard = test_guard();
    let mut payload = Map::new();
    payload.insert("event_unknown".to_string(), json!("bad"));

    let error = unwrap_event_error(BaseEvent::try_new("UnknownEventField", payload));
    assert!(error.contains("event_unknown"));
}

#[test]
fn test_model_prefixed_field_rejected_in_payload() {
    let _guard = test_guard();
    let mut payload = Map::new();
    payload.insert("model_config".to_string(), json!("bad"));

    let error = unwrap_event_error(BaseEvent::try_new("ModelFieldEvent", payload));
    assert!(error.contains("model_config"));
}

#[test]
fn test_builtin_event_prefixed_override_is_allowed() {
    let _guard = test_guard();
    let bus = EventBus::new(Some("BaseEventAllowedConfigBus".to_string()));
    let event = BaseEventAllowedEventConfigEvent {
        ..Default::default()
    };

    assert_eq!(event.event_timeout, None);
    assert_eq!(event.event_slow_timeout, None);
    assert_eq!(event.event_handler_timeout, None);
    let event = bus.emit(event);
    let inner = event.inner.inner.lock();
    assert_eq!(inner.event_timeout, Some(123.0));
    assert_eq!(inner.event_slow_timeout, Some(9.0));
    assert_eq!(inner.event_handler_timeout, Some(45.0));
    drop(inner);
    bus.destroy();
}

#[test]
fn test_from_json_accepts_event_parent_id_null_and_preserves_it_in_to_json_output() {
    let _guard = test_guard();
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
    let _guard = test_guard();
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

// Folded from test_base_event_eventbus_proxy.rs to keep test layout class-based.
mod folded_test_base_event_eventbus_proxy {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard, OnceLock,
    };

    use abxbus_rust::{base_event::BaseEvent, event_bus::EventBus, types::EventStatus};
    use futures::executor::block_on;
    use serde_json::{json, Map, Value};

    fn base_event(event_type: &str, payload: Value) -> Arc<BaseEvent> {
        let Value::Object(payload) = payload else {
            panic!("test payload must be an object");
        };
        BaseEvent::new(event_type, payload)
    }

    fn unique_bus_name(prefix: &str) -> String {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(1);
        format!("Proxy{prefix}{}", NEXT_ID.fetch_add(1, Ordering::Relaxed))
    }

    fn proxy_test_guard() -> MutexGuard<'static, ()> {
        static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("proxy test lock")
    }

    fn history_event(bus: &Arc<EventBus>, event_type: &str) -> Arc<BaseEvent> {
        bus.runtime_payload_for_test()
            .values()
            .find(|event| event.inner.lock().event_type == event_type)
            .cloned()
            .unwrap_or_else(|| panic!("missing {event_type} in history"))
    }

    #[test]
    fn test_event_event_bus_inside_handler_returns_the_dispatching_bus() {
        let _guard = proxy_test_guard();
        let bus_name = unique_bus_name("TestBus");
        let bus = EventBus::new(Some(bus_name.clone()));
        let handler_called = Arc::new(Mutex::new(false));
        let handler_bus_name = Arc::new(Mutex::new(None::<String>));
        let child_event = Arc::new(Mutex::new(None::<Arc<BaseEvent>>));

        let handler_called_for_handler = handler_called.clone();
        let handler_bus_name_for_handler = handler_bus_name.clone();
        let child_event_for_handler = child_event.clone();
        bus.on_raw("MainEvent", "main_handler", move |event| {
            let handler_called = handler_called_for_handler.clone();
            let handler_bus_name = handler_bus_name_for_handler.clone();
            let child_event = child_event_for_handler.clone();
            async move {
                *handler_called.lock().expect("handler_called lock") = true;
                let current_bus = event.event_bus().expect("event bus inside handler");
                *handler_bus_name.lock().expect("handler_bus_name lock") =
                    Some(current_bus.name.clone());
                let child = current_bus.emit_child_base(base_event("ChildEvent", json!({})));
                *child_event.lock().expect("child_event lock") = Some(child);
                Ok(json!(null))
            }
        });
        bus.on_raw("ChildEvent", "child_handler", |_event| async move {
            Ok(json!(null))
        });

        let event = bus.emit_base(base_event("MainEvent", json!({})));
        let _ = block_on(event.wait());
        assert!(block_on(bus.wait_until_idle(None)));

        assert!(*handler_called.lock().expect("handler_called lock"));
        assert_eq!(
            handler_bus_name
                .lock()
                .expect("handler_bus_name lock")
                .as_deref(),
            Some(bus_name.as_str())
        );
        let child = child_event
            .lock()
            .expect("child_event lock")
            .clone()
            .expect("child event should have been dispatched");
        assert_eq!(child.inner.lock().event_type, "ChildEvent");
        assert_eq!(
            child.event_bus().map(|bus| bus.name.clone()).as_deref(),
            Some(bus_name.as_str())
        );
        bus.destroy();
    }

    #[test]
    fn test_legacy_bus_property_is_not_exposed_inside_handlers() {
        let _guard = proxy_test_guard();
        let bus = EventBus::new(Some("NoLegacyEventBusPropertyBus".to_string()));
        let has_serialized_legacy_bus = Arc::new(Mutex::new(true));

        let has_serialized_legacy_bus_for_handler = has_serialized_legacy_bus.clone();
        bus.on_raw("MainEvent", "handler", move |event| {
            let has_serialized_legacy_bus = has_serialized_legacy_bus_for_handler.clone();
            async move {
                *has_serialized_legacy_bus.lock().expect("legacy bus lock") =
                    event.to_json_value().get("bus").is_some();
                Ok(json!(null))
            }
        });

        let event = bus.emit_base(base_event("MainEvent", json!({})));
        let _ = block_on(event.wait());
        assert!(!*has_serialized_legacy_bus.lock().expect("legacy bus lock"));
        assert!(base_event("DetachedEvent", json!({}))
            .to_json_value()
            .get("bus")
            .is_none());
        bus.destroy();
    }

    #[test]
    fn test_event_bus_aliases_bus_property() {
        let _guard = proxy_test_guard();
        let bus = EventBus::new(Some("AliasBus".to_string()));
        let seen_bus_id = Arc::new(Mutex::new(None::<String>));
        let seen_event_bus_id = Arc::new(Mutex::new(None::<String>));

        let seen_bus_id_for_handler = seen_bus_id.clone();
        let seen_event_bus_id_for_handler = seen_event_bus_id.clone();
        bus.on_raw("MainEvent", "handler", move |event| {
            let seen_bus_id = seen_bus_id_for_handler.clone();
            let seen_event_bus_id = seen_event_bus_id_for_handler.clone();
            async move {
                *seen_bus_id.lock().expect("seen bus id") = event.bus().map(|bus| bus.id.clone());
                *seen_event_bus_id.lock().expect("seen event bus id") =
                    event.event_bus().map(|bus| bus.id.clone());
                Ok(json!(null))
            }
        });

        let event = bus.emit_base(base_event("MainEvent", json!({})));
        let _ = block_on(event.wait());

        assert_eq!(
            seen_bus_id.lock().expect("seen bus id").as_deref(),
            Some(bus.id.as_str())
        );
        assert_eq!(
            seen_event_bus_id
                .lock()
                .expect("seen event bus id")
                .as_deref(),
            Some(bus.id.as_str())
        );
        assert!(!event
            .to_json_value()
            .as_object()
            .unwrap()
            .contains_key("bus"));
        bus.destroy();
    }

    #[test]
    fn test_event_event_bus_is_set_for_child_events_emitted_in_handler() {
        let _guard = proxy_test_guard();
        let bus_name = unique_bus_name("EventBusPropertyFallbackBus");
        let bus = EventBus::new(Some(bus_name.clone()));
        let child_bus_name = Arc::new(Mutex::new(None::<String>));

        let child_bus_name_for_handler = child_bus_name.clone();
        bus.on_raw("MainEvent", "handler", move |event| {
            let child_bus_name = child_bus_name_for_handler.clone();
            async move {
                let current_bus = event.event_bus().expect("handler bus");
                let child = current_bus.emit_child_base(base_event("ChildEvent", json!({})));
                *child_bus_name.lock().expect("child bus lock") =
                    child.event_bus().map(|bus| bus.name.clone());
                Ok(json!(null))
            }
        });
        bus.on_raw("ChildEvent", "child_handler", |_event| async move {
            Ok(json!(null))
        });

        let event = bus.emit_base(base_event("MainEvent", json!({})));
        let _ = block_on(event.wait());
        assert!(block_on(bus.wait_until_idle(None)));
        assert_eq!(
            child_bus_name.lock().expect("child bus lock").as_deref(),
            Some(bus_name.as_str())
        );
        bus.destroy();
    }

    #[test]
    fn test_event_event_bus_is_absent_on_detached_events() {
        let _guard = proxy_test_guard();
        let bus_name = unique_bus_name("EventBusPropertyDetachedBus");
        let bus = EventBus::new(Some(bus_name.clone()));
        bus.on_raw(
            "MainEvent",
            "handler",
            |_event| async move { Ok(json!(null)) },
        );

        let original = bus.emit_base(base_event("MainEvent", json!({})));
        let _ = block_on(original.wait());

        assert_eq!(
            original.event_bus().map(|bus| bus.name.clone()).as_deref(),
            Some(bus_name.as_str())
        );
        let detached = BaseEvent::from_json_value(original.to_json_value());
        assert!(detached.event_bus().is_none());
        assert_eq!(detached.inner.lock().event_path, vec![bus.label()]);
        bus.destroy();
    }

    #[test]
    fn test_event_event_bus_is_available_outside_handler_context() {
        let _guard = proxy_test_guard();
        let bus_name = unique_bus_name("EventBusPropertyOutsideHandlerBus");
        let bus = EventBus::new(Some(bus_name.clone()));
        let event = bus.emit_base(base_event("MainEvent", json!({})));
        let _ = block_on(event.wait());

        assert_eq!(
            event.event_bus().map(|bus| bus.name.clone()).as_deref(),
            Some(bus_name.as_str())
        );
        bus.destroy();
    }

    #[test]
    fn test_event_event_bus_returns_correct_bus_when_multiple_buses_exist() {
        let _guard = proxy_test_guard();
        let bus1_name = unique_bus_name("Bus1");
        let bus2_name = unique_bus_name("Bus2");
        let bus1 = EventBus::new(Some(bus1_name.clone()));
        let bus2 = EventBus::new(Some(bus2_name.clone()));
        let handler1_bus_name = Arc::new(Mutex::new(None::<String>));
        let handler2_bus_name = Arc::new(Mutex::new(None::<String>));

        let handler1_bus_name_for_handler = handler1_bus_name.clone();
        bus1.on_raw("MainEvent", "handler1", move |event| {
            let handler1_bus_name = handler1_bus_name_for_handler.clone();
            async move {
                *handler1_bus_name.lock().expect("handler1 bus lock") =
                    event.event_bus().map(|bus| bus.name.clone());
                Ok(json!(null))
            }
        });
        let handler2_bus_name_for_handler = handler2_bus_name.clone();
        bus2.on_raw("MainEvent", "handler2", move |event| {
            let handler2_bus_name = handler2_bus_name_for_handler.clone();
            async move {
                *handler2_bus_name.lock().expect("handler2 bus lock") =
                    event.event_bus().map(|bus| bus.name.clone());
                Ok(json!(null))
            }
        });

        let event1 = bus1.emit_base(base_event("MainEvent", json!({})));
        let _ = block_on(event1.wait());
        let event2 = bus2.emit_base(base_event("MainEvent", json!({})));
        let _ = block_on(event2.wait());

        assert_eq!(
            handler1_bus_name
                .lock()
                .expect("handler1 bus lock")
                .as_deref(),
            Some(bus1_name.as_str())
        );
        assert_eq!(
            handler2_bus_name
                .lock()
                .expect("handler2 bus lock")
                .as_deref(),
            Some(bus2_name.as_str())
        );
        bus1.destroy();
        bus2.destroy();
    }

    #[test]
    fn test_event_event_bus_reflects_the_currently_processing_bus_when_forwarded() {
        let _guard = proxy_test_guard();
        let bus1_name = unique_bus_name("Bus1");
        let bus2_name = unique_bus_name("Bus2");
        let bus1 = EventBus::new(Some(bus1_name));
        let bus2 = EventBus::new(Some(bus2_name.clone()));
        let bus2_handler_bus_name = Arc::new(Mutex::new(None::<String>));

        let bus2_for_forward = bus2.clone();
        bus1.on_raw("*", "forward_to_bus2", move |event| {
            let bus2 = bus2_for_forward.clone();
            async move {
                bus2.emit_base(event);
                Ok(json!(null))
            }
        });

        let handler_bus_name = bus2_handler_bus_name.clone();
        bus2.on_raw("MainEvent", "bus2_handler", move |event| {
            let handler_bus_name = handler_bus_name.clone();
            async move {
                *handler_bus_name.lock().expect("handler_bus_name lock") =
                    event.event_bus().map(|bus| bus.name.clone());
                Ok(json!(null))
            }
        });

        let event = bus1.emit_base(base_event("MainEvent", json!({})));
        let _ = block_on(event.wait());
        assert!(block_on(bus1.wait_until_idle(None)));
        assert!(block_on(bus2.wait_until_idle(None)));

        assert_eq!(
            bus2_handler_bus_name
                .lock()
                .expect("handler_bus_name lock")
                .as_deref(),
            Some(bus2_name.as_str())
        );
        assert_eq!(
            event.inner.lock().event_path,
            vec![bus1.label(), bus2.label()]
        );
        bus1.destroy();
        bus2.destroy();
    }

    #[test]
    fn test_event_event_bus_in_nested_handlers_sees_the_same_bus() {
        let _guard = proxy_test_guard();
        let bus_name = unique_bus_name("MainBus");
        let bus = EventBus::new(Some(bus_name.clone()));
        let outer_bus_name = Arc::new(Mutex::new(None::<String>));
        let inner_bus_name = Arc::new(Mutex::new(None::<String>));

        let outer_bus_name_for_handler = outer_bus_name.clone();
        bus.on_raw("MainEvent", "outer_handler", move |event| {
            let outer_bus_name = outer_bus_name_for_handler.clone();
            async move {
                let current_bus = event.event_bus().expect("outer bus");
                *outer_bus_name.lock().expect("outer bus lock") = Some(current_bus.name.clone());
                let child = current_bus.emit_child_base(base_event("ChildEvent", json!({})));
                let _ = child.now().await;
                Ok(json!(null))
            }
        });

        let inner_bus_name_for_handler = inner_bus_name.clone();
        bus.on_raw("ChildEvent", "inner_handler", move |event| {
            let inner_bus_name = inner_bus_name_for_handler.clone();
            async move {
                *inner_bus_name.lock().expect("inner bus lock") =
                    event.event_bus().map(|bus| bus.name.clone());
                Ok(json!(null))
            }
        });

        let parent = bus.emit_base(base_event("MainEvent", json!({})));
        let _ = block_on(parent.wait());

        assert_eq!(
            outer_bus_name.lock().expect("outer bus lock").as_deref(),
            Some(bus_name.as_str())
        );
        assert_eq!(
            inner_bus_name.lock().expect("inner bus lock").as_deref(),
            Some(bus_name.as_str())
        );
        bus.destroy();
    }

    #[test]
    fn test_event_emit_awaited_children_pass_explicit_handler_context_to_immediate_processing() {
        let _guard = proxy_test_guard();
        let bus = EventBus::new(Some("ExplicitEventEmitHandlerContextBus".to_string()));
        let child_ref = Arc::new(Mutex::new(None::<Arc<BaseEvent>>));

        let child_ref_for_handler = child_ref.clone();
        bus.on_raw("MainEvent", "main_handler", move |event| {
            let child_ref = child_ref_for_handler.clone();
            async move {
                let current_bus = event.event_bus().expect("handler bus");
                let child = current_bus.emit_child_base(base_event("ChildEvent", json!({})));
                let _ = child.now().await;
                *child_ref.lock().expect("child lock") = Some(child);
                Ok(json!(null))
            }
        });
        bus.on_raw("ChildEvent", "child_handler", |_event| async move {
            Ok(json!("child-ok"))
        });

        let parent = bus.emit_base(base_event("MainEvent", json!({})));
        let _ = block_on(parent.wait());
        let child = child_ref
            .lock()
            .expect("child lock")
            .clone()
            .expect("child event");
        let child_inner = child.inner.lock();
        let parent_inner = parent.inner.lock();
        assert_eq!(
            child_inner.event_parent_id.as_deref(),
            Some(parent_inner.event_id.as_str())
        );
        assert_eq!(
            child_inner.event_emitted_by_handler_id.as_deref(),
            parent_inner
                .event_results
                .values()
                .find(|result| result.handler.handler_name == "main_handler")
                .map(|result| result.handler.id.as_str())
        );
        assert!(child_inner.event_blocks_parent_completion);
        bus.destroy();
    }

    #[test]
    fn test_event_emit_sets_parent_child_relationships_through_3_levels() {
        let _guard = proxy_test_guard();
        let bus_name = unique_bus_name("MainBus");
        let bus = EventBus::new(Some(bus_name.clone()));
        let execution_order = Arc::new(Mutex::new(Vec::<String>::new()));
        let child_ref = Arc::new(Mutex::new(None::<Arc<BaseEvent>>));
        let grandchild_ref = Arc::new(Mutex::new(None::<Arc<BaseEvent>>));

        let order_for_parent = execution_order.clone();
        let child_ref_for_parent = child_ref.clone();
        bus.on_raw("MainEvent", "parent_handler", move |event| {
            let order = order_for_parent.clone();
            let child_ref = child_ref_for_parent.clone();
            async move {
                order
                    .lock()
                    .expect("order lock")
                    .push("parent_start".to_string());
                let current_bus = event.event_bus().expect("parent bus");
                let child = current_bus.emit_child_base(base_event("ChildEvent", json!({})));
                let _ = child.now().await;
                *child_ref.lock().expect("child lock") = Some(child);
                order
                    .lock()
                    .expect("order lock")
                    .push("parent_end".to_string());
                Ok(json!(null))
            }
        });

        let order_for_child = execution_order.clone();
        let grandchild_ref_for_child = grandchild_ref.clone();
        bus.on_raw("ChildEvent", "child_handler", move |event| {
            let order = order_for_child.clone();
            let grandchild_ref = grandchild_ref_for_child.clone();
            async move {
                order
                    .lock()
                    .expect("order lock")
                    .push("child_start".to_string());
                let current_bus = event.event_bus().expect("child bus");
                let grandchild =
                    current_bus.emit_child_base(base_event("GrandchildEvent", json!({})));
                let _ = grandchild.now().await;
                *grandchild_ref.lock().expect("grandchild lock") = Some(grandchild);
                order
                    .lock()
                    .expect("order lock")
                    .push("child_end".to_string());
                Ok(json!(null))
            }
        });

        let order_for_grandchild = execution_order.clone();
        bus.on_raw("GrandchildEvent", "grandchild_handler", move |event| {
            let order = order_for_grandchild.clone();
            let bus_name = bus_name.clone();
            async move {
                assert_eq!(
                    event.event_bus().map(|bus| bus.name.clone()).as_deref(),
                    Some(bus_name.as_str())
                );
                order
                    .lock()
                    .expect("order lock")
                    .push("grandchild_start".to_string());
                order
                    .lock()
                    .expect("order lock")
                    .push("grandchild_end".to_string());
                Ok(json!(null))
            }
        });

        let parent = bus.emit_base(base_event("MainEvent", json!({})));
        let _ = block_on(parent.wait());
        let child = child_ref
            .lock()
            .expect("child lock")
            .clone()
            .expect("child event");
        let grandchild = grandchild_ref
            .lock()
            .expect("grandchild lock")
            .clone()
            .expect("grandchild event");

        assert_eq!(
            execution_order.lock().expect("order lock").as_slice(),
            &[
                "parent_start".to_string(),
                "child_start".to_string(),
                "grandchild_start".to_string(),
                "grandchild_end".to_string(),
                "child_end".to_string(),
                "parent_end".to_string(),
            ]
        );
        assert_eq!(parent.inner.lock().event_status, EventStatus::Completed);
        assert_eq!(child.inner.lock().event_status, EventStatus::Completed);
        assert_eq!(grandchild.inner.lock().event_status, EventStatus::Completed);
        assert_eq!(
            child.inner.lock().event_parent_id.as_deref(),
            Some(parent.inner.lock().event_id.as_str())
        );
        assert_eq!(
            grandchild.inner.lock().event_parent_id.as_deref(),
            Some(child.inner.lock().event_id.as_str())
        );
        assert!(bus.event_is_child_of(&child, &parent));
        assert!(bus.event_is_child_of(&grandchild, &parent));
        bus.destroy();
    }

    #[test]
    fn test_event_emit_with_forwarding_child_dispatch_goes_to_the_correct_bus() {
        let _guard = proxy_test_guard();
        let bus1_name = unique_bus_name("Bus1");
        let bus2_name = unique_bus_name("Bus2");
        let bus1 = EventBus::new(Some(bus1_name));
        let bus2 = EventBus::new(Some(bus2_name.clone()));
        let child_handler_bus_name = Arc::new(Mutex::new(None::<String>));
        let child_ref = Arc::new(Mutex::new(None::<Arc<BaseEvent>>));

        let bus2_for_forward = bus2.clone();
        bus1.on_raw("*", "forward_to_bus2", move |event| {
            let bus2 = bus2_for_forward.clone();
            async move {
                bus2.emit_base(event);
                Ok(json!(null))
            }
        });

        let child_ref_for_handler = child_ref.clone();
        bus2.on_raw("MainEvent", "bus2_main_handler", move |event| {
            let child_ref = child_ref_for_handler.clone();
            let bus2_name = bus2_name.clone();
            async move {
                let current_bus = event.event_bus().expect("forwarded handler bus");
                assert_eq!(current_bus.name, bus2_name);
                let child = current_bus.emit_child_base(base_event("ChildEvent", json!({})));
                let _ = child.now().await;
                *child_ref.lock().expect("child_ref lock") = Some(child);
                Ok(json!(null))
            }
        });

        let child_bus_name = child_handler_bus_name.clone();
        bus2.on_raw("ChildEvent", "bus2_child_handler", move |event| {
            let child_bus_name = child_bus_name.clone();
            async move {
                *child_bus_name.lock().expect("child_bus_name lock") =
                    event.event_bus().map(|bus| bus.name.clone());
                Ok(json!("child-ok"))
            }
        });

        let parent = bus1.emit_base(base_event("MainEvent", json!({})));
        let _ = block_on(parent.wait());
        assert!(block_on(bus1.wait_until_idle(None)));
        assert!(block_on(bus2.wait_until_idle(None)));

        let child = child_ref
            .lock()
            .expect("child_ref lock")
            .clone()
            .expect("child event");
        assert_eq!(
            child_handler_bus_name
                .lock()
                .expect("child_bus_name lock")
                .as_deref(),
            Some(bus2.name.as_str())
        );
        assert_eq!(child.inner.lock().event_status, EventStatus::Completed);
        assert_eq!(
            child.inner.lock().event_parent_id.as_deref(),
            Some(parent.inner.lock().event_id.as_str())
        );
        assert_eq!(child.inner.lock().event_path, vec![bus2.label()]);
        bus1.destroy();
        bus2.destroy();
    }

    #[test]
    fn test_event_event_bus_is_set_on_the_event_after_dispatch_outside_handler() {
        let _guard = proxy_test_guard();
        let bus_name = unique_bus_name("TestBus");
        let bus = EventBus::new(Some(bus_name.clone()));
        let raw_event = BaseEvent::new("MainEvent", Map::new());
        assert!(raw_event.event_bus().is_none());

        let dispatched = bus.emit_base(raw_event);
        assert_eq!(
            dispatched
                .event_bus()
                .map(|bus| bus.name.clone())
                .as_deref(),
            Some(bus_name.as_str())
        );
        let _ = block_on(dispatched.wait());
        bus.destroy();
    }

    #[test]
    fn test_event_emit_from_handler_correctly_attributes_event_emitted_by_handler_id() {
        let _guard = proxy_test_guard();
        let bus = EventBus::new(Some(unique_bus_name("TestBus")));

        bus.on_raw("MainEvent", "main_handler", move |event| async move {
            let current_bus = event.event_bus().expect("handler bus");
            current_bus.emit_child_base(base_event("ChildEvent", json!({})));
            Ok(json!(null))
        });
        bus.on_raw("ChildEvent", "child_handler", |_event| async move {
            Ok(json!(null))
        });

        let parent = bus.emit_base(base_event("MainEvent", json!({})));
        assert!(block_on(bus.wait_until_idle(None)));
        let child = history_event(&bus, "ChildEvent");

        assert_eq!(
            child.inner.lock().event_parent_id.as_deref(),
            Some(parent.inner.lock().event_id.as_str())
        );
        let emitted_by = child
            .inner
            .lock()
            .event_emitted_by_handler_id
            .clone()
            .expect("event_emitted_by_handler_id");
        assert!(parent.inner.lock().event_results.contains_key(&emitted_by));
        bus.destroy();
    }

    #[test]
    fn test_dispatch_preserves_explicit_event_parent_id_and_does_not_override_it() {
        let _guard = proxy_test_guard();
        let bus = EventBus::new(Some("ExplicitParentBus".to_string()));
        let explicit_parent_id = "018f8e40-1234-7000-8000-000000001234".to_string();

        let explicit_parent_id_for_handler = explicit_parent_id.clone();
        bus.on_raw("MainEvent", "main_handler", move |event| {
            let explicit_parent_id = explicit_parent_id_for_handler.clone();
            async move {
                let current_bus = event.event_bus().expect("handler bus");
                let child = base_event("ChildEvent", json!({}));
                child.inner.lock().event_parent_id = Some(explicit_parent_id);
                current_bus.emit_child_base(child);
                Ok(json!(null))
            }
        });

        let parent = bus.emit_base(base_event("MainEvent", json!({})));
        assert!(block_on(bus.wait_until_idle(None)));
        let child = history_event(&bus, "ChildEvent");
        assert_eq!(
            child.inner.lock().event_parent_id.as_deref(),
            Some(explicit_parent_id.as_str())
        );
        assert_ne!(
            child.inner.lock().event_parent_id.as_deref(),
            Some(parent.inner.lock().event_id.as_str())
        );
        bus.destroy();
    }

    #[test]
    fn test_event_is_child_of_and_event_is_parent_of_work_for_direct_children() {
        let _guard = proxy_test_guard();
        let bus = EventBus::new(Some("ParentChildBus".to_string()));
        bus.on_raw(
            "LineageParentEvent",
            "parent_handler",
            move |event| async move {
                event
                    .event_bus()
                    .expect("handler bus")
                    .emit_child_base(base_event("LineageChildEvent", json!({})));
                Ok(json!(null))
            },
        );

        let parent = bus.emit_base(base_event("LineageParentEvent", json!({})));
        assert!(block_on(bus.wait_until_idle(None)));
        let child = history_event(&bus, "LineageChildEvent");

        assert_eq!(
            child.inner.lock().event_parent_id.as_deref(),
            Some(parent.inner.lock().event_id.as_str())
        );
        assert!(bus.event_is_child_of(&child, &parent));
        assert!(bus.event_is_parent_of(&parent, &child));
        bus.destroy();
    }

    #[test]
    fn test_event_is_child_of_works_for_grandchildren() {
        let _guard = proxy_test_guard();
        let bus = EventBus::new(Some("GrandchildBus".to_string()));
        bus.on_raw(
            "LineageParentEvent",
            "parent_handler",
            move |event| async move {
                event
                    .event_bus()
                    .expect("handler bus")
                    .emit_child_base(base_event("LineageChildEvent", json!({})));
                Ok(json!(null))
            },
        );
        bus.on_raw(
            "LineageChildEvent",
            "child_handler",
            move |event| async move {
                event
                    .event_bus()
                    .expect("handler bus")
                    .emit_child_base(base_event("LineageGrandchildEvent", json!({})));
                Ok(json!(null))
            },
        );

        let parent = bus.emit_base(base_event("LineageParentEvent", json!({})));
        assert!(block_on(bus.wait_until_idle(None)));
        let child = history_event(&bus, "LineageChildEvent");
        let grandchild = history_event(&bus, "LineageGrandchildEvent");

        assert!(bus.event_is_child_of(&child, &parent));
        assert!(bus.event_is_child_of(&grandchild, &parent));
        assert_eq!(
            grandchild.inner.lock().event_parent_id.as_deref(),
            Some(child.inner.lock().event_id.as_str())
        );
        assert!(bus.event_is_parent_of(&parent, &grandchild));
        bus.destroy();
    }

    #[test]
    fn test_event_is_child_of_returns_false_for_unrelated_events() {
        let _guard = proxy_test_guard();
        let bus = EventBus::new(Some("UnrelatedBus".to_string()));

        let parent = bus.emit_base(base_event("LineageParentEvent", json!({})));
        let unrelated = bus.emit_base(base_event("LineageUnrelatedEvent", json!({})));
        let _ = block_on(parent.wait());
        let _ = block_on(unrelated.wait());

        assert!(!bus.event_is_child_of(&unrelated, &parent));
        assert!(!bus.event_is_parent_of(&parent, &unrelated));
        bus.destroy();
    }
}

// Folded from test_base_event_runtime_state.rs to keep test layout class-based.
mod folded_test_base_event_runtime_state {
    use abxbus_rust::event;
    use std::{
        sync::{mpsc, Arc, Mutex},
        thread,
        time::Duration,
    };

    use abxbus_rust::{
        base_event::{now_iso, BaseEvent},
        event_bus::EventBus,
        event_result::EventResultStatus,
        types::EventStatus,
    };
    use futures::executor::block_on;
    use serde_json::{json, Map, Value};

    event! {
        struct RuntimeSampleEvent {
            data: String,
            event_result_type: String,
            event_type: "RuntimeSampleEvent",
        }
    }
    event! {
        struct RuntimeSeededEvent {
            data: String,
            event_result_type: String,
            event_type: "RuntimeSeededEvent",
        }
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

        let event = bus.emit(RuntimeSampleEvent {
            data: "processing_test".to_string(),
            ..Default::default()
        });
        let _ = block_on(event.now());

        let event = event.inner.inner.lock();
        assert!(event.event_started_at.is_some());
        assert!(event.event_completed_at.is_some());
        assert_eq!(event.event_status, EventStatus::Completed);
        bus.destroy();
    }

    #[test]
    fn test_event_without_handlers_completes_and_serializes_runtime_state() {
        let event = RuntimeSampleEvent {
            data: "no_handlers".to_string(),
            ..Default::default()
        };
        let bus = EventBus::new(Some("RuntimeStateNoHandlersBus".to_string()));

        assert_eq!(event.event_started_at, None);
        assert_eq!(event.event_completed_at, None);

        let processed_event = bus.emit(event);
        let _ = block_on(processed_event.now());

        let processed = processed_event.inner.inner.lock();
        assert_eq!(processed.event_status, EventStatus::Completed);
        assert_eq!(processed.event_pending_bus_count, 0);
        assert!(processed.event_results.is_empty());
        assert!(processed.event_started_at.is_some());
        assert!(processed.event_completed_at.is_some());
        bus.destroy();
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
        let _ = block_on(processed_event.now());

        {
            let processed = processed_event.inner.lock();
            assert_eq!(processed.event_status, EventStatus::Completed);
            assert!(processed.event_started_at.is_some());
            assert!(processed.event_completed_at.is_some());
        }

        let mut seeded_payload = Map::new();
        seeded_payload.insert("data".to_string(), json!("manual_seeded_result"));
        let seeded_event = BaseEvent::new("RuntimeSeededEvent", seeded_payload);
        let handler_entry = bus.on_raw("RuntimeSeededEvent", "handler", |_event| async {
            Ok(json!("done"))
        });
        let seeded_result = seeded_event.event_result_update(
            &handler_entry,
            Some(EventResultStatus::Started),
            None,
            None,
            None,
        );
        assert_eq!(seeded_event.inner.lock().event_status, EventStatus::Started);
        assert_eq!(seeded_event.inner.lock().event_completed_at, None);
        seeded_event
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
        assert_eq!(seeded_event.inner.lock().event_completed_at, None);

        let reconciled = bus.emit_base(seeded_event);
        let _ = block_on(reconciled.now());
        let reconciled = reconciled.inner.lock();
        assert_eq!(reconciled.event_status, EventStatus::Completed);
        assert!(reconciled.event_started_at.is_some());
        assert!(reconciled.event_completed_at.is_some());
        bus.destroy();
    }

    #[test]
    fn test_event_copy_preserves_runtime_attrs() {
        let event = sample_event("copy_test");
        let copied_event = BaseEvent::from_json_value(event.to_json_value());

        assert_eq!(copied_event.inner.lock().event_started_at, None);
        assert_eq!(copied_event.inner.lock().event_completed_at, None);
    }

    #[test]
    fn test_event_started_at_is_serialized_and_stateful() {
        let bus = EventBus::new(Some("RuntimeStateStartedAtBus".to_string()));
        let event = sample_event("serialize_started_at");
        let pending_payload = event.to_json_value();
        assert!(pending_payload
            .as_object()
            .unwrap()
            .contains_key("event_started_at"));
        assert_eq!(pending_payload["event_started_at"], Value::Null);

        let handler_entry = bus.on_raw("RuntimeSampleEvent", "handler", |_event| async {
            Ok(json!("ok"))
        });
        event.event_result_update(
            &handler_entry,
            Some(EventResultStatus::Started),
            None,
            None,
            None,
        );
        let first_started_at = event.to_json_value()["event_started_at"]
            .as_str()
            .expect("started at")
            .to_string();

        let forced_started_at = "2020-01-01T00:00:00.000000000Z".to_string();
        event
            .inner
            .lock()
            .event_results
            .get_mut(&handler_entry.id)
            .expect("handler result")
            .started_at = Some(forced_started_at.clone());

        let second_started_at = event.to_json_value()["event_started_at"]
            .as_str()
            .expect("started at")
            .to_string();
        assert_eq!(second_started_at, first_started_at);
        assert_ne!(second_started_at, forced_started_at);
        bus.destroy();
    }

    #[test]
    fn test_event_result_update_started_marks_event_started_and_clears_completion() {
        let bus = EventBus::new(Some("RuntimeStateResultUpdateStartedBus".to_string()));
        let event = sample_event("result_update_started");
        event.inner.lock().event_completed_at = Some(now_iso());
        let handler_entry = bus.on_raw("RuntimeSampleEvent", "handler", |_event| async {
            Ok(json!("ok"))
        });

        let result = event.event_result_update(
            &handler_entry,
            Some(EventResultStatus::Started),
            None,
            None,
            None,
        );

        assert_eq!(result.status, EventResultStatus::Started);
        let event = event.inner.lock();
        assert_eq!(event.event_status, EventStatus::Started);
        assert!(event.event_started_at.is_some());
        assert_eq!(event.event_completed_at, None);
        bus.destroy();
    }

    #[test]
    fn test_event_status_is_serialized_and_stateful() {
        let bus = EventBus::new(Some("RuntimeStateStatusBus".to_string()));
        let event = sample_event("serialize_status");
        assert_eq!(event.to_json_value()["event_status"], "pending");

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

        let processing_event = bus.emit_base(event);
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("handler entered");
        assert_eq!(processing_event.to_json_value()["event_status"], "started");

        release_tx.send(()).expect("release send");
        let _ = block_on(processing_event.now());
        assert_eq!(
            processing_event.to_json_value()["event_status"],
            "completed"
        );
        bus.destroy();
    }
}

// Folded from test_ids.rs to keep test layout class-based.
mod folded_test_ids {
    use abxbus_rust::{
        event_bus::EventBus,
        event_handler::EventHandler,
        id::{compute_handler_id, handler_id_namespace},
    };
    use serde_json::Map;
    use uuid::Uuid;

    #[test]
    fn test_bus_and_event_ids_are_uuid_v7() {
        let bus = EventBus::new(Some("BusId".to_string()));
        let bus_id = Uuid::parse_str(&bus.id).expect("bus id must parse");
        assert_eq!(bus_id.get_version_num(), 7);

        let event = abxbus_rust::base_event::BaseEvent::new("work", Map::new());
        let event_id = Uuid::parse_str(&event.inner.lock().event_id).expect("event id must parse");
        assert_eq!(event_id.get_version_num(), 7);
    }

    #[test]
    fn test_handler_id_uses_v5_namespace_seed_compatible_with_python_ts() {
        let eventbus_id = "018f6f0e-79b2-7cc5-aed9-f0f9a4e5e6b0";
        let handler_name = "module.fn";
        let handler_registered_at = "2026-01-01T00:00:00.000Z";
        let event_pattern = "work";
        let expected_seed =
            format!("{eventbus_id}|{handler_name}|unknown|{handler_registered_at}|{event_pattern}");

        let expected = Uuid::new_v5(&handler_id_namespace(), expected_seed.as_bytes()).to_string();
        let actual = compute_handler_id(
            eventbus_id,
            handler_name,
            None,
            handler_registered_at,
            event_pattern,
        );
        assert_eq!(actual, expected);

        let entry = EventHandler::from_callable(
            event_pattern.to_string(),
            handler_name.to_string(),
            "BusId".to_string(),
            eventbus_id.to_string(),
            std::sync::Arc::new(|_event| Box::pin(async { Ok(serde_json::Value::Null) })),
        );
        let ns = Uuid::parse_str(&entry.id).expect("handler id must parse");
        assert_eq!(ns.get_version_num(), 5);
    }
}
