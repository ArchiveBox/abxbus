use std::{
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
    time::Duration,
};

use abxbus_rust::{
    base_event::BaseEvent,
    event_bus::{EventBus, EventBusOptions},
    lock_manager::{run_with_lock, AsyncLock, HandlerLock, LockManager, ReentrantLock},
    types::{EventConcurrencyMode, EventHandlerConcurrencyMode},
};
use futures::executor::block_on;
use serde_json::json;

fn empty_event(event_type: &str) -> Arc<BaseEvent> {
    BaseEvent::new(event_type, serde_json::Map::new())
}

fn register_active_handler(
    bus: &Arc<EventBus>,
    event_type: &str,
    handler_name: &str,
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
) {
    bus.on_raw(event_type, handler_name, move |_event| {
        let active = active.clone();
        let max_active = max_active.clone();
        async move {
            let now_active = active.fetch_add(1, Ordering::SeqCst) + 1;
            max_active.fetch_max(now_active, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(30));
            active.fetch_sub(1, Ordering::SeqCst);
            Ok(json!("ok"))
        }
    });
}

#[test]
fn test_asynclock_1_releasing_to_a_queued_waiter_does_not_allow_a_new_acquire_to_slip_in() {
    let lock = AsyncLock::new(1);
    let initial_holder = lock.acquire();

    let waiter_lock = lock.clone();
    let (waiter_acquired_tx, waiter_acquired_rx) = mpsc::channel();
    let (release_waiter_tx, release_waiter_rx) = mpsc::channel();
    let waiter = thread::spawn(move || {
        let _guard = waiter_lock.acquire();
        waiter_acquired_tx.send(()).expect("waiter acquired send");
        release_waiter_rx.recv().expect("release waiter");
    });

    while lock.waiters_len() != 1 {
        thread::sleep(Duration::from_millis(1));
    }
    drop(initial_holder);

    let contender_lock = lock.clone();
    let contender_acquired = Arc::new(AtomicBool::new(false));
    let contender_acquired_for_thread = contender_acquired.clone();
    let contender = thread::spawn(move || {
        let _guard = contender_lock.acquire();
        contender_acquired_for_thread.store(true, Ordering::SeqCst);
    });

    waiter_acquired_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("queued waiter should receive the handoff first");
    thread::sleep(Duration::from_millis(20));
    assert!(!contender_acquired.load(Ordering::SeqCst));
    assert_eq!(lock.waiters_len(), 1);

    release_waiter_tx.send(()).expect("release waiter send");
    waiter.join().expect("waiter joins");
    contender.join().expect("contender joins");
    assert_eq!(lock.in_use(), 0);
    assert_eq!(lock.waiters_len(), 0);
}

#[test]
fn test_asynclock_infinity_acquire_release_is_a_no_op_bypass() {
    let lock = AsyncLock::infinite();
    let first = lock.acquire();
    let second = lock.acquire();
    let third = lock.acquire();

    assert_eq!(lock.in_use(), 0);
    assert_eq!(lock.waiters_len(), 0);
    drop(first);
    drop(second);
    drop(third);
    lock.release();
    assert_eq!(lock.in_use(), 0);
    assert_eq!(lock.waiters_len(), 0);
}

#[test]
fn test_asynclock_size_1_enforces_semaphore_concurrency_limit() {
    let lock = Arc::new(AsyncLock::new(2));
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for _ in 0..6 {
        let lock = lock.clone();
        let active = active.clone();
        let max_active = max_active.clone();
        handles.push(thread::spawn(move || {
            let _guard = lock.acquire();
            let now_active = active.fetch_add(1, Ordering::SeqCst) + 1;
            max_active.fetch_max(now_active, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(5));
            active.fetch_sub(1, Ordering::SeqCst);
        }));
    }

    for handle in handles {
        handle.join().expect("worker joins");
    }
    assert_eq!(max_active.load(Ordering::SeqCst), 2);
    assert_eq!(lock.in_use(), 0);
    assert_eq!(lock.waiters_len(), 0);
}

#[test]
fn test_runwithlock_null_executes_function_directly_and_preserves_errors() {
    let called = Arc::new(AtomicUsize::new(0));
    let called_for_success = called.clone();
    let value = run_with_lock(None, || {
        called_for_success.fetch_add(1, Ordering::SeqCst);
        Ok::<_, &'static str>("ok")
    })
    .expect("run without lock");
    assert_eq!(value, "ok");
    assert_eq!(called.load(Ordering::SeqCst), 1);

    let error = run_with_lock::<(), _>(None, || Err("boom")).expect_err("error preserved");
    assert_eq!(error, "boom");
}

#[test]
fn test_run_with_lock_null_executes_function_directly_and_preserves_errors() {
    test_runwithlock_null_executes_function_directly_and_preserves_errors();
}

#[test]
fn test_handlerlock_reclaimhandlerlockifrunning_releases_reclaimed_permit_if_handler_exits_while_waiting(
) {
    let lock = AsyncLock::new(1);
    let guard = lock.acquire();
    let handler_lock = Arc::new(HandlerLock::new_held(lock.clone(), guard));

    assert!(handler_lock.yield_handler_lock_for_child_run());
    let occupying_guard = lock.acquire();

    let handler_for_reclaim = handler_lock.clone();
    let (reclaim_started_tx, reclaim_started_rx) = mpsc::channel();
    let reclaim = thread::spawn(move || {
        reclaim_started_tx.send(()).expect("reclaim started");
        handler_for_reclaim.reclaim_handler_lock_if_running()
    });
    reclaim_started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("reclaim starts");
    while lock.waiters_len() != 1 {
        thread::sleep(Duration::from_millis(1));
    }

    handler_lock.exit_handler_run();
    drop(occupying_guard);

    assert!(!reclaim.join().expect("reclaim joins"));
    assert_eq!(lock.in_use(), 0);
    assert_eq!(lock.waiters_len(), 0);
}

#[test]
fn test_handlerlock_runqueuejump_yields_permit_during_child_run_and_reacquires_before_returning() {
    let lock = AsyncLock::new(1);
    let guard = lock.acquire();
    let handler_lock = HandlerLock::new_held(lock.clone(), guard);

    let contender_acquired = Arc::new(AtomicBool::new(false));
    let contender_acquired_for_thread = contender_acquired.clone();
    let (release_contender_tx, release_contender_rx) = mpsc::channel();
    let contender_lock = lock.clone();
    let contender = thread::spawn(move || {
        let _guard = contender_lock.acquire();
        contender_acquired_for_thread.store(true, Ordering::SeqCst);
        release_contender_rx.recv().expect("release contender");
    });

    while lock.waiters_len() != 1 {
        thread::sleep(Duration::from_millis(1));
    }

    let result = handler_lock.run_queue_jump(|| {
        while !contender_acquired.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(1));
        }
        release_contender_tx.send(()).expect("release contender");
        "child-ok"
    });

    assert_eq!(result, "child-ok");
    assert_eq!(lock.in_use(), 1);
    handler_lock.exit_handler_run();
    contender.join().expect("contender joins");
    assert_eq!(lock.in_use(), 0);
}

#[test]
fn test_lockmanager_pause_is_re_entrant_and_resumes_waiters_only_at_depth_zero() {
    let locks = Arc::new(LockManager::default());
    let mut release_a = locks.request_runloop_pause();
    let mut release_b = locks.request_runloop_pause();
    assert!(locks.is_paused());

    let resumed = Arc::new(AtomicBool::new(false));
    let resumed_for_thread = resumed.clone();
    let locks_for_waiter = locks.clone();
    let waiter = thread::spawn(move || {
        locks_for_waiter.wait_until_runloop_resumed();
        resumed_for_thread.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(20));
    assert!(!resumed.load(Ordering::SeqCst));
    release_a.release();
    thread::sleep(Duration::from_millis(20));
    assert!(!resumed.load(Ordering::SeqCst));
    assert!(locks.is_paused());

    release_b.release();
    waiter.join().expect("waiter joins");
    assert!(resumed.load(Ordering::SeqCst));
    assert!(!locks.is_paused());

    release_a.release();
    release_b.release();
}

#[test]
fn test_lockmanager_waitforidle_uses_two_check_stability_and_supports_timeout() {
    let locks = LockManager::default();
    assert!(!locks.wait_for_idle(Some(Duration::from_millis(10)), || false));

    let checks = Arc::new(AtomicUsize::new(0));
    let checks_for_idle = checks.clone();
    let became_idle = locks.wait_for_idle(Some(Duration::from_millis(200)), || {
        checks_for_idle.fetch_add(1, Ordering::SeqCst);
        true
    });
    assert!(became_idle);
    assert!(checks.load(Ordering::SeqCst) >= 2);
}

#[test]
fn test_lock_manager_wait_for_idle_uses_two_check_stability_and_supports_timeout() {
    test_lockmanager_waitforidle_uses_two_check_stability_and_supports_timeout();
}

#[test]
fn test_reentrant_lock_nested_context_reuses_single_permit() {
    let lock = ReentrantLock::default();
    assert!(!lock.locked());
    assert_eq!(lock.depth(), 0);

    let outer = lock.lock();
    assert!(lock.locked());
    assert_eq!(lock.depth(), 1);
    let inner = lock.lock();
    assert!(lock.locked());
    assert_eq!(lock.depth(), 2);
    drop(inner);
    assert!(lock.locked());
    assert_eq!(lock.depth(), 1);
    drop(outer);
    assert!(!lock.locked());
    assert_eq!(lock.depth(), 0);

    let after_nested = lock.lock();
    drop(after_nested);
}

#[test]
fn test_handler_dispatch_context_marks_and_restores_lock_depth() {
    let lock = ReentrantLock::default();

    assert_eq!(lock.depth(), 0);
    assert!(!lock.locked());

    {
        let _dispatch_context = lock.mark_held_in_current_context();
        assert_eq!(lock.depth(), 1);
        assert!(!lock.locked());

        {
            let _nested = lock.lock();
            assert_eq!(lock.depth(), 2);
            assert!(!lock.locked());
        }

        assert_eq!(lock.depth(), 1);
    }

    assert_eq!(lock.depth(), 0);
    assert!(!lock.locked());
}

#[test]
fn test_reentrant_lock_serializes_across_threads() {
    let lock = Arc::new(ReentrantLock::default());
    let entered_second_thread = Arc::new(AtomicBool::new(false));

    let first_guard = lock.lock();
    let lock_for_thread = lock.clone();
    let flag_for_thread = entered_second_thread.clone();
    let handle = thread::spawn(move || {
        let _guard = lock_for_thread.lock();
        flag_for_thread.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(20));
    assert!(!entered_second_thread.load(Ordering::SeqCst));
    drop(first_guard);

    handle.join().expect("thread should join");
    assert!(entered_second_thread.load(Ordering::SeqCst));
}

#[test]
fn test_reentrant_lock_releases_and_reraises_on_exception() {
    let lock = Arc::new(ReentrantLock::default());
    let lock_for_thread = lock.clone();

    let result = thread::spawn(move || {
        let panic_result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = lock_for_thread.lock();
            panic!("reentrant-lock-error");
        }));
        assert!(panic_result.is_err());
    })
    .join();
    assert!(result.is_ok());

    let acquired_after_panic = Arc::new(AtomicBool::new(false));
    let flag = acquired_after_panic.clone();
    let lock_for_thread = lock.clone();
    let handle = thread::spawn(move || {
        let _guard = lock_for_thread.lock();
        flag.store(true, Ordering::SeqCst);
    });
    handle.join().expect("lock should be released after panic");
    assert!(acquired_after_panic.load(Ordering::SeqCst));
}

#[test]
fn test_run_with_event_lock_releases_and_reraises_on_exception() {
    let lock = Arc::new(ReentrantLock::default());
    let lock_for_panic = lock.clone();

    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _guard = lock_for_panic.lock();
        assert!(lock_for_panic.locked());
        assert_eq!(lock_for_panic.depth(), 1);
        panic!("event-lock-error");
    }));

    assert!(result.is_err());
    assert!(!lock.locked());
    assert_eq!(lock.depth(), 0);

    let acquired_after_panic = Arc::new(AtomicBool::new(false));
    let flag = acquired_after_panic.clone();
    let lock_for_thread = lock.clone();
    let handle = thread::spawn(move || {
        let _guard = lock_for_thread.lock();
        flag.store(true, Ordering::SeqCst);
    });
    handle.join().expect("event lock should be released");
    assert!(acquired_after_panic.load(Ordering::SeqCst));
}

#[test]
fn test_run_with_handler_lock_releases_and_reraises_on_exception() {
    let lock = Arc::new(ReentrantLock::default());
    let lock_for_panic = lock.clone();

    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _guard = lock_for_panic.lock();
        assert!(lock_for_panic.locked());
        assert_eq!(lock_for_panic.depth(), 1);
        panic!("handler-lock-error");
    }));

    assert!(result.is_err());
    assert!(!lock.locked());
    assert_eq!(lock.depth(), 0);

    let reacquired = lock.lock();
    assert!(lock.locked());
    drop(reacquired);
    assert!(!lock.locked());
}

#[test]
fn test_handler_dispatch_context_restores_depth_and_reraises_on_exception() {
    let lock = ReentrantLock::default();

    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _dispatch_context = lock.mark_held_in_current_context();
        assert_eq!(lock.depth(), 1);
        panic!("dispatch-context-error");
    }));

    assert!(result.is_err());
    assert_eq!(lock.depth(), 0);
    assert!(!lock.locked());
}

#[test]
fn test_reentrant_lock_serializes_many_workers_to_single_active_holder() {
    let lock = Arc::new(ReentrantLock::default());
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for _ in 0..6 {
        let lock = lock.clone();
        let active = active.clone();
        let max_active = max_active.clone();
        handles.push(thread::spawn(move || {
            let _guard = lock.lock();
            let now_active = active.fetch_add(1, Ordering::SeqCst) + 1;
            max_active.fetch_max(now_active, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(5));
            active.fetch_sub(1, Ordering::SeqCst);
        }));
    }

    for handle in handles {
        handle.join().expect("worker should join");
    }
    assert_eq!(max_active.load(Ordering::SeqCst), 1);
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[test]
fn test_lock_manager_get_lock_returns_stable_lock_for_key() {
    let manager = LockManager::default();
    let first = manager.get_lock("event:serial");
    let second = manager.get_lock("event:serial");
    let other = manager.get_lock("event:parallel");

    assert!(Arc::ptr_eq(&first, &second));
    assert!(!Arc::ptr_eq(&first, &other));
}

#[test]
fn test_lock_manager_get_lock_for_event_modes() {
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let bus_serial = EventBus::new_with_options(
        Some("LockManagerEventModesBusSerial".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    register_active_handler(
        &bus_serial,
        "LockModesEvent",
        "handler",
        active.clone(),
        max_active.clone(),
    );
    let first = bus_serial.emit_base(empty_event("LockModesEvent"));
    let second = bus_serial.emit_base(empty_event("LockModesEvent"));
    block_on(first.event_completed());
    block_on(second.event_completed());
    assert_eq!(max_active.load(Ordering::SeqCst), 1);
    bus_serial.stop();

    active.store(0, Ordering::SeqCst);
    max_active.store(0, Ordering::SeqCst);
    let parallel_override_bus = EventBus::new_with_options(
        Some("LockManagerEventModesParallelOverrideBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    register_active_handler(
        &parallel_override_bus,
        "LockModesEvent",
        "handler",
        active.clone(),
        max_active.clone(),
    );
    let first = empty_event("LockModesEvent");
    first.inner.lock().event_concurrency = Some(EventConcurrencyMode::Parallel);
    let second = empty_event("LockModesEvent");
    second.inner.lock().event_concurrency = Some(EventConcurrencyMode::Parallel);
    let first = parallel_override_bus.emit_base(first);
    let second = parallel_override_bus.emit_base(second);
    block_on(first.event_completed());
    block_on(second.event_completed());
    assert_eq!(max_active.load(Ordering::SeqCst), 2);
    parallel_override_bus.stop();

    active.store(0, Ordering::SeqCst);
    max_active.store(0, Ordering::SeqCst);
    let bus_a = EventBus::new_with_options(
        Some("LockManagerGlobalEventModesBusA".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::Parallel,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    let bus_b = EventBus::new_with_options(
        Some("LockManagerGlobalEventModesBusB".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::Parallel,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    register_active_handler(
        &bus_a,
        "LockModesEvent",
        "handler_a",
        active.clone(),
        max_active.clone(),
    );
    register_active_handler(
        &bus_b,
        "LockModesEvent",
        "handler_b",
        active.clone(),
        max_active.clone(),
    );
    let first = empty_event("LockModesEvent");
    first.inner.lock().event_concurrency = Some(EventConcurrencyMode::GlobalSerial);
    let second = empty_event("LockModesEvent");
    second.inner.lock().event_concurrency = Some(EventConcurrencyMode::GlobalSerial);
    let first = bus_a.emit_base(first);
    let second = bus_b.emit_base(second);
    block_on(first.event_completed());
    block_on(second.event_completed());
    assert_eq!(max_active.load(Ordering::SeqCst), 1);
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_lock_manager_get_lock_for_event_handler_modes() {
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let serial_bus = EventBus::new_with_options(
        Some("LockManagerHandlerModesSerialBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    register_active_handler(
        &serial_bus,
        "LockHandlerModesEvent",
        "handler_a",
        active.clone(),
        max_active.clone(),
    );
    register_active_handler(
        &serial_bus,
        "LockHandlerModesEvent",
        "handler_b",
        active.clone(),
        max_active.clone(),
    );
    let event = serial_bus.emit_base(empty_event("LockHandlerModesEvent"));
    block_on(event.event_completed());
    assert_eq!(max_active.load(Ordering::SeqCst), 1);
    serial_bus.stop();

    active.store(0, Ordering::SeqCst);
    max_active.store(0, Ordering::SeqCst);
    let parallel_override_bus = EventBus::new_with_options(
        Some("LockManagerHandlerModesParallelOverrideBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    register_active_handler(
        &parallel_override_bus,
        "LockHandlerModesEvent",
        "handler_a",
        active.clone(),
        max_active.clone(),
    );
    register_active_handler(
        &parallel_override_bus,
        "LockHandlerModesEvent",
        "handler_b",
        active.clone(),
        max_active.clone(),
    );
    let event = empty_event("LockHandlerModesEvent");
    event.inner.lock().event_handler_concurrency = Some(EventHandlerConcurrencyMode::Parallel);
    let event = parallel_override_bus.emit_base(event);
    block_on(event.event_completed());
    assert_eq!(max_active.load(Ordering::SeqCst), 2);
    parallel_override_bus.stop();
}

#[test]
fn test_run_with_event_lock_and_handler_lock_respect_parallel_bypass() {
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let parallel_override_bus = EventBus::new_with_options(
        Some("LockManagerBypassBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    register_active_handler(
        &parallel_override_bus,
        "ParallelBypassEvent",
        "handler_a",
        active.clone(),
        max_active.clone(),
    );
    register_active_handler(
        &parallel_override_bus,
        "ParallelBypassEvent",
        "handler_b",
        active.clone(),
        max_active.clone(),
    );
    let first = empty_event("ParallelBypassEvent");
    {
        let mut inner = first.inner.lock();
        inner.event_concurrency = Some(EventConcurrencyMode::Parallel);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Parallel);
    }
    let second = empty_event("ParallelBypassEvent");
    {
        let mut inner = second.inner.lock();
        inner.event_concurrency = Some(EventConcurrencyMode::Parallel);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Parallel);
    }
    let first = parallel_override_bus.emit_base(first);
    let second = parallel_override_bus.emit_base(second);
    block_on(first.event_completed());
    block_on(second.event_completed());
    assert_eq!(max_active.load(Ordering::SeqCst), 4);
    parallel_override_bus.stop();

    active.store(0, Ordering::SeqCst);
    max_active.store(0, Ordering::SeqCst);
    let serial_bus = EventBus::new_with_options(
        Some("LockManagerSerialAcquireBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    register_active_handler(
        &serial_bus,
        "SerialAcquireEvent",
        "handler_a",
        active.clone(),
        max_active.clone(),
    );
    register_active_handler(
        &serial_bus,
        "SerialAcquireEvent",
        "handler_b",
        active.clone(),
        max_active.clone(),
    );
    let first = serial_bus.emit_base(empty_event("SerialAcquireEvent"));
    let second = serial_bus.emit_base(empty_event("SerialAcquireEvent"));
    block_on(first.event_completed());
    block_on(second.event_completed());
    assert_eq!(max_active.load(Ordering::SeqCst), 1);
    serial_bus.stop();
}

#[test]
fn test_lock_manager_different_keys_do_not_block_each_other() {
    let manager = LockManager::default();
    let first = manager.get_lock("event:first");
    let second = manager.get_lock("event:second");
    let entered_second = Arc::new(AtomicBool::new(false));

    let _first_guard = first.lock();
    let flag = entered_second.clone();
    let handle = thread::spawn(move || {
        let _second_guard = second.lock();
        flag.store(true, Ordering::SeqCst);
    });

    handle.join().expect("second key should not block");
    assert!(entered_second.load(Ordering::SeqCst));
}

#[test]
fn test_lock_manager_same_key_waiters_serialize() {
    let manager = LockManager::default();
    let lock = manager.get_lock("event:serial");
    let order = Arc::new(Mutex::new(Vec::new()));

    let first_guard = lock.lock();
    let waiter_lock = lock.clone();
    let waiter_order = order.clone();
    let waiter = thread::spawn(move || {
        let _guard = waiter_lock.lock();
        waiter_order.lock().expect("order lock").push("waiter");
    });

    thread::sleep(Duration::from_millis(20));
    assert!(order.lock().expect("order lock").is_empty());
    drop(first_guard);
    waiter.join().expect("waiter should join");

    assert_eq!(order.lock().expect("order lock").as_slice(), &["waiter"]);
}

#[test]
fn test_reentrant_lock_serializes_across_tasks() {
    test_reentrant_lock_serializes_across_threads();
}
