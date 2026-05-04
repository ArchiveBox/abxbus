use std::{
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use abxbus_rust::{
    base_event::BaseEvent,
    event_bus::{EventBus, EventBusOptions},
    lock_manager::{LockManager, ReentrantLock},
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
    bus.on(event_type, handler_name, move |_event| {
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
fn test_reentrant_lock_nested_context_reuses_single_permit() {
    let lock = ReentrantLock::default();
    let outer = lock.lock();
    let inner = lock.lock();
    drop(inner);
    drop(outer);

    let after_nested = lock.lock();
    drop(after_nested);
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
