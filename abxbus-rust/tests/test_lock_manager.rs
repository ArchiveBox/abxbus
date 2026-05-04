use std::{
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use abxbus_rust::lock_manager::{LockManager, ReentrantLock};

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
