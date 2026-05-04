use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
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
fn test_lock_manager_get_lock_returns_stable_lock_for_key() {
    let manager = LockManager::default();
    let first = manager.get_lock("event:serial");
    let second = manager.get_lock("event:serial");
    let other = manager.get_lock("event:parallel");

    assert!(Arc::ptr_eq(&first, &second));
    assert!(!Arc::ptr_eq(&first, &other));
}
