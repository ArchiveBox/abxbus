use std::{collections::HashMap, sync::Arc};

use parking_lot::Mutex;

#[derive(Default, Clone)]
pub struct ReentrantLock {
    lock: Arc<Mutex<()>>,
}

impl ReentrantLock {
    pub fn lock(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.lock.lock()
    }
}

#[derive(Default)]
pub struct LockManager {
    locks: Mutex<HashMap<String, Arc<ReentrantLock>>>,
}

impl LockManager {
    pub fn get_lock(&self, key: &str) -> Arc<ReentrantLock> {
        let mut locks = self.locks.lock();
        locks
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(ReentrantLock::default()))
            .clone()
    }
}
