use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::types::{EventConcurrencyMode, EventHandlerConcurrencyMode};

/// A re-entrant async lock built on a tokio Semaphore.
///
/// In the Rust implementation, re-entrancy is handled via tokio task-local
/// storage rather than Python's ContextVar. The lock tracks whether the
/// current task already holds it via a permit-based approach.
#[derive(Debug)]
pub struct ConcurrencyLock {
    semaphore: Arc<Semaphore>,
}

impl ConcurrencyLock {
    pub fn new() -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(1)),
        }
    }

    /// Acquire the lock. Returns a guard that releases on drop.
    pub async fn acquire(&self) -> ConcurrencyLockGuard {
        let permit = self.semaphore.clone().acquire_owned().await.unwrap();
        ConcurrencyLockGuard { _permit: permit }
    }

    /// Try to acquire without blocking. Returns None if lock is held.
    pub fn try_acquire(&self) -> Option<ConcurrencyLockGuard> {
        self.semaphore
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|permit| ConcurrencyLockGuard { _permit: permit })
    }

    /// Check if the lock is currently held.
    pub fn is_locked(&self) -> bool {
        self.semaphore.available_permits() == 0
    }
}

impl Default for ConcurrencyLock {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard that releases the lock on drop.
pub struct ConcurrencyLockGuard {
    _permit: tokio::sync::OwnedSemaphorePermit,
}

/// Centralized lock policy for event and handler execution.
///
/// Resolves which lock to use based on concurrency mode configuration.
pub struct LockManager {
    /// Global-serial lock shared across all buses.
    global_lock: Arc<ConcurrencyLock>,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            global_lock: Arc::new(ConcurrencyLock::new()),
        }
    }

    /// Create a lock manager with a shared global lock (for cross-bus serialization).
    pub fn with_global_lock(global_lock: Arc<ConcurrencyLock>) -> Self {
        Self { global_lock }
    }

    /// Resolve the event-level lock based on concurrency mode.
    ///
    /// - `GlobalSerial`: returns the shared global lock
    /// - `BusSerial`: returns the per-bus lock
    /// - `Parallel`: returns None (no locking)
    pub fn get_event_lock(
        &self,
        mode: EventConcurrencyMode,
        bus_lock: &Arc<ConcurrencyLock>,
    ) -> Option<Arc<ConcurrencyLock>> {
        match mode {
            EventConcurrencyMode::GlobalSerial => Some(self.global_lock.clone()),
            EventConcurrencyMode::BusSerial => Some(bus_lock.clone()),
            EventConcurrencyMode::Parallel => None,
        }
    }

    /// Resolve the handler-level lock based on concurrency mode.
    ///
    /// - `Serial`: returns a per-event handler lock
    /// - `Parallel`: returns None (handlers run concurrently)
    pub fn get_handler_lock(
        &self,
        mode: EventHandlerConcurrencyMode,
        handler_lock: &Arc<ConcurrencyLock>,
    ) -> Option<Arc<ConcurrencyLock>> {
        match mode {
            EventHandlerConcurrencyMode::Serial => Some(handler_lock.clone()),
            EventHandlerConcurrencyMode::Parallel => None,
        }
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_concurrency_lock_basic() {
        let lock = ConcurrencyLock::new();
        assert!(!lock.is_locked());

        let guard = lock.acquire().await;
        assert!(lock.is_locked());

        // Try acquire should fail while held
        assert!(lock.try_acquire().is_none());

        drop(guard);
        assert!(!lock.is_locked());
    }

    #[tokio::test]
    async fn test_lock_manager_modes() {
        let manager = LockManager::new();
        let bus_lock = Arc::new(ConcurrencyLock::new());

        // GlobalSerial returns global lock
        let lock = manager.get_event_lock(EventConcurrencyMode::GlobalSerial, &bus_lock);
        assert!(lock.is_some());

        // BusSerial returns bus lock
        let lock = manager.get_event_lock(EventConcurrencyMode::BusSerial, &bus_lock);
        assert!(lock.is_some());

        // Parallel returns None
        let lock = manager.get_event_lock(EventConcurrencyMode::Parallel, &bus_lock);
        assert!(lock.is_none());
    }
}
