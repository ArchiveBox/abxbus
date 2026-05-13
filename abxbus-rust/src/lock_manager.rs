use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Condvar, Mutex as StdMutex},
    thread::{self, ThreadId},
    time::{Duration, Instant},
};

use parking_lot::{Mutex, ReentrantMutex, ReentrantMutexGuard};

use crate::{
    base_event::BaseEvent,
    event_bus::EventBus,
    event_result::EventResult,
    types::{EventConcurrencyMode, EventHandlerConcurrencyMode},
};

#[derive(Clone)]
pub struct AsyncLock {
    inner: Arc<AsyncLockInner>,
}

struct AsyncLockInner {
    size: Option<usize>,
    state: StdMutex<AsyncLockState>,
}

#[derive(Default)]
struct AsyncLockState {
    in_use: usize,
    waiters: VecDeque<Arc<AsyncLockWaiter>>,
}

#[derive(Default)]
struct AsyncLockWaiter {
    ready: StdMutex<bool>,
    cvar: Condvar,
}

pub struct AsyncLockGuard {
    lock: AsyncLock,
    active: bool,
}

impl AsyncLock {
    pub fn new(size: usize) -> Self {
        assert!(size > 0, "AsyncLock size must be greater than zero");
        Self {
            inner: Arc::new(AsyncLockInner {
                size: Some(size),
                state: StdMutex::new(AsyncLockState::default()),
            }),
        }
    }

    pub fn infinite() -> Self {
        Self {
            inner: Arc::new(AsyncLockInner {
                size: None,
                state: StdMutex::new(AsyncLockState::default()),
            }),
        }
    }

    pub fn acquire(&self) -> AsyncLockGuard {
        let Some(size) = self.inner.size else {
            return AsyncLockGuard {
                lock: self.clone(),
                active: false,
            };
        };

        let waiter = {
            let mut state = self.inner.state.lock().expect("async lock state");
            if state.in_use < size && state.waiters.is_empty() {
                state.in_use += 1;
                return AsyncLockGuard {
                    lock: self.clone(),
                    active: true,
                };
            }

            let waiter = Arc::new(AsyncLockWaiter::default());
            state.waiters.push_back(waiter.clone());
            waiter
        };

        let mut ready = waiter.ready.lock().expect("async lock waiter");
        while !*ready {
            ready = waiter.cvar.wait(ready).expect("async lock waiter");
        }

        AsyncLockGuard {
            lock: self.clone(),
            active: true,
        }
    }

    pub fn release(&self) {
        let Some(_size) = self.inner.size else {
            return;
        };

        let next_waiter = {
            let mut state = self.inner.state.lock().expect("async lock state");
            if let Some(waiter) = state.waiters.pop_front() {
                Some(waiter)
            } else {
                state.in_use = state.in_use.saturating_sub(1);
                None
            }
        };

        if let Some(waiter) = next_waiter {
            let mut ready = waiter.ready.lock().expect("async lock waiter");
            *ready = true;
            waiter.cvar.notify_one();
        }
    }

    pub fn in_use(&self) -> usize {
        self.inner.state.lock().expect("async lock state").in_use
    }

    pub fn waiters_len(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("async lock state")
            .waiters
            .len()
    }
}

impl Drop for AsyncLockGuard {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            self.lock.release();
        }
    }
}

pub fn run_with_lock<T, E>(
    lock: Option<&AsyncLock>,
    run: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    let _guard = lock.map(AsyncLock::acquire);
    run()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HandlerExecutionState {
    Held,
    Yielded,
    Closed,
}

pub struct HandlerLock {
    lock: Option<AsyncLock>,
    state: StdMutex<HandlerExecutionState>,
    guard: StdMutex<Option<AsyncLockGuard>>,
}

impl HandlerLock {
    pub fn new_held(lock: AsyncLock, guard: AsyncLockGuard) -> Self {
        Self {
            lock: Some(lock),
            state: StdMutex::new(HandlerExecutionState::Held),
            guard: StdMutex::new(Some(guard)),
        }
    }

    pub fn null() -> Self {
        Self {
            lock: None,
            state: StdMutex::new(HandlerExecutionState::Held),
            guard: StdMutex::new(None),
        }
    }

    pub fn yield_handler_lock_for_child_run(&self) -> bool {
        let mut state = self.state.lock().expect("handler lock state");
        if self.lock.is_none() || *state != HandlerExecutionState::Held {
            return false;
        }
        *state = HandlerExecutionState::Yielded;
        drop(state);
        self.guard.lock().expect("handler lock guard").take();
        true
    }

    pub fn reclaim_handler_lock_if_running(&self) -> bool {
        let Some(lock) = self.lock.clone() else {
            return false;
        };
        if *self.state.lock().expect("handler lock state") != HandlerExecutionState::Yielded {
            return false;
        }

        let guard = lock.acquire();
        let mut state = self.state.lock().expect("handler lock state");
        if *state != HandlerExecutionState::Yielded {
            drop(guard);
            return false;
        }

        *self.guard.lock().expect("handler lock guard") = Some(guard);
        *state = HandlerExecutionState::Held;
        true
    }

    pub fn exit_handler_run(&self) {
        let mut state = self.state.lock().expect("handler lock state");
        if *state == HandlerExecutionState::Closed {
            return;
        }
        let should_release = *state == HandlerExecutionState::Held;
        *state = HandlerExecutionState::Closed;
        drop(state);
        if should_release {
            self.guard.lock().expect("handler lock guard").take();
        }
    }

    pub fn run_queue_jump<T>(&self, run: impl FnOnce() -> T) -> T {
        let yielded = self.yield_handler_lock_for_child_run();
        let value = run();
        if yielded {
            self.reclaim_handler_lock_if_running();
        }
        value
    }
}

#[derive(Default, Clone)]
pub struct ReentrantLock {
    lock: Arc<ReentrantMutex<()>>,
    state: Arc<StdMutex<ReentrantLockState>>,
}

#[derive(Default)]
struct ReentrantLockState {
    actual_holders: usize,
    thread_depths: HashMap<ThreadId, usize>,
}

pub struct ReentrantLockGuard<'a> {
    lock: &'a ReentrantLock,
    actual_guard: Option<ReentrantMutexGuard<'a, ()>>,
}

pub struct ReentrantContextMark<'a> {
    lock: &'a ReentrantLock,
    thread_id: ThreadId,
    previous_depth: usize,
    restored: bool,
}

impl ReentrantLock {
    pub fn lock(&self) -> ReentrantLockGuard<'_> {
        let current_depth = self.depth();
        if current_depth > 0 {
            self.set_depth_for_current_thread(current_depth + 1);
            return ReentrantLockGuard {
                lock: self,
                actual_guard: None,
            };
        }

        let guard = self.lock.lock();
        {
            let mut state = self.state.lock().expect("reentrant lock state");
            state.actual_holders += 1;
            state.thread_depths.insert(thread::current().id(), 1);
        }
        ReentrantLockGuard {
            lock: self,
            actual_guard: Some(guard),
        }
    }

    pub fn locked(&self) -> bool {
        self.state
            .lock()
            .expect("reentrant lock state")
            .actual_holders
            > 0
    }

    pub fn depth(&self) -> usize {
        self.state
            .lock()
            .expect("reentrant lock state")
            .thread_depths
            .get(&thread::current().id())
            .copied()
            .unwrap_or(0)
    }

    pub fn mark_held_in_current_context(&self) -> ReentrantContextMark<'_> {
        let thread_id = thread::current().id();
        let previous_depth = {
            let mut state = self.state.lock().expect("reentrant lock state");
            let previous_depth = state.thread_depths.get(&thread_id).copied().unwrap_or(0);
            state.thread_depths.insert(thread_id, previous_depth + 1);
            previous_depth
        };
        ReentrantContextMark {
            lock: self,
            thread_id,
            previous_depth,
            restored: false,
        }
    }

    fn set_depth_for_current_thread(&self, depth: usize) {
        let mut state = self.state.lock().expect("reentrant lock state");
        Self::set_thread_depth(&mut state, thread::current().id(), depth);
    }

    fn set_thread_depth(state: &mut ReentrantLockState, thread_id: ThreadId, depth: usize) {
        if depth == 0 {
            state.thread_depths.remove(&thread_id);
        } else {
            state.thread_depths.insert(thread_id, depth);
        }
    }
}

impl Drop for ReentrantLockGuard<'_> {
    fn drop(&mut self) {
        let thread_id = thread::current().id();
        let mut state = self.lock.state.lock().expect("reentrant lock state");
        let depth = state.thread_depths.get(&thread_id).copied().unwrap_or(0);
        let next_depth = depth.saturating_sub(1);
        ReentrantLock::set_thread_depth(&mut state, thread_id, next_depth);
        if self.actual_guard.is_some() {
            state.actual_holders = state.actual_holders.saturating_sub(1);
        }
        drop(state);
        self.actual_guard.take();
    }
}

impl ReentrantContextMark<'_> {
    pub fn restore(&mut self) {
        if self.restored {
            return;
        }
        self.restored = true;
        let mut state = self.lock.state.lock().expect("reentrant lock state");
        ReentrantLock::set_thread_depth(&mut state, self.thread_id, self.previous_depth);
    }
}

impl Drop for ReentrantContextMark<'_> {
    fn drop(&mut self) {
        self.restore();
    }
}

#[derive(Default)]
pub struct LockManager {
    locks: Mutex<HashMap<String, Arc<ReentrantLock>>>,
    pause_depth: StdMutex<usize>,
    pause_cvar: Condvar,
}

impl LockManager {
    pub fn get_lock(&self, key: &str) -> Arc<ReentrantLock> {
        let mut locks = self.locks.lock();
        locks
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(ReentrantLock::default()))
            .clone()
    }

    pub fn clear(&self) {
        self.locks.lock().clear();
    }

    pub fn get_lock_for_event(
        &self,
        bus: &EventBus,
        event: &Arc<BaseEvent>,
    ) -> Option<Arc<ReentrantLock>> {
        let resolved = event
            .inner
            .lock()
            .event_concurrency
            .unwrap_or(bus.event_concurrency);
        match resolved {
            EventConcurrencyMode::Parallel => None,
            EventConcurrencyMode::GlobalSerial => Some(EventBus::global_serial_lock()),
            EventConcurrencyMode::BusSerial => Some(bus.event_bus_serial_lock()),
        }
    }

    pub fn get_lock_for_event_handler(
        &self,
        _bus: &EventBus,
        event: &Arc<BaseEvent>,
        _event_result: &EventResult,
    ) -> Option<Arc<ReentrantLock>> {
        let resolved = event
            .inner
            .lock()
            .event_handler_concurrency
            .unwrap_or(_bus.event_handler_concurrency);
        match resolved {
            EventHandlerConcurrencyMode::Parallel => None,
            EventHandlerConcurrencyMode::Serial => {
                let event_id = event.inner.lock().event_id.clone();
                Some(self.get_lock(&format!("handler:{event_id}")))
            }
        }
    }

    pub fn request_runloop_pause(&self) -> RunloopPauseGuard<'_> {
        let mut pause_depth = self.pause_depth.lock().expect("pause depth");
        *pause_depth += 1;
        RunloopPauseGuard {
            manager: self,
            released: false,
        }
    }

    pub fn wait_until_runloop_resumed(&self) {
        let mut pause_depth = self.pause_depth.lock().expect("pause depth");
        while *pause_depth > 0 {
            pause_depth = self.pause_cvar.wait(pause_depth).expect("pause depth");
        }
    }

    pub fn is_paused(&self) -> bool {
        *self.pause_depth.lock().expect("pause depth") > 0
    }

    pub fn wait_for_idle(
        &self,
        timeout: Option<Duration>,
        mut is_idle_and_queue_empty: impl FnMut() -> bool,
    ) -> bool {
        let start = Instant::now();
        let mut consecutive_idle_checks = 0;
        loop {
            if is_idle_and_queue_empty() {
                consecutive_idle_checks += 1;
                if consecutive_idle_checks >= 2 {
                    return true;
                }
            } else {
                consecutive_idle_checks = 0;
            }

            if timeout.is_some_and(|timeout| start.elapsed() >= timeout) {
                return false;
            }
            thread::sleep(Duration::from_millis(1));
        }
    }
}

pub struct RunloopPauseGuard<'a> {
    manager: &'a LockManager,
    released: bool,
}

impl RunloopPauseGuard<'_> {
    pub fn release(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let mut pause_depth = self.manager.pause_depth.lock().expect("pause depth");
        *pause_depth = pause_depth.saturating_sub(1);
        if *pause_depth == 0 {
            self.manager.pause_cvar.notify_all();
        }
    }
}

impl Drop for RunloopPauseGuard<'_> {
    fn drop(&mut self) {
        self.release();
    }
}
