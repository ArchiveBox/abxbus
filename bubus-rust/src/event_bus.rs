use std::{
    collections::{HashMap, VecDeque},
    sync::{mpsc as std_mpsc, Arc, OnceLock},
    thread,
    time::{Duration, Instant},
};

use event_listener::Event;
use futures::executor::block_on;
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::Value;

use crate::{
    base_event::{now_iso, BaseEvent},
    event_handler::{EventHandler, EventHandlerCallable},
    event_result::{EventResult, EventResultStatus},
    id::uuid_v7_string,
    lock_manager::ReentrantLock,
    types::{
        EventConcurrencyMode, EventHandlerCompletionMode, EventHandlerConcurrencyMode, EventStatus,
    },
};

static GLOBAL_SERIAL_LOCK: OnceLock<Arc<ReentrantLock>> = OnceLock::new();
thread_local! {
    static CURRENT_EVENT_ID: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
    static CURRENT_HANDLER_ID: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

struct FindWaiter {
    id: u64,
    pattern: String,
    child_of_event_id: Option<String>,
    sender: std_mpsc::Sender<Arc<BaseEvent>>,
}

struct BusRuntime {
    queue: Mutex<VecDeque<Arc<BaseEvent>>>,
    queue_notify: Event,
    stop: Mutex<bool>,
    events: Mutex<HashMap<String, Arc<BaseEvent>>>,
    history_order: Mutex<VecDeque<String>>,
    max_history_size: Option<usize>,
    max_history_drop: bool,
    find_waiters: Mutex<Vec<FindWaiter>>,
    next_waiter_id: Mutex<u64>,
}

#[derive(Clone, Serialize)]
pub struct EventBus {
    pub name: String,
    pub id: String,
    pub event_concurrency: EventConcurrencyMode,
    pub event_timeout: Option<f64>,
    pub event_slow_timeout: Option<f64>,
    pub event_handler_concurrency: EventHandlerConcurrencyMode,
    pub event_handler_completion: EventHandlerCompletionMode,
    pub event_handler_slow_timeout: Option<f64>,
    #[serde(skip)]
    handlers: Arc<Mutex<HashMap<String, Vec<EventHandler>>>>,
    #[serde(skip)]
    runtime: Arc<BusRuntime>,
    #[serde(skip)]
    bus_serial_lock: Arc<ReentrantLock>,
}

impl EventBus {
    pub fn new(name: Option<String>) -> Arc<Self> {
        Self::new_with_history(name, Some(100), false)
    }

    pub fn new_with_history(
        name: Option<String>,
        max_history_size: Option<usize>,
        max_history_drop: bool,
    ) -> Arc<Self> {
        let bus = Arc::new(Self {
            name: name.unwrap_or_else(|| "EventBus".to_string()),
            id: uuid_v7_string(),
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_timeout: Some(60.0),
            event_slow_timeout: Some(300.0),
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_handler_completion: EventHandlerCompletionMode::All,
            event_handler_slow_timeout: Some(30.0),
            handlers: Arc::new(Mutex::new(HashMap::new())),
            runtime: Arc::new(BusRuntime {
                queue: Mutex::new(VecDeque::new()),
                queue_notify: Event::new(),
                stop: Mutex::new(false),
                events: Mutex::new(HashMap::new()),
                history_order: Mutex::new(VecDeque::new()),
                max_history_size,
                max_history_drop,
                find_waiters: Mutex::new(Vec::new()),
                next_waiter_id: Mutex::new(0),
            }),
            bus_serial_lock: Arc::new(ReentrantLock::default()),
        });
        Self::start_loop(bus.clone());
        bus
    }

    fn start_loop(bus: Arc<Self>) {
        thread::spawn(move || {
            block_on(async move {
                loop {
                    if !bus.runtime.queue.lock().is_empty() {
                        thread::sleep(Duration::from_millis(1));
                    }
                    let next_event = bus.runtime.queue.lock().pop_front();
                    if let Some(event) = next_event {
                        let bus_for_task = bus.clone();
                        let mode = event
                            .inner
                            .lock()
                            .event_concurrency
                            .unwrap_or(bus.event_concurrency);
                        match mode {
                            EventConcurrencyMode::Parallel => {
                                thread::spawn(move || {
                                    block_on(bus_for_task.process_event(event));
                                });
                            }
                            EventConcurrencyMode::GlobalSerial
                            | EventConcurrencyMode::BusSerial => {
                                bus.process_event(event).await;
                            }
                        }
                        continue;
                    }

                    if *bus.runtime.stop.lock() {
                        break;
                    }
                    bus.runtime.queue_notify.listen().await;
                }
            });
        });
    }

    pub fn stop(&self) {
        *self.runtime.stop.lock() = true;
        self.runtime.queue_notify.notify(usize::MAX);
    }

    pub fn runtime_payload_for_test(&self) -> HashMap<String, Arc<BaseEvent>> {
        self.runtime.events.lock().clone()
    }

    pub fn event_history_ids(&self) -> Vec<String> {
        self.runtime.history_order.lock().iter().cloned().collect()
    }

    pub fn on<F, Fut>(&self, pattern: &str, handler_name: &str, handler_fn: F) -> EventHandler
    where
        F: Fn(Arc<BaseEvent>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Value, String>> + Send + 'static,
    {
        let callable: EventHandlerCallable = Arc::new(move |event| Box::pin(handler_fn(event)));
        let entry = EventHandler::from_callable(
            pattern.to_string(),
            handler_name.to_string(),
            self.name.clone(),
            self.id.clone(),
            callable,
        );
        self.handlers
            .lock()
            .entry(pattern.to_string())
            .or_default()
            .push(entry.clone());
        entry
    }

    pub fn off(&self, pattern: &str, handler_id: Option<&str>) {
        let mut handlers = self.handlers.lock();
        if let Some(list) = handlers.get_mut(pattern) {
            if let Some(handler_id) = handler_id {
                list.retain(|handler| handler.id != handler_id);
            } else {
                list.clear();
            }
        }
    }

    pub fn emit(&self, event: Arc<BaseEvent>) -> Arc<BaseEvent> {
        self.emit_with_options(event, false)
    }

    pub fn emit_with_options(&self, event: Arc<BaseEvent>, queue_jump: bool) -> Arc<BaseEvent> {
        if !self.register_in_history(event.clone()) {
            event.mark_completed();
            return event;
        }

        {
            let mut inner = event.inner.lock();
            inner.event_pending_bus_count += 1;
            inner
                .event_path
                .push(format!("{}#{}", self.name, &self.id[0..4]));
            CURRENT_EVENT_ID.with(|id| inner.event_parent_id = id.borrow().clone());
            CURRENT_HANDLER_ID.with(|id| inner.event_emitted_by_handler_id = id.borrow().clone());
        }

        {
            let mut queue = self.runtime.queue.lock();
            if queue_jump {
                queue.push_front(event.clone());
            } else {
                queue.push_back(event.clone());
            }
        }

        self.notify_find_waiters(event.clone());
        self.runtime.queue_notify.notify(1);
        event
    }

    fn register_in_history(&self, event: Arc<BaseEvent>) -> bool {
        let event_id = event.inner.lock().event_id.clone();

        if let Some(max_size) = self.runtime.max_history_size {
            let current_size = self.runtime.history_order.lock().len();
            if current_size >= max_size {
                if self.runtime.max_history_drop {
                    while self.runtime.history_order.lock().len() >= max_size {
                        if let Some(oldest) = self.runtime.history_order.lock().pop_front() {
                            self.runtime.events.lock().remove(&oldest);
                        } else {
                            break;
                        }
                    }
                } else {
                    return false;
                }
            }
        }

        self.runtime.events.lock().insert(event_id.clone(), event);
        self.runtime.history_order.lock().push_back(event_id);
        true
    }

    pub async fn find(
        &self,
        pattern: &str,
        past: bool,
        future: Option<f64>,
        child_of: Option<Arc<BaseEvent>>,
    ) -> Option<Arc<BaseEvent>> {
        if past {
            let child_of_event_id = child_of
                .as_ref()
                .map(|event| event.inner.lock().event_id.clone());
            if let Some(matched) = self.find_in_history(pattern, child_of_event_id.as_deref()) {
                return Some(matched);
            }
        }

        let future = future?;
        let (tx, rx) = std_mpsc::channel();
        let waiter_id = {
            let mut next = self.runtime.next_waiter_id.lock();
            *next += 1;
            *next
        };

        self.runtime.find_waiters.lock().push(FindWaiter {
            id: waiter_id,
            pattern: pattern.to_string(),
            child_of_event_id: child_of.map(|event| event.inner.lock().event_id.clone()),
            sender: tx,
        });

        let result = rx.recv_timeout(Duration::from_secs_f64(future)).ok();
        self.runtime
            .find_waiters
            .lock()
            .retain(|waiter| waiter.id != waiter_id);
        result
    }

    fn find_in_history(
        &self,
        pattern: &str,
        child_of_event_id: Option<&str>,
    ) -> Option<Arc<BaseEvent>> {
        let history = self.runtime.history_order.lock().clone();
        for event_id in history.iter().rev() {
            let event = self.runtime.events.lock().get(event_id).cloned()?;
            if !self.matches_pattern(&event, pattern) {
                continue;
            }
            if let Some(parent_id) = child_of_event_id {
                if !self.event_is_child_of_ids(&event.inner.lock().event_id, parent_id) {
                    continue;
                }
            }
            return Some(event);
        }
        None
    }

    fn matches_pattern(&self, event: &Arc<BaseEvent>, pattern: &str) -> bool {
        if pattern == "*" {
            return true;
        }
        event.inner.lock().event_type == pattern
    }

    fn notify_find_waiters(&self, event: Arc<BaseEvent>) {
        let event_id = event.inner.lock().event_id.clone();
        let mut matched_waiter_ids = Vec::new();
        let mut matched_senders = Vec::new();

        {
            let waiters = self.runtime.find_waiters.lock();
            for waiter in waiters.iter() {
                if !self.matches_pattern(&event, &waiter.pattern) {
                    continue;
                }
                if let Some(parent_id) = waiter.child_of_event_id.as_deref() {
                    if !self.event_is_child_of_ids(&event_id, parent_id) {
                        continue;
                    }
                }
                matched_waiter_ids.push(waiter.id);
                matched_senders.push(waiter.sender.clone());
            }
        }

        if !matched_waiter_ids.is_empty() {
            self.runtime
                .find_waiters
                .lock()
                .retain(|waiter| !matched_waiter_ids.contains(&waiter.id));
            for sender in matched_senders {
                let _ = sender.send(event.clone());
            }
        }
    }

    pub fn event_is_child_of(
        &self,
        child_event: &Arc<BaseEvent>,
        parent_event: &Arc<BaseEvent>,
    ) -> bool {
        let child_id = child_event.inner.lock().event_id.clone();
        let parent_id = parent_event.inner.lock().event_id.clone();
        self.event_is_child_of_ids(&child_id, &parent_id)
    }

    pub fn event_is_parent_of(
        &self,
        parent_event: &Arc<BaseEvent>,
        child_event: &Arc<BaseEvent>,
    ) -> bool {
        self.event_is_child_of(child_event, parent_event)
    }

    fn event_is_child_of_ids(&self, child_event_id: &str, parent_event_id: &str) -> bool {
        if child_event_id == parent_event_id {
            return false;
        }

        let mut current_id = child_event_id.to_string();
        loop {
            let Some(current_event) = self.runtime.events.lock().get(&current_id).cloned() else {
                return false;
            };
            let current_parent = current_event.inner.lock().event_parent_id.clone();
            let Some(current_parent_id) = current_parent else {
                return false;
            };
            if current_parent_id == parent_event_id {
                return true;
            }
            current_id = current_parent_id;
        }
    }

    pub async fn wait_until_idle(&self, timeout: Option<f64>) -> bool {
        let start = Instant::now();
        loop {
            let queue_empty = self.runtime.queue.lock().is_empty();
            let all_completed = self.runtime.events.lock().values().all(|event| {
                let status = event.inner.lock().event_status;
                status == EventStatus::Completed
            });
            if queue_empty && all_completed {
                return true;
            }

            if let Some(timeout) = timeout {
                if start.elapsed() > Duration::from_secs_f64(timeout) {
                    return false;
                }
            }
            thread::sleep(Duration::from_millis(5));
        }
    }

    async fn process_event(&self, event: Arc<BaseEvent>) {
        let mode = event
            .inner
            .lock()
            .event_concurrency
            .unwrap_or(self.event_concurrency);
        match mode {
            EventConcurrencyMode::GlobalSerial => {
                let _guard = GLOBAL_SERIAL_LOCK
                    .get_or_init(|| Arc::new(ReentrantLock::default()))
                    .lock();
                self.process_event_inner(event).await;
            }
            EventConcurrencyMode::BusSerial => {
                let _guard = self.bus_serial_lock.lock();
                self.process_event_inner(event).await;
            }
            EventConcurrencyMode::Parallel => {
                self.process_event_inner(event).await;
            }
        }
    }

    async fn process_event_inner(&self, event: Arc<BaseEvent>) {
        event.mark_started();
        let started_at = Instant::now();

        let event_type = event.inner.lock().event_type.clone();
        let mut handlers = self
            .handlers
            .lock()
            .get(&event_type)
            .cloned()
            .unwrap_or_default();
        handlers.extend(self.handlers.lock().get("*").cloned().unwrap_or_default());

        let handler_concurrency = event
            .inner
            .lock()
            .event_handler_concurrency
            .unwrap_or(self.event_handler_concurrency);
        let handler_completion = event
            .inner
            .lock()
            .event_handler_completion
            .unwrap_or(self.event_handler_completion);

        let event_timeout = event.inner.lock().event_timeout.or(self.event_timeout);

        match handler_concurrency {
            EventHandlerConcurrencyMode::Serial => {
                for handler in handlers {
                    let timed_out = self
                        .run_handler_with_context(event.clone(), handler, started_at, event_timeout)
                        .await;
                    if timed_out {
                        break;
                    }
                    if handler_completion == EventHandlerCompletionMode::First
                        && self.has_winner(&event)
                    {
                        break;
                    }
                }
            }
            EventHandlerConcurrencyMode::Parallel => {
                let mut join_handles = Vec::new();
                for handler in handlers {
                    let bus = self.clone();
                    let event_clone = event.clone();
                    join_handles.push(thread::spawn(move || {
                        block_on(bus.run_handler_with_context(
                            event_clone,
                            handler,
                            started_at,
                            event_timeout,
                        ))
                    }));
                }
                for handle in join_handles {
                    let _ = handle.join();
                    if handler_completion == EventHandlerCompletionMode::First
                        && self.has_winner(&event)
                    {
                        break;
                    }
                }
            }
        }

        if let Some(timeout) = event_timeout {
            if started_at.elapsed() > Duration::from_secs_f64(timeout) {
                self.cancel_children(&event, &format!("parent event timed out after {timeout}s"));
            }
        }

        if let Some(slow) = event
            .inner
            .lock()
            .event_slow_timeout
            .or(self.event_slow_timeout)
        {
            if started_at.elapsed() > Duration::from_secs_f64(slow) {
                eprintln!(
                    "slow event warning: {} took {:?}",
                    event.inner.lock().event_type,
                    started_at.elapsed()
                );
            }
        }

        {
            let mut inner = event.inner.lock();
            inner.event_pending_bus_count = inner.event_pending_bus_count.saturating_sub(1);
            if inner.event_status != EventStatus::Completed {
                inner.event_completed_at = Some(now_iso());
            }
        }
        event.mark_completed();

        if self.runtime.max_history_size == Some(0) {
            let event_id = event.inner.lock().event_id.clone();
            self.runtime.events.lock().remove(&event_id);
            self.runtime
                .history_order
                .lock()
                .retain(|id| id != &event_id);
        }
    }

    fn cancel_children(&self, event: &Arc<BaseEvent>, reason: &str) {
        let results = event.inner.lock().event_results.clone();
        for result in results.values() {
            for child_id in &result.event_children {
                if let Some(child) = self.runtime.events.lock().get(child_id).cloned() {
                    for child_result in child.inner.lock().event_results.values_mut() {
                        if child_result.status == EventResultStatus::Pending
                            || child_result.status == EventResultStatus::Started
                        {
                            child_result.status = EventResultStatus::Error;
                            child_result.error = Some(format!("cancelled: {reason}"));
                            child_result.completed_at = Some(now_iso());
                        }
                    }
                    child.mark_completed();
                }
            }
        }
    }

    fn has_winner(&self, event: &Arc<BaseEvent>) -> bool {
        event.inner.lock().event_results.values().any(|result| {
            result.status == EventResultStatus::Completed
                && result.error.is_none()
                && result.result.is_some()
        })
    }

    async fn run_handler_with_context(
        &self,
        event: Arc<BaseEvent>,
        handler: EventHandler,
        event_started_at: Instant,
        event_timeout: Option<f64>,
    ) -> bool {
        let handler_id = handler.id.clone();
        CURRENT_EVENT_ID.with(|id| *id.borrow_mut() = Some(event.inner.lock().event_id.clone()));
        CURRENT_HANDLER_ID.with(|id| *id.borrow_mut() = Some(handler_id.clone()));
        let timed_out = self
            .run_handler(event, handler, event_started_at, event_timeout)
            .await;
        CURRENT_HANDLER_ID.with(|id| *id.borrow_mut() = None);
        CURRENT_EVENT_ID.with(|id| *id.borrow_mut() = None);
        timed_out
    }

    async fn run_handler(
        &self,
        event: Arc<BaseEvent>,
        handler: EventHandler,
        event_started_at: Instant,
        event_timeout: Option<f64>,
    ) -> bool {
        let handler_timeout = handler
            .handler_timeout
            .or(event.inner.lock().event_handler_timeout)
            .or(event_timeout);

        let remaining_event_timeout = event_timeout.map(|timeout| {
            let elapsed = event_started_at.elapsed().as_secs_f64();
            (timeout - elapsed).max(0.0)
        });

        let timeout = match (handler_timeout, remaining_event_timeout) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };

        let mut result = EventResult::new(
            event.inner.lock().event_id.clone(),
            handler.clone(),
            timeout,
        );
        result.status = EventResultStatus::Started;
        result.started_at = Some(now_iso());
        event
            .inner
            .lock()
            .event_results
            .insert(handler.id.clone(), result.clone());

        let call = handler
            .callable
            .as_ref()
            .expect("handler callable missing")
            .clone();
        let (tx, rx) = std_mpsc::channel();
        let event_clone = event.clone();
        let context_event_id = event.inner.lock().event_id.clone();
        let context_handler_id = handler.id.clone();
        thread::spawn(move || {
            CURRENT_EVENT_ID.with(|id| *id.borrow_mut() = Some(context_event_id));
            CURRENT_HANDLER_ID.with(|id| *id.borrow_mut() = Some(context_handler_id));
            let response = block_on(call(event_clone));
            CURRENT_HANDLER_ID.with(|id| *id.borrow_mut() = None);
            CURRENT_EVENT_ID.with(|id| *id.borrow_mut() = None);
            let _ = tx.send(response);
        });

        let call_started = Instant::now();
        let call_result = if let Some(timeout_secs) = timeout {
            rx.recv_timeout(Duration::from_secs_f64(timeout_secs))
                .map_err(|_| "timeout".to_string())
        } else {
            rx.recv().map_err(|_| "handler channel closed".to_string())
        };

        let mut current = event
            .inner
            .lock()
            .event_results
            .get(&handler.id)
            .cloned()
            .expect("missing result row");

        match call_result {
            Ok(Ok(value)) => {
                current.status = EventResultStatus::Completed;
                current.result = Some(value);
            }
            Ok(Err(error)) => {
                current.status = EventResultStatus::Error;
                current.error = Some(error);
            }
            Err(error) => {
                current.status = EventResultStatus::Error;
                current.error = Some(format!("EventHandlerAbortedError: {error}"));
                current.completed_at = Some(now_iso());
                if let Some(latest) = event.inner.lock().event_results.get(&handler.id).cloned() {
                    current.event_children = latest.event_children;
                }
                event
                    .inner
                    .lock()
                    .event_results
                    .insert(handler.id.clone(), current);
                return true;
            }
        }

        if let Some(slow_timeout) = handler
            .handler_slow_timeout
            .or(event.inner.lock().event_handler_slow_timeout)
            .or(self.event_handler_slow_timeout)
        {
            if call_started.elapsed() > Duration::from_secs_f64(slow_timeout) {
                eprintln!(
                    "slow handler warning: {} took {:?}",
                    handler.handler_name,
                    call_started.elapsed()
                );
            }
        }

        current.completed_at = Some(now_iso());
        if let Some(latest) = event.inner.lock().event_results.get(&handler.id).cloned() {
            current.event_children = latest.event_children;
        }
        event.inner.lock().event_results.insert(handler.id, current);
        false
    }
}
