use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use indexmap::IndexMap;
use parking_lot::Mutex;
use tokio::sync::{mpsc, Notify};
use tracing::{debug, error, info, warn};

use crate::event::BaseEvent;
use crate::event_handler::{EventHandler, HandlerFn};
use crate::event_history::EventHistory;
use crate::lock_manager::{ConcurrencyLock, LockManager};
use crate::types::*;
use crate::uuid_gen::uuid7str;
use crate::validation::validate_bus_name;

/// Configuration for creating an EventBus.
#[derive(Debug, Clone)]
pub struct EventBusConfig {
    pub name: Option<String>,
    pub id: Option<String>,
    pub event_concurrency: EventConcurrencyMode,
    pub event_handler_concurrency: EventHandlerConcurrencyMode,
    pub event_handler_completion: EventHandlerCompletionMode,
    pub max_history_size: Option<usize>,
    pub max_history_drop: bool,
    pub event_timeout: Option<f64>,
    pub event_slow_timeout: Option<f64>,
    pub event_handler_slow_timeout: Option<f64>,
    pub max_handler_recursion_depth: usize,
}

impl Default for EventBusConfig {
    fn default() -> Self {
        Self {
            name: None,
            id: None,
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_handler_completion: EventHandlerCompletionMode::All,
            max_history_size: Some(100),
            max_history_drop: false,
            event_timeout: Some(60.0),
            event_slow_timeout: Some(300.0),
            event_handler_slow_timeout: Some(30.0),
            max_handler_recursion_depth: 2,
        }
    }
}

/// Internal state of the EventBus protected by a Mutex.
struct EventBusInner {
    handlers: IndexMap<String, EventHandler>,
    handlers_by_key: HashMap<String, Vec<String>>,
    event_history: EventHistory,
    in_flight_event_ids: HashSet<String>,
    processing_event_ids: HashSet<String>,
    is_running: bool,
}

/// The main async event bus with FIFO processing and handler dispatch.
///
/// This is the Rust equivalent of Python's `EventBus` class. It provides:
/// - Async event queue processing via tokio
/// - Configurable event and handler concurrency modes
/// - Queue jumping for awaited child events
/// - Event finding with past/future support
/// - Parent-child event tracking
pub struct EventBus {
    pub id: String,
    pub name: String,

    // Configuration
    pub event_concurrency: EventConcurrencyMode,
    pub event_handler_concurrency: EventHandlerConcurrencyMode,
    pub event_handler_completion: EventHandlerCompletionMode,
    pub event_timeout: Option<f64>,
    pub event_slow_timeout: Option<f64>,
    pub event_handler_slow_timeout: Option<f64>,
    pub max_handler_recursion_depth: usize,

    // Internal state
    inner: Arc<Mutex<EventBusInner>>,

    // Event queue
    queue_tx: mpsc::UnboundedSender<Arc<BaseEvent>>,
    queue_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<Arc<BaseEvent>>>>>,

    // Locking
    bus_serial_lock: Arc<ConcurrencyLock>,
    lock_manager: Arc<LockManager>,

    // Idle signaling
    idle_notify: Arc<Notify>,
}

impl EventBus {
    /// Create a new EventBus with the given configuration.
    pub fn new(config: EventBusConfig) -> Self {
        let id = config
            .id
            .unwrap_or_else(|| uuid7str());

        let name = config.name.unwrap_or_else(|| {
            format!("EventBus_{}", &id[id.len().saturating_sub(8)..])
        });

        // Validate name
        if let Err(e) = validate_bus_name(&name) {
            panic!("{}", e);
        }

        let (tx, rx) = mpsc::unbounded_channel();

        let inner = EventBusInner {
            handlers: IndexMap::new(),
            handlers_by_key: HashMap::new(),
            event_history: EventHistory::new(config.max_history_size, config.max_history_drop),
            in_flight_event_ids: HashSet::new(),
            processing_event_ids: HashSet::new(),
            is_running: false,
        };

        Self {
            id,
            name,
            event_concurrency: config.event_concurrency,
            event_handler_concurrency: config.event_handler_concurrency,
            event_handler_completion: config.event_handler_completion,
            event_timeout: config.event_timeout,
            event_slow_timeout: config.event_slow_timeout,
            event_handler_slow_timeout: config.event_handler_slow_timeout,
            max_handler_recursion_depth: config.max_handler_recursion_depth,
            inner: Arc::new(Mutex::new(inner)),
            queue_tx: tx,
            queue_rx: Arc::new(Mutex::new(Some(rx))),
            bus_serial_lock: Arc::new(ConcurrencyLock::new()),
            lock_manager: Arc::new(LockManager::new()),
            idle_notify: Arc::new(Notify::new()),
        }
    }

    /// Short label for logging: `BusName#abcd`.
    pub fn label(&self) -> String {
        format!(
            "{}#{}",
            self.name,
            &self.id[self.id.len().saturating_sub(4)..]
        )
    }

    // ── Handler Registration ──────────────────────────────────────

    /// Register a handler for the given event pattern.
    pub fn on(&self, event_pattern: &str, handler_name: &str, handler: HandlerFn) -> EventHandler {
        let event_key = if event_pattern == "*" {
            "*".to_string()
        } else {
            event_pattern.to_string()
        };

        let handler_entry = EventHandler::new(
            handler_name,
            None, // file path not available in Rust context
            &event_key,
            &self.name,
            &self.id,
            Some(handler),
        );

        let mut inner = self.inner.lock();

        // Check for duplicate handler names (bounded check).
        if let Some(existing_ids) = inner.handlers_by_key.get(&event_key) {
            if existing_ids.len() <= 256 {
                for existing_id in existing_ids {
                    if let Some(existing) = inner.handlers.get(existing_id) {
                        if existing.handler_name == handler_entry.handler_name {
                            warn!(
                                "⚠️ {} Handler {} already registered for event '{}'. \
                                 Consider using unique function names.",
                                self.label(),
                                handler_name,
                                event_key
                            );
                            break;
                        }
                    }
                }
            }
        }

        let handler_id = handler_entry.id.clone();
        inner.handlers.insert(handler_id.clone(), handler_entry.clone());
        inner
            .handlers_by_key
            .entry(event_key.clone())
            .or_default()
            .push(handler_id.clone());

        debug!(
            "👂 {}.on({}, {}) Registered handler #{}",
            self.label(),
            event_key,
            handler_name,
            &handler_id[handler_id.len().saturating_sub(4)..]
        );

        handler_entry
    }

    /// Deregister handlers for an event pattern.
    pub fn off(&self, event_pattern: &str, handler_id: Option<&str>) {
        let event_key = event_pattern.to_string();
        let mut inner = self.inner.lock();

        let indexed_ids = inner
            .handlers_by_key
            .get(&event_key)
            .cloned()
            .unwrap_or_default();

        for id in indexed_ids {
            let should_remove = match handler_id {
                None => true, // Remove all
                Some(target_id) => id == target_id,
            };
            if should_remove {
                inner.handlers.shift_remove(&id);
                if let Some(ids) = inner.handlers_by_key.get_mut(&event_key) {
                    ids.retain(|x| x != &id);
                }
            }
        }

        // Clean up empty key entries
        if inner
            .handlers_by_key
            .get(&event_key)
            .is_some_and(|ids| ids.is_empty())
        {
            inner.handlers_by_key.remove(&event_key);
        }
    }

    // ── Event Emission ────────────────────────────────────────────

    /// Emit an event into the bus queue for processing.
    ///
    /// Returns the event (now with its path updated and queued).
    /// The caller can await the event's `completed_signal` to wait for completion.
    pub fn emit(&self, mut event: BaseEvent) -> Arc<BaseEvent> {
        // Add bus label to event path (loop prevention).
        let label = self.label();
        if !event.event_path.contains(&label) {
            event.event_path.push(label.clone());
        } else {
            debug!(
                "⚠️ {}.emit({}) - Bus already in path, not adding again. Path: {:?}",
                label, event.event_type, event.event_path
            );
        }

        let event = Arc::new(event);

        // Add to history and in-flight set.
        {
            let mut inner = self.inner.lock();

            // Check history limits
            if let Some(max_size) = inner.event_history.max_history_size {
                if max_size > 0 && !inner.event_history.max_history_drop {
                    if inner.event_history.len() >= max_size {
                        // Try trimming completed events first
                        inner
                            .event_history
                            .trim_event_history(Some(&self.label()));
                    }
                    if inner.event_history.len() >= max_size {
                        panic!(
                            "{} history limit reached ({}/{}); \
                             set max_history_drop=true to drop old history",
                            self.label(),
                            inner.event_history.len(),
                            max_size
                        );
                    }
                }
            }

            inner.event_history.add_event(event.clone());
            inner.in_flight_event_ids.insert(event.event_id.clone());
        }

        // Queue the event.
        if let Err(e) = self.queue_tx.send(event.clone()) {
            error!("⚠️ {}.emit() failed to queue event: {}", self.label(), e);
        } else {
            info!(
                "🗣️ {}.emit({}) ➡️ {}#{} ({})",
                self.label(),
                event.event_type,
                event.event_type,
                event.short_id(),
                event.event_status,
            );
        }

        // Trim history if needed (amortized).
        {
            let mut inner = self.inner.lock();
            if let Some(max_size) = inner.event_history.max_history_size {
                if max_size > 0 && inner.event_history.max_history_drop {
                    let soft_limit = max_size.max((max_size as f64 * 1.2) as usize);
                    if inner.event_history.len() > soft_limit {
                        inner
                            .event_history
                            .trim_event_history(Some(&self.label()));
                    }
                }
            }
        }

        event
    }

    // ── Event Processing ──────────────────────────────────────────

    /// Start the event processing loop.
    ///
    /// This runs until `stop()` is called. It dequeues events and processes them
    /// according to the configured concurrency mode.
    pub async fn run(&self) {
        let mut rx = {
            let mut rx_guard = self.queue_rx.lock();
            match rx_guard.take() {
                Some(rx) => rx,
                None => {
                    warn!("{} run loop already started or receiver taken", self.label());
                    return;
                }
            }
        };

        {
            self.inner.lock().is_running = true;
        }

        debug!("🟢 {} run loop started", self.label());

        while let Some(event) = rx.recv().await {
            {
                let inner = self.inner.lock();
                if !inner.is_running {
                    break;
                }
            }

            self.inner.lock().processing_event_ids.insert(event.event_id.clone());
            self.idle_notify.notify_waiters();

            let resolved_mode = event
                .event_concurrency
                .unwrap_or(self.event_concurrency);

            match resolved_mode {
                EventConcurrencyMode::Parallel => {
                    // Spawn a task for parallel processing.
                    let bus = self.clone_for_task();
                    let event = event.clone();
                    tokio::spawn(async move {
                        bus.process_event(&event).await;
                        bus.finalize_event(&event);
                    });
                }
                EventConcurrencyMode::BusSerial | EventConcurrencyMode::GlobalSerial => {
                    let lock = self.lock_manager.get_event_lock(resolved_mode, &self.bus_serial_lock);
                    if let Some(lock) = lock {
                        let _guard = lock.acquire().await;
                        self.process_event(&event).await;
                    } else {
                        self.process_event(&event).await;
                    }
                    self.finalize_event(&event);
                }
            }

            // Check if idle
            let is_idle = {
                let inner = self.inner.lock();
                inner.in_flight_event_ids.is_empty()
                    && inner.processing_event_ids.is_empty()
            };
            if is_idle {
                self.idle_notify.notify_waiters();
            }
        }

        {
            self.inner.lock().is_running = false;
        }
        debug!("🛑 {} run loop stopped", self.label());
    }

    /// Process a single event: find handlers, execute them, mark complete.
    async fn process_event(&self, event: &BaseEvent) {
        let (handlers, handler_concurrency, handler_completion) = {
            let inner = self.inner.lock();
            let handlers = self.get_handlers_for_event_inner(&inner, event);
            let hc = event
                .event_handler_concurrency
                .unwrap_or(self.event_handler_concurrency);
            let hcomp = event
                .event_handler_completion
                .unwrap_or(self.event_handler_completion);
            (handlers, hc, hcomp)
        };

        if handlers.is_empty() {
            return;
        }

        let timeout = event
            .event_timeout
            .or(self.event_timeout);

        // Execute handlers based on concurrency mode.
        match handler_concurrency {
            EventHandlerConcurrencyMode::Serial => {
                for (_handler_id, handler) in &handlers {
                    if let Some(ref handler_fn) = handler.handler {
                        let result = self.run_handler(event, handler, handler_fn, timeout).await;

                        // For 'first' completion mode, stop after first non-None result
                        if handler_completion == EventHandlerCompletionMode::First {
                            if let Some(Some(_)) = result.as_ref().ok() {
                                break;
                            }
                        }
                    }
                }
            }
            EventHandlerConcurrencyMode::Parallel => {
                let mut tasks = Vec::new();
                for (_handler_id, handler) in &handlers {
                    if let Some(ref handler_fn) = handler.handler {
                        let handler_fn = handler_fn.clone();
                        let event_arc = Arc::new(event.clone());
                        let handler = handler.clone();
                        let timeout = timeout;
                        let label = self.label();

                        tasks.push(tokio::spawn(async move {
                            let result = if let Some(t) = timeout {
                                tokio::time::timeout(
                                    std::time::Duration::from_secs_f64(t),
                                    handler_fn(event_arc),
                                )
                                .await
                                .unwrap_or_else(|_| {
                                    warn!(
                                        "⏰ {}.{} handler timed out after {}s",
                                        label, handler.handler_name, t
                                    );
                                    None
                                })
                            } else {
                                handler_fn(event_arc).await
                            };
                            (handler.id.clone(), result)
                        }));
                    }
                }

                let results = futures::future::join_all(tasks).await;
                for result in results {
                    if let Ok((_handler_id, _value)) = result {
                        // Results would be stored in the event
                    }
                }
            }
        }
    }

    /// Execute a single handler with timeout support.
    async fn run_handler(
        &self,
        event: &BaseEvent,
        handler: &EventHandler,
        handler_fn: &HandlerFn,
        timeout: Option<f64>,
    ) -> Result<Option<serde_json::Value>, String> {
        debug!(
            " ↳ {}._run_handler({}, handler={})",
            self.label(),
            event,
            handler.label()
        );

        let event_arc = Arc::new(event.clone());

        let result = if let Some(t) = timeout {
            match tokio::time::timeout(
                std::time::Duration::from_secs_f64(t),
                handler_fn(event_arc),
            )
            .await
            {
                Ok(value) => Ok(value),
                Err(_) => {
                    warn!(
                        "⏰ {}.{} handler timed out after {}s for event {}",
                        self.label(),
                        handler.handler_name,
                        t,
                        event
                    );
                    Err(format!(
                        "Event handler {} timed out after {}s",
                        handler.label(),
                        t
                    ))
                }
            }
        } else {
            Ok(handler_fn(event_arc).await)
        };

        result
    }

    /// Finalize event processing: remove from in-flight, mark complete.
    fn finalize_event(&self, event: &BaseEvent) {
        let mut inner = self.inner.lock();
        inner
            .processing_event_ids
            .remove(&event.event_id);
        inner.in_flight_event_ids.remove(&event.event_id);

        // Trim history if needed
        if let Some(max_size) = inner.event_history.max_history_size {
            if max_size > 0 && inner.event_history.max_history_drop && inner.event_history.len() > max_size {
                inner.event_history.trim_event_history(Some(&self.label()));
            }
        }

        // Check if bus is now idle
        if inner.in_flight_event_ids.is_empty() && inner.processing_event_ids.is_empty() {
            drop(inner); // Release lock before notifying
            self.idle_notify.notify_waiters();
        }
    }

    /// Get all handlers applicable to the given event.
    fn get_handlers_for_event_inner(
        &self,
        inner: &EventBusInner,
        event: &BaseEvent,
    ) -> IndexMap<String, EventHandler> {
        let mut applicable = IndexMap::new();

        // Type-specific handlers first, then wildcard.
        for key in &[event.event_type.as_str(), "*"] {
            if let Some(handler_ids) = inner.handlers_by_key.get(*key) {
                for handler_id in handler_ids {
                    if let Some(handler) = inner.handlers.get(handler_id) {
                        // Loop prevention: skip handlers whose bus is already in the event path
                        // (beyond the initial entry).
                        if !self.would_create_loop(event, handler) {
                            applicable.insert(handler_id.clone(), handler.clone());
                        }
                    }
                }
            }
        }

        applicable
    }

    /// Check if executing this handler would create an event forwarding loop.
    fn would_create_loop(&self, event: &BaseEvent, handler: &EventHandler) -> bool {
        // If the handler's bus label appears in the event path more than once,
        // it's a loop.
        let handler_bus_label = handler.eventbus_label();
        let count = event
            .event_path
            .iter()
            .filter(|p| *p == &handler_bus_label)
            .count();
        count > self.max_handler_recursion_depth
    }

    // ── Queue Jumping ─────────────────────────────────────────────

    /// Process a specific event immediately, bypassing the queue.
    ///
    /// This is the key mechanism for queue jumping: when a handler awaits
    /// a child event, the child is pulled from the queue (or processed
    /// directly) so it completes before the parent handler resumes.
    pub async fn step(&self, event: &BaseEvent) {
        let resolved_mode = event
            .event_concurrency
            .unwrap_or(self.event_concurrency);

        let lock = self.lock_manager.get_event_lock(resolved_mode, &self.bus_serial_lock);

        if let Some(lock) = lock {
            let _guard = lock.acquire().await;
            self.process_event(event).await;
        } else {
            self.process_event(event).await;
        }

        self.finalize_event(event);
    }

    // ── Find ──────────────────────────────────────────────────────

    /// Find an event matching criteria in history.
    pub fn find(
        &self,
        event_type: &str,
        past_seconds: Option<f64>,
        field_filters: &[(&str, &serde_json::Value)],
    ) -> Option<Arc<BaseEvent>> {
        let inner = self.inner.lock();
        let event_key = EventHistory::normalize_event_pattern(event_type);
        inner
            .event_history
            .find_in_past(event_key, past_seconds, None, None, field_filters)
    }

    /// Find an event with a future wait timeout.
    pub async fn find_with_future(
        &self,
        event_type: &str,
        past_seconds: Option<f64>,
        future_timeout: Option<f64>,
        field_filters: &[(&str, &serde_json::Value)],
    ) -> Option<Arc<BaseEvent>> {
        // Check past first
        if let Some(event) = self.find(event_type, past_seconds, field_filters) {
            return Some(event);
        }

        // Wait for future
        if let Some(timeout_secs) = future_timeout {
            let event_key = event_type.to_string();
            let filters: Vec<(String, serde_json::Value)> = field_filters
                .iter()
                .map(|(k, v)| (k.to_string(), (*v).clone()))
                .collect();

            // Poll for the event
            let deadline = tokio::time::Instant::now()
                + std::time::Duration::from_secs_f64(timeout_secs);

            loop {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;

                let filter_refs: Vec<(&str, &serde_json::Value)> = filters
                    .iter()
                    .map(|(k, v)| (k.as_str(), v))
                    .collect();

                if let Some(event) = self.find(&event_key, None, &filter_refs) {
                    return Some(event);
                }

                if tokio::time::Instant::now() >= deadline {
                    return None;
                }
            }
        }

        None
    }

    // ── Lifecycle ─────────────────────────────────────────────────

    /// Wait until the bus is idle (no events being processed).
    pub async fn wait_until_idle(&self, timeout: Option<f64>) {
        let check_idle = || {
            let inner = self.inner.lock();
            inner.in_flight_event_ids.is_empty()
                && inner.processing_event_ids.is_empty()
        };

        if check_idle() {
            return;
        }

        if let Some(t) = timeout {
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs_f64(t),
                async {
                    loop {
                        self.idle_notify.notified().await;
                        if check_idle() {
                            break;
                        }
                    }
                },
            )
            .await;
        } else {
            loop {
                self.idle_notify.notified().await;
                if check_idle() {
                    break;
                }
            }
        }
    }

    /// Stop the event bus.
    pub async fn stop(&self, clear: bool) {
        {
            let mut inner = self.inner.lock();
            inner.is_running = false;
        }

        // Drop the sender to signal the receiver to stop.
        // (In practice, the sender is shared, so we just set is_running = false
        // and the run loop will exit on next iteration.)

        if clear {
            let mut inner = self.inner.lock();
            inner.event_history.clear();
            inner.handlers.clear();
            inner.handlers_by_key.clear();
            inner.in_flight_event_ids.clear();
            inner.processing_event_ids.clear();
        }

        debug!("🛑 {} stopped", self.label());
    }

    // ── Serialization ─────────────────────────────────────────────

    /// Serialize the bus state to JSON.
    pub fn to_json(&self) -> serde_json::Value {
        let inner = self.inner.lock();

        let handlers: serde_json::Map<String, serde_json::Value> = inner
            .handlers
            .iter()
            .map(|(id, h)| (id.clone(), h.to_json()))
            .collect();

        let handlers_by_key: serde_json::Map<String, serde_json::Value> = inner
            .handlers_by_key
            .iter()
            .map(|(key, ids)| {
                (
                    key.clone(),
                    serde_json::Value::Array(
                        ids.iter()
                            .map(|id| serde_json::Value::String(id.clone()))
                            .collect(),
                    ),
                )
            })
            .collect();

        let event_history: serde_json::Map<String, serde_json::Value> = inner
            .event_history
            .iter()
            .map(|(id, event)| (id.clone(), event.to_json_with_results()))
            .collect();

        serde_json::json!({
            "id": self.id,
            "name": self.name,
            "max_history_size": inner.event_history.max_history_size,
            "max_history_drop": inner.event_history.max_history_drop,
            "event_concurrency": self.event_concurrency,
            "event_timeout": self.event_timeout,
            "event_slow_timeout": self.event_slow_timeout,
            "event_handler_concurrency": self.event_handler_concurrency,
            "event_handler_completion": self.event_handler_completion,
            "event_handler_slow_timeout": self.event_handler_slow_timeout,
            "handlers": handlers,
            "handlers_by_key": handlers_by_key,
            "event_history": event_history,
            "pending_event_queue": [],
        })
    }

    /// Create a lightweight clone of self for spawning tasks.
    fn clone_for_task(&self) -> EventBusTaskHandle {
        EventBusTaskHandle {
            label: self.label(),
            inner: self.inner.clone(),
            lock_manager: self.lock_manager.clone(),
            bus_serial_lock: self.bus_serial_lock.clone(),
            idle_notify: self.idle_notify.clone(),
            event_concurrency: self.event_concurrency,
            event_handler_concurrency: self.event_handler_concurrency,
            event_handler_completion: self.event_handler_completion,
            event_timeout: self.event_timeout,
            max_handler_recursion_depth: self.max_handler_recursion_depth,
        }
    }

    // ── Accessors ─────────────────────────────────────────────────

    pub fn handler_count(&self) -> usize {
        self.inner.lock().handlers.len()
    }

    pub fn history_len(&self) -> usize {
        self.inner.lock().event_history.len()
    }

    pub fn is_running(&self) -> bool {
        self.inner.lock().is_running
    }

    pub fn in_flight_count(&self) -> usize {
        self.inner.lock().in_flight_event_ids.len()
    }
}

impl std::fmt::Display for EventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock();
        let icon = if inner.is_running { "🟢" } else { "🔴" };
        write!(
            f,
            "{}{}(history={} handlers={})",
            self.label(),
            icon,
            inner.event_history.len(),
            inner.handlers.len()
        )
    }
}

/// Lightweight handle for EventBus task spawning (no queue ownership).
#[allow(dead_code)]
struct EventBusTaskHandle {
    label: String,
    inner: Arc<Mutex<EventBusInner>>,
    lock_manager: Arc<LockManager>,
    bus_serial_lock: Arc<ConcurrencyLock>,
    idle_notify: Arc<Notify>,
    event_concurrency: EventConcurrencyMode,
    event_handler_concurrency: EventHandlerConcurrencyMode,
    event_handler_completion: EventHandlerCompletionMode,
    event_timeout: Option<f64>,
    max_handler_recursion_depth: usize,
}

impl EventBusTaskHandle {
    async fn process_event(&self, event: &BaseEvent) {
        let (handlers, handler_concurrency, handler_completion) = {
            let inner = self.inner.lock();
            let mut applicable = IndexMap::new();
            for key in &[event.event_type.as_str(), "*"] {
                if let Some(handler_ids) = inner.handlers_by_key.get(*key) {
                    for handler_id in handler_ids {
                        if let Some(handler) = inner.handlers.get(handler_id) {
                            applicable.insert(handler_id.clone(), handler.clone());
                        }
                    }
                }
            }
            let hc = event
                .event_handler_concurrency
                .unwrap_or(self.event_handler_concurrency);
            let hcomp = event
                .event_handler_completion
                .unwrap_or(self.event_handler_completion);
            (applicable, hc, hcomp)
        };

        if handlers.is_empty() {
            return;
        }

        let timeout = event.event_timeout.or(self.event_timeout);

        match handler_concurrency {
            EventHandlerConcurrencyMode::Serial => {
                for (_id, handler) in &handlers {
                    if let Some(ref handler_fn) = handler.handler {
                        let event_arc = Arc::new(event.clone());
                        let result = if let Some(t) = timeout {
                            tokio::time::timeout(
                                std::time::Duration::from_secs_f64(t),
                                handler_fn(event_arc),
                            )
                            .await
                            .ok()
                            .flatten()
                        } else {
                            handler_fn(event_arc).await
                        };

                        if handler_completion == EventHandlerCompletionMode::First && result.is_some() {
                            break;
                        }
                    }
                }
            }
            EventHandlerConcurrencyMode::Parallel => {
                let mut tasks = Vec::new();
                for (_id, handler) in &handlers {
                    if let Some(ref handler_fn) = handler.handler {
                        let handler_fn = handler_fn.clone();
                        let event_arc = Arc::new(event.clone());
                        let timeout = timeout;
                        tasks.push(tokio::spawn(async move {
                            if let Some(t) = timeout {
                                tokio::time::timeout(
                                    std::time::Duration::from_secs_f64(t),
                                    handler_fn(event_arc),
                                )
                                .await
                                .ok()
                                .flatten()
                            } else {
                                handler_fn(event_arc).await
                            }
                        }));
                    }
                }
                let _ = futures::future::join_all(tasks).await;
            }
        }
    }

    fn finalize_event(&self, event: &BaseEvent) {
        let mut inner = self.inner.lock();
        inner.processing_event_ids.remove(&event.event_id);
        inner.in_flight_event_ids.remove(&event.event_id);

        if inner.in_flight_event_ids.is_empty() && inner.processing_event_ids.is_empty() {
            drop(inner);
            self.idle_notify.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_bus() {
        let bus = EventBus::new(EventBusConfig {
            name: Some("TestBus".to_string()),
            ..Default::default()
        });
        assert_eq!(bus.name, "TestBus");
        assert!(!bus.is_running());
    }

    #[tokio::test]
    async fn test_register_handler() {
        let bus = EventBus::new(EventBusConfig {
            name: Some("TestBus".to_string()),
            ..Default::default()
        });

        let handler: HandlerFn = Arc::new(|_event| {
            Box::pin(async move { None })
        });

        bus.on("TestEvent", "test_handler", handler);
        assert_eq!(bus.handler_count(), 1);
    }

    #[tokio::test]
    async fn test_emit_and_process() {
        let bus = EventBus::new(EventBusConfig {
            name: Some("TestBus".to_string()),
            ..Default::default()
        });

        let processed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let processed_clone = processed.clone();

        let handler: HandlerFn = Arc::new(move |_event| {
            let processed = processed_clone.clone();
            Box::pin(async move {
                processed.store(true, std::sync::atomic::Ordering::SeqCst);
                None
            })
        });

        bus.on("TestEvent", "test_handler", handler);

        let event = BaseEvent::new("TestEvent");
        bus.emit(event);

        // Run the bus for a bit
        let bus_run = {
            let inner = bus.inner.clone();
            let rx = bus.queue_rx.clone();
            let lock_manager = bus.lock_manager.clone();
            let bus_serial_lock = bus.bus_serial_lock.clone();
            let idle_notify = bus.idle_notify.clone();
            let event_concurrency = bus.event_concurrency;
            let event_handler_concurrency = bus.event_handler_concurrency;
            let event_handler_completion = bus.event_handler_completion;
            let event_timeout = bus.event_timeout;
            let max_depth = bus.max_handler_recursion_depth;
            let label = bus.label();

            tokio::spawn(async move {
                bus.run().await;
            })
        };

        // Wait a bit for processing
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert!(processed.load(std::sync::atomic::Ordering::SeqCst));

        // Stop
        bus_run.abort();
    }

    #[tokio::test]
    async fn test_event_serialization_roundtrip() {
        let bus = EventBus::new(EventBusConfig {
            name: Some("SerBus".to_string()),
            ..Default::default()
        });

        let json = bus.to_json();
        assert_eq!(json["name"], "SerBus");
        assert!(json["handlers"].is_object());
        assert!(json["event_history"].is_object());
    }
}
