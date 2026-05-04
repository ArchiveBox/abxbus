use std::{
    collections::{HashMap, VecDeque},
    sync::{mpsc as std_mpsc, Arc, OnceLock, Weak},
    thread,
    time::{Duration, Instant},
};

use event_listener::Event;
use futures::executor::block_on;
use parking_lot::Mutex;
use serde::{Serialize, Serializer};
use serde_json::{json, Map, Value};

use crate::{
    base_event::{now_iso, BaseEvent},
    event_handler::{EventHandler, EventHandlerCallable, EventHandlerOptions},
    event_result::{EventResult, EventResultStatus},
    id::uuid_v7_string,
    lock_manager::ReentrantLock,
    types::{
        EventConcurrencyMode, EventHandlerCompletionMode, EventHandlerConcurrencyMode, EventStatus,
    },
};

static GLOBAL_SERIAL_LOCK: OnceLock<Arc<ReentrantLock>> = OnceLock::new();
static ALL_INSTANCES: OnceLock<Mutex<Vec<Weak<EventBus>>>> = OnceLock::new();
thread_local! {
    static CURRENT_BUS_ID: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
    static CURRENT_EVENT_ID: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
    static CURRENT_HANDLER_ID: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

struct FindWaiter {
    id: u64,
    pattern: String,
    child_of_event_id: Option<String>,
    where_filter: Option<HashMap<String, Value>>,
    sender: std_mpsc::Sender<Arc<BaseEvent>>,
}

struct BusRuntime {
    queue: Mutex<VecDeque<Arc<BaseEvent>>>,
    queue_notify: Event,
    stop: Mutex<bool>,
    loop_started: Mutex<bool>,
    events: Mutex<HashMap<String, Arc<BaseEvent>>>,
    history_order: Mutex<VecDeque<String>>,
    max_history_size: Option<usize>,
    max_history_drop: bool,
    find_waiters: Mutex<Vec<FindWaiter>>,
    next_waiter_id: Mutex<u64>,
}

#[derive(Clone)]
pub struct EventBus {
    pub name: String,
    pub id: String,
    pub event_concurrency: EventConcurrencyMode,
    pub event_timeout: Option<f64>,
    pub event_slow_timeout: Option<f64>,
    pub event_handler_concurrency: EventHandlerConcurrencyMode,
    pub event_handler_completion: EventHandlerCompletionMode,
    pub event_handler_slow_timeout: Option<f64>,
    pub event_handler_detect_file_paths: bool,
    handlers: Arc<Mutex<HashMap<String, Vec<EventHandler>>>>,
    runtime: Arc<BusRuntime>,
    bus_serial_lock: Arc<ReentrantLock>,
}

impl Serialize for EventBus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_json_value().serialize(serializer)
    }
}

#[derive(Debug, Clone)]
pub struct EventBusOptions {
    pub id: Option<String>,
    pub max_history_size: Option<usize>,
    pub max_history_drop: bool,
    pub event_concurrency: EventConcurrencyMode,
    pub event_timeout: Option<f64>,
    pub event_slow_timeout: Option<f64>,
    pub event_handler_concurrency: EventHandlerConcurrencyMode,
    pub event_handler_completion: EventHandlerCompletionMode,
    pub event_handler_slow_timeout: Option<f64>,
    pub event_handler_detect_file_paths: bool,
}

#[derive(Clone, Default)]
pub struct FindOptions {
    pub past: bool,
    pub past_window: Option<f64>,
    pub future: Option<f64>,
    pub child_of: Option<Arc<BaseEvent>>,
    pub where_filter: Option<HashMap<String, Value>>,
}

impl Default for EventBusOptions {
    fn default() -> Self {
        Self {
            id: None,
            max_history_size: Some(100),
            max_history_drop: false,
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_timeout: Some(60.0),
            event_slow_timeout: Some(300.0),
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_handler_completion: EventHandlerCompletionMode::All,
            event_handler_slow_timeout: Some(30.0),
            event_handler_detect_file_paths: true,
        }
    }
}

impl EventBus {
    pub fn new(name: Option<String>) -> Arc<Self> {
        Self::new_with_options(name, EventBusOptions::default())
    }

    pub fn new_with_history(
        name: Option<String>,
        max_history_size: Option<usize>,
        max_history_drop: bool,
    ) -> Arc<Self> {
        Self::new_with_options(
            name,
            EventBusOptions {
                max_history_size,
                max_history_drop,
                ..EventBusOptions::default()
            },
        )
    }

    pub fn new_with_options(name: Option<String>, options: EventBusOptions) -> Arc<Self> {
        Self::new_with_options_and_loop(name, options, true)
    }

    fn new_with_options_and_loop(
        name: Option<String>,
        options: EventBusOptions,
        start_loop: bool,
    ) -> Arc<Self> {
        if let Some(timeout) = options.event_timeout {
            assert!(timeout > 0.0, "event_timeout must be > 0 or None");
        }
        if let Some(timeout) = options.event_slow_timeout {
            assert!(timeout > 0.0, "event_slow_timeout must be > 0 or None");
        }
        if let Some(timeout) = options.event_handler_slow_timeout {
            assert!(
                timeout > 0.0,
                "event_handler_slow_timeout must be > 0 or None"
            );
        }

        let id = options.id.unwrap_or_else(uuid_v7_string);
        let resolved_name = name.unwrap_or_else(|| "EventBus".to_string());
        assert!(
            resolved_name
                .chars()
                .next()
                .is_some_and(|ch| ch == '_' || ch.is_ascii_alphabetic())
                && resolved_name
                    .chars()
                    .all(|ch| ch == '_' || ch.is_ascii_alphanumeric()),
            "EventBus name must be a unique identifier string, got: {resolved_name}"
        );

        let bus = Arc::new(Self {
            name: resolved_name,
            id,
            event_concurrency: options.event_concurrency,
            event_timeout: options.event_timeout,
            event_slow_timeout: options.event_slow_timeout,
            event_handler_concurrency: options.event_handler_concurrency,
            event_handler_completion: options.event_handler_completion,
            event_handler_slow_timeout: options.event_handler_slow_timeout,
            event_handler_detect_file_paths: options.event_handler_detect_file_paths,
            handlers: Arc::new(Mutex::new(HashMap::new())),
            runtime: Arc::new(BusRuntime {
                queue: Mutex::new(VecDeque::new()),
                queue_notify: Event::new(),
                stop: Mutex::new(false),
                loop_started: Mutex::new(false),
                events: Mutex::new(HashMap::new()),
                history_order: Mutex::new(VecDeque::new()),
                max_history_size: options.max_history_size,
                max_history_drop: options.max_history_drop,
                find_waiters: Mutex::new(Vec::new()),
                next_waiter_id: Mutex::new(0),
            }),
            bus_serial_lock: Arc::new(ReentrantLock::default()),
        });
        Self::register_instance(&bus);
        if start_loop {
            bus.ensure_loop_started();
        }
        bus
    }

    fn register_instance(bus: &Arc<Self>) {
        let mut instances = ALL_INSTANCES.get_or_init(|| Mutex::new(Vec::new())).lock();
        instances.retain(|entry| entry.upgrade().is_some());
        instances.push(Arc::downgrade(bus));
    }

    pub fn all_instances_contains(bus: &Arc<Self>) -> bool {
        let mut instances = ALL_INSTANCES.get_or_init(|| Mutex::new(Vec::new())).lock();
        instances.retain(|entry| entry.upgrade().is_some());
        instances
            .iter()
            .filter_map(Weak::upgrade)
            .any(|entry| Arc::ptr_eq(&entry, bus))
    }

    pub fn all_instances_len() -> usize {
        let mut instances = ALL_INSTANCES.get_or_init(|| Mutex::new(Vec::new())).lock();
        instances.retain(|entry| entry.upgrade().is_some());
        instances.len()
    }

    fn ensure_loop_started(&self) {
        let mut started = self.runtime.loop_started.lock();
        if *started {
            return;
        }
        *started = true;
        let bus = self.clone();
        drop(started);
        Self::start_loop(bus);
    }

    fn start_loop(bus: Self) {
        thread::spawn(move || {
            block_on(async move {
                loop {
                    let listener = bus.runtime.queue_notify.listen();
                    let next_event = bus.runtime.queue.lock().pop_front();
                    if let Some(event) = next_event {
                        drop(listener);
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
                                thread::sleep(Duration::from_millis(1));
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
                    listener.await;
                }
            });
        });
    }

    pub fn stop(&self) {
        *self.runtime.stop.lock() = true;
        self.runtime.queue_notify.notify(usize::MAX);
    }

    pub fn label(&self) -> String {
        format!(
            "{}#{}",
            self.name,
            &self.id[self.id.len().saturating_sub(4)..]
        )
    }

    pub fn max_history_size(&self) -> Option<usize> {
        self.runtime.max_history_size
    }

    pub fn max_history_drop(&self) -> bool {
        self.runtime.max_history_drop
    }

    pub fn event_history_size(&self) -> usize {
        self.runtime.history_order.lock().len()
    }

    pub fn is_idle_and_queue_empty(&self) -> bool {
        let queue_empty = self.runtime.queue.lock().is_empty();
        let all_completed = self.runtime.events.lock().values().all(|event| {
            let status = event.inner.lock().event_status;
            status == EventStatus::Completed
        });
        queue_empty && all_completed
    }

    pub fn runtime_payload_for_test(&self) -> HashMap<String, Arc<BaseEvent>> {
        self.runtime.events.lock().clone()
    }

    pub fn event_history_ids(&self) -> Vec<String> {
        self.runtime.history_order.lock().iter().cloned().collect()
    }

    pub fn to_json_value(&self) -> Value {
        let handlers_by_pattern = self.handlers.lock().clone();
        let mut handlers = Map::new();
        let mut handlers_by_key = Map::new();
        for (pattern, entries) in handlers_by_pattern {
            let mut ids = Vec::new();
            for handler in entries {
                ids.push(Value::String(handler.id.clone()));
                handlers.insert(handler.id.clone(), handler.to_json_value());
            }
            handlers_by_key.insert(pattern, Value::Array(ids));
        }

        let mut event_history = Map::new();
        let events = self.runtime.events.lock().clone();
        for event_id in self.runtime.history_order.lock().iter() {
            if let Some(event) = events.get(event_id) {
                event_history.insert(event_id.clone(), event.to_json_value());
            }
        }

        let mut pending_event_queue = Vec::new();
        for event in self.runtime.queue.lock().iter() {
            let event_id = event.inner.lock().event_id.clone();
            if !event_history.contains_key(&event_id) {
                event_history.insert(event_id.clone(), event.to_json_value());
            }
            pending_event_queue.push(Value::String(event_id));
        }

        json!({
            "id": self.id,
            "name": self.name,
            "max_history_size": self.runtime.max_history_size,
            "max_history_drop": self.runtime.max_history_drop,
            "event_concurrency": self.event_concurrency,
            "event_timeout": self.event_timeout,
            "event_slow_timeout": self.event_slow_timeout,
            "event_handler_concurrency": self.event_handler_concurrency,
            "event_handler_completion": self.event_handler_completion,
            "event_handler_slow_timeout": self.event_handler_slow_timeout,
            "event_handler_detect_file_paths": self.event_handler_detect_file_paths,
            "handlers": handlers,
            "handlers_by_key": handlers_by_key,
            "event_history": event_history,
            "pending_event_queue": pending_event_queue,
        })
    }

    pub fn log_tree(&self) -> String {
        let events = self.runtime.events.lock().clone();
        let history_order = self.runtime.history_order.lock().clone();
        let mut children_by_parent: HashMap<String, Vec<Arc<BaseEvent>>> = HashMap::new();
        for event in events.values() {
            let parent_id = event.inner.lock().event_parent_id.clone();
            if let Some(parent_id) = parent_id {
                if parent_id != event.inner.lock().event_id && events.contains_key(&parent_id) {
                    children_by_parent
                        .entry(parent_id)
                        .or_default()
                        .push(event.clone());
                }
            }
        }
        for children in children_by_parent.values_mut() {
            children.sort_by(|left, right| {
                left.inner
                    .lock()
                    .event_created_at
                    .cmp(&right.inner.lock().event_created_at)
            });
        }

        let mut roots = Vec::new();
        for event_id in history_order {
            let Some(event) = events.get(&event_id).cloned() else {
                continue;
            };
            let inner = event.inner.lock();
            let is_root = inner.event_parent_id.as_ref().is_none_or(|parent_id| {
                parent_id == &inner.event_id || !events.contains_key(parent_id)
            });
            drop(inner);
            if is_root {
                roots.push(event);
            }
        }
        if roots.is_empty() {
            return "(No events in history)".to_string();
        }

        let mut lines = Vec::new();
        lines.push(format!("📊 Event History Tree for {}", self.label()));
        lines.push("=".repeat(80));
        let mut visited = std::collections::HashSet::new();
        for (index, event) in roots.iter().enumerate() {
            let is_last = index == roots.len() - 1;
            self.push_log_event_lines(
                &mut lines,
                event.clone(),
                "",
                is_last,
                &children_by_parent,
                &events,
                &mut visited,
            );
        }
        lines.push("=".repeat(80));
        lines.join("\n")
    }

    fn push_log_event_lines(
        &self,
        lines: &mut Vec<String>,
        event: Arc<BaseEvent>,
        indent: &str,
        is_last: bool,
        children_by_parent: &HashMap<String, Vec<Arc<BaseEvent>>>,
        events: &HashMap<String, Arc<BaseEvent>>,
        visited: &mut std::collections::HashSet<String>,
    ) {
        let event_data = event.inner.lock().clone();
        let connector = if is_last { "└── " } else { "├── " };
        let status_icon = match event_data.event_status {
            EventStatus::Completed => "✅",
            EventStatus::Started => "🏃",
            EventStatus::Pending => "⏳",
        };
        let mut timing = format!(
            "[{}",
            Self::format_log_timestamp(&event_data.event_created_at)
        );
        if let Some(completed_at) = &event_data.event_completed_at {
            if let Some(duration) =
                Self::duration_seconds(&event_data.event_created_at, completed_at)
            {
                timing.push_str(&format!(" ({duration:.3}s)"));
            }
        }
        timing.push(']');
        lines.push(format!(
            "{indent}{connector}{status_icon} {}#{} {timing}",
            event_data.event_type,
            Self::short_suffix(&event_data.event_id)
        ));

        if !visited.insert(event_data.event_id.clone()) {
            return;
        }

        let extension = if is_last { "    " } else { "│   " };
        let child_indent = format!("{indent}{extension}");
        let mut result_items: Vec<EventResult> =
            event_data.event_results.values().cloned().collect();
        result_items.sort_by(|left, right| {
            left.handler
                .handler_registered_at
                .cmp(&right.handler.handler_registered_at)
                .then_with(|| left.handler.id.cmp(&right.handler.id))
        });

        let mut printed_child_ids = std::collections::HashSet::new();
        let children = children_by_parent
            .get(&event_data.event_id)
            .cloned()
            .unwrap_or_default();
        let extra_children: Vec<Arc<BaseEvent>> = children
            .iter()
            .filter(|child| child.inner.lock().event_emitted_by_handler_id.is_none())
            .cloned()
            .collect();

        let total = result_items.len() + extra_children.len();
        let mut item_index = 0;
        for result in result_items {
            item_index += 1;
            let is_last_item = item_index == total;
            self.push_log_result_lines(
                lines,
                &result,
                &child_indent,
                is_last_item,
                events,
                visited,
                &mut printed_child_ids,
            );
        }
        for child in extra_children {
            item_index += 1;
            if !printed_child_ids.insert(child.inner.lock().event_id.clone()) {
                continue;
            }
            self.push_log_event_lines(
                lines,
                child,
                &child_indent,
                item_index == total,
                children_by_parent,
                events,
                visited,
            );
        }
    }

    fn push_log_result_lines(
        &self,
        lines: &mut Vec<String>,
        result: &EventResult,
        indent: &str,
        is_last: bool,
        events: &HashMap<String, Arc<BaseEvent>>,
        visited: &mut std::collections::HashSet<String>,
        printed_child_ids: &mut std::collections::HashSet<String>,
    ) {
        let connector = if is_last { "└── " } else { "├── " };
        let status_icon = match result.status {
            EventResultStatus::Completed => "✅",
            EventResultStatus::Error
                if result.error.as_deref().is_some_and(|error| {
                    error.starts_with("EventHandlerCancelledError:")
                        || error.starts_with("EventHandlerAbortedError:")
                }) =>
            {
                "🚫"
            }
            EventResultStatus::Error => "❌",
            EventResultStatus::Started => "🏃",
            EventResultStatus::Pending => "⏳",
        };
        let handler_display = format!(
            "{}#{}.{}#{}",
            result.handler.eventbus_name,
            Self::short_suffix(&result.handler.eventbus_id),
            result.handler.handler_name,
            Self::short_suffix(&result.handler.id)
        );
        let mut line = format!("{indent}{connector}{status_icon} {handler_display}");
        if let Some(started_at) = &result.started_at {
            line.push_str(&format!(" [{}", Self::format_log_timestamp(started_at)));
            if let Some(completed_at) = &result.completed_at {
                if let Some(duration) = Self::duration_seconds(started_at, completed_at) {
                    line.push_str(&format!(" ({duration:.3}s)"));
                }
            }
            line.push(']');
        }
        if result.status == EventResultStatus::Error {
            if let Some(error) = &result.error {
                if let Some(message) = error.strip_prefix("EventHandlerTimeoutError: ") {
                    line.push_str(&format!(" ⏱️ Timeout: {message}"));
                } else if let Some(message) = error.strip_prefix("EventHandlerCancelledError: ") {
                    line.push_str(&format!(" Cancelled: {message}"));
                } else if let Some(message) = error.strip_prefix("EventHandlerAbortedError: ") {
                    line.push_str(&format!(" Aborted: {message}"));
                } else {
                    line.push_str(&format!(" ☠️ {}", Self::format_error_for_log(error)));
                }
            }
        } else if result.status == EventResultStatus::Completed {
            line.push_str(&format!(
                " → {}",
                Self::format_result_value(result.result.as_ref())
            ));
        }
        lines.push(line);

        let extension = if is_last { "    " } else { "│   " };
        let child_indent = format!("{indent}{extension}");
        let children: Vec<Arc<BaseEvent>> = result
            .event_children
            .iter()
            .filter_map(|child_id| events.get(child_id).cloned())
            .filter(|child| !visited.contains(&child.inner.lock().event_id))
            .collect();
        for (index, child) in children.iter().enumerate() {
            printed_child_ids.insert(child.inner.lock().event_id.clone());
            self.push_log_event_lines(
                lines,
                child.clone(),
                &child_indent,
                index == children.len() - 1,
                &HashMap::new(),
                events,
                visited,
            );
        }
    }

    fn short_suffix(value: &str) -> String {
        value[value.len().saturating_sub(4)..].to_string()
    }

    fn format_log_timestamp(value: &str) -> String {
        chrono::DateTime::parse_from_rfc3339(value)
            .map(|timestamp| {
                timestamp
                    .with_timezone(&chrono::Utc)
                    .format("%H:%M:%S%.3f")
                    .to_string()
            })
            .unwrap_or_else(|_| "N/A".to_string())
    }

    fn duration_seconds(started_at: &str, completed_at: &str) -> Option<f64> {
        let started = chrono::DateTime::parse_from_rfc3339(started_at).ok()?;
        let completed = chrono::DateTime::parse_from_rfc3339(completed_at).ok()?;
        completed
            .signed_duration_since(started)
            .num_nanoseconds()
            .map(|nanos| nanos as f64 / 1_000_000_000.0)
    }

    fn format_result_value(value: Option<&Value>) -> String {
        match value {
            None | Some(Value::Null) => "None".to_string(),
            Some(Value::String(value)) => {
                serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_string())
            }
            Some(Value::Number(value)) => value.to_string(),
            Some(Value::Bool(value)) => value.to_string(),
            Some(Value::Array(value)) => format!("list({} items)", value.len()),
            Some(Value::Object(value)) => format!("dict({} items)", value.len()),
        }
    }

    fn format_error_for_log(error: &str) -> String {
        if error.contains(": ") {
            error.to_string()
        } else {
            format!("Error: {error}")
        }
    }

    pub fn from_json_value(value: Value) -> Arc<Self> {
        let Value::Object(payload) = value else {
            panic!("EventBus.from_json_value(data) requires an object");
        };

        let options = EventBusOptions {
            id: payload
                .get("id")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            max_history_size: match payload.get("max_history_size") {
                Some(Value::Null) => None,
                Some(value) => value.as_u64().map(|value| value as usize),
                None => Some(100),
            },
            max_history_drop: payload
                .get("max_history_drop")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            event_concurrency: payload
                .get("event_concurrency")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok())
                .unwrap_or(EventConcurrencyMode::BusSerial),
            event_timeout: match payload.get("event_timeout") {
                Some(Value::Null) => None,
                Some(value) => value.as_f64(),
                None => Some(60.0),
            },
            event_slow_timeout: match payload.get("event_slow_timeout") {
                Some(Value::Null) => None,
                Some(value) => value.as_f64(),
                None => Some(300.0),
            },
            event_handler_concurrency: payload
                .get("event_handler_concurrency")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok())
                .unwrap_or(EventHandlerConcurrencyMode::Serial),
            event_handler_completion: payload
                .get("event_handler_completion")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok())
                .unwrap_or(EventHandlerCompletionMode::All),
            event_handler_slow_timeout: match payload.get("event_handler_slow_timeout") {
                Some(Value::Null) => None,
                Some(value) => value.as_f64(),
                None => Some(30.0),
            },
            event_handler_detect_file_paths: payload
                .get("event_handler_detect_file_paths")
                .and_then(Value::as_bool)
                .unwrap_or(true),
        };
        let name = payload
            .get("name")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        let bus = Self::new_with_options_and_loop(name, options, false);

        let mut handlers_by_id = HashMap::new();
        if let Some(Value::Object(raw_handlers)) = payload.get("handlers") {
            for (handler_id, handler_value) in raw_handlers {
                let mut handler = EventHandler::from_json_value(handler_value.clone());
                if handler.id.is_empty() {
                    handler.id = handler_id.clone();
                }
                handlers_by_id.insert(handler.id.clone(), handler);
            }
        }

        {
            let mut handlers = bus.handlers.lock();
            handlers.clear();
            if let Some(Value::Object(raw_handlers_by_key)) = payload.get("handlers_by_key") {
                for (pattern, raw_ids) in raw_handlers_by_key {
                    let Some(ids) = raw_ids.as_array() else {
                        continue;
                    };
                    for handler_id in ids.iter().filter_map(Value::as_str) {
                        if let Some(handler) = handlers_by_id.get(handler_id).cloned() {
                            handlers.entry(pattern.clone()).or_default().push(handler);
                        }
                    }
                }
            } else {
                for handler in handlers_by_id.values().cloned() {
                    handlers
                        .entry(handler.event_pattern.clone())
                        .or_default()
                        .push(handler);
                }
            }
        }

        bus.runtime.events.lock().clear();
        bus.runtime.history_order.lock().clear();
        if let Some(Value::Object(raw_event_history)) = payload.get("event_history") {
            for (event_id_hint, event_value) in raw_event_history {
                let event = BaseEvent::from_json_value(event_value.clone());
                let event_id = event.inner.lock().event_id.clone();
                if event_id.is_empty() {
                    event.inner.lock().event_id = event_id_hint.clone();
                }
                for result in event.inner.lock().event_results.values() {
                    let handler = result.handler.clone();
                    let mut handlers = bus.handlers.lock();
                    let exists = handlers
                        .get(&handler.event_pattern)
                        .map(|entries| entries.iter().any(|entry| entry.id == handler.id))
                        .unwrap_or(false);
                    if !exists {
                        handlers
                            .entry(handler.event_pattern.clone())
                            .or_default()
                            .push(handler);
                    }
                }
                let event_id = event.inner.lock().event_id.clone();
                bus.runtime.events.lock().insert(event_id.clone(), event);
                bus.runtime.history_order.lock().push_back(event_id);
            }
        }

        bus.runtime.queue.lock().clear();
        if let Some(Value::Array(raw_pending_ids)) = payload.get("pending_event_queue") {
            for event_id in raw_pending_ids.iter().filter_map(Value::as_str) {
                if let Some(event) = bus.runtime.events.lock().get(event_id).cloned() {
                    bus.runtime.queue.lock().push_back(event);
                }
            }
        }

        bus
    }

    #[track_caller]
    pub fn on<F, Fut>(&self, pattern: &str, handler_name: &str, handler_fn: F) -> EventHandler
    where
        F: Fn(Arc<BaseEvent>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Value, String>> + Send + 'static,
    {
        self.on_with_options(
            pattern,
            handler_name,
            EventHandlerOptions::default(),
            handler_fn,
        )
    }

    #[track_caller]
    pub fn on_with_options<F, Fut>(
        &self,
        pattern: &str,
        handler_name: &str,
        mut options: EventHandlerOptions,
        handler_fn: F,
    ) -> EventHandler
    where
        F: Fn(Arc<BaseEvent>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Value, String>> + Send + 'static,
    {
        if let Some(timeout) = options.handler_timeout {
            assert!(timeout > 0.0, "handler_timeout must be > 0 or None");
        }
        if let Some(timeout) = options.handler_slow_timeout {
            assert!(timeout > 0.0, "handler_slow_timeout must be > 0 or None");
        }
        let detect_handler_file_path = options
            .detect_handler_file_path
            .unwrap_or(self.event_handler_detect_file_paths);
        options.detect_handler_file_path = Some(detect_handler_file_path);
        if detect_handler_file_path && options.handler_file_path.is_none() {
            let caller = std::panic::Location::caller();
            options.handler_file_path = Some(format!("{}:{}", caller.file(), caller.line()));
        }
        let callable: EventHandlerCallable = Arc::new(move |event| Box::pin(handler_fn(event)));
        let entry = EventHandler::from_callable_with_options(
            pattern.to_string(),
            handler_name.to_string(),
            self.name.clone(),
            self.id.clone(),
            callable,
            options,
        );
        let mut handlers = self.handlers.lock();
        let entries = handlers.entry(pattern.to_string()).or_default();
        if let Some(existing) = entries.iter_mut().find(|handler| handler.id == entry.id) {
            *existing = entry.clone();
        } else {
            entries.push(entry.clone());
        }
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
            if list.is_empty() {
                handlers.remove(pattern);
            }
        }
    }

    pub fn emit_base(&self, event: Arc<BaseEvent>) -> Arc<BaseEvent> {
        self.enqueue_base(event)
    }

    pub(crate) fn enqueue_base(&self, event: Arc<BaseEvent>) -> Arc<BaseEvent> {
        self.enqueue_base_with_options(event, false)
    }

    pub(crate) fn enqueue_base_with_options(
        &self,
        event: Arc<BaseEvent>,
        queue_jump: bool,
    ) -> Arc<BaseEvent> {
        self.enqueue_base_with_relation(event, queue_jump, false)
    }

    pub(crate) fn enqueue_child_base(&self, event: Arc<BaseEvent>) -> Arc<BaseEvent> {
        self.enqueue_child_base_with_options(event, false)
    }

    pub(crate) fn enqueue_child_base_with_options(
        &self,
        event: Arc<BaseEvent>,
        queue_jump: bool,
    ) -> Arc<BaseEvent> {
        self.enqueue_base_with_relation(event, queue_jump, true)
    }

    fn enqueue_base_with_relation(
        &self,
        event: Arc<BaseEvent>,
        queue_jump: bool,
        track_child: bool,
    ) -> Arc<BaseEvent> {
        let bus_label = self.label();
        if event.inner.lock().event_path.contains(&bus_label) {
            return event;
        }

        if !self.register_in_history(event.clone()) {
            panic!(
                "EventBus history limit reached: max_history_size={} and max_history_drop=false",
                self.runtime.max_history_size.unwrap_or(0)
            );
        }

        {
            let mut inner = event.inner.lock();
            inner.event_pending_bus_count += 1;
            inner.event_path.push(bus_label);
            if track_child && inner.event_parent_id.is_none() {
                CURRENT_EVENT_ID.with(|id| {
                    let current_parent = id.borrow().clone();
                    if current_parent.as_deref() != Some(inner.event_id.as_str()) {
                        inner.event_parent_id = current_parent;
                    }
                });
            }
            if track_child && inner.event_emitted_by_handler_id.is_none() {
                CURRENT_HANDLER_ID.with(|id| {
                    inner.event_emitted_by_handler_id = id.borrow().clone();
                });
            }
        }

        if track_child {
            let emitted_child_id = event.inner.lock().event_id.clone();
            CURRENT_EVENT_ID.with(|current_event_id| {
                CURRENT_HANDLER_ID.with(|current_handler_id| {
                    let Some(parent_id) = current_event_id.borrow().clone() else {
                        return;
                    };
                    let Some(handler_id) = current_handler_id.borrow().clone() else {
                        return;
                    };
                    let Some(parent_event) = self.runtime.events.lock().get(&parent_id).cloned()
                    else {
                        return;
                    };
                    let mut parent_inner = parent_event.inner.lock();
                    if let Some(result) = parent_inner.event_results.get_mut(&handler_id) {
                        if !result.event_children.contains(&emitted_child_id) {
                            result.event_children.push(emitted_child_id.clone());
                        }
                    }
                });
            });
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
        self.ensure_loop_started();
        self.runtime.queue_notify.notify(1);
        event
    }

    pub fn queue_jump_if_waited(event: Arc<BaseEvent>) {
        let Some((current_bus_id, _, _)) =
            Self::mark_blocks_parent_completion_if_awaited(event.clone())
        else {
            return;
        };

        let event_path = event.inner.lock().event_path.clone();
        if event_path.is_empty() {
            return;
        }

        let mut instances = ALL_INSTANCES.get_or_init(|| Mutex::new(Vec::new())).lock();
        instances.retain(|entry| entry.upgrade().is_some());
        let live_buses: Vec<Arc<EventBus>> = instances.iter().filter_map(Weak::upgrade).collect();
        drop(instances);

        let mut ordered = Vec::new();
        for label in event_path {
            for bus in &live_buses {
                if bus.label() == label
                    && !ordered
                        .iter()
                        .any(|entry: &Arc<EventBus>| Arc::ptr_eq(entry, bus))
                {
                    ordered.push(bus.clone());
                }
            }
        }

        let initiating_mode = ordered
            .iter()
            .find(|bus| bus.id == current_bus_id)
            .map(|bus| {
                event
                    .inner
                    .lock()
                    .event_concurrency
                    .unwrap_or(bus.event_concurrency)
            });

        for bus in ordered {
            let bus_mode = event
                .inner
                .lock()
                .event_concurrency
                .unwrap_or(bus.event_concurrency);
            let bypass_event_lock = bus.id == current_bus_id
                || (initiating_mode == Some(EventConcurrencyMode::GlobalSerial)
                    && bus_mode == EventConcurrencyMode::GlobalSerial);
            bus.process_event_immediately_if_queued(event.clone(), bypass_event_lock);
        }
    }

    pub fn mark_blocks_parent_completion_if_awaited(
        event: Arc<BaseEvent>,
    ) -> Option<(String, String, String)> {
        let context = CURRENT_BUS_ID.with(|bus_id| {
            CURRENT_EVENT_ID.with(|event_id| {
                CURRENT_HANDLER_ID.with(|handler_id| {
                    Some((
                        bus_id.borrow().clone()?,
                        event_id.borrow().clone()?,
                        handler_id.borrow().clone()?,
                    ))
                })
            })
        });
        let Some((current_bus_id, current_event_id, current_handler_id)) = context else {
            return None;
        };

        {
            let mut inner = event.inner.lock();
            if inner.event_parent_id.as_deref() == Some(current_event_id.as_str())
                && inner.event_emitted_by_handler_id.as_deref() == Some(current_handler_id.as_str())
                && inner.event_id != current_event_id
            {
                let is_linked_child = ALL_INSTANCES
                    .get_or_init(|| Mutex::new(Vec::new()))
                    .lock()
                    .iter()
                    .filter_map(Weak::upgrade)
                    .filter_map(|bus| bus.runtime.events.lock().get(&current_event_id).cloned())
                    .any(|parent| {
                        parent
                            .inner
                            .lock()
                            .event_results
                            .get(&current_handler_id)
                            .is_some_and(|result| result.event_children.contains(&inner.event_id))
                    });
                if is_linked_child {
                    inner.event_blocks_parent_completion = true;
                }
            }
        }

        Some((current_bus_id, current_event_id, current_handler_id))
    }

    fn process_event_immediately_if_queued(&self, event: Arc<BaseEvent>, bypass_event_lock: bool) {
        let event_id = event.inner.lock().event_id.clone();
        if event.inner.lock().event_status == EventStatus::Completed {
            return;
        }
        if event
            .inner
            .lock()
            .event_results
            .values()
            .any(|result| result.handler.eventbus_id == self.id)
        {
            return;
        }

        let removed = {
            let mut queue = self.runtime.queue.lock();
            if let Some(index) = queue
                .iter()
                .position(|queued| queued.inner.lock().event_id == event_id)
            {
                queue.remove(index);
                true
            } else {
                false
            }
        };
        if !removed {
            return;
        }

        let bus = self.clone();
        thread::spawn(move || {
            if bypass_event_lock {
                block_on(bus.process_event_inner(event));
            } else {
                block_on(bus.process_event(event));
            }
        });
    }

    fn register_in_history(&self, event: Arc<BaseEvent>) -> bool {
        let event_id = event.inner.lock().event_id.clone();

        if let Some(max_size) = self.runtime.max_history_size {
            if max_size > 0 {
                let current_size = self.runtime.history_order.lock().len();
                if current_size >= max_size && !self.runtime.max_history_drop {
                    return false;
                }
                self.trim_history_to_capacity(max_size, true);
            }
        }

        self.runtime.events.lock().insert(event_id.clone(), event);
        self.runtime.history_order.lock().push_back(event_id);
        true
    }

    fn trim_history_to_capacity(&self, max_size: usize, include_equal: bool) {
        if max_size == 0 {
            return;
        }
        loop {
            let current_len = self.runtime.history_order.lock().len();
            if current_len < max_size || (!include_equal && current_len == max_size) {
                break;
            }
            let Some(oldest) = self.runtime.history_order.lock().front().cloned() else {
                break;
            };
            let is_active = self
                .runtime
                .events
                .lock()
                .get(&oldest)
                .map(|event| {
                    let status = event.inner.lock().event_status;
                    status == EventStatus::Pending || status == EventStatus::Started
                })
                .unwrap_or(false);
            if is_active {
                break;
            }
            self.runtime.history_order.lock().pop_front();
            self.runtime.events.lock().remove(&oldest);
        }
    }

    pub async fn find(
        &self,
        pattern: &str,
        past: bool,
        future: Option<f64>,
        child_of: Option<Arc<BaseEvent>>,
    ) -> Option<Arc<BaseEvent>> {
        self.find_with_options(
            pattern,
            FindOptions {
                past,
                past_window: None,
                future,
                child_of,
                where_filter: None,
            },
        )
        .await
    }

    pub async fn find_with_options(
        &self,
        pattern: &str,
        options: FindOptions,
    ) -> Option<Arc<BaseEvent>> {
        let child_of_event_id = options
            .child_of
            .as_ref()
            .map(|event| event.inner.lock().event_id.clone());

        let mut waiter_id: Option<u64> = None;
        let mut waiter_rx: Option<std_mpsc::Receiver<Arc<BaseEvent>>> = None;

        if options.future.is_some() {
            let (tx, rx) = std_mpsc::channel();
            let id = {
                let mut next = self.runtime.next_waiter_id.lock();
                *next += 1;
                *next
            };
            self.runtime.find_waiters.lock().push(FindWaiter {
                id,
                pattern: pattern.to_string(),
                child_of_event_id: child_of_event_id.clone(),
                where_filter: options.where_filter.clone(),
                sender: tx,
            });
            waiter_id = Some(id);
            waiter_rx = Some(rx);

            if options.past {
                if let Some(matched) = self.find_in_history(
                    pattern,
                    child_of_event_id.as_deref(),
                    options.where_filter.as_ref(),
                    options.past_window,
                ) {
                    self.runtime
                        .find_waiters
                        .lock()
                        .retain(|waiter| waiter.id != id);
                    return Some(matched);
                }
            }
        } else if options.past {
            return self.find_in_history(
                pattern,
                child_of_event_id.as_deref(),
                options.where_filter.as_ref(),
                options.past_window,
            );
        }

        let timeout = options.future?;
        let result =
            waiter_rx.and_then(|rx| rx.recv_timeout(Duration::from_secs_f64(timeout)).ok());

        if let Some(id) = waiter_id {
            self.runtime
                .find_waiters
                .lock()
                .retain(|waiter| waiter.id != id);
        }

        result
    }

    fn find_in_history(
        &self,
        pattern: &str,
        child_of_event_id: Option<&str>,
        where_filter: Option<&HashMap<String, Value>>,
        past_window: Option<f64>,
    ) -> Option<Arc<BaseEvent>> {
        let history = self.runtime.history_order.lock().clone();
        for event_id in history.iter().rev() {
            let Some(event) = self.runtime.events.lock().get(event_id).cloned() else {
                continue;
            };
            if !Self::is_within_past_window(&event, past_window) {
                continue;
            }
            if !self.matches_pattern(&event, pattern) {
                continue;
            }
            if let Some(parent_id) = child_of_event_id {
                let event_id = event.inner.lock().event_id.clone();
                if !self.event_is_child_of_ids(&event_id, parent_id) {
                    continue;
                }
            }
            if !Self::matches_where_filter(&event, where_filter) {
                continue;
            }
            return Some(event);
        }
        None
    }

    fn is_within_past_window(event: &Arc<BaseEvent>, past_window: Option<f64>) -> bool {
        let Some(past_window) = past_window else {
            return true;
        };
        let created_at = event.inner.lock().event_created_at.clone();
        let Ok(created_at) = chrono::DateTime::parse_from_rfc3339(&created_at) else {
            return false;
        };
        let age = chrono::Utc::now().signed_duration_since(created_at.with_timezone(&chrono::Utc));
        age.num_nanoseconds()
            .map(|nanos| (nanos as f64) / 1_000_000_000.0 <= past_window)
            .unwrap_or(false)
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
                if !Self::matches_where_filter(&event, waiter.where_filter.as_ref()) {
                    continue;
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

    fn matches_where_filter(
        event: &Arc<BaseEvent>,
        where_filter: Option<&HashMap<String, Value>>,
    ) -> bool {
        let Some(where_filter) = where_filter else {
            return true;
        };
        if where_filter.is_empty() {
            return true;
        }

        let Value::Object(record) = event.to_json_value() else {
            return false;
        };
        where_filter
            .iter()
            .all(|(key, expected)| record.get(key) == Some(expected))
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
            if self.is_idle_and_queue_empty() {
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
        self.create_pending_handler_results(&event, &handlers, event_timeout);

        match handler_concurrency {
            EventHandlerConcurrencyMode::Serial => {
                for (index, handler) in handlers.iter().cloned().enumerate() {
                    let timed_out = self
                        .run_handler_with_context(event.clone(), handler, started_at, event_timeout)
                        .await;
                    if timed_out {
                        break;
                    }
                    if handler_completion == EventHandlerCompletionMode::First {
                        let winner_id = self.winning_handler_id(&event);
                        if winner_id.is_some() {
                            self.cancel_remaining_first_mode_results(
                                &event,
                                &handlers[index + 1..],
                            );
                            break;
                        }
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
                }
                if handler_completion == EventHandlerCompletionMode::First {
                    if let Some(winner_id) = self.winning_handler_id(&event) {
                        self.cancel_parallel_first_mode_losers(&event, &winner_id);
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

        let should_complete = {
            let mut inner = event.inner.lock();
            inner.event_pending_bus_count = inner.event_pending_bus_count.saturating_sub(1);
            let done = inner.event_pending_bus_count == 0;
            if done && inner.event_status != EventStatus::Completed {
                inner.event_completed_at = Some(now_iso());
            }
            done
        };
        if self.runtime.max_history_size == Some(0) {
            let event_id = event.inner.lock().event_id.clone();
            self.runtime.events.lock().remove(&event_id);
            self.runtime
                .history_order
                .lock()
                .retain(|id| id != &event_id);
        } else if let Some(max_size) = self.runtime.max_history_size {
            self.trim_history_to_capacity(max_size, false);
        }

        if should_complete {
            event.mark_completed();
        }
    }

    fn cancel_children(&self, event: &Arc<BaseEvent>, reason: &str) {
        let results = event.inner.lock().event_results.clone();
        for result in results.values() {
            for child_id in &result.event_children {
                if let Some(child) = self.runtime.events.lock().get(child_id).cloned() {
                    if !child.inner.lock().event_blocks_parent_completion {
                        continue;
                    }
                    self.cancel_children(&child, reason);
                    for child_result in child.inner.lock().event_results.values_mut() {
                        if child_result.status == EventResultStatus::Pending {
                            child_result.status = EventResultStatus::Error;
                            child_result.error = Some(format!(
                                "EventHandlerCancelledError: Cancelled pending handler due to parent error: {reason}"
                            ));
                            child_result.completed_at = Some(now_iso());
                        } else if child_result.status == EventResultStatus::Started {
                            child_result.status = EventResultStatus::Error;
                            child_result.error = Some(format!(
                                "EventHandlerAbortedError: Aborted running handler due to parent error: {reason}"
                            ));
                            child_result.completed_at = Some(now_iso());
                        }
                    }
                    child.mark_completed();
                }
            }
        }
    }

    fn create_pending_handler_results(
        &self,
        event: &Arc<BaseEvent>,
        handlers: &[EventHandler],
        event_timeout: Option<f64>,
    ) {
        let event_id = event.inner.lock().event_id.clone();
        let pending_results: Vec<(String, EventResult)> = handlers
            .iter()
            .map(|handler| {
                (
                    handler.id.clone(),
                    EventResult::new(
                        event_id.clone(),
                        handler.clone(),
                        self.result_timeout_for_handler(event, handler, event_timeout),
                    ),
                )
            })
            .collect();
        let mut inner = event.inner.lock();
        for (handler_id, result) in pending_results {
            inner.event_results.entry(handler_id).or_insert(result);
        }
    }

    fn result_timeout_for_handler(
        &self,
        event: &Arc<BaseEvent>,
        handler: &EventHandler,
        event_timeout: Option<f64>,
    ) -> Option<f64> {
        let resolved_handler_timeout = handler
            .handler_timeout
            .or(event.inner.lock().event_handler_timeout)
            .or(self.event_timeout);

        match (resolved_handler_timeout, event_timeout) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    fn winning_handler_id(&self, event: &Arc<BaseEvent>) -> Option<String> {
        let mut candidates: Vec<(String, EventResult)> = event
            .inner
            .lock()
            .event_results
            .iter()
            .filter_map(|(handler_id, result)| {
                if result.status == EventResultStatus::Completed
                    && result.error.is_none()
                    && !matches!(result.result, None | Some(Value::Null))
                {
                    Some((handler_id.clone(), result.clone()))
                } else {
                    None
                }
            })
            .collect();
        candidates.sort_by(|left, right| {
            left.1
                .completed_at
                .cmp(&right.1.completed_at)
                .then_with(|| left.1.started_at.cmp(&right.1.started_at))
                .then_with(|| {
                    left.1
                        .handler
                        .handler_registered_at
                        .cmp(&right.1.handler.handler_registered_at)
                })
                .then_with(|| left.0.cmp(&right.0))
        });
        candidates
            .into_iter()
            .next()
            .map(|(handler_id, _)| handler_id)
    }

    fn cancel_remaining_first_mode_results(
        &self,
        event: &Arc<BaseEvent>,
        remaining_handlers: &[EventHandler],
    ) {
        let mut inner = event.inner.lock();
        for handler in remaining_handlers {
            if let Some(result) = inner.event_results.get_mut(&handler.id) {
                if result.status != EventResultStatus::Pending {
                    continue;
                }
                result.status = EventResultStatus::Error;
                result.result = None;
                result.error =
                    Some("EventHandlerCancelledError: Cancelled: first() resolved".to_string());
                if result.started_at.is_none() {
                    result.started_at = Some(now_iso());
                }
                result.completed_at = Some(now_iso());
            }
        }
    }

    fn cancel_parallel_first_mode_losers(&self, event: &Arc<BaseEvent>, winner_id: &str) {
        let mut inner = event.inner.lock();
        let winner_completed_at = inner
            .event_results
            .get(winner_id)
            .and_then(|result| result.completed_at.clone());
        for (handler_id, result) in inner.event_results.iter_mut() {
            if handler_id == winner_id {
                continue;
            }
            if result.status == EventResultStatus::Pending {
                result.error =
                    Some("EventHandlerCancelledError: Cancelled: first() resolved".to_string());
            } else if result.status == EventResultStatus::Started {
                result.error =
                    Some("EventHandlerAbortedError: Aborted: first() resolved".to_string());
            } else if result.status == EventResultStatus::Completed
                && winner_completed_at
                    .as_ref()
                    .zip(result.completed_at.as_ref())
                    .is_some_and(|(winner_at, result_at)| result_at >= winner_at)
            {
                result.error =
                    Some("EventHandlerAbortedError: Aborted: first() resolved".to_string());
            } else {
                continue;
            }
            result.status = EventResultStatus::Error;
            result.result = None;
            if result.started_at.is_none() {
                result.started_at = Some(now_iso());
            }
            result.completed_at = Some(now_iso());
        }
    }

    async fn run_handler_with_context(
        &self,
        event: Arc<BaseEvent>,
        handler: EventHandler,
        event_started_at: Instant,
        event_timeout: Option<f64>,
    ) -> bool {
        let handler_id = handler.id.clone();
        CURRENT_BUS_ID.with(|id| *id.borrow_mut() = Some(self.id.clone()));
        CURRENT_EVENT_ID.with(|id| *id.borrow_mut() = Some(event.inner.lock().event_id.clone()));
        CURRENT_HANDLER_ID.with(|id| *id.borrow_mut() = Some(handler_id.clone()));
        let timed_out = self
            .run_handler(event, handler, event_started_at, event_timeout)
            .await;
        CURRENT_HANDLER_ID.with(|id| *id.borrow_mut() = None);
        CURRENT_EVENT_ID.with(|id| *id.borrow_mut() = None);
        CURRENT_BUS_ID.with(|id| *id.borrow_mut() = None);
        timed_out
    }

    async fn run_handler(
        &self,
        event: Arc<BaseEvent>,
        handler: EventHandler,
        event_started_at: Instant,
        event_timeout: Option<f64>,
    ) -> bool {
        let explicit_handler_timeout = handler
            .handler_timeout
            .or(event.inner.lock().event_handler_timeout);
        let resolved_handler_timeout = explicit_handler_timeout.or(self.event_timeout);

        let result_timeout = self.result_timeout_for_handler(&event, &handler, event_timeout);

        let remaining_event_timeout = event_timeout.map(|timeout| {
            let elapsed = event_started_at.elapsed().as_secs_f64();
            (timeout - elapsed).max(0.0)
        });

        let call_timeout = match (resolved_handler_timeout, remaining_event_timeout) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };

        let existing_result = { event.inner.lock().event_results.get(&handler.id).cloned() };
        if existing_result
            .as_ref()
            .is_some_and(|result| result.status != EventResultStatus::Pending)
        {
            return false;
        }
        let mut result = existing_result.unwrap_or_else(|| {
            EventResult::new(
                event.inner.lock().event_id.clone(),
                handler.clone(),
                result_timeout,
            )
        });
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
        let context_bus_id = self.id.clone();
        let context_event_id = event.inner.lock().event_id.clone();
        let context_handler_id = handler.id.clone();
        thread::spawn(move || {
            CURRENT_BUS_ID.with(|id| *id.borrow_mut() = Some(context_bus_id));
            CURRENT_EVENT_ID.with(|id| *id.borrow_mut() = Some(context_event_id));
            CURRENT_HANDLER_ID.with(|id| *id.borrow_mut() = Some(context_handler_id));
            let response = block_on(call(event_clone));
            CURRENT_HANDLER_ID.with(|id| *id.borrow_mut() = None);
            CURRENT_EVENT_ID.with(|id| *id.borrow_mut() = None);
            CURRENT_BUS_ID.with(|id| *id.borrow_mut() = None);
            let _ = tx.send(response);
        });

        let call_started = Instant::now();
        let call_result = if let Some(timeout_secs) = call_timeout {
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
        if current.status != EventResultStatus::Started {
            return false;
        }

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
                let handler_timeout_won = explicit_handler_timeout.is_some_and(|handler_timeout| {
                    remaining_event_timeout
                        .map(|event_remaining| handler_timeout <= event_remaining)
                        .unwrap_or(true)
                });
                let error_type = if error == "timeout" && handler_timeout_won {
                    "EventHandlerTimeoutError"
                } else {
                    "EventHandlerAbortedError"
                };
                current.error = Some(format!("{error_type}: {error}"));
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
