use std::collections::{HashMap, HashSet, VecDeque};

use indexmap::IndexMap;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use abxbus::monotonic_dt;
use abxbus::uuid_gen;
use abxbus::validation;

// ── Standalone Functions ──────────────────────────────────────────

#[pyfunction]
#[pyo3(signature = (isostring=None))]
fn monotonic_datetime(isostring: Option<&str>) -> PyResult<String> {
    monotonic_dt::monotonic_datetime(isostring)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}

#[pyfunction]
fn format_epoch_ns_to_iso(epoch_ns: i64) -> PyResult<String> {
    monotonic_dt::format_epoch_ns_to_iso(epoch_ns)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}

#[pyfunction]
fn uuid7str() -> String {
    uuid_gen::uuid7str()
}

#[pyfunction]
fn uuid5_handler_id(seed: &str) -> String {
    uuid_gen::uuid5_handler_id(seed)
}

#[pyfunction]
fn compute_handler_id(
    eventbus_id: &str,
    handler_name: &str,
    file_path: &str,
    registered_at: &str,
    event_pattern: &str,
) -> String {
    uuid_gen::compute_handler_id(eventbus_id, handler_name, file_path, registered_at, event_pattern)
}

#[pyfunction]
fn validate_uuid_str(s: &str) -> PyResult<String> {
    uuid_gen::validate_uuid(s)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}

#[pyfunction]
fn validate_event_name(s: &str) -> PyResult<String> {
    validation::validate_event_name(s)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}

#[pyfunction]
fn validate_event_path_entry(s: &str) -> PyResult<String> {
    validation::validate_event_path_entry(s)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}

#[pyfunction]
fn is_valid_identifier(s: &str) -> bool {
    validation::is_valid_identifier(s)
}

#[pyfunction]
fn validate_bus_name(s: &str) -> PyResult<String> {
    validation::validate_bus_name(s)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}

#[pyfunction]
fn check_reserved_event_fields(keys: Vec<String>, known_fields: Vec<String>) -> PyResult<()> {
    let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
    let field_refs: Vec<&str> = known_fields.iter().map(|s| s.as_str()).collect();
    validation::check_reserved_event_fields(&key_refs, &field_refs)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}

// ── Event metadata stored in Rust ─────────────────────────────────

/// Minimal event metadata stored in Rust for scheduling decisions.
/// The full Pydantic BaseEvent object lives in Python for type safety.
#[derive(Clone, Debug)]
struct EventMeta {
    event_id: String,
    event_type: String,
    event_path: Vec<String>,
    event_status: String, // "pending" | "started" | "completed"
    event_parent_id: Option<String>,
    event_created_at: String,
    event_concurrency: Option<String>,
    event_handler_concurrency: Option<String>,
    event_handler_completion: Option<String>,
    event_timeout: Option<f64>,
    event_handler_timeout: Option<f64>,
    event_slow_timeout: Option<f64>,
    event_handler_slow_timeout: Option<f64>,
    // handler_id -> result status ("pending"|"started"|"completed"|"error")
    result_statuses: HashMap<String, String>,
    // handler_id -> [child_event_id]
    result_children: HashMap<String, Vec<String>>,
    emitted_by_handler_id: Option<String>,
}

/// Handler metadata stored in Rust for pattern matching.
#[derive(Clone, Debug)]
struct HandlerMeta {
    id: String,
    handler_name: String,
    event_pattern: String,
    eventbus_name: String,
    eventbus_id: String,
}

// ── RustEventBusCore ──────────────────────────────────────────────

/// The Rust scheduling core for an EventBus instance.
///
/// Owns all state and makes all scheduling decisions:
/// - Handler registry + pattern matching
/// - Event metadata store
/// - History storage with fast find/trim
/// - Queue ordering + queue jumping
/// - Loop detection
/// - Timeout cascade resolution
/// - Completion detection
/// - Lock policy resolution
///
/// Python EventBus is a thin async executor that asks this core "what next?"
#[pyclass]
struct RustEventBusCore {
    bus_id: String,
    bus_name: String,

    // Bus-level defaults
    event_concurrency: String,
    event_handler_concurrency: String,
    event_handler_completion: String,
    event_timeout: Option<f64>,
    event_slow_timeout: Option<f64>,
    event_handler_slow_timeout: Option<f64>,
    max_handler_recursion_depth: usize,

    // Handler registry
    handlers: IndexMap<String, HandlerMeta>,
    handlers_by_key: HashMap<String, Vec<String>>,

    // Event metadata (not the full Python objects — just scheduling-relevant fields)
    events: HashMap<String, EventMeta>,

    // History: ordered by insertion time (for find_in_past iteration)
    history_order: VecDeque<String>,
    max_history_size: Option<usize>,
    max_history_drop: bool,

    // In-flight tracking
    in_flight_event_ids: HashSet<String>,
    processing_event_ids: HashSet<String>,
}

#[pymethods]
impl RustEventBusCore {
    #[new]
    #[pyo3(signature = (
        bus_id,
        bus_name,
        event_concurrency = "bus-serial".to_string(),
        event_handler_concurrency = "serial".to_string(),
        event_handler_completion = "all".to_string(),
        event_timeout = Some(60.0),
        event_slow_timeout = Some(300.0),
        event_handler_slow_timeout = Some(30.0),
        max_handler_recursion_depth = 2,
        max_history_size = Some(100),
        max_history_drop = false,
    ))]
    fn new(
        bus_id: String,
        bus_name: String,
        event_concurrency: String,
        event_handler_concurrency: String,
        event_handler_completion: String,
        event_timeout: Option<f64>,
        event_slow_timeout: Option<f64>,
        event_handler_slow_timeout: Option<f64>,
        max_handler_recursion_depth: usize,
        max_history_size: Option<usize>,
        max_history_drop: bool,
    ) -> Self {
        Self {
            bus_id,
            bus_name,
            event_concurrency,
            event_handler_concurrency,
            event_handler_completion,
            event_timeout,
            event_slow_timeout,
            event_handler_slow_timeout,
            max_handler_recursion_depth,
            handlers: IndexMap::new(),
            handlers_by_key: HashMap::new(),
            events: HashMap::new(),
            history_order: VecDeque::new(),
            max_history_size,
            max_history_drop,
            in_flight_event_ids: HashSet::new(),
            processing_event_ids: HashSet::new(),
        }
    }

    // ── Handler Registry ──────────────────────────────────────────

    /// Register a handler pattern. Returns handler_id.
    fn register_handler(
        &mut self,
        handler_id: String,
        handler_name: String,
        event_pattern: String,
        eventbus_name: String,
        eventbus_id: String,
    ) {
        let meta = HandlerMeta {
            id: handler_id.clone(),
            handler_name,
            event_pattern: event_pattern.clone(),
            eventbus_name,
            eventbus_id,
        };
        self.handlers.insert(handler_id.clone(), meta);
        self.handlers_by_key
            .entry(event_pattern)
            .or_default()
            .push(handler_id);
    }

    /// Unregister a handler by id and pattern.
    fn unregister_handler(&mut self, handler_id: &str, event_pattern: &str) {
        self.handlers.shift_remove(handler_id);
        if let Some(ids) = self.handlers_by_key.get_mut(event_pattern) {
            ids.retain(|id| id != handler_id);
            if ids.is_empty() {
                self.handlers_by_key.remove(event_pattern);
            }
        }
    }

    /// Get all handler IDs registered for a given pattern.
    fn get_handler_ids_for_pattern(&self, event_pattern: &str) -> Vec<String> {
        self.handlers_by_key
            .get(event_pattern)
            .cloned()
            .unwrap_or_default()
    }

    /// Get handler count.
    fn handler_count(&self) -> usize {
        self.handlers.len()
    }

    // ── Event Metadata ────────────────────────────────────────────

    /// Store event metadata for scheduling. Called from Python emit().
    #[pyo3(signature = (
        event_id,
        event_type,
        event_path,
        event_status,
        event_parent_id = None,
        event_created_at = None,
        event_concurrency = None,
        event_handler_concurrency = None,
        event_handler_completion = None,
        event_timeout = None,
        event_handler_timeout = None,
        event_slow_timeout = None,
        event_handler_slow_timeout = None,
        emitted_by_handler_id = None,
    ))]
    fn store_event_meta(
        &mut self,
        event_id: String,
        event_type: String,
        event_path: Vec<String>,
        event_status: String,
        event_parent_id: Option<String>,
        event_created_at: Option<String>,
        event_concurrency: Option<String>,
        event_handler_concurrency: Option<String>,
        event_handler_completion: Option<String>,
        event_timeout: Option<f64>,
        event_handler_timeout: Option<f64>,
        event_slow_timeout: Option<f64>,
        event_handler_slow_timeout: Option<f64>,
        emitted_by_handler_id: Option<String>,
    ) {
        let meta = EventMeta {
            event_id: event_id.clone(),
            event_type,
            event_path,
            event_status,
            event_parent_id,
            event_created_at: event_created_at
                .unwrap_or_else(|| monotonic_dt::monotonic_datetime(None).unwrap_or_default()),
            event_concurrency,
            event_handler_concurrency,
            event_handler_completion,
            event_timeout,
            event_handler_timeout,
            event_slow_timeout,
            event_handler_slow_timeout,
            result_statuses: HashMap::new(),
            result_children: HashMap::new(),
            emitted_by_handler_id,
        };
        // Add to history order if not already present
        if !self.events.contains_key(&event_id) {
            self.history_order.push_back(event_id.clone());
        }
        self.events.insert(event_id, meta);
    }

    /// Update event status.
    fn update_event_status(&mut self, event_id: &str, status: &str) {
        if let Some(meta) = self.events.get_mut(event_id) {
            meta.event_status = status.to_string();
        }
    }

    /// Update event path.
    fn update_event_path(&mut self, event_id: &str, path: Vec<String>) {
        if let Some(meta) = self.events.get_mut(event_id) {
            meta.event_path = path;
        }
    }

    /// Record a handler result status for an event.
    fn update_result_status(&mut self, event_id: &str, handler_id: &str, status: &str) {
        if let Some(meta) = self.events.get_mut(event_id) {
            meta.result_statuses
                .insert(handler_id.to_string(), status.to_string());
        }
    }

    /// Record a child event for a handler result.
    fn add_result_child(&mut self, event_id: &str, handler_id: &str, child_event_id: &str) {
        if let Some(meta) = self.events.get_mut(event_id) {
            meta.result_children
                .entry(handler_id.to_string())
                .or_default()
                .push(child_event_id.to_string());
        }
    }

    /// Check if event metadata exists.
    fn has_event(&self, event_id: &str) -> bool {
        self.events.contains_key(event_id)
    }

    /// Remove event metadata.
    fn remove_event(&mut self, event_id: &str) -> bool {
        let removed = self.events.remove(event_id).is_some();
        if removed {
            self.history_order.retain(|id| id != event_id);
        }
        removed
    }

    /// Get event count in history.
    fn history_len(&self) -> usize {
        self.events.len()
    }

    /// Clear all history.
    fn clear_history(&mut self) {
        self.events.clear();
        self.history_order.clear();
    }

    // ── In-flight Tracking ────────────────────────────────────────

    fn mark_inflight(&mut self, event_id: &str) {
        self.in_flight_event_ids.insert(event_id.to_string());
    }

    fn clear_inflight(&mut self, event_id: &str) {
        self.in_flight_event_ids.remove(event_id);
    }

    fn mark_processing(&mut self, event_id: &str) {
        self.processing_event_ids.insert(event_id.to_string());
    }

    fn clear_processing(&mut self, event_id: &str) {
        self.processing_event_ids.remove(event_id);
    }

    fn is_idle(&self) -> bool {
        self.in_flight_event_ids.is_empty() && self.processing_event_ids.is_empty()
    }

    fn inflight_count(&self) -> usize {
        self.in_flight_event_ids.len()
    }

    fn processing_count(&self) -> usize {
        self.processing_event_ids.len()
    }

    fn has_inflight_or_processing(&self) -> bool {
        !self.in_flight_event_ids.is_empty() || !self.processing_event_ids.is_empty()
    }

    fn clear_all_inflight(&mut self) {
        self.in_flight_event_ids.clear();
        self.processing_event_ids.clear();
    }

    // ── Handler Matching (the "brain") ────────────────────────────

    /// Get applicable handler IDs for an event, filtering loops.
    ///
    /// This is the core scheduling decision: which handlers should run for this event.
    /// Returns list of handler_ids in registration order.
    ///
    /// Args:
    ///   event_id: The event to match handlers for
    ///   existing_result_handler_ids: Handler IDs that already have results on this event
    ///   existing_result_statuses: Map of handler_id -> status for existing results
    fn get_handlers_for_event(
        &self,
        event_id: &str,
        existing_result_handler_ids: Vec<String>,
        existing_result_statuses: HashMap<String, String>,
    ) -> Vec<String> {
        let meta = match self.events.get(event_id) {
            Some(m) => m,
            None => return vec![],
        };

        let mut applicable = Vec::new();

        // Type-specific handlers first, then wildcard
        for key in &[meta.event_type.as_str(), "*"] {
            if let Some(handler_ids) = self.handlers_by_key.get(*key) {
                for handler_id in handler_ids {
                    if let Some(handler) = self.handlers.get(handler_id) {
                        if !self.would_create_loop_inner(meta, handler, &existing_result_handler_ids, &existing_result_statuses) {
                            applicable.push(handler_id.clone());
                        }
                    }
                }
            }
        }

        applicable
    }

    /// Check if executing this handler would create a loop.
    fn would_create_loop(
        &self,
        event_id: &str,
        handler_id: &str,
        existing_result_handler_ids: Vec<String>,
        existing_result_statuses: HashMap<String, String>,
    ) -> bool {
        let meta = match self.events.get(event_id) {
            Some(m) => m,
            None => return false,
        };
        let handler = match self.handlers.get(handler_id) {
            Some(h) => h,
            None => return false,
        };
        self.would_create_loop_inner(meta, handler, &existing_result_handler_ids, &existing_result_statuses)
    }

    /// Walk the parent chain to count how many times a handler appears (recursion depth).
    fn handler_recursion_depth(&self, event_id: &str, handler_id: &str) -> usize {
        let mut depth = 0usize;
        let mut visited = HashSet::new();

        let meta = match self.events.get(event_id) {
            Some(m) => m,
            None => return 0,
        };

        let mut current_parent_id = meta.event_parent_id.as_deref();

        while let Some(pid) = current_parent_id {
            if visited.contains(pid) {
                break;
            }
            visited.insert(pid.to_string());

            if let Some(parent_meta) = self.events.get(pid) {
                if let Some(status) = parent_meta.result_statuses.get(handler_id) {
                    if status == "pending" || status == "started" || status == "completed" {
                        depth += 1;
                    }
                }
                current_parent_id = parent_meta.event_parent_id.as_deref();
            } else {
                break;
            }
        }

        depth
    }

    // ── Parent-Child Relationships ────────────────────────────────

    /// Check if event is a descendant of ancestor.
    fn is_child_of(&self, event_id: &str, ancestor_id: &str) -> bool {
        let meta = match self.events.get(event_id) {
            Some(m) => m,
            None => return false,
        };

        let mut current_id = meta.event_parent_id.as_deref();
        let mut visited = HashSet::new();

        while let Some(cid) = current_id {
            if visited.contains(cid) {
                break;
            }
            if cid == ancestor_id {
                return true;
            }
            visited.insert(cid.to_string());
            current_id = self
                .events
                .get(cid)
                .and_then(|m| m.event_parent_id.as_deref());
        }

        false
    }

    // ── History Search (find_in_past) ─────────────────────────────

    /// Find the most recent event matching criteria in history.
    ///
    /// Returns event_id of the match, or None.
    ///
    /// Args:
    ///   event_key: event type string or "*" for all
    ///   past_seconds: optional lookback window in seconds
    ///   child_of_event_id: optional ancestor event_id constraint
    ///   field_filters: dict of field_name -> value for exact-match filtering
    #[pyo3(signature = (event_key, past_seconds=None, child_of_event_id=None, field_filters=None))]
    fn find_in_history(
        &self,
        event_key: &str,
        past_seconds: Option<f64>,
        child_of_event_id: Option<&str>,
        field_filters: Option<HashMap<String, String>>,
    ) -> Option<String> {
        let cutoff = past_seconds.map(|secs| {
            let cutoff_dt = chrono::Utc::now() - chrono::Duration::milliseconds((secs * 1000.0) as i64);
            monotonic_dt::monotonic_datetime(Some(
                &cutoff_dt.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string(),
            ))
            .unwrap_or_default()
        });

        // Iterate in reverse (most recent first)
        for event_id in self.history_order.iter().rev() {
            let meta = match self.events.get(event_id) {
                Some(m) => m,
                None => continue,
            };

            // Cutoff check
            if let Some(ref cutoff_str) = cutoff {
                if meta.event_created_at < *cutoff_str {
                    continue;
                }
            }

            // Pattern match
            if event_key != "*" && meta.event_type != event_key {
                continue;
            }

            // Child-of check
            if let Some(ancestor_id) = child_of_event_id {
                if !self.is_child_of(&meta.event_id, ancestor_id) {
                    continue;
                }
            }

            // Field filters (on core metadata fields)
            if let Some(ref filters) = field_filters {
                let mut all_match = true;
                for (field, expected) in filters {
                    let actual = match field.as_str() {
                        "event_type" => Some(meta.event_type.as_str()),
                        "event_status" => Some(meta.event_status.as_str()),
                        "event_id" => Some(meta.event_id.as_str()),
                        _ => None, // user fields need Python-side check
                    };
                    if let Some(actual_val) = actual {
                        if actual_val != expected {
                            all_match = false;
                            break;
                        }
                    }
                    // If field not found in Rust metadata, skip (Python will do full check)
                }
                if !all_match {
                    continue;
                }
            }

            return Some(event_id.clone());
        }

        None
    }

    // ── History Trimming ──────────────────────────────────────────

    /// Trim event history to stay within max_history_size.
    /// Returns number of events removed as a list of removed event_ids.
    #[pyo3(signature = (owner_label=None))]
    fn trim_history(&mut self, owner_label: Option<&str>) -> Vec<String> {
        let max_size = match self.max_history_size {
            None => return vec![],
            Some(0) => {
                // Remove all completed events
                let completed_ids: Vec<String> = self
                    .history_order
                    .iter()
                    .filter(|id| {
                        self.events
                            .get(id.as_str())
                            .is_some_and(|m| m.event_status == "completed")
                    })
                    .cloned()
                    .collect();
                for id in &completed_ids {
                    self.events.remove(id);
                }
                self.history_order.retain(|id| !completed_ids.contains(id));
                return completed_ids;
            }
            Some(size) => size,
        };

        if !self.max_history_drop || self.events.len() <= max_size {
            return vec![];
        }

        let mut removed = Vec::new();
        let mut remaining_overage = self.events.len() - max_size;

        // First pass: remove completed events (oldest first)
        let completed_ids: Vec<String> = self
            .history_order
            .iter()
            .filter(|id| {
                self.events
                    .get(id.as_str())
                    .is_some_and(|m| m.event_status == "completed")
            })
            .take(remaining_overage)
            .cloned()
            .collect();

        for id in completed_ids {
            self.events.remove(&id);
            removed.push(id);
            remaining_overage -= 1;
            if remaining_overage == 0 {
                break;
            }
        }

        // Second pass: remove any events if still over
        if remaining_overage > 0 {
            let oldest_ids: Vec<String> = self
                .history_order
                .iter()
                .filter(|id| self.events.contains_key(id.as_str()))
                .take(remaining_overage)
                .cloned()
                .collect();
            for id in oldest_ids {
                self.events.remove(&id);
                removed.push(id);
            }
        }

        // Clean up history_order
        let events_ref = &self.events;
        self.history_order.retain(|id| events_ref.contains_key(id));

        removed
    }

    /// Check if history size exceeds limit (for emit-time backpressure check).
    fn history_at_limit(&self) -> bool {
        match self.max_history_size {
            None => false,
            Some(0) => false,
            Some(size) => self.events.len() >= size,
        }
    }

    /// Check if history exceeds soft limit (for amortized trim).
    fn history_exceeds_soft_limit(&self) -> bool {
        match self.max_history_size {
            None => false,
            Some(0) => false,
            Some(size) => {
                let soft_limit = std::cmp::max(size, (size as f64 * 1.2) as usize);
                self.events.len() > soft_limit
            }
        }
    }

    // ── Timeout Resolution ────────────────────────────────────────

    /// Resolve the effective handler timeout using the cascade:
    /// handler.handler_timeout > event.event_handler_timeout > bus.event_timeout
    /// Then min(resolved_handler, resolved_event), optionally capped by override.
    #[pyo3(signature = (
        handler_timeout = None,
        handler_timeout_explicitly_set = false,
        event_handler_timeout = None,
        event_timeout = None,
        timeout_override = None,
    ))]
    fn resolve_handler_timeout(
        &self,
        handler_timeout: Option<f64>,
        handler_timeout_explicitly_set: bool,
        event_handler_timeout: Option<f64>,
        event_timeout: Option<f64>,
        timeout_override: Option<f64>,
    ) -> Option<f64> {
        let resolved_handler_timeout = if handler_timeout_explicitly_set {
            handler_timeout
        } else if event_handler_timeout.is_some() {
            event_handler_timeout
        } else {
            self.event_timeout
        };

        let resolved_event_timeout = event_timeout.or(self.event_timeout);

        let resolved = match (resolved_handler_timeout, resolved_event_timeout) {
            (None, None) => None,
            (Some(h), None) => Some(h),
            (None, Some(e)) => Some(e),
            (Some(h), Some(e)) => Some(h.min(e)),
        };

        match (timeout_override, resolved) {
            (None, r) => r,
            (Some(o), None) => Some(o),
            (Some(o), Some(r)) => Some(o.min(r)),
        }
    }

    /// Resolve the event-level timeout.
    fn resolve_event_timeout(
        &self,
        event_timeout: Option<f64>,
        timeout_override: Option<f64>,
    ) -> Option<f64> {
        let resolved = event_timeout.or(self.event_timeout);
        match timeout_override {
            None => resolved,
            Some(o) => match resolved {
                None => Some(o),
                Some(r) => Some(o.min(r)),
            },
        }
    }

    /// Resolve event slow timeout.
    fn resolve_event_slow_timeout(&self, event_slow_timeout: Option<f64>) -> Option<f64> {
        event_slow_timeout.or(self.event_slow_timeout)
    }

    /// Resolve handler slow timeout with cascade.
    #[pyo3(signature = (
        handler_slow_timeout = None,
        handler_slow_timeout_explicitly_set = false,
        event_handler_slow_timeout = None,
        event_slow_timeout = None,
    ))]
    fn resolve_handler_slow_timeout(
        &self,
        handler_slow_timeout: Option<f64>,
        handler_slow_timeout_explicitly_set: bool,
        event_handler_slow_timeout: Option<f64>,
        event_slow_timeout: Option<f64>,
    ) -> Option<f64> {
        if handler_slow_timeout_explicitly_set {
            return handler_slow_timeout;
        }
        if event_handler_slow_timeout.is_some() {
            return event_handler_slow_timeout;
        }
        if event_slow_timeout.is_some() {
            return event_slow_timeout;
        }
        self.event_handler_slow_timeout
    }

    // ── Lock Policy Resolution ────────────────────────────────────

    /// Resolve which lock mode to use for event-level locking.
    /// Returns: "global-serial", "bus-serial", "parallel", or "none"
    fn resolve_event_lock_mode(&self, event_id: &str) -> String {
        let mode = self
            .events
            .get(event_id)
            .and_then(|m| m.event_concurrency.as_deref())
            .unwrap_or(&self.event_concurrency);
        mode.to_string()
    }

    /// Resolve which lock mode to use for handler-level locking.
    /// Returns: "serial" or "parallel"
    fn resolve_handler_lock_mode(&self, event_id: &str) -> String {
        let mode = self
            .events
            .get(event_id)
            .and_then(|m| m.event_handler_concurrency.as_deref())
            .unwrap_or(&self.event_handler_concurrency);
        mode.to_string()
    }

    /// Resolve handler completion mode.
    /// Returns: "all" or "first"
    fn resolve_handler_completion_mode(&self, event_id: &str) -> String {
        let mode = self
            .events
            .get(event_id)
            .and_then(|m| m.event_handler_completion.as_deref())
            .unwrap_or(&self.event_handler_completion);
        mode.to_string()
    }

    // ── Completion Detection ──────────────────────────────────────

    /// Check if all handler results for an event are terminal (completed/error).
    fn all_results_terminal(&self, event_id: &str) -> bool {
        let meta = match self.events.get(event_id) {
            Some(m) => m,
            None => return false,
        };
        if meta.result_statuses.is_empty() {
            return false;
        }
        meta.result_statuses
            .values()
            .all(|s| s == "completed" || s == "error")
    }

    /// Check if event and all its children are complete.
    /// Returns true if the event should be marked completed.
    fn check_event_tree_complete(&self, event_id: &str) -> bool {
        let meta = match self.events.get(event_id) {
            Some(m) => m,
            None => return false,
        };

        // All results must be terminal
        if meta.result_statuses.is_empty() {
            return false;
        }
        if !meta
            .result_statuses
            .values()
            .all(|s| s == "completed" || s == "error")
        {
            return false;
        }

        // All children must be completed
        for child_ids in meta.result_children.values() {
            for child_id in child_ids {
                if let Some(child_meta) = self.events.get(child_id) {
                    if child_meta.event_status != "completed" {
                        return false;
                    }
                }
            }
        }

        true
    }

    // ── Bus Label ─────────────────────────────────────────────────

    fn label(&self) -> String {
        let suffix = if self.bus_id.len() >= 4 {
            &self.bus_id[self.bus_id.len() - 4..]
        } else {
            &self.bus_id
        };
        format!("{}#{}", self.bus_name, suffix)
    }

    // ── Bulk Operations ───────────────────────────────────────────

    /// Clear all state (for stop(clear=True)).
    fn clear_all(&mut self) {
        self.handlers.clear();
        self.handlers_by_key.clear();
        self.events.clear();
        self.history_order.clear();
        self.in_flight_event_ids.clear();
        self.processing_event_ids.clear();
    }

    /// Get all event_ids in history order.
    fn history_event_ids(&self) -> Vec<String> {
        self.history_order.iter().cloned().collect()
    }

    /// Get event metadata as a dict (for debugging/serialization).
    fn get_event_meta(&self, py: Python<'_>, event_id: &str) -> PyResult<Option<PyObject>> {
        let meta = match self.events.get(event_id) {
            Some(m) => m,
            None => return Ok(None),
        };
        let dict = PyDict::new(py);
        dict.set_item("event_id", &meta.event_id)?;
        dict.set_item("event_type", &meta.event_type)?;
        dict.set_item("event_status", &meta.event_status)?;
        dict.set_item("event_parent_id", &meta.event_parent_id)?;
        dict.set_item("event_created_at", &meta.event_created_at)?;
        dict.set_item("event_concurrency", &meta.event_concurrency)?;
        dict.set_item("event_handler_concurrency", &meta.event_handler_concurrency)?;
        dict.set_item("event_handler_completion", &meta.event_handler_completion)?;
        dict.set_item("event_timeout", &meta.event_timeout)?;
        dict.set_item("event_handler_timeout", &meta.event_handler_timeout)?;
        Ok(Some(dict.into()))
    }

    // ── Setters ───────────────────────────────────────────────────

    #[setter]
    fn set_bus_name(&mut self, name: String) {
        self.bus_name = name;
    }

    #[getter]
    fn get_bus_name(&self) -> &str {
        &self.bus_name
    }

    #[getter]
    fn get_bus_id(&self) -> &str {
        &self.bus_id
    }

    #[getter]
    fn get_max_history_size(&self) -> Option<usize> {
        self.max_history_size
    }

    #[setter]
    fn set_max_history_size(&mut self, size: Option<usize>) {
        self.max_history_size = size;
    }

    #[getter]
    fn get_max_history_drop(&self) -> bool {
        self.max_history_drop
    }

    #[setter]
    fn set_max_history_drop(&mut self, drop: bool) {
        self.max_history_drop = drop;
    }
}

impl RustEventBusCore {
    /// Internal loop detection logic.
    /// Check re-entrancy: handler already has in-flight/completed result for this event.
    /// Forwarding loop prevention is handled at emit() by checking event_path.
    /// Recursion depth check stays in Python (needs to raise RuntimeError).
    fn would_create_loop_inner(
        &self,
        event_meta: &EventMeta,
        handler: &HandlerMeta,
        existing_result_handler_ids: &[String],
        existing_result_statuses: &HashMap<String, String>,
    ) -> bool {
        if existing_result_handler_ids.contains(&handler.id) {
            if let Some(status) = existing_result_statuses.get(&handler.id) {
                if status == "started" {
                    return true;
                }
                if status == "pending" && event_meta.event_status == "started" {
                    return true;
                }
                if status == "completed" || status == "error" {
                    return true;
                }
            }
        }
        false
    }
}

// ── Python Module ─────────────────────────────────────────────────

#[pymodule]
fn _abxbus_rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Timestamp functions
    m.add_function(wrap_pyfunction!(monotonic_datetime, m)?)?;
    m.add_function(wrap_pyfunction!(format_epoch_ns_to_iso, m)?)?;

    // UUID functions
    m.add_function(wrap_pyfunction!(uuid7str, m)?)?;
    m.add_function(wrap_pyfunction!(uuid5_handler_id, m)?)?;
    m.add_function(wrap_pyfunction!(compute_handler_id, m)?)?;
    m.add_function(wrap_pyfunction!(validate_uuid_str, m)?)?;

    // Validators
    m.add_function(wrap_pyfunction!(validate_event_name, m)?)?;
    m.add_function(wrap_pyfunction!(validate_event_path_entry, m)?)?;
    m.add_function(wrap_pyfunction!(is_valid_identifier, m)?)?;
    m.add_function(wrap_pyfunction!(validate_bus_name, m)?)?;
    m.add_function(wrap_pyfunction!(check_reserved_event_fields, m)?)?;

    // Core class
    m.add_class::<RustEventBusCore>()?;

    Ok(())
}
