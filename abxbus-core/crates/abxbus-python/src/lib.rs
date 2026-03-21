use pyo3::prelude::*;

use abxbus::event_history::EventHistory;
use abxbus::monotonic_dt;
use abxbus::uuid_gen;
use abxbus::validation;

// ── Standalone Functions ──────────────────────────────────────────

/// Generate a monotonic datetime string, or normalize a provided ISO string.
#[pyfunction]
#[pyo3(signature = (isostring=None))]
fn monotonic_datetime(isostring: Option<&str>) -> PyResult<String> {
    monotonic_dt::monotonic_datetime(isostring)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}

/// Format epoch nanoseconds to canonical ISO 8601 string.
#[pyfunction]
fn format_epoch_ns_to_iso(epoch_ns: i64) -> PyResult<String> {
    monotonic_dt::format_epoch_ns_to_iso(epoch_ns)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}

/// Generate a new UUID v7 string.
#[pyfunction]
fn uuid7str() -> String {
    uuid_gen::uuid7str()
}

/// Generate a deterministic UUID v5 handler ID from a seed string.
#[pyfunction]
fn uuid5_handler_id(seed: &str) -> String {
    uuid_gen::uuid5_handler_id(seed)
}

/// Compute a handler ID from its component parts.
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

/// Validate and normalize a UUID string.
#[pyfunction]
fn validate_uuid_str(s: &str) -> PyResult<String> {
    uuid_gen::validate_uuid(s)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}

/// Validate an event type name.
#[pyfunction]
fn validate_event_name(s: &str) -> PyResult<String> {
    validation::validate_event_name(s)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}

/// Validate an event_path entry string.
#[pyfunction]
fn validate_event_path_entry(s: &str) -> PyResult<String> {
    validation::validate_event_path_entry(s)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}

/// Check if a string is a valid Python/JS identifier.
#[pyfunction]
fn is_valid_identifier(s: &str) -> bool {
    validation::is_valid_identifier(s)
}

/// Validate a bus name.
#[pyfunction]
fn validate_bus_name(s: &str) -> PyResult<String> {
    validation::validate_bus_name(s)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}

/// Check for reserved field names in event payload keys.
#[pyfunction]
fn check_reserved_event_fields(keys: Vec<String>, known_fields: Vec<String>) -> PyResult<()> {
    let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
    let field_refs: Vec<&str> = known_fields.iter().map(|s| s.as_str()).collect();
    validation::check_reserved_event_fields(&key_refs, &field_refs)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}

// ── Python-wrapped EventHistory ───────────────────────────────────

/// Rust-accelerated event history with fast find() and trim().
#[pyclass]
struct RustEventHistory {
    inner: EventHistory,
}

#[pymethods]
impl RustEventHistory {
    #[new]
    #[pyo3(signature = (max_history_size=Some(100), max_history_drop=false))]
    fn new(max_history_size: Option<usize>, max_history_drop: bool) -> Self {
        Self {
            inner: EventHistory::new(max_history_size, max_history_drop),
        }
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn has_event(&self, event_id: &str) -> bool {
        self.inner.has_event(event_id)
    }

    fn remove_event(&mut self, event_id: &str) -> bool {
        self.inner.remove_event(event_id)
    }

    fn clear(&mut self) {
        self.inner.clear();
    }

    /// Trim event history to stay within max_history_size.
    #[pyo3(signature = (owner_label=None))]
    fn trim_event_history(&mut self, owner_label: Option<&str>) -> usize {
        self.inner.trim_event_history(owner_label)
    }
}

// ── Python Module ─────────────────────────────────────────────────

/// The Rust-accelerated core for abxbus.
///
/// Provides high-performance replacements for CPU-bound hot paths:
/// - `monotonic_datetime()` — lock-free monotonic timestamp generation
/// - `uuid7str()` / `uuid5_handler_id()` — fast UUID generation
/// - Field validators — identifier checks, UUID normalization
/// - `RustEventHistory` — fast event storage with find() and trim()
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

    // Classes
    m.add_class::<RustEventHistory>()?;

    Ok(())
}
