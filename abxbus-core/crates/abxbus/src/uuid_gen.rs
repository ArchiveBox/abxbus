use uuid::Uuid;

use crate::types::HANDLER_ID_NAMESPACE;

/// Generate a new UUID v7 string (time-ordered, random).
pub fn uuid7str() -> String {
    Uuid::now_v7().to_string()
}

/// Generate a deterministic UUID v5 handler ID from a seed string.
///
/// Matches the Python/TS algorithm:
/// `uuid5(HANDLER_ID_NAMESPACE, "{bus_id}|{handler_name}|{file_path}|{registered_at}|{event_pattern}")`
pub fn uuid5_handler_id(seed: &str) -> String {
    Uuid::new_v5(&*HANDLER_ID_NAMESPACE, seed.as_bytes()).to_string()
}

/// Compute a handler ID from its component parts.
pub fn compute_handler_id(
    eventbus_id: &str,
    handler_name: &str,
    file_path: &str,
    registered_at: &str,
    event_pattern: &str,
) -> String {
    let seed = format!(
        "{}|{}|{}|{}|{}",
        eventbus_id, handler_name, file_path, registered_at, event_pattern
    );
    uuid5_handler_id(&seed)
}

/// Validate and normalize a UUID string.
pub fn validate_uuid(s: &str) -> Result<String, String> {
    let parsed = Uuid::parse_str(s).map_err(|e| format!("Invalid UUID: {}: {}", s, e))?;
    Ok(parsed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uuid7str_unique() {
        let a = uuid7str();
        let b = uuid7str();
        assert_ne!(a, b);
        assert_eq!(a.len(), 36);
    }

    #[test]
    fn test_uuid5_deterministic() {
        let a = uuid5_handler_id("test-seed");
        let b = uuid5_handler_id("test-seed");
        assert_eq!(a, b);

        let c = uuid5_handler_id("different-seed");
        assert_ne!(a, c);
    }

    #[test]
    fn test_compute_handler_id() {
        let id = compute_handler_id(
            "bus-123",
            "my_handler",
            "test.py:10",
            "2025-01-01T00:00:00.000000000Z",
            "MyEvent",
        );
        assert_eq!(id.len(), 36);

        // Deterministic
        let id2 = compute_handler_id(
            "bus-123",
            "my_handler",
            "test.py:10",
            "2025-01-01T00:00:00.000000000Z",
            "MyEvent",
        );
        assert_eq!(id, id2);
    }

    #[test]
    fn test_validate_uuid() {
        let valid = validate_uuid("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(valid, "550e8400-e29b-41d4-a716-446655440000");

        assert!(validate_uuid("not-a-uuid").is_err());
    }
}
