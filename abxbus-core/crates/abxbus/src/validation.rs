use crate::types::RESERVED_USER_EVENT_FIELDS;

/// Check if a string is a valid Python/JS identifier.
/// Matches Python's `str.isidentifier()` behavior.
pub fn is_valid_identifier(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let mut chars = s.chars();
    let first = chars.next().unwrap();
    if !first.is_alphabetic() && first != '_' {
        return false;
    }
    chars.all(|c| c.is_alphanumeric() || c == '_')
}

/// Validate an event type name (must be a valid identifier, no leading underscore).
pub fn validate_event_name(s: &str) -> Result<String, String> {
    if !is_valid_identifier(s) || s.starts_with('_') {
        return Err(format!("Invalid event name: {}", s));
    }
    Ok(s.to_string())
}

/// Validate and normalize a UUID string.
pub fn validate_uuid_str(s: &str) -> Result<String, String> {
    crate::uuid_gen::validate_uuid(s)
}

/// Validate a Python id string (digits with optional dots).
pub fn validate_python_id_str(s: &str) -> Result<String, String> {
    if s.replace('.', "").chars().all(|c| c.is_ascii_digit()) && !s.is_empty() {
        Ok(s.to_string())
    } else {
        Err(format!("Invalid Python ID: {}", s))
    }
}

/// Validate an event_path entry string (format: `BusName#abcd`).
pub fn validate_event_path_entry(s: &str) -> Result<String, String> {
    if !s.contains('#') {
        return Err(format!(
            "Invalid event_path entry: {} (expected BusName#abcd)",
            s
        ));
    }
    let (bus_name, short_id) = s.rsplit_once('#').unwrap();
    if !is_valid_identifier(bus_name) || !short_id.chars().all(|c| c.is_alphanumeric()) || short_id.len() != 4 {
        return Err(format!(
            "Invalid event_path entry: {} (expected BusName#abcd)",
            s
        ));
    }
    Ok(s.to_string())
}

/// Check for reserved field names and invalid prefixes in event payload keys.
/// Returns an error message if any field violates the rules.
pub fn check_reserved_event_fields(
    keys: &[&str],
    known_base_event_fields: &[&str],
) -> Result<(), String> {
    for key in keys {
        // Check reserved runtime field names
        if RESERVED_USER_EVENT_FIELDS.contains(key) {
            return Err(format!(
                "Field \"{}\" is reserved for BaseEvent runtime APIs and cannot be set in event payload",
                key
            ));
        }
        // Check unknown event_ prefixed fields
        if key.starts_with("event_") && !known_base_event_fields.contains(key) {
            return Err(format!(
                "Field \"{}\" starts with \"event_\" but is not a recognized BaseEvent field",
                key
            ));
        }
        // Check model_ prefix
        if key.starts_with("model_") {
            return Err(format!(
                "Field \"{}\" starts with \"model_\" and is reserved for Pydantic model internals",
                key
            ));
        }
    }
    Ok(())
}

/// Validate an event bus name (must be a valid identifier, no leading underscore).
pub fn validate_bus_name(s: &str) -> Result<String, String> {
    if !is_valid_identifier(s) || s.starts_with('_') {
        return Err(format!("Invalid event bus name: {:?}", s));
    }
    Ok(s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid_identifier() {
        assert!(is_valid_identifier("hello"));
        assert!(is_valid_identifier("MyEvent"));
        assert!(is_valid_identifier("_private"));
        assert!(is_valid_identifier("test_123"));
        assert!(!is_valid_identifier(""));
        assert!(!is_valid_identifier("123abc"));
        assert!(!is_valid_identifier("hello world"));
        assert!(!is_valid_identifier("hello-world"));
    }

    #[test]
    fn test_validate_event_name() {
        assert!(validate_event_name("MyEvent").is_ok());
        assert!(validate_event_name("_private").is_err()); // No leading underscore
        assert!(validate_event_name("123abc").is_err());
    }

    #[test]
    fn test_validate_event_path_entry() {
        assert!(validate_event_path_entry("BusName#ab12").is_ok());
        assert!(validate_event_path_entry("MyBus#0000").is_ok());
        assert!(validate_event_path_entry("nohash").is_err());
        assert!(validate_event_path_entry("Bus#abc").is_err()); // Only 3 chars
        assert!(validate_event_path_entry("Bus#abcde").is_err()); // 5 chars
    }

    #[test]
    fn test_check_reserved_fields() {
        let known = &["event_type", "event_id", "event_status"];
        assert!(check_reserved_event_fields(&["bus"], known).is_err());
        assert!(check_reserved_event_fields(&["first"], known).is_err());
        assert!(check_reserved_event_fields(&["event_unknown_field"], known).is_err());
        assert!(check_reserved_event_fields(&["model_something"], known).is_err());
        assert!(check_reserved_event_fields(&["event_type"], known).is_ok());
        assert!(check_reserved_event_fields(&["custom_field"], known).is_ok());
    }
}
