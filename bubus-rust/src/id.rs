use uuid::Uuid;

pub fn uuid_v7_string() -> String {
    Uuid::now_v7().to_string()
}

pub fn handler_id_namespace() -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"bubus-handler")
}

pub fn compute_handler_id(
    eventbus_id: &str,
    handler_name: &str,
    handler_file_path: Option<&str>,
    handler_registered_at: &str,
    event_pattern: &str,
) -> String {
    let file_path = handler_file_path.unwrap_or("unknown");
    let seed = format!(
        "{}|{}|{}|{}|{}",
        eventbus_id, handler_name, file_path, handler_registered_at, event_pattern
    );
    Uuid::new_v5(&handler_id_namespace(), seed.as_bytes()).to_string()
}
