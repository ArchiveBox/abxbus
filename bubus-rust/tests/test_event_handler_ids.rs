use bubus_rust::id::compute_handler_id;
use uuid::Uuid;

#[test]
fn test_compute_handler_id_matches_uuidv5_seed_algorithm() {
    let eventbus_id = "0195f6ac-9f10-7e4b-bf69-fb33c68ca13e";
    let handler_name = "tests.handlers.handle_work";
    let handler_file_path = Some("~/repo/tests/handlers.py:10");
    let handler_registered_at = "2025-01-01T00:00:00.000000Z";
    let event_pattern = "work";

    let computed = compute_handler_id(
        eventbus_id,
        handler_name,
        handler_file_path,
        handler_registered_at,
        event_pattern,
    );

    let namespace = Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"bubus-handler");
    let seed = format!(
        "{}|{}|{}|{}|{}",
        eventbus_id,
        handler_name,
        handler_file_path.unwrap(),
        handler_registered_at,
        event_pattern
    );
    let expected = Uuid::new_v5(&namespace, seed.as_bytes()).to_string();

    assert_eq!(computed, expected);
}
