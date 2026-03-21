use bubus_rust::{
    event_bus::EventBus,
    event_handler::EventHandler,
    id::{compute_handler_id, handler_id_namespace},
};
use serde_json::Map;
use uuid::Uuid;

#[test]
fn test_bus_and_event_ids_are_uuid_v7() {
    let bus = EventBus::new(Some("BusId".to_string()));
    let bus_id = Uuid::parse_str(&bus.id).expect("bus id must parse");
    assert_eq!(bus_id.get_version_num(), 7);

    let event = bubus_rust::base_event::BaseEvent::new("work", Map::new());
    let event_id = Uuid::parse_str(&event.inner.lock().event_id).expect("event id must parse");
    assert_eq!(event_id.get_version_num(), 7);
}

#[test]
fn test_handler_id_uses_v5_namespace_seed_compatible_with_python_ts() {
    let eventbus_id = "018f6f0e-79b2-7cc5-aed9-f0f9a4e5e6b0";
    let handler_name = "module.fn";
    let handler_registered_at = "2026-01-01T00:00:00.000Z";
    let event_pattern = "work";
    let expected_seed =
        format!("{eventbus_id}|{handler_name}|unknown|{handler_registered_at}|{event_pattern}");

    let expected = Uuid::new_v5(&handler_id_namespace(), expected_seed.as_bytes()).to_string();
    let actual = compute_handler_id(
        eventbus_id,
        handler_name,
        None,
        handler_registered_at,
        event_pattern,
    );
    assert_eq!(actual, expected);

    let entry = EventHandler::from_callable(
        event_pattern.to_string(),
        handler_name.to_string(),
        "BusId".to_string(),
        eventbus_id.to_string(),
        std::sync::Arc::new(|_event| Box::pin(async { Ok(serde_json::Value::Null) })),
    );
    let ns = Uuid::parse_str(&entry.id).expect("handler id must parse");
    assert_eq!(ns.get_version_num(), 5);
}
