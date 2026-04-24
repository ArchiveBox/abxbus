use bubus_rust::{
    event_handler::EventHandler,
    event_result::{EventResult, EventResultStatus},
};

#[test]
fn test_event_result_defaults() {
    let handler = EventHandler {
        id: "h1".into(),
        event_pattern: "work".into(),
        handler_name: "handler".into(),
        handler_file_path: None,
        handler_timeout: None,
        handler_slow_timeout: None,
        handler_registered_at: "2026-01-01T00:00:00.000Z".into(),
        eventbus_name: "bus".into(),
        eventbus_id: "bus-id".into(),
        callable: None,
    };

    let result = EventResult::new("event-id".into(), handler, Some(5.0));
    assert_eq!(result.status, EventResultStatus::Pending);
    assert_eq!(result.timeout, Some(5.0));
}
