use std::sync::Arc;

use bubus_rust::{base_event::BaseEvent, types::EventStatus};
use futures::executor::block_on;
use serde_json::{json, Map};

fn mk_event(event_type: &str) -> Arc<BaseEvent> {
    let mut payload = Map::new();
    payload.insert("value".to_string(), json!(1));
    BaseEvent::new(event_type.to_string(), payload)
}

#[test]
fn test_base_event_json_roundtrip() {
    let event = mk_event("test_event");
    let json_value = event.to_json_value();
    let deserialized = BaseEvent::from_json_value(json_value.clone());
    assert_eq!(json_value, deserialized.to_json_value());
}

#[test]
fn test_base_event_runtime_state_transitions() {
    let event = mk_event("runtime_event");
    assert_eq!(event.inner.lock().event_status, EventStatus::Pending);
    event.mark_started();
    assert_eq!(event.inner.lock().event_status, EventStatus::Started);
    event.mark_completed();
    assert_eq!(event.inner.lock().event_status, EventStatus::Completed);
    block_on(event.wait_completed());
}
