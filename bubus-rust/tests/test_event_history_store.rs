use bubus_rust::{base_event::BaseEvent, event_bus::EventBus};
use futures::executor::block_on;
use serde_json::Map;

#[test]
fn test_max_history_drop_true_keeps_recent_entries() {
    let bus = EventBus::new_with_history(Some("HistoryDropBus".to_string()), Some(2), true);

    for i in 0..3 {
        let event = BaseEvent::new(format!("evt_{i}"), Map::new());
        bus.emit_raw(event.clone());
        block_on(event.wait_completed());
    }

    let history = bus.event_history_ids();
    assert_eq!(history.len(), 2);
    assert!(history.iter().any(|id| id.contains('-')));
    bus.stop();
}

#[test]
fn test_max_history_drop_false_rejects_new_emit_when_full() {
    let bus = EventBus::new_with_history(Some("HistoryRejectBus".to_string()), Some(1), false);

    let first = BaseEvent::new("first", Map::new());
    bus.emit_raw(first.clone());
    block_on(first.wait_completed());

    let second = BaseEvent::new("second", Map::new());
    bus.emit_raw(second.clone());
    block_on(second.wait_completed());

    assert_eq!(second.inner.lock().event_path.len(), 0);
    bus.stop();
}
