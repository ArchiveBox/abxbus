use abxbus_rust::event;
use abxbus_rust::event_bus::EventBus;
use futures::executor::block_on;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}

event! {
    struct HistoryEvent {
        event_result_type: EmptyResult,
        event_type: "history_event",
    }
}
#[test]
fn test_max_history_drop_true_keeps_recent_entries() {
    let bus = EventBus::new_with_history(Some("HistoryDropBus".to_string()), Some(2), true);

    for _ in 0..3 {
        let event = bus.emit(HistoryEvent {
            ..Default::default()
        });
        block_on(event.done());
    }

    let history = bus.event_history_ids();
    assert_eq!(history.len(), 2);
    assert!(history.iter().any(|id| id.contains('-')));
    bus.stop();
}

#[test]
#[should_panic(expected = "history limit reached")]
fn test_max_history_drop_false_rejects_new_emit_when_full() {
    let bus = EventBus::new_with_history(Some("HistoryRejectBus".to_string()), Some(1), false);

    let first = bus.emit(HistoryEvent {
        ..Default::default()
    });
    block_on(first.done());

    bus.emit(HistoryEvent {
        ..Default::default()
    });
}
