use abxbus::{base_event::BaseEvent, event_bus::EventBus};
use futures::executor::block_on;
use serde_json::json;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[test]
fn test_history_pressure_preserves_completed_parent_of_pending_child() {
    let bus = EventBus::new_with_history(Some("HistoryAncestry".into()), Some(2), true);
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    bus.on_raw("HeldChild", "hold", move |_event| {
        let release_rx = release_rx.clone();
        async move {
            release_rx
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(2))
                .unwrap();
            Ok(json!("ok"))
        }
    });
    let parent = bus.emit_base(BaseEvent::new("Parent", serde_json::Map::new()));
    let _ = block_on(parent.wait());
    let extra = bus.emit_base(BaseEvent::new("Extra", serde_json::Map::new()));
    let _ = block_on(extra.wait());
    let parent_id = parent.inner.lock().event_id.clone();
    let child = BaseEvent::new("HeldChild", serde_json::Map::new());
    child.inner.lock().event_parent_id = Some(parent_id.clone());
    let child = bus.emit_base(child);
    let child_id = child.inner.lock().event_id.clone();
    for _ in 0..8 {
        bus.emit_base(BaseEvent::new("Extra", serde_json::Map::new()));
    }
    let retained = bus.runtime_payload_for_test();
    release_tx.send(()).unwrap();
    let _ = block_on(child.wait());
    assert!(block_on(bus.wait_until_idle(None)));
    assert!(bus.event_history_size() <= 2);
    bus.destroy();
    assert!(
        retained.contains_key(&parent_id),
        "history pressure removed pending child's parent"
    );
    assert!(retained.contains_key(&child_id));
}
