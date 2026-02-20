use bubus_rust::{base_event::BaseEvent, event_bus::EventBus};
use futures::executor::block_on;
use serde_json::{json, Map};

#[test]
fn test_on_returns_handler_and_off_removes_handler() {
    let bus = EventBus::new(Some("OnOffBus".to_string()));

    let handler = bus.on("work", "h1", |_event| async move { Ok(json!("ok")) });
    let event_1 = BaseEvent::new("work", Map::new());
    bus.emit(event_1.clone());
    block_on(event_1.wait_completed());
    assert_eq!(event_1.inner.lock().event_results.len(), 1);

    bus.off("work", Some(&handler.id));
    let event_2 = BaseEvent::new("work", Map::new());
    bus.emit(event_2.clone());
    block_on(event_2.wait_completed());
    assert_eq!(event_2.inner.lock().event_results.len(), 0);

    bus.stop();
}
