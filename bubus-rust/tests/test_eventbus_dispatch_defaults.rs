use std::sync::Arc;

use bubus_rust::{
    base_event::BaseEvent,
    event_bus::EventBus,
    types::{EventHandlerCompletionMode, EventHandlerConcurrencyMode},
};
use futures::executor::block_on;
use serde_json::{json, Map};

fn mk_event(event_type: &str) -> Arc<BaseEvent> {
    let mut payload = Map::new();
    payload.insert("value".to_string(), json!(1));
    BaseEvent::new(event_type.to_string(), payload)
}

#[test]
fn test_bus_default_handler_settings_are_applied() {
    let bus = EventBus::new(Some("BusDefaults".to_string()));

    bus.on("work", "h1", |_event| async move { Ok(json!("ok")) });
    let event = mk_event("work");
    {
        let mut inner = event.inner.lock();
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Serial);
        inner.event_handler_completion = Some(EventHandlerCompletionMode::All);
    }
    bus.emit(event.clone());
    block_on(event.wait_completed());

    assert_eq!(event.inner.lock().event_results.len(), 1);
    bus.stop();
}
