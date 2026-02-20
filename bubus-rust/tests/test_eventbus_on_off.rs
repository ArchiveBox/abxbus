use bubus_rust::{
    event_bus::EventBus,
    typed::{EventSpec, TypedEvent},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Serialize, Deserialize)]
struct EmptyPayload {}
#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}
struct WorkEvent;
impl EventSpec for WorkEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "work";
}

#[test]
fn test_on_returns_handler_and_off_removes_handler() {
    let bus = EventBus::new(Some("OnOffBus".to_string()));

    let handler = bus.on("work", "h1", |_event| async move { Ok(json!("ok")) });
    let event_1 = bus.emit::<WorkEvent>(TypedEvent::<WorkEvent>::new(EmptyPayload {}));
    block_on(event_1.wait_completed());
    assert_eq!(event_1.inner.inner.lock().event_results.len(), 1);

    bus.off("work", Some(&handler.id));
    let event_2 = bus.emit::<WorkEvent>(TypedEvent::<WorkEvent>::new(EmptyPayload {}));
    block_on(event_2.wait_completed());
    assert_eq!(event_2.inner.inner.lock().event_results.len(), 0);

    bus.stop();
}
