use bubus_rust::{
    event_bus::EventBus,
    typed::{EventSpec, TypedEvent},
    types::{EventHandlerCompletionMode, EventHandlerConcurrencyMode},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Serialize, Deserialize)]
struct Payload {
    value: i64,
}
#[derive(Clone, Serialize, Deserialize)]
struct ResultT {
    value: String,
}
struct WorkEvent;
impl EventSpec for WorkEvent {
    type Payload = Payload;
    type Result = ResultT;
    const EVENT_TYPE: &'static str = "work";
}

#[test]
fn test_bus_default_handler_settings_are_applied() {
    let bus = EventBus::new(Some("BusDefaults".to_string()));

    bus.on("work", "h1", |_event| async move { Ok(json!("ok")) });
    let event = TypedEvent::<WorkEvent>::new(Payload { value: 1 });
    {
        let mut inner = event.inner.inner.lock();
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Serial);
        inner.event_handler_completion = Some(EventHandlerCompletionMode::All);
    }
    let event = bus.emit_existing(event);
    block_on(event.wait_completed());

    assert_eq!(event.inner.inner.lock().event_results.len(), 1);
    bus.stop();
}
