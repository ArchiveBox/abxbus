use abxbus_rust::{
    event_bus::EventBus,
    event_handler::EventHandlerOptions,
    typed::{EventSpec, TypedEvent},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

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
fn test_on_stores_eventhandler_entry_and_index() {
    let bus = EventBus::new(Some("RegistryBus".to_string()));

    let entry = bus.on("work", "handler", |_event| async move { Ok(json!("work")) });
    let payload = bus.to_json_value();

    assert!(payload["handlers"]
        .as_object()
        .expect("handlers")
        .contains_key(&entry.id));
    assert_eq!(payload["handlers"][&entry.id]["event_pattern"], "work");
    assert_eq!(payload["handlers"][&entry.id]["handler_name"], "handler");
    assert_eq!(
        payload["handlers_by_key"]["work"],
        json!([entry.id.clone()])
    );

    let dispatched = bus.emit::<WorkEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(dispatched.wait_completed());
    let results = dispatched.inner.inner.lock().event_results.clone();
    assert!(results.contains_key(&entry.id));
    assert_eq!(results[&entry.id].handler.id, entry.id);
    bus.stop();
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

#[test]
fn test_off_removes_handler_id_or_all_and_prunes_empty_index() {
    let bus = EventBus::new(Some("RegistryOffBus".to_string()));

    let entry_a = bus.on("work", "handler_a", |_event| async move { Ok(Value::Null) });
    let entry_b = bus.on("work", "handler_b", |_event| async move { Ok(Value::Null) });
    let entry_c = bus.on("work", "handler_c", |_event| async move { Ok(Value::Null) });

    bus.off("work", Some(&entry_a.id));
    let payload_after_a = bus.to_json_value();
    assert!(!payload_after_a["handlers"]
        .as_object()
        .expect("handlers")
        .contains_key(&entry_a.id));
    assert_eq!(
        payload_after_a["handlers_by_key"]["work"],
        json!([entry_b.id.clone(), entry_c.id.clone()])
    );

    bus.off("work", Some(&entry_b.id));
    let payload_after_b = bus.to_json_value();
    assert!(!payload_after_b["handlers"]
        .as_object()
        .expect("handlers")
        .contains_key(&entry_b.id));
    assert_eq!(
        payload_after_b["handlers_by_key"]["work"],
        json!([entry_c.id.clone()])
    );

    bus.off("work", Some(&entry_c.id));
    let payload_after_c = bus.to_json_value();
    assert!(!payload_after_c["handlers_by_key"]
        .as_object()
        .expect("handlers_by_key")
        .contains_key("work"));

    bus.on("work", "handler_a", |_event| async move { Ok(Value::Null) });
    bus.on("work", "handler_b", |_event| async move { Ok(Value::Null) });
    bus.off("work", None);
    let payload_after_all = bus.to_json_value();
    assert!(!payload_after_all["handlers_by_key"]
        .as_object()
        .expect("handlers_by_key")
        .contains_key("work"));
    assert!(payload_after_all["handlers"]
        .as_object()
        .expect("handlers")
        .values()
        .all(|entry| entry["event_pattern"] != "work"));

    let dispatched = bus.emit::<WorkEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(dispatched.wait_completed());
    assert_eq!(dispatched.inner.inner.lock().event_results.len(), 0);
    bus.stop();
}

#[test]
fn test_on_with_options_supports_id_override_and_handler_file_path() {
    let bus = EventBus::new(Some("RegistryOptionsBus".to_string()));
    let explicit_id = "018f8e40-1234-7000-8000-000000009999".to_string();

    let entry = bus.on_with_options(
        "work",
        "handler",
        EventHandlerOptions {
            id: Some(explicit_id.clone()),
            handler_file_path: Some("~/project/app.rs:123".to_string()),
            handler_timeout: Some(0.5),
            handler_slow_timeout: Some(0.25),
            handler_registered_at: Some("2025-01-02T03:04:05.678901000Z".to_string()),
        },
        |_event| async move { Ok(json!("ok")) },
    );

    assert_eq!(entry.id, explicit_id);
    assert_eq!(
        entry.handler_file_path.as_deref(),
        Some("~/project/app.rs:123")
    );
    assert_eq!(entry.handler_timeout, Some(0.5));
    assert_eq!(entry.handler_slow_timeout, Some(0.25));

    let payload = bus.to_json_value();
    let handler_payload = &payload["handlers"][&entry.id];
    assert_eq!(handler_payload["id"], explicit_id);
    assert_eq!(handler_payload["handler_file_path"], "~/project/app.rs:123");
    assert_eq!(handler_payload["handler_timeout"], 0.5);
    assert_eq!(handler_payload["handler_slow_timeout"], 0.25);
    assert_eq!(
        handler_payload["handler_registered_at"],
        "2025-01-02T03:04:05.678901000Z"
    );
    bus.stop();
}
