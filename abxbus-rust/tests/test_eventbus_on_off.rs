use abxbus_rust::{
    event_bus::EventBus,
    event_handler::EventHandlerOptions,
    event_result::EventResultStatus,
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

#[derive(Clone, Serialize, Deserialize)]
struct TokenPayload {
    required_token: String,
}
struct RegistryTypingEvent;
impl EventSpec for RegistryTypingEvent {
    type Payload = TokenPayload;
    type Result = String;
    const EVENT_TYPE: &'static str = "RegistryTypingEvent";
}

struct OtherEvent;
impl EventSpec for OtherEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "other";
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
            ..EventHandlerOptions::default()
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

#[test]
fn test_on_accepts_handlers_and_dispatch_captures_return_values() {
    let bus = EventBus::new(Some("RegistryNormalizeBus".to_string()));
    let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let calls_for_handler = calls.clone();

    let entry = bus.on("work", "sync_handler", move |event| {
        let calls = calls_for_handler.clone();
        async move {
            calls
                .lock()
                .expect("calls lock")
                .push(event.inner.lock().event_id.clone());
            Ok(json!("normalized"))
        }
    });

    let dispatched = bus.emit::<WorkEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(dispatched.wait_completed());
    let result = dispatched.inner.inner.lock().event_results[&entry.id].clone();

    assert_eq!(result.status, EventResultStatus::Completed);
    assert_eq!(result.result, Some(json!("normalized")));
    assert_eq!(calls.lock().expect("calls lock").len(), 1);
    bus.stop();
}

#[test]
fn test_handler_async_preserves_typed_arg_return_contracts_for_typed_handlers() {
    let bus = EventBus::new(Some("RegistryTypingSyncBus".to_string()));

    let entry = bus
        .on_typed::<RegistryTypingEvent, _, _>("typed_sync_handler", |event| async move {
            Ok(event.payload().required_token)
        });

    let event = bus.emit::<RegistryTypingEvent>(TypedEvent::new(TokenPayload {
        required_token: "sync".to_string(),
    }));
    block_on(event.wait_completed());

    let result = event.inner.inner.lock().event_results[&entry.id].clone();
    assert_eq!(result.status, EventResultStatus::Completed);
    assert_eq!(result.result, Some(json!("sync")));
    assert_eq!(event.first_result().as_deref(), Some("sync"));
    bus.stop();
}

#[test]
fn test_off_removing_all_for_one_event_key_preserves_other_event_keys() {
    let bus = EventBus::new(Some("RegistryOffSelectiveBus".to_string()));

    bus.on(
        "work",
        "work_handler",
        |_event| async move { Ok(json!("work")) },
    );
    let other_entry = bus.on("other", "other_handler", |_event| async move {
        Ok(json!("other"))
    });

    bus.off("work", None);
    let payload = bus.to_json_value();
    assert!(!payload["handlers_by_key"]
        .as_object()
        .expect("handlers_by_key")
        .contains_key("work"));
    assert_eq!(
        payload["handlers_by_key"]["other"],
        json!([other_entry.id.clone()])
    );

    let work = bus.emit::<WorkEvent>(TypedEvent::new(EmptyPayload {}));
    let other = bus.emit::<OtherEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(async {
        work.wait_completed().await;
        other.wait_completed().await;
    });

    assert_eq!(work.inner.inner.lock().event_results.len(), 0);
    assert_eq!(other.inner.inner.lock().event_results.len(), 1);
    assert_eq!(
        other.inner.inner.lock().event_results[&other_entry.id].result,
        Some(json!("other"))
    );
    bus.stop();
}

#[test]
fn test_on_uses_explicit_handler_name_in_json_and_log_tree() {
    let bus = EventBus::new(Some("RegistryDefinedClassNameBus".to_string()));
    let handler_name = "OriginalHandlerClass.on_DefinedClassNameEvent";

    let entry = bus.on(
        "work",
        handler_name,
        |_event| async move { Ok(json!("ok")) },
    );
    let event = bus.emit::<WorkEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());
    let payload = bus.to_json_value();
    let output = bus.log_tree();

    assert_eq!(
        payload["handlers"][&entry.id]["handler_name"],
        json!(handler_name)
    );
    assert!(output.contains(&format!("{}#", bus.name)));
    assert!(output.contains(&format!("{handler_name}#")));
    bus.stop();
}
