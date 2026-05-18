use abxbus::{
    base_event::{BaseEvent, EventResultOptions},
    event,
    event_bus::EventBus,
    event_handler::EventHandlerOptions,
    event_result::EventResultStatus,
};
use futures::executor::block_on;
use serde_json::{json, Value};

event! {
    struct WorkEvent {
        event_type: "work",
    }
}

event! {
    struct RegistryTypingEvent {
        required_token: String,
        event_result_type: String,
    }
}

event! {
    struct OtherEvent {
        event_type: "other",
    }
}

#[test]
fn test_on_stores_eventhandler_entry_and_index() {
    let bus = EventBus::new(Some("RegistryBus".to_string()));

    let entry = bus.on_raw("work", "handler", |_event| async move { Ok(json!("work")) });
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

    let dispatched = bus.emit(WorkEvent {
        ..Default::default()
    });
    let _ = block_on(dispatched.now());
    let results = dispatched.event_results.read();
    assert!(results.contains_key(&entry.id));
    assert_eq!(results[&entry.id].handler.id, entry.id);
    bus.destroy();
}

#[test]
fn test_on_returns_handler_and_off_removes_handler() {
    let bus = EventBus::new(Some("OnOffBus".to_string()));

    let handler = bus.on_raw("work", "h1", |_event| async move { Ok(json!("ok")) });
    let event_1 = bus.emit(WorkEvent {
        ..Default::default()
    });
    let _ = block_on(event_1.now());
    assert_eq!(event_1.event_results.read().len(), 1);

    bus.off("work", Some(&handler.id));
    let event_2 = bus.emit(WorkEvent {
        ..Default::default()
    });
    let _ = block_on(event_2.now());
    assert_eq!(event_2.event_results.read().len(), 0);

    bus.destroy();
}

#[test]
fn test_off_removes_handler_id_or_all_and_prunes_empty_index() {
    let bus = EventBus::new(Some("RegistryOffBus".to_string()));

    let entry_a = bus.on_raw("work", "handler_a", |_event| async move { Ok(Value::Null) });
    let entry_b = bus.on_raw("work", "handler_b", |_event| async move { Ok(Value::Null) });
    let entry_c = bus.on_raw("work", "handler_c", |_event| async move { Ok(Value::Null) });

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

    bus.on_raw("work", "handler_a", |_event| async move { Ok(Value::Null) });
    bus.on_raw("work", "handler_b", |_event| async move { Ok(Value::Null) });
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

    let dispatched = bus.emit(WorkEvent {
        ..Default::default()
    });
    let _ = block_on(dispatched.now());
    assert_eq!(dispatched.event_results.read().len(), 0);
    bus.destroy();
}

#[test]
fn test_on_with_options_supports_id_override_and_handler_file_path() {
    let bus = EventBus::new(Some("RegistryOptionsBus".to_string()));
    let explicit_id = "018f8e40-1234-7000-8000-000000009999".to_string();

    let entry = bus.on_raw_with_options(
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
    bus.destroy();
}

#[test]
fn test_on_accepts_handlers_and_dispatch_captures_return_values() {
    let bus = EventBus::new(Some("RegistryNormalizeBus".to_string()));
    let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let calls_for_handler = calls.clone();

    let entry = bus.on_raw_sync("work", "sync_handler", move |event| {
        calls_for_handler
            .lock()
            .expect("calls lock")
            .push(event.inner.lock().event_id.clone());
        Ok(json!("normalized"))
    });

    let dispatched = bus.emit(WorkEvent {
        ..Default::default()
    });
    let _ = block_on(dispatched.now());
    let result = dispatched._inner_event().inner.lock().event_results[&entry.id].clone();

    assert_eq!(result.status, EventResultStatus::Completed);
    assert_eq!(result.result, Some(json!("normalized")));
    assert_eq!(calls.lock().expect("calls lock").len(), 1);
    bus.destroy();
}

#[test]
fn test_on_normalizes_sync_handler_to_async_callable() {
    let bus = EventBus::new(Some("RegistryNormalizeSyncBus".to_string()));
    let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let calls_for_handler = calls.clone();

    let entry = bus.on_raw_sync("work", "sync_handler", move |event| {
        calls_for_handler
            .lock()
            .expect("calls lock")
            .push(event.inner.lock().event_id.clone());
        Ok(json!("normalized"))
    });

    let direct_event = BaseEvent::new("work", Default::default());
    let direct_result = block_on((entry.callable.as_ref().expect("callable"))(
        direct_event.clone(),
    ))
    .expect("direct sync wrapper");
    assert_eq!(direct_result, json!("normalized"));

    let dispatched = bus.emit(WorkEvent {
        ..Default::default()
    });
    let _ = block_on(dispatched.now());
    let result = dispatched._inner_event().inner.lock().event_results[&entry.id].clone();

    assert_eq!(result.status, EventResultStatus::Completed);
    assert_eq!(result.result, Some(json!("normalized")));
    assert_eq!(calls.lock().expect("calls lock").len(), 2);
    bus.destroy();
}

#[test]
fn test_on_keeps_async_handlers_normalized_through_handler_async() {
    let bus = EventBus::new(Some("RegistryAsyncNormalizeBus".to_string()));
    let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let calls_for_handler = calls.clone();

    let entry = bus.on_raw("work", "async_handler", move |event| {
        let calls = calls_for_handler.clone();
        async move {
            calls
                .lock()
                .expect("calls lock")
                .push(event.inner.lock().event_id.clone());
            Ok(json!("async_normalized"))
        }
    });

    let direct_event = BaseEvent::new("work", Default::default());
    let direct_result = block_on((entry.callable.as_ref().expect("callable"))(direct_event))
        .expect("direct async handler");
    assert_eq!(direct_result, json!("async_normalized"));

    let dispatched = bus.emit(WorkEvent {
        ..Default::default()
    });
    let _ = block_on(dispatched.now());
    let result = dispatched._inner_event().inner.lock().event_results[&entry.id].clone();

    assert_eq!(result.status, EventResultStatus::Completed);
    assert_eq!(result.result, Some(json!("async_normalized")));
    assert_eq!(calls.lock().expect("calls lock").len(), 2);
    bus.destroy();
}

#[test]
fn test_handler_async_preserves_typed_arg_return_contracts_for_sync_handlers() {
    let bus = EventBus::new(Some("RegistryTypingSyncBus".to_string()));

    let entry = bus.on(
        RegistryTypingEvent,
        |event: RegistryTypingEvent| async move { Ok(event.required_token.clone()) },
    );

    let event = bus.emit(RegistryTypingEvent {
        required_token: "sync".to_string(),
        ..Default::default()
    });
    let _ = block_on(event.now());

    let result = event._inner_event().inner.lock().event_results[&entry.id].clone();
    assert_eq!(result.status, EventResultStatus::Completed);
    assert_eq!(result.result, Some(json!("sync")));
    assert_eq!(
        block_on(event.event_result_with_options(EventResultOptions::default()))
            .expect("first result")
            .as_deref(),
        Some("sync")
    );
    bus.destroy();
}

#[test]
fn test_handler_async_preserves_typed_arg_return_contracts_for_async_handlers() {
    let bus = EventBus::new(Some("RegistryTypingSyncBus".to_string()));

    let entry = bus.on(
        RegistryTypingEvent,
        |event: RegistryTypingEvent| async move { Ok(event.required_token.clone()) },
    );

    let event = bus.emit(RegistryTypingEvent {
        required_token: "sync".to_string(),
        ..Default::default()
    });
    let _ = block_on(event.now());

    let result = event._inner_event().inner.lock().event_results[&entry.id].clone();
    assert_eq!(result.status, EventResultStatus::Completed);
    assert_eq!(result.result, Some(json!("sync")));
    assert_eq!(
        block_on(event.event_result_with_options(EventResultOptions::default()))
            .expect("first result")
            .as_deref(),
        Some("sync")
    );
    bus.destroy();
}

#[test]
fn test_off_removing_all_for_one_event_key_preserves_other_event_keys() {
    let bus = EventBus::new(Some("RegistryOffSelectiveBus".to_string()));

    bus.on_raw(
        "work",
        "work_handler",
        |_event| async move { Ok(json!("work")) },
    );
    let other_entry = bus.on_raw("other", "other_handler", |_event| async move {
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

    let work = bus.emit(WorkEvent {
        ..Default::default()
    });
    let other = bus.emit(OtherEvent {
        ..Default::default()
    });
    block_on(async {
        let _ = work.now().await;
        let _ = other.now().await;
    });

    assert_eq!(work.event_results.read().len(), 0);
    assert_eq!(other.event_results.read().len(), 1);
    assert_eq!(
        other._inner_event().inner.lock().event_results[&other_entry.id].result,
        Some(json!("other"))
    );
    bus.destroy();
}

#[test]
fn test_on_uses_explicit_handler_name_in_json_and_log_tree() {
    let bus = EventBus::new(Some("RegistryDefinedClassNameBus".to_string()));
    let handler_name = "OriginalHandlerClass.on_DefinedClassNameEvent";

    let entry = bus.on_raw(
        "work",
        handler_name,
        |_event| async move { Ok(json!("ok")) },
    );
    let event = bus.emit(WorkEvent {
        ..Default::default()
    });
    let _ = block_on(event.now());
    let payload = bus.to_json_value();
    let output = bus.log_tree();

    assert_eq!(
        payload["handlers"][&entry.id]["handler_name"],
        json!(handler_name)
    );
    assert!(output.contains(&format!("{}#", bus.name)));
    assert!(output.contains(&format!("{handler_name}#")));
    bus.destroy();
}
