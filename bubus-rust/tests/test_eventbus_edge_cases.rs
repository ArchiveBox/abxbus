use bubus_rust::{base_event::BaseEvent, event_bus::EventBus, event_result::EventResultStatus};
use futures::executor::block_on;
use serde_json::{json, Map};

#[test]
fn test_emit_with_no_handlers_completes_event() {
    let bus = EventBus::new(Some("NoHandlers".to_string()));
    let event = BaseEvent::new("nothing", Map::new());

    bus.emit_raw(event.clone());
    block_on(event.wait_completed());

    let inner = event.inner.lock();
    assert_eq!(inner.event_results.len(), 0);
    assert_eq!(inner.event_pending_bus_count, 0);
    assert!(inner.event_started_at.is_some());
    assert!(inner.event_completed_at.is_some());
    drop(inner);
    bus.stop();
}

#[test]
fn test_wildcard_handler_runs_for_any_event_type() {
    let bus = EventBus::new(Some("WildcardBus".to_string()));
    bus.on("*", "catch_all", |_event| async move { Ok(json!("all")) });
    let event = BaseEvent::new("specific_event", Map::new());

    bus.emit_raw(event.clone());
    block_on(event.wait_completed());

    let results = event.inner.lock().event_results.clone();
    assert_eq!(results.len(), 1);
    let only = results.values().next().expect("missing result");
    assert_eq!(only.result, Some(json!("all")));
    bus.stop();
}

#[test]
fn test_handler_error_populates_error_status() {
    let bus = EventBus::new(Some("ErrorBus".to_string()));
    bus.on(
        "work",
        "bad",
        |_event| async move { Err("boom".to_string()) },
    );
    let event = BaseEvent::new("work", Map::new());

    bus.emit_raw(event.clone());
    block_on(event.wait_completed());

    let results = event.inner.lock().event_results.clone();
    assert_eq!(results.len(), 1);
    let only = results.values().next().expect("missing result");
    assert_eq!(only.status, EventResultStatus::Error);
    assert_eq!(only.error.as_deref(), Some("boom"));
    bus.stop();
}
