use abxbus_rust::{
    event_bus::EventBus,
    event_result::EventResultStatus,
    typed::{EventSpec, TypedEvent},
    types::EventStatus,
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::{Arc, Mutex};

#[derive(Clone, Serialize, Deserialize)]
struct EmptyPayload {}
#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}

struct NothingEvent;
impl EventSpec for NothingEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "nothing";
}
struct SpecificEvent;
impl EventSpec for SpecificEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "specific_event";
}
struct WorkEvent;
impl EventSpec for WorkEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "work";
}
#[derive(Clone, Serialize, Deserialize)]
struct ResetPayload {
    label: String,
}
struct ResetCoverageEvent;
impl EventSpec for ResetCoverageEvent {
    type Payload = ResetPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "ResetCoverageEvent";
}

#[test]
fn test_event_reset_creates_fresh_pending_event_for_cross_bus_dispatch() {
    let bus_a = EventBus::new(Some("ResetCoverageBusA".to_string()));
    let bus_b = EventBus::new(Some("ResetCoverageBusB".to_string()));
    let seen_a = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_b = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_a_for_handler = seen_a.clone();
    let seen_b_for_handler = seen_b.clone();

    bus_a.on("ResetCoverageEvent", "record_a", move |event| {
        let seen_a = seen_a_for_handler.clone();
        async move {
            let label = event
                .inner
                .lock()
                .payload
                .get("label")
                .and_then(|value| value.as_str())
                .expect("label")
                .to_string();
            seen_a.lock().expect("seen_a lock").push(label);
            Ok(json!(null))
        }
    });
    bus_b.on("ResetCoverageEvent", "record_b", move |event| {
        let seen_b = seen_b_for_handler.clone();
        async move {
            let label = event
                .inner
                .lock()
                .payload
                .get("label")
                .and_then(|value| value.as_str())
                .expect("label")
                .to_string();
            seen_b.lock().expect("seen_b lock").push(label);
            Ok(json!(null))
        }
    });

    let completed = bus_a.emit::<ResetCoverageEvent>(TypedEvent::new(ResetPayload {
        label: "hello".to_string(),
    }));
    block_on(completed.wait_completed());
    assert_eq!(
        completed.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    assert_eq!(completed.inner.inner.lock().event_results.len(), 1);

    let fresh = TypedEvent::<ResetCoverageEvent>::from_base_event(completed.inner.event_reset());
    assert_ne!(
        fresh.inner.inner.lock().event_id,
        completed.inner.inner.lock().event_id
    );
    assert_eq!(fresh.inner.inner.lock().event_status, EventStatus::Pending);
    assert!(fresh.inner.inner.lock().event_started_at.is_none());
    assert!(fresh.inner.inner.lock().event_completed_at.is_none());
    assert_eq!(fresh.inner.inner.lock().event_results.len(), 0);

    let forwarded = bus_b.emit::<ResetCoverageEvent>(fresh);
    block_on(forwarded.wait_completed());

    assert_eq!(
        seen_a.lock().expect("seen_a lock").as_slice(),
        &["hello".to_string()]
    );
    assert_eq!(
        seen_b.lock().expect("seen_b lock").as_slice(),
        &["hello".to_string()]
    );
    let event_path = forwarded.inner.inner.lock().event_path.clone();
    assert!(event_path
        .iter()
        .any(|path| path.starts_with("ResetCoverageBusA#")));
    assert!(event_path
        .iter()
        .any(|path| path.starts_with("ResetCoverageBusB#")));
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_emit_with_no_handlers_completes_event() {
    let bus = EventBus::new(Some("NoHandlers".to_string()));
    let event = bus.emit::<NothingEvent>(TypedEvent::<NothingEvent>::new(EmptyPayload {}));

    block_on(event.wait_completed());

    let inner = event.inner.inner.lock();
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
    let event = bus.emit::<SpecificEvent>(TypedEvent::<SpecificEvent>::new(EmptyPayload {}));

    block_on(event.wait_completed());

    let results = event.inner.inner.lock().event_results.clone();
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
    let event = bus.emit::<WorkEvent>(TypedEvent::<WorkEvent>::new(EmptyPayload {}));

    block_on(event.wait_completed());

    let results = event.inner.inner.lock().event_results.clone();
    assert_eq!(results.len(), 1);
    let only = results.values().next().expect("missing result");
    assert_eq!(only.status, EventResultStatus::Error);
    assert_eq!(only.error.as_deref(), Some("boom"));
    bus.stop();
}
