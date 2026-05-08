use abxbus_rust::{
    event_bus::EventBus,
    event_result::EventResultStatus,
    typed::{BaseEventHandle, EventSpec},
    types::EventStatus,
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    sync::{mpsc, Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Clone, Serialize, Deserialize)]
struct EmptyPayload {}
#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}

struct NothingEvent;
impl EventSpec for NothingEvent {
    type payload = EmptyPayload;
    type event_result_type = EmptyResult;
    const event_type: &'static str = "nothing";
}
struct SpecificEvent;
impl EventSpec for SpecificEvent {
    type payload = EmptyPayload;
    type event_result_type = EmptyResult;
    const event_type: &'static str = "specific_event";
}
struct WorkEvent;
impl EventSpec for WorkEvent {
    type payload = EmptyPayload;
    type event_result_type = EmptyResult;
    const event_type: &'static str = "work";
}
#[derive(Clone, Serialize, Deserialize)]
struct ResetPayload {
    label: String,
}
struct ResetCoverageEvent;
impl EventSpec for ResetCoverageEvent {
    type payload = ResetPayload;
    type event_result_type = EmptyResult;
    const event_type: &'static str = "ResetCoverageEvent";
}

struct IdleTimeoutCoverageEvent;
impl EventSpec for IdleTimeoutCoverageEvent {
    type payload = EmptyPayload;
    type event_result_type = EmptyResult;
    const event_type: &'static str = "IdleTimeoutCoverageEvent";
}

struct StopCoverageEvent;
impl EventSpec for StopCoverageEvent {
    type payload = EmptyPayload;
    type event_result_type = EmptyResult;
    const event_type: &'static str = "StopCoverageEvent";
}

#[test]
fn test_event_reset_creates_fresh_pending_event_for_cross_bus_dispatch() {
    let bus_a = EventBus::new(Some("ResetCoverageBusA".to_string()));
    let bus_b = EventBus::new(Some("ResetCoverageBusB".to_string()));
    let seen_a = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_b = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_a_for_handler = seen_a.clone();
    let seen_b_for_handler = seen_b.clone();

    bus_a.on_raw("ResetCoverageEvent", "record_a", move |event| {
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
    bus_b.on_raw("ResetCoverageEvent", "record_b", move |event| {
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

    let completed = bus_a.emit(BaseEventHandle::<ResetCoverageEvent>::new(ResetPayload {
        label: "hello".to_string(),
    }));
    block_on(completed.wait_completed());
    assert_eq!(
        completed.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    assert_eq!(completed.inner.inner.lock().event_results.len(), 1);

    let fresh =
        BaseEventHandle::<ResetCoverageEvent>::from_base_event(completed.inner.event_reset());
    assert_ne!(
        fresh.inner.inner.lock().event_id,
        completed.inner.inner.lock().event_id
    );
    assert_eq!(fresh.inner.inner.lock().event_status, EventStatus::Pending);
    assert!(fresh.inner.inner.lock().event_started_at.is_none());
    assert!(fresh.inner.inner.lock().event_completed_at.is_none());
    assert_eq!(fresh.inner.inner.lock().event_results.len(), 0);

    let forwarded = bus_b.emit(fresh);
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
fn test_wait_until_idle_timeout_path_recovers_after_inflight_handler_finishes() {
    let bus = EventBus::new(Some("IdleTimeoutCoverageBus".to_string()));
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));

    bus.on_raw("IdleTimeoutCoverageEvent", "slow_handler", move |_event| {
        let started_tx = started_tx.clone();
        let release_rx = release_rx.clone();
        async move {
            let _ = started_tx.send(());
            release_rx
                .lock()
                .expect("release lock")
                .recv_timeout(Duration::from_secs(2))
                .expect("release handler");
            Ok(json!(null))
        }
    });

    let pending = bus.emit(BaseEventHandle::<IdleTimeoutCoverageEvent>::new(
        EmptyPayload {},
    ));
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("handler should start");

    let start = Instant::now();
    let idle = block_on(bus.wait_until_idle(Some(0.01)));
    let elapsed = start.elapsed();
    assert!(!idle);
    assert!(elapsed < Duration::from_millis(500));
    assert_ne!(
        pending.inner.inner.lock().event_status,
        EventStatus::Completed
    );

    release_tx.send(()).expect("release handler");
    block_on(pending.wait_completed());
    assert!(block_on(bus.wait_until_idle(Some(1.0))));
    assert_eq!(
        pending.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    bus.stop();
}

#[test]
fn test_stop_timeout_zero_clears_running_bus_and_releases_name() {
    let bus_name = "StopCoverageBus".to_string();
    let bus = EventBus::new(Some(bus_name.clone()));
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));

    bus.on_raw("StopCoverageEvent", "slow_handler", move |_event| {
        let started_tx = started_tx.clone();
        let release_rx = release_rx.clone();
        async move {
            let _ = started_tx.send(());
            let _ = release_rx
                .lock()
                .expect("release lock")
                .recv_timeout(Duration::from_millis(200));
            Ok(json!(null))
        }
    });

    let _pending = bus.emit(BaseEventHandle::<StopCoverageEvent>::new(EmptyPayload {}));
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("handler should start");

    let start = Instant::now();
    bus.stop();
    let elapsed = start.elapsed();
    assert!(elapsed < Duration::from_millis(500));
    assert!(bus.is_stopped_for_test());
    assert!(!EventBus::all_instances_contains(&bus));

    release_tx.send(()).expect("release handler");

    let replacement = EventBus::new(Some(bus_name));
    replacement.on_raw("StopCoverageEvent", "handler", |_event| async move {
        Ok(json!(null))
    });
    let event = replacement.emit(BaseEventHandle::<StopCoverageEvent>::new(EmptyPayload {}));
    block_on(event.wait_completed());
    assert_eq!(
        event.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    replacement.stop();
}

#[test]
fn test_emit_with_no_handlers_completes_event() {
    let bus = EventBus::new(Some("NoHandlers".to_string()));
    let event = bus.emit(BaseEventHandle::<NothingEvent>::new(EmptyPayload {}));

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
    bus.on_raw("*", "catch_all", |_event| async move { Ok(json!("all")) });
    let event = bus.emit(BaseEventHandle::<SpecificEvent>::new(EmptyPayload {}));

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
    bus.on_raw(
        "work",
        "bad",
        |_event| async move { Err("boom".to_string()) },
    );
    let event = bus.emit(BaseEventHandle::<WorkEvent>::new(EmptyPayload {}));

    block_on(event.wait_completed());

    let results = event.inner.inner.lock().event_results.clone();
    assert_eq!(results.len(), 1);
    let only = results.values().next().expect("missing result");
    assert_eq!(only.status, EventResultStatus::Error);
    assert_eq!(only.error.as_deref(), Some("boom"));
    bus.stop();
}
