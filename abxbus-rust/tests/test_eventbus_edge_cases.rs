use abxbus_rust::event;
use abxbus_rust::{
    event_bus::EventBus, event_result::EventResultStatus, typed::BaseEventHandle,
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
struct EmptyResult {}

event! {
    struct NothingEvent {
        event_result_type: EmptyResult,
        event_type: "nothing",
    }
}
event! {
    struct SpecificEvent {
        event_result_type: EmptyResult,
        event_type: "specific_event",
    }
}
event! {
    struct WorkEvent {
        event_result_type: EmptyResult,
        event_type: "work",
    }
}
event! {
    struct ResetCoverageEvent {
        label: String,
        event_result_type: EmptyResult,
        event_type: "ResetCoverageEvent",
    }
}
event! {
    struct IdleTimeoutCoverageEvent {
        event_result_type: EmptyResult,
        event_type: "IdleTimeoutCoverageEvent",
    }
}
event! {
    struct DestroyCoverageEvent {
        event_result_type: EmptyResult,
        event_type: "DestroyCoverageEvent",
    }
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

    let completed = bus_a.emit(ResetCoverageEvent {
        label: "hello".to_string(),
        ..Default::default()
    });
    let _ = block_on(completed.now());
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
    let _ = block_on(forwarded.now());

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
    bus_a.destroy();
    bus_b.destroy();
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

    let pending = bus.emit(IdleTimeoutCoverageEvent {
        ..Default::default()
    });
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
    let _ = block_on(pending.now());
    assert!(block_on(bus.wait_until_idle(Some(1.0))));
    assert_eq!(
        pending.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    bus.destroy();
}

#[test]
fn test_destroy_timeout_zero_clears_running_bus_and_releases_name() {
    let bus_name = "DestroyCoverageBus".to_string();
    let bus = EventBus::new(Some(bus_name.clone()));
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));

    bus.on_raw("DestroyCoverageEvent", "slow_handler", move |_event| {
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

    let _pending = bus.emit(DestroyCoverageEvent {
        ..Default::default()
    });
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("handler should start");

    let start = Instant::now();
    bus.destroy();
    let elapsed = start.elapsed();
    assert!(elapsed < Duration::from_millis(500));
    assert!(bus.is_destroyed_for_test());
    assert!(!EventBus::all_instances_contains(&bus));

    release_tx.send(()).expect("release handler");

    let replacement = EventBus::new(Some(bus_name));
    replacement.on_raw("DestroyCoverageEvent", "handler", |_event| async move {
        Ok(json!(null))
    });
    let event = replacement.emit(DestroyCoverageEvent {
        ..Default::default()
    });
    let _ = block_on(event.now());
    assert_eq!(
        event.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    replacement.destroy();
}

#[test]
fn test_emit_with_no_handlers_completes_event() {
    let bus = EventBus::new(Some("NoHandlers".to_string()));
    let event = bus.emit(NothingEvent {
        ..Default::default()
    });

    let _ = block_on(event.now());

    let inner = event.inner.inner.lock();
    assert_eq!(inner.event_results.len(), 0);
    assert_eq!(inner.event_pending_bus_count, 0);
    assert!(inner.event_started_at.is_some());
    assert!(inner.event_completed_at.is_some());
    drop(inner);
    bus.destroy();
}

#[test]
fn test_wildcard_handler_runs_for_any_event_type() {
    let bus = EventBus::new(Some("WildcardBus".to_string()));
    bus.on_raw("*", "catch_all", |_event| async move { Ok(json!("all")) });
    let event = bus.emit(SpecificEvent {
        ..Default::default()
    });

    let _ = block_on(event.now());

    let results = event.inner.inner.lock().event_results.clone();
    assert_eq!(results.len(), 1);
    let only = results.values().next().expect("missing result");
    assert_eq!(only.result, Some(json!("all")));
    bus.destroy();
}

#[test]
fn test_handler_error_populates_error_status() {
    let bus = EventBus::new(Some("ErrorBus".to_string()));
    bus.on_raw(
        "work",
        "bad",
        |_event| async move { Err("boom".to_string()) },
    );
    let event = bus.emit(WorkEvent {
        ..Default::default()
    });

    let _ = block_on(event.now());

    let results = event.inner.inner.lock().event_results.clone();
    assert_eq!(results.len(), 1);
    let only = results.values().next().expect("missing result");
    assert_eq!(only.status, EventResultStatus::Error);
    assert_eq!(only.error.as_deref(), Some("boom"));
    bus.destroy();
}
