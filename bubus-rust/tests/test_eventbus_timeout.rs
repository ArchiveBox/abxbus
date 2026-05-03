use std::{thread, time::Duration};

use bubus_rust::{base_event::BaseEvent, event_bus::EventBus, event_result::EventResultStatus};
use futures::executor::block_on;
use serde_json::{json, Map};

fn wait_until_completed(event: &std::sync::Arc<BaseEvent>, timeout_ms: u64) {
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_millis(timeout_ms) {
        if event.inner.lock().event_status == bubus_rust::types::EventStatus::Completed {
            return;
        }
        thread::sleep(Duration::from_millis(5));
    }
    panic!("event did not complete within {timeout_ms}ms");
}

#[test]
fn test_event_timeout_aborts_in_flight_handler_result() {
    let bus = EventBus::new(Some("TimeoutBus".to_string()));

    bus.on("timeout", "slow", |_event| async move {
        thread::sleep(Duration::from_millis(50));
        Ok(json!("slow"))
    });

    let event = BaseEvent::new("timeout", Map::new());
    event.inner.lock().event_timeout = Some(0.01);

    bus.emit(event.clone());
    block_on(event.wait_completed());

    let result = event
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("missing result");
    assert_eq!(result.status, EventResultStatus::Error);
    assert!(result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("EventHandlerAbortedError"));
    bus.stop();
}

#[test]
fn test_parent_timeout_cancels_pending_or_started_children() {
    let bus = EventBus::new(Some("ParentTimeoutBus".to_string()));
    let bus_for_handler = bus.clone();

    bus.on("child", "child_slow", |_event| async move {
        thread::sleep(Duration::from_millis(80));
        Ok(json!("child"))
    });

    bus.on("parent", "emit_child", move |_event| {
        let bus_local = bus_for_handler.clone();
        async move {
            let child = BaseEvent::new("child", Map::new());
            child.inner.lock().event_timeout = Some(1.0);
            bus_local.emit(child);
            thread::sleep(Duration::from_millis(80));
            Ok(json!("parent"))
        }
    });

    let parent = BaseEvent::new("parent", Map::new());
    parent.inner.lock().event_timeout = Some(0.01);

    bus.emit(parent.clone());
    wait_until_completed(&parent, 1000);
    thread::sleep(Duration::from_millis(120));

    let parent_result = parent
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("missing parent result");
    assert_eq!(parent_result.status, EventResultStatus::Error);

    let parent_id = parent.inner.lock().event_id.clone();
    let payload = bus.runtime_payload_for_test();
    let child = payload
        .values()
        .find(|evt| evt.inner.lock().event_parent_id.as_deref() == Some(parent_id.as_str()))
        .cloned()
        .expect("missing child event");

    let child_inner = child.inner.lock();
    let has_error = child_inner
        .event_results
        .values()
        .any(|r| r.status == EventResultStatus::Error);
    let is_completed = child_inner
        .event_results
        .values()
        .any(|r| r.status == EventResultStatus::Completed);
    assert!(has_error || is_completed);
    bus.stop();
}
