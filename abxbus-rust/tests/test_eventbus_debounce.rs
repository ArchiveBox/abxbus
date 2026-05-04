use std::{
    sync::{mpsc, Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use abxbus_rust::{
    event_bus::{EventBus, FindOptions},
    typed::{EventSpec, TypedEvent},
    types::EventStatus,
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Serialize, Deserialize)]
struct EmptyPayload {}
#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}

struct ParentEvent;
impl EventSpec for ParentEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "ParentEvent";
}

#[derive(Clone, Serialize, Deserialize)]
struct ScreenshotPayload {
    target_id: String,
}
struct ScreenshotEvent;
impl EventSpec for ScreenshotEvent {
    type Payload = ScreenshotPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "ScreenshotEvent";
}

struct SyncEvent;
impl EventSpec for SyncEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "SyncEvent";
}

const TARGET_ID_1: &str = "9b447756-908c-7b75-8a51-4a2c2b4d9b14";
const TARGET_ID_2: &str = "194870e1-fa02-70a4-8101-d10d57c3449c";

fn target_filter(target_id: &str) -> std::collections::HashMap<String, Value> {
    let mut filter = std::collections::HashMap::new();
    filter.insert("target_id".to_string(), json!(target_id));
    filter
}

fn wait_for_string(slot: &Arc<Mutex<Option<String>>>) -> String {
    let start = Instant::now();
    loop {
        if let Some(value) = slot.lock().expect("slot lock").clone() {
            return value;
        }
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "timed out waiting for value"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn test_simple_debounce_with_child_of_reuses_recent_event() {
    let bus = EventBus::new(Some("DebounceBus".to_string()));
    let bus_for_parent = bus.clone();
    let child_id = Arc::new(Mutex::new(None::<String>));
    let child_id_for_parent = child_id.clone();

    bus.on("ParentEvent", "emit_screenshot", move |_event| {
        let bus = bus_for_parent.clone();
        let child_id = child_id_for_parent.clone();
        async move {
            let child = bus.emit_child::<ScreenshotEvent>(TypedEvent::new(ScreenshotPayload {
                target_id: TARGET_ID_1.to_string(),
            }));
            *child_id.lock().expect("child id lock") =
                Some(child.inner.inner.lock().event_id.clone());
            child.wait_completed().await;
            Ok(json!("parent_done"))
        }
    });
    bus.on(
        "ScreenshotEvent",
        "complete_screenshot",
        |_event| async move { Ok(json!("screenshot_done")) },
    );

    let parent = bus.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(parent.wait_completed());
    let emitted_child_id = wait_for_string(&child_id);

    let reused = block_on(bus.find_with_options(
        "ScreenshotEvent",
        FindOptions {
            past: true,
            past_window: Some(10.0),
            future: None,
            child_of: Some(parent.inner.clone()),
            where_filter: None,
            where_predicate: None,
        },
    ))
    .unwrap_or_else(|| {
        bus.emit::<ScreenshotEvent>(TypedEvent::new(ScreenshotPayload {
            target_id: TARGET_ID_2.to_string(),
        }))
        .inner
    });
    block_on(reused.event_completed());

    assert_eq!(reused.inner.lock().event_id, emitted_child_id);
    assert_eq!(
        reused.inner.lock().event_parent_id.as_deref(),
        Some(parent.inner.inner.lock().event_id.as_str())
    );
    bus.stop();
}

#[test]
fn test_advanced_debounce_prefers_history_then_waits_future_then_dispatches() {
    let bus = EventBus::new(Some("AdvancedDebounceBus".to_string()));
    let bus_for_find = bus.clone();
    let bus_for_emit = bus.clone();
    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
        let found = block_on(bus_for_find.find("SyncEvent", false, Some(0.5), None));
        tx.send(found).expect("send future find result");
    });

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        bus_for_emit.emit::<SyncEvent>(TypedEvent::new(EmptyPayload {}));
    });

    let resolved = block_on(bus.find("SyncEvent", true, None, None)).or_else(|| {
        rx.recv_timeout(Duration::from_secs(1))
            .expect("future find")
    });
    let resolved = resolved.unwrap_or_else(|| {
        bus.emit::<SyncEvent>(TypedEvent::new(EmptyPayload {}))
            .inner
    });
    block_on(resolved.event_completed());

    assert_eq!(resolved.inner.lock().event_type, "SyncEvent");
    bus.stop();
}

#[test]
fn test_returns_existing_fresh_event() {
    let bus = EventBus::new(Some("DebounceFreshBus".to_string()));
    bus.on("ScreenshotEvent", "complete", |_event| async move {
        Ok(json!("done"))
    });

    let original = bus.emit::<ScreenshotEvent>(TypedEvent::new(ScreenshotPayload {
        target_id: TARGET_ID_1.to_string(),
    }));
    block_on(original.wait_completed());

    let found = block_on(bus.find_with_options(
        "ScreenshotEvent",
        FindOptions {
            past: true,
            past_window: Some(5.0),
            where_filter: Some(target_filter(TARGET_ID_1)),
            ..FindOptions::default()
        },
    ))
    .unwrap_or_else(|| {
        bus.emit::<ScreenshotEvent>(TypedEvent::new(ScreenshotPayload {
            target_id: TARGET_ID_1.to_string(),
        }))
        .inner
    });
    block_on(found.event_completed());

    let found_id = found.inner.lock().event_id.clone();
    let original_id = original.inner.inner.lock().event_id.clone();
    assert_eq!(found_id, original_id);
    bus.stop();
}

#[test]
fn test_dispatches_new_when_no_match() {
    let bus = EventBus::new(Some("DebounceNoMatchBus".to_string()));
    bus.on("ScreenshotEvent", "complete", |_event| async move {
        Ok(json!("done"))
    });

    let result = block_on(bus.find_with_options(
        "ScreenshotEvent",
        FindOptions {
            past: true,
            where_filter: Some(target_filter(TARGET_ID_1)),
            ..FindOptions::default()
        },
    ))
    .unwrap_or_else(|| {
        bus.emit::<ScreenshotEvent>(TypedEvent::new(ScreenshotPayload {
            target_id: TARGET_ID_1.to_string(),
        }))
        .inner
    });
    block_on(result.event_completed());

    assert_eq!(
        result.inner.lock().payload.get("target_id"),
        Some(&json!(TARGET_ID_1))
    );
    assert_eq!(result.inner.lock().event_status, EventStatus::Completed);
    bus.stop();
}

#[test]
fn test_dispatches_new_when_stale() {
    let bus = EventBus::new(Some("DebounceStaleBus".to_string()));
    bus.on("ScreenshotEvent", "complete", |_event| async move {
        Ok(json!("done"))
    });

    let original = bus.emit::<ScreenshotEvent>(TypedEvent::new(ScreenshotPayload {
        target_id: TARGET_ID_1.to_string(),
    }));
    block_on(original.wait_completed());

    let result = block_on(bus.find_with_options(
        "ScreenshotEvent",
        FindOptions {
            past: true,
            where_filter: Some(target_filter(TARGET_ID_2)),
            ..FindOptions::default()
        },
    ))
    .unwrap_or_else(|| {
        bus.emit::<ScreenshotEvent>(TypedEvent::new(ScreenshotPayload {
            target_id: TARGET_ID_1.to_string(),
        }))
        .inner
    });
    block_on(result.event_completed());

    let screenshots = bus
        .runtime_payload_for_test()
        .values()
        .filter(|event| event.inner.lock().event_type == "ScreenshotEvent")
        .count();
    assert_eq!(screenshots, 2);
    bus.stop();
}

#[test]
fn test_find_past_only_returns_immediately_without_waiting() {
    let bus = EventBus::new(Some("DebouncePastOnlyBus".to_string()));

    let start = Instant::now();
    let result = block_on(bus.find("ParentEvent", true, None, None));
    let elapsed = start.elapsed();

    assert!(result.is_none());
    assert!(elapsed < Duration::from_millis(50));
    bus.stop();
}

#[test]
fn test_find_past_float_returns_immediately_without_waiting() {
    let bus = EventBus::new(Some("DebouncePastWindowBus".to_string()));

    let start = Instant::now();
    let result = block_on(bus.find_with_options(
        "ParentEvent",
        FindOptions {
            past: true,
            past_window: Some(5.0),
            future: None,
            ..FindOptions::default()
        },
    ));
    let elapsed = start.elapsed();

    assert!(result.is_none());
    assert!(elapsed < Duration::from_millis(50));
    bus.stop();
}

#[test]
fn test_or_chain_without_waiting_finds_existing() {
    let bus = EventBus::new(Some("DebounceOrChainExistingBus".to_string()));
    bus.on("ScreenshotEvent", "complete", |_event| async move {
        Ok(json!("done"))
    });

    let original = bus.emit::<ScreenshotEvent>(TypedEvent::new(ScreenshotPayload {
        target_id: TARGET_ID_1.to_string(),
    }));
    block_on(original.wait_completed());

    let start = Instant::now();
    let result = block_on(bus.find_with_options(
        "ScreenshotEvent",
        FindOptions {
            past: true,
            future: None,
            where_filter: Some(target_filter(TARGET_ID_1)),
            ..FindOptions::default()
        },
    ))
    .unwrap_or_else(|| {
        bus.emit::<ScreenshotEvent>(TypedEvent::new(ScreenshotPayload {
            target_id: TARGET_ID_1.to_string(),
        }))
        .inner
    });
    block_on(result.event_completed());
    let elapsed = start.elapsed();

    let result_id = result.inner.lock().event_id.clone();
    let original_id = original.inner.inner.lock().event_id.clone();
    assert_eq!(result_id, original_id);
    assert!(elapsed < Duration::from_millis(100));
    bus.stop();
}

#[test]
fn test_or_chain_without_waiting_dispatches_when_no_match() {
    let bus = EventBus::new(Some("DebounceOrChainNoMatchBus".to_string()));
    bus.on("ScreenshotEvent", "complete", |_event| async move {
        Ok(json!("done"))
    });

    let start = Instant::now();
    let result = block_on(bus.find_with_options(
        "ScreenshotEvent",
        FindOptions {
            past: true,
            future: None,
            where_filter: Some(target_filter(TARGET_ID_1)),
            ..FindOptions::default()
        },
    ))
    .unwrap_or_else(|| {
        bus.emit::<ScreenshotEvent>(TypedEvent::new(ScreenshotPayload {
            target_id: TARGET_ID_1.to_string(),
        }))
        .inner
    });
    block_on(result.event_completed());
    let elapsed = start.elapsed();

    assert_eq!(
        result.inner.lock().payload.get("target_id"),
        Some(&json!(TARGET_ID_1))
    );
    assert!(elapsed < Duration::from_millis(100));
    bus.stop();
}

#[test]
fn test_or_chain_multiple_sequential_lookups() {
    let bus = EventBus::new(Some("DebounceSequentialBus".to_string()));
    bus.on("ScreenshotEvent", "complete", |_event| async move {
        Ok(json!("done"))
    });

    let start = Instant::now();
    let result_1 = block_on(bus.find_with_options(
        "ScreenshotEvent",
        FindOptions {
            past: true,
            where_filter: Some(target_filter(TARGET_ID_1)),
            ..FindOptions::default()
        },
    ))
    .unwrap_or_else(|| {
        bus.emit::<ScreenshotEvent>(TypedEvent::new(ScreenshotPayload {
            target_id: TARGET_ID_1.to_string(),
        }))
        .inner
    });
    let result_2 = block_on(bus.find_with_options(
        "ScreenshotEvent",
        FindOptions {
            past: true,
            where_filter: Some(target_filter(TARGET_ID_1)),
            ..FindOptions::default()
        },
    ))
    .unwrap_or_else(|| {
        bus.emit::<ScreenshotEvent>(TypedEvent::new(ScreenshotPayload {
            target_id: TARGET_ID_1.to_string(),
        }))
        .inner
    });
    let result_3 = block_on(bus.find_with_options(
        "ScreenshotEvent",
        FindOptions {
            past: true,
            where_filter: Some(target_filter(TARGET_ID_2)),
            ..FindOptions::default()
        },
    ))
    .unwrap_or_else(|| {
        bus.emit::<ScreenshotEvent>(TypedEvent::new(ScreenshotPayload {
            target_id: TARGET_ID_2.to_string(),
        }))
        .inner
    });

    block_on(result_1.event_completed());
    block_on(result_2.event_completed());
    block_on(result_3.event_completed());

    assert!(start.elapsed() < Duration::from_millis(200));
    let result_1_id = result_1.inner.lock().event_id.clone();
    let result_2_id = result_2.inner.lock().event_id.clone();
    let result_3_id = result_3.inner.lock().event_id.clone();
    assert_eq!(result_1_id, result_2_id);
    assert_ne!(result_1_id, result_3_id);
    assert_eq!(
        result_3.inner.lock().payload.get("target_id"),
        Some(&json!(TARGET_ID_2))
    );
    bus.stop();
}
