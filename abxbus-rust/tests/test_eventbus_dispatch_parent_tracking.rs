use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use abxbus_rust::{
    base_event::BaseEvent,
    event_bus::EventBus,
    typed::{EventSpec, TypedEvent},
    types::EventStatus,
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;

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

struct ChildEvent;
impl EventSpec for ChildEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "ChildEvent";
}

struct GrandchildEvent;
impl EventSpec for GrandchildEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "GrandchildEvent";
}

#[test]
fn test_basic_parent_tracking_child_events_get_event_parent_id() {
    let bus = EventBus::new(Some("ParentTrackingBus".to_string()));
    let bus_for_handler = bus.clone();

    bus.on("ParentEvent", "parent_handler", move |_event| {
        let bus = bus_for_handler.clone();
        async move {
            bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            Ok(json!("parent"))
        }
    });
    bus.on("ChildEvent", "child_handler", |_event| async move {
        Ok(json!("child"))
    });

    let parent = bus.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(bus.wait_until_idle(None));

    let parent_id = parent.inner.inner.lock().event_id.clone();
    let payload = bus.runtime_payload_for_test();
    let child = payload
        .values()
        .find(|event| event.inner.lock().event_type == "ChildEvent")
        .cloned()
        .expect("child event");
    assert_eq!(
        child.inner.lock().event_parent_id.as_deref(),
        Some(parent_id.as_str())
    );
    assert!(bus.event_is_child_of(&child, &parent.inner));
    assert!(bus.event_is_parent_of(&parent.inner, &child));
    bus.stop();
}

#[test]
fn test_multi_level_parent_tracking_preserves_lineage() {
    let bus = EventBus::new(Some("MultiLevelParentTrackingBus".to_string()));
    let bus_for_parent = bus.clone();
    let bus_for_child = bus.clone();

    bus.on("ParentEvent", "parent_handler", move |_event| {
        let bus = bus_for_parent.clone();
        async move {
            bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            Ok(json!("parent"))
        }
    });
    bus.on("ChildEvent", "child_handler", move |_event| {
        let bus = bus_for_child.clone();
        async move {
            bus.emit_child::<GrandchildEvent>(TypedEvent::new(EmptyPayload {}));
            Ok(json!("child"))
        }
    });
    bus.on(
        "GrandchildEvent",
        "grandchild_handler",
        |_event| async move { Ok(json!("grandchild")) },
    );

    let parent = bus.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(bus.wait_until_idle(None));

    let payload = bus.runtime_payload_for_test();
    let child = payload
        .values()
        .find(|event| event.inner.lock().event_type == "ChildEvent")
        .cloned()
        .expect("child event");
    let grandchild = payload
        .values()
        .find(|event| event.inner.lock().event_type == "GrandchildEvent")
        .cloned()
        .expect("grandchild event");
    let parent_id = parent.inner.inner.lock().event_id.clone();
    let child_id = child.inner.lock().event_id.clone();
    assert_eq!(
        child.inner.lock().event_parent_id.as_deref(),
        Some(parent_id.as_str())
    );
    assert_eq!(
        grandchild.inner.lock().event_parent_id.as_deref(),
        Some(child_id.as_str())
    );
    assert!(bus.event_is_child_of(&grandchild, &parent.inner));
    bus.stop();
}

#[test]
fn test_event_children_tracks_multiple_children_from_a_single_handler() {
    let bus = EventBus::new(Some("EventChildrenBus".to_string()));
    let bus_for_handler = bus.clone();

    let parent_handler = bus.on("ParentEvent", "parent_handler", move |_event| {
        let bus = bus_for_handler.clone();
        async move {
            bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            bus.emit_child::<GrandchildEvent>(TypedEvent::new(EmptyPayload {}));
            Ok(json!("parent"))
        }
    });
    bus.on("ChildEvent", "child_handler", |_event| async move {
        Ok(json!("child"))
    });
    bus.on(
        "GrandchildEvent",
        "grandchild_handler",
        |_event| async move { Ok(json!("grandchild")) },
    );

    let parent = bus.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(bus.wait_until_idle(None));

    let event_children = {
        let parent_inner = parent.inner.inner.lock();
        parent_inner
            .event_results
            .get(&parent_handler.id)
            .expect("parent result")
            .event_children
            .clone()
    };
    assert_eq!(event_children.len(), 2);

    let payload = bus.runtime_payload_for_test();
    let child_ids: Vec<String> = payload
        .values()
        .filter(|event| {
            matches!(
                event.inner.lock().event_type.as_str(),
                "ChildEvent" | "GrandchildEvent"
            )
        })
        .map(|event| event.inner.lock().event_id.clone())
        .collect();
    for child_id in child_ids {
        assert!(event_children.contains(&child_id));
    }
    bus.stop();
}

#[test]
fn test_explicit_event_parent_id_is_not_overridden() {
    let bus = EventBus::new(Some("ExplicitParentBus".to_string()));
    let bus_for_handler = bus.clone();
    let explicit_parent_id = "018f8e40-1234-7000-8000-000000001234".to_string();
    let explicit_parent_id_for_handler = explicit_parent_id.clone();

    bus.on("ParentEvent", "parent_handler", move |_event| {
        let bus = bus_for_handler.clone();
        let explicit_parent_id = explicit_parent_id_for_handler.clone();
        async move {
            let child = TypedEvent::<ChildEvent>::new(EmptyPayload {});
            child.inner.inner.lock().event_parent_id = Some(explicit_parent_id);
            bus.emit_child(child);
            Ok(json!("parent"))
        }
    });

    let _parent = bus.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(bus.wait_until_idle(None));

    let payload = bus.runtime_payload_for_test();
    let child = payload
        .values()
        .find(|event| event.inner.lock().event_type == "ChildEvent")
        .cloned()
        .expect("child event");
    assert_eq!(
        child.inner.lock().event_parent_id.as_deref(),
        Some(explicit_parent_id.as_str())
    );
    bus.stop();
}

#[test]
fn test_event_children_is_empty_when_handlers_do_not_emit_children() {
    let bus = EventBus::new(Some("NoChildrenBus".to_string()));
    let handler = bus.on("ParentEvent", "parent_handler", |_event| async move {
        Ok(json!("parent"))
    });

    let parent = bus.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(parent.wait_completed());

    let parent_inner = parent.inner.inner.lock();
    let result = parent_inner
        .event_results
        .get(&handler.id)
        .expect("parent result");
    assert!(result.event_children.is_empty());
    bus.stop();
}

#[test]
fn test_event_emit_without_await_sets_parentage_without_blocking_parent_completion() {
    let bus = EventBus::new(Some("UnawaitedEventEmitCompletionBus".to_string()));
    let bus_for_handler = bus.clone();
    let child_ref = Arc::new(Mutex::new(None::<Arc<BaseEvent>>));
    let child_ref_for_handler = child_ref.clone();
    let (child_started_tx, child_started_rx) = std::sync::mpsc::channel();
    let (release_child_tx, release_child_rx) = std::sync::mpsc::channel();
    let release_child_rx = Arc::new(Mutex::new(release_child_rx));

    bus.on("ParentEvent", "parent_handler", move |_event| {
        let bus = bus_for_handler.clone();
        let child_ref = child_ref_for_handler.clone();
        async move {
            let child = bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            assert!(!child.inner.inner.lock().event_blocks_parent_completion);
            *child_ref.lock().expect("child ref lock") = Some(child.inner.clone());
            Ok(json!("parent"))
        }
    });
    bus.on("ChildEvent", "child_handler", move |_event| {
        let child_started_tx = child_started_tx.clone();
        let release_child_rx = release_child_rx.clone();
        async move {
            let _ = child_started_tx.send(());
            release_child_rx
                .lock()
                .expect("release child lock")
                .recv_timeout(Duration::from_secs(2))
                .expect("release child");
            Ok(json!("child"))
        }
    });

    let parent = bus.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(parent.wait_completed());
    assert_eq!(
        parent.inner.inner.lock().event_status,
        EventStatus::Completed
    );

    child_started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("child should start after parent");
    let child = child_ref
        .lock()
        .expect("child ref lock")
        .clone()
        .expect("child ref");
    let parent_id = parent.inner.inner.lock().event_id.clone();
    assert_eq!(
        child.inner.lock().event_parent_id.as_deref(),
        Some(parent_id.as_str())
    );
    assert!(child.inner.lock().event_emitted_by_handler_id.is_some());
    assert!(!child.inner.lock().event_blocks_parent_completion);
    assert_ne!(child.inner.lock().event_status, EventStatus::Completed);

    release_child_tx.send(()).expect("release child send");
    block_on(bus.wait_until_idle(Some(2.0)));
    assert_eq!(child.inner.lock().event_status, EventStatus::Completed);
    bus.stop();
}

#[test]
fn test_awaited_event_emit_child_blocks_parent_completion_and_queue_jumps() {
    let bus = EventBus::new(Some("EventChildrenCompletionBus".to_string()));
    let bus_for_handler = bus.clone();
    let child_ref = Arc::new(Mutex::new(None::<Arc<BaseEvent>>));
    let child_ref_for_handler = child_ref.clone();
    let (child_started_tx, child_started_rx) = std::sync::mpsc::channel();
    let (release_child_tx, release_child_rx) = std::sync::mpsc::channel();
    let release_child_rx = Arc::new(Mutex::new(release_child_rx));

    bus.on("ParentEvent", "parent_handler", move |_event| {
        let bus = bus_for_handler.clone();
        let child_ref = child_ref_for_handler.clone();
        async move {
            let child = bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            assert!(!child.inner.inner.lock().event_blocks_parent_completion);
            *child_ref.lock().expect("child ref lock") = Some(child.inner.clone());
            child.wait_completed().await;
            assert!(child.inner.inner.lock().event_blocks_parent_completion);
            Ok(json!("parent"))
        }
    });
    bus.on("ChildEvent", "child_handler", move |_event| {
        let child_started_tx = child_started_tx.clone();
        let release_child_rx = release_child_rx.clone();
        async move {
            let _ = child_started_tx.send(());
            release_child_rx
                .lock()
                .expect("release child lock")
                .recv_timeout(Duration::from_secs(2))
                .expect("release child");
            Ok(json!("child"))
        }
    });

    let parent = bus.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    child_started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("child should queue-jump and start");
    thread::sleep(Duration::from_millis(30));
    assert_ne!(
        parent.inner.inner.lock().event_status,
        EventStatus::Completed
    );

    let child = child_ref
        .lock()
        .expect("child ref lock")
        .clone()
        .expect("child ref");
    assert!(child.inner.lock().event_blocks_parent_completion);
    release_child_tx.send(()).expect("release child send");
    block_on(parent.wait_completed());
    assert_eq!(
        parent.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    assert_eq!(child.inner.lock().event_status, EventStatus::Completed);
    bus.stop();
}

#[test]
fn test_bus_emit_inside_handler_dispatches_root_event_by_default() {
    let bus = EventBus::new(Some("RootChildCompletionBus".to_string()));
    let bus_for_handler = bus.clone();

    bus.on("ParentEvent", "parent_handler", move |_event| {
        let bus = bus_for_handler.clone();
        async move {
            bus.emit::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            Ok(json!("parent"))
        }
    });
    bus.on("ChildEvent", "child_handler", |_event| async move {
        Ok(json!("child"))
    });

    let parent = bus.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(bus.wait_until_idle(None));

    let payload = bus.runtime_payload_for_test();
    let child = payload
        .values()
        .find(|event| event.inner.lock().event_type == "ChildEvent")
        .cloned()
        .expect("child event");
    assert_eq!(child.inner.lock().event_parent_id, None);
    assert_eq!(child.inner.lock().event_emitted_by_handler_id, None);
    assert!(!child.inner.lock().event_blocks_parent_completion);

    let parent_inner = parent.inner.inner.lock();
    let result = parent_inner
        .event_results
        .values()
        .next()
        .expect("parent result");
    assert!(result.event_children.is_empty());
    bus.stop();
}
