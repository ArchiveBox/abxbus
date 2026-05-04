use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use abxbus_rust::{
    base_event::BaseEvent,
    event_bus::{EventBus, EventBusOptions},
    typed::{EventSpec, TypedEvent},
    types::{EventHandlerConcurrencyMode, EventStatus},
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
fn test_multiple_children_from_same_parent_keep_same_event_parent_id() {
    let bus = EventBus::new(Some("MultiChildBus".to_string()));
    let bus_for_handler = bus.clone();

    bus.on("ParentEvent", "parent_handler", move |_event| {
        let bus = bus_for_handler.clone();
        async move {
            for _ in 0..3 {
                bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            }
            Ok(json!("spawned_children"))
        }
    });
    bus.on("ChildEvent", "child_handler", |_event| async move {
        Ok(json!("child"))
    });

    let parent = bus.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(bus.wait_until_idle(None));

    let parent_id = parent.inner.inner.lock().event_id.clone();
    let payload = bus.runtime_payload_for_test();
    let children: Vec<_> = payload
        .values()
        .filter(|event| event.inner.lock().event_type == "ChildEvent")
        .cloned()
        .collect();
    assert_eq!(children.len(), 3);
    for child in children {
        assert_eq!(
            child.inner.lock().event_parent_id.as_deref(),
            Some(parent_id.as_str())
        );
    }
    bus.stop();
}

#[test]
fn test_parallel_parent_handlers_preserve_parent_tracking() {
    let bus = EventBus::new_with_options(
        Some("ParallelParentTrackingBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    let bus_for_handler_1 = bus.clone();
    let bus_for_handler_2 = bus.clone();

    bus.on("ParentEvent", "handler_1", move |_event| {
        let bus = bus_for_handler_1.clone();
        async move {
            thread::sleep(Duration::from_millis(10));
            bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            Ok(json!("h1"))
        }
    });
    bus.on("ParentEvent", "handler_2", move |_event| {
        let bus = bus_for_handler_2.clone();
        async move {
            thread::sleep(Duration::from_millis(20));
            bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            Ok(json!("h2"))
        }
    });
    bus.on("ChildEvent", "child_handler", |_event| async move {
        Ok(json!("child"))
    });

    let parent = bus.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(bus.wait_until_idle(None));

    let parent_id = parent.inner.inner.lock().event_id.clone();
    let payload = bus.runtime_payload_for_test();
    let children: Vec<_> = payload
        .values()
        .filter(|event| event.inner.lock().event_type == "ChildEvent")
        .cloned()
        .collect();
    assert_eq!(children.len(), 2);
    for child in children {
        assert_eq!(
            child.inner.lock().event_parent_id.as_deref(),
            Some(parent_id.as_str())
        );
        assert!(child.inner.lock().event_emitted_by_handler_id.is_some());
    }
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
fn test_multiple_parent_handlers_contribute_to_one_event_children_list() {
    let bus = EventBus::new(Some("EventChildrenMultiHandlerBus".to_string()));
    let bus_for_handler_1 = bus.clone();
    let bus_for_handler_2 = bus.clone();

    let handler_1 = bus.on("ParentEvent", "handler_1", move |_event| {
        let bus = bus_for_handler_1.clone();
        async move {
            bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            Ok(json!("h1"))
        }
    });
    let handler_2 = bus.on("ParentEvent", "handler_2", move |_event| {
        let bus = bus_for_handler_2.clone();
        async move {
            bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            Ok(json!("h2"))
        }
    });
    bus.on("ChildEvent", "child_handler", |_event| async move {
        Ok(json!("child"))
    });

    let parent = bus.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(bus.wait_until_idle(None));

    let parent_inner = parent.inner.inner.lock();
    let handler_1_children = parent_inner
        .event_results
        .get(&handler_1.id)
        .expect("handler 1 result")
        .event_children
        .clone();
    let handler_2_children = parent_inner
        .event_results
        .get(&handler_2.id)
        .expect("handler 2 result")
        .event_children
        .clone();
    assert_eq!(handler_1_children.len(), 1);
    assert_eq!(handler_2_children.len(), 2);
    assert_eq!(handler_1_children.len() + handler_2_children.len(), 3);
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
fn test_cross_eventbus_dispatch_preserves_parent_tracking() {
    let bus_1 = EventBus::new(Some("CrossParentBus1".to_string()));
    let bus_2 = EventBus::new(Some("CrossParentBus2".to_string()));
    let bus_1_for_handler = bus_1.clone();
    let bus_2_for_handler = bus_2.clone();

    bus_1.on("ParentEvent", "parent_handler", move |_event| {
        let bus_1 = bus_1_for_handler.clone();
        let bus_2 = bus_2_for_handler.clone();
        async move {
            let child = bus_1.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            bus_2.emit::<ChildEvent>(TypedEvent::from_base_event(child.inner.clone()));
            Ok(json!("bus1"))
        }
    });
    bus_2.on("ChildEvent", "bus2_child_handler", |_event| async move {
        Ok(json!("bus2"))
    });

    let parent = bus_1.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(async {
        bus_1.wait_until_idle(None).await;
        bus_2.wait_until_idle(None).await;
    });

    let parent_id = parent.inner.inner.lock().event_id.clone();
    let received_child = bus_2
        .runtime_payload_for_test()
        .values()
        .find(|event| event.inner.lock().event_type == "ChildEvent")
        .cloned()
        .expect("received child");
    assert_eq!(
        received_child.inner.lock().event_parent_id.as_deref(),
        Some(parent_id.as_str())
    );
    bus_1.stop();
    bus_2.stop();
}

#[test]
fn test_erroring_parent_handlers_still_preserve_child_event_parent_id() {
    let bus = EventBus::new(Some("ErrorOnlyParentTrackingBus".to_string()));
    let bus_for_failing_handler = bus.clone();
    let bus_for_success_handler = bus.clone();

    bus.on("ParentEvent", "failing_handler", move |_event| {
        let bus = bus_for_failing_handler.clone();
        async move {
            bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            Err("expected parent handler failure".to_string())
        }
    });
    bus.on("ParentEvent", "success_handler", move |_event| {
        let bus = bus_for_success_handler.clone();
        async move {
            bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            Ok(json!("recovered"))
        }
    });
    bus.on("ChildEvent", "child_handler", |_event| async move {
        Ok(json!("child"))
    });

    let parent = bus.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(bus.wait_until_idle(None));

    let parent_id = parent.inner.inner.lock().event_id.clone();
    let payload = bus.runtime_payload_for_test();
    let children: Vec<_> = payload
        .values()
        .filter(|event| event.inner.lock().event_type == "ChildEvent")
        .cloned()
        .collect();
    assert_eq!(children.len(), 2);
    for child in children {
        assert_eq!(
            child.inner.lock().event_parent_id.as_deref(),
            Some(parent_id.as_str())
        );
    }
    let parent_errors = parent
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .filter(|result| result.error.is_some())
        .count();
    assert_eq!(parent_errors, 1);
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
