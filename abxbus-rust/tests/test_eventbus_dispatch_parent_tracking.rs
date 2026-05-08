use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use abxbus_rust::{
    base_event::BaseEvent,
    event_bus::{EventBus, EventBusOptions},
    typed::{BaseEventHandle, EventSpec},
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
    type payload = EmptyPayload;
    type event_result_type = EmptyResult;
    const event_type: &'static str = "ParentEvent";
}

struct ChildEvent;
impl EventSpec for ChildEvent {
    type payload = EmptyPayload;
    type event_result_type = EmptyResult;
    const event_type: &'static str = "ChildEvent";
}

struct GrandchildEvent;
impl EventSpec for GrandchildEvent {
    type payload = EmptyPayload;
    type event_result_type = EmptyResult;
    const event_type: &'static str = "GrandchildEvent";
}

fn child_ids_for(event: &Arc<BaseEvent>) -> Vec<String> {
    event
        .inner
        .lock()
        .event_results
        .values()
        .flat_map(|result| result.event_children.clone())
        .collect()
}

fn event_children_for(bus: &EventBus, event: &Arc<BaseEvent>) -> Vec<Arc<BaseEvent>> {
    let payload = bus.runtime_payload_for_test();
    child_ids_for(event)
        .into_iter()
        .filter_map(|child_id| payload.get(&child_id).cloned())
        .collect()
}

#[test]
fn test_basic_parent_tracking_child_events_get_event_parent_id() {
    let bus = EventBus::new(Some("ParentTrackingBus".to_string()));
    let bus_for_handler = bus.clone();

    bus.on_raw("ParentEvent", "parent_handler", move |_event| {
        let bus = bus_for_handler.clone();
        async move {
            bus.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            Ok(json!("parent"))
        }
    });
    bus.on_raw("ChildEvent", "child_handler", |_event| async move {
        Ok(json!("child"))
    });

    let parent = bus.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
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

    bus.on_raw("ParentEvent", "parent_handler", move |_event| {
        let bus = bus_for_parent.clone();
        async move {
            bus.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            Ok(json!("parent"))
        }
    });
    bus.on_raw("ChildEvent", "child_handler", move |_event| {
        let bus = bus_for_child.clone();
        async move {
            bus.emit_child(BaseEventHandle::<GrandchildEvent>::new(EmptyPayload {}));
            Ok(json!("child"))
        }
    });
    bus.on_raw(
        "GrandchildEvent",
        "grandchild_handler",
        |_event| async move { Ok(json!("grandchild")) },
    );

    let parent = bus.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
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

    bus.on_raw("ParentEvent", "parent_handler", move |_event| {
        let bus = bus_for_handler.clone();
        async move {
            for _ in 0..3 {
                bus.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            }
            Ok(json!("spawned_children"))
        }
    });
    bus.on_raw("ChildEvent", "child_handler", |_event| async move {
        Ok(json!("child"))
    });

    let parent = bus.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
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

    bus.on_raw("ParentEvent", "handler_1", move |_event| {
        let bus = bus_for_handler_1.clone();
        async move {
            thread::sleep(Duration::from_millis(10));
            bus.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            Ok(json!("h1"))
        }
    });
    bus.on_raw("ParentEvent", "handler_2", move |_event| {
        let bus = bus_for_handler_2.clone();
        async move {
            thread::sleep(Duration::from_millis(20));
            bus.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            Ok(json!("h2"))
        }
    });
    bus.on_raw("ChildEvent", "child_handler", |_event| async move {
        Ok(json!("child"))
    });

    let parent = bus.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
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
fn test_sync_handler_parent_tracking() {
    let bus = EventBus::new(Some("SyncParentTrackingBus".to_string()));
    let child_events = Arc::new(Mutex::new(Vec::new()));

    let bus_for_sync = bus.clone();
    let child_events_for_sync = child_events.clone();
    bus.on_raw_sync("ParentEvent", "sync_handler", move |_event| {
        let child = bus_for_sync.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
        child_events_for_sync
            .lock()
            .expect("child events lock")
            .push(child.inner.clone());
        Ok(json!("sync_handled"))
    });

    let bus_for_failing = bus.clone();
    let child_events_for_failing = child_events.clone();
    bus.on_raw_sync("ParentEvent", "failing_handler", move |_event| {
        let child = bus_for_failing.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
        child_events_for_failing
            .lock()
            .expect("child events lock")
            .push(child.inner.clone());
        Err("expected parent-tracking error path".to_string())
    });

    let bus_for_success = bus.clone();
    let child_events_for_success = child_events.clone();
    bus.on_raw_sync("ParentEvent", "success_handler", move |_event| {
        let child = bus_for_success.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
        child_events_for_success
            .lock()
            .expect("child events lock")
            .push(child.inner.clone());
        Ok(json!("success"))
    });

    bus.on_raw_sync("ChildEvent", "child_handler", |_event| {
        Ok(json!("child_handled"))
    });

    let parent = bus.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
    block_on(parent.done());
    block_on(bus.wait_until_idle(None));

    let parent_id = parent.inner.inner.lock().event_id.clone();
    let child_events = child_events.lock().expect("child events lock").clone();
    assert_eq!(child_events.len(), 3);
    for child in child_events {
        assert_eq!(
            child.inner.lock().event_parent_id.as_deref(),
            Some(parent_id.as_str())
        );
    }
    let parent_errors = parent.inner.event_errors();
    assert_eq!(parent_errors.len(), 1);
    assert!(parent_errors[0].contains("expected parent-tracking error path"));
    bus.stop();
}

#[test]
fn test_event_children_tracks_multiple_children_from_a_single_handler() {
    let bus = EventBus::new(Some("EventChildrenBus".to_string()));
    let bus_for_handler = bus.clone();

    let parent_handler = bus.on_raw("ParentEvent", "parent_handler", move |_event| {
        let bus = bus_for_handler.clone();
        async move {
            bus.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            bus.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            bus.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            Ok(json!("parent"))
        }
    });
    bus.on_raw("ChildEvent", "child_handler", |_event| async move {
        Ok(json!("child"))
    });

    let parent = bus.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
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
    assert_eq!(event_children.len(), 3);

    let payload = bus.runtime_payload_for_test();
    let children: Vec<_> = payload
        .values()
        .filter(|event| event.inner.lock().event_type == "ChildEvent")
        .cloned()
        .collect();
    assert_eq!(children.len(), 3);
    let parent_id = parent.inner.inner.lock().event_id.clone();
    for child in &children {
        let child_inner = child.inner.lock();
        assert_eq!(
            child_inner.event_parent_id.as_deref(),
            Some(parent_id.as_str())
        );
        assert_eq!(child_inner.event_status, EventStatus::Completed);
    }
    for child_id in children
        .iter()
        .map(|child| child.inner.lock().event_id.clone())
        .collect::<Vec<_>>()
    {
        assert!(event_children.contains(&child_id));
    }
    let parent_event_children = event_children_for(&bus, &parent.inner);
    assert_eq!(parent_event_children.len(), 3);
    assert!(parent_event_children
        .iter()
        .all(|child| child.inner.lock().event_type == "ChildEvent"));
    bus.stop();
}

#[test]
fn test_event_children_tracks_direct_and_nested_descendants() {
    let bus = EventBus::new(Some("ChildrenTrackingBus".to_string()));
    let bus_for_parent = bus.clone();
    let bus_for_child = bus.clone();

    let parent_handler = bus.on_raw("ParentEvent", "parent_handler", move |_event| {
        let bus = bus_for_parent.clone();
        async move {
            bus.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            Ok(json!("parent"))
        }
    });
    let child_handler = bus.on_raw("ChildEvent", "child_handler", move |_event| {
        let bus = bus_for_child.clone();
        async move {
            bus.emit_child(BaseEventHandle::<GrandchildEvent>::new(EmptyPayload {}));
            Ok(json!("child"))
        }
    });
    bus.on_raw(
        "GrandchildEvent",
        "grandchild_handler",
        |_event| async move { Ok(json!("grandchild")) },
    );

    let parent = bus.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
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
    let child_id = child.inner.lock().event_id.clone();
    let grandchild_id = grandchild.inner.lock().event_id.clone();

    let parent_children = parent
        .inner
        .inner
        .lock()
        .event_results
        .get(&parent_handler.id)
        .expect("parent result")
        .event_children
        .clone();
    assert_eq!(parent_children, vec![child_id.clone()]);
    let parent_event_children = event_children_for(&bus, &parent.inner);
    assert_eq!(parent_event_children.len(), 1);
    assert_eq!(
        parent_event_children[0].inner.lock().event_id.as_str(),
        child_id.as_str()
    );

    let child_children = child
        .inner
        .lock()
        .event_results
        .get(&child_handler.id)
        .expect("child result")
        .event_children
        .clone();
    assert_eq!(child_children, vec![grandchild_id.clone()]);
    let child_event_children = event_children_for(&bus, &child);
    assert_eq!(child_event_children.len(), 1);
    assert_eq!(
        child_event_children[0].inner.lock().event_id.as_str(),
        grandchild_id.as_str()
    );
    assert_eq!(
        child_event_children[0].inner.lock().event_type,
        "GrandchildEvent"
    );
    bus.stop();
}

#[test]
fn test_multiple_parent_handlers_contribute_to_one_event_children_list() {
    let bus = EventBus::new(Some("EventChildrenMultiHandlerBus".to_string()));
    let bus_for_handler_1 = bus.clone();
    let bus_for_handler_2 = bus.clone();

    let handler_1 = bus.on_raw("ParentEvent", "handler_1", move |_event| {
        let bus = bus_for_handler_1.clone();
        async move {
            bus.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            Ok(json!("h1"))
        }
    });
    let handler_2 = bus.on_raw("ParentEvent", "handler_2", move |_event| {
        let bus = bus_for_handler_2.clone();
        async move {
            bus.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            bus.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            Ok(json!("h2"))
        }
    });
    bus.on_raw("ChildEvent", "child_handler", |_event| async move {
        Ok(json!("child"))
    });

    let parent = bus.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
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
    drop(parent_inner);
    let parent_event_children = event_children_for(&bus, &parent.inner);
    assert_eq!(parent_event_children.len(), 3);
    assert!(parent_event_children
        .iter()
        .all(|child| child.inner.lock().event_type == "ChildEvent"));
    bus.stop();
}

#[test]
fn test_explicit_event_parent_id_is_not_overridden() {
    let bus = EventBus::new(Some("ExplicitParentBus".to_string()));
    let bus_for_handler = bus.clone();
    let explicit_parent_id = "018f8e40-1234-7000-8000-000000001234".to_string();
    let explicit_parent_id_for_handler = explicit_parent_id.clone();

    bus.on_raw("ParentEvent", "parent_handler", move |_event| {
        let bus = bus_for_handler.clone();
        let explicit_parent_id = explicit_parent_id_for_handler.clone();
        async move {
            let child = BaseEventHandle::<ChildEvent>::new(EmptyPayload {});
            child.inner.inner.lock().event_parent_id = Some(explicit_parent_id);
            bus.emit_child(child);
            Ok(json!("parent"))
        }
    });

    let _parent = bus.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
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

    bus_1.on_raw("ParentEvent", "parent_handler", move |_event| {
        let bus_1 = bus_1_for_handler.clone();
        let bus_2 = bus_2_for_handler.clone();
        async move {
            let child = bus_1.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            bus_2.emit(BaseEventHandle::<ChildEvent>::from_base_event(
                child.inner.clone(),
            ));
            Ok(json!("bus1"))
        }
    });
    bus_2.on_raw("ChildEvent", "bus2_child_handler", |_event| async move {
        Ok(json!("bus2"))
    });

    let parent = bus_1.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
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
fn test_cross_bus_bus_emit_inside_handler_does_not_link_parent_when_exactly_one_handler_is_active()
{
    let bus_1 = EventBus::new(Some("ExplicitEventEmitParentLinkBus1".to_string()));
    let bus_2 = EventBus::new(Some("ExplicitEventEmitParentLinkBus2".to_string()));
    let bus_2_for_handler = bus_2.clone();
    let child_ref = Arc::new(Mutex::new(None::<Arc<BaseEvent>>));
    let child_ref_for_handler = child_ref.clone();

    bus_1.on_raw("ParentEvent", "parent_handler", move |_event| {
        let bus_2 = bus_2_for_handler.clone();
        let child_ref = child_ref_for_handler.clone();
        async move {
            let child = bus_2.emit(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            *child_ref.lock().expect("child ref lock") = Some(child.inner.clone());
            Ok(json!("parent"))
        }
    });
    bus_2.on_raw("ChildEvent", "child_handler", |_event| async move {
        Ok(json!("child"))
    });

    let parent = bus_1.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
    block_on(async {
        bus_1.wait_until_idle(None).await;
        bus_2.wait_until_idle(None).await;
    });

    let child = child_ref
        .lock()
        .expect("child ref lock")
        .clone()
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
    bus_1.stop();
    bus_2.stop();
}

#[test]
fn test_bus_emit_outside_handler_does_not_guess_a_parent_when_multiple_handlers_are_active() {
    let bus_1 = EventBus::new(Some("NoParentGuessAmbiguousBus1".to_string()));
    let bus_2 = EventBus::new(Some("NoParentGuessAmbiguousBus2".to_string()));
    let bus_3 = EventBus::new(Some("NoParentGuessAmbiguousBus3".to_string()));
    let (started_a_tx, started_a_rx) = std::sync::mpsc::channel();
    let (started_b_tx, started_b_rx) = std::sync::mpsc::channel();
    let (release_a_tx, release_a_rx) = std::sync::mpsc::channel();
    let (release_b_tx, release_b_rx) = std::sync::mpsc::channel();
    let release_a_rx = Arc::new(Mutex::new(release_a_rx));
    let release_b_rx = Arc::new(Mutex::new(release_b_rx));

    bus_1.on_raw("ParentEvent", "handler_a", move |_event| {
        let started_a_tx = started_a_tx.clone();
        let release_a_rx = release_a_rx.clone();
        async move {
            let _ = started_a_tx.send(());
            release_a_rx
                .lock()
                .expect("release a lock")
                .recv_timeout(Duration::from_secs(2))
                .expect("release a");
            Ok(json!("a_done"))
        }
    });
    bus_2.on_raw("ParentEvent", "handler_b", move |_event| {
        let started_b_tx = started_b_tx.clone();
        let release_b_rx = release_b_rx.clone();
        async move {
            let _ = started_b_tx.send(());
            release_b_rx
                .lock()
                .expect("release b lock")
                .recv_timeout(Duration::from_secs(2))
                .expect("release b");
            Ok(json!("b_done"))
        }
    });
    bus_3.on_raw("ChildEvent", "child_handler", |_event| async move {
        Ok(json!("child_done"))
    });

    let parent_a = bus_1.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
    let parent_b = bus_2.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
    started_a_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("parent a started");
    started_b_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("parent b started");

    let unrelated_child = bus_3.emit(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
    release_a_tx.send(()).expect("release a send");
    release_b_tx.send(()).expect("release b send");
    block_on(async {
        parent_a.done().await;
        parent_b.done().await;
        unrelated_child.done().await;
        bus_1.wait_until_idle(None).await;
        bus_2.wait_until_idle(None).await;
        bus_3.wait_until_idle(None).await;
    });

    assert_eq!(unrelated_child.inner.inner.lock().event_parent_id, None);
    assert_eq!(
        unrelated_child
            .inner
            .inner
            .lock()
            .event_emitted_by_handler_id,
        None
    );
    bus_1.stop();
    bus_2.stop();
    bus_3.stop();
}

#[test]
fn test_erroring_parent_handlers_still_preserve_child_event_parent_id() {
    let bus = EventBus::new(Some("ErrorOnlyParentTrackingBus".to_string()));
    let bus_for_failing_handler = bus.clone();
    let bus_for_success_handler = bus.clone();

    bus.on_raw("ParentEvent", "failing_handler", move |_event| {
        let bus = bus_for_failing_handler.clone();
        async move {
            bus.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            Err("expected parent handler failure".to_string())
        }
    });
    bus.on_raw("ParentEvent", "success_handler", move |_event| {
        let bus = bus_for_success_handler.clone();
        async move {
            bus.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            Ok(json!("recovered"))
        }
    });
    bus.on_raw("ChildEvent", "child_handler", |_event| async move {
        Ok(json!("child"))
    });

    let parent = bus.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
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
    let handler = bus.on_raw("ParentEvent", "parent_handler", |_event| async move {
        Ok(json!("parent"))
    });

    let parent = bus.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
    block_on(parent.done());

    let parent_inner = parent.inner.inner.lock();
    let result = parent_inner
        .event_results
        .get(&handler.id)
        .expect("parent result");
    assert!(result.event_children.is_empty());
    drop(parent_inner);
    assert!(event_children_for(&bus, &parent.inner).is_empty());
    bus.stop();
}

#[test]
fn test_parent_completion_waits_for_awaited_children() {
    let bus = EventBus::new(Some("EventChildrenCompletionMultiBus".to_string()));
    let bus_for_handler = bus.clone();
    let child_refs = Arc::new(Mutex::new(Vec::<Arc<BaseEvent>>::new()));
    let child_refs_for_handler = child_refs.clone();
    let (child_started_tx, child_started_rx) = std::sync::mpsc::channel();
    let (release_children_tx, release_children_rx) = std::sync::mpsc::channel();
    let release_children_rx = Arc::new(Mutex::new(release_children_rx));

    bus.on_raw("ParentEvent", "parent_handler", move |_event| {
        let bus = bus_for_handler.clone();
        let child_refs = child_refs_for_handler.clone();
        async move {
            let child_a = bus.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            let child_b = bus.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            assert!(!child_a.inner.inner.lock().event_blocks_parent_completion);
            assert!(!child_b.inner.inner.lock().event_blocks_parent_completion);
            {
                let mut refs = child_refs.lock().expect("child refs lock");
                refs.push(child_a.inner.clone());
                refs.push(child_b.inner.clone());
            }
            child_a.done().await;
            child_b.done().await;
            Ok(json!("parent"))
        }
    });
    bus.on_raw("ChildEvent", "child_handler", move |_event| {
        let child_started_tx = child_started_tx.clone();
        let release_children_rx = release_children_rx.clone();
        async move {
            let _ = child_started_tx.send(());
            release_children_rx
                .lock()
                .expect("release children lock")
                .recv_timeout(Duration::from_secs(2))
                .expect("release children");
            Ok(json!("child"))
        }
    });

    let parent = bus.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
    child_started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("child should start");
    thread::sleep(Duration::from_millis(30));
    assert_ne!(
        parent.inner.inner.lock().event_status,
        EventStatus::Completed
    );

    let parent_children_before_release: usize = parent
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .map(|result| result.event_children.len())
        .sum();
    assert_eq!(parent_children_before_release, 2);

    release_children_tx.send(()).expect("release child a send");
    release_children_tx.send(()).expect("release child b send");
    block_on(parent.done());
    assert!(block_on(bus.wait_until_idle(Some(2.0))));
    assert_eq!(
        parent.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    let child_refs = child_refs.lock().expect("child refs lock").clone();
    assert_eq!(child_refs.len(), 2);
    for child in child_refs {
        assert!(child.inner.lock().event_blocks_parent_completion);
        assert_eq!(child.inner.lock().event_status, EventStatus::Completed);
    }
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

    bus.on_raw("ParentEvent", "parent_handler", move |_event| {
        let bus = bus_for_handler.clone();
        let child_ref = child_ref_for_handler.clone();
        async move {
            let child = bus.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            assert!(!child.inner.inner.lock().event_blocks_parent_completion);
            *child_ref.lock().expect("child ref lock") = Some(child.inner.clone());
            Ok(json!("parent"))
        }
    });
    bus.on_raw("ChildEvent", "child_handler", move |_event| {
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

    let parent = bus.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
    block_on(parent.done());
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
    let parent_event_children = event_children_for(&bus, &parent.inner);
    assert_eq!(parent_event_children.len(), 1);
    let parent_child_id = parent_event_children[0].inner.lock().event_id.clone();
    let child_id = child.inner.lock().event_id.clone();
    assert_eq!(parent_child_id, child_id);
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

    bus.on_raw("ParentEvent", "parent_handler", move |_event| {
        let bus = bus_for_handler.clone();
        let child_ref = child_ref_for_handler.clone();
        async move {
            let child = bus.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            assert!(!child.inner.inner.lock().event_blocks_parent_completion);
            *child_ref.lock().expect("child ref lock") = Some(child.inner.clone());
            child.done().await;
            assert!(child.inner.inner.lock().event_blocks_parent_completion);
            Ok(json!("parent"))
        }
    });
    bus.on_raw("ChildEvent", "child_handler", move |_event| {
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

    let parent = bus.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
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
    let parent_event_children = event_children_for(&bus, &parent.inner);
    assert_eq!(parent_event_children.len(), 1);
    let parent_child_id = parent_event_children[0].inner.lock().event_id.clone();
    let child_id = child.inner.lock().event_id.clone();
    assert_eq!(parent_child_id, child_id);
    release_child_tx.send(()).expect("release child send");
    block_on(parent.done());
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

    bus.on_raw("ParentEvent", "parent_handler", move |_event| {
        let bus = bus_for_handler.clone();
        async move {
            bus.emit(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            Ok(json!("parent"))
        }
    });
    bus.on_raw("ChildEvent", "child_handler", |_event| async move {
        Ok(json!("child"))
    });

    let parent = bus.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
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
    drop(parent_inner);
    assert!(event_children_for(&bus, &parent.inner).is_empty());
    bus.stop();
}

#[test]
fn test_bus_emit_inside_handler_does_not_link_parent_when_not_using_event_emit() {
    let bus = EventBus::new(Some("ExplicitEventEmitParentLinkBus".to_string()));
    let bus_for_handler = bus.clone();
    let child_ref = Arc::new(Mutex::new(None::<Arc<BaseEvent>>));
    let child_ref_for_handler = child_ref.clone();
    let (child_started_tx, child_started_rx) = std::sync::mpsc::channel();
    let (release_child_tx, release_child_rx) = std::sync::mpsc::channel();
    let release_child_rx = Arc::new(Mutex::new(release_child_rx));

    bus.on_raw("ParentEvent", "parent_handler", move |_event| {
        let bus = bus_for_handler.clone();
        let child_ref = child_ref_for_handler.clone();
        async move {
            let child = bus.emit(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            *child_ref.lock().expect("child ref lock") = Some(child.inner.clone());
            Ok(json!("parent"))
        }
    });
    bus.on_raw("ChildEvent", "child_handler", move |_event| {
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

    let parent = bus.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
    child_started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("child should start");
    block_on(parent.done());
    assert_eq!(
        parent.inner.inner.lock().event_status,
        EventStatus::Completed
    );

    let child = child_ref
        .lock()
        .expect("child ref lock")
        .clone()
        .expect("child event");
    assert_eq!(child.inner.lock().event_parent_id, None);
    assert_eq!(child.inner.lock().event_emitted_by_handler_id, None);
    assert!(!child.inner.lock().event_blocks_parent_completion);
    assert_ne!(child.inner.lock().event_status, EventStatus::Completed);

    let parent_children: usize = parent
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .map(|result| result.event_children.len())
        .sum();
    assert_eq!(parent_children, 0);
    assert!(event_children_for(&bus, &parent.inner).is_empty());

    release_child_tx.send(()).expect("release child send");
    assert!(block_on(bus.wait_until_idle(Some(2.0))));
    assert_eq!(child.inner.lock().event_status, EventStatus::Completed);
    bus.stop();
}

#[test]
fn test_outside_done_of_bus_emit_child_keeps_it_independent_of_active_handler() {
    let bus = EventBus::new(Some("RootChildExternalDoneBus".to_string()));
    let bus_for_handler = bus.clone();
    let child_ref = Arc::new(Mutex::new(None::<Arc<BaseEvent>>));
    let child_ref_for_handler = child_ref.clone();
    let (parent_holding_tx, parent_holding_rx) = std::sync::mpsc::channel();
    let (release_parent_tx, release_parent_rx) = std::sync::mpsc::channel();
    let release_parent_rx = Arc::new(Mutex::new(release_parent_rx));
    let (release_child_tx, release_child_rx) = std::sync::mpsc::channel();
    let release_child_rx = Arc::new(Mutex::new(release_child_rx));

    bus.on_raw("ParentEvent", "parent_handler", move |_event| {
        let bus = bus_for_handler.clone();
        let child_ref = child_ref_for_handler.clone();
        let parent_holding_tx = parent_holding_tx.clone();
        let release_parent_rx = release_parent_rx.clone();
        async move {
            let child = bus.emit(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            *child_ref.lock().expect("child ref lock") = Some(child.inner.clone());
            let _ = parent_holding_tx.send(());
            release_parent_rx
                .lock()
                .expect("release parent lock")
                .recv_timeout(Duration::from_secs(2))
                .expect("release parent");
            Ok(json!("parent"))
        }
    });
    bus.on_raw("ChildEvent", "child_handler", move |_event| {
        let release_child_rx = release_child_rx.clone();
        async move {
            release_child_rx
                .lock()
                .expect("release child lock")
                .recv_timeout(Duration::from_secs(2))
                .expect("release child");
            Ok(json!("child"))
        }
    });

    let parent = bus.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
    parent_holding_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("parent holding");
    let child = child_ref
        .lock()
        .expect("child ref lock")
        .clone()
        .expect("child ref");
    assert_eq!(child.inner.lock().event_parent_id, None);
    assert_eq!(child.inner.lock().event_emitted_by_handler_id, None);
    assert!(!child.inner.lock().event_blocks_parent_completion);

    let child_for_wait = child.clone();
    let wait_thread = thread::spawn(move || {
        block_on(child_for_wait.done());
    });
    thread::sleep(Duration::from_millis(10));
    assert!(!child.inner.lock().event_blocks_parent_completion);

    release_parent_tx.send(()).expect("release parent send");
    block_on(parent.done());
    assert_eq!(
        parent.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    assert_ne!(child.inner.lock().event_status, EventStatus::Completed);

    release_child_tx.send(()).expect("release child send");
    wait_thread.join().expect("child wait thread");
    block_on(bus.wait_until_idle(Some(2.0)));
    assert_eq!(child.inner.lock().event_status, EventStatus::Completed);
    bus.stop();
}

#[test]
fn test_outside_event_completed_wait_of_bus_emit_child_keeps_it_independent_of_active_handler() {
    let bus = EventBus::new(Some("RootChildExternalEventCompletedBus".to_string()));
    let bus_for_handler = bus.clone();
    let child_ref = Arc::new(Mutex::new(None::<Arc<BaseEvent>>));
    let child_ref_for_handler = child_ref.clone();
    let (parent_holding_tx, parent_holding_rx) = std::sync::mpsc::channel();
    let (release_parent_tx, release_parent_rx) = std::sync::mpsc::channel();
    let release_parent_rx = Arc::new(Mutex::new(release_parent_rx));
    let (release_child_tx, release_child_rx) = std::sync::mpsc::channel();
    let release_child_rx = Arc::new(Mutex::new(release_child_rx));

    bus.on_raw("ParentEvent", "parent_handler", move |_event| {
        let bus = bus_for_handler.clone();
        let child_ref = child_ref_for_handler.clone();
        let parent_holding_tx = parent_holding_tx.clone();
        let release_parent_rx = release_parent_rx.clone();
        async move {
            let child = bus.emit(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            *child_ref.lock().expect("child ref lock") = Some(child.inner.clone());
            let _ = parent_holding_tx.send(());
            release_parent_rx
                .lock()
                .expect("release parent lock")
                .recv_timeout(Duration::from_secs(2))
                .expect("release parent");
            Ok(json!("parent"))
        }
    });
    bus.on_raw("ChildEvent", "child_handler", move |_event| {
        let release_child_rx = release_child_rx.clone();
        async move {
            release_child_rx
                .lock()
                .expect("release child lock")
                .recv_timeout(Duration::from_secs(2))
                .expect("release child");
            Ok(json!("child"))
        }
    });

    let parent = bus.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
    parent_holding_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("parent holding");
    let child = child_ref
        .lock()
        .expect("child ref lock")
        .clone()
        .expect("child ref");
    assert_eq!(child.inner.lock().event_parent_id, None);
    assert_eq!(child.inner.lock().event_emitted_by_handler_id, None);
    assert!(!child.inner.lock().event_blocks_parent_completion);

    let child_for_wait = child.clone();
    let wait_thread = thread::spawn(move || {
        block_on(child_for_wait.event_completed());
    });
    thread::sleep(Duration::from_millis(10));
    assert!(!child.inner.lock().event_blocks_parent_completion);

    release_parent_tx.send(()).expect("release parent send");
    block_on(parent.done());
    assert_eq!(
        parent.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    assert_ne!(child.inner.lock().event_status, EventStatus::Completed);

    release_child_tx.send(()).expect("release child send");
    wait_thread.join().expect("child wait thread");
    block_on(bus.wait_until_idle(Some(2.0)));
    assert_eq!(child.inner.lock().event_status, EventStatus::Completed);
    bus.stop();
}

#[test]
fn test_awaited_bus_emit_child_remains_independent_and_does_not_block_parent_completion() {
    let bus = EventBus::new(Some("AwaitedBusEmitRootBus".to_string()));
    let bus_for_handler = bus.clone();
    let child_ref = Arc::new(Mutex::new(None::<Arc<BaseEvent>>));
    let child_ref_for_handler = child_ref.clone();

    bus.on_raw("ParentEvent", "parent_handler", move |_event| {
        let bus = bus_for_handler.clone();
        let child_ref = child_ref_for_handler.clone();
        async move {
            let child = bus.emit(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            assert_eq!(child.inner.inner.lock().event_parent_id, None);
            assert_eq!(child.inner.inner.lock().event_emitted_by_handler_id, None);
            assert!(!child.inner.inner.lock().event_blocks_parent_completion);
            child.done().await;
            assert!(!child.inner.inner.lock().event_blocks_parent_completion);
            *child_ref.lock().expect("child ref lock") = Some(child.inner.clone());
            Ok(json!("parent"))
        }
    });
    bus.on_raw("ChildEvent", "child_handler", |_event| async move {
        Ok(json!("child"))
    });

    let parent = bus.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
    block_on(parent.done());
    let child = child_ref
        .lock()
        .expect("child ref lock")
        .clone()
        .expect("child ref");
    assert_eq!(child.inner.lock().event_parent_id, None);
    assert!(!child.inner.lock().event_blocks_parent_completion);
    let parent_inner = parent.inner.inner.lock();
    let result = parent_inner
        .event_results
        .values()
        .next()
        .expect("parent result");
    assert!(result.event_children.is_empty());
    drop(parent_inner);
    assert!(event_children_for(&bus, &parent.inner).is_empty());
    bus.stop();
}

#[test]
fn test_forwarded_events_are_not_counted_as_parent_event_children() {
    let bus_1 = EventBus::new(Some("ForwardBus1".to_string()));
    let bus_2 = EventBus::new(Some("ForwardBus2".to_string()));
    let bus_2_for_forward = bus_2.clone();

    bus_1.on_raw("*", "forward_to_bus2", move |event| {
        let bus_2 = bus_2_for_forward.clone();
        async move {
            bus_2.emit_base(event);
            Ok(json!(null))
        }
    });

    let parent = bus_1.emit(BaseEventHandle::<ParentEvent>::new(EmptyPayload {}));
    block_on(async {
        bus_1.wait_until_idle(None).await;
        bus_2.wait_until_idle(None).await;
        parent.done().await;
    });

    let parent_inner = parent.inner.inner.lock();
    assert!(parent_inner
        .event_results
        .values()
        .all(|result| result.event_children.is_empty()));
    assert_eq!(parent_inner.event_parent_id, None);
    drop(parent_inner);
    assert!(event_children_for(&bus_1, &parent.inner).is_empty());
    bus_1.stop();
    bus_2.stop();
}

#[test]
fn test_multiple_children_same_parent() {
    test_multiple_children_from_same_parent_keep_same_event_parent_id();
}

#[test]
fn test_basic_parent_tracking() {
    test_basic_parent_tracking_child_events_get_event_parent_id();
}

#[test]
fn test_multi_level_parent_tracking() {
    test_multi_level_parent_tracking_preserves_lineage();
}

#[test]
fn test_parallel_handler_concurrency_parent_tracking() {
    test_parallel_parent_handlers_preserve_parent_tracking();
}

#[test]
fn test_explicit_parent_not_overridden() {
    test_explicit_event_parent_id_is_not_overridden();
}

#[test]
fn test_cross_eventbus_parent_tracking() {
    test_cross_eventbus_dispatch_preserves_parent_tracking();
}

#[test]
fn test_error_handler_parent_tracking() {
    test_erroring_parent_handlers_still_preserve_child_event_parent_id();
}

#[test]
fn test_event_children_tracking() {
    test_event_children_tracks_multiple_children_from_a_single_handler();
}

#[test]
fn test_nested_event_children_tracking() {
    test_event_children_tracks_direct_and_nested_descendants();
}

#[test]
fn test_multiple_handlers_event_children() {
    test_multiple_parent_handlers_contribute_to_one_event_children_list();
}

#[test]
fn test_event_children_empty_when_no_children() {
    test_event_children_is_empty_when_handlers_do_not_emit_children();
}

#[test]
fn test_forwarded_events_not_counted_as_children() {
    test_forwarded_events_are_not_counted_as_parent_event_children();
}

#[test]
fn test_event_emit_without_await_sets_parentage_without_blocking_completion() {
    test_event_emit_without_await_sets_parentage_without_blocking_parent_completion();
}

#[test]
fn test_bus_emit_inside_handler_dispatches_detached_event_by_default() {
    test_bus_emit_inside_handler_does_not_link_parent_when_not_using_event_emit();
}

#[test]
fn test_awaited_event_emit_marks_child_as_parent_completion_blocking() {
    test_awaited_event_emit_child_blocks_parent_completion_and_queue_jumps();
}

#[test]
fn test_awaiting_bus_emitted_child_keeps_independent_parentage() {
    test_awaited_bus_emit_child_remains_independent_and_does_not_block_parent_completion();
}

#[test]
fn test_parent_tracking_works_with_sync_handlers_and_handler_errors() {
    test_sync_handler_parent_tracking();
    test_erroring_parent_handlers_still_preserve_child_event_parent_id();
}

#[test]
fn test_bus_emit_inside_a_handler_dispatches_a_root_event_by_default() {
    test_bus_emit_inside_handler_dispatches_root_event_by_default();
}

#[test]
fn test_outside_await_of_bus_emit_child_done_keeps_it_independent_of_the_active_handler() {
    test_outside_done_of_bus_emit_child_keeps_it_independent_of_active_handler();
}

#[test]
fn test_outside_eventcompleted_wait_of_bus_emit_child_keeps_it_independent_of_the_active_handler() {
    test_outside_event_completed_wait_of_bus_emit_child_keeps_it_independent_of_active_handler();
}
