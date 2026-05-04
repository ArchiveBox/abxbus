use std::sync::{Arc, Mutex};

use abxbus_rust::{base_event::BaseEvent, event_bus::EventBus, types::EventStatus};
use futures::executor::block_on;
use serde_json::{json, Map, Value};

fn base_event(event_type: &str, payload: Value) -> Arc<BaseEvent> {
    let Value::Object(payload) = payload else {
        panic!("test payload must be an object");
    };
    BaseEvent::new(event_type, payload)
}

fn history_event(bus: &Arc<EventBus>, event_type: &str) -> Arc<BaseEvent> {
    bus.runtime_payload_for_test()
        .values()
        .find(|event| event.inner.lock().event_type == event_type)
        .cloned()
        .unwrap_or_else(|| panic!("missing {event_type} in history"))
}

#[test]
fn test_event_event_bus_inside_handler_returns_the_dispatching_bus() {
    let bus = EventBus::new(Some("TestBus".to_string()));
    let handler_called = Arc::new(Mutex::new(false));
    let handler_bus_name = Arc::new(Mutex::new(None::<String>));
    let child_event = Arc::new(Mutex::new(None::<Arc<BaseEvent>>));

    let handler_called_for_handler = handler_called.clone();
    let handler_bus_name_for_handler = handler_bus_name.clone();
    let child_event_for_handler = child_event.clone();
    bus.on("MainEvent", "main_handler", move |event| {
        let handler_called = handler_called_for_handler.clone();
        let handler_bus_name = handler_bus_name_for_handler.clone();
        let child_event = child_event_for_handler.clone();
        async move {
            *handler_called.lock().expect("handler_called lock") = true;
            let current_bus = event.event_bus().expect("event bus inside handler");
            *handler_bus_name.lock().expect("handler_bus_name lock") =
                Some(current_bus.name.clone());
            let child = current_bus.emit_child_base(base_event("ChildEvent", json!({})));
            *child_event.lock().expect("child_event lock") = Some(child);
            Ok(json!(null))
        }
    });
    bus.on("ChildEvent", "child_handler", |_event| async move {
        Ok(json!(null))
    });

    let event = bus.emit_base(base_event("MainEvent", json!({})));
    block_on(event.event_completed());
    assert!(block_on(bus.wait_until_idle(None)));

    assert!(*handler_called.lock().expect("handler_called lock"));
    assert_eq!(
        handler_bus_name
            .lock()
            .expect("handler_bus_name lock")
            .as_deref(),
        Some("TestBus")
    );
    let child = child_event
        .lock()
        .expect("child_event lock")
        .clone()
        .expect("child event should have been dispatched");
    assert_eq!(child.inner.lock().event_type, "ChildEvent");
    assert_eq!(
        child.event_bus().map(|bus| bus.name.clone()).as_deref(),
        Some("TestBus")
    );
    bus.stop();
}

#[test]
fn test_legacy_bus_property_is_not_exposed_inside_handlers() {
    let bus = EventBus::new(Some("NoLegacyEventBusPropertyBus".to_string()));
    let has_serialized_legacy_bus = Arc::new(Mutex::new(true));

    let has_serialized_legacy_bus_for_handler = has_serialized_legacy_bus.clone();
    bus.on("MainEvent", "handler", move |event| {
        let has_serialized_legacy_bus = has_serialized_legacy_bus_for_handler.clone();
        async move {
            *has_serialized_legacy_bus.lock().expect("legacy bus lock") =
                event.to_json_value().get("bus").is_some();
            Ok(json!(null))
        }
    });

    let event = bus.emit_base(base_event("MainEvent", json!({})));
    block_on(event.event_completed());
    assert!(!*has_serialized_legacy_bus.lock().expect("legacy bus lock"));
    assert!(base_event("DetachedEvent", json!({}))
        .to_json_value()
        .get("bus")
        .is_none());
    bus.stop();
}

#[test]
fn test_event_event_bus_is_set_for_child_events_emitted_in_handler() {
    let bus = EventBus::new(Some("EventBusPropertyFallbackBus".to_string()));
    let child_bus_name = Arc::new(Mutex::new(None::<String>));

    let child_bus_name_for_handler = child_bus_name.clone();
    bus.on("MainEvent", "handler", move |event| {
        let child_bus_name = child_bus_name_for_handler.clone();
        async move {
            let current_bus = event.event_bus().expect("handler bus");
            let child = current_bus.emit_child_base(base_event("ChildEvent", json!({})));
            *child_bus_name.lock().expect("child bus lock") =
                child.event_bus().map(|bus| bus.name.clone());
            Ok(json!(null))
        }
    });
    bus.on("ChildEvent", "child_handler", |_event| async move {
        Ok(json!(null))
    });

    let event = bus.emit_base(base_event("MainEvent", json!({})));
    block_on(event.event_completed());
    assert!(block_on(bus.wait_until_idle(None)));
    assert_eq!(
        child_bus_name.lock().expect("child bus lock").as_deref(),
        Some("EventBusPropertyFallbackBus")
    );
    bus.stop();
}

#[test]
fn test_event_event_bus_is_absent_on_detached_events() {
    let bus = EventBus::new(Some("EventBusPropertyDetachedBus".to_string()));
    bus.on(
        "MainEvent",
        "handler",
        |_event| async move { Ok(json!(null)) },
    );

    let original = bus.emit_base(base_event("MainEvent", json!({})));
    block_on(original.event_completed());

    assert_eq!(
        original.event_bus().map(|bus| bus.name.clone()).as_deref(),
        Some("EventBusPropertyDetachedBus")
    );
    let detached = BaseEvent::from_json_value(original.to_json_value());
    assert!(detached.event_bus().is_none());
    assert_eq!(detached.inner.lock().event_path, vec![bus.label()]);
    bus.stop();
}

#[test]
fn test_event_event_bus_is_available_outside_handler_context() {
    let bus = EventBus::new(Some("EventBusPropertyOutsideHandlerBus".to_string()));
    let event = bus.emit_base(base_event("MainEvent", json!({})));
    block_on(event.event_completed());

    assert_eq!(
        event.event_bus().map(|bus| bus.name.clone()).as_deref(),
        Some("EventBusPropertyOutsideHandlerBus")
    );
    bus.stop();
}

#[test]
fn test_event_event_bus_returns_correct_bus_when_multiple_buses_exist() {
    let bus1 = EventBus::new(Some("Bus1".to_string()));
    let bus2 = EventBus::new(Some("Bus2".to_string()));
    let handler1_bus_name = Arc::new(Mutex::new(None::<String>));
    let handler2_bus_name = Arc::new(Mutex::new(None::<String>));

    let handler1_bus_name_for_handler = handler1_bus_name.clone();
    bus1.on("MainEvent", "handler1", move |event| {
        let handler1_bus_name = handler1_bus_name_for_handler.clone();
        async move {
            *handler1_bus_name.lock().expect("handler1 bus lock") =
                event.event_bus().map(|bus| bus.name.clone());
            Ok(json!(null))
        }
    });
    let handler2_bus_name_for_handler = handler2_bus_name.clone();
    bus2.on("MainEvent", "handler2", move |event| {
        let handler2_bus_name = handler2_bus_name_for_handler.clone();
        async move {
            *handler2_bus_name.lock().expect("handler2 bus lock") =
                event.event_bus().map(|bus| bus.name.clone());
            Ok(json!(null))
        }
    });

    let event1 = bus1.emit_base(base_event("MainEvent", json!({})));
    block_on(event1.event_completed());
    let event2 = bus2.emit_base(base_event("MainEvent", json!({})));
    block_on(event2.event_completed());

    assert_eq!(
        handler1_bus_name
            .lock()
            .expect("handler1 bus lock")
            .as_deref(),
        Some("Bus1")
    );
    assert_eq!(
        handler2_bus_name
            .lock()
            .expect("handler2 bus lock")
            .as_deref(),
        Some("Bus2")
    );
    bus1.stop();
    bus2.stop();
}

#[test]
fn test_event_event_bus_reflects_the_currently_processing_bus_when_forwarded() {
    let bus1 = EventBus::new(Some("Bus1".to_string()));
    let bus2 = EventBus::new(Some("Bus2".to_string()));
    let bus2_handler_bus_name = Arc::new(Mutex::new(None::<String>));

    let bus2_for_forward = bus2.clone();
    bus1.on("*", "forward_to_bus2", move |event| {
        let bus2 = bus2_for_forward.clone();
        async move {
            bus2.emit_base(event);
            Ok(json!(null))
        }
    });

    let handler_bus_name = bus2_handler_bus_name.clone();
    bus2.on("MainEvent", "bus2_handler", move |event| {
        let handler_bus_name = handler_bus_name.clone();
        async move {
            *handler_bus_name.lock().expect("handler_bus_name lock") =
                event.event_bus().map(|bus| bus.name.clone());
            Ok(json!(null))
        }
    });

    let event = bus1.emit_base(base_event("MainEvent", json!({})));
    block_on(event.event_completed());
    assert!(block_on(bus1.wait_until_idle(None)));
    assert!(block_on(bus2.wait_until_idle(None)));

    assert_eq!(
        bus2_handler_bus_name
            .lock()
            .expect("handler_bus_name lock")
            .as_deref(),
        Some("Bus2")
    );
    assert_eq!(
        event.inner.lock().event_path,
        vec![bus1.label(), bus2.label()]
    );
    bus1.stop();
    bus2.stop();
}

#[test]
fn test_event_event_bus_in_nested_handlers_sees_the_same_bus() {
    let bus = EventBus::new(Some("MainBus".to_string()));
    let outer_bus_name = Arc::new(Mutex::new(None::<String>));
    let inner_bus_name = Arc::new(Mutex::new(None::<String>));

    let outer_bus_name_for_handler = outer_bus_name.clone();
    bus.on("MainEvent", "outer_handler", move |event| {
        let outer_bus_name = outer_bus_name_for_handler.clone();
        async move {
            let current_bus = event.event_bus().expect("outer bus");
            *outer_bus_name.lock().expect("outer bus lock") = Some(current_bus.name.clone());
            let child = current_bus.emit_child_base(base_event("ChildEvent", json!({})));
            child.wait_completed().await;
            Ok(json!(null))
        }
    });

    let inner_bus_name_for_handler = inner_bus_name.clone();
    bus.on("ChildEvent", "inner_handler", move |event| {
        let inner_bus_name = inner_bus_name_for_handler.clone();
        async move {
            *inner_bus_name.lock().expect("inner bus lock") =
                event.event_bus().map(|bus| bus.name.clone());
            Ok(json!(null))
        }
    });

    let parent = bus.emit_base(base_event("MainEvent", json!({})));
    block_on(parent.event_completed());

    assert_eq!(
        outer_bus_name.lock().expect("outer bus lock").as_deref(),
        Some("MainBus")
    );
    assert_eq!(
        inner_bus_name.lock().expect("inner bus lock").as_deref(),
        Some("MainBus")
    );
    bus.stop();
}

#[test]
fn test_event_emit_awaited_children_pass_explicit_handler_context_to_immediate_processing() {
    let bus = EventBus::new(Some("ExplicitEventEmitHandlerContextBus".to_string()));
    let child_ref = Arc::new(Mutex::new(None::<Arc<BaseEvent>>));

    let child_ref_for_handler = child_ref.clone();
    bus.on("MainEvent", "main_handler", move |event| {
        let child_ref = child_ref_for_handler.clone();
        async move {
            let current_bus = event.event_bus().expect("handler bus");
            let child = current_bus.emit_child_base(base_event("ChildEvent", json!({})));
            child.wait_completed().await;
            *child_ref.lock().expect("child lock") = Some(child);
            Ok(json!(null))
        }
    });
    bus.on("ChildEvent", "child_handler", |_event| async move {
        Ok(json!("child-ok"))
    });

    let parent = bus.emit_base(base_event("MainEvent", json!({})));
    block_on(parent.event_completed());
    let child = child_ref
        .lock()
        .expect("child lock")
        .clone()
        .expect("child event");
    let child_inner = child.inner.lock();
    let parent_inner = parent.inner.lock();
    assert_eq!(
        child_inner.event_parent_id.as_deref(),
        Some(parent_inner.event_id.as_str())
    );
    assert_eq!(
        child_inner.event_emitted_by_handler_id.as_deref(),
        parent_inner
            .event_results
            .values()
            .find(|result| result.handler.handler_name == "main_handler")
            .map(|result| result.handler.id.as_str())
    );
    assert!(child_inner.event_blocks_parent_completion);
    bus.stop();
}

#[test]
fn test_event_emit_sets_parent_child_relationships_through_3_levels() {
    let bus = EventBus::new(Some("MainBus".to_string()));
    let execution_order = Arc::new(Mutex::new(Vec::<String>::new()));
    let child_ref = Arc::new(Mutex::new(None::<Arc<BaseEvent>>));
    let grandchild_ref = Arc::new(Mutex::new(None::<Arc<BaseEvent>>));

    let order_for_parent = execution_order.clone();
    let child_ref_for_parent = child_ref.clone();
    bus.on("MainEvent", "parent_handler", move |event| {
        let order = order_for_parent.clone();
        let child_ref = child_ref_for_parent.clone();
        async move {
            order
                .lock()
                .expect("order lock")
                .push("parent_start".to_string());
            let current_bus = event.event_bus().expect("parent bus");
            let child = current_bus.emit_child_base(base_event("ChildEvent", json!({})));
            child.wait_completed().await;
            *child_ref.lock().expect("child lock") = Some(child);
            order
                .lock()
                .expect("order lock")
                .push("parent_end".to_string());
            Ok(json!(null))
        }
    });

    let order_for_child = execution_order.clone();
    let grandchild_ref_for_child = grandchild_ref.clone();
    bus.on("ChildEvent", "child_handler", move |event| {
        let order = order_for_child.clone();
        let grandchild_ref = grandchild_ref_for_child.clone();
        async move {
            order
                .lock()
                .expect("order lock")
                .push("child_start".to_string());
            let current_bus = event.event_bus().expect("child bus");
            let grandchild = current_bus.emit_child_base(base_event("GrandchildEvent", json!({})));
            grandchild.wait_completed().await;
            *grandchild_ref.lock().expect("grandchild lock") = Some(grandchild);
            order
                .lock()
                .expect("order lock")
                .push("child_end".to_string());
            Ok(json!(null))
        }
    });

    let order_for_grandchild = execution_order.clone();
    bus.on("GrandchildEvent", "grandchild_handler", move |event| {
        let order = order_for_grandchild.clone();
        async move {
            assert_eq!(
                event.event_bus().map(|bus| bus.name.clone()).as_deref(),
                Some("MainBus")
            );
            order
                .lock()
                .expect("order lock")
                .push("grandchild_start".to_string());
            order
                .lock()
                .expect("order lock")
                .push("grandchild_end".to_string());
            Ok(json!(null))
        }
    });

    let parent = bus.emit_base(base_event("MainEvent", json!({})));
    block_on(parent.event_completed());
    let child = child_ref
        .lock()
        .expect("child lock")
        .clone()
        .expect("child event");
    let grandchild = grandchild_ref
        .lock()
        .expect("grandchild lock")
        .clone()
        .expect("grandchild event");

    assert_eq!(
        execution_order.lock().expect("order lock").as_slice(),
        &[
            "parent_start".to_string(),
            "child_start".to_string(),
            "grandchild_start".to_string(),
            "grandchild_end".to_string(),
            "child_end".to_string(),
            "parent_end".to_string(),
        ]
    );
    assert_eq!(parent.inner.lock().event_status, EventStatus::Completed);
    assert_eq!(child.inner.lock().event_status, EventStatus::Completed);
    assert_eq!(grandchild.inner.lock().event_status, EventStatus::Completed);
    assert_eq!(
        child.inner.lock().event_parent_id.as_deref(),
        Some(parent.inner.lock().event_id.as_str())
    );
    assert_eq!(
        grandchild.inner.lock().event_parent_id.as_deref(),
        Some(child.inner.lock().event_id.as_str())
    );
    assert!(bus.event_is_child_of(&child, &parent));
    assert!(bus.event_is_child_of(&grandchild, &parent));
    bus.stop();
}

#[test]
fn test_event_emit_with_forwarding_child_dispatch_goes_to_the_correct_bus() {
    let bus1 = EventBus::new(Some("Bus1".to_string()));
    let bus2 = EventBus::new(Some("Bus2".to_string()));
    let child_handler_bus_name = Arc::new(Mutex::new(None::<String>));
    let child_ref = Arc::new(Mutex::new(None::<Arc<BaseEvent>>));

    let bus2_for_forward = bus2.clone();
    bus1.on("*", "forward_to_bus2", move |event| {
        let bus2 = bus2_for_forward.clone();
        async move {
            bus2.emit_base(event);
            Ok(json!(null))
        }
    });

    let child_ref_for_handler = child_ref.clone();
    bus2.on("MainEvent", "bus2_main_handler", move |event| {
        let child_ref = child_ref_for_handler.clone();
        async move {
            let current_bus = event.event_bus().expect("forwarded handler bus");
            assert_eq!(current_bus.name, "Bus2");
            let child = current_bus.emit_child_base(base_event("ChildEvent", json!({})));
            child.wait_completed().await;
            *child_ref.lock().expect("child_ref lock") = Some(child);
            Ok(json!(null))
        }
    });

    let child_bus_name = child_handler_bus_name.clone();
    bus2.on("ChildEvent", "bus2_child_handler", move |event| {
        let child_bus_name = child_bus_name.clone();
        async move {
            *child_bus_name.lock().expect("child_bus_name lock") =
                event.event_bus().map(|bus| bus.name.clone());
            Ok(json!("child-ok"))
        }
    });

    let parent = bus1.emit_base(base_event("MainEvent", json!({})));
    block_on(parent.event_completed());
    assert!(block_on(bus1.wait_until_idle(None)));
    assert!(block_on(bus2.wait_until_idle(None)));

    let child = child_ref
        .lock()
        .expect("child_ref lock")
        .clone()
        .expect("child event");
    assert_eq!(
        child_handler_bus_name
            .lock()
            .expect("child_bus_name lock")
            .as_deref(),
        Some("Bus2")
    );
    assert_eq!(child.inner.lock().event_status, EventStatus::Completed);
    assert_eq!(
        child.inner.lock().event_parent_id.as_deref(),
        Some(parent.inner.lock().event_id.as_str())
    );
    assert_eq!(child.inner.lock().event_path, vec![bus2.label()]);
    bus1.stop();
    bus2.stop();
}

#[test]
fn test_event_event_bus_is_set_on_the_event_after_dispatch_outside_handler() {
    let bus = EventBus::new(Some("TestBus".to_string()));
    let raw_event = BaseEvent::new("MainEvent", Map::new());
    assert!(raw_event.event_bus().is_none());

    let dispatched = bus.emit_base(raw_event);
    assert_eq!(
        dispatched
            .event_bus()
            .map(|bus| bus.name.clone())
            .as_deref(),
        Some("TestBus")
    );
    block_on(dispatched.event_completed());
    bus.stop();
}

#[test]
fn test_event_emit_from_handler_correctly_attributes_event_emitted_by_handler_id() {
    let bus = EventBus::new(Some("TestBus".to_string()));

    bus.on("MainEvent", "main_handler", move |event| async move {
        let current_bus = event.event_bus().expect("handler bus");
        current_bus.emit_child_base(base_event("ChildEvent", json!({})));
        Ok(json!(null))
    });
    bus.on("ChildEvent", "child_handler", |_event| async move {
        Ok(json!(null))
    });

    let parent = bus.emit_base(base_event("MainEvent", json!({})));
    assert!(block_on(bus.wait_until_idle(None)));
    let child = history_event(&bus, "ChildEvent");

    assert_eq!(
        child.inner.lock().event_parent_id.as_deref(),
        Some(parent.inner.lock().event_id.as_str())
    );
    let emitted_by = child
        .inner
        .lock()
        .event_emitted_by_handler_id
        .clone()
        .expect("event_emitted_by_handler_id");
    assert!(parent.inner.lock().event_results.contains_key(&emitted_by));
    bus.stop();
}

#[test]
fn test_dispatch_preserves_explicit_event_parent_id_and_does_not_override_it() {
    let bus = EventBus::new(Some("ExplicitParentBus".to_string()));
    let explicit_parent_id = "018f8e40-1234-7000-8000-000000001234".to_string();

    let explicit_parent_id_for_handler = explicit_parent_id.clone();
    bus.on("MainEvent", "main_handler", move |event| {
        let explicit_parent_id = explicit_parent_id_for_handler.clone();
        async move {
            let current_bus = event.event_bus().expect("handler bus");
            let child = base_event("ChildEvent", json!({}));
            child.inner.lock().event_parent_id = Some(explicit_parent_id);
            current_bus.emit_child_base(child);
            Ok(json!(null))
        }
    });

    let parent = bus.emit_base(base_event("MainEvent", json!({})));
    assert!(block_on(bus.wait_until_idle(None)));
    let child = history_event(&bus, "ChildEvent");
    assert_eq!(
        child.inner.lock().event_parent_id.as_deref(),
        Some(explicit_parent_id.as_str())
    );
    assert_ne!(
        child.inner.lock().event_parent_id.as_deref(),
        Some(parent.inner.lock().event_id.as_str())
    );
    bus.stop();
}

#[test]
fn test_event_is_child_of_and_event_is_parent_of_work_for_direct_children() {
    let bus = EventBus::new(Some("ParentChildBus".to_string()));
    bus.on(
        "LineageParentEvent",
        "parent_handler",
        move |event| async move {
            event
                .event_bus()
                .expect("handler bus")
                .emit_child_base(base_event("LineageChildEvent", json!({})));
            Ok(json!(null))
        },
    );

    let parent = bus.emit_base(base_event("LineageParentEvent", json!({})));
    assert!(block_on(bus.wait_until_idle(None)));
    let child = history_event(&bus, "LineageChildEvent");

    assert_eq!(
        child.inner.lock().event_parent_id.as_deref(),
        Some(parent.inner.lock().event_id.as_str())
    );
    assert!(bus.event_is_child_of(&child, &parent));
    assert!(bus.event_is_parent_of(&parent, &child));
    bus.stop();
}

#[test]
fn test_event_is_child_of_works_for_grandchildren() {
    let bus = EventBus::new(Some("GrandchildBus".to_string()));
    bus.on(
        "LineageParentEvent",
        "parent_handler",
        move |event| async move {
            event
                .event_bus()
                .expect("handler bus")
                .emit_child_base(base_event("LineageChildEvent", json!({})));
            Ok(json!(null))
        },
    );
    bus.on(
        "LineageChildEvent",
        "child_handler",
        move |event| async move {
            event
                .event_bus()
                .expect("handler bus")
                .emit_child_base(base_event("LineageGrandchildEvent", json!({})));
            Ok(json!(null))
        },
    );

    let parent = bus.emit_base(base_event("LineageParentEvent", json!({})));
    assert!(block_on(bus.wait_until_idle(None)));
    let child = history_event(&bus, "LineageChildEvent");
    let grandchild = history_event(&bus, "LineageGrandchildEvent");

    assert!(bus.event_is_child_of(&child, &parent));
    assert!(bus.event_is_child_of(&grandchild, &parent));
    assert_eq!(
        grandchild.inner.lock().event_parent_id.as_deref(),
        Some(child.inner.lock().event_id.as_str())
    );
    assert!(bus.event_is_parent_of(&parent, &grandchild));
    bus.stop();
}

#[test]
fn test_event_is_child_of_returns_false_for_unrelated_events() {
    let bus = EventBus::new(Some("UnrelatedBus".to_string()));

    let parent = bus.emit_base(base_event("LineageParentEvent", json!({})));
    let unrelated = bus.emit_base(base_event("LineageUnrelatedEvent", json!({})));
    block_on(parent.event_completed());
    block_on(unrelated.event_completed());

    assert!(!bus.event_is_child_of(&unrelated, &parent));
    assert!(!bus.event_is_parent_of(&parent, &unrelated));
    bus.stop();
}
