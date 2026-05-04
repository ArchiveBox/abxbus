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
