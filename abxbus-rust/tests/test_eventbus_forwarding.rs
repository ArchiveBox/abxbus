use abxbus_rust::event;
use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use abxbus_rust::{
    base_event::{EventResultOptions, EventWaitOptions},
    event_bus::{EventBus, EventBusOptions},
    types::{
        EventConcurrencyMode, EventHandlerCompletionMode, EventHandlerConcurrencyMode, EventStatus,
    },
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}

event! {
    struct PingEvent {
        value: i64,
        event_result_type: EmptyResult,
        event_type: "PingEvent",
    }
}
event! {
    struct OrderEvent {
        order: i64,
        event_result_type: EmptyResult,
        event_type: "OrderEvent",
    }
}
event! {
    struct ProxyDispatchRootEvent {
        event_result_type: EmptyResult,
        event_type: "ProxyDispatchRootEvent",
    }
}
event! {
    struct ProxyDispatchChildEvent {
        event_result_type: EmptyResult,
        event_type: "ProxyDispatchChildEvent",
    }
}
event! {
    struct DefaultsChildEvent {
        mode: String,
        event_result_type: String,
        event_type: "ForwardedDefaultsChildEvent",
    }
}
event! {
    struct ForwardedFirstDefaultsEvent {
        event_result_type: String,
        event_type: "ForwardedFirstDefaultsEvent",
    }
}

fn push_log(log: &Arc<Mutex<Vec<String>>>, entry: &str) {
    log.lock().expect("log lock").push(entry.to_string());
}

fn forwarding_index_of(log: &[String], entry: &str) -> usize {
    log.iter()
        .position(|value| value == entry)
        .unwrap_or_else(|| panic!("missing log entry: {entry}; got {log:?}"))
}

fn new_forwarding_bus(
    name: &str,
    event_handler_concurrency: EventHandlerConcurrencyMode,
    event_handler_completion: EventHandlerCompletionMode,
) -> Arc<EventBus> {
    EventBus::new_with_options(
        Some(name.to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency,
            event_handler_completion,
            ..EventBusOptions::default()
        },
    )
}
#[test]
fn test_events_forward_between_buses_without_duplication() {
    let bus_a = EventBus::new(Some("BusA".to_string()));
    let bus_b = EventBus::new(Some("BusB".to_string()));
    let bus_c = EventBus::new(Some("BusC".to_string()));

    let seen_a = Arc::new(Mutex::new(Vec::new()));
    let seen_b = Arc::new(Mutex::new(Vec::new()));
    let seen_c = Arc::new(Mutex::new(Vec::new()));

    let seen_a_handler = seen_a.clone();
    bus_a.on_raw("PingEvent", "seen_a", move |event| {
        let seen = seen_a_handler.clone();
        async move {
            seen.lock()
                .expect("seen_a lock")
                .push(event.inner.lock().event_id.clone());
            Ok(json!(null))
        }
    });
    let seen_b_handler = seen_b.clone();
    bus_b.on_raw("PingEvent", "seen_b", move |event| {
        let seen = seen_b_handler.clone();
        async move {
            seen.lock()
                .expect("seen_b lock")
                .push(event.inner.lock().event_id.clone());
            Ok(json!(null))
        }
    });
    let seen_c_handler = seen_c.clone();
    bus_c.on_raw("PingEvent", "seen_c", move |event| {
        let seen = seen_c_handler.clone();
        async move {
            seen.lock()
                .expect("seen_c lock")
                .push(event.inner.lock().event_id.clone());
            Ok(json!(null))
        }
    });

    let bus_b_for_forward = bus_b.clone();
    bus_a.on_raw("*", "forward_to_b", move |event| {
        let bus_b = bus_b_for_forward.clone();
        async move {
            bus_b.emit_base(event);
            Ok(json!(null))
        }
    });
    let bus_c_for_forward = bus_c.clone();
    bus_b.on_raw("*", "forward_to_c", move |event| {
        let bus_c = bus_c_for_forward.clone();
        async move {
            bus_c.emit_base(event);
            Ok(json!(null))
        }
    });

    let event = bus_a.emit(PingEvent {
        value: 1,
        ..Default::default()
    });
    let _ = block_on(event.now());
    block_on(bus_a.wait_until_idle(None));
    block_on(bus_b.wait_until_idle(None));
    block_on(bus_c.wait_until_idle(None));

    let event_id = event.event_id.clone();
    assert_eq!(
        seen_a.lock().expect("seen_a lock").as_slice(),
        std::slice::from_ref(&event_id)
    );
    assert_eq!(
        seen_b.lock().expect("seen_b lock").as_slice(),
        std::slice::from_ref(&event_id)
    );
    assert_eq!(
        seen_c.lock().expect("seen_c lock").as_slice(),
        std::slice::from_ref(&event_id)
    );
    assert_eq!(
        event._inner_event().inner.lock().event_path,
        vec![bus_a.label(), bus_b.label(), bus_c.label()]
    );
    assert_eq!(event._inner_event().inner.lock().event_pending_bus_count, 0);
    bus_a.destroy();
    bus_b.destroy();
    bus_c.destroy();
}

#[test]
fn test_tree_level_hierarchy_bubbling() {
    let parent_bus = EventBus::new(Some("ParentBus".to_string()));
    let child_bus = EventBus::new(Some("ChildBus".to_string()));
    let subchild_bus = EventBus::new(Some("SubchildBus".to_string()));

    let events_at_parent = Arc::new(Mutex::new(Vec::new()));
    let events_at_child = Arc::new(Mutex::new(Vec::new()));
    let events_at_subchild = Arc::new(Mutex::new(Vec::new()));

    for (bus, seen, handler_name) in [
        (parent_bus.clone(), events_at_parent.clone(), "parent_seen"),
        (child_bus.clone(), events_at_child.clone(), "child_seen"),
        (
            subchild_bus.clone(),
            events_at_subchild.clone(),
            "subchild_seen",
        ),
    ] {
        bus.on_raw("PingEvent", handler_name, move |event| {
            let seen = seen.clone();
            async move {
                seen.lock()
                    .expect("seen lock")
                    .push(event.inner.lock().event_id.clone());
                Ok(json!(null))
            }
        });
    }

    let parent_for_forward = parent_bus.clone();
    child_bus.on_raw("*", "forward_to_parent", move |event| {
        let parent_bus = parent_for_forward.clone();
        async move {
            parent_bus.emit_base(event);
            Ok(json!(null))
        }
    });
    let child_for_forward = child_bus.clone();
    subchild_bus.on_raw("*", "forward_to_child", move |event| {
        let child_bus = child_for_forward.clone();
        async move {
            child_bus.emit_base(event);
            Ok(json!(null))
        }
    });

    let bottom = subchild_bus.emit(PingEvent {
        value: 1,
        ..Default::default()
    });
    let _ = block_on(bottom.now());
    block_on(subchild_bus.wait_until_idle(None));
    block_on(child_bus.wait_until_idle(None));
    block_on(parent_bus.wait_until_idle(None));

    let bottom_id = bottom.event_id.clone();
    assert_eq!(
        events_at_subchild.lock().expect("subchild lock").as_slice(),
        std::slice::from_ref(&bottom_id)
    );
    assert_eq!(
        events_at_child.lock().expect("child lock").as_slice(),
        std::slice::from_ref(&bottom_id)
    );
    assert_eq!(
        events_at_parent.lock().expect("parent lock").as_slice(),
        std::slice::from_ref(&bottom_id)
    );
    assert_eq!(
        bottom._inner_event().inner.lock().event_path,
        vec![subchild_bus.label(), child_bus.label(), parent_bus.label()]
    );

    events_at_parent.lock().expect("parent lock").clear();
    events_at_child.lock().expect("child lock").clear();
    events_at_subchild.lock().expect("subchild lock").clear();

    let middle = child_bus.emit(PingEvent {
        value: 2,
        ..Default::default()
    });
    let _ = block_on(middle.now());
    block_on(child_bus.wait_until_idle(None));
    block_on(parent_bus.wait_until_idle(None));

    let middle_id = middle.event_id.clone();
    assert!(events_at_subchild.lock().expect("subchild lock").is_empty());
    assert_eq!(
        events_at_child.lock().expect("child lock").as_slice(),
        std::slice::from_ref(&middle_id)
    );
    assert_eq!(
        events_at_parent.lock().expect("parent lock").as_slice(),
        std::slice::from_ref(&middle_id)
    );
    assert_eq!(
        middle._inner_event().inner.lock().event_path,
        vec![child_bus.label(), parent_bus.label()]
    );

    parent_bus.destroy();
    child_bus.destroy();
    subchild_bus.destroy();
}

#[test]
fn test_forwarding_disambiguates_buses_that_share_the_same_name() {
    let bus_a = EventBus::new(Some("SharedName".to_string()));
    let bus_b = EventBus::new(Some("SharedName".to_string()));

    let seen_a = Arc::new(Mutex::new(Vec::new()));
    let seen_b = Arc::new(Mutex::new(Vec::new()));

    let seen_a_handler = seen_a.clone();
    bus_a.on_raw("PingEvent", "seen_a", move |event| {
        let seen = seen_a_handler.clone();
        async move {
            seen.lock()
                .expect("seen_a lock")
                .push(event.inner.lock().event_id.clone());
            Ok(json!(null))
        }
    });
    let seen_b_handler = seen_b.clone();
    bus_b.on_raw("PingEvent", "seen_b", move |event| {
        let seen = seen_b_handler.clone();
        async move {
            seen.lock()
                .expect("seen_b lock")
                .push(event.inner.lock().event_id.clone());
            Ok(json!(null))
        }
    });

    let bus_b_for_forward = bus_b.clone();
    bus_a.on_raw("*", "forward_to_shared_name_peer", move |event| {
        let bus_b = bus_b_for_forward.clone();
        async move {
            bus_b.emit_base(event);
            Ok(json!(null))
        }
    });

    let event = bus_a.emit(PingEvent {
        value: 99,
        ..Default::default()
    });
    let _ = block_on(event.now());
    block_on(bus_a.wait_until_idle(None));
    block_on(bus_b.wait_until_idle(None));

    let event_id = event.event_id.clone();
    assert_eq!(
        seen_a.lock().expect("seen_a lock").as_slice(),
        std::slice::from_ref(&event_id)
    );
    assert_eq!(
        seen_b.lock().expect("seen_b lock").as_slice(),
        std::slice::from_ref(&event_id)
    );
    assert_ne!(bus_a.label(), bus_b.label());
    assert_eq!(
        event._inner_event().inner.lock().event_path,
        vec![bus_a.label(), bus_b.label()]
    );
    bus_a.destroy();
    bus_b.destroy();
}

#[test]
fn test_await_event_now_waits_for_handlers_on_forwarded_buses() {
    let bus_a = EventBus::new(Some("ForwardWaitA".to_string()));
    let bus_b = EventBus::new(Some("ForwardWaitB".to_string()));
    let bus_c = EventBus::new(Some("ForwardWaitC".to_string()));
    let completion_log = Arc::new(Mutex::new(Vec::new()));

    let log_a = completion_log.clone();
    bus_a.on_raw("PingEvent", "handler_a", move |_event| {
        let log = log_a.clone();
        async move {
            thread::sleep(Duration::from_millis(10));
            log.lock().expect("log lock").push("A");
            Ok(json!(null))
        }
    });

    let log_b = completion_log.clone();
    bus_b.on_raw("PingEvent", "handler_b", move |_event| {
        let log = log_b.clone();
        async move {
            thread::sleep(Duration::from_millis(30));
            log.lock().expect("log lock").push("B");
            Ok(json!(null))
        }
    });
    let log_c = completion_log.clone();
    bus_c.on_raw("PingEvent", "handler_c", move |_event| {
        let log = log_c.clone();
        async move {
            thread::sleep(Duration::from_millis(50));
            log.lock().expect("log lock").push("C");
            Ok(json!(null))
        }
    });

    let bus_b_for_forward = bus_b.clone();
    bus_a.on_raw("*", "forward_to_b", move |event| {
        let bus_b = bus_b_for_forward.clone();
        async move {
            bus_b.emit_base(event);
            Ok(json!(null))
        }
    });
    let bus_c_for_forward = bus_c.clone();
    bus_b.on_raw("*", "forward_to_c", move |event| {
        let bus_c = bus_c_for_forward.clone();
        async move {
            bus_c.emit_base(event);
            Ok(json!(null))
        }
    });

    let event = bus_a.emit(PingEvent {
        value: 2,
        ..Default::default()
    });
    let _ = block_on(event.now());
    block_on(bus_a.wait_until_idle(None));
    block_on(bus_b.wait_until_idle(None));
    block_on(bus_c.wait_until_idle(None));

    let mut log = completion_log.lock().expect("log lock").clone();
    log.sort();
    assert_eq!(log, vec!["A", "B", "C"]);
    assert_eq!(event._inner_event().inner.lock().event_pending_bus_count, 0);
    assert_eq!(
        event._inner_event().inner.lock().event_path,
        vec![bus_a.label(), bus_b.label(), bus_c.label()]
    );
    bus_a.destroy();
    bus_b.destroy();
    bus_c.destroy();
}

#[test]
fn test_circular_forwarding_from_first_peer_does_not_loop() {
    let peer1 = EventBus::new(Some("Peer1".to_string()));
    let peer2 = EventBus::new(Some("Peer2".to_string()));
    let peer3 = EventBus::new(Some("Peer3".to_string()));

    let events_at_peer1 = Arc::new(Mutex::new(Vec::new()));
    let events_at_peer2 = Arc::new(Mutex::new(Vec::new()));
    let events_at_peer3 = Arc::new(Mutex::new(Vec::new()));

    for (bus, seen, handler_name) in [
        (peer1.clone(), events_at_peer1.clone(), "seen_peer1"),
        (peer2.clone(), events_at_peer2.clone(), "seen_peer2"),
        (peer3.clone(), events_at_peer3.clone(), "seen_peer3"),
    ] {
        bus.on_raw("PingEvent", handler_name, move |event| {
            let seen = seen.clone();
            async move {
                seen.lock()
                    .expect("seen lock")
                    .push(event.inner.lock().event_id.clone());
                Ok(json!(null))
            }
        });
    }

    let peer2_for_forward = peer2.clone();
    peer1.on_raw("*", "forward_to_peer2", move |event| {
        let peer2 = peer2_for_forward.clone();
        async move {
            peer2.emit_base(event);
            Ok(json!(null))
        }
    });
    let peer3_for_forward = peer3.clone();
    peer2.on_raw("*", "forward_to_peer3", move |event| {
        let peer3 = peer3_for_forward.clone();
        async move {
            peer3.emit_base(event);
            Ok(json!(null))
        }
    });
    let peer1_for_forward = peer1.clone();
    peer3.on_raw("*", "forward_to_peer1", move |event| {
        let peer1 = peer1_for_forward.clone();
        async move {
            peer1.emit_base(event);
            Ok(json!(null))
        }
    });

    let event = peer1.emit(PingEvent {
        value: 42,
        ..Default::default()
    });
    let _ = block_on(event.now());
    block_on(peer1.wait_until_idle(None));
    block_on(peer2.wait_until_idle(None));
    block_on(peer3.wait_until_idle(None));

    let event_id = event.event_id.clone();
    assert_eq!(
        events_at_peer1.lock().expect("peer1 lock").as_slice(),
        std::slice::from_ref(&event_id)
    );
    assert_eq!(
        events_at_peer2.lock().expect("peer2 lock").as_slice(),
        std::slice::from_ref(&event_id)
    );
    assert_eq!(
        events_at_peer3.lock().expect("peer3 lock").as_slice(),
        std::slice::from_ref(&event_id)
    );
    assert_eq!(
        event._inner_event().inner.lock().event_path,
        vec![peer1.label(), peer2.label(), peer3.label()]
    );

    peer1.destroy();
    peer2.destroy();
    peer3.destroy();
}

#[test]
fn test_circular_forwarding_from_middle_peer_does_not_loop() {
    let bus_a = EventBus::new(Some("RacePeer1".to_string()));
    let bus_b = EventBus::new(Some("RacePeer2".to_string()));
    let bus_c = EventBus::new(Some("RacePeer3".to_string()));

    let seen_a = Arc::new(Mutex::new(Vec::new()));
    let seen_b = Arc::new(Mutex::new(Vec::new()));
    let seen_c = Arc::new(Mutex::new(Vec::new()));

    for (bus, seen, handler_name) in [
        (bus_a.clone(), seen_a.clone(), "seen_a"),
        (bus_b.clone(), seen_b.clone(), "seen_b"),
        (bus_c.clone(), seen_c.clone(), "seen_c"),
    ] {
        bus.on_raw("PingEvent", handler_name, move |event| {
            let seen = seen.clone();
            async move {
                seen.lock()
                    .expect("seen lock")
                    .push(event.inner.lock().event_id.clone());
                Ok(json!(null))
            }
        });
    }

    let bus_b_for_forward = bus_b.clone();
    bus_a.on_raw("*", "forward_to_b", move |event| {
        let bus_b = bus_b_for_forward.clone();
        async move {
            bus_b.emit_base(event);
            Ok(json!(null))
        }
    });
    let bus_c_for_forward = bus_c.clone();
    bus_b.on_raw("*", "forward_to_c", move |event| {
        let bus_c = bus_c_for_forward.clone();
        async move {
            bus_c.emit_base(event);
            Ok(json!(null))
        }
    });
    let bus_a_for_forward = bus_a.clone();
    bus_c.on_raw("*", "forward_to_a", move |event| {
        let bus_a = bus_a_for_forward.clone();
        async move {
            bus_a.emit_base(event);
            Ok(json!(null))
        }
    });

    let warmup = bus_a.emit(PingEvent {
        value: 42,
        ..Default::default()
    });
    let _ = block_on(warmup.now());
    block_on(bus_a.wait_until_idle(None));
    block_on(bus_b.wait_until_idle(None));
    block_on(bus_c.wait_until_idle(None));
    seen_a.lock().expect("seen_a lock").clear();
    seen_b.lock().expect("seen_b lock").clear();
    seen_c.lock().expect("seen_c lock").clear();

    let event = bus_b.emit(PingEvent {
        value: 7,
        ..Default::default()
    });
    let _ = block_on(event.now());
    block_on(bus_a.wait_until_idle(None));
    block_on(bus_b.wait_until_idle(None));
    block_on(bus_c.wait_until_idle(None));

    let event_id = event.event_id.clone();
    assert_eq!(
        seen_a.lock().expect("seen_a lock").as_slice(),
        std::slice::from_ref(&event_id)
    );
    assert_eq!(
        seen_b.lock().expect("seen_b lock").as_slice(),
        std::slice::from_ref(&event_id)
    );
    assert_eq!(
        seen_c.lock().expect("seen_c lock").as_slice(),
        std::slice::from_ref(&event_id)
    );
    assert_eq!(
        event._inner_event().inner.lock().event_path,
        vec![bus_b.label(), bus_c.label(), bus_a.label()]
    );
    assert_eq!(
        event.event_status.read(),
        EventStatus::Completed
    );
    bus_a.destroy();
    bus_b.destroy();
    bus_c.destroy();
}

#[test]
fn test_await_event_now_waits_when_forwarding_handler_is_async_delayed() {
    let bus_a = EventBus::new(Some("BusADelayedForward".to_string()));
    let bus_b = EventBus::new(Some("BusBDelayedForward".to_string()));

    let bus_a_done = Arc::new(Mutex::new(false));
    let bus_b_done = Arc::new(Mutex::new(false));

    let bus_a_done_handler = bus_a_done.clone();
    bus_a.on_raw("PingEvent", "handler_a", move |_event| {
        let done = bus_a_done_handler.clone();
        async move {
            thread::sleep(Duration::from_millis(20));
            *done.lock().expect("bus_a_done lock") = true;
            Ok(json!(null))
        }
    });

    let bus_b_done_handler = bus_b_done.clone();
    bus_b.on_raw("PingEvent", "handler_b", move |_event| {
        let done = bus_b_done_handler.clone();
        async move {
            thread::sleep(Duration::from_millis(10));
            *done.lock().expect("bus_b_done lock") = true;
            Ok(json!(null))
        }
    });

    let bus_b_for_forward = bus_b.clone();
    bus_a.on_raw("*", "delayed_forward_to_b", move |event| {
        let bus_b = bus_b_for_forward.clone();
        async move {
            thread::sleep(Duration::from_millis(30));
            bus_b.emit_base(event);
            Ok(json!(null))
        }
    });

    let event = bus_a.emit(PingEvent {
        value: 3,
        ..Default::default()
    });
    let _ = block_on(event.now());

    assert!(*bus_a_done.lock().expect("bus_a_done lock"));
    assert!(*bus_b_done.lock().expect("bus_b_done lock"));
    assert_eq!(event._inner_event().inner.lock().event_pending_bus_count, 0);
    assert_eq!(
        event._inner_event().inner.lock().event_path,
        vec![bus_a.label(), bus_b.label()]
    );
    bus_a.destroy();
    bus_b.destroy();
}

#[test]
fn test_forwarding_same_event_does_not_set_self_parent_id() {
    let origin = EventBus::new(Some("SelfParentOrigin".to_string()));
    let target = EventBus::new(Some("SelfParentTarget".to_string()));

    origin.on_raw("PingEvent", "origin_handler", |_event| async move {
        Ok(json!("origin-ok"))
    });
    target.on_raw("PingEvent", "target_handler", |_event| async move {
        Ok(json!("target-ok"))
    });

    let target_for_forward = target.clone();
    origin.on_raw("*", "forward_to_target", move |event| {
        let target = target_for_forward.clone();
        async move {
            target.emit_base(event);
            Ok(json!(null))
        }
    });

    let event = origin.emit(PingEvent {
        value: 9,
        ..Default::default()
    });
    let _ = block_on(event.now());
    block_on(origin.wait_until_idle(None));
    block_on(target.wait_until_idle(None));

    assert_eq!(event.event_parent_id.clone(), None);
    assert_eq!(
        event._inner_event().inner.lock().event_path,
        vec![origin.label(), target.label()]
    );
    origin.destroy();
    target.destroy();
}

#[test]
fn test_forwarded_event_uses_processing_bus_defaults() {
    let bus_a_timeout = 1.5;
    let bus_b_timeout = 2.5;
    let bus_a = EventBus::new_with_options(
        Some("ForwardDefaultsA".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_handler_completion: EventHandlerCompletionMode::All,
            event_timeout: Some(bus_a_timeout),
            ..EventBusOptions::default()
        },
    );
    let bus_b = EventBus::new_with_options(
        Some("ForwardDefaultsB".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            event_handler_completion: EventHandlerCompletionMode::All,
            event_timeout: Some(bus_b_timeout),
            ..EventBusOptions::default()
        },
    );
    let log = Arc::new(Mutex::new(Vec::new()));
    let inherited_ref: Arc<Mutex<Option<Arc<abxbus_rust::base_event::BaseEvent>>>> =
        Arc::new(Mutex::new(None));

    let log_b1 = log.clone();
    bus_b.on_raw("ForwardedDefaultsChildEvent", "b1", move |event| {
        let log = log_b1.clone();
        async move {
            {
                let inner = event.inner.lock();
                assert_eq!(inner.event_timeout, None);
                assert_eq!(inner.event_handler_concurrency, None);
                assert_eq!(inner.event_handler_completion, None);
            }
            let mode = event
                .inner
                .lock()
                .payload
                .get("mode")
                .and_then(serde_json::Value::as_str)
                .expect("mode")
                .to_string();
            push_log(&log, &format!("{mode}:b1_start"));
            thread::sleep(Duration::from_millis(15));
            push_log(&log, &format!("{mode}:b1_end"));
            Ok(json!("b1"))
        }
    });
    let log_b2 = log.clone();
    bus_b.on_raw("ForwardedDefaultsChildEvent", "b2", move |event| {
        let log = log_b2.clone();
        async move {
            {
                let inner = event.inner.lock();
                assert_eq!(inner.event_timeout, None);
                assert_eq!(inner.event_handler_concurrency, None);
                assert_eq!(inner.event_handler_completion, None);
            }
            let mode = event
                .inner
                .lock()
                .payload
                .get("mode")
                .and_then(serde_json::Value::as_str)
                .expect("mode")
                .to_string();
            push_log(&log, &format!("{mode}:b2_start"));
            thread::sleep(Duration::from_millis(5));
            push_log(&log, &format!("{mode}:b2_end"));
            Ok(json!("b2"))
        }
    });

    let bus_a_for_trigger = bus_a.clone();
    let bus_b_for_trigger = bus_b.clone();
    let inherited_ref_for_trigger = inherited_ref.clone();
    bus_a.on_raw(
        "ForwardedDefaultsTriggerEvent",
        "trigger_handler",
        move |_event| {
            let bus_a = bus_a_for_trigger.clone();
            let bus_b = bus_b_for_trigger.clone();
            let inherited_ref = inherited_ref_for_trigger.clone();
            async move {
                let inherited = bus_a.emit_child(DefaultsChildEvent {
                    mode: "inherited".to_string(),
                    ..Default::default()
                });
                *inherited_ref.lock().expect("inherited ref") = Some(inherited._inner_event());
                bus_b.emit(inherited.clone());
                let _ = inherited.now().await;
                Ok(json!(null))
            }
        },
    );

    let top = bus_a.emit_base(abxbus_rust::base_event::BaseEvent::new(
        "ForwardedDefaultsTriggerEvent",
        serde_json::Map::new(),
    ));
    let _ = block_on(top.now());
    block_on(bus_a.wait_until_idle(Some(2.0)));
    block_on(bus_b.wait_until_idle(Some(2.0)));

    let log = log.lock().expect("log lock").clone();
    assert!(
        forwarding_index_of(&log, "inherited:b2_start")
            < forwarding_index_of(&log, "inherited:b1_end")
    );
    let inherited = inherited_ref
        .lock()
        .expect("inherited ref")
        .clone()
        .expect("inherited event");
    let inner = inherited.inner.lock();
    assert_eq!(inner.event_timeout, None);
    assert_eq!(inner.event_handler_concurrency, None);
    assert_eq!(inner.event_handler_completion, None);
    let bus_b_results: Vec<_> = inner
        .event_results
        .values()
        .filter(|result| result.handler.eventbus_id == bus_b.id)
        .collect();
    assert!(!bus_b_results.is_empty());
    assert!(bus_b_results
        .iter()
        .all(|result| result.timeout == Some(bus_b_timeout)));
    drop(inner);
    bus_a.destroy();
    bus_b.destroy();
}

#[test]
fn test_forwarded_event_preserves_explicit_handler_concurrency_override() {
    let bus_a = new_forwarding_bus(
        "ForwardOverrideA",
        EventHandlerConcurrencyMode::Parallel,
        EventHandlerCompletionMode::All,
    );
    let bus_b = new_forwarding_bus(
        "ForwardOverrideB",
        EventHandlerConcurrencyMode::Parallel,
        EventHandlerCompletionMode::All,
    );
    let log = Arc::new(Mutex::new(Vec::new()));

    let log_b1 = log.clone();
    bus_b.on_raw("ForwardedDefaultsChildEvent", "b1", move |event| {
        let log = log_b1.clone();
        async move {
            let mode = event
                .inner
                .lock()
                .payload
                .get("mode")
                .and_then(serde_json::Value::as_str)
                .expect("mode")
                .to_string();
            push_log(&log, &format!("{mode}:b1_start"));
            thread::sleep(Duration::from_millis(15));
            push_log(&log, &format!("{mode}:b1_end"));
            Ok(json!("b1"))
        }
    });
    let log_b2 = log.clone();
    bus_b.on_raw("ForwardedDefaultsChildEvent", "b2", move |event| {
        let log = log_b2.clone();
        async move {
            let mode = event
                .inner
                .lock()
                .payload
                .get("mode")
                .and_then(serde_json::Value::as_str)
                .expect("mode")
                .to_string();
            push_log(&log, &format!("{mode}:b2_start"));
            thread::sleep(Duration::from_millis(5));
            push_log(&log, &format!("{mode}:b2_end"));
            Ok(json!("b2"))
        }
    });

    let bus_a_for_trigger = bus_a.clone();
    let bus_b_for_trigger = bus_b.clone();
    bus_a.on_raw(
        "ForwardedDefaultsTriggerEvent",
        "trigger_handler",
        move |_event| {
            let bus_a = bus_a_for_trigger.clone();
            let bus_b = bus_b_for_trigger.clone();
            async move {
                let mut override_event = DefaultsChildEvent {
                    mode: "override".to_string(),
                    ..Default::default()
                };
                override_event.event_handler_concurrency =
                    Some(EventHandlerConcurrencyMode::Serial);
                let override_event = bus_a.emit_child(override_event);
                bus_b.emit(override_event.clone());
                let _ = override_event.now().await;
                Ok(json!(null))
            }
        },
    );

    let top = bus_a.emit_base(abxbus_rust::base_event::BaseEvent::new(
        "ForwardedDefaultsTriggerEvent",
        serde_json::Map::new(),
    ));
    let _ = block_on(top.now());
    block_on(bus_a.wait_until_idle(Some(2.0)));
    block_on(bus_b.wait_until_idle(Some(2.0)));

    let log = log.lock().expect("log lock").clone();
    assert!(
        forwarding_index_of(&log, "override:b1_end")
            < forwarding_index_of(&log, "override:b2_start")
    );
    bus_a.destroy();
    bus_b.destroy();
}

#[test]
fn test_forwarded_first_mode_uses_processing_bus_handler_concurrency_defaults() {
    let bus_a = new_forwarding_bus(
        "ForwardedFirstDefaultsA",
        EventHandlerConcurrencyMode::Serial,
        EventHandlerCompletionMode::All,
    );
    let bus_b = new_forwarding_bus(
        "ForwardedFirstDefaultsB",
        EventHandlerConcurrencyMode::Parallel,
        EventHandlerCompletionMode::First,
    );
    let log = Arc::new(Mutex::new(Vec::new()));

    let bus_b_for_forward = bus_b.clone();
    bus_a.on_raw("*", "forward_to_b", move |event| {
        let bus_b = bus_b_for_forward.clone();
        async move {
            bus_b.emit_base(event);
            Ok(json!(null))
        }
    });

    let slow_log = log.clone();
    bus_b.on_raw("ForwardedFirstDefaultsEvent", "slow", move |_event| {
        let log = slow_log.clone();
        async move {
            push_log(&log, "slow_start");
            thread::sleep(Duration::from_millis(20));
            push_log(&log, "slow_end");
            Ok(json!("slow"))
        }
    });
    let fast_log = log.clone();
    bus_b.on_raw("ForwardedFirstDefaultsEvent", "fast", move |_event| {
        let log = fast_log.clone();
        async move {
            push_log(&log, "fast_start");
            thread::sleep(Duration::from_millis(1));
            push_log(&log, "fast_end");
            Ok(json!("fast"))
        }
    });

    let event = bus_a.emit(ForwardedFirstDefaultsEvent {
        ..Default::default()
    });
    let _ = block_on(event.now_with_options(EventWaitOptions {
        timeout: None,
        first_result: true,
    }));
    let result = block_on(event.event_result_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: false,
        include: None,
    }))
    .expect("first result");
    block_on(bus_a.wait_until_idle(Some(2.0)));
    block_on(bus_b.wait_until_idle(Some(2.0)));

    let log = log.lock().expect("log lock").clone();
    assert_eq!(result, Some("fast".to_string()));
    assert!(log.contains(&"slow_start".to_string()));
    assert!(log.contains(&"fast_start".to_string()));
    bus_a.destroy();
    bus_b.destroy();
}

#[test]
fn test_proxy_dispatch_auto_links_child_events_like_emit() {
    let bus = EventBus::new(Some("ProxyDispatchAutoLinkBus".to_string()));
    let bus_for_root = bus.clone();

    bus.on_raw("ProxyDispatchRootEvent", "root_handler", move |_event| {
        let bus = bus_for_root.clone();
        async move {
            bus.emit_child(ProxyDispatchChildEvent {
                ..Default::default()
            });
            Ok(json!("root"))
        }
    });
    bus.on_raw(
        "ProxyDispatchChildEvent",
        "child_handler",
        |_event| async move { Ok(json!("child")) },
    );

    let root = bus.emit(ProxyDispatchRootEvent {
        ..Default::default()
    });
    let _ = block_on(root.now());
    block_on(bus.wait_until_idle(None));

    let root_id = root.event_id.clone();
    let child_ids: Vec<String> = root
        ._inner_event().inner.lock()
        .event_results
        .values()
        .flat_map(|result| result.event_children.clone())
        .collect();
    assert_eq!(child_ids.len(), 1);
    let payload = bus.runtime_payload_for_test();
    let child = payload.get(&child_ids[0]).cloned().expect("child event");
    assert_eq!(
        child.inner.lock().event_parent_id.as_deref(),
        Some(root_id.as_str())
    );
    assert_eq!(child.inner.lock().event_id, child_ids[0]);
    bus.destroy();
}

#[test]
fn test_proxy_dispatch_of_same_event_does_not_self_parent_or_self_link_child() {
    let bus = EventBus::new(Some("ProxyDispatchSameEventBus".to_string()));
    let bus_for_root = bus.clone();

    bus.on_raw("ProxyDispatchRootEvent", "root_handler", move |event| {
        let bus = bus_for_root.clone();
        async move {
            bus.emit_base(event);
            Ok(json!("root"))
        }
    });

    let root = bus.emit(ProxyDispatchRootEvent {
        ..Default::default()
    });
    let _ = block_on(root.now());
    block_on(bus.wait_until_idle(None));

    let base = root._inner_event();
    let inner = base.inner.lock();
    let child_ids: Vec<String> = inner
        .event_results
        .values()
        .flat_map(|result| result.event_children.clone())
        .collect();
    assert_eq!(inner.event_parent_id, None);
    assert!(child_ids.is_empty());
    drop(inner);
    bus.destroy();
}

#[test]
fn test_events_are_processed_in_fifo_order() {
    let bus = EventBus::new(Some("FifoBus".to_string()));
    let processed_orders = Arc::new(Mutex::new(Vec::new()));

    let processed_orders_handler = processed_orders.clone();
    bus.on_raw("OrderEvent", "order_handler", move |event| {
        let processed_orders = processed_orders_handler.clone();
        async move {
            let order = event
                .inner
                .lock()
                .payload
                .get("order")
                .and_then(|value| value.as_i64())
                .expect("order payload");
            if order % 2 == 0 {
                thread::sleep(Duration::from_millis(30));
            } else {
                thread::sleep(Duration::from_millis(5));
            }
            processed_orders.lock().expect("orders lock").push(order);
            Ok(json!(null))
        }
    });

    for order in 0..10 {
        bus.emit(OrderEvent {
            order,
            ..Default::default()
        });
    }

    block_on(bus.wait_until_idle(None));
    assert_eq!(
        processed_orders.lock().expect("orders lock").as_slice(),
        &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    );
    bus.destroy();
}
