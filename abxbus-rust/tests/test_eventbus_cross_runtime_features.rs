use abxbus_rust::event;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use abxbus_rust::{
    event_bus::{EventBus, EventBusOptions, FindOptions},
    types::{EventConcurrencyMode, EventHandlerConcurrencyMode, EventStatus},
};
use futures::executor::block_on;
use serde_json::{json, Value};

event! {
    struct QueueJumpRootEvent {
        event_result_type: String,
        event_type: "QueueJumpRootEvent",
    }
}
event! {
    struct QueueJumpChildEvent {
        event_result_type: String,
        event_type: "QueueJumpChildEvent",
    }
}
event! {
    struct QueueJumpSiblingEvent {
        event_result_type: String,
        event_type: "QueueJumpSiblingEvent",
    }
}
event! {
    struct ConcurrencyIntersectionEvent {
        token: i64,
        event_result_type: String,
        event_type: "ConcurrencyIntersectionEvent",
    }
}
event! {
    struct TimeoutEnforcementEvent {
        event_result_type: String,
        event_type: "TimeoutEnforcementEvent",
        event_timeout: 0.02,
    }
}
event! {
    struct TimeoutFollowupEvent {
        event_result_type: String,
        event_type: "TimeoutFollowupEvent",
    }
}
event! {
    struct ZeroHistoryEvent {
        value: String,
        event_result_type: String,
        event_type: "ZeroHistoryEvent",
    }
}
event! {
    struct ContextParentEvent {
        event_result_type: String,
        event_type: "ContextParentEvent",
    }
}
event! {
    struct ContextChildEvent {
        event_result_type: String,
        event_type: "ContextChildEvent",
    }
}
event! {
    struct PendingVisibilityEvent {
        tag: String,
        event_result_type: String,
        event_type: "PendingVisibilityEvent",
    }
}
event! {
    struct BackpressureEvent {
        value: String,
        event_result_type: String,
        event_type: "BackpressureEvent",
    }
}
fn push(order: &Arc<Mutex<Vec<String>>>, entry: &str) {
    order.lock().expect("order lock").push(entry.to_string());
}

fn where_eq(key: &str, value: Value) -> HashMap<String, Value> {
    HashMap::from([(key.to_string(), value)])
}

#[test]
fn test_queue_jump_preserves_parent_child_lineage_and_find_visibility() {
    let bus = EventBus::new_with_options(
        Some("ParityQueueJumpBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let execution_order = Arc::new(Mutex::new(Vec::new()));
    let child_event_id = Arc::new(Mutex::new(None::<String>));

    let bus_for_root = bus.clone();
    let order_for_root = execution_order.clone();
    bus.on_raw("QueueJumpRootEvent", "on_root", move |_event| {
        let bus = bus_for_root.clone();
        let order = order_for_root.clone();
        async move {
            push(&order, "root:start");
            let child = bus.emit_child(QueueJumpChildEvent {
                ..Default::default()
            });
            let _ = child.now().await;
            push(&order, "root:end");
            Ok(json!("root-ok"))
        }
    });

    let order_for_child = execution_order.clone();
    let child_id_for_child = child_event_id.clone();
    bus.on_raw("QueueJumpChildEvent", "on_child", move |event| {
        let order = order_for_child.clone();
        let child_event_id = child_id_for_child.clone();
        async move {
            *child_event_id.lock().expect("child id lock") =
                Some(event.inner.lock().event_id.clone());
            push(&order, "child");
            thread::sleep(Duration::from_millis(5));
            Ok(json!("child-ok"))
        }
    });

    let order_for_sibling = execution_order.clone();
    bus.on_raw("QueueJumpSiblingEvent", "on_sibling", move |_event| {
        let order = order_for_sibling.clone();
        async move {
            push(&order, "sibling");
            Ok(json!("sibling-ok"))
        }
    });

    let root = bus.emit(QueueJumpRootEvent {
        ..Default::default()
    });
    let sibling = bus.emit(QueueJumpSiblingEvent {
        ..Default::default()
    });
    let _ = block_on(root.now());
    let _ = block_on(sibling.now());
    block_on(bus.wait_until_idle(Some(2.0)));

    assert_eq!(
        execution_order.lock().expect("order lock").as_slice(),
        ["root:start", "child", "root:end", "sibling"]
    );

    let found_child =
        block_on(bus.find("QueueJumpChildEvent", true, None, Some(root.inner.clone())))
            .expect("child should be findable");
    let expected_child_id = child_event_id
        .lock()
        .expect("child id lock")
        .clone()
        .expect("captured child id");
    assert_eq!(found_child.inner.lock().event_id, expected_child_id);
    assert_eq!(
        found_child.inner.lock().event_parent_id.as_deref(),
        Some(root.inner.inner.lock().event_id.as_str())
    );
    let root_result = root
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .find(|result| result.handler.handler_name == "on_root")
        .cloned()
        .expect("root result");
    assert!(root_result.event_children.contains(&expected_child_id));
    bus.stop();
}

#[test]
fn test_concurrency_intersection_parallel_events_with_serial_handlers_stays_serial_per_event() {
    let bus = EventBus::new_with_options(
        Some("ParityConcurrencyIntersectionBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::Parallel,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            max_history_size: None,
            ..EventBusOptions::default()
        },
    );

    let current_by_event = Arc::new(Mutex::new(HashMap::<String, i64>::new()));
    let max_by_event = Arc::new(Mutex::new(HashMap::<String, i64>::new()));
    let global_current = Arc::new(Mutex::new(0_i64));
    let global_max = Arc::new(Mutex::new(0_i64));

    for handler_name in ["tracked_handler_a", "tracked_handler_b"] {
        let current_by_event = current_by_event.clone();
        let max_by_event = max_by_event.clone();
        let global_current = global_current.clone();
        let global_max = global_max.clone();
        bus.on_raw("ConcurrencyIntersectionEvent", handler_name, move |event| {
            let current_by_event = current_by_event.clone();
            let max_by_event = max_by_event.clone();
            let global_current = global_current.clone();
            let global_max = global_max.clone();
            async move {
                let event_id = event.inner.lock().event_id.clone();
                {
                    let mut current_by_event =
                        current_by_event.lock().expect("current_by_event lock");
                    let current = current_by_event.entry(event_id.clone()).or_insert(0);
                    *current += 1;
                    let mut max_by_event = max_by_event.lock().expect("max_by_event lock");
                    let max = max_by_event.entry(event_id.clone()).or_insert(0);
                    *max = (*max).max(*current);
                }
                {
                    let mut current = global_current.lock().expect("global current lock");
                    *current += 1;
                    let mut max = global_max.lock().expect("global max lock");
                    *max = (*max).max(*current);
                }
                thread::sleep(Duration::from_millis(10));
                {
                    let mut current_by_event =
                        current_by_event.lock().expect("current_by_event lock");
                    *current_by_event.get_mut(&event_id).expect("event current") -= 1;
                }
                *global_current.lock().expect("global current lock") -= 1;
                Ok(json!("ok"))
            }
        });
    }

    let events: Vec<_> = (0..8)
        .map(|token| {
            bus.emit(ConcurrencyIntersectionEvent {
                token,
                ..Default::default()
            })
        })
        .collect();
    for event in &events {
        let _ = block_on(event.now());
    }
    block_on(bus.wait_until_idle(Some(2.0)));

    let max_by_event = max_by_event.lock().expect("max_by_event lock").clone();
    for event in events {
        let event_id = event.inner.inner.lock().event_id.clone();
        assert_eq!(max_by_event.get(&event_id), Some(&1));
        assert!(
            event
                .inner
                .inner
                .lock()
                .event_results
                .values()
                .all(|result| result.status
                    == abxbus_rust::event_result::EventResultStatus::Completed)
        );
    }
    assert!(*global_max.lock().expect("global max lock") >= 2);
    bus.stop();
}

#[test]
fn test_timeout_enforcement_preserves_follow_up_processing_and_queue_state() {
    let bus = EventBus::new_with_options(
        Some("ParityTimeoutEnforcementBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );

    bus.on_raw(
        "TimeoutEnforcementEvent",
        "slow_handler_a",
        |_event| async move {
            thread::sleep(Duration::from_millis(200));
            Ok(json!("slow-a"))
        },
    );
    bus.on_raw(
        "TimeoutEnforcementEvent",
        "slow_handler_b",
        |_event| async move {
            thread::sleep(Duration::from_millis(200));
            Ok(json!("slow-b"))
        },
    );
    bus.on_raw(
        "TimeoutFollowupEvent",
        "followup_handler",
        |_event| async move { Ok(json!("followup-ok")) },
    );

    let timed_out = bus.emit(TimeoutEnforcementEvent {
        ..Default::default()
    });
    let _ = block_on(timed_out.now());
    assert_eq!(
        timed_out.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    assert!(timed_out
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .all(|result| result.status == abxbus_rust::event_result::EventResultStatus::Error));

    let followup = bus.emit(TimeoutFollowupEvent {
        ..Default::default()
    });
    let _ = block_on(followup.now());
    assert!(followup
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .all(|result| result.status == abxbus_rust::event_result::EventResultStatus::Completed));
    assert_eq!(
        followup
            .inner
            .inner
            .lock()
            .event_results
            .values()
            .next()
            .and_then(|result| result.result.clone()),
        Some(json!("followup-ok"))
    );

    block_on(bus.wait_until_idle(Some(2.0)));
    assert!(bus.is_idle_and_queue_empty());
    bus.stop();
}

#[test]
fn test_zero_history_backpressure_with_find_future_still_resolves_new_events() {
    let bus = EventBus::new_with_history(Some("ParityZeroHistoryBus".to_string()), Some(0), false);
    bus.on_raw("ZeroHistoryEvent", "handler", |event| async move {
        let value = event
            .inner
            .lock()
            .payload
            .get("value")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
            .to_string();
        Ok(json!(format!("ok:{value}")))
    });

    let first = bus.emit(ZeroHistoryEvent {
        value: "first".to_string(),
        ..Default::default()
    });
    let first_id = first.inner.inner.lock().event_id.clone();
    let _ = block_on(first.now());
    assert!(!bus.event_history_ids().contains(&first_id));
    assert!(block_on(bus.find("ZeroHistoryEvent", true, None, None)).is_none());

    let bus_for_dispatch = bus.clone();
    let captured_future_id = Arc::new(Mutex::new(None::<String>));
    let captured_for_dispatch = captured_future_id.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        let future_event = ZeroHistoryEvent {
            value: "future".to_string(),
            event_id: "018f8e40-1234-7000-8000-00000000f000".to_string(),
            ..Default::default()
        };
        let future_event_id = future_event.event_id.clone();
        *captured_for_dispatch
            .lock()
            .expect("captured future id lock") = Some(future_event_id);
        bus_for_dispatch.emit(future_event);
    });

    let future_match = block_on(bus.find_with_options(
        "ZeroHistoryEvent",
        FindOptions {
            past: false,
            future: Some(1.0),
            where_filter: Some(where_eq("value", json!("future"))),
            ..FindOptions::default()
        },
    ))
    .expect("future match should resolve");

    assert_eq!(
        future_match
            .inner
            .lock()
            .payload
            .get("value")
            .and_then(Value::as_str),
        Some("future")
    );
    let captured_future_id = captured_future_id
        .lock()
        .expect("captured future id lock")
        .clone()
        .expect("captured future id");
    assert_eq!(future_match.inner.lock().event_id, captured_future_id);
    block_on(bus.wait_until_idle(Some(2.0)));
    assert_eq!(bus.event_history_size(), 0);
    bus.stop();
}

#[test]
fn test_context_propagates_through_forwarding_and_child_dispatch_with_lineage_intact() {
    let bus_a = EventBus::new(Some("ParityContextForwardA".to_string()));
    let bus_b = EventBus::new(Some("ParityContextForwardB".to_string()));
    let parent_event_id = Arc::new(Mutex::new(None::<String>));
    let child_parent_id = Arc::new(Mutex::new(None::<String>));

    let bus_b_forward = bus_b.clone();
    bus_a.on_raw("*", "forward_to_b", move |event| {
        let bus_b = bus_b_forward.clone();
        async move {
            bus_b.emit_base(event);
            Ok(json!(null))
        }
    });

    let bus_b_for_parent = bus_b.clone();
    let parent_event_id_for_parent = parent_event_id.clone();
    bus_b.on_raw("ContextParentEvent", "on_parent", move |event| {
        let bus_b = bus_b_for_parent.clone();
        let parent_event_id = parent_event_id_for_parent.clone();
        async move {
            *parent_event_id.lock().expect("parent id lock") =
                Some(event.inner.lock().event_id.clone());
            let child = bus_b.emit_child(ContextChildEvent {
                ..Default::default()
            });
            let _ = child.now().await;
            Ok(json!("parent-ok"))
        }
    });

    let child_parent_id_for_child = child_parent_id.clone();
    bus_b.on_raw("ContextChildEvent", "on_child", move |event| {
        let child_parent_id = child_parent_id_for_child.clone();
        async move {
            *child_parent_id.lock().expect("child parent id lock") =
                event.inner.lock().event_parent_id.clone();
            Ok(json!("child-ok"))
        }
    });

    let parent = bus_a.emit(ContextParentEvent {
        ..Default::default()
    });
    let _ = block_on(parent.now());
    block_on(bus_b.wait_until_idle(Some(2.0)));

    let parent_event_id = parent_event_id
        .lock()
        .expect("parent id lock")
        .clone()
        .expect("captured parent id");
    assert_eq!(
        child_parent_id
            .lock()
            .expect("child parent id lock")
            .as_deref(),
        Some(parent_event_id.as_str())
    );
    assert!(parent
        .inner
        .inner
        .lock()
        .event_path
        .first()
        .is_some_and(|path| path.starts_with("ParityContextForwardA#")));
    assert!(parent
        .inner
        .inner
        .lock()
        .event_path
        .iter()
        .any(|path| path.starts_with("ParityContextForwardB#")));

    let found_child =
        block_on(bus_b.find("ContextChildEvent", true, None, Some(parent.inner.clone())))
            .expect("child should be findable");
    assert_eq!(
        found_child.inner.lock().event_parent_id.as_deref(),
        Some(parent.inner.inner.lock().event_id.as_str())
    );
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_pending_queue_find_visibility_transitions_to_completed_after_release() {
    let bus = EventBus::new_with_options(
        Some("ParityPendingFindBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            max_history_size: None,
            ..EventBusOptions::default()
        },
    );
    let (started_tx, started_rx) = std::sync::mpsc::channel();

    bus.on_raw("PendingVisibilityEvent", "handler", move |event| {
        let started_tx = started_tx.clone();
        async move {
            let tag = event
                .inner
                .lock()
                .payload
                .get("tag")
                .and_then(Value::as_str)
                .unwrap_or("<missing>")
                .to_string();
            if tag == "blocking" {
                let _ = started_tx.send(());
                thread::sleep(Duration::from_millis(80));
            }
            Ok(json!(format!("ok:{tag}")))
        }
    });

    let blocking = bus.emit(PendingVisibilityEvent {
        tag: "blocking".to_string(),
        ..Default::default()
    });
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("blocking handler should start");
    let queued = bus.emit(PendingVisibilityEvent {
        tag: "queued".to_string(),
        ..Default::default()
    });
    thread::sleep(Duration::from_millis(10));

    let pending = block_on(bus.find_with_options(
        "PendingVisibilityEvent",
        FindOptions {
            past: true,
            where_filter: Some(HashMap::from([
                ("tag".to_string(), json!("queued")),
                ("event_status".to_string(), json!("pending")),
            ])),
            ..FindOptions::default()
        },
    ))
    .expect("pending queued event should be visible");
    let pending_id = pending.inner.lock().event_id.clone();
    let queued_id = queued.inner.inner.lock().event_id.clone();
    assert_eq!(pending_id, queued_id);

    let _ = block_on(blocking.now());
    let _ = block_on(queued.now());
    block_on(bus.wait_until_idle(Some(2.0)));

    let completed = block_on(bus.find_with_options(
        "PendingVisibilityEvent",
        FindOptions {
            past: true,
            where_filter: Some(HashMap::from([
                ("tag".to_string(), json!("queued")),
                ("event_status".to_string(), json!("completed")),
            ])),
            ..FindOptions::default()
        },
    ))
    .expect("completed queued event should be visible");
    let completed_id = completed.inner.lock().event_id.clone();
    let queued_id = queued.inner.inner.lock().event_id.clone();
    assert_eq!(completed_id, queued_id);
    assert!(bus.is_idle_and_queue_empty());
    bus.stop();
}

#[test]
fn test_history_backpressure_rejects_overflow_and_preserves_findable_history() {
    let bus = EventBus::new_with_history(Some("ParityBackpressureBus".to_string()), Some(1), false);
    bus.on_raw("BackpressureEvent", "handler", |event| async move {
        let value = event
            .inner
            .lock()
            .payload
            .get("value")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
            .to_string();
        Ok(json!(format!("ok:{value}")))
    });

    let first = bus.emit(BackpressureEvent {
        value: "first".to_string(),
        ..Default::default()
    });
    let first_id = first.inner.inner.lock().event_id.clone();
    let _ = block_on(first.now());
    assert_eq!(bus.event_history_size(), 1);
    assert!(bus.event_history_ids().contains(&first_id));

    let found_first = block_on(bus.find_with_options(
        "BackpressureEvent",
        FindOptions {
            past: true,
            where_filter: Some(where_eq("value", json!("first"))),
            ..FindOptions::default()
        },
    ))
    .expect("first event remains findable");
    assert_eq!(found_first.inner.lock().event_id, first_id);

    let overflow = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        bus.emit(BackpressureEvent {
            value: "second".to_string(),
            ..Default::default()
        });
    }));
    assert!(overflow.is_err());
    assert_eq!(bus.event_history_size(), 1);
    assert!(bus.event_history_ids().contains(&first_id));
    assert!(bus.is_idle_and_queue_empty());
    bus.stop();
}

#[test]
fn test_concurrency_intersection_parallel_events_with_serial_handlers() {
    test_concurrency_intersection_parallel_events_with_serial_handlers_stays_serial_per_event();
}

#[test]
fn test_timeout_enforcement_does_not_break_followup_processing_or_queue_state() {
    test_timeout_enforcement_preserves_follow_up_processing_and_queue_state();
}
