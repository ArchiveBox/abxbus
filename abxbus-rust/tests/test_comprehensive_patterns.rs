use abxbus_rust::event;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, Mutex, MutexGuard, OnceLock,
    },
    thread,
    time::Duration,
};

use abxbus_rust::{
    base_event::EventWaitOptions,
    event_bus::{EventBus, EventBusOptions},
    typed::BaseEventHandle,
    types::{
        EventConcurrencyMode, EventHandlerCompletionMode, EventHandlerConcurrencyMode, EventStatus,
    },
};
use futures::{executor::block_on, join};
use serde_json::json;

fn unique_bus_name(prefix: &str) -> String {
    static NEXT_ID: AtomicUsize = AtomicUsize::new(1);
    format!(
        "Comprehensive{prefix}{}",
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    )
}

event! {
    struct Event1 {
        event_result_type: String,
        event_type: "Event1",
    }
}
event! {
    struct Event2 {
        event_result_type: String,
        event_type: "Event2",
    }
}
event! {
    struct Event3 {
        event_result_type: String,
        event_type: "Event3",
    }
}
event! {
    struct ChildEvent {
        event_result_type: String,
        event_type: "ChildEvent",
    }
}
event! {
    struct ChildA {
        event_result_type: String,
        event_type: "ChildA",
    }
}
event! {
    struct ChildB {
        event_result_type: String,
        event_type: "ChildB",
    }
}
event! {
    struct ChildC {
        event_result_type: String,
        event_type: "ChildC",
    }
}
event! {
    struct Child1 {
        event_result_type: String,
        event_type: "Child1",
    }
}
event! {
    struct Child2 {
        event_result_type: String,
        event_type: "Child2",
    }
}
event! {
    struct ParentEvent {
        event_result_type: String,
        event_type: "ParentEvent",
    }
}
event! {
    struct ImmediateChildEvent {
        event_result_type: String,
        event_type: "ImmediateChildEvent",
    }
}
event! {
    struct QueuedChildEvent {
        event_result_type: String,
        event_type: "QueuedChildEvent",
    }
}
event! {
    struct RootEvent {
        event_result_type: String,
        event_type: "RootEvent",
    }
}
event! {
    struct Event4 {
        event_result_type: String,
        event_type: "Event4",
    }
}
event! {
    struct SlowEvent {
        event_result_type: String,
        event_type: "SlowEvent",
    }
}
event! {
    struct DefaultsChildEvent {
        mode: String,
        event_result_type: String,
        event_type: "DefaultsChildEvent",
    }
}
event! {
    struct ForwardedFirstEvent {
        event_result_type: String,
        event_type: "ForwardedFirstEvent",
    }
}
fn push(order: &Arc<Mutex<Vec<String>>>, entry: &str) {
    order.lock().expect("order lock").push(entry.to_string());
}

fn index_of(order: &[String], entry: &str) -> usize {
    order
        .iter()
        .position(|value| value == entry)
        .unwrap_or_else(|| panic!("missing order entry: {entry}; got {order:?}"))
}

fn count_entries(order: &[String], entry: &str) -> usize {
    order.iter().filter(|value| value.as_str() == entry).count()
}

static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn test_guard() -> MutexGuard<'static, ()> {
    TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn wait_for_entry(order: &Arc<Mutex<Vec<String>>>, entry: &str) {
    for _ in 0..200 {
        if order
            .lock()
            .expect("order lock")
            .iter()
            .any(|value| value == entry)
        {
            return;
        }
        thread::sleep(Duration::from_millis(5));
    }
    panic!(
        "timed out waiting for {entry}; got {:?}",
        order.lock().expect("order lock").clone()
    );
}

fn new_bus_with_concurrency(
    name: &str,
    event_concurrency: EventConcurrencyMode,
    event_handler_concurrency: EventHandlerConcurrencyMode,
    event_handler_completion: EventHandlerCompletionMode,
) -> Arc<EventBus> {
    EventBus::new_with_options(
        Some(name.to_string()),
        EventBusOptions {
            event_concurrency,
            event_handler_concurrency,
            event_handler_completion,
            ..EventBusOptions::default()
        },
    )
}

#[test]
fn test_comprehensive_patterns_forwarding_async_sync_dispatch_parent_tracking() {
    let _guard = test_guard();
    let bus1_name = unique_bus_name("bus1");
    let bus2_name = unique_bus_name("bus2");
    let bus1 = EventBus::new(Some(bus1_name.clone()));
    let bus2 = EventBus::new(Some(bus2_name.clone()));
    let results = Arc::new(Mutex::new(Vec::<(usize, String)>::new()));
    let execution_counter = Arc::new(Mutex::new(0usize));

    let bus2_results = results.clone();
    let bus2_counter = execution_counter.clone();
    bus2.on_raw("*", "child_bus2_event_handler", move |event| {
        let results = bus2_results.clone();
        let counter = bus2_counter.clone();
        async move {
            let seq = {
                let mut count = counter.lock().expect("counter lock");
                *count += 1;
                *count
            };
            let event_type_short = event
                .inner
                .lock()
                .event_type
                .trim_end_matches("Event")
                .to_string();
            results
                .lock()
                .expect("results lock")
                .push((seq, format!("bus2_handler_{event_type_short}")));
            Ok(json!("forwarded bus result"))
        }
    });

    let bus2_for_forward = bus2.clone();
    bus1.on_raw("*", "emit", move |event| {
        let bus2 = bus2_for_forward.clone();
        async move {
            bus2.emit_base(event);
            Ok(json!(null))
        }
    });

    let bus1_for_parent = bus1.clone();
    let bus2_label = bus2.label();
    let parent_results = results.clone();
    let parent_counter = execution_counter.clone();
    bus1.on_raw("ParentEvent", "parent_bus1_handler", move |event| {
        let bus = bus1_for_parent.clone();
        let bus2_label = bus2_label.clone();
        let results = parent_results.clone();
        let counter = parent_counter.clone();
        let bus1_name = bus1_name.clone();
        let bus2_name = bus2_name.clone();
        async move {
            let seq = {
                let mut count = counter.lock().expect("counter lock");
                *count += 1;
                *count
            };
            results
                .lock()
                .expect("results lock")
                .push((seq, "parent_start".to_string()));

            let child_event_async = bus.emit_child(QueuedChildEvent {
                ..Default::default()
            });
            assert_ne!(
                child_event_async.inner.inner.lock().event_status,
                EventStatus::Completed
            );

            let child_event_sync = bus.emit_child(ImmediateChildEvent {
                ..Default::default()
            });
            let _ = child_event_sync.now().await;
            assert_eq!(
                child_event_sync.inner.inner.lock().event_status,
                EventStatus::Completed
            );
            assert!(
                child_event_sync
                    .inner
                    .inner
                    .lock()
                    .event_path
                    .contains(&bus2_label),
                "awaited child should be forwarded to bus2"
            );

            let sync_results = child_event_sync.inner.inner.lock().event_results.clone();
            assert!(sync_results.values().any(|result| {
                result.handler.eventbus_name == bus1_name && result.handler.handler_name == "emit"
            }));
            assert!(sync_results.values().any(|result| {
                result.handler.eventbus_name == bus2_name
                    && result.handler.handler_name == "child_bus2_event_handler"
            }));

            let parent_id = event.inner.lock().event_id.clone();
            assert_eq!(
                child_event_async
                    .inner
                    .inner
                    .lock()
                    .event_parent_id
                    .as_deref(),
                Some(parent_id.as_str())
            );
            assert_eq!(
                child_event_sync
                    .inner
                    .inner
                    .lock()
                    .event_parent_id
                    .as_deref(),
                Some(parent_id.as_str())
            );

            let seq = {
                let mut count = counter.lock().expect("counter lock");
                *count += 1;
                *count
            };
            results
                .lock()
                .expect("results lock")
                .push((seq, "parent_end".to_string()));
            Ok(json!("parent_done"))
        }
    });

    let parent_event = bus1.emit(ParentEvent {
        ..Default::default()
    });
    let _ = block_on(parent_event.now());
    block_on(bus1.wait_until_idle(Some(2.0)));
    block_on(bus2.wait_until_idle(Some(2.0)));

    let parent_id = parent_event.inner.inner.lock().event_id.clone();
    let bus1_events = bus1.to_json_value();
    let event_history = bus1_events
        .get("event_history")
        .and_then(serde_json::Value::as_object)
        .expect("event history object");
    let child_events: Vec<_> = event_history
        .values()
        .filter(|event| {
            event
                .get("event_type")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|event_type| {
                    event_type == "ImmediateChildEvent" || event_type == "QueuedChildEvent"
                })
        })
        .collect();
    assert!(!child_events.is_empty());
    assert!(child_events.iter().all(|event| {
        event
            .get("event_parent_id")
            .and_then(serde_json::Value::as_str)
            == Some(parent_id.as_str())
    }));

    let mut sorted_results = results.lock().expect("results lock").clone();
    sorted_results.sort_by_key(|(seq, _)| *seq);
    let execution_order: Vec<String> = sorted_results.into_iter().map(|(_, value)| value).collect();
    assert_eq!(
        execution_order.first().map(String::as_str),
        Some("parent_start")
    );
    assert!(execution_order.contains(&"bus2_handler_ImmediateChild".to_string()));
    assert_eq!(
        count_entries(&execution_order, "bus2_handler_ImmediateChild"),
        1
    );
    assert_eq!(
        count_entries(&execution_order, "bus2_handler_QueuedChild"),
        1
    );
    assert_eq!(count_entries(&execution_order, "bus2_handler_Parent"), 1);
    if execution_order.contains(&"parent_end".to_string()) {
        assert!(index_of(&execution_order, "parent_end") > 1);
    }

    bus1.stop();
    bus2.stop();
}

#[test]
fn test_race_condition_stress() {
    let _guard = test_guard();
    let bus1 = EventBus::new(Some("RaceBus1".to_string()));
    let bus2 = EventBus::new(Some("RaceBus2".to_string()));
    let results = Arc::new(Mutex::new(Vec::<String>::new()));

    let bus2_for_forward = bus2.clone();
    bus1.on_raw("*", "forward_to_bus2", move |event| {
        let bus2 = bus2_for_forward.clone();
        async move {
            bus2.emit_base(event);
            Ok(json!(null))
        }
    });

    for bus in [&bus1, &bus2] {
        for pattern in ["QueuedChildEvent", "ImmediateChildEvent"] {
            let results = results.clone();
            let label = bus.label();
            bus.on_raw(
                pattern,
                &format!("{pattern}_child_handler"),
                move |_event| {
                    let results = results.clone();
                    let label = label.clone();
                    async move {
                        results
                            .lock()
                            .expect("results lock")
                            .push(format!("child_{label}"));
                        thread::sleep(Duration::from_millis(1));
                        Ok(json!(format!("child_done_{label}")))
                    }
                },
            );
        }
    }

    let bus1_for_parent = bus1.clone();
    bus1.on_raw("RootEvent", "parent_handler", move |event| {
        let bus = bus1_for_parent.clone();
        async move {
            let mut children = Vec::new();
            for _ in 0..3 {
                children.push(
                    bus.emit_child(QueuedChildEvent {
                        ..Default::default()
                    })
                    .inner,
                );
            }
            for _ in 0..3 {
                let child = bus.emit_child(ImmediateChildEvent {
                    ..Default::default()
                });
                let _ = child.now().await;
                assert_eq!(
                    child.inner.inner.lock().event_status,
                    EventStatus::Completed
                );
                children.push(child.inner);
            }
            let parent_id = event.inner.lock().event_id.clone();
            assert!(children.iter().all(|child| {
                child.inner.lock().event_parent_id.as_deref() == Some(parent_id.as_str())
            }));
            Ok(json!("parent_done"))
        }
    });
    bus1.on_raw("RootEvent", "bad_handler", |_event| async move {
        Ok(json!(null))
    });

    for run in 0..5 {
        results.lock().expect("results lock").clear();
        let event = bus1.emit(RootEvent {
            ..Default::default()
        });
        let _ = block_on(event.now());
        block_on(bus1.wait_until_idle(Some(2.0)));
        block_on(bus2.wait_until_idle(Some(2.0)));

        let results = results.lock().expect("results lock").clone();
        assert_eq!(
            count_entries(&results, &format!("child_{}", bus1.label())),
            6,
            "run {run}: expected six child handlers on bus1, got {results:?}"
        );
        assert_eq!(
            count_entries(&results, &format!("child_{}", bus2.label())),
            6,
            "run {run}: expected six child handlers on bus2, got {results:?}"
        );
    }

    bus1.stop();
    bus2.stop();
}

#[test]
fn test_multi_bus_queues_are_independent_when_awaiting_child() {
    let _guard = test_guard();
    let bus1 = EventBus::new(Some(unique_bus_name("Bus1")));
    let bus2 = EventBus::new(Some(unique_bus_name("Bus2")));
    let execution_order = Arc::new(Mutex::new(Vec::new()));

    let bus1_for_event1 = bus1.clone();
    let order_for_event1 = execution_order.clone();
    bus1.on_raw("Event1", "event1_handler", move |_event| {
        let bus = bus1_for_event1.clone();
        let order = order_for_event1.clone();
        async move {
            push(&order, "Bus1_Event1_start");
            let child = bus.emit_child(ChildEvent {
                ..Default::default()
            });
            push(&order, "Child_dispatched_to_Bus1");
            let _ = child.now().await;
            push(&order, "Child_await_returned");
            push(&order, "Bus1_Event1_end");
            Ok(json!("event1_done"))
        }
    });

    for (bus, pattern, start, end, result) in [
        (
            bus1.clone(),
            "Event2",
            "Bus1_Event2_start",
            "Bus1_Event2_end",
            "event2_done",
        ),
        (
            bus2.clone(),
            "Event3",
            "Bus2_Event3_start",
            "Bus2_Event3_end",
            "event3_done",
        ),
        (
            bus2.clone(),
            "Event4",
            "Bus2_Event4_start",
            "Bus2_Event4_end",
            "event4_done",
        ),
        (
            bus1.clone(),
            "ChildEvent",
            "Child_start",
            "Child_end",
            "child_done",
        ),
    ] {
        let order = execution_order.clone();
        bus.on_raw(pattern, &format!("{pattern}_handler"), move |_event| {
            let order = order.clone();
            async move {
                push(&order, start);
                push(&order, end);
                Ok(json!(result))
            }
        });
    }

    let event1 = bus1.emit(Event1 {
        ..Default::default()
    });
    bus1.emit(Event2 {
        ..Default::default()
    });
    bus2.emit(Event3 {
        ..Default::default()
    });
    bus2.emit(Event4 {
        ..Default::default()
    });

    wait_for_entry(&execution_order, "Bus2_Event3_start");
    let _ = block_on(event1.now());

    let order = execution_order.lock().expect("order lock").clone();
    assert!(order.contains(&"Child_start".to_string()));
    assert!(order.contains(&"Child_end".to_string()));
    assert!(index_of(&order, "Child_end") < index_of(&order, "Bus1_Event1_end"));
    if order.contains(&"Bus1_Event2_start".to_string()) {
        assert!(index_of(&order, "Bus1_Event2_start") > index_of(&order, "Bus1_Event1_end"));
    }
    assert!(index_of(&order, "Bus2_Event3_start") < index_of(&order, "Bus1_Event1_end"));

    block_on(bus1.wait_until_idle(Some(2.0)));
    block_on(bus2.wait_until_idle(Some(2.0)));
    let order = execution_order.lock().expect("order lock").clone();
    assert!(order.contains(&"Bus1_Event2_start".to_string()));
    assert!(order.contains(&"Bus2_Event3_start".to_string()));
    assert!(order.contains(&"Bus2_Event4_start".to_string()));
    bus1.stop();
    bus2.stop();
}

#[test]
fn test_awaited_child_jumps_queue_without_overshoot() {
    let _guard = test_guard();
    let bus = EventBus::new(Some("ComprehensiveNoOvershootBus".to_string()));
    let execution_order = Arc::new(Mutex::new(Vec::new()));

    let bus_for_event1 = bus.clone();
    let order_for_event1 = execution_order.clone();
    bus.on_raw("Event1", "event1_handler", move |_event| {
        let bus = bus_for_event1.clone();
        let order = order_for_event1.clone();
        async move {
            push(&order, "Event1_start");
            let child = bus.emit_child(ChildEvent {
                ..Default::default()
            });
            push(&order, "Child_dispatched");
            let _ = child.now().await;
            push(&order, "Child_await_returned");
            push(&order, "Event1_end");
            Ok(json!("event1_done"))
        }
    });

    let order_for_event2 = execution_order.clone();
    bus.on_raw("Event2", "event2_handler", move |_event| {
        let order = order_for_event2.clone();
        async move {
            push(&order, "Event2_start");
            push(&order, "Event2_end");
            Ok(json!("event2_done"))
        }
    });

    let order_for_event3 = execution_order.clone();
    bus.on_raw("Event3", "event3_handler", move |_event| {
        let order = order_for_event3.clone();
        async move {
            push(&order, "Event3_start");
            push(&order, "Event3_end");
            Ok(json!("event3_done"))
        }
    });

    let order_for_child = execution_order.clone();
    bus.on_raw("ChildEvent", "child_handler", move |_event| {
        let order = order_for_child.clone();
        async move {
            push(&order, "Child_start");
            push(&order, "Child_end");
            Ok(json!("child_done"))
        }
    });

    let event1 = bus.emit(Event1 {
        ..Default::default()
    });
    let event2 = bus.emit(Event2 {
        ..Default::default()
    });
    let event3 = bus.emit(Event3 {
        ..Default::default()
    });

    let _ = block_on(event1.now());
    block_on(bus.wait_until_idle(Some(2.0)));

    let order = execution_order.lock().expect("order lock").clone();
    let child_start_idx = index_of(&order, "Child_start");
    let child_end_idx = index_of(&order, "Child_end");
    let event1_end_idx = index_of(&order, "Event1_end");
    let event2_start_idx = index_of(&order, "Event2_start");
    let event3_start_idx = index_of(&order, "Event3_start");

    assert!(child_start_idx < event1_end_idx);
    assert!(child_end_idx < event1_end_idx);
    assert!(event2_start_idx > event1_end_idx);
    assert!(event3_start_idx > event1_end_idx);
    assert!(event2_start_idx < event3_start_idx);
    assert_eq!(
        event2.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    assert_eq!(
        event3.inner.inner.lock().event_status,
        EventStatus::Completed
    );
    bus.stop();
}

#[test]
fn test_now_on_non_proxied_event_keeps_bus_paused_during_queue_jump() {
    let _guard = test_guard();
    let bus = EventBus::new(Some("RawDoneBus".to_string()));
    let execution_order = Arc::new(Mutex::new(Vec::new()));

    let bus_for_event1 = bus.clone();
    let order_for_event1 = execution_order.clone();
    bus.on_raw("Event1", "event1_handler", move |_event| {
        let bus = bus_for_event1.clone();
        let order = order_for_event1.clone();
        async move {
            push(&order, "Event1_start");
            let child = bus.emit(ChildA {
                ..Default::default()
            });
            let _ = child.now().await;
            push(&order, "RawChild_await_returned");
            assert!(
                !order
                    .lock()
                    .expect("order lock")
                    .contains(&"Event2_start".to_string()),
                "queued sibling must not run while the parent handler is still active"
            );
            push(&order, "Event1_end");
            Ok(json!("event1_done"))
        }
    });

    let order_for_child = execution_order.clone();
    bus.on_raw("ChildA", "raw_child_handler", move |_event| {
        let order = order_for_child.clone();
        async move {
            push(&order, "RawChild_start");
            push(&order, "RawChild_end");
            Ok(json!("raw_child_done"))
        }
    });

    let order_for_event2 = execution_order.clone();
    bus.on_raw("Event2", "event2_handler", move |_event| {
        let order = order_for_event2.clone();
        async move {
            push(&order, "Event2_start");
            push(&order, "Event2_end");
            Ok(json!("event2_done"))
        }
    });

    let event1 = bus.emit(Event1 {
        ..Default::default()
    });
    bus.emit(Event2 {
        ..Default::default()
    });
    let _ = block_on(event1.now());
    block_on(bus.wait_until_idle(Some(2.0)));

    let order = execution_order.lock().expect("order lock").clone();
    assert!(index_of(&order, "RawChild_end") < index_of(&order, "Event1_end"));
    assert!(index_of(&order, "Event2_start") > index_of(&order, "Event1_end"));
    let child = bus
        .runtime_payload_for_test()
        .values()
        .find(|event| event.inner.lock().event_type == "ChildA")
        .cloned()
        .expect("raw child");
    assert_eq!(child.inner.lock().event_parent_id, None);
    assert!(!child.inner.lock().event_blocks_parent_completion);
    bus.stop();
}

#[test]
fn test_bus_pause_state_clears_after_queue_jump_completes() {
    let _guard = test_guard();
    let bus = EventBus::new(Some("DepthBalanceBus".to_string()));
    let execution_order = Arc::new(Mutex::new(Vec::new()));

    let bus_for_event1 = bus.clone();
    let order_for_event1 = execution_order.clone();
    bus.on_raw("Event1", "event1_handler", move |_event| {
        let bus = bus_for_event1.clone();
        let order = order_for_event1.clone();
        async move {
            push(&order, "Event1_start");
            let child_a = bus.emit_child(ChildA {
                ..Default::default()
            });
            let _ = child_a.now().await;
            push(&order, "ChildA_await_returned");
            assert!(!order
                .lock()
                .expect("order lock")
                .contains(&"Event2_start".to_string()));

            let child_b = bus.emit_child(ChildB {
                ..Default::default()
            });
            assert!(!order
                .lock()
                .expect("order lock")
                .contains(&"Event2_start".to_string()));
            let _ = child_b.now().await;
            push(&order, "ChildB_await_returned");
            assert!(!order
                .lock()
                .expect("order lock")
                .contains(&"Event2_start".to_string()));
            push(&order, "Event1_end");
            Ok(json!("event1_done"))
        }
    });

    for (pattern, start, end) in [
        ("ChildA", "ChildA_start", "ChildA_end"),
        ("ChildB", "ChildB_start", "ChildB_end"),
        ("Event2", "Event2_start", "Event2_end"),
    ] {
        let order = execution_order.clone();
        bus.on_raw(pattern, &format!("{pattern}_handler"), move |_event| {
            let order = order.clone();
            async move {
                push(&order, start);
                push(&order, end);
                Ok(json!(null))
            }
        });
    }

    let event1 = bus.emit(Event1 {
        ..Default::default()
    });
    bus.emit(Event2 {
        ..Default::default()
    });
    let _ = block_on(event1.now());
    block_on(bus.wait_until_idle(Some(2.0)));

    let order = execution_order.lock().expect("order lock").clone();
    assert!(index_of(&order, "ChildA_end") < index_of(&order, "ChildA_await_returned"));
    assert!(index_of(&order, "ChildB_end") < index_of(&order, "ChildB_await_returned"));
    assert!(index_of(&order, "Event2_start") > index_of(&order, "Event1_end"));
    bus.stop();
}

#[test]
fn test_isinsidehandler_is_per_bus_not_global() {
    let _guard = test_guard();
    let bus_a = EventBus::new(Some("InsideHandlerA".to_string()));
    let bus_b = EventBus::new(Some("InsideHandlerB".to_string()));
    let execution_order = Arc::new(Mutex::new(Vec::new()));
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));

    let order_for_a = execution_order.clone();
    let release_for_a = release_rx.clone();
    bus_a.on_raw("Event1", "bus_a_handler", move |_event| {
        let order = order_for_a.clone();
        let release = release_for_a.clone();
        let started_tx = started_tx.clone();
        async move {
            push(&order, "bus_a_start");
            let _ = started_tx.send(());
            release
                .lock()
                .expect("release lock")
                .recv_timeout(Duration::from_secs(2))
                .expect("release bus_a handler");
            push(&order, "bus_a_end");
            Ok(json!(null))
        }
    });

    let order_for_b = execution_order.clone();
    bus_b.on_raw("Event2", "bus_b_handler", move |_event| {
        let order = order_for_b.clone();
        async move {
            push(&order, "bus_b_start");
            push(&order, "bus_b_end");
            Ok(json!(null))
        }
    });

    let event_a = bus_a.emit(Event1 {
        ..Default::default()
    });
    started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("bus_a handler started");
    let event_b = bus_b.emit(Event2 {
        ..Default::default()
    });
    let _ = block_on(event_b.now());
    release_tx.send(()).expect("release bus_a handler");
    let _ = block_on(event_a.now());

    let order = execution_order.lock().expect("order lock").clone();
    assert!(index_of(&order, "bus_b_start") < index_of(&order, "bus_a_end"));
    assert!(index_of(&order, "bus_b_end") < index_of(&order, "bus_a_end"));
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_is_inside_handler_is_per_bus_not_global() {
    test_isinsidehandler_is_per_bus_not_global();
}

#[test]
fn test_dispatch_multiple_await_one_skips_others_until_after_handler_completes() {
    let _guard = test_guard();
    let bus = EventBus::new(Some("ComprehensiveMultiDispatchBus".to_string()));
    let execution_order = Arc::new(Mutex::new(Vec::new()));
    let (event1_started_tx, event1_started_rx) = mpsc::channel();
    let (allow_children_tx, allow_children_rx) = mpsc::channel();
    let allow_children_rx = Arc::new(Mutex::new(allow_children_rx));

    let bus_for_event1 = bus.clone();
    let order_for_event1 = execution_order.clone();
    let allow_children_for_event1 = allow_children_rx.clone();
    bus.on_raw("Event1", "event1_handler", move |_event| {
        let bus = bus_for_event1.clone();
        let order = order_for_event1.clone();
        let event1_started_tx = event1_started_tx.clone();
        let allow_children = allow_children_for_event1.clone();
        async move {
            push(&order, "Event1_start");
            event1_started_tx.send(()).expect("signal Event1 start");
            allow_children
                .lock()
                .expect("allow children lock")
                .recv()
                .expect("wait for queued sibling events");
            bus.emit_child(ChildA {
                ..Default::default()
            });
            push(&order, "ChildA_dispatched");
            let child_b = bus.emit_child(ChildB {
                ..Default::default()
            });
            push(&order, "ChildB_dispatched");
            bus.emit_child(ChildC {
                ..Default::default()
            });
            push(&order, "ChildC_dispatched");
            let _ = child_b.now().await;
            push(&order, "ChildB_await_returned");
            push(&order, "Event1_end");
            Ok(json!("event1_done"))
        }
    });

    for (pattern, start, end, result) in [
        ("Event2", "Event2_start", "Event2_end", "event2_done"),
        ("Event3", "Event3_start", "Event3_end", "event3_done"),
        ("ChildA", "ChildA_start", "ChildA_end", "child_a_done"),
        ("ChildB", "ChildB_start", "ChildB_end", "child_b_done"),
        ("ChildC", "ChildC_start", "ChildC_end", "child_c_done"),
    ] {
        let order = execution_order.clone();
        bus.on_raw(pattern, &format!("{pattern}_handler"), move |_event| {
            let order = order.clone();
            async move {
                push(&order, start);
                push(&order, end);
                Ok(json!(result))
            }
        });
    }

    let event1 = bus.emit(Event1 {
        ..Default::default()
    });
    event1_started_rx.recv().expect("Event1 handler started");
    bus.emit(Event2 {
        ..Default::default()
    });
    bus.emit(Event3 {
        ..Default::default()
    });
    allow_children_tx.send(()).expect("release Event1 children");

    let _ = block_on(event1.now());
    block_on(bus.wait_until_idle(Some(2.0)));

    let order = execution_order.lock().expect("order lock").clone();
    let event1_end_idx = index_of(&order, "Event1_end");
    let child_b_end_idx = index_of(&order, "ChildB_end");
    let event2_start_idx = index_of(&order, "Event2_start");
    let event3_start_idx = index_of(&order, "Event3_start");
    let child_a_start_idx = index_of(&order, "ChildA_start");
    let child_c_start_idx = index_of(&order, "ChildC_start");

    assert!(child_b_end_idx < event1_end_idx);
    assert!(event2_start_idx > event1_end_idx);
    assert!(event3_start_idx > event1_end_idx);
    assert!(child_a_start_idx > event1_end_idx);
    assert!(child_c_start_idx > event1_end_idx);
    assert!(event2_start_idx < event3_start_idx);
    assert!(event3_start_idx < child_a_start_idx);
    assert!(child_a_start_idx < child_c_start_idx);
    bus.stop();
}

#[test]
fn test_awaiting_an_already_completed_event_is_a_no_op() {
    let _guard = test_guard();
    let bus = EventBus::new(Some("AlreadyCompletedBus".to_string()));
    let event1 = bus.emit(Event1 {
        ..Default::default()
    });
    let _ = block_on(event1.now());
    assert_eq!(
        event1.inner.inner.lock().event_status,
        EventStatus::Completed
    );

    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    bus.on_raw("SlowEvent", "blocker", move |_event| {
        let release_rx = release_rx.clone();
        let started_tx = started_tx.clone();
        async move {
            let _ = started_tx.send(());
            release_rx
                .lock()
                .expect("release lock")
                .recv_timeout(Duration::from_secs(2))
                .expect("release blocker");
            Ok(json!(null))
        }
    });
    bus.on_raw("Event2", "event2_handler", |_event| async move {
        Ok(json!("event2_done"))
    });

    let blocker = bus.emit(SlowEvent {
        ..Default::default()
    });
    started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("blocker started");
    let event2 = bus.emit(Event2 {
        ..Default::default()
    });
    let _ = block_on(event1.now());
    assert_eq!(event2.inner.inner.lock().event_status, EventStatus::Pending);

    release_tx.send(()).expect("release blocker");
    let _ = block_on(blocker.now());
    let _ = block_on(event2.now());
    bus.stop();
}

#[test]
fn test_multiple_awaits_on_same_event() {
    let _guard = test_guard();
    let bus = EventBus::new(Some("ComprehensiveMultiAwaitBus".to_string()));
    let execution_order = Arc::new(Mutex::new(Vec::new()));
    let await_results = Arc::new(Mutex::new(Vec::new()));

    let bus_for_event1 = bus.clone();
    let order_for_event1 = execution_order.clone();
    let await_results_for_event1 = await_results.clone();
    bus.on_raw("Event1", "event1_handler", move |_event| {
        let bus = bus_for_event1.clone();
        let order = order_for_event1.clone();
        let await_results = await_results_for_event1.clone();
        async move {
            push(&order, "Event1_start");
            let child = bus.emit_child(ChildEvent {
                ..Default::default()
            });
            let child_for_await1 =
                BaseEventHandle::<ChildEvent>::from_base_event(child.inner.clone());
            let child_for_await2 =
                BaseEventHandle::<ChildEvent>::from_base_event(child.inner.clone());
            let await_results_1 = await_results.clone();
            let await_results_2 = await_results.clone();
            join!(
                async move {
                    let _ = child_for_await1.now().await;
                    push(&await_results_1, "await1_completed");
                },
                async move {
                    let _ = child_for_await2.now().await;
                    push(&await_results_2, "await2_completed");
                }
            );
            push(&order, "Both_awaits_completed");
            push(&order, "Event1_end");
            Ok(json!("event1_done"))
        }
    });

    let order_for_event2 = execution_order.clone();
    bus.on_raw("Event2", "event2_handler", move |_event| {
        let order = order_for_event2.clone();
        async move {
            push(&order, "Event2_start");
            push(&order, "Event2_end");
            Ok(json!("event2_done"))
        }
    });

    let order_for_child = execution_order.clone();
    bus.on_raw("ChildEvent", "child_handler", move |_event| {
        let order = order_for_child.clone();
        async move {
            push(&order, "Child_start");
            thread::sleep(Duration::from_millis(10));
            push(&order, "Child_end");
            Ok(json!("child_done"))
        }
    });

    let event1 = bus.emit(Event1 {
        ..Default::default()
    });
    bus.emit(Event2 {
        ..Default::default()
    });

    let _ = block_on(event1.now());

    let order = execution_order.lock().expect("order lock").clone();
    let await_results = await_results.lock().expect("await results lock").clone();
    assert_eq!(await_results.len(), 2);
    assert!(await_results.contains(&"await1_completed".to_string()));
    assert!(await_results.contains(&"await2_completed".to_string()));
    assert!(index_of(&order, "Child_end") < index_of(&order, "Event1_end"));
    assert!(!order.contains(&"Event2_start".to_string()));

    block_on(bus.wait_until_idle(Some(2.0)));
    bus.stop();
}

#[test]
fn test_deeply_nested_awaited_children() {
    let _guard = test_guard();
    let bus = EventBus::new(Some("ComprehensiveDeepNestedBus".to_string()));
    let execution_order = Arc::new(Mutex::new(Vec::new()));

    let bus_for_event1 = bus.clone();
    let order_for_event1 = execution_order.clone();
    bus.on_raw("Event1", "event1_handler", move |_event| {
        let bus = bus_for_event1.clone();
        let order = order_for_event1.clone();
        async move {
            push(&order, "Event1_start");
            let child1 = bus.emit_child(Child1 {
                ..Default::default()
            });
            let _ = child1.now().await;
            push(&order, "Event1_end");
            Ok(json!("event1_done"))
        }
    });

    let bus_for_child1 = bus.clone();
    let order_for_child1 = execution_order.clone();
    bus.on_raw("Child1", "child1_handler", move |_event| {
        let bus = bus_for_child1.clone();
        let order = order_for_child1.clone();
        async move {
            push(&order, "Child1_start");
            let child2 = bus.emit_child(Child2 {
                ..Default::default()
            });
            let _ = child2.now().await;
            push(&order, "Child1_end");
            Ok(json!("child1_done"))
        }
    });

    let order_for_child2 = execution_order.clone();
    bus.on_raw("Child2", "child2_handler", move |_event| {
        let order = order_for_child2.clone();
        async move {
            push(&order, "Child2_start");
            push(&order, "Child2_end");
            Ok(json!("child2_done"))
        }
    });

    let order_for_event2 = execution_order.clone();
    bus.on_raw("Event2", "event2_handler", move |_event| {
        let order = order_for_event2.clone();
        async move {
            push(&order, "Event2_start");
            push(&order, "Event2_end");
            Ok(json!("event2_done"))
        }
    });

    let event1 = bus.emit(Event1 {
        ..Default::default()
    });
    bus.emit(Event2 {
        ..Default::default()
    });

    let _ = block_on(event1.now());

    let order = execution_order.lock().expect("order lock").clone();
    assert!(index_of(&order, "Child2_end") < index_of(&order, "Child1_end"));
    assert!(index_of(&order, "Child1_end") < index_of(&order, "Event1_end"));
    assert!(!order.contains(&"Event2_start".to_string()));

    block_on(bus.wait_until_idle(Some(2.0)));
    let order = execution_order.lock().expect("order lock").clone();
    assert!(index_of(&order, "Event2_start") > index_of(&order, "Event1_end"));
    bus.stop();
}

#[test]
fn test_bug_queue_jump_two_bus_serial_handlers_should_serialize_on_each_bus() {
    let _guard = test_guard();
    let bus_a = new_bus_with_concurrency(
        "QJ2BS_A",
        EventConcurrencyMode::BusSerial,
        EventHandlerConcurrencyMode::Serial,
        EventHandlerCompletionMode::All,
    );
    let bus_b = new_bus_with_concurrency(
        "QJ2BS_B",
        EventConcurrencyMode::BusSerial,
        EventHandlerConcurrencyMode::Serial,
        EventHandlerCompletionMode::All,
    );
    let log = Arc::new(Mutex::new(Vec::new()));

    let bus_a_for_trigger = bus_a.clone();
    let bus_b_for_trigger = bus_b.clone();
    bus_a.on_raw("Event1", "trigger_handler", move |_event| {
        let bus_a = bus_a_for_trigger.clone();
        let bus_b = bus_b_for_trigger.clone();
        async move {
            let child = bus_a.emit_child(ChildEvent {
                ..Default::default()
            });
            bus_b.emit_base(child.inner.clone());
            let _ = child.now().await;
            Ok(json!(null))
        }
    });

    for (bus, first_start, first_end, second_start, second_end) in [
        (bus_a.clone(), "a1_start", "a1_end", "a2_start", "a2_end"),
        (bus_b.clone(), "b1_start", "b1_end", "b2_start", "b2_end"),
    ] {
        let log_first = log.clone();
        bus.on_raw("ChildEvent", first_start, move |_event| {
            let log = log_first.clone();
            async move {
                push(&log, first_start);
                thread::sleep(Duration::from_millis(15));
                push(&log, first_end);
                Ok(json!(null))
            }
        });
        let log_second = log.clone();
        bus.on_raw("ChildEvent", second_start, move |_event| {
            let log = log_second.clone();
            async move {
                push(&log, second_start);
                thread::sleep(Duration::from_millis(5));
                push(&log, second_end);
                Ok(json!(null))
            }
        });
    }

    let top = bus_a.emit(Event1 {
        ..Default::default()
    });
    let _ = block_on(top.now());
    block_on(bus_a.wait_until_idle(Some(2.0)));
    block_on(bus_b.wait_until_idle(Some(2.0)));

    let log = log.lock().expect("log lock").clone();
    assert!(index_of(&log, "a1_end") < index_of(&log, "a2_start"));
    assert!(index_of(&log, "b1_end") < index_of(&log, "b2_start"));
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_bug_queue_jump_two_bus_global_handler_lock_should_serialize_across_both_buses() {
    let _guard = test_guard();
    let bus_a = new_bus_with_concurrency(
        "QJ2GS_A",
        EventConcurrencyMode::BusSerial,
        EventHandlerConcurrencyMode::Serial,
        EventHandlerCompletionMode::All,
    );
    let bus_b = new_bus_with_concurrency(
        "QJ2GS_B",
        EventConcurrencyMode::BusSerial,
        EventHandlerConcurrencyMode::Serial,
        EventHandlerCompletionMode::All,
    );
    let log = Arc::new(Mutex::new(Vec::new()));
    let global_handler_lock = Arc::new(Mutex::new(()));

    let bus_a_for_trigger = bus_a.clone();
    let bus_b_for_trigger = bus_b.clone();
    bus_a.on_raw("Event1", "trigger_handler", move |_event| {
        let bus_a = bus_a_for_trigger.clone();
        let bus_b = bus_b_for_trigger.clone();
        async move {
            let child = bus_a.emit_child(ChildEvent {
                ..Default::default()
            });
            bus_b.emit_base(child.inner.clone());
            let _ = child.now().await;
            Ok(json!(null))
        }
    });

    for (bus, first_start, first_end, second_start, second_end) in [
        (bus_a.clone(), "a1_start", "a1_end", "a2_start", "a2_end"),
        (bus_b.clone(), "b1_start", "b1_end", "b2_start", "b2_end"),
    ] {
        let log_first = log.clone();
        let lock_first = global_handler_lock.clone();
        bus.on_raw_sync("ChildEvent", first_start, move |_event| {
            let _guard = lock_first.lock().expect("global handler lock");
            push(&log_first, first_start);
            thread::sleep(Duration::from_millis(15));
            push(&log_first, first_end);
            Ok(json!(null))
        });
        let log_second = log.clone();
        let lock_second = global_handler_lock.clone();
        bus.on_raw_sync("ChildEvent", second_start, move |_event| {
            let _guard = lock_second.lock().expect("global handler lock");
            push(&log_second, second_start);
            thread::sleep(Duration::from_millis(5));
            push(&log_second, second_end);
            Ok(json!(null))
        });
    }

    let top = bus_a.emit(Event1 {
        ..Default::default()
    });
    let _ = block_on(top.now());
    block_on(bus_a.wait_until_idle(Some(2.0)));
    block_on(bus_b.wait_until_idle(Some(2.0)));

    let log = log.lock().expect("log lock").clone();
    assert!(
        index_of(&log, "a1_end") < index_of(&log, "a2_start"),
        "global lock: a1 should finish before a2 starts. Got: {log:?}"
    );
    assert!(
        index_of(&log, "b1_end") < index_of(&log, "b2_start"),
        "global lock: b1 should finish before b2 starts. Got: {log:?}"
    );
    for entry_pair in log.chunks(2) {
        assert_eq!(
            entry_pair.len(),
            2,
            "global lock: every handler start must be followed by its end. Got: {log:?}"
        );
        assert!(
            entry_pair[0].ends_with("_start") && entry_pair[1].ends_with("_end"),
            "global lock: handlers must not overlap across buses. Got: {log:?}"
        );
        assert_eq!(
            entry_pair[0].replace("_start", ""),
            entry_pair[1].replace("_end", ""),
            "global lock: handler start/end pair mismatch. Got: {log:?}"
        );
    }
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_bug_queue_jump_two_bus_mixed_bus_a_serial_bus_b_parallel() {
    let _guard = test_guard();
    let bus_a = new_bus_with_concurrency(
        "QJ2Mix1_A",
        EventConcurrencyMode::BusSerial,
        EventHandlerConcurrencyMode::Serial,
        EventHandlerCompletionMode::All,
    );
    let bus_b = new_bus_with_concurrency(
        "QJ2Mix1_B",
        EventConcurrencyMode::BusSerial,
        EventHandlerConcurrencyMode::Parallel,
        EventHandlerCompletionMode::All,
    );
    let log = Arc::new(Mutex::new(Vec::new()));

    let bus_a_for_trigger = bus_a.clone();
    let bus_b_for_trigger = bus_b.clone();
    bus_a.on_raw("Event1", "trigger_handler", move |_event| {
        let bus_a = bus_a_for_trigger.clone();
        let bus_b = bus_b_for_trigger.clone();
        async move {
            let child = bus_a.emit_child(ChildEvent {
                ..Default::default()
            });
            bus_b.emit_base(child.inner.clone());
            let _ = child.now().await;
            Ok(json!(null))
        }
    });

    for (bus, first_start, first_end, second_start, second_end) in [
        (bus_a.clone(), "a1_start", "a1_end", "a2_start", "a2_end"),
        (bus_b.clone(), "b1_start", "b1_end", "b2_start", "b2_end"),
    ] {
        let log_first = log.clone();
        bus.on_raw("ChildEvent", first_start, move |_event| {
            let log = log_first.clone();
            async move {
                push(&log, first_start);
                thread::sleep(Duration::from_millis(15));
                push(&log, first_end);
                Ok(json!(null))
            }
        });
        let log_second = log.clone();
        bus.on_raw("ChildEvent", second_start, move |_event| {
            let log = log_second.clone();
            async move {
                push(&log, second_start);
                thread::sleep(Duration::from_millis(5));
                push(&log, second_end);
                Ok(json!(null))
            }
        });
    }

    let top = bus_a.emit(Event1 {
        ..Default::default()
    });
    let _ = block_on(top.now());
    block_on(bus_a.wait_until_idle(Some(2.0)));
    block_on(bus_b.wait_until_idle(Some(2.0)));

    let log = log.lock().expect("log lock").clone();
    assert!(index_of(&log, "a1_end") < index_of(&log, "a2_start"));
    assert!(index_of(&log, "b2_start") < index_of(&log, "b1_end"));
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_bug_queue_jump_two_bus_mixed_bus_a_parallel_bus_b_serial() {
    let _guard = test_guard();
    let bus_a = new_bus_with_concurrency(
        "QJ2Mix2_A",
        EventConcurrencyMode::BusSerial,
        EventHandlerConcurrencyMode::Parallel,
        EventHandlerCompletionMode::All,
    );
    let bus_b = new_bus_with_concurrency(
        "QJ2Mix2_B",
        EventConcurrencyMode::BusSerial,
        EventHandlerConcurrencyMode::Serial,
        EventHandlerCompletionMode::All,
    );
    let log = Arc::new(Mutex::new(Vec::new()));

    let bus_a_for_trigger = bus_a.clone();
    let bus_b_for_trigger = bus_b.clone();
    bus_a.on_raw("Event1", "trigger_handler", move |_event| {
        let bus_a = bus_a_for_trigger.clone();
        let bus_b = bus_b_for_trigger.clone();
        async move {
            let child = bus_a.emit_child(ChildEvent {
                ..Default::default()
            });
            bus_b.emit_base(child.inner.clone());
            let _ = child.now().await;
            Ok(json!(null))
        }
    });

    for (bus, first_start, first_end, second_start, second_end) in [
        (bus_a.clone(), "a1_start", "a1_end", "a2_start", "a2_end"),
        (bus_b.clone(), "b1_start", "b1_end", "b2_start", "b2_end"),
    ] {
        let log_first = log.clone();
        bus.on_raw("ChildEvent", first_start, move |_event| {
            let log = log_first.clone();
            async move {
                push(&log, first_start);
                thread::sleep(Duration::from_millis(15));
                push(&log, first_end);
                Ok(json!(null))
            }
        });
        let log_second = log.clone();
        bus.on_raw("ChildEvent", second_start, move |_event| {
            let log = log_second.clone();
            async move {
                push(&log, second_start);
                thread::sleep(Duration::from_millis(5));
                push(&log, second_end);
                Ok(json!(null))
            }
        });
    }

    let top = bus_a.emit(Event1 {
        ..Default::default()
    });
    let _ = block_on(top.now());
    block_on(bus_a.wait_until_idle(Some(2.0)));
    block_on(bus_b.wait_until_idle(Some(2.0)));

    let log = log.lock().expect("log lock").clone();
    assert!(index_of(&log, "a2_start") < index_of(&log, "a1_end"));
    assert!(index_of(&log, "b1_end") < index_of(&log, "b2_start"));
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_forwarded_event_uses_processing_bus_defaults_unless_explicit_overrides_are_set() {
    let _guard = test_guard();
    let bus_a = new_bus_with_concurrency(
        "QJDefaults_A",
        EventConcurrencyMode::BusSerial,
        EventHandlerConcurrencyMode::Serial,
        EventHandlerCompletionMode::All,
    );
    let bus_b = new_bus_with_concurrency(
        "QJDefaults_B",
        EventConcurrencyMode::BusSerial,
        EventHandlerConcurrencyMode::Parallel,
        EventHandlerCompletionMode::All,
    );
    let log = Arc::new(Mutex::new(Vec::new()));

    let log_b1 = log.clone();
    bus_b.on_raw("DefaultsChildEvent", "b1", move |event| {
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
            push(&log, &format!("{mode}:b1_start"));
            thread::sleep(Duration::from_millis(15));
            push(&log, &format!("{mode}:b1_end"));
            Ok(json!(null))
        }
    });
    let log_b2 = log.clone();
    bus_b.on_raw("DefaultsChildEvent", "b2", move |event| {
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
            push(&log, &format!("{mode}:b2_start"));
            thread::sleep(Duration::from_millis(5));
            push(&log, &format!("{mode}:b2_end"));
            Ok(json!(null))
        }
    });

    let bus_a_for_trigger = bus_a.clone();
    let bus_b_for_trigger = bus_b.clone();
    bus_a.on_raw("Event1", "trigger_handler", move |_event| {
        let bus_a = bus_a_for_trigger.clone();
        let bus_b = bus_b_for_trigger.clone();
        async move {
            let inherited = bus_a.emit_child(DefaultsChildEvent {
                mode: "inherited".to_string(),
                ..Default::default()
            });
            bus_b.emit_base(inherited.inner.clone());
            let _ = inherited.now().await;

            let mut override_event = DefaultsChildEvent {
                mode: "override".to_string(),
                ..Default::default()
            };
            override_event.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Serial);
            let override_event = bus_a.emit_child(override_event);
            bus_b.emit_base(override_event.inner.clone());
            let _ = override_event.now().await;
            Ok(json!(null))
        }
    });

    let top = bus_a.emit(Event1 {
        ..Default::default()
    });
    let _ = block_on(top.now());
    block_on(bus_a.wait_until_idle(Some(2.0)));
    block_on(bus_b.wait_until_idle(Some(2.0)));

    let log = log.lock().expect("log lock").clone();
    assert!(index_of(&log, "inherited:b2_start") < index_of(&log, "inherited:b1_end"));
    assert!(index_of(&log, "override:b1_end") < index_of(&log, "override:b2_start"));
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_forwarded_first_mode_uses_processing_bus_handler_concurrency_defaults() {
    let _guard = test_guard();
    let bus_a = new_bus_with_concurrency(
        "ForwardedFirstDefaults_A",
        EventConcurrencyMode::BusSerial,
        EventHandlerConcurrencyMode::Serial,
        EventHandlerCompletionMode::All,
    );
    let bus_b = new_bus_with_concurrency(
        "ForwardedFirstDefaults_B",
        EventConcurrencyMode::BusSerial,
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
    bus_b.on_raw("ForwardedFirstEvent", "slow", move |_event| {
        let log = slow_log.clone();
        async move {
            push(&log, "slow_start");
            thread::sleep(Duration::from_millis(20));
            push(&log, "slow_end");
            Ok(json!("slow"))
        }
    });
    let fast_log = log.clone();
    bus_b.on_raw("ForwardedFirstEvent", "fast", move |_event| {
        let log = fast_log.clone();
        async move {
            push(&log, "fast_start");
            thread::sleep(Duration::from_millis(1));
            push(&log, "fast_end");
            Ok(json!("fast"))
        }
    });

    let event = bus_a.emit(ForwardedFirstEvent {
        ..Default::default()
    });
    let _ = block_on(event.now_with_options(EventWaitOptions {
        timeout: None,
        first_result: true,
    }));
    block_on(bus_a.wait_until_idle(Some(2.0)));
    block_on(bus_b.wait_until_idle(Some(2.0)));
    let log = log.lock().expect("log lock").clone();
    assert!(log.contains(&"slow_start".to_string()));
    assert!(log.contains(&"fast_start".to_string()));
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_bug_queue_jump_should_respect_bus_serial_event_concurrency_on_forward_bus() {
    let _guard = test_guard();
    let bus_a = new_bus_with_concurrency(
        "QJEvt_A",
        EventConcurrencyMode::BusSerial,
        EventHandlerConcurrencyMode::Serial,
        EventHandlerCompletionMode::All,
    );
    let bus_b = new_bus_with_concurrency(
        "QJEvt_B",
        EventConcurrencyMode::BusSerial,
        EventHandlerConcurrencyMode::Serial,
        EventHandlerCompletionMode::All,
    );
    let log = Arc::new(Mutex::new(Vec::new()));

    let slow_log = log.clone();
    bus_b.on_raw("SlowEvent", "slow", move |_event| {
        let log = slow_log.clone();
        async move {
            push(&log, "slow_start");
            thread::sleep(Duration::from_millis(40));
            push(&log, "slow_end");
            Ok(json!(null))
        }
    });
    let child_b_log = log.clone();
    bus_b.on_raw("ChildEvent", "child_b", move |_event| {
        let log = child_b_log.clone();
        async move {
            push(&log, "child_b_start");
            thread::sleep(Duration::from_millis(5));
            push(&log, "child_b_end");
            Ok(json!(null))
        }
    });
    let child_a_log = log.clone();
    bus_a.on_raw("ChildEvent", "child_a", move |_event| {
        let log = child_a_log.clone();
        async move {
            push(&log, "child_a_start");
            thread::sleep(Duration::from_millis(5));
            push(&log, "child_a_end");
            Ok(json!(null))
        }
    });

    let bus_a_for_trigger = bus_a.clone();
    let bus_b_for_trigger = bus_b.clone();
    bus_a.on_raw("Event1", "trigger_handler", move |_event| {
        let bus_a = bus_a_for_trigger.clone();
        let bus_b = bus_b_for_trigger.clone();
        async move {
            let child = bus_a.emit_child(ChildEvent {
                ..Default::default()
            });
            bus_b.emit_base(child.inner.clone());
            let _ = child.now().await;
            Ok(json!(null))
        }
    });

    bus_b.emit(SlowEvent {
        ..Default::default()
    });
    wait_for_entry(&log, "slow_start");
    let top = bus_a.emit(Event1 {
        ..Default::default()
    });
    let _ = block_on(top.now());
    block_on(bus_a.wait_until_idle(Some(2.0)));
    block_on(bus_b.wait_until_idle(Some(2.0)));

    let log = log.lock().expect("log lock").clone();
    assert!(index_of(&log, "slow_end") < index_of(&log, "child_b_start"));
    assert!(log.contains(&"child_a_start".to_string()));
    assert!(log.contains(&"child_a_end".to_string()));
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_queue_jump_with_fully_parallel_forward_bus_starts_immediately() {
    let _guard = test_guard();
    let bus_a = new_bus_with_concurrency(
        "QJFullPar_A",
        EventConcurrencyMode::BusSerial,
        EventHandlerConcurrencyMode::Serial,
        EventHandlerCompletionMode::All,
    );
    let bus_b = new_bus_with_concurrency(
        "QJFullPar_B",
        EventConcurrencyMode::Parallel,
        EventHandlerConcurrencyMode::Parallel,
        EventHandlerCompletionMode::All,
    );
    let log = Arc::new(Mutex::new(Vec::new()));

    let slow_log = log.clone();
    bus_b.on_raw("SlowEvent", "slow", move |_event| {
        let log = slow_log.clone();
        async move {
            push(&log, "slow_start");
            thread::sleep(Duration::from_millis(40));
            push(&log, "slow_end");
            Ok(json!(null))
        }
    });
    let child_log = log.clone();
    bus_b.on_raw("ChildEvent", "child_b", move |_event| {
        let log = child_log.clone();
        async move {
            push(&log, "child_b_start");
            thread::sleep(Duration::from_millis(5));
            push(&log, "child_b_end");
            Ok(json!(null))
        }
    });

    let bus_a_for_trigger = bus_a.clone();
    let bus_b_for_trigger = bus_b.clone();
    bus_a.on_raw("Event1", "trigger_handler", move |_event| {
        let bus_a = bus_a_for_trigger.clone();
        let bus_b = bus_b_for_trigger.clone();
        async move {
            let child = bus_a.emit_child(ChildEvent {
                ..Default::default()
            });
            bus_b.emit_base(child.inner.clone());
            let _ = child.now().await;
            Ok(json!(null))
        }
    });

    bus_b.emit(SlowEvent {
        ..Default::default()
    });
    wait_for_entry(&log, "slow_start");
    let top = bus_a.emit(Event1 {
        ..Default::default()
    });
    let _ = block_on(top.now());
    block_on(bus_a.wait_until_idle(Some(2.0)));
    block_on(bus_b.wait_until_idle(Some(2.0)));

    let log = log.lock().expect("log lock").clone();
    assert!(index_of(&log, "child_b_start") < index_of(&log, "slow_end"));
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_queue_jump_with_parallel_events_and_serial_handlers_on_forward_bus_still_overlaps_across_events(
) {
    let _guard = test_guard();
    let bus_a = new_bus_with_concurrency(
        "QJEvtParHSer_A",
        EventConcurrencyMode::BusSerial,
        EventHandlerConcurrencyMode::Serial,
        EventHandlerCompletionMode::All,
    );
    let bus_b = new_bus_with_concurrency(
        "QJEvtParHSer_B",
        EventConcurrencyMode::Parallel,
        EventHandlerConcurrencyMode::Serial,
        EventHandlerCompletionMode::All,
    );
    let log = Arc::new(Mutex::new(Vec::new()));

    let slow_log = log.clone();
    bus_b.on_raw("SlowEvent", "slow", move |_event| {
        let log = slow_log.clone();
        async move {
            push(&log, "slow_start");
            thread::sleep(Duration::from_millis(40));
            push(&log, "slow_end");
            Ok(json!(null))
        }
    });
    let child_log = log.clone();
    bus_b.on_raw("ChildEvent", "child_b", move |_event| {
        let log = child_log.clone();
        async move {
            push(&log, "child_b_start");
            thread::sleep(Duration::from_millis(5));
            push(&log, "child_b_end");
            Ok(json!(null))
        }
    });

    let bus_a_for_trigger = bus_a.clone();
    let bus_b_for_trigger = bus_b.clone();
    bus_a.on_raw("Event1", "trigger_handler", move |_event| {
        let bus_a = bus_a_for_trigger.clone();
        let bus_b = bus_b_for_trigger.clone();
        async move {
            let child = bus_a.emit_child(ChildEvent {
                ..Default::default()
            });
            bus_b.emit_base(child.inner.clone());
            let _ = child.now().await;
            Ok(json!(null))
        }
    });

    bus_b.emit(SlowEvent {
        ..Default::default()
    });
    wait_for_entry(&log, "slow_start");
    let top = bus_a.emit(Event1 {
        ..Default::default()
    });
    let _ = block_on(top.now());
    block_on(bus_a.wait_until_idle(Some(2.0)));
    block_on(bus_b.wait_until_idle(Some(2.0)));

    let log = log.lock().expect("log lock").clone();
    assert!(index_of(&log, "child_b_start") < index_of(&log, "slow_end"));
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_forwarded_first_mode_uses_processing_bus_handler_defaults() {
    test_forwarded_first_mode_uses_processing_bus_handler_concurrency_defaults();
}

#[test]
fn test_comprehensive_patterns() {
    test_comprehensive_patterns_forwarding_async_sync_dispatch_parent_tracking();
}

#[test]
fn test_multi_bus_forwarding_with_queued_events() {
    test_multi_bus_queues_are_independent_when_awaiting_child();
}

#[test]
fn test_awaited_child_jumps_queue_no_overshoot() {
    test_awaited_child_jumps_queue_without_overshoot();
}

#[test]
fn test_dispatch_multiple_await_one_skips_others() {
    test_dispatch_multiple_await_one_skips_others_until_after_handler_completes();
}

#[test]
fn test_await_already_completed_event() {
    test_awaiting_an_already_completed_event_is_a_no_op();
}

#[test]
fn test_multiple_awaits_same_event() {
    test_multiple_awaits_on_same_event();
}

#[test]
fn test_forwarded_event_uses_processing_bus_defaults_unless_overridden() {
    test_forwarded_event_uses_processing_bus_defaults_unless_explicit_overrides_are_set();
}
