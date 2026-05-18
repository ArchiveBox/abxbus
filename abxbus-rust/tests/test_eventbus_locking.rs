use abxbus::event;
use std::{
    collections::HashMap,
    env, fs,
    process::Command,
    sync::atomic::{AtomicBool, Ordering},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use abxbus::{
    event_bus::{EventBus, EventBusOptions},
    event_result::EventResultStatus,
    types::{EventConcurrencyMode, EventHandlerConcurrencyMode, EventStatus},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}
event! {
    struct QEvent {
        idx: i64,
        event_result_type: EmptyResult,
        event_type: "q",
    }
}
event! {
    struct WorkEvent {
        event_result_type: EmptyResult,
        event_type: "work",
    }
}
event! {
    struct ParentEvent {
        event_result_type: EmptyResult,
        event_type: "parent",
    }
}
event! {
    struct SiblingEvent {
        event_result_type: EmptyResult,
        event_type: "sibling",
    }
}
event! {
    struct SerialEvent {
        order: i64,
        source: String,
        event_result_type: EmptyResult,
        event_type: "serial",
    }
}
fn bump_in_flight(in_flight: &Arc<Mutex<i64>>, max_in_flight: &Arc<Mutex<i64>>) {
    let current = {
        let mut in_flight = in_flight.lock().expect("in_flight lock");
        *in_flight += 1;
        *in_flight
    };
    let mut max_seen = max_in_flight.lock().expect("max_in_flight lock");
    *max_seen = (*max_seen).max(current);
}

fn drop_in_flight(in_flight: &Arc<Mutex<i64>>) {
    let mut in_flight = in_flight.lock().expect("in_flight lock");
    *in_flight -= 1;
}

#[test]
fn test_queue_jump() {
    let bus = EventBus::new(Some("BusJump".to_string()));
    let order = Arc::new(Mutex::new(Vec::new()));
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let order_for_handler = order.clone();

    bus.on_raw("q", "h", move |event| {
        let order = order_for_handler.clone();
        let started_tx = started_tx.clone();
        async move {
            let value = event
                .inner
                .lock()
                .payload
                .get("idx")
                .and_then(serde_json::Value::as_i64)
                .expect("idx payload");
            order.lock().expect("order lock").push(value);
            if value == 0 {
                let _ = started_tx.send(());
                thread::sleep(Duration::from_millis(50));
            }
            Ok(json!(value))
        }
    });

    let blocker = bus.emit(QEvent {
        idx: 0,
        ..Default::default()
    });
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("blocker should start");
    let sibling = bus.emit(QEvent {
        idx: 1,
        ..Default::default()
    });
    let jumped = bus.emit_with_options(
        QEvent {
            idx: 2,
            ..Default::default()
        },
        true,
    );

    block_on(async {
        let _ = blocker.now().await;
        let _ = sibling.now().await;
        let _ = jumped.now().await;
    });

    let order = order.lock().expect("order lock").clone();
    assert_eq!(order, vec![0, 2, 1]);

    let sibling_started = sibling
        ._inner_event()
        .inner
        .lock()
        .event_started_at
        .clone()
        .unwrap_or_default();
    let jumped_started = jumped
        ._inner_event()
        .inner
        .lock()
        .event_started_at
        .clone()
        .unwrap_or_default();
    assert!(jumped_started <= sibling_started);
    bus.destroy();
}

#[test]
fn test_emit_with_queue_jump_preempts_queued_sibling_on_same_bus() {
    let bus = EventBus::new(Some("BusJumpNamedParity".to_string()));
    let order = Arc::new(Mutex::new(Vec::new()));
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let order_for_handler = order.clone();

    bus.on_raw("q", "h", move |event| {
        let order = order_for_handler.clone();
        let started_tx = started_tx.clone();
        async move {
            let value = event
                .inner
                .lock()
                .payload
                .get("idx")
                .and_then(serde_json::Value::as_i64)
                .expect("idx payload");
            order.lock().expect("order lock").push(value);
            if value == 0 {
                let _ = started_tx.send(());
                thread::sleep(Duration::from_millis(50));
            }
            Ok(json!(value))
        }
    });

    let blocker = bus.emit(QEvent {
        idx: 0,
        ..Default::default()
    });
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("blocker should start");
    let sibling = bus.emit(QEvent {
        idx: 1,
        ..Default::default()
    });
    let jumped = bus.emit_with_options(
        QEvent {
            idx: 2,
            ..Default::default()
        },
        true,
    );

    block_on(async {
        let _ = blocker.now().await;
        let _ = sibling.now().await;
        let _ = jumped.now().await;
    });

    assert_eq!(order.lock().expect("order lock").as_slice(), &[0, 2, 1]);
    bus.destroy();
}

#[test]
fn test_bus_serial_processes_in_order() {
    let bus = EventBus::new(Some("BusSerial".to_string()));

    bus.on_raw("work", "slow", |_event| async move {
        thread::sleep(Duration::from_millis(15));
        Ok(json!(1))
    });

    let event1 = WorkEvent {
        event_concurrency: Some(EventConcurrencyMode::BusSerial),
        ..Default::default()
    };
    let event2 = WorkEvent {
        event_concurrency: Some(EventConcurrencyMode::BusSerial),
        ..Default::default()
    };
    let event1 = bus.emit(event1);
    let event2 = bus.emit(event2);

    block_on(async {
        let _ = event1.now().await;
        let _ = event2.now().await;
    });

    let event1_started = event1
        ._inner_event()
        .inner
        .lock()
        .event_started_at
        .clone()
        .unwrap_or_default();
    let event2_started = event2
        ._inner_event()
        .inner
        .lock()
        .event_started_at
        .clone()
        .unwrap_or_default();
    assert!(event1_started <= event2_started);
    bus.destroy();
}

#[test]
fn test_bus_serial_fifo_order_preserved_per_bus_with_interleaving() {
    let bus_a = EventBus::new_with_options(
        Some("BusSerialOrderA".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );
    let bus_b = EventBus::new_with_options(
        Some("BusSerialOrderB".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );
    let starts_a = Arc::new(Mutex::new(Vec::new()));
    let starts_b = Arc::new(Mutex::new(Vec::new()));

    let starts_a_for_handler = starts_a.clone();
    bus_a.on_raw("serial", "record_a", move |event| {
        let starts = starts_a_for_handler.clone();
        async move {
            let order = event
                .inner
                .lock()
                .payload
                .get("order")
                .and_then(serde_json::Value::as_i64)
                .expect("order payload");
            starts.lock().expect("starts_a lock").push(order);
            thread::sleep(Duration::from_millis(2));
            Ok(json!(null))
        }
    });
    let starts_b_for_handler = starts_b.clone();
    bus_b.on_raw("serial", "record_b", move |event| {
        let starts = starts_b_for_handler.clone();
        async move {
            let order = event
                .inner
                .lock()
                .payload
                .get("order")
                .and_then(serde_json::Value::as_i64)
                .expect("order payload");
            starts.lock().expect("starts_b lock").push(order);
            thread::sleep(Duration::from_millis(2));
            Ok(json!(null))
        }
    });

    for order in 0..4 {
        bus_a.emit(SerialEvent {
            order,
            source: "a".to_string(),
            ..Default::default()
        });
        bus_b.emit(SerialEvent {
            order,
            source: "b".to_string(),
            ..Default::default()
        });
    }

    block_on(async {
        assert!(bus_a.wait_until_idle(Some(2.0)).await);
        assert!(bus_b.wait_until_idle(Some(2.0)).await);
    });

    assert_eq!(
        starts_a.lock().expect("starts_a lock").as_slice(),
        &[0, 1, 2, 3]
    );
    assert_eq!(
        starts_b.lock().expect("starts_b lock").as_slice(),
        &[0, 1, 2, 3]
    );
    bus_a.destroy();
    bus_b.destroy();
}

#[test]
fn test_event_concurrency_global_serial_allows_only_one_inflight_across_buses() {
    let bus_a = EventBus::new_with_options(
        Some("GlobalSerialA".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::GlobalSerial,
            ..EventBusOptions::default()
        },
    );
    let bus_b = EventBus::new_with_options(
        Some("GlobalSerialB".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::GlobalSerial,
            ..EventBusOptions::default()
        },
    );
    let in_flight = Arc::new(Mutex::new(0));
    let max_in_flight = Arc::new(Mutex::new(0));
    let starts = Arc::new(Mutex::new(Vec::new()));

    for bus in [&bus_a, &bus_b] {
        let in_flight = in_flight.clone();
        let max_in_flight = max_in_flight.clone();
        let starts = starts.clone();
        bus.on_raw("serial", "global_serial_handler", move |event| {
            let in_flight = in_flight.clone();
            let max_in_flight = max_in_flight.clone();
            let starts = starts.clone();
            async move {
                let payload = event.inner.lock().payload.clone();
                let source = payload
                    .get("source")
                    .and_then(serde_json::Value::as_str)
                    .expect("source")
                    .to_string();
                let order = payload
                    .get("order")
                    .and_then(serde_json::Value::as_i64)
                    .expect("order");
                bump_in_flight(&in_flight, &max_in_flight);
                starts
                    .lock()
                    .expect("starts lock")
                    .push(format!("{source}:{order}"));
                thread::sleep(Duration::from_millis(10));
                drop_in_flight(&in_flight);
                Ok(json!(null))
            }
        });
    }

    for i in 0..3 {
        bus_a.emit(SerialEvent {
            order: i,
            source: "a".to_string(),
            ..Default::default()
        });
        bus_b.emit(SerialEvent {
            order: i,
            source: "b".to_string(),
            ..Default::default()
        });
    }

    block_on(async {
        assert!(bus_a.wait_until_idle(Some(2.0)).await);
        assert!(bus_b.wait_until_idle(Some(2.0)).await);
    });

    assert_eq!(*max_in_flight.lock().expect("max lock"), 1);
    let starts = starts.lock().expect("starts lock").clone();
    let starts_a: Vec<i64> = starts
        .iter()
        .filter(|value| value.starts_with("a:"))
        .map(|value| value[2..].parse().expect("order"))
        .collect();
    let starts_b: Vec<i64> = starts
        .iter()
        .filter(|value| value.starts_with("b:"))
        .map(|value| value[2..].parse().expect("order"))
        .collect();
    assert_eq!(starts_a, vec![0, 1, 2]);
    assert_eq!(starts_b, vec![0, 1, 2]);
    bus_a.destroy();
    bus_b.destroy();
}

#[test]
fn test_global_serial_awaited_child_jumps_ahead_of_queued_events_across_buses() {
    let bus_a = EventBus::new_with_options(
        Some("GlobalSerialParent".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::GlobalSerial,
            ..EventBusOptions::default()
        },
    );
    let bus_b = EventBus::new_with_options(
        Some("GlobalSerialChild".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::GlobalSerial,
            ..EventBusOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));

    let order_for_child = order.clone();
    bus_b.on_raw("work", "child_handler", move |_event| {
        let order = order_for_child.clone();
        async move {
            order
                .lock()
                .expect("order lock")
                .push("child_start".to_string());
            thread::sleep(Duration::from_millis(5));
            order
                .lock()
                .expect("order lock")
                .push("child_end".to_string());
            Ok(json!(null))
        }
    });

    let order_for_queued = order.clone();
    bus_b.on_raw("q", "queued_handler", move |_event| {
        let order = order_for_queued.clone();
        async move {
            order
                .lock()
                .expect("order lock")
                .push("queued_start".to_string());
            Ok(json!(null))
        }
    });

    let bus_a_for_parent = bus_a.clone();
    let bus_b_for_parent = bus_b.clone();
    let order_for_parent = order.clone();
    bus_a.on_raw("parent", "parent_handler", move |_event| {
        let bus_a = bus_a_for_parent.clone();
        let bus_b = bus_b_for_parent.clone();
        let order = order_for_parent.clone();
        async move {
            order
                .lock()
                .expect("order lock")
                .push("parent_start".to_string());
            bus_b.emit(QEvent {
                idx: 1,
                ..Default::default()
            });
            let child = bus_a.emit_child(WorkEvent {
                ..Default::default()
            });
            bus_b.emit(
                <WorkEvent as abxbus::typed::TypedEventObject>::_from_inner_event(
                    child._inner_event(),
                ),
            );
            order
                .lock()
                .expect("order lock")
                .push("child_dispatched".to_string());
            let _ = child.now().await;
            order
                .lock()
                .expect("order lock")
                .push("child_awaited".to_string());
            Ok(json!(null))
        }
    });

    let parent = bus_a.emit(ParentEvent {
        ..Default::default()
    });
    let _ = block_on(parent.now());
    block_on(bus_b.wait_until_idle(Some(2.0)));

    let order = order.lock().expect("order lock").clone();
    let child_start_idx = order
        .iter()
        .position(|entry| entry == "child_start")
        .expect("child start");
    let child_end_idx = order
        .iter()
        .position(|entry| entry == "child_end")
        .expect("child end");
    let queued_start_idx = order
        .iter()
        .position(|entry| entry == "queued_start")
        .expect("queued start");
    assert!(child_start_idx < queued_start_idx);
    assert!(child_end_idx < queued_start_idx);
    bus_a.destroy();
    bus_b.destroy();
}

#[test]
fn test_wait_waits_in_queue_order_inside_handler_without_queue_jump() {
    let bus = EventBus::new_with_options(
        Some("QueueOrderEventCompletedBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::Parallel,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    let bus_for_parent = bus.clone();
    let order = Arc::new(Mutex::new(Vec::new()));
    let child_ref = Arc::new(Mutex::new(None::<Arc<abxbus::base_event::BaseEvent>>));
    let sibling_started = Arc::new(AtomicBool::new(false));

    let order_for_parent = order.clone();
    let child_ref_for_parent = child_ref.clone();
    let sibling_started_for_parent = sibling_started.clone();
    bus.on_raw("parent", "parent_handler", move |_event| {
        let bus = bus_for_parent.clone();
        let order = order_for_parent.clone();
        let child_ref = child_ref_for_parent.clone();
        let sibling_started = sibling_started_for_parent.clone();
        async move {
            order
                .lock()
                .expect("order lock")
                .push("parent_start".to_string());
            bus.emit(SiblingEvent {
                ..Default::default()
            });
            let deadline = std::time::Instant::now() + Duration::from_millis(500);
            while !sibling_started.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
            let child = bus.emit_child(WorkEvent {
                ..Default::default()
            });
            *child_ref.lock().expect("child ref lock") = Some(child._inner_event());
            let _ = child.wait().await;
            order
                .lock()
                .expect("order lock")
                .push("parent_end".to_string());
            Ok(json!(null))
        }
    });

    let order_for_sibling = order.clone();
    let sibling_started_for_sibling = sibling_started.clone();
    bus.on_raw("sibling", "sibling_handler", move |_event| {
        let order = order_for_sibling.clone();
        let sibling_started = sibling_started_for_sibling.clone();
        async move {
            order
                .lock()
                .expect("order lock")
                .push("sibling_start".to_string());
            sibling_started.store(true, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(5));
            order
                .lock()
                .expect("order lock")
                .push("sibling_end".to_string());
            Ok(json!(null))
        }
    });

    let order_for_child = order.clone();
    bus.on_raw("work", "child_handler", move |_event| {
        let order = order_for_child.clone();
        async move {
            order
                .lock()
                .expect("order lock")
                .push("child_start".to_string());
            thread::sleep(Duration::from_millis(5));
            order
                .lock()
                .expect("order lock")
                .push("child_end".to_string());
            Ok(json!(null))
        }
    });

    let parent = bus.emit(ParentEvent {
        ..Default::default()
    });
    let _ = block_on(parent.now());
    block_on(bus.wait_until_idle(Some(2.0)));

    let order = order.lock().expect("order lock").clone();
    let sibling_start_idx = order
        .iter()
        .position(|entry| entry == "sibling_start")
        .expect("sibling start");
    let child_start_idx = order
        .iter()
        .position(|entry| entry == "child_start")
        .expect("child start");
    let child_end_idx = order
        .iter()
        .position(|entry| entry == "child_end")
        .expect("child end");
    let parent_end_idx = order
        .iter()
        .position(|entry| entry == "parent_end")
        .expect("parent end");
    assert!(sibling_start_idx < child_start_idx);
    assert!(child_end_idx < parent_end_idx);

    let child = child_ref
        .lock()
        .expect("child ref lock")
        .clone()
        .expect("child ref");
    assert!(!child.inner.lock().event_blocks_parent_completion);
    bus.destroy();
}

#[test]
fn test_event_concurrency_bus_serial_serializes_per_bus_but_overlaps_across_buses() {
    let bus_a = EventBus::new_with_options(
        Some("BusSerialA".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );
    let bus_b = EventBus::new_with_options(
        Some("BusSerialB".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );
    let in_flight_global = Arc::new(Mutex::new(0));
    let max_in_flight_global = Arc::new(Mutex::new(0));

    for bus in [&bus_a, &bus_b] {
        let in_flight_global = in_flight_global.clone();
        let max_in_flight_global = max_in_flight_global.clone();
        bus.on_raw("serial", "bus_serial_handler", move |_event| {
            let in_flight_global = in_flight_global.clone();
            let max_in_flight_global = max_in_flight_global.clone();
            async move {
                bump_in_flight(&in_flight_global, &max_in_flight_global);
                thread::sleep(Duration::from_millis(30));
                drop_in_flight(&in_flight_global);
                Ok(json!(null))
            }
        });
    }

    bus_a.emit(SerialEvent {
        order: 0,
        source: "a".to_string(),
        ..Default::default()
    });
    bus_b.emit(SerialEvent {
        order: 0,
        source: "b".to_string(),
        ..Default::default()
    });

    block_on(async {
        assert!(bus_a.wait_until_idle(Some(2.0)).await);
        assert!(bus_b.wait_until_idle(Some(2.0)).await);
    });

    assert!(*max_in_flight_global.lock().expect("max lock") >= 2);
    bus_a.destroy();
    bus_b.destroy();
}

#[test]
fn test_bus_serial_awaiting_child_on_one_bus_does_not_block_other_bus_queue() {
    let bus_a = EventBus::new_with_options(
        Some("BusSerialParentBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );
    let bus_b = EventBus::new_with_options(
        Some("BusSerialOtherBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));

    let order_for_child = order.clone();
    bus_a.on_raw("work", "child_handler", move |_event| {
        let order = order_for_child.clone();
        async move {
            order
                .lock()
                .expect("order lock")
                .push("child_start".to_string());
            thread::sleep(Duration::from_millis(25));
            order
                .lock()
                .expect("order lock")
                .push("child_end".to_string());
            Ok(json!(null))
        }
    });

    let bus_a_for_parent = bus_a.clone();
    let order_for_parent = order.clone();
    bus_a.on_raw("parent", "parent_handler", move |_event| {
        let bus_a = bus_a_for_parent.clone();
        let order = order_for_parent.clone();
        async move {
            order
                .lock()
                .expect("order lock")
                .push("parent_start".to_string());
            let child = bus_a.emit_child(WorkEvent {
                ..Default::default()
            });
            let _ = child.now().await;
            order
                .lock()
                .expect("order lock")
                .push("parent_end".to_string());
            Ok(json!(null))
        }
    });

    let order_for_other = order.clone();
    bus_b.on_raw("sibling", "other_handler", move |_event| {
        let order = order_for_other.clone();
        async move {
            order
                .lock()
                .expect("order lock")
                .push("other_start".to_string());
            thread::sleep(Duration::from_millis(2));
            order
                .lock()
                .expect("order lock")
                .push("other_end".to_string());
            Ok(json!(null))
        }
    });

    let parent = bus_a.emit(ParentEvent {
        ..Default::default()
    });
    thread::sleep(Duration::from_millis(1));
    bus_b.emit(SiblingEvent {
        ..Default::default()
    });

    block_on(async {
        let _ = parent.now().await;
        assert!(bus_a.wait_until_idle(Some(2.0)).await);
        assert!(bus_b.wait_until_idle(Some(2.0)).await);
    });

    let order = order.lock().expect("order lock").clone();
    let other_start_idx = order
        .iter()
        .position(|entry| entry == "other_start")
        .expect("other_start");
    let parent_end_idx = order
        .iter()
        .position(|entry| entry == "parent_end")
        .expect("parent_end");
    assert!(other_start_idx < parent_end_idx);
    bus_a.destroy();
    bus_b.destroy();
}

#[test]
fn test_event_concurrency_parallel_allows_same_bus_events_to_overlap() {
    let bus = EventBus::new_with_options(
        Some("ParallelEventBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::Parallel,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    let in_flight = Arc::new(Mutex::new(0));
    let max_in_flight = Arc::new(Mutex::new(0));
    let in_flight_for_handler = in_flight.clone();
    let max_for_handler = max_in_flight.clone();
    bus.on_raw("serial", "parallel_event_handler", move |_event| {
        let in_flight = in_flight_for_handler.clone();
        let max_in_flight = max_for_handler.clone();
        async move {
            bump_in_flight(&in_flight, &max_in_flight);
            thread::sleep(Duration::from_millis(40));
            drop_in_flight(&in_flight);
            Ok(json!(null))
        }
    });

    bus.emit(SerialEvent {
        order: 0,
        source: "same".to_string(),
        ..Default::default()
    });
    bus.emit(SerialEvent {
        order: 1,
        source: "same".to_string(),
        ..Default::default()
    });

    block_on(async {
        assert!(bus.wait_until_idle(Some(2.0)).await);
    });
    assert!(*max_in_flight.lock().expect("max lock") >= 2);
    bus.destroy();
}

#[test]
fn test_event_handler_concurrency_parallel_runs_handlers_for_same_event_concurrently() {
    let bus = EventBus::new_with_options(
        Some("ParallelHandlerBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    let in_flight = Arc::new(Mutex::new(0));
    let max_in_flight = Arc::new(Mutex::new(0));

    for handler_name in ["handler_a", "handler_b"] {
        let in_flight = in_flight.clone();
        let max_in_flight = max_in_flight.clone();
        bus.on_raw("work", handler_name, move |_event| {
            let in_flight = in_flight.clone();
            let max_in_flight = max_in_flight.clone();
            async move {
                bump_in_flight(&in_flight, &max_in_flight);
                thread::sleep(Duration::from_millis(30));
                drop_in_flight(&in_flight);
                Ok(json!(null))
            }
        });
    }

    let event = bus.emit(WorkEvent {
        ..Default::default()
    });
    let _ = block_on(event.now());
    assert!(*max_in_flight.lock().expect("max lock") >= 2);
    bus.destroy();
}

#[test]
fn test_event_concurrency_override_parallel_beats_bus_serial_default() {
    let bus = EventBus::new_with_options(
        Some("OverrideParallelBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    let in_flight = Arc::new(Mutex::new(0));
    let max_in_flight = Arc::new(Mutex::new(0));
    let in_flight_for_handler = in_flight.clone();
    let max_for_handler = max_in_flight.clone();
    bus.on_raw("serial", "override_parallel_handler", move |_event| {
        let in_flight = in_flight_for_handler.clone();
        let max_in_flight = max_for_handler.clone();
        async move {
            bump_in_flight(&in_flight, &max_in_flight);
            thread::sleep(Duration::from_millis(40));
            drop_in_flight(&in_flight);
            Ok(json!(null))
        }
    });

    for order in 0..2 {
        let mut event = SerialEvent {
            order,
            source: "override".to_string(),
            ..Default::default()
        };
        event.event_concurrency = Some(EventConcurrencyMode::Parallel);
        bus.emit(event);
    }

    block_on(async {
        assert!(bus.wait_until_idle(Some(2.0)).await);
    });
    assert!(*max_in_flight.lock().expect("max lock") >= 2);
    bus.destroy();
}

#[test]
fn test_event_concurrency_override_bus_serial_beats_bus_parallel_default() {
    let bus = EventBus::new_with_options(
        Some("OverrideBusSerialBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::Parallel,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    let in_flight = Arc::new(Mutex::new(0));
    let max_in_flight = Arc::new(Mutex::new(0));
    let in_flight_for_handler = in_flight.clone();
    let max_for_handler = max_in_flight.clone();
    bus.on_raw("serial", "override_bus_serial_handler", move |_event| {
        let in_flight = in_flight_for_handler.clone();
        let max_in_flight = max_for_handler.clone();
        async move {
            bump_in_flight(&in_flight, &max_in_flight);
            thread::sleep(Duration::from_millis(30));
            drop_in_flight(&in_flight);
            Ok(json!(null))
        }
    });

    for order in 0..2 {
        let mut event = SerialEvent {
            order,
            source: "override".to_string(),
            ..Default::default()
        };
        event.event_concurrency = Some(EventConcurrencyMode::BusSerial);
        bus.emit(event);
    }

    block_on(async {
        assert!(bus.wait_until_idle(Some(2.0)).await);
    });
    assert_eq!(*max_in_flight.lock().expect("max lock"), 1);
    bus.destroy();
}

#[test]
fn test_queue_jump_awaited_child_preempts_queued_sibling_on_same_bus() {
    let bus = EventBus::new_with_options(
        Some("QueueJumpBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));
    let captured_child = Arc::new(Mutex::new(None::<Arc<abxbus::base_event::BaseEvent>>));

    let bus_for_parent = bus.clone();
    let order_for_parent = order.clone();
    let child_for_parent = captured_child.clone();
    bus.on_raw("parent", "parent_handler", move |_event| {
        let bus = bus_for_parent.clone();
        let order = order_for_parent.clone();
        let captured_child = child_for_parent.clone();
        async move {
            order
                .lock()
                .expect("order lock")
                .push("parent_start".to_string());
            let child = bus.emit_child(WorkEvent {
                ..Default::default()
            });
            *captured_child.lock().expect("captured child lock") = Some(child._inner_event());
            let _ = child.now().await;
            order
                .lock()
                .expect("order lock")
                .push("parent_end".to_string());
            Ok(json!(null))
        }
    });

    let order_for_child = order.clone();
    bus.on_raw("work", "child_handler", move |_event| {
        let order = order_for_child.clone();
        async move {
            order
                .lock()
                .expect("order lock")
                .push("child_start".to_string());
            thread::sleep(Duration::from_millis(5));
            order
                .lock()
                .expect("order lock")
                .push("child_end".to_string());
            Ok(json!(null))
        }
    });

    let order_for_sibling = order.clone();
    bus.on_raw("sibling", "sibling_handler", move |_event| {
        let order = order_for_sibling.clone();
        async move {
            order
                .lock()
                .expect("order lock")
                .push("sibling".to_string());
            Ok(json!(null))
        }
    });

    let parent = bus.emit(ParentEvent {
        ..Default::default()
    });
    let sibling = bus.emit(SiblingEvent {
        ..Default::default()
    });

    block_on(async {
        let _ = parent.now().await;
        let _ = sibling.now().await;
        assert!(bus.wait_until_idle(Some(1.0)).await);
    });

    assert_eq!(
        order.lock().expect("order lock").as_slice(),
        &[
            "parent_start".to_string(),
            "child_start".to_string(),
            "child_end".to_string(),
            "parent_end".to_string(),
            "sibling".to_string(),
        ]
    );

    let child = captured_child
        .lock()
        .expect("captured child lock")
        .clone()
        .expect("captured child");
    let parent_id = parent.event_id.clone();
    let child_inner = child.inner.lock();
    assert_eq!(
        child_inner.event_parent_id.as_deref(),
        Some(parent_id.as_str())
    );
    assert!(child_inner.event_blocks_parent_completion);
    assert!(child_inner.event_emitted_by_handler_id.is_some());
    bus.destroy();
}

#[test]
fn test_global_serial_with_handler_parallel_allows_handlers_but_not_events_to_overlap() {
    let bus_a = EventBus::new_with_options(
        Some("GlobalSerialParallelA".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::GlobalSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    let bus_b = EventBus::new_with_options(
        Some("GlobalSerialParallelB".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::GlobalSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    let in_flight = Arc::new(Mutex::new(0));
    let max_in_flight = Arc::new(Mutex::new(0));

    for bus in [&bus_a, &bus_b] {
        for handler_name in ["handler_a", "handler_b"] {
            let in_flight = in_flight.clone();
            let max_in_flight = max_in_flight.clone();
            bus.on_raw("work", handler_name, move |_event| {
                let in_flight = in_flight.clone();
                let max_in_flight = max_in_flight.clone();
                async move {
                    bump_in_flight(&in_flight, &max_in_flight);
                    thread::sleep(Duration::from_millis(30));
                    drop_in_flight(&in_flight);
                    Ok(json!(null))
                }
            });
        }
    }

    bus_a.emit(WorkEvent {
        ..Default::default()
    });
    bus_b.emit(WorkEvent {
        ..Default::default()
    });

    block_on(async {
        assert!(bus_a.wait_until_idle(Some(2.0)).await);
        assert!(bus_b.wait_until_idle(Some(2.0)).await);
    });

    assert_eq!(*max_in_flight.lock().expect("max lock"), 2);
    bus_a.destroy();
    bus_b.destroy();
}

#[test]
fn test_event_parallel_with_handler_serial_serializes_handlers_within_each_event() {
    let bus = EventBus::new_with_options(
        Some("ParallelEventsSerialHandlersBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::Parallel,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let global_in_flight = Arc::new(Mutex::new(0));
    let global_max = Arc::new(Mutex::new(0));
    let per_event_in_flight = Arc::new(Mutex::new(HashMap::<String, i64>::new()));
    let per_event_max = Arc::new(Mutex::new(HashMap::<String, i64>::new()));

    for handler_name in ["handler_a", "handler_b"] {
        let global_in_flight = global_in_flight.clone();
        let global_max = global_max.clone();
        let per_event_in_flight = per_event_in_flight.clone();
        let per_event_max = per_event_max.clone();
        bus.on_raw("serial", handler_name, move |event| {
            let global_in_flight = global_in_flight.clone();
            let global_max = global_max.clone();
            let per_event_in_flight = per_event_in_flight.clone();
            let per_event_max = per_event_max.clone();
            async move {
                let event_id = event.inner.lock().event_id.clone();
                bump_in_flight(&global_in_flight, &global_max);
                let current = {
                    let mut counts = per_event_in_flight
                        .lock()
                        .expect("per_event_in_flight lock");
                    let count = counts.entry(event_id.clone()).or_insert(0);
                    *count += 1;
                    *count
                };
                {
                    let mut maxes = per_event_max.lock().expect("per_event_max lock");
                    let max_seen = maxes.entry(event_id.clone()).or_insert(0);
                    *max_seen = (*max_seen).max(current);
                }
                thread::sleep(Duration::from_millis(30));
                {
                    let mut counts = per_event_in_flight
                        .lock()
                        .expect("per_event_in_flight lock");
                    *counts.get_mut(&event_id).expect("event count") -= 1;
                }
                drop_in_flight(&global_in_flight);
                Ok(json!(null))
            }
        });
    }

    for order in 0..2 {
        bus.emit(SerialEvent {
            order,
            source: "parallel".to_string(),
            ..Default::default()
        });
    }

    block_on(async {
        assert!(bus.wait_until_idle(Some(2.0)).await);
    });

    assert!(*global_max.lock().expect("global max lock") >= 2);
    assert!(per_event_max
        .lock()
        .expect("per_event_max lock")
        .values()
        .all(|max_seen| *max_seen == 1));
    bus.destroy();
}

#[test]
fn test_event_parallel_with_handler_serial_handlers_overlap_across_buses() {
    let bus_a = EventBus::new_with_options(
        Some("ParallelBusHandlersA".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::Parallel,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let bus_b = EventBus::new_with_options(
        Some("ParallelBusHandlersB".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::Parallel,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let in_flight = Arc::new(Mutex::new(0));
    let max_in_flight = Arc::new(Mutex::new(0));

    for bus in [&bus_a, &bus_b] {
        let in_flight = in_flight.clone();
        let max_in_flight = max_in_flight.clone();
        bus.on_raw("serial", "cross_bus_handler", move |_event| {
            let in_flight = in_flight.clone();
            let max_in_flight = max_in_flight.clone();
            async move {
                bump_in_flight(&in_flight, &max_in_flight);
                thread::sleep(Duration::from_millis(30));
                drop_in_flight(&in_flight);
                Ok(json!(null))
            }
        });
    }

    bus_a.emit(SerialEvent {
        order: 0,
        source: "a".to_string(),
        ..Default::default()
    });
    bus_b.emit(SerialEvent {
        order: 0,
        source: "b".to_string(),
        ..Default::default()
    });

    block_on(async {
        assert!(bus_a.wait_until_idle(Some(2.0)).await);
        assert!(bus_b.wait_until_idle(Some(2.0)).await);
    });

    assert!(*max_in_flight.lock().expect("max lock") >= 2);
    bus_a.destroy();
    bus_b.destroy();
}

#[test]
fn test_event_concurrency_null_resolves_to_bus_defaults() {
    let bus = EventBus::new_with_options(
        Some("AutoBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );
    let in_flight = Arc::new(Mutex::new(0));
    let max_in_flight = Arc::new(Mutex::new(0));
    let in_flight_for_handler = in_flight.clone();
    let max_for_handler = max_in_flight.clone();
    bus.on_raw("serial", "auto_event_handler", move |_event| {
        let in_flight = in_flight_for_handler.clone();
        let max_in_flight = max_for_handler.clone();
        async move {
            bump_in_flight(&in_flight, &max_in_flight);
            thread::sleep(Duration::from_millis(20));
            drop_in_flight(&in_flight);
            Ok(json!(null))
        }
    });

    for order in 0..2 {
        let mut event = SerialEvent {
            order,
            source: "auto".to_string(),
            ..Default::default()
        };
        event.event_concurrency = None;
        bus.emit(event);
    }

    block_on(async {
        assert!(bus.wait_until_idle(Some(2.0)).await);
    });
    assert_eq!(*max_in_flight.lock().expect("max lock"), 1);
    bus.destroy();
}

#[test]
fn test_event_handler_concurrency_null_resolves_to_bus_defaults() {
    let bus = EventBus::new_with_options(
        Some("AutoHandlerBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let in_flight = Arc::new(Mutex::new(0));
    let max_in_flight = Arc::new(Mutex::new(0));

    for handler_name in ["handler_a", "handler_b"] {
        let in_flight = in_flight.clone();
        let max_in_flight = max_in_flight.clone();
        bus.on_raw("work", handler_name, move |_event| {
            let in_flight = in_flight.clone();
            let max_in_flight = max_in_flight.clone();
            async move {
                bump_in_flight(&in_flight, &max_in_flight);
                thread::sleep(Duration::from_millis(20));
                drop_in_flight(&in_flight);
                Ok(json!(null))
            }
        });
    }

    let mut event = WorkEvent {
        ..Default::default()
    };
    event.event_handler_concurrency = None;
    let event = bus.emit(event);
    let _ = block_on(event.now());

    assert_eq!(*max_in_flight.lock().expect("max lock"), 1);
    bus.destroy();
}

#[test]
fn test_queue_jump_same_event_handlers_on_separate_buses_stay_isolated_without_forwarding() {
    let bus_a = EventBus::new_with_options(
        Some("QueueJumpIsolatedA".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );
    let bus_b = EventBus::new_with_options(
        Some("QueueJumpIsolatedB".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));
    let bus_a_shared_runs = Arc::new(Mutex::new(0));
    let bus_b_shared_runs = Arc::new(Mutex::new(0));

    let order_for_a = order.clone();
    let runs_for_a = bus_a_shared_runs.clone();
    bus_a.on_raw("work", "bus_a_shared", move |_event| {
        let order = order_for_a.clone();
        let runs = runs_for_a.clone();
        async move {
            *runs.lock().expect("runs lock") += 1;
            order
                .lock()
                .expect("order lock")
                .push("bus_a_shared_start".to_string());
            thread::sleep(Duration::from_millis(10));
            order
                .lock()
                .expect("order lock")
                .push("bus_a_shared_end".to_string());
            Ok(json!(null))
        }
    });
    let order_for_b = order.clone();
    let runs_for_b = bus_b_shared_runs.clone();
    bus_b.on_raw("work", "bus_b_shared", move |_event| {
        let order = order_for_b.clone();
        let runs = runs_for_b.clone();
        async move {
            *runs.lock().expect("runs lock") += 1;
            order
                .lock()
                .expect("order lock")
                .push("bus_b_shared_start".to_string());
            Ok(json!(null))
        }
    });
    let order_for_sibling = order.clone();
    bus_a.on_raw("q", "bus_a_sibling", move |_event| {
        let order = order_for_sibling.clone();
        async move {
            order
                .lock()
                .expect("order lock")
                .push("bus_a_sibling_start".to_string());
            Ok(json!(null))
        }
    });
    let bus_a_for_parent = bus_a.clone();
    let order_for_parent = order.clone();
    bus_a.on_raw("parent", "parent_handler", move |_event| {
        let bus_a = bus_a_for_parent.clone();
        let order = order_for_parent.clone();
        async move {
            order
                .lock()
                .expect("order lock")
                .push("parent_start".to_string());
            bus_a.emit(QEvent {
                idx: 1,
                ..Default::default()
            });
            let shared = bus_a.emit_child(WorkEvent {
                ..Default::default()
            });
            order
                .lock()
                .expect("order lock")
                .push("shared_dispatched".to_string());
            let _ = shared.now().await;
            order
                .lock()
                .expect("order lock")
                .push("shared_awaited".to_string());
            Ok(json!(null))
        }
    });

    let parent = bus_a.emit(ParentEvent {
        ..Default::default()
    });
    let _ = block_on(parent.now());
    block_on(async {
        assert!(bus_a.wait_until_idle(Some(2.0)).await);
        assert!(bus_b.wait_until_idle(Some(2.0)).await);
    });

    assert_eq!(*bus_a_shared_runs.lock().expect("runs lock"), 1);
    assert_eq!(*bus_b_shared_runs.lock().expect("runs lock"), 0);
    let order = order.lock().expect("order lock").clone();
    assert!(!order.contains(&"bus_b_shared_start".to_string()));
    let bus_a_shared_end_idx = order
        .iter()
        .position(|entry| entry == "bus_a_shared_end")
        .expect("bus_a shared end");
    let bus_a_sibling_start_idx = order
        .iter()
        .position(|entry| entry == "bus_a_sibling_start")
        .expect("bus_a sibling start");
    assert!(bus_a_shared_end_idx < bus_a_sibling_start_idx);
    bus_a.destroy();
    bus_b.destroy();
}

#[test]
fn test_awaited_bus_emit_inside_handler_queue_jumps_but_stays_untracked_root_event() {
    let bus = EventBus::new_with_options(
        Some("AwaitedBusEmitRootBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );
    let bus_for_handler = bus.clone();
    let child_ref = Arc::new(Mutex::new(None::<Arc<abxbus::base_event::BaseEvent>>));
    let child_ref_for_handler = child_ref.clone();

    bus.on_raw("parent", "parent_handler", move |_event| {
        let bus = bus_for_handler.clone();
        let child_ref = child_ref_for_handler.clone();
        async move {
            let child = bus.emit(WorkEvent {
                ..Default::default()
            });
            assert_eq!(child.event_parent_id.clone(), None);
            assert_eq!(child.event_emitted_by_handler_id.clone(), None);
            assert!(!child.event_blocks_parent_completion);
            *child_ref.lock().expect("child ref lock") = Some(child._inner_event());
            let _ = child.now().await;
            assert!(!child.event_blocks_parent_completion);
            Ok(json!(null))
        }
    });
    bus.on_raw("work", "child_handler", |_event| async move {
        Ok(json!("child"))
    });

    let parent = bus.emit(ParentEvent {
        ..Default::default()
    });
    let _ = block_on(parent.now());
    block_on(bus.wait_until_idle(Some(2.0)));

    let child = child_ref
        .lock()
        .expect("child ref lock")
        .clone()
        .expect("child ref");
    assert_eq!(child.inner.lock().event_parent_id, None);
    assert_eq!(child.inner.lock().event_emitted_by_handler_id, None);
    assert!(!child.inner.lock().event_blocks_parent_completion);
    bus.destroy();
}

#[test]
fn test_awaited_bus_emit_inside_handler_preempts_queued_sibling_without_parentage() {
    let bus = EventBus::new_with_options(
        Some("AwaitedBusEmitQueueJumpBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );
    let bus_for_handler = bus.clone();
    let order = Arc::new(Mutex::new(Vec::new()));
    let child_ref = Arc::new(Mutex::new(None::<Arc<abxbus::base_event::BaseEvent>>));

    let order_for_parent = order.clone();
    let child_ref_for_parent = child_ref.clone();
    bus.on_raw("parent", "parent_handler", move |_event| {
        let bus = bus_for_handler.clone();
        let order = order_for_parent.clone();
        let child_ref = child_ref_for_parent.clone();
        async move {
            order
                .lock()
                .expect("order lock")
                .push("parent_start".to_string());
            bus.emit(SiblingEvent {
                ..Default::default()
            });
            let child = bus.emit(WorkEvent {
                ..Default::default()
            });
            *child_ref.lock().expect("child ref lock") = Some(child._inner_event());
            let _ = child.now().await;
            order
                .lock()
                .expect("order lock")
                .push("parent_end".to_string());
            Ok(json!(null))
        }
    });

    let order_for_child = order.clone();
    bus.on_raw("work", "child_handler", move |_event| {
        let order = order_for_child.clone();
        async move {
            order
                .lock()
                .expect("order lock")
                .push("child_start".to_string());
            Ok(json!("child"))
        }
    });

    let order_for_sibling = order.clone();
    bus.on_raw("sibling", "sibling_handler", move |_event| {
        let order = order_for_sibling.clone();
        async move {
            order
                .lock()
                .expect("order lock")
                .push("sibling_start".to_string());
            Ok(json!("sibling"))
        }
    });

    let parent = bus.emit(ParentEvent {
        ..Default::default()
    });
    let _ = block_on(parent.now());
    block_on(bus.wait_until_idle(Some(2.0)));

    let order = order.lock().expect("order lock").clone();
    let child_start_idx = order
        .iter()
        .position(|entry| entry == "child_start")
        .expect("child start");
    let sibling_start_idx = order
        .iter()
        .position(|entry| entry == "sibling_start")
        .expect("sibling start");
    let parent_end_idx = order
        .iter()
        .position(|entry| entry == "parent_end")
        .expect("parent end");
    assert!(child_start_idx < sibling_start_idx);
    assert!(parent_end_idx < sibling_start_idx);

    let child = child_ref
        .lock()
        .expect("child ref lock")
        .clone()
        .expect("child ref");
    assert_eq!(child.inner.lock().event_parent_id, None);
    assert_eq!(child.inner.lock().event_emitted_by_handler_id, None);
    assert!(!child.inner.lock().event_blocks_parent_completion);
    bus.destroy();
}

#[test]
fn test_awaiting_in_flight_event_does_not_double_run_handlers() {
    let bus = EventBus::new_with_options(
        Some("InFlightBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::Parallel,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    let handler_runs = Arc::new(Mutex::new(0));
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));

    let runs_for_handler = handler_runs.clone();
    let release_for_handler = release_rx.clone();
    bus.on_raw("work", "in_flight_handler", move |_event| {
        let started_tx = started_tx.clone();
        let runs = runs_for_handler.clone();
        let release_rx = release_for_handler.clone();
        async move {
            *runs.lock().expect("runs lock") += 1;
            let _ = started_tx.send(());
            release_rx
                .lock()
                .expect("release lock")
                .recv_timeout(Duration::from_secs(2))
                .expect("release signal");
            Ok(json!(null))
        }
    });

    let child = bus.emit(WorkEvent {
        ..Default::default()
    });
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("handler should start");

    let child_for_wait =
        <WorkEvent as abxbus::typed::TypedEventObject>::_from_inner_event(child._inner_event());
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let _ = block_on(child_for_wait.now());
        let _ = done_tx.send(());
    });
    assert!(done_rx.recv_timeout(Duration::from_millis(30)).is_err());

    release_tx.send(()).expect("release send");
    done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("done should resolve");
    block_on(bus.wait_until_idle(Some(2.0)));
    assert_eq!(*handler_runs.lock().expect("runs lock"), 1);
    bus.destroy();
}

#[test]
fn test_edge_case_event_with_no_handlers_completes_immediately() {
    let bus = EventBus::new(Some("NoHandlerBus".to_string()));

    let event = bus.emit(WorkEvent {
        ..Default::default()
    });
    block_on(async {
        let _ = event.now().await;
        assert!(bus.wait_until_idle(Some(2.0)).await);
    });

    let base = event._inner_event();
    let inner = base.inner.lock();
    assert_eq!(inner.event_status, EventStatus::Completed);
    assert_eq!(inner.event_pending_bus_count, 0);
    assert_eq!(inner.event_results.len(), 0);
    bus.destroy();
}

#[test]
fn test_fifo_forwarded_events_preserve_order_on_target_bus_bus_serial() {
    let bus_a = EventBus::new_with_options(
        Some("ForwardOrderA".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );
    let bus_b = EventBus::new_with_options(
        Some("ForwardOrderB".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );
    let order_a = Arc::new(Mutex::new(Vec::new()));
    let order_b = Arc::new(Mutex::new(Vec::new()));
    let bus_b_id = bus_b.id.clone();

    let order_a_for_handler = order_a.clone();
    let bus_b_for_forward = bus_b.clone();
    bus_a.on_raw("serial", "forward_order_a", move |event| {
        let order_a = order_a_for_handler.clone();
        let bus_b = bus_b_for_forward.clone();
        async move {
            let order = event
                .inner
                .lock()
                .payload
                .get("order")
                .and_then(serde_json::Value::as_i64)
                .expect("order payload");
            order_a.lock().expect("order_a lock").push(order);
            bus_b.emit_base(event);
            thread::sleep(Duration::from_millis(2));
            Ok(json!(null))
        }
    });

    let order_b_for_handler = order_b.clone();
    let bus_b_id_for_handler = bus_b_id.clone();
    bus_b.on_raw("serial", "forward_order_b", move |event| {
        let order_b = order_b_for_handler.clone();
        let bus_b_id = bus_b_id_for_handler.clone();
        async move {
            let (order, in_flight_on_bus_b) = {
                let inner = event.inner.lock();
                let order = inner
                    .payload
                    .get("order")
                    .and_then(serde_json::Value::as_i64)
                    .expect("order payload");
                let in_flight_on_bus_b = inner
                    .event_results
                    .values()
                    .filter(|result| result.handler.eventbus_id == bus_b_id)
                    .filter(|result| {
                        result.status == EventResultStatus::Pending
                            || result.status == EventResultStatus::Started
                    })
                    .count();
                (order, in_flight_on_bus_b)
            };
            assert!(in_flight_on_bus_b <= 1);
            order_b.lock().expect("order_b lock").push(order);
            thread::sleep(Duration::from_millis(1));
            Ok(json!(null))
        }
    });

    for order in 0..5 {
        bus_a.emit(SerialEvent {
            order,
            source: "a".to_string(),
            ..Default::default()
        });
    }

    block_on(async {
        assert!(bus_a.wait_until_idle(Some(2.0)).await);
        assert!(bus_b.wait_until_idle(Some(2.0)).await);
    });

    let events_by_id = bus_b.runtime_payload_for_test();
    let history_orders: Vec<i64> = bus_b
        .event_history_ids()
        .iter()
        .map(|id| {
            events_by_id
                .get(id)
                .expect("history event")
                .inner
                .lock()
                .payload
                .get("order")
                .and_then(serde_json::Value::as_i64)
                .expect("order payload")
        })
        .collect();
    let results_sizes: Vec<usize> = bus_b
        .event_history_ids()
        .iter()
        .map(|id| {
            events_by_id
                .get(id)
                .expect("history event")
                .inner
                .lock()
                .event_results
                .len()
        })
        .collect();
    let bus_b_result_counts: Vec<usize> = bus_b
        .event_history_ids()
        .iter()
        .map(|id| {
            events_by_id
                .get(id)
                .expect("history event")
                .inner
                .lock()
                .event_results
                .values()
                .filter(|result| result.handler.eventbus_id == bus_b_id)
                .count()
        })
        .collect();
    let processed_flags: Vec<bool> = bus_b
        .event_history_ids()
        .iter()
        .map(|id| {
            events_by_id
                .get(id)
                .expect("history event")
                .inner
                .lock()
                .event_results
                .values()
                .filter(|result| result.handler.eventbus_id == bus_b_id)
                .all(|result| {
                    result.status == EventResultStatus::Completed
                        || result.status == EventResultStatus::Error
                })
        })
        .collect();
    let pending_counts: Vec<usize> = bus_b
        .event_history_ids()
        .iter()
        .map(|id| {
            events_by_id
                .get(id)
                .expect("history event")
                .inner
                .lock()
                .event_results
                .values()
                .filter(|result| result.status == EventResultStatus::Pending)
                .count()
        })
        .collect();

    assert_eq!(
        order_a.lock().expect("order_a lock").as_slice(),
        &[0, 1, 2, 3, 4]
    );
    assert_eq!(
        order_b.lock().expect("order_b lock").as_slice(),
        &[0, 1, 2, 3, 4]
    );
    assert_eq!(history_orders, vec![0, 1, 2, 3, 4]);
    assert_eq!(results_sizes, vec![2, 2, 2, 2, 2]);
    assert_eq!(bus_b_result_counts, vec![1, 1, 1, 1, 1]);
    assert_eq!(processed_flags, vec![true, true, true, true, true]);
    assert_eq!(pending_counts, vec![0, 0, 0, 0, 0]);
    bus_a.destroy();
    bus_b.destroy();
}

#[test]
fn test_fifo_forwarded_events_preserve_order_across_chained_buses_bus_serial() {
    let bus_a = EventBus::new_with_options(
        Some("ForwardChainA".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );
    let bus_b = EventBus::new_with_options(
        Some("ForwardChainB".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );
    let bus_c = EventBus::new_with_options(
        Some("ForwardChainC".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );
    let order_c = Arc::new(Mutex::new(Vec::new()));

    bus_b.on_raw("serial", "forward_chain_b_handler", |_event| async move {
        thread::sleep(Duration::from_millis(2));
        Ok(json!(null))
    });

    let order_c_for_handler = order_c.clone();
    bus_c.on_raw("serial", "forward_chain_c_handler", move |event| {
        let order_c = order_c_for_handler.clone();
        async move {
            let order = event
                .inner
                .lock()
                .payload
                .get("order")
                .and_then(serde_json::Value::as_i64)
                .expect("order payload");
            order_c.lock().expect("order_c lock").push(order);
            thread::sleep(Duration::from_millis(1));
            Ok(json!(null))
        }
    });

    let bus_b_for_forward = bus_b.clone();
    bus_a.on_raw("*", "forward_chain_a_to_b", move |event| {
        let bus_b = bus_b_for_forward.clone();
        async move {
            bus_b.emit_base(event);
            Ok(json!(null))
        }
    });
    let bus_c_for_forward = bus_c.clone();
    bus_b.on_raw("*", "forward_chain_b_to_c", move |event| {
        let bus_c = bus_c_for_forward.clone();
        async move {
            bus_c.emit_base(event);
            Ok(json!(null))
        }
    });

    for order in 0..6 {
        bus_a.emit(SerialEvent {
            order,
            source: "a".to_string(),
            ..Default::default()
        });
    }

    block_on(async {
        assert!(bus_a.wait_until_idle(Some(2.0)).await);
        assert!(bus_b.wait_until_idle(Some(2.0)).await);
        assert!(bus_c.wait_until_idle(Some(2.0)).await);
    });

    assert_eq!(
        order_c.lock().expect("order_c lock").as_slice(),
        &[0, 1, 2, 3, 4, 5]
    );
    bus_a.destroy();
    bus_b.destroy();
    bus_c.destroy();
}

#[derive(Debug, Clone)]
enum RetryTestError {
    Network,
    Validation,
    AlwaysFails,
    Retry(abxbus::retry::RetryError),
}

impl From<abxbus::retry::RetryError> for RetryTestError {
    fn from(error: abxbus::retry::RetryError) -> Self {
        Self::Retry(error)
    }
}

fn retry_network_errors(error: &RetryTestError) -> bool {
    matches!(error, RetryTestError::Network)
}

// Folded from retry tests to keep test layout class-based.

// ─── Basic retry behavior ────────────────────────────────────────────────────

#[test]
fn test_retry_function_succeeds_on_first_attempt_with_no_retries_needed() {
    struct Service;
    impl Service {
        abxbus::retry! {
            max_attempts = 3;
            async fn run(&self) -> Result<&'static str, RetryTestError> {
                        Ok("ok")
                    }
        }
    }

    assert_eq!(block_on(Service.run()).unwrap(), "ok");
}

#[test]
fn test_retry_standalone_function_without_event_bus() {
    abxbus::retry! {
        max_attempts = 2;
        async fn standalone() -> Result<&'static str, RetryTestError> {
                    Ok("ok")
                }
    }

    assert_eq!(block_on(standalone()).unwrap(), "ok");
}

#[test]
fn test_retry_function_retries_on_failure_and_eventually_succeeds() {
    struct Service {
        calls: Arc<Mutex<i32>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 3;
            async fn run(&self) -> Result<&'static str, RetryTestError> {
                        let mut calls = self.calls.lock().expect("calls lock");
                        *calls += 1;
                        if *calls < 3 {
                            return Err(RetryTestError::Network);
                        }
                        Ok("ok")
                    }
        }
    }

    let calls = Arc::new(Mutex::new(0));
    let service = Service {
        calls: calls.clone(),
    };
    assert_eq!(block_on(service.run()).unwrap(), "ok");
    assert_eq!(*calls.lock().expect("calls lock"), 3);
}

#[test]
fn test_retry_throws_after_exhausting_all_attempts() {
    struct Service {
        calls: Arc<Mutex<i32>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 3;
            async fn run(&self) -> Result<(), RetryTestError> {
                        *self.calls.lock().expect("calls lock") += 1;
                        Err(RetryTestError::AlwaysFails)
                    }
        }
    }

    let calls = Arc::new(Mutex::new(0));
    let service = Service {
        calls: calls.clone(),
    };
    assert!(matches!(
        block_on(service.run()),
        Err(RetryTestError::AlwaysFails)
    ));
    assert_eq!(*calls.lock().expect("calls lock"), 3);
}

#[test]
fn test_retry_max_attempts_one_means_no_retries_single_attempt() {
    struct Service {
        calls: Arc<Mutex<i32>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 1;
            async fn run(&self) -> Result<(), RetryTestError> {
                        *self.calls.lock().expect("calls lock") += 1;
                        Err(RetryTestError::AlwaysFails)
                    }
        }
    }

    let calls = Arc::new(Mutex::new(0));
    let service = Service {
        calls: calls.clone(),
    };
    assert!(block_on(service.run()).is_err());
    assert_eq!(*calls.lock().expect("calls lock"), 1);
}

#[test]
fn test_retry_default_max_attempts_one_means_single_attempt() {
    struct Service {
        calls: Arc<Mutex<i32>>,
    }
    impl Service {
        abxbus::retry! {
            ;
            async fn run(&self) -> Result<(), RetryTestError> {
                        *self.calls.lock().expect("calls lock") += 1;
                        Err(RetryTestError::AlwaysFails)
                    }
        }
    }

    let calls = Arc::new(Mutex::new(0));
    let service = Service {
        calls: calls.clone(),
    };
    assert!(block_on(service.run()).is_err());
    assert_eq!(*calls.lock().expect("calls lock"), 1);
}

// ─── Retry delays ────────────────────────────────────────────────────────────

#[test]
fn test_retry_retry_after_introduces_delay_between_attempts() {
    struct Service {
        calls: Arc<Mutex<i32>>,
        timestamps: Arc<Mutex<Vec<Instant>>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 3, retry_after = 0.05;
            async fn run(&self) -> Result<&'static str, RetryTestError> {
                        self.timestamps
                            .lock()
                            .expect("timestamps lock")
                            .push(Instant::now());
                        let mut calls = self.calls.lock().expect("calls lock");
                        *calls += 1;
                        if *calls < 3 {
                            return Err(RetryTestError::Network);
                        }
                        Ok("ok")
                    }
        }
    }

    let service = Service {
        calls: Arc::new(Mutex::new(0)),
        timestamps: Arc::new(Mutex::new(Vec::new())),
    };
    assert_eq!(block_on(service.run()).unwrap(), "ok");
    let timestamps = service.timestamps.lock().expect("timestamps lock");
    assert!(timestamps[1].duration_since(timestamps[0]) >= Duration::from_millis(40));
    assert!(timestamps[2].duration_since(timestamps[1]) >= Duration::from_millis(40));
}

#[test]
fn test_retry_retry_backoff_factor_increases_delay_between_attempts() {
    struct Service {
        calls: Arc<Mutex<i32>>,
        timestamps: Arc<Mutex<Vec<Instant>>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 4, retry_after = 0.03, retry_backoff_factor = 2.0;
            async fn run(&self) -> Result<&'static str, RetryTestError> {
                        self.timestamps
                            .lock()
                            .expect("timestamps lock")
                            .push(Instant::now());
                        let mut calls = self.calls.lock().expect("calls lock");
                        *calls += 1;
                        if *calls < 4 {
                            return Err(RetryTestError::Network);
                        }
                        Ok("ok")
                    }
        }
    }

    let service = Service {
        calls: Arc::new(Mutex::new(0)),
        timestamps: Arc::new(Mutex::new(Vec::new())),
    };
    assert_eq!(block_on(service.run()).unwrap(), "ok");
    let timestamps = service.timestamps.lock().expect("timestamps lock");
    let gap1 = timestamps[1].duration_since(timestamps[0]);
    let gap2 = timestamps[2].duration_since(timestamps[1]);
    let gap3 = timestamps[3].duration_since(timestamps[2]);
    assert!(gap1 >= Duration::from_millis(20));
    assert!(gap2 >= Duration::from_millis(45));
    assert!(gap3 >= Duration::from_millis(90));
    assert!(gap2 > gap1);
    assert!(gap3 > gap2);
}

// ─── Retry error filtering ───────────────────────────────────────────────────

#[test]
fn test_retry_retry_on_errors_retries_only_matching_error_types() {
    struct Service {
        calls: Arc<Mutex<i32>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 3, retry_if = retry_network_errors;
            async fn run(&self) -> Result<&'static str, RetryTestError> {
                        let mut calls = self.calls.lock().expect("calls lock");
                        *calls += 1;
                        if *calls < 3 {
                            return Err(RetryTestError::Network);
                        }
                        Ok("ok")
                    }
        }
    }

    let calls = Arc::new(Mutex::new(0));
    let service = Service {
        calls: calls.clone(),
    };
    assert_eq!(block_on(service.run()).unwrap(), "ok");
    assert_eq!(*calls.lock().expect("calls lock"), 3);
}

#[test]
fn test_retry_retry_on_errors_does_not_retry_non_matching_errors() {
    struct Service {
        calls: Arc<Mutex<i32>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 3, retry_if = retry_network_errors;
            async fn run(&self) -> Result<(), RetryTestError> {
                        *self.calls.lock().expect("calls lock") += 1;
                        Err(RetryTestError::Validation)
                    }
        }
    }

    let calls = Arc::new(Mutex::new(0));
    let service = Service {
        calls: calls.clone(),
    };
    assert!(matches!(
        block_on(service.run()),
        Err(RetryTestError::Validation)
    ));
    assert_eq!(*calls.lock().expect("calls lock"), 1);
}

// ─── Timeout behavior ────────────────────────────────────────────────────────

#[test]
fn test_retry_timeout_triggers_retry_timeout_error_on_slow_attempts() {
    struct Service;
    impl Service {
        abxbus::retry! {
            max_attempts = 1, timeout = 0.02;
            async fn run(&self) -> Result<&'static str, RetryTestError> {
                        futures_timer::Delay::new(Duration::from_millis(50)).await;
                        Ok("slow")
                    }
        }
    }

    assert!(matches!(
        block_on(Service.run()),
        Err(RetryTestError::Retry(
            abxbus::retry::RetryError::Timeout { .. }
        ))
    ));
}

#[test]
fn test_retry_timeout_allows_fast_attempts_to_succeed() {
    struct Service;
    impl Service {
        abxbus::retry! {
            max_attempts = 1, timeout = 0.05;
            async fn run(&self) -> Result<&'static str, RetryTestError> {
                        Ok("ok")
                    }
        }
    }

    assert_eq!(block_on(Service.run()).unwrap(), "ok");
}

#[test]
fn test_retry_timed_out_attempts_are_retried_when_max_attempts_gt_one() {
    struct Service {
        calls: Arc<Mutex<i32>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 3, timeout = 0.02;
            async fn run(&self) -> Result<&'static str, RetryTestError> {
                        let call = {
                            let mut calls = self.calls.lock().expect("calls lock");
                            *calls += 1;
                            *calls
                        };
                        if call < 3 {
                            futures_timer::Delay::new(Duration::from_millis(50)).await;
                            return Ok("slow");
                        }
                        Ok("ok")
                    }
        }
    }

    let calls = Arc::new(Mutex::new(0));
    let service = Service {
        calls: calls.clone(),
    };
    assert_eq!(block_on(service.run()).unwrap(), "ok");
    assert_eq!(*calls.lock().expect("calls lock"), 3);
}

// ─── Semaphore behavior ──────────────────────────────────────────────────────

#[test]
fn test_retry_semaphore_limit_controls_max_concurrent_executions() {
    struct Service {
        active: Arc<Mutex<i64>>,
        max_active: Arc<Mutex<i64>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 1, semaphore_limit = 2, semaphore_name = "rust_retry_async_limit";
            async fn run(&self) -> Result<(), RetryTestError> {
                        bump_in_flight(&self.active, &self.max_active);
                        futures_timer::Delay::new(Duration::from_millis(50)).await;
                        drop_in_flight(&self.active);
                        Ok(())
                    }
        }
    }

    let service = Arc::new(Service {
        active: Arc::new(Mutex::new(0)),
        max_active: Arc::new(Mutex::new(0)),
    });
    block_on(futures::future::join_all((0..6).map(|_| {
        let service = service.clone();
        async move { service.run().await }
    })));
    assert_eq!(*service.max_active.lock().expect("max lock"), 2);
}

#[test]
fn test_retry_semaphore_lax_false_throws_semaphore_timeout_error_when_slots_are_full() {
    struct Holder;
    impl Holder {
        abxbus::retry! {
            max_attempts = 1,
            semaphore_limit = 1,
            semaphore_name = "rust_retry_async_lax_false",
            semaphore_lax = false,
            semaphore_timeout = 0.05;
            async fn hold(&self) -> Result<&'static str, RetryTestError> {
                        futures_timer::Delay::new(Duration::from_millis(200)).await;
                        Ok("held")
                    }
        }
    }
    struct Contender;
    impl Contender {
        abxbus::retry! {
            max_attempts = 1,
            semaphore_limit = 1,
            semaphore_name = "rust_retry_async_lax_false",
            semaphore_lax = false,
            semaphore_timeout = 0.05;
            async fn run(&self) -> Result<&'static str, RetryTestError> {
                        Ok("contender")
                    }
        }
    }

    block_on(async {
        let holder = Holder;
        let (held, contended) = futures::future::join(holder.hold(), async {
            futures_timer::Delay::new(Duration::from_millis(10)).await;
            Contender.run().await
        })
        .await;
        assert_eq!(held.unwrap(), "held");
        assert!(matches!(
            contended,
            Err(RetryTestError::Retry(
                abxbus::retry::RetryError::SemaphoreTimeout { .. }
            ))
        ));
    });
}

#[test]
fn test_retry_semaphore_lax_true_default_proceeds_without_semaphore_on_timeout() {
    struct Holder;
    impl Holder {
        abxbus::retry! {
            max_attempts = 1,
            semaphore_limit = 1,
            semaphore_name = "rust_retry_async_lax_true",
            semaphore_lax = false,
            semaphore_timeout = 0.05;
            async fn hold(&self) -> Result<&'static str, RetryTestError> {
                        futures_timer::Delay::new(Duration::from_millis(200)).await;
                        Ok("held")
                    }
        }
    }
    struct Contender;
    impl Contender {
        abxbus::retry! {
            max_attempts = 1,
            semaphore_limit = 1,
            semaphore_name = "rust_retry_async_lax_true",
            semaphore_lax = true,
            semaphore_timeout = 0.05;
            async fn run(&self) -> Result<&'static str, RetryTestError> {
                        Ok("contender")
                    }
        }
    }

    block_on(async {
        let holder = Holder;
        let (held, contended) = futures::future::join(holder.hold(), async {
            futures_timer::Delay::new(Duration::from_millis(10)).await;
            Contender.run().await
        })
        .await;
        assert_eq!(held.unwrap(), "held");
        assert_eq!(contended.unwrap(), "contender");
    });
}

#[test]
fn test_retry_passes_arguments_through_to_wrapped_function() {
    abxbus::retry! {
        max_attempts = 1;
        async fn join_args(a: i32, b: &str) -> Result<String, RetryTestError> {
                Ok(format!("{a}-{b}"))
            }
    }

    assert_eq!(block_on(join_args(1, "hello")).unwrap(), "1-hello");
}

#[test]
fn test_retry_semaphore_is_held_across_all_retry_attempts() {
    struct Service {
        active: Arc<Mutex<i64>>,
        max_active: Arc<Mutex<i64>>,
        total_calls: Arc<Mutex<i32>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 3, semaphore_limit = 1, semaphore_name = "rust_retry_async_across_retries";
            async fn run(&self) -> Result<&'static str, RetryTestError> {
                        bump_in_flight(&self.active, &self.max_active);
                        let call = {
                            let mut total = self.total_calls.lock().expect("total calls lock");
                            *total += 1;
                            *total
                        };
                        futures_timer::Delay::new(Duration::from_millis(10)).await;
                        drop_in_flight(&self.active);
                        if call % 2 == 1 {
                            return Err(RetryTestError::Network);
                        }
                        Ok("ok")
                    }
        }
    }

    let service = Arc::new(Service {
        active: Arc::new(Mutex::new(0)),
        max_active: Arc::new(Mutex::new(0)),
        total_calls: Arc::new(Mutex::new(0)),
    });
    let results = block_on(futures::future::join_all((0..3).map(|_| {
        let service = service.clone();
        async move { service.run().await.unwrap() }
    })));
    assert_eq!(results, vec!["ok", "ok", "ok"]);
    assert_eq!(*service.max_active.lock().expect("max lock"), 1);
    assert_eq!(*service.total_calls.lock().expect("total lock"), 6);
}

#[test]
fn test_retry_semaphore_released_even_when_all_attempts_fail() {
    struct Service;
    impl Service {
        abxbus::retry! {
            max_attempts = 2, semaphore_limit = 1, semaphore_name = "rust_retry_async_release_on_fail";
            async fn run(&self) -> Result<(), RetryTestError> {
                        Err(RetryTestError::AlwaysFails)
                    }
        }
    }

    assert!(block_on(Service.run()).is_err());
    assert!(block_on(Service.run()).is_err());
}

// ─── Re-entrancy behavior ────────────────────────────────────────────────────

#[test]
fn test_retry_reentrant_call_on_same_semaphore_does_not_deadlock() {
    struct Service;
    impl Service {
        abxbus::retry! {
            max_attempts = 1, semaphore_limit = 1, semaphore_name = "rust_retry_async_reentrant";
            async fn inner(&self) -> Result<&'static str, RetryTestError> {
                        Ok("inner ok")
                    }
        }

        abxbus::retry! {
            max_attempts = 1, semaphore_limit = 1, semaphore_name = "rust_retry_async_reentrant";
            async fn outer(&self) -> Result<String, RetryTestError> {
                        Ok(format!("outer got: {}", self.inner().await?))
                    }
        }
    }

    assert_eq!(block_on(Service.outer()).unwrap(), "outer got: inner ok");
}

#[test]
fn test_retry_recursive_function_with_semaphore_does_not_deadlock() {
    struct Service {
        depth: Arc<Mutex<i32>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 1, semaphore_limit = 1, semaphore_name = "rust_retry_async_recursive";
            async fn recurse(&self, n: i32) -> Result<i32, RetryTestError> {
                        *self.depth.lock().expect("depth lock") += 1;
                        if n <= 1 {
                            return Ok(1);
                        }
                        Ok(n + Box::pin(self.recurse(n - 1)).await?)
                    }
        }
    }

    let depth = Arc::new(Mutex::new(0));
    let service = Service {
        depth: depth.clone(),
    };
    assert_eq!(block_on(service.recurse(5)).unwrap(), 15);
    assert_eq!(*depth.lock().expect("depth lock"), 5);
}

#[test]
fn test_retry_three_level_nested_reentrancy_does_not_deadlock() {
    struct Service;
    impl Service {
        abxbus::retry! {
            max_attempts = 1, semaphore_limit = 1, semaphore_name = "rust_retry_async_nested";
            async fn level3(&self) -> Result<&'static str, RetryTestError> {
                        Ok("level3")
                    }
        }

        abxbus::retry! {
            max_attempts = 1, semaphore_limit = 1, semaphore_name = "rust_retry_async_nested";
            async fn level2(&self) -> Result<String, RetryTestError> {
                        Ok(format!("level2>{}", self.level3().await?))
                    }
        }

        abxbus::retry! {
            max_attempts = 1, semaphore_limit = 1, semaphore_name = "rust_retry_async_nested";
            async fn level1(&self) -> Result<String, RetryTestError> {
                        Ok(format!("level1>{}", self.level2().await?))
                    }
        }
    }

    assert_eq!(block_on(Service.level1()).unwrap(), "level1>level2>level3");
}

// ─── Semaphore scopes ────────────────────────────────────────────────────────

#[test]
fn test_retry_semaphore_scope_class_shares_semaphore_across_instances_of_same_class() {
    struct Worker {
        active: Arc<Mutex<i64>>,
        max_active: Arc<Mutex<i64>>,
    }
    impl Worker {
        abxbus::retry! {
            max_attempts = 1,
            semaphore_limit = 1,
            semaphore_scope = "class",
            semaphore_name = "rust_retry_async_scope_class";
            async fn run(&self) -> Result<(), RetryTestError> {
                        bump_in_flight(&self.active, &self.max_active);
                        futures_timer::Delay::new(Duration::from_millis(50)).await;
                        drop_in_flight(&self.active);
                        Ok(())
                    }
        }
    }

    let active = Arc::new(Mutex::new(0));
    let max_active = Arc::new(Mutex::new(0));
    let a = Worker {
        active: active.clone(),
        max_active: max_active.clone(),
    };
    let b = Worker { active, max_active };
    let (a_result, b_result) = block_on(futures::future::join(a.run(), b.run()));
    a_result.unwrap();
    b_result.unwrap();
    assert_eq!(*a.max_active.lock().expect("max lock"), 1);
}

#[test]
fn test_retry_semaphore_scope_instance_gives_each_instance_its_own_semaphore() {
    struct Worker {
        active: Arc<Mutex<i64>>,
        max_active: Arc<Mutex<i64>>,
    }
    impl Worker {
        abxbus::retry! {
            max_attempts = 1,
            semaphore_limit = 1,
            semaphore_scope = "instance",
            semaphore_name = "rust_retry_async_scope_instance";
            async fn run(&self) -> Result<(), RetryTestError> {
                        bump_in_flight(&self.active, &self.max_active);
                        futures_timer::Delay::new(Duration::from_millis(50)).await;
                        drop_in_flight(&self.active);
                        Ok(())
                    }
        }
    }

    let active = Arc::new(Mutex::new(0));
    let max_active = Arc::new(Mutex::new(0));
    let a = Worker {
        active: active.clone(),
        max_active: max_active.clone(),
    };
    let b = Worker { active, max_active };
    let (a_result, b_result) = block_on(futures::future::join(a.run(), b.run()));
    a_result.unwrap();
    b_result.unwrap();
    assert_eq!(*a.max_active.lock().expect("max lock"), 2);
}

#[test]
fn test_retry_semaphore_scope_instance_serializes_calls_on_same_instance() {
    struct Worker {
        active: Arc<Mutex<i64>>,
        max_active: Arc<Mutex<i64>>,
    }
    impl Worker {
        abxbus::retry! {
            max_attempts = 1,
            semaphore_limit = 1,
            semaphore_scope = "instance",
            semaphore_name = "rust_retry_async_scope_instance_same";
            async fn run(&self) -> Result<(), RetryTestError> {
                        bump_in_flight(&self.active, &self.max_active);
                        futures_timer::Delay::new(Duration::from_millis(50)).await;
                        drop_in_flight(&self.active);
                        Ok(())
                    }
        }
    }

    let worker = Worker {
        active: Arc::new(Mutex::new(0)),
        max_active: Arc::new(Mutex::new(0)),
    };
    let (first, second, third) = block_on(futures::future::join3(
        worker.run(),
        worker.run(),
        worker.run(),
    ));
    first.unwrap();
    second.unwrap();
    third.unwrap();
    assert_eq!(*worker.max_active.lock().expect("max lock"), 1);
}

#[test]
fn test_retry_semaphore_name_function_uses_call_args_for_keying() {
    struct Service {
        active: Arc<Mutex<i64>>,
        max_active: Arc<Mutex<i64>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 1,
            semaphore_limit = 1,
            semaphore_name = format!("{a}-{b}");
            async fn keyed(&self, a: &str, b: &str) -> Result<(), RetryTestError> {
                        let _ = (a, b);
                        bump_in_flight(&self.active, &self.max_active);
                        futures_timer::Delay::new(Duration::from_millis(50)).await;
                        drop_in_flight(&self.active);
                        Ok(())
                    }
        }
    }

    let service = Service {
        active: Arc::new(Mutex::new(0)),
        max_active: Arc::new(Mutex::new(0)),
    };
    let (same_a, same_b) = block_on(futures::future::join(
        service.keyed("same", "key"),
        service.keyed("same", "key"),
    ));
    same_a.unwrap();
    same_b.unwrap();
    assert_eq!(*service.max_active.lock().expect("max lock"), 1);

    *service.max_active.lock().expect("max lock") = 0;
    let (diff_a, diff_b) = block_on(futures::future::join(
        service.keyed("a", "1"),
        service.keyed("b", "2"),
    ));
    diff_a.unwrap();
    diff_b.unwrap();
    assert!(*service.max_active.lock().expect("max lock") >= 2);
}

// ─── Sync retry behavior ─────────────────────────────────────────────────────

#[test]
fn test_retry_sync_function_succeeds_on_first_attempt_with_no_retries_needed() {
    struct Service;
    impl Service {
        abxbus::retry! {
            max_attempts = 3;
            fn run(&self) -> Result<&'static str, RetryTestError> {
                        Ok("ok")
                    }
        }
    }

    assert_eq!(Service.run().unwrap(), "ok");
}

#[test]
fn test_retry_sync_standalone_function_without_event_bus() {
    abxbus::retry! {
        max_attempts = 2;
        fn standalone() -> Result<&'static str, RetryTestError> {
                    Ok("ok")
                }
    }

    assert_eq!(standalone().unwrap(), "ok");
}

#[test]
fn test_retry_sync_function_retries_on_failure_and_eventually_succeeds() {
    struct Service {
        calls: Arc<Mutex<i32>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 3;
            fn run(&self) -> Result<&'static str, RetryTestError> {
                        let mut calls = self.calls.lock().expect("calls lock");
                        *calls += 1;
                        if *calls < 3 {
                            return Err(RetryTestError::Network);
                        }
                        Ok("ok")
                    }
        }
    }

    let calls = Arc::new(Mutex::new(0));
    assert_eq!(
        Service {
            calls: calls.clone()
        }
        .run()
        .unwrap(),
        "ok"
    );
    assert_eq!(*calls.lock().expect("calls lock"), 3);
}

#[test]
fn test_retry_sync_throws_after_exhausting_all_attempts() {
    struct Service {
        calls: Arc<Mutex<i32>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 3;
            fn run(&self) -> Result<(), RetryTestError> {
                        *self.calls.lock().expect("calls lock") += 1;
                        Err(RetryTestError::AlwaysFails)
                    }
        }
    }

    let calls = Arc::new(Mutex::new(0));
    assert!(Service {
        calls: calls.clone()
    }
    .run()
    .is_err());
    assert_eq!(*calls.lock().expect("calls lock"), 3);
}

#[test]
fn test_retry_sync_max_attempts_one_means_no_retries_single_attempt() {
    struct Service {
        calls: Arc<Mutex<i32>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 1;
            fn run(&self) -> Result<(), RetryTestError> {
                        *self.calls.lock().expect("calls lock") += 1;
                        Err(RetryTestError::AlwaysFails)
                    }
        }
    }

    let calls = Arc::new(Mutex::new(0));
    assert!(Service {
        calls: calls.clone()
    }
    .run()
    .is_err());
    assert_eq!(*calls.lock().expect("calls lock"), 1);
}

#[test]
fn test_retry_sync_default_max_attempts_one_means_single_attempt() {
    struct Service {
        calls: Arc<Mutex<i32>>,
    }
    impl Service {
        abxbus::retry! {
            ;
            fn run(&self) -> Result<(), RetryTestError> {
                        *self.calls.lock().expect("calls lock") += 1;
                        Err(RetryTestError::AlwaysFails)
                    }
        }
    }

    let calls = Arc::new(Mutex::new(0));
    assert!(Service {
        calls: calls.clone()
    }
    .run()
    .is_err());
    assert_eq!(*calls.lock().expect("calls lock"), 1);
}

#[test]
fn test_retry_sync_retry_after_introduces_blocking_delay_between_attempts() {
    struct Service {
        calls: Arc<Mutex<i32>>,
        timestamps: Arc<Mutex<Vec<Instant>>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 3, retry_after = 0.05;
            fn run(&self) -> Result<&'static str, RetryTestError> {
                        self.timestamps
                            .lock()
                            .expect("timestamps lock")
                            .push(Instant::now());
                        let mut calls = self.calls.lock().expect("calls lock");
                        *calls += 1;
                        if *calls < 3 {
                            return Err(RetryTestError::Network);
                        }
                        Ok("ok")
                    }
        }
    }

    let service = Service {
        calls: Arc::new(Mutex::new(0)),
        timestamps: Arc::new(Mutex::new(Vec::new())),
    };
    assert_eq!(service.run().unwrap(), "ok");
    let timestamps = service.timestamps.lock().expect("timestamps lock");
    assert!(timestamps[1].duration_since(timestamps[0]) >= Duration::from_millis(40));
    assert!(timestamps[2].duration_since(timestamps[1]) >= Duration::from_millis(40));
}

#[test]
fn test_retry_sync_retry_backoff_factor_increases_blocking_delay_between_attempts() {
    struct Service {
        calls: Arc<Mutex<i32>>,
        timestamps: Arc<Mutex<Vec<Instant>>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 4, retry_after = 0.03, retry_backoff_factor = 2.0;
            fn run(&self) -> Result<&'static str, RetryTestError> {
                        self.timestamps
                            .lock()
                            .expect("timestamps lock")
                            .push(Instant::now());
                        let mut calls = self.calls.lock().expect("calls lock");
                        *calls += 1;
                        if *calls < 4 {
                            return Err(RetryTestError::Network);
                        }
                        Ok("ok")
                    }
        }
    }

    let service = Service {
        calls: Arc::new(Mutex::new(0)),
        timestamps: Arc::new(Mutex::new(Vec::new())),
    };
    assert_eq!(service.run().unwrap(), "ok");
    let timestamps = service.timestamps.lock().expect("timestamps lock");
    let gap1 = timestamps[1].duration_since(timestamps[0]);
    let gap2 = timestamps[2].duration_since(timestamps[1]);
    let gap3 = timestamps[3].duration_since(timestamps[2]);
    assert!(gap1 >= Duration::from_millis(20));
    assert!(gap2 >= Duration::from_millis(45));
    assert!(gap3 >= Duration::from_millis(90));
}

#[test]
fn test_retry_sync_retry_on_errors_retries_only_matching_error_types() {
    struct Service {
        calls: Arc<Mutex<i32>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 3, retry_if = retry_network_errors;
            fn run(&self) -> Result<&'static str, RetryTestError> {
                        let mut calls = self.calls.lock().expect("calls lock");
                        *calls += 1;
                        if *calls < 3 {
                            return Err(RetryTestError::Network);
                        }
                        Ok("ok")
                    }
        }
    }

    let calls = Arc::new(Mutex::new(0));
    assert_eq!(
        Service {
            calls: calls.clone()
        }
        .run()
        .unwrap(),
        "ok"
    );
    assert_eq!(*calls.lock().expect("calls lock"), 3);
}

#[test]
fn test_retry_sync_retry_on_errors_does_not_retry_non_matching_errors() {
    struct Service {
        calls: Arc<Mutex<i32>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 3, retry_if = retry_network_errors;
            fn run(&self) -> Result<(), RetryTestError> {
                        *self.calls.lock().expect("calls lock") += 1;
                        Err(RetryTestError::Validation)
                    }
        }
    }

    let calls = Arc::new(Mutex::new(0));
    assert!(matches!(
        Service {
            calls: calls.clone()
        }
        .run(),
        Err(RetryTestError::Validation)
    ));
    assert_eq!(*calls.lock().expect("calls lock"), 1);
}

#[test]
fn test_retry_sync_timeout_triggers_retry_timeout_error_on_slow_attempts() {
    struct Service;
    impl Service {
        abxbus::retry! {
            max_attempts = 1, timeout = 0.02;
            fn run(&self) -> Result<&'static str, RetryTestError> {
                        thread::sleep(Duration::from_millis(50));
                        Ok("slow")
                    }
        }
    }

    assert!(matches!(
        Service.run(),
        Err(RetryTestError::Retry(
            abxbus::retry::RetryError::Timeout { .. }
        ))
    ));
}

#[test]
fn test_retry_sync_timeout_allows_fast_attempts_to_succeed() {
    struct Service;
    impl Service {
        abxbus::retry! {
            max_attempts = 1, timeout = 0.05;
            fn run(&self) -> Result<&'static str, RetryTestError> {
                        Ok("ok")
                    }
        }
    }

    assert_eq!(Service.run().unwrap(), "ok");
}

#[test]
fn test_retry_sync_timed_out_attempts_are_retried_when_max_attempts_gt_one() {
    struct Service {
        calls: Arc<Mutex<i32>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 3, timeout = 0.02;
            fn run(&self) -> Result<&'static str, RetryTestError> {
                        let call = {
                            let mut calls = self.calls.lock().expect("calls lock");
                            *calls += 1;
                            *calls
                        };
                        if call < 3 {
                            thread::sleep(Duration::from_millis(50));
                            return Ok("slow");
                        }
                        Ok("ok")
                    }
        }
    }

    let calls = Arc::new(Mutex::new(0));
    assert_eq!(
        Service {
            calls: calls.clone()
        }
        .run()
        .unwrap(),
        "ok"
    );
    assert_eq!(*calls.lock().expect("calls lock"), 3);
}

#[test]
fn test_retry_sync_semaphore_limit_controls_max_concurrent_executions() {
    struct Service {
        active: Arc<Mutex<i64>>,
        max_active: Arc<Mutex<i64>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 1, semaphore_limit = 2, semaphore_name = "rust_retry_sync_limit";
            fn run(&self) -> Result<(), RetryTestError> {
                        bump_in_flight(&self.active, &self.max_active);
                        thread::sleep(Duration::from_millis(50));
                        drop_in_flight(&self.active);
                        Ok(())
                    }
        }
    }

    let service = Arc::new(Service {
        active: Arc::new(Mutex::new(0)),
        max_active: Arc::new(Mutex::new(0)),
    });
    let threads: Vec<_> = (0..6)
        .map(|_| {
            let service = service.clone();
            thread::spawn(move || service.run().unwrap())
        })
        .collect();
    for handle in threads {
        handle.join().expect("thread join");
    }
    assert_eq!(*service.max_active.lock().expect("max lock"), 2);
}

#[test]
fn test_retry_sync_semaphore_lax_false_throws_semaphore_timeout_error_when_slots_are_full() {
    struct Service;
    impl Service {
        abxbus::retry! {
            max_attempts = 1,
            semaphore_limit = 1,
            semaphore_name = "rust_retry_sync_lax_false",
            semaphore_lax = false,
            semaphore_timeout = 0.05;
            fn run(&self) -> Result<&'static str, RetryTestError> {
                        thread::sleep(Duration::from_millis(200));
                        Ok("held")
                    }
        }
    }

    let service = Arc::new(Service);
    let holder = {
        let service = service.clone();
        thread::spawn(move || service.run().unwrap())
    };
    thread::sleep(Duration::from_millis(10));
    assert!(matches!(
        service.run(),
        Err(RetryTestError::Retry(
            abxbus::retry::RetryError::SemaphoreTimeout { .. }
        ))
    ));
    holder.join().expect("holder join");
}

#[test]
fn test_retry_sync_semaphore_lax_true_default_proceeds_without_semaphore_on_timeout() {
    struct Service;
    impl Service {
        abxbus::retry! {
            max_attempts = 1,
            semaphore_limit = 1,
            semaphore_name = "rust_retry_sync_lax_true",
            semaphore_lax = true,
            semaphore_timeout = 0.05;
            fn run(&self) -> Result<&'static str, RetryTestError> {
                        thread::sleep(Duration::from_millis(200));
                        Ok("ok")
                    }
        }
    }

    let service = Arc::new(Service);
    let holder = {
        let service = service.clone();
        thread::spawn(move || service.run().unwrap())
    };
    thread::sleep(Duration::from_millis(10));
    assert_eq!(service.run().unwrap(), "ok");
    holder.join().expect("holder join");
}

#[test]
fn test_retry_sync_passes_arguments_through_to_wrapped_function() {
    abxbus::retry! {
        max_attempts = 1;
        fn join_args(a: i32, b: &str) -> Result<String, RetryTestError> {
                Ok(format!("{a}-{b}"))
            }
    }

    assert_eq!(join_args(1, "hello").unwrap(), "1-hello");
}

#[test]
fn test_retry_sync_semaphore_is_held_across_all_retry_attempts() {
    struct Service {
        active: Arc<Mutex<i64>>,
        max_active: Arc<Mutex<i64>>,
        total_calls: Arc<Mutex<i32>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 3, semaphore_limit = 1, semaphore_name = "rust_retry_sync_across_retries";
            fn run(&self) -> Result<&'static str, RetryTestError> {
                        bump_in_flight(&self.active, &self.max_active);
                        let call = {
                            let mut total = self.total_calls.lock().expect("total calls lock");
                            *total += 1;
                            *total
                        };
                        thread::sleep(Duration::from_millis(10));
                        drop_in_flight(&self.active);
                        if call % 2 == 1 {
                            return Err(RetryTestError::Network);
                        }
                        Ok("ok")
                    }
        }
    }

    let service = Arc::new(Service {
        active: Arc::new(Mutex::new(0)),
        max_active: Arc::new(Mutex::new(0)),
        total_calls: Arc::new(Mutex::new(0)),
    });
    let threads: Vec<_> = (0..3)
        .map(|_| {
            let service = service.clone();
            thread::spawn(move || service.run().unwrap())
        })
        .collect();
    let mut results = Vec::new();
    for handle in threads {
        results.push(handle.join().expect("thread join"));
    }
    assert_eq!(results, vec!["ok", "ok", "ok"]);
    assert_eq!(*service.max_active.lock().expect("max lock"), 1);
    assert_eq!(*service.total_calls.lock().expect("total lock"), 6);
}

#[test]
fn test_retry_sync_semaphore_released_even_when_all_attempts_fail() {
    struct Service;
    impl Service {
        abxbus::retry! {
            max_attempts = 2, semaphore_limit = 1, semaphore_name = "rust_retry_sync_release_on_fail";
            fn run(&self) -> Result<(), RetryTestError> {
                        Err(RetryTestError::AlwaysFails)
                    }
        }
    }

    assert!(Service.run().is_err());
    assert!(Service.run().is_err());
}

#[test]
fn test_retry_sync_reentrant_call_on_same_semaphore_does_not_deadlock() {
    struct Service;
    impl Service {
        abxbus::retry! {
            max_attempts = 1, semaphore_limit = 1, semaphore_name = "rust_retry_sync_reentrant";
            fn inner(&self) -> Result<&'static str, RetryTestError> {
                        Ok("inner ok")
                    }
        }

        abxbus::retry! {
            max_attempts = 1, semaphore_limit = 1, semaphore_name = "rust_retry_sync_reentrant";
            fn outer(&self) -> Result<String, RetryTestError> {
                        Ok(format!("outer got: {}", self.inner()?))
                    }
        }
    }

    assert_eq!(Service.outer().unwrap(), "outer got: inner ok");
}

#[test]
fn test_retry_sync_recursive_function_with_semaphore_does_not_deadlock() {
    struct Service {
        depth: Arc<Mutex<i32>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 1, semaphore_limit = 1, semaphore_name = "rust_retry_sync_recursive";
            fn recurse(&self, n: i32) -> Result<i32, RetryTestError> {
                        *self.depth.lock().expect("depth lock") += 1;
                        if n <= 1 {
                            return Ok(1);
                        }
                        Ok(n + self.recurse(n - 1)?)
                    }
        }
    }

    let depth = Arc::new(Mutex::new(0));
    assert_eq!(
        Service {
            depth: depth.clone()
        }
        .recurse(5)
        .unwrap(),
        15
    );
    assert_eq!(*depth.lock().expect("depth lock"), 5);
}

#[test]
fn test_retry_sync_three_level_nested_reentrancy_does_not_deadlock() {
    struct Service;
    impl Service {
        abxbus::retry! {
            max_attempts = 1, semaphore_limit = 1, semaphore_name = "rust_retry_sync_nested";
            fn level3(&self) -> Result<&'static str, RetryTestError> {
                        Ok("level3")
                    }
        }

        abxbus::retry! {
            max_attempts = 1, semaphore_limit = 1, semaphore_name = "rust_retry_sync_nested";
            fn level2(&self) -> Result<String, RetryTestError> {
                        Ok(format!("level2>{}", self.level3()?))
                    }
        }

        abxbus::retry! {
            max_attempts = 1, semaphore_limit = 1, semaphore_name = "rust_retry_sync_nested";
            fn level1(&self) -> Result<String, RetryTestError> {
                        Ok(format!("level1>{}", self.level2()?))
                    }
        }
    }

    assert_eq!(Service.level1().unwrap(), "level1>level2>level3");
}

#[test]
fn test_retry_sync_semaphore_scope_class_shares_semaphore_across_instances_of_same_class() {
    struct Worker {
        active: Arc<Mutex<i64>>,
        max_active: Arc<Mutex<i64>>,
    }
    impl Worker {
        abxbus::retry! {
            max_attempts = 1,
            semaphore_limit = 1,
            semaphore_scope = "class",
            semaphore_name = "rust_retry_sync_scope_class";
            fn run(&self) -> Result<(), RetryTestError> {
                        bump_in_flight(&self.active, &self.max_active);
                        thread::sleep(Duration::from_millis(50));
                        drop_in_flight(&self.active);
                        Ok(())
                    }
        }
    }

    let active = Arc::new(Mutex::new(0));
    let max_active = Arc::new(Mutex::new(0));
    let a = Arc::new(Worker {
        active: active.clone(),
        max_active: max_active.clone(),
    });
    let b = Arc::new(Worker { active, max_active });
    let a_thread = {
        let a = a.clone();
        thread::spawn(move || a.run().unwrap())
    };
    let b_thread = {
        let b = b.clone();
        thread::spawn(move || b.run().unwrap())
    };
    a_thread.join().expect("a thread");
    b_thread.join().expect("b thread");
    assert_eq!(*a.max_active.lock().expect("max lock"), 1);
}

#[test]
fn test_retry_sync_semaphore_scope_instance_gives_each_instance_its_own_semaphore() {
    struct Worker {
        active: Arc<Mutex<i64>>,
        max_active: Arc<Mutex<i64>>,
    }
    impl Worker {
        abxbus::retry! {
            max_attempts = 1,
            semaphore_limit = 1,
            semaphore_scope = "instance",
            semaphore_name = "rust_retry_sync_scope_instance";
            fn run(&self) -> Result<(), RetryTestError> {
                        bump_in_flight(&self.active, &self.max_active);
                        thread::sleep(Duration::from_millis(50));
                        drop_in_flight(&self.active);
                        Ok(())
                    }
        }
    }

    let active = Arc::new(Mutex::new(0));
    let max_active = Arc::new(Mutex::new(0));
    let a = Arc::new(Worker {
        active: active.clone(),
        max_active: max_active.clone(),
    });
    let b = Arc::new(Worker { active, max_active });
    let a_thread = {
        let a = a.clone();
        thread::spawn(move || a.run().unwrap())
    };
    let b_thread = {
        let b = b.clone();
        thread::spawn(move || b.run().unwrap())
    };
    a_thread.join().expect("a thread");
    b_thread.join().expect("b thread");
    assert_eq!(*a.max_active.lock().expect("max lock"), 2);
}

#[test]
fn test_retry_sync_semaphore_scope_instance_serializes_calls_on_same_instance() {
    struct Worker {
        active: Arc<Mutex<i64>>,
        max_active: Arc<Mutex<i64>>,
    }
    impl Worker {
        abxbus::retry! {
            max_attempts = 1,
            semaphore_limit = 1,
            semaphore_scope = "instance",
            semaphore_name = "rust_retry_sync_scope_instance_same";
            fn run(&self) -> Result<(), RetryTestError> {
                        bump_in_flight(&self.active, &self.max_active);
                        thread::sleep(Duration::from_millis(50));
                        drop_in_flight(&self.active);
                        Ok(())
                    }
        }
    }

    let worker = Arc::new(Worker {
        active: Arc::new(Mutex::new(0)),
        max_active: Arc::new(Mutex::new(0)),
    });
    let threads: Vec<_> = (0..3)
        .map(|_| {
            let worker = worker.clone();
            thread::spawn(move || worker.run().unwrap())
        })
        .collect();
    for handle in threads {
        handle.join().expect("thread join");
    }
    assert_eq!(*worker.max_active.lock().expect("max lock"), 1);
}

#[test]
fn test_retry_sync_semaphore_name_function_uses_call_args_for_keying() {
    struct Service {
        active: Arc<Mutex<i64>>,
        max_active: Arc<Mutex<i64>>,
    }
    impl Service {
        abxbus::retry! {
            max_attempts = 1,
            semaphore_limit = 1,
            semaphore_name = format!("{a}-{b}");
            fn keyed(&self, a: &str, b: &str) -> Result<(), RetryTestError> {
                        let _ = (a, b);
                        bump_in_flight(&self.active, &self.max_active);
                        thread::sleep(Duration::from_millis(50));
                        drop_in_flight(&self.active);
                        Ok(())
                    }
        }
    }

    let service = Arc::new(Service {
        active: Arc::new(Mutex::new(0)),
        max_active: Arc::new(Mutex::new(0)),
    });
    let same_a = {
        let service = service.clone();
        thread::spawn(move || service.keyed("same", "key").unwrap())
    };
    let same_b = {
        let service = service.clone();
        thread::spawn(move || service.keyed("same", "key").unwrap())
    };
    same_a.join().expect("same a");
    same_b.join().expect("same b");
    assert_eq!(*service.max_active.lock().expect("max lock"), 1);

    *service.max_active.lock().expect("max lock") = 0;
    let diff_a = {
        let service = service.clone();
        thread::spawn(move || service.keyed("a", "1").unwrap())
    };
    let diff_b = {
        let service = service.clone();
        thread::spawn(move || service.keyed("b", "2").unwrap())
    };
    diff_a.join().expect("diff a");
    diff_b.join().expect("diff b");
    assert!(*service.max_active.lock().expect("max lock") >= 2);
}

fn retry_test_now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn retry_spawn_multiprocess_worker(
    mode: &str,
    semaphore_name: &str,
    result_dir: &str,
    id: &str,
) -> Command {
    let mut command = Command::new(env::current_exe().expect("current test executable"));
    command
        .arg("--exact")
        .arg(match mode {
            "async" => "retry_multiprocess_async_worker_entry",
            "sync" => "retry_multiprocess_sync_worker_entry",
            _ => unreachable!("unsupported retry worker mode"),
        })
        .arg("--nocapture")
        .env("ABXBUS_RETRY_MP_WORKER", mode)
        .env("ABXBUS_RETRY_MP_SEMAPHORE", semaphore_name)
        .env("ABXBUS_RETRY_MP_RESULT_DIR", result_dir)
        .env("ABXBUS_RETRY_MP_ID", id);
    command
}

fn retry_read_worker_timestamp(result_dir: &str, id: &str, phase: &str) -> u128 {
    fs::read_to_string(format!("{result_dir}/{id}.{phase}"))
        .expect("worker timestamp")
        .trim()
        .parse::<u128>()
        .expect("worker timestamp integer")
}

fn retry_assert_multiprocess_workers_serialized(result_dir: &str) {
    let a_start = retry_read_worker_timestamp(result_dir, "a", "start");
    let a_end = retry_read_worker_timestamp(result_dir, "a", "end");
    let b_start = retry_read_worker_timestamp(result_dir, "b", "start");
    let b_end = retry_read_worker_timestamp(result_dir, "b", "end");

    assert!(
        a_end <= b_start + 25 || b_end <= a_start + 25,
        "workers overlapped: a={a_start}..{a_end}, b={b_start}..{b_end}"
    );
}

#[test]
fn retry_multiprocess_async_worker_entry() {
    if env::var("ABXBUS_RETRY_MP_WORKER").ok().as_deref() != Some("async") {
        return;
    }

    struct Worker;
    impl Worker {
        abxbus::retry! {
            max_attempts = 1,
            semaphore_limit = 1,
            semaphore_scope = "multiprocess",
            semaphore_timeout = 5.0,
            semaphore_lax = false,
            semaphore_name = name;
            async fn run(&self, name: &str, result_dir: &str, id: &str) -> Result<(), RetryTestError> {
                        let _ = name;
                        fs::write(format!("{result_dir}/{id}.start"), retry_test_now_millis().to_string())
                            .expect("write worker start");
                        futures_timer::Delay::new(Duration::from_millis(250)).await;
                        fs::write(format!("{result_dir}/{id}.end"), retry_test_now_millis().to_string())
                            .expect("write worker end");
                        Ok(())
                    }
        }
    }

    let semaphore_name = env::var("ABXBUS_RETRY_MP_SEMAPHORE").expect("worker semaphore");
    let result_dir = env::var("ABXBUS_RETRY_MP_RESULT_DIR").expect("worker result dir");
    let id = env::var("ABXBUS_RETRY_MP_ID").expect("worker id");
    block_on(Worker.run(&semaphore_name, &result_dir, &id)).unwrap();
}

#[test]
fn retry_multiprocess_sync_worker_entry() {
    if env::var("ABXBUS_RETRY_MP_WORKER").ok().as_deref() != Some("sync") {
        return;
    }

    struct Worker;
    impl Worker {
        abxbus::retry! {
            max_attempts = 1,
            semaphore_limit = 1,
            semaphore_scope = "multiprocess",
            semaphore_timeout = 5.0,
            semaphore_lax = false,
            semaphore_name = name;
            fn run(&self, name: &str, result_dir: &str, id: &str) -> Result<(), RetryTestError> {
                        let _ = name;
                        fs::write(format!("{result_dir}/{id}.start"), retry_test_now_millis().to_string())
                            .expect("write worker start");
                        thread::sleep(Duration::from_millis(250));
                        fs::write(format!("{result_dir}/{id}.end"), retry_test_now_millis().to_string())
                            .expect("write worker end");
                        Ok(())
                    }
        }
    }

    let semaphore_name = env::var("ABXBUS_RETRY_MP_SEMAPHORE").expect("worker semaphore");
    let result_dir = env::var("ABXBUS_RETRY_MP_RESULT_DIR").expect("worker result dir");
    let id = env::var("ABXBUS_RETRY_MP_ID").expect("worker id");
    Worker.run(&semaphore_name, &result_dir, &id).unwrap();
}

#[test]
fn test_retry_semaphore_scope_multiprocess_serializes_across_rust_processes() {
    let unique = retry_test_now_millis();
    let semaphore_name = format!("rust-retry-multiprocess-{unique}");
    let result_dir = env::temp_dir()
        .join(format!("abxbus-rust-retry-multiprocess-{unique}"))
        .to_string_lossy()
        .to_string();
    fs::create_dir_all(&result_dir).expect("result dir");

    let mut first = retry_spawn_multiprocess_worker("async", &semaphore_name, &result_dir, "a")
        .spawn()
        .expect("spawn first worker");
    let mut second = retry_spawn_multiprocess_worker("async", &semaphore_name, &result_dir, "b")
        .spawn()
        .expect("spawn second worker");

    assert!(first.wait().expect("first worker").success());
    assert!(second.wait().expect("second worker").success());
    retry_assert_multiprocess_workers_serialized(&result_dir);
}

#[test]
fn test_retry_sync_semaphore_scope_multiprocess_serializes_across_rust_processes() {
    let unique = retry_test_now_millis();
    let semaphore_name = format!("rust-retry-sync-multiprocess-{unique}");
    let result_dir = env::temp_dir()
        .join(format!("abxbus-rust-retry-sync-multiprocess-{unique}"))
        .to_string_lossy()
        .to_string();
    fs::create_dir_all(&result_dir).expect("result dir");

    let mut first = retry_spawn_multiprocess_worker("sync", &semaphore_name, &result_dir, "a")
        .spawn()
        .expect("spawn first worker");
    let mut second = retry_spawn_multiprocess_worker("sync", &semaphore_name, &result_dir, "b")
        .spawn()
        .expect("spawn second worker");

    assert!(first.wait().expect("first worker").success());
    assert!(second.wait().expect("second worker").success());
    retry_assert_multiprocess_workers_serialized(&result_dir);
}
