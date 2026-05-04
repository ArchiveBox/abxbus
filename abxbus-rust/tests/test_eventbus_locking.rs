use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use abxbus_rust::{
    event_bus::{EventBus, EventBusOptions},
    typed::{EventSpec, TypedEvent},
    types::{EventConcurrencyMode, EventHandlerConcurrencyMode},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Serialize, Deserialize)]
struct QPayload {
    idx: i64,
}
#[derive(Clone, Serialize, Deserialize)]
struct EmptyPayload {}
#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}
struct QEvent;
impl EventSpec for QEvent {
    type Payload = QPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "q";
}
struct WorkEvent;
impl EventSpec for WorkEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "work";
}
struct ParentEvent;
impl EventSpec for ParentEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "parent";
}
#[derive(Clone, Serialize, Deserialize)]
struct SerialPayload {
    order: i64,
    source: String,
}
struct SerialEvent;
impl EventSpec for SerialEvent {
    type Payload = SerialPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "serial";
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

    bus.on("q", "h", move |event| {
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

    let blocker = bus.emit::<QEvent>(TypedEvent::<QEvent>::new(QPayload { idx: 0 }));
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("blocker should start");
    let sibling = bus.emit::<QEvent>(TypedEvent::<QEvent>::new(QPayload { idx: 1 }));
    let jumped =
        bus.emit_with_options::<QEvent>(TypedEvent::<QEvent>::new(QPayload { idx: 2 }), true);

    block_on(async {
        blocker.wait_completed().await;
        sibling.wait_completed().await;
        jumped.wait_completed().await;
    });

    let order = order.lock().expect("order lock").clone();
    assert_eq!(order, vec![0, 2, 1]);

    let sibling_started = sibling
        .inner
        .inner
        .lock()
        .event_started_at
        .clone()
        .unwrap_or_default();
    let jumped_started = jumped
        .inner
        .inner
        .lock()
        .event_started_at
        .clone()
        .unwrap_or_default();
    assert!(jumped_started <= sibling_started);
    bus.stop();
}

#[test]
fn test_emit_with_queue_jump_preempts_queued_sibling_on_same_bus() {
    let bus = EventBus::new(Some("BusJumpNamedParity".to_string()));
    let order = Arc::new(Mutex::new(Vec::new()));
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let order_for_handler = order.clone();

    bus.on("q", "h", move |event| {
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

    let blocker = bus.emit::<QEvent>(TypedEvent::<QEvent>::new(QPayload { idx: 0 }));
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("blocker should start");
    let sibling = bus.emit::<QEvent>(TypedEvent::<QEvent>::new(QPayload { idx: 1 }));
    let jumped =
        bus.emit_with_options::<QEvent>(TypedEvent::<QEvent>::new(QPayload { idx: 2 }), true);

    block_on(async {
        blocker.wait_completed().await;
        sibling.wait_completed().await;
        jumped.wait_completed().await;
    });

    assert_eq!(order.lock().expect("order lock").as_slice(), &[0, 2, 1]);
    bus.stop();
}

#[test]
fn test_bus_serial_processes_in_order() {
    let bus = EventBus::new(Some("BusSerial".to_string()));

    bus.on("work", "slow", |_event| async move {
        thread::sleep(Duration::from_millis(15));
        Ok(json!(1))
    });

    let event1 = TypedEvent::<WorkEvent>::new(EmptyPayload {});
    let event2 = TypedEvent::<WorkEvent>::new(EmptyPayload {});
    event1.inner.inner.lock().event_concurrency = Some(EventConcurrencyMode::BusSerial);
    event2.inner.inner.lock().event_concurrency = Some(EventConcurrencyMode::BusSerial);
    let event1 = bus.emit(event1);
    let event2 = bus.emit(event2);

    block_on(async {
        event1.wait_completed().await;
        event2.wait_completed().await;
    });

    let event1_started = event1
        .inner
        .inner
        .lock()
        .event_started_at
        .clone()
        .unwrap_or_default();
    let event2_started = event2
        .inner
        .inner
        .lock()
        .event_started_at
        .clone()
        .unwrap_or_default();
    assert!(event1_started <= event2_started);
    bus.stop();
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
        bus.on("serial", "global_serial_handler", move |event| {
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
        bus_a.emit::<SerialEvent>(TypedEvent::new(SerialPayload {
            order: i,
            source: "a".to_string(),
        }));
        bus_b.emit::<SerialEvent>(TypedEvent::new(SerialPayload {
            order: i,
            source: "b".to_string(),
        }));
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
    bus_a.stop();
    bus_b.stop();
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
    bus_b.on("work", "child_handler", move |_event| {
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
    bus_b.on("q", "queued_handler", move |_event| {
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
    bus_a.on("parent", "parent_handler", move |_event| {
        let bus_a = bus_a_for_parent.clone();
        let bus_b = bus_b_for_parent.clone();
        let order = order_for_parent.clone();
        async move {
            order
                .lock()
                .expect("order lock")
                .push("parent_start".to_string());
            bus_b.emit::<QEvent>(TypedEvent::new(QPayload { idx: 1 }));
            let child = bus_a.emit_child::<WorkEvent>(TypedEvent::new(EmptyPayload {}));
            bus_b.emit::<WorkEvent>(TypedEvent::from_base_event(child.inner.clone()));
            order
                .lock()
                .expect("order lock")
                .push("child_dispatched".to_string());
            child.wait_completed().await;
            order
                .lock()
                .expect("order lock")
                .push("child_awaited".to_string());
            Ok(json!(null))
        }
    });

    let parent = bus_a.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(parent.wait_completed());
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
    bus_a.stop();
    bus_b.stop();
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
        bus.on("serial", "bus_serial_handler", move |_event| {
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

    bus_a.emit::<SerialEvent>(TypedEvent::new(SerialPayload {
        order: 0,
        source: "a".to_string(),
    }));
    bus_b.emit::<SerialEvent>(TypedEvent::new(SerialPayload {
        order: 0,
        source: "b".to_string(),
    }));

    block_on(async {
        assert!(bus_a.wait_until_idle(Some(2.0)).await);
        assert!(bus_b.wait_until_idle(Some(2.0)).await);
    });

    assert!(*max_in_flight_global.lock().expect("max lock") >= 2);
    bus_a.stop();
    bus_b.stop();
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
    bus.on("serial", "parallel_event_handler", move |_event| {
        let in_flight = in_flight_for_handler.clone();
        let max_in_flight = max_for_handler.clone();
        async move {
            bump_in_flight(&in_flight, &max_in_flight);
            thread::sleep(Duration::from_millis(40));
            drop_in_flight(&in_flight);
            Ok(json!(null))
        }
    });

    bus.emit::<SerialEvent>(TypedEvent::new(SerialPayload {
        order: 0,
        source: "same".to_string(),
    }));
    bus.emit::<SerialEvent>(TypedEvent::new(SerialPayload {
        order: 1,
        source: "same".to_string(),
    }));

    block_on(async {
        assert!(bus.wait_until_idle(Some(2.0)).await);
    });
    assert!(*max_in_flight.lock().expect("max lock") >= 2);
    bus.stop();
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
        bus.on("work", handler_name, move |_event| {
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

    let event = bus.emit::<WorkEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());
    assert!(*max_in_flight.lock().expect("max lock") >= 2);
    bus.stop();
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
    bus.on("serial", "override_parallel_handler", move |_event| {
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
        let event = TypedEvent::<SerialEvent>::new(SerialPayload {
            order,
            source: "override".to_string(),
        });
        event.inner.inner.lock().event_concurrency = Some(EventConcurrencyMode::Parallel);
        bus.emit(event);
    }

    block_on(async {
        assert!(bus.wait_until_idle(Some(2.0)).await);
    });
    assert!(*max_in_flight.lock().expect("max lock") >= 2);
    bus.stop();
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
    bus.on("serial", "override_bus_serial_handler", move |_event| {
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
        let event = TypedEvent::<SerialEvent>::new(SerialPayload {
            order,
            source: "override".to_string(),
        });
        event.inner.inner.lock().event_concurrency = Some(EventConcurrencyMode::BusSerial);
        bus.emit(event);
    }

    block_on(async {
        assert!(bus.wait_until_idle(Some(2.0)).await);
    });
    assert_eq!(*max_in_flight.lock().expect("max lock"), 1);
    bus.stop();
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
            bus.on("work", handler_name, move |_event| {
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

    bus_a.emit::<WorkEvent>(TypedEvent::new(EmptyPayload {}));
    bus_b.emit::<WorkEvent>(TypedEvent::new(EmptyPayload {}));

    block_on(async {
        assert!(bus_a.wait_until_idle(Some(2.0)).await);
        assert!(bus_b.wait_until_idle(Some(2.0)).await);
    });

    assert_eq!(*max_in_flight.lock().expect("max lock"), 2);
    bus_a.stop();
    bus_b.stop();
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
        bus.on("serial", handler_name, move |event| {
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
        bus.emit::<SerialEvent>(TypedEvent::new(SerialPayload {
            order,
            source: "parallel".to_string(),
        }));
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
    bus.stop();
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
        bus.on("serial", "cross_bus_handler", move |_event| {
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

    bus_a.emit::<SerialEvent>(TypedEvent::new(SerialPayload {
        order: 0,
        source: "a".to_string(),
    }));
    bus_b.emit::<SerialEvent>(TypedEvent::new(SerialPayload {
        order: 0,
        source: "b".to_string(),
    }));

    block_on(async {
        assert!(bus_a.wait_until_idle(Some(2.0)).await);
        assert!(bus_b.wait_until_idle(Some(2.0)).await);
    });

    assert!(*max_in_flight.lock().expect("max lock") >= 2);
    bus_a.stop();
    bus_b.stop();
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
    bus.on("serial", "auto_event_handler", move |_event| {
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
        let event = TypedEvent::<SerialEvent>::new(SerialPayload {
            order,
            source: "auto".to_string(),
        });
        event.inner.inner.lock().event_concurrency = None;
        bus.emit(event);
    }

    block_on(async {
        assert!(bus.wait_until_idle(Some(2.0)).await);
    });
    assert_eq!(*max_in_flight.lock().expect("max lock"), 1);
    bus.stop();
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
        bus.on("work", handler_name, move |_event| {
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

    let event = TypedEvent::<WorkEvent>::new(EmptyPayload {});
    event.inner.inner.lock().event_handler_concurrency = None;
    let event = bus.emit(event);
    block_on(event.wait_completed());

    assert_eq!(*max_in_flight.lock().expect("max lock"), 1);
    bus.stop();
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
    bus_a.on("work", "bus_a_shared", move |_event| {
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
    bus_b.on("work", "bus_b_shared", move |_event| {
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
    bus_a.on("q", "bus_a_sibling", move |_event| {
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
    bus_a.on("parent", "parent_handler", move |_event| {
        let bus_a = bus_a_for_parent.clone();
        let order = order_for_parent.clone();
        async move {
            order
                .lock()
                .expect("order lock")
                .push("parent_start".to_string());
            bus_a.emit::<QEvent>(TypedEvent::new(QPayload { idx: 1 }));
            let shared = bus_a.emit_child::<WorkEvent>(TypedEvent::new(EmptyPayload {}));
            order
                .lock()
                .expect("order lock")
                .push("shared_dispatched".to_string());
            shared.wait_completed().await;
            order
                .lock()
                .expect("order lock")
                .push("shared_awaited".to_string());
            Ok(json!(null))
        }
    });

    let parent = bus_a.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(parent.wait_completed());
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
    bus_a.stop();
    bus_b.stop();
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
    let child_ref = Arc::new(Mutex::new(None::<Arc<abxbus_rust::base_event::BaseEvent>>));
    let child_ref_for_handler = child_ref.clone();

    bus.on("parent", "parent_handler", move |_event| {
        let bus = bus_for_handler.clone();
        let child_ref = child_ref_for_handler.clone();
        async move {
            let child = bus.emit::<WorkEvent>(TypedEvent::new(EmptyPayload {}));
            assert_eq!(child.inner.inner.lock().event_parent_id, None);
            assert_eq!(child.inner.inner.lock().event_emitted_by_handler_id, None);
            assert!(!child.inner.inner.lock().event_blocks_parent_completion);
            *child_ref.lock().expect("child ref lock") = Some(child.inner.clone());
            child.wait_completed().await;
            assert!(!child.inner.inner.lock().event_blocks_parent_completion);
            Ok(json!(null))
        }
    });
    bus.on("work", "child_handler", |_event| async move {
        Ok(json!("child"))
    });

    let parent = bus.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(parent.wait_completed());
    block_on(bus.wait_until_idle(Some(2.0)));

    let child = child_ref
        .lock()
        .expect("child ref lock")
        .clone()
        .expect("child ref");
    assert_eq!(child.inner.lock().event_parent_id, None);
    assert_eq!(child.inner.lock().event_emitted_by_handler_id, None);
    assert!(!child.inner.lock().event_blocks_parent_completion);
    bus.stop();
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
    bus.on("work", "in_flight_handler", move |_event| {
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

    let child = bus.emit::<WorkEvent>(TypedEvent::new(EmptyPayload {}));
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("handler should start");

    let child_for_wait = TypedEvent::<WorkEvent>::from_base_event(child.inner.clone());
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        block_on(child_for_wait.wait_completed());
        let _ = done_tx.send(());
    });
    assert!(done_rx.recv_timeout(Duration::from_millis(30)).is_err());

    release_tx.send(()).expect("release send");
    done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("done should resolve");
    block_on(bus.wait_until_idle(Some(2.0)));
    assert_eq!(*handler_runs.lock().expect("runs lock"), 1);
    bus.stop();
}
