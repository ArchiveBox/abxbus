use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use abxbus_rust::{
    event_bus::EventBus,
    typed::{EventSpec, TypedEvent},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Serialize, Deserialize)]
struct PingPayload {
    value: i64,
}

#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}

struct PingEvent;
impl EventSpec for PingEvent {
    type Payload = PingPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "PingEvent";
}

#[derive(Clone, Serialize, Deserialize)]
struct OrderPayload {
    order: i64,
}

struct OrderEvent;
impl EventSpec for OrderEvent {
    type Payload = OrderPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "OrderEvent";
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
    bus_a.on("PingEvent", "seen_a", move |event| {
        let seen = seen_a_handler.clone();
        async move {
            seen.lock()
                .expect("seen_a lock")
                .push(event.inner.lock().event_id.clone());
            Ok(json!(null))
        }
    });
    let seen_b_handler = seen_b.clone();
    bus_b.on("PingEvent", "seen_b", move |event| {
        let seen = seen_b_handler.clone();
        async move {
            seen.lock()
                .expect("seen_b lock")
                .push(event.inner.lock().event_id.clone());
            Ok(json!(null))
        }
    });
    let seen_c_handler = seen_c.clone();
    bus_c.on("PingEvent", "seen_c", move |event| {
        let seen = seen_c_handler.clone();
        async move {
            seen.lock()
                .expect("seen_c lock")
                .push(event.inner.lock().event_id.clone());
            Ok(json!(null))
        }
    });

    let bus_b_for_forward = bus_b.clone();
    bus_a.on("*", "forward_to_b", move |event| {
        let bus_b = bus_b_for_forward.clone();
        async move {
            bus_b.emit_base(event);
            Ok(json!(null))
        }
    });
    let bus_c_for_forward = bus_c.clone();
    bus_b.on("*", "forward_to_c", move |event| {
        let bus_c = bus_c_for_forward.clone();
        async move {
            bus_c.emit_base(event);
            Ok(json!(null))
        }
    });

    let event = bus_a.emit::<PingEvent>(TypedEvent::new(PingPayload { value: 1 }));
    block_on(event.wait_completed());
    block_on(bus_a.wait_until_idle(None));
    block_on(bus_b.wait_until_idle(None));
    block_on(bus_c.wait_until_idle(None));

    let event_id = event.inner.inner.lock().event_id.clone();
    assert_eq!(
        seen_a.lock().expect("seen_a lock").as_slice(),
        &[event_id.clone()]
    );
    assert_eq!(
        seen_b.lock().expect("seen_b lock").as_slice(),
        &[event_id.clone()]
    );
    assert_eq!(
        seen_c.lock().expect("seen_c lock").as_slice(),
        &[event_id.clone()]
    );
    assert_eq!(
        event.inner.inner.lock().event_path,
        vec![bus_a.label(), bus_b.label(), bus_c.label()]
    );
    assert_eq!(event.inner.inner.lock().event_pending_bus_count, 0);
    bus_a.stop();
    bus_b.stop();
    bus_c.stop();
}

#[test]
fn test_forwarding_disambiguates_buses_that_share_the_same_name() {
    let bus_a = EventBus::new(Some("SharedName".to_string()));
    let bus_b = EventBus::new(Some("SharedName".to_string()));

    let seen_a = Arc::new(Mutex::new(Vec::new()));
    let seen_b = Arc::new(Mutex::new(Vec::new()));

    let seen_a_handler = seen_a.clone();
    bus_a.on("PingEvent", "seen_a", move |event| {
        let seen = seen_a_handler.clone();
        async move {
            seen.lock()
                .expect("seen_a lock")
                .push(event.inner.lock().event_id.clone());
            Ok(json!(null))
        }
    });
    let seen_b_handler = seen_b.clone();
    bus_b.on("PingEvent", "seen_b", move |event| {
        let seen = seen_b_handler.clone();
        async move {
            seen.lock()
                .expect("seen_b lock")
                .push(event.inner.lock().event_id.clone());
            Ok(json!(null))
        }
    });

    let bus_b_for_forward = bus_b.clone();
    bus_a.on("*", "forward_to_shared_name_peer", move |event| {
        let bus_b = bus_b_for_forward.clone();
        async move {
            bus_b.emit_base(event);
            Ok(json!(null))
        }
    });

    let event = bus_a.emit::<PingEvent>(TypedEvent::new(PingPayload { value: 99 }));
    block_on(event.wait_completed());
    block_on(bus_a.wait_until_idle(None));
    block_on(bus_b.wait_until_idle(None));

    let event_id = event.inner.inner.lock().event_id.clone();
    assert_eq!(
        seen_a.lock().expect("seen_a lock").as_slice(),
        &[event_id.clone()]
    );
    assert_eq!(
        seen_b.lock().expect("seen_b lock").as_slice(),
        &[event_id.clone()]
    );
    assert_ne!(bus_a.label(), bus_b.label());
    assert_eq!(
        event.inner.inner.lock().event_path,
        vec![bus_a.label(), bus_b.label()]
    );
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_circular_forwarding_a_to_b_to_c_to_a_does_not_loop() {
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
        bus.on("PingEvent", handler_name, move |event| {
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
    peer1.on("*", "forward_to_peer2", move |event| {
        let peer2 = peer2_for_forward.clone();
        async move {
            peer2.emit_base(event);
            Ok(json!(null))
        }
    });
    let peer3_for_forward = peer3.clone();
    peer2.on("*", "forward_to_peer3", move |event| {
        let peer3 = peer3_for_forward.clone();
        async move {
            peer3.emit_base(event);
            Ok(json!(null))
        }
    });
    let peer1_for_forward = peer1.clone();
    peer3.on("*", "forward_to_peer1", move |event| {
        let peer1 = peer1_for_forward.clone();
        async move {
            peer1.emit_base(event);
            Ok(json!(null))
        }
    });

    let event = peer1.emit::<PingEvent>(TypedEvent::new(PingPayload { value: 42 }));
    block_on(event.wait_completed());
    block_on(peer1.wait_until_idle(None));
    block_on(peer2.wait_until_idle(None));
    block_on(peer3.wait_until_idle(None));

    let event_id = event.inner.inner.lock().event_id.clone();
    assert_eq!(
        events_at_peer1.lock().expect("peer1 lock").as_slice(),
        &[event_id.clone()]
    );
    assert_eq!(
        events_at_peer2.lock().expect("peer2 lock").as_slice(),
        &[event_id.clone()]
    );
    assert_eq!(
        events_at_peer3.lock().expect("peer3 lock").as_slice(),
        &[event_id]
    );
    assert_eq!(
        event.inner.inner.lock().event_path,
        vec![peer1.label(), peer2.label(), peer3.label()]
    );
    peer1.stop();
    peer2.stop();
    peer3.stop();
}

#[test]
fn test_await_forwarded_event_waits_for_target_bus_handlers() {
    let bus_a = EventBus::new(Some("BusAWait".to_string()));
    let bus_b = EventBus::new(Some("BusBWait".to_string()));
    let completion_log = Arc::new(Mutex::new(Vec::new()));

    let log_a = completion_log.clone();
    bus_a.on("PingEvent", "handler_a", move |_event| {
        let log = log_a.clone();
        async move {
            thread::sleep(Duration::from_millis(10));
            log.lock().expect("log lock").push("A");
            Ok(json!(null))
        }
    });

    let log_b = completion_log.clone();
    bus_b.on("PingEvent", "handler_b", move |_event| {
        let log = log_b.clone();
        async move {
            thread::sleep(Duration::from_millis(30));
            log.lock().expect("log lock").push("B");
            Ok(json!(null))
        }
    });

    let bus_b_for_forward = bus_b.clone();
    bus_a.on("*", "forward_to_b", move |event| {
        let bus_b = bus_b_for_forward.clone();
        async move {
            bus_b.emit_base(event);
            Ok(json!(null))
        }
    });

    let event = bus_a.emit::<PingEvent>(TypedEvent::new(PingPayload { value: 2 }));
    block_on(event.wait_completed());

    let mut log = completion_log.lock().expect("log lock").clone();
    log.sort();
    assert_eq!(log, vec!["A", "B"]);
    assert_eq!(event.inner.inner.lock().event_pending_bus_count, 0);
    assert_eq!(
        event.inner.inner.lock().event_path,
        vec![bus_a.label(), bus_b.label()]
    );
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_await_forwarded_event_waits_when_forwarding_handler_is_async_delayed() {
    let bus_a = EventBus::new(Some("BusADelayedForward".to_string()));
    let bus_b = EventBus::new(Some("BusBDelayedForward".to_string()));

    let bus_a_done = Arc::new(Mutex::new(false));
    let bus_b_done = Arc::new(Mutex::new(false));

    let bus_a_done_handler = bus_a_done.clone();
    bus_a.on("PingEvent", "handler_a", move |_event| {
        let done = bus_a_done_handler.clone();
        async move {
            thread::sleep(Duration::from_millis(20));
            *done.lock().expect("bus_a_done lock") = true;
            Ok(json!(null))
        }
    });

    let bus_b_done_handler = bus_b_done.clone();
    bus_b.on("PingEvent", "handler_b", move |_event| {
        let done = bus_b_done_handler.clone();
        async move {
            thread::sleep(Duration::from_millis(10));
            *done.lock().expect("bus_b_done lock") = true;
            Ok(json!(null))
        }
    });

    let bus_b_for_forward = bus_b.clone();
    bus_a.on("*", "delayed_forward_to_b", move |event| {
        let bus_b = bus_b_for_forward.clone();
        async move {
            thread::sleep(Duration::from_millis(30));
            bus_b.emit_base(event);
            Ok(json!(null))
        }
    });

    let event = bus_a.emit::<PingEvent>(TypedEvent::new(PingPayload { value: 3 }));
    block_on(event.wait_completed());

    assert!(*bus_a_done.lock().expect("bus_a_done lock"));
    assert!(*bus_b_done.lock().expect("bus_b_done lock"));
    assert_eq!(event.inner.inner.lock().event_pending_bus_count, 0);
    assert_eq!(
        event.inner.inner.lock().event_path,
        vec![bus_a.label(), bus_b.label()]
    );
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_forwarding_same_event_does_not_set_self_parent_id() {
    let origin = EventBus::new(Some("SelfParentOrigin".to_string()));
    let target = EventBus::new(Some("SelfParentTarget".to_string()));

    origin.on("PingEvent", "origin_handler", |_event| async move {
        Ok(json!("origin-ok"))
    });
    target.on("PingEvent", "target_handler", |_event| async move {
        Ok(json!("target-ok"))
    });

    let target_for_forward = target.clone();
    origin.on("*", "forward_to_target", move |event| {
        let target = target_for_forward.clone();
        async move {
            target.emit_base(event);
            Ok(json!(null))
        }
    });

    let event = origin.emit::<PingEvent>(TypedEvent::new(PingPayload { value: 9 }));
    block_on(event.wait_completed());
    block_on(origin.wait_until_idle(None));
    block_on(target.wait_until_idle(None));

    assert_eq!(event.inner.inner.lock().event_parent_id, None);
    assert_eq!(
        event.inner.inner.lock().event_path,
        vec![origin.label(), target.label()]
    );
    origin.stop();
    target.stop();
}

#[test]
fn test_events_are_processed_in_fifo_order() {
    let bus = EventBus::new(Some("FifoBus".to_string()));
    let processed_orders = Arc::new(Mutex::new(Vec::new()));

    let processed_orders_handler = processed_orders.clone();
    bus.on("OrderEvent", "order_handler", move |event| {
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
        bus.emit::<OrderEvent>(TypedEvent::new(OrderPayload { order }));
    }

    block_on(bus.wait_until_idle(None));
    assert_eq!(
        processed_orders.lock().expect("orders lock").as_slice(),
        &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    );
    bus.stop();
}
