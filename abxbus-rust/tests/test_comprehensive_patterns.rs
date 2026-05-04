use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use abxbus_rust::{
    event_bus::EventBus,
    typed::{EventSpec, TypedEvent},
    types::EventStatus,
};
use futures::{executor::block_on, join};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Serialize, Deserialize)]
struct EmptyPayload {}

struct Event1;
impl EventSpec for Event1 {
    type Payload = EmptyPayload;
    type Result = String;
    const EVENT_TYPE: &'static str = "Event1";
}

struct Event2;
impl EventSpec for Event2 {
    type Payload = EmptyPayload;
    type Result = String;
    const EVENT_TYPE: &'static str = "Event2";
}

struct Event3;
impl EventSpec for Event3 {
    type Payload = EmptyPayload;
    type Result = String;
    const EVENT_TYPE: &'static str = "Event3";
}

struct ChildEvent;
impl EventSpec for ChildEvent {
    type Payload = EmptyPayload;
    type Result = String;
    const EVENT_TYPE: &'static str = "ChildEvent";
}

struct ChildA;
impl EventSpec for ChildA {
    type Payload = EmptyPayload;
    type Result = String;
    const EVENT_TYPE: &'static str = "ChildA";
}

struct ChildB;
impl EventSpec for ChildB {
    type Payload = EmptyPayload;
    type Result = String;
    const EVENT_TYPE: &'static str = "ChildB";
}

struct ChildC;
impl EventSpec for ChildC {
    type Payload = EmptyPayload;
    type Result = String;
    const EVENT_TYPE: &'static str = "ChildC";
}

struct Child1;
impl EventSpec for Child1 {
    type Payload = EmptyPayload;
    type Result = String;
    const EVENT_TYPE: &'static str = "Child1";
}

struct Child2;
impl EventSpec for Child2 {
    type Payload = EmptyPayload;
    type Result = String;
    const EVENT_TYPE: &'static str = "Child2";
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

#[test]
fn test_awaited_child_jumps_queue_no_overshoot() {
    let bus = EventBus::new(Some("ComprehensiveNoOvershootBus".to_string()));
    let execution_order = Arc::new(Mutex::new(Vec::new()));

    let bus_for_event1 = bus.clone();
    let order_for_event1 = execution_order.clone();
    bus.on("Event1", "event1_handler", move |_event| {
        let bus = bus_for_event1.clone();
        let order = order_for_event1.clone();
        async move {
            push(&order, "Event1_start");
            let child = bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            push(&order, "Child_dispatched");
            child.wait_completed().await;
            push(&order, "Child_await_returned");
            push(&order, "Event1_end");
            Ok(json!("event1_done"))
        }
    });

    let order_for_event2 = execution_order.clone();
    bus.on("Event2", "event2_handler", move |_event| {
        let order = order_for_event2.clone();
        async move {
            push(&order, "Event2_start");
            push(&order, "Event2_end");
            Ok(json!("event2_done"))
        }
    });

    let order_for_event3 = execution_order.clone();
    bus.on("Event3", "event3_handler", move |_event| {
        let order = order_for_event3.clone();
        async move {
            push(&order, "Event3_start");
            push(&order, "Event3_end");
            Ok(json!("event3_done"))
        }
    });

    let order_for_child = execution_order.clone();
    bus.on("ChildEvent", "child_handler", move |_event| {
        let order = order_for_child.clone();
        async move {
            push(&order, "Child_start");
            push(&order, "Child_end");
            Ok(json!("child_done"))
        }
    });

    let event1 = bus.emit::<Event1>(TypedEvent::new(EmptyPayload {}));
    let event2 = bus.emit::<Event2>(TypedEvent::new(EmptyPayload {}));
    let event3 = bus.emit::<Event3>(TypedEvent::new(EmptyPayload {}));

    block_on(event1.wait_completed());
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
fn test_dispatch_multiple_await_one_skips_others() {
    let bus = EventBus::new(Some("ComprehensiveMultiDispatchBus".to_string()));
    let execution_order = Arc::new(Mutex::new(Vec::new()));

    let bus_for_event1 = bus.clone();
    let order_for_event1 = execution_order.clone();
    bus.on("Event1", "event1_handler", move |_event| {
        let bus = bus_for_event1.clone();
        let order = order_for_event1.clone();
        async move {
            push(&order, "Event1_start");
            bus.emit_child::<ChildA>(TypedEvent::new(EmptyPayload {}));
            push(&order, "ChildA_dispatched");
            let child_b = bus.emit_child::<ChildB>(TypedEvent::new(EmptyPayload {}));
            push(&order, "ChildB_dispatched");
            bus.emit_child::<ChildC>(TypedEvent::new(EmptyPayload {}));
            push(&order, "ChildC_dispatched");
            child_b.wait_completed().await;
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
        bus.on(pattern, &format!("{pattern}_handler"), move |_event| {
            let order = order.clone();
            async move {
                push(&order, start);
                push(&order, end);
                Ok(json!(result))
            }
        });
    }

    let event1 = bus.emit::<Event1>(TypedEvent::new(EmptyPayload {}));
    bus.emit::<Event2>(TypedEvent::new(EmptyPayload {}));
    bus.emit::<Event3>(TypedEvent::new(EmptyPayload {}));

    block_on(event1.wait_completed());
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
fn test_multiple_awaits_same_event() {
    let bus = EventBus::new(Some("ComprehensiveMultiAwaitBus".to_string()));
    let execution_order = Arc::new(Mutex::new(Vec::new()));
    let await_results = Arc::new(Mutex::new(Vec::new()));

    let bus_for_event1 = bus.clone();
    let order_for_event1 = execution_order.clone();
    let await_results_for_event1 = await_results.clone();
    bus.on("Event1", "event1_handler", move |_event| {
        let bus = bus_for_event1.clone();
        let order = order_for_event1.clone();
        let await_results = await_results_for_event1.clone();
        async move {
            push(&order, "Event1_start");
            let child = bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            let child_for_await1 = TypedEvent::<ChildEvent>::from_base_event(child.inner.clone());
            let child_for_await2 = TypedEvent::<ChildEvent>::from_base_event(child.inner.clone());
            let await_results_1 = await_results.clone();
            let await_results_2 = await_results.clone();
            join!(
                async move {
                    child_for_await1.wait_completed().await;
                    push(&await_results_1, "await1_completed");
                },
                async move {
                    child_for_await2.wait_completed().await;
                    push(&await_results_2, "await2_completed");
                }
            );
            push(&order, "Both_awaits_completed");
            push(&order, "Event1_end");
            Ok(json!("event1_done"))
        }
    });

    let order_for_event2 = execution_order.clone();
    bus.on("Event2", "event2_handler", move |_event| {
        let order = order_for_event2.clone();
        async move {
            push(&order, "Event2_start");
            push(&order, "Event2_end");
            Ok(json!("event2_done"))
        }
    });

    let order_for_child = execution_order.clone();
    bus.on("ChildEvent", "child_handler", move |_event| {
        let order = order_for_child.clone();
        async move {
            push(&order, "Child_start");
            thread::sleep(Duration::from_millis(10));
            push(&order, "Child_end");
            Ok(json!("child_done"))
        }
    });

    let event1 = bus.emit::<Event1>(TypedEvent::new(EmptyPayload {}));
    bus.emit::<Event2>(TypedEvent::new(EmptyPayload {}));

    block_on(event1.wait_completed());

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
    let bus = EventBus::new(Some("ComprehensiveDeepNestedBus".to_string()));
    let execution_order = Arc::new(Mutex::new(Vec::new()));

    let bus_for_event1 = bus.clone();
    let order_for_event1 = execution_order.clone();
    bus.on("Event1", "event1_handler", move |_event| {
        let bus = bus_for_event1.clone();
        let order = order_for_event1.clone();
        async move {
            push(&order, "Event1_start");
            let child1 = bus.emit_child::<Child1>(TypedEvent::new(EmptyPayload {}));
            child1.wait_completed().await;
            push(&order, "Event1_end");
            Ok(json!("event1_done"))
        }
    });

    let bus_for_child1 = bus.clone();
    let order_for_child1 = execution_order.clone();
    bus.on("Child1", "child1_handler", move |_event| {
        let bus = bus_for_child1.clone();
        let order = order_for_child1.clone();
        async move {
            push(&order, "Child1_start");
            let child2 = bus.emit_child::<Child2>(TypedEvent::new(EmptyPayload {}));
            child2.wait_completed().await;
            push(&order, "Child1_end");
            Ok(json!("child1_done"))
        }
    });

    let order_for_child2 = execution_order.clone();
    bus.on("Child2", "child2_handler", move |_event| {
        let order = order_for_child2.clone();
        async move {
            push(&order, "Child2_start");
            push(&order, "Child2_end");
            Ok(json!("child2_done"))
        }
    });

    let order_for_event2 = execution_order.clone();
    bus.on("Event2", "event2_handler", move |_event| {
        let order = order_for_event2.clone();
        async move {
            push(&order, "Event2_start");
            push(&order, "Event2_end");
            Ok(json!("event2_done"))
        }
    });

    let event1 = bus.emit::<Event1>(TypedEvent::new(EmptyPayload {}));
    bus.emit::<Event2>(TypedEvent::new(EmptyPayload {}));

    block_on(event1.wait_completed());

    let order = execution_order.lock().expect("order lock").clone();
    assert!(index_of(&order, "Child2_end") < index_of(&order, "Child1_end"));
    assert!(index_of(&order, "Child1_end") < index_of(&order, "Event1_end"));
    assert!(!order.contains(&"Event2_start".to_string()));

    block_on(bus.wait_until_idle(Some(2.0)));
    let order = execution_order.lock().expect("order lock").clone();
    assert!(index_of(&order, "Event2_start") > index_of(&order, "Event1_end"));
    bus.stop();
}
