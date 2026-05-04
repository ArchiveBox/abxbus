use std::{
    collections::BTreeSet,
    sync::{Arc, Barrier},
    thread,
};

use abxbus_rust::{
    event_bus::{EventBus, EventBusOptions},
    typed::{EventSpec, TypedEvent},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Serialize, Deserialize)]
struct EmptyPayload {}

#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}

struct GcHistoryEvent;
impl EventSpec for GcHistoryEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "GcHistoryEvent";
}

struct GcImplicitEvent;
impl EventSpec for GcImplicitEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "GcImplicitEvent";
}

#[test]
fn test_name_conflict_with_live_reference() {
    let bus1 = EventBus::new(Some("GCTestConflict".to_string()));
    let bus2 = EventBus::new(Some("GCTestConflict".to_string()));

    assert_eq!(bus1.name, "GCTestConflict");
    assert!(bus2.name.starts_with("GCTestConflict_"));
    assert_ne!(bus2.name, "GCTestConflict");
    assert_eq!(bus2.name.len(), "GCTestConflict_".len() + 8);
    bus1.stop();
    bus2.stop();
}

#[test]
fn test_name_no_conflict_after_deletion() {
    let weak_ref = {
        let bus1 = EventBus::new(Some("GCTestBus1".to_string()));
        Arc::downgrade(&bus1)
    };
    assert!(weak_ref.upgrade().is_none());

    let bus2 = EventBus::new(Some("GCTestBus1".to_string()));
    assert_eq!(bus2.name, "GCTestBus1");
    bus2.stop();
}

#[test]
fn test_name_no_conflict_with_no_reference() {
    {
        let _ = EventBus::new(Some("GCTestBus2".to_string()));
    }

    let bus2 = EventBus::new(Some("GCTestBus2".to_string()));
    assert_eq!(bus2.name, "GCTestBus2");
    bus2.stop();
}

#[test]
fn test_name_conflict_with_weak_reference_only() {
    let weak_ref = {
        let bus1 = EventBus::new(Some("GCTestBus3".to_string()));
        let weak_ref = Arc::downgrade(&bus1);
        assert!(weak_ref.upgrade().is_some());
        weak_ref
    };

    assert!(weak_ref.upgrade().is_none());
    let bus2 = EventBus::new(Some("GCTestBus3".to_string()));
    assert_eq!(bus2.name, "GCTestBus3");
    bus2.stop();
}

#[test]
fn test_multiple_buses_with_gc() {
    let bus1 = EventBus::new(Some("GCMulti1".to_string()));
    {
        let _ = EventBus::new(Some("GCMulti2".to_string()));
    }
    let bus3 = EventBus::new(Some("GCMulti3".to_string()));
    {
        let _ = EventBus::new(Some("GCMulti4".to_string()));
    }

    let bus2_new = EventBus::new(Some("GCMulti2".to_string()));
    let bus4_new = EventBus::new(Some("GCMulti4".to_string()));
    assert_eq!(bus2_new.name, "GCMulti2");
    assert_eq!(bus4_new.name, "GCMulti4");

    let bus1_conflict = EventBus::new(Some("GCMulti1".to_string()));
    assert!(bus1_conflict.name.starts_with("GCMulti1_"));
    assert_ne!(bus1_conflict.name, bus1.name);

    let bus3_conflict = EventBus::new(Some("GCMulti3".to_string()));
    assert!(bus3_conflict.name.starts_with("GCMulti3_"));
    assert_ne!(bus3_conflict.name, bus3.name);

    bus1.stop();
    bus2_new.stop();
    bus3.stop();
    bus4_new.stop();
    bus1_conflict.stop();
    bus3_conflict.stop();
}

#[test]
fn test_name_conflict_after_stop_and_clear() {
    let bus1 = EventBus::new(Some("GCStopClear".to_string()));
    bus1.stop();

    let bus2 = EventBus::new(Some("GCStopClear".to_string()));
    assert_eq!(bus2.name, "GCStopClear");
    bus2.stop();
}

#[test]
fn test_weakset_behavior() {
    let initial_count = EventBus::all_instances_len();
    let bus1 = EventBus::new(Some("WeakTest1".to_string()));
    let bus2 = EventBus::new(Some("WeakTest2".to_string()));
    let bus3 = EventBus::new(Some("WeakTest3".to_string()));
    let weak2 = Arc::downgrade(&bus2);

    assert!(EventBus::all_instances_contains(&bus1));
    assert!(EventBus::all_instances_contains(&bus2));
    assert!(EventBus::all_instances_contains(&bus3));
    assert_eq!(EventBus::all_instances_len(), initial_count + 3);

    drop(bus2);
    assert!(weak2.upgrade().is_none());
    assert!(EventBus::all_instances_contains(&bus1));
    assert!(EventBus::all_instances_contains(&bus3));
    assert!(EventBus::all_instances_len() <= initial_count + 2);
    bus1.stop();
    bus3.stop();
}

#[test]
fn test_eventbus_removed_from_weakset() {
    {
        let _ = EventBus::new(Some("GCDeadBus".to_string()));
    }

    let bus = EventBus::new(Some("GCDeadBus".to_string()));
    assert_eq!(bus.name, "GCDeadBus");
    assert!(EventBus::all_instances_contains(&bus));
    bus.stop();
}

#[test]
fn test_concurrent_name_creation() {
    let workers = 8;
    let barrier = Arc::new(Barrier::new(workers));
    let handles = (0..workers)
        .map(|_| {
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                EventBus::new(Some("ConcurrentTest".to_string()))
            })
        })
        .collect::<Vec<_>>();

    let buses = handles
        .into_iter()
        .map(|handle| handle.join().expect("worker creates bus"))
        .collect::<Vec<_>>();
    let names = buses.iter().map(|bus| bus.name.clone()).collect::<Vec<_>>();
    let unique_names = names.iter().cloned().collect::<BTreeSet<_>>();

    assert_eq!(unique_names.len(), workers);
    assert!(unique_names.contains("ConcurrentTest"));
    assert!(unique_names.iter().all(|name| {
        name == "ConcurrentTest"
            || (name.starts_with("ConcurrentTest_") && name.len() == "ConcurrentTest_".len() + 8)
    }));

    for bus in buses {
        bus.stop();
    }
}

#[test]
fn test_unreferenced_buses_with_history_can_be_cleaned_without_instance_leak() {
    let baseline_instances = EventBus::all_instances_len();
    let mut refs = Vec::new();

    for index in 0..10 {
        let bus = EventBus::new_with_options(
            Some(format!("GCNoStopBus_{index}")),
            EventBusOptions {
                max_history_size: Some(40),
                ..EventBusOptions::default()
            },
        );
        bus.on("GcHistoryEvent", "history_handler", |_event| async move {
            Ok(json!("ok"))
        });
        for _ in 0..20 {
            let event = bus.emit::<GcHistoryEvent>(TypedEvent::new(EmptyPayload {}));
            block_on(event.wait_completed());
        }
        block_on(bus.wait_until_idle(Some(1.0)));
        refs.push(Arc::downgrade(&bus));
        bus.stop();
    }

    assert!(refs.iter().all(|weak_ref| weak_ref.upgrade().is_none()));
    assert!(EventBus::all_instances_len() <= baseline_instances);
}

#[test]
fn test_unreferenced_buses_with_history_are_collected_without_stop() {
    let baseline_instances = EventBus::all_instances_len();
    let mut refs = Vec::new();

    for index in 0..10 {
        let bus = EventBus::new_with_options(
            Some(format!("GCImplicitNoStop_{index}")),
            EventBusOptions {
                max_history_size: Some(30),
                ..EventBusOptions::default()
            },
        );
        bus.on("GcImplicitEvent", "implicit_handler", |_event| async move {
            Ok(json!("ok"))
        });
        for _ in 0..20 {
            let event = bus.emit::<GcImplicitEvent>(TypedEvent::new(EmptyPayload {}));
            block_on(event.wait_completed());
        }
        block_on(bus.wait_until_idle(Some(1.0)));
        refs.push(Arc::downgrade(&bus));
    }

    assert!(refs.iter().all(|weak_ref| weak_ref.upgrade().is_none()));
    assert!(EventBus::all_instances_len() <= baseline_instances);
}
