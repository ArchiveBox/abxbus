use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier,
    },
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

static NEXT_BUS_NAME: AtomicUsize = AtomicUsize::new(1);

fn unique_bus_name(prefix: &str) -> String {
    format!("{prefix}_{}", NEXT_BUS_NAME.fetch_add(1, Ordering::Relaxed))
}

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
    let requested_name = unique_bus_name("GCTestConflict");
    let bus1 = EventBus::new(Some(requested_name.clone()));
    let bus2 = EventBus::new(Some(requested_name.clone()));

    assert_eq!(bus1.name, requested_name);
    assert!(bus2.name.starts_with(&format!("{requested_name}_")));
    assert_ne!(bus2.name, requested_name);
    assert_eq!(bus2.name.len(), requested_name.len() + 1 + 8);
    bus1.stop();
    bus2.stop();
}

#[test]
fn test_name_no_conflict_after_deletion() {
    let requested_name = unique_bus_name("GCTestBus1");
    let weak_ref = {
        let bus1 = EventBus::new(Some(requested_name.clone()));
        Arc::downgrade(&bus1)
    };
    assert!(weak_ref.upgrade().is_none());

    let bus2 = EventBus::new(Some(requested_name.clone()));
    assert_eq!(bus2.name, requested_name);
    bus2.stop();
}

#[test]
fn test_name_no_conflict_with_no_reference() {
    let requested_name = unique_bus_name("GCTestBus2");
    {
        let _ = EventBus::new(Some(requested_name.clone()));
    }

    let bus2 = EventBus::new(Some(requested_name.clone()));
    assert_eq!(bus2.name, requested_name);
    bus2.stop();
}

#[test]
fn test_name_conflict_with_weak_reference_only() {
    let requested_name = unique_bus_name("GCTestBus3");
    let weak_ref = {
        let bus1 = EventBus::new(Some(requested_name.clone()));
        let weak_ref = Arc::downgrade(&bus1);
        assert!(weak_ref.upgrade().is_some());
        weak_ref
    };

    assert!(weak_ref.upgrade().is_none());
    let bus2 = EventBus::new(Some(requested_name.clone()));
    assert_eq!(bus2.name, requested_name);
    bus2.stop();
}

#[test]
fn test_multiple_buses_with_gc() {
    let name1 = unique_bus_name("GCMulti1");
    let name2 = unique_bus_name("GCMulti2");
    let name3 = unique_bus_name("GCMulti3");
    let name4 = unique_bus_name("GCMulti4");
    let bus1 = EventBus::new(Some(name1.clone()));
    {
        let _ = EventBus::new(Some(name2.clone()));
    }
    let bus3 = EventBus::new(Some(name3.clone()));
    {
        let _ = EventBus::new(Some(name4.clone()));
    }

    let bus2_new = EventBus::new(Some(name2.clone()));
    let bus4_new = EventBus::new(Some(name4.clone()));
    assert_eq!(bus2_new.name, name2);
    assert_eq!(bus4_new.name, name4);

    let bus1_conflict = EventBus::new(Some(name1.clone()));
    assert!(bus1_conflict.name.starts_with(&format!("{name1}_")));
    assert_ne!(bus1_conflict.name, bus1.name);

    let bus3_conflict = EventBus::new(Some(name3.clone()));
    assert!(bus3_conflict.name.starts_with(&format!("{name3}_")));
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
    let requested_name = unique_bus_name("GCStopClear");
    let bus1 = EventBus::new(Some(requested_name.clone()));
    bus1.stop();

    let bus2 = EventBus::new(Some(requested_name.clone()));
    assert_eq!(bus2.name, requested_name);
    bus2.stop();
}

#[test]
fn test_weakset_behavior() {
    let bus1 = EventBus::new(Some(unique_bus_name("WeakTest1")));
    let bus2 = EventBus::new(Some(unique_bus_name("WeakTest2")));
    let bus3 = EventBus::new(Some(unique_bus_name("WeakTest3")));
    let bus2_id = bus2.id.clone();
    let weak2 = Arc::downgrade(&bus2);

    assert!(EventBus::all_instances_contains(&bus1));
    assert!(EventBus::all_instances_contains(&bus2));
    assert!(EventBus::all_instances_contains(&bus3));

    drop(bus2);
    assert!(weak2.upgrade().is_none());
    EventBus::all_instances_len();
    assert!(EventBus::live_instance_by_id(&bus2_id).is_none());
    assert!(EventBus::all_instances_contains(&bus1));
    assert!(EventBus::all_instances_contains(&bus3));
    bus1.stop();
    bus3.stop();
}

#[test]
fn test_eventbus_removed_from_weakset() {
    let requested_name = unique_bus_name("GCDeadBus");
    {
        let _ = EventBus::new(Some(requested_name.clone()));
    }

    let bus = EventBus::new(Some(requested_name.clone()));
    assert_eq!(bus.name, requested_name);
    assert!(EventBus::all_instances_contains(&bus));
    bus.stop();
}

#[test]
fn test_concurrent_name_creation() {
    let workers = 8;
    let requested_name = unique_bus_name("ConcurrentTest");
    let barrier = Arc::new(Barrier::new(workers));
    let handles = (0..workers)
        .map(|_| {
            let barrier = barrier.clone();
            let requested_name = requested_name.clone();
            thread::spawn(move || {
                barrier.wait();
                EventBus::new(Some(requested_name))
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
    assert!(unique_names.contains(&requested_name));
    assert!(unique_names.iter().all(|name| {
        name == &requested_name
            || (name.starts_with(&format!("{requested_name}_"))
                && name.len() == requested_name.len() + 1 + 8)
    }));

    for bus in buses {
        bus.stop();
    }
}

#[test]
fn test_unreferenced_buses_with_history_can_be_cleaned_without_instance_leak() {
    let prefix = unique_bus_name("GCNoStopBus");
    let mut refs = Vec::new();
    let mut ids = Vec::new();

    for index in 0..10 {
        let bus = EventBus::new_with_options(
            Some(format!("{prefix}_{index}")),
            EventBusOptions {
                max_history_size: Some(40),
                ..EventBusOptions::default()
            },
        );
        ids.push(bus.id.clone());
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
    EventBus::all_instances_len();
    assert!(ids
        .iter()
        .all(|eventbus_id| EventBus::live_instance_by_id(eventbus_id).is_none()));
}

#[test]
fn test_unreferenced_buses_with_history_are_collected_without_stop() {
    let prefix = unique_bus_name("GCImplicitNoStop");
    let mut refs = Vec::new();
    let mut ids = Vec::new();

    for index in 0..10 {
        let bus = EventBus::new_with_options(
            Some(format!("{prefix}_{index}")),
            EventBusOptions {
                max_history_size: Some(30),
                ..EventBusOptions::default()
            },
        );
        ids.push(bus.id.clone());
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
    EventBus::all_instances_len();
    assert!(ids
        .iter()
        .all(|eventbus_id| EventBus::live_instance_by_id(eventbus_id).is_none()));
}
