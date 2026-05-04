use std::{collections::HashMap, thread, time::Duration};

use abxbus_rust::{
    event_bus::{EventBus, FindOptions},
    typed::{EventSpec, TypedEvent},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Serialize, Deserialize)]
struct EmptyPayload {}
#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}
struct WorkEvent;
impl EventSpec for WorkEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "work";
}
struct FutureEvent;
impl EventSpec for FutureEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "future_event";
}

#[derive(Clone, Serialize, Deserialize)]
struct FilterPayload {
    value: String,
    category: String,
}
struct FilterEvent;
impl EventSpec for FilterEvent {
    type Payload = FilterPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "filter_event";
}
struct OtherFilterEvent;
impl EventSpec for OtherFilterEvent {
    type Payload = FilterPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "other_filter_event";
}

#[test]
fn test_find_past_match_returns_event() {
    let bus = EventBus::new(Some("FindBus".to_string()));
    bus.on("work", "h1", |_event| async move { Ok(json!("ok")) });

    let event = bus.emit::<WorkEvent>(TypedEvent::<WorkEvent>::new(EmptyPayload {}));
    block_on(event.wait_completed());

    let found = block_on(bus.find("work", true, None, None));
    assert!(found.is_some());
    assert_eq!(found.expect("missing").inner.lock().event_type, "work");

    bus.stop();
}

#[test]
fn test_find_future_waits_for_new_event() {
    let bus = EventBus::new(Some("FindFutureBus".to_string()));
    let bus_for_emit = bus.clone();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(30));
        bus_for_emit.emit::<FutureEvent>(TypedEvent::<FutureEvent>::new(EmptyPayload {}));
    });

    let found = block_on(bus.find("future_event", false, Some(0.5), None));
    assert!(found.is_some());
    bus.stop();
}

#[test]
fn test_find_supports_metadata_filters_like_event_status() {
    let bus = EventBus::new(Some("FindMetadataBus".to_string()));
    let event = bus.emit::<FilterEvent>(TypedEvent::new(FilterPayload {
        value: "one".to_string(),
        category: "alpha".to_string(),
    }));
    block_on(event.wait_completed());

    let mut where_filter = HashMap::new();
    where_filter.insert("event_status".to_string(), json!("completed"));
    let found = block_on(bus.find_with_options(
        "filter_event",
        FindOptions {
            past: true,
            where_filter: Some(where_filter),
            ..FindOptions::default()
        },
    ));

    assert!(found.is_some());
    let found_id = found.unwrap().inner.lock().event_id.clone();
    let event_id = event.inner.inner.lock().event_id.clone();
    assert_eq!(found_id, event_id);
    bus.stop();
}

#[test]
fn test_find_supports_non_event_data_field_equality_filters() {
    let bus = EventBus::new(Some("FindPayloadBus".to_string()));
    let _old = bus.emit::<FilterEvent>(TypedEvent::new(FilterPayload {
        value: "one".to_string(),
        category: "alpha".to_string(),
    }));
    let target = bus.emit::<FilterEvent>(TypedEvent::new(FilterPayload {
        value: "two".to_string(),
        category: "beta".to_string(),
    }));
    block_on(bus.wait_until_idle(None));

    let mut where_filter = HashMap::new();
    where_filter.insert("value".to_string(), json!("two"));
    where_filter.insert("category".to_string(), json!("beta"));
    let found = block_on(bus.find_with_options(
        "filter_event",
        FindOptions {
            past: true,
            where_filter: Some(where_filter),
            ..FindOptions::default()
        },
    ))
    .expect("expected payload match");

    let found_id = found.inner.lock().event_id.clone();
    let target_id = target.inner.inner.lock().event_id.clone();
    assert_eq!(found_id, target_id);
    bus.stop();
}

#[test]
fn test_find_where_filter_works_with_future_waiting() {
    let bus = EventBus::new(Some("FindFutureWhereBus".to_string()));
    let bus_for_emit = bus.clone();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        bus_for_emit.emit::<FilterEvent>(TypedEvent::new(FilterPayload {
            value: "wrong".to_string(),
            category: "alpha".to_string(),
        }));
        thread::sleep(Duration::from_millis(20));
        bus_for_emit.emit::<FilterEvent>(TypedEvent::new(FilterPayload {
            value: "right".to_string(),
            category: "alpha".to_string(),
        }));
    });

    let mut where_filter = HashMap::new();
    where_filter.insert("value".to_string(), json!("right"));
    let found = block_on(bus.find_with_options(
        "filter_event",
        FindOptions {
            past: false,
            future: Some(0.5),
            where_filter: Some(where_filter),
            ..FindOptions::default()
        },
    ))
    .expect("expected future filtered event");

    assert_eq!(
        found.inner.lock().payload.get("value"),
        Some(&json!("right"))
    );
    bus.stop();
}

#[test]
fn test_find_wildcard_with_where_filter_matches_across_event_types_in_history() {
    let bus = EventBus::new(Some("FindWildcardWhereBus".to_string()));
    bus.emit::<FilterEvent>(TypedEvent::new(FilterPayload {
        value: "same".to_string(),
        category: "alpha".to_string(),
    }));
    let target = bus.emit::<OtherFilterEvent>(TypedEvent::new(FilterPayload {
        value: "same".to_string(),
        category: "beta".to_string(),
    }));
    block_on(bus.wait_until_idle(None));

    let mut where_filter = HashMap::new();
    where_filter.insert("category".to_string(), json!("beta"));
    let found = block_on(bus.find_with_options(
        "*",
        FindOptions {
            past: true,
            where_filter: Some(where_filter),
            ..FindOptions::default()
        },
    ))
    .expect("expected wildcard where match");

    let found_id = found.inner.lock().event_id.clone();
    let target_id = target.inner.inner.lock().event_id.clone();
    assert_eq!(found_id, target_id);
    bus.stop();
}
