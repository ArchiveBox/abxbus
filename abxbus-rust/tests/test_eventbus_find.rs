use std::{
    collections::HashMap,
    sync::{mpsc, Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use abxbus_rust::{
    event_bus::{EventBus, FindOptions},
    typed::{EventSpec, TypedEvent},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;

fn wait_for_string(slot: &Arc<Mutex<Option<String>>>) -> String {
    let start = Instant::now();
    loop {
        if let Some(value) = slot.lock().expect("slot lock").clone() {
            return value;
        }
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "timed out waiting for value"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

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
struct ParentEvent;
impl EventSpec for ParentEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "parent";
}
struct ChildEvent;
impl EventSpec for ChildEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "child";
}
struct GrandchildEvent;
impl EventSpec for GrandchildEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "grandchild";
}
struct UnrelatedEvent;
impl EventSpec for UnrelatedEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "unrelated";
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
struct SystemEvent;
impl EventSpec for SystemEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "SystemEvent";
}
struct UserActionEvent;
impl EventSpec for UserActionEvent {
    type Payload = FilterPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "UserActionEvent";
}
#[derive(Clone, Serialize, Deserialize)]
struct NavigatePayload {
    url: String,
}
struct NavigateEvent;
impl EventSpec for NavigateEvent {
    type Payload = NavigatePayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "navigate";
}
#[derive(Clone, Serialize, Deserialize)]
struct TabPayload {
    tab_id: String,
}
struct TabCreatedEvent;
impl EventSpec for TabCreatedEvent {
    type Payload = TabPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "tab_created";
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
fn test_find_past_returns_most_recent_dispatched_event() {
    let bus = EventBus::new(Some("FindPastMostRecentBus".to_string()));
    bus.on("work", "complete", |_event| async move { Ok(json!("ok")) });

    let first = bus.emit::<WorkEvent>(TypedEvent::<WorkEvent>::new(EmptyPayload {}));
    block_on(first.wait_completed());
    thread::sleep(Duration::from_millis(20));
    let second = bus.emit::<WorkEvent>(TypedEvent::<WorkEvent>::new(EmptyPayload {}));
    block_on(second.wait_completed());

    let found = block_on(bus.find("work", true, None, None)).expect("most recent event");
    let found_id = found.inner.lock().event_id.clone();
    let second_id = second.inner.inner.lock().event_id.clone();
    assert_eq!(found_id, second_id);
    bus.stop();
}

#[test]
fn test_find_past_returns_null_when_no_matching_event_exists() {
    let bus = EventBus::new(Some("FindPastNoneBus".to_string()));

    let start = Instant::now();
    let found = block_on(bus.find("work", true, None, None));

    assert!(found.is_none());
    assert!(start.elapsed() < Duration::from_millis(100));
    bus.stop();
}

#[test]
fn test_find_past_history_lookup_is_bus_scoped() {
    let bus_a = EventBus::new(Some("FindScopeA".to_string()));
    let bus_b = EventBus::new(Some("FindScopeB".to_string()));
    bus_b.on(
        "work",
        "complete",
        |_event| async move { Ok(json!("done")) },
    );

    let event_on_b = bus_b.emit::<WorkEvent>(TypedEvent::<WorkEvent>::new(EmptyPayload {}));

    let found_on_a = block_on(bus_a.find("work", true, None, None));
    let found_on_b = block_on(bus_b.find("work", true, None, None));

    assert!(found_on_a.is_none());
    let found_id = found_on_b
        .expect("bus b event")
        .inner
        .lock()
        .event_id
        .clone();
    let emitted_id = event_on_b.inner.inner.lock().event_id.clone();
    assert_eq!(found_id, emitted_id);
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_find_past_result_retains_origin_bus_label_in_event_path() {
    let bus = EventBus::new(Some("FindOriginBus".to_string()));

    let event = bus.emit::<WorkEvent>(TypedEvent::<WorkEvent>::new(EmptyPayload {}));
    block_on(event.wait_completed());

    let found = block_on(bus.find("work", true, None, None)).expect("found event");
    assert_eq!(found.inner.lock().event_path.first(), Some(&bus.label()));
    bus.stop();
}

#[test]
fn test_find_past_float_filters_by_time_window() {
    let bus = EventBus::new(Some("FindPastFloatBus".to_string()));
    bus.on("work", "complete", |_event| async move { Ok(json!("ok")) });

    let old_event = bus.emit::<WorkEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(old_event.wait_completed());
    old_event.inner.inner.lock().event_created_at = "2020-01-01T00:00:00.000Z".to_string();

    let stale = block_on(bus.find_with_options(
        "work",
        FindOptions {
            past: true,
            past_window: Some(0.01),
            ..FindOptions::default()
        },
    ));
    assert!(stale.is_none());

    let fresh_event = bus.emit::<WorkEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(fresh_event.wait_completed());

    let fresh = block_on(bus.find_with_options(
        "work",
        FindOptions {
            past: true,
            past_window: Some(1.0),
            ..FindOptions::default()
        },
    ))
    .expect("fresh event should be within window");
    let found_id = fresh.inner.lock().event_id.clone();
    let fresh_event_id = fresh_event.inner.inner.lock().event_id.clone();
    assert_eq!(found_id, fresh_event_id);
    bus.stop();
}

#[test]
fn test_find_past_returns_null_when_all_events_are_too_old() {
    let bus = EventBus::new(Some("FindTooOldBus".to_string()));
    bus.on("work", "complete", |_event| async move { Ok(json!("ok")) });

    let old_event = bus.emit::<WorkEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(old_event.wait_completed());
    old_event.inner.inner.lock().event_created_at = "2020-01-01T00:00:00.000Z".to_string();

    let found = block_on(bus.find_with_options(
        "work",
        FindOptions {
            past: true,
            past_window: Some(0.05),
            ..FindOptions::default()
        },
    ));
    assert!(found.is_none());
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
fn test_find_future_works_with_string_event_keys() {
    let bus = EventBus::new(Some("FindFutureStringBus".to_string()));
    let bus_for_emit = bus.clone();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(30));
        bus_for_emit.emit::<WorkEvent>(TypedEvent::<WorkEvent>::new(EmptyPayload {}));
    });

    let found = block_on(bus.find("work", false, Some(0.5), None)).expect("future event");
    assert_eq!(found.inner.lock().event_type, "work");
    bus.stop();
}

#[test]
fn test_max_history_size_zero_disables_past_history_search_but_future_find_still_resolves() {
    let bus = EventBus::new_with_history(Some("FindZeroHistoryBus".to_string()), Some(0), false);
    let bus_for_find = bus.clone();
    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
        let found = block_on(bus_for_find.find("work", false, Some(0.5), None));
        tx.send(found.map(|event| event.inner.lock().event_id.clone()))
            .expect("send found event");
    });
    thread::sleep(Duration::from_millis(20));

    let dispatched = bus.emit::<WorkEvent>(TypedEvent::<WorkEvent>::new(EmptyPayload {}));
    let future_id = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("future find should resolve")
        .expect("found future event");
    assert_eq!(future_id, dispatched.inner.inner.lock().event_id);

    block_on(dispatched.wait_completed());
    assert_eq!(bus.event_history_size(), 0);
    assert!(block_on(bus.find("work", true, None, None)).is_none());
    bus.stop();
}

#[test]
fn test_find_defaults_to_past_true_future_false_when_both_are_undefined() {
    let bus = EventBus::new(Some("FindDefaultWindowBus".to_string()));

    let start = Instant::now();
    let missing = block_on(bus.find("work", true, None, None));
    assert!(missing.is_none());
    assert!(start.elapsed() < Duration::from_millis(100));

    let dispatched = bus.emit::<WorkEvent>(TypedEvent::<WorkEvent>::new(EmptyPayload {}));
    let found = block_on(bus.find("work", true, None, None)).expect("past event");
    let found_id = found.inner.lock().event_id.clone();
    let dispatched_id = dispatched.inner.inner.lock().event_id.clone();
    assert_eq!(found_id, dispatched_id);
    bus.stop();
}

#[test]
fn test_find_future_ignores_past_events() {
    let bus = EventBus::new(Some("FindFutureIgnoresPastBus".to_string()));

    let prior = bus.emit::<WorkEvent>(TypedEvent::<WorkEvent>::new(EmptyPayload {}));
    block_on(prior.wait_completed());

    let found = block_on(bus.find("work", false, Some(0.05), None));
    assert!(found.is_none());
    bus.stop();
}

#[test]
fn test_find_future_ignores_already_dispatched_in_flight_events_when_past_false() {
    let bus = EventBus::new(Some("FindFutureIgnoresInflightBus".to_string()));

    bus.on("work", "slow", |_event| async move {
        thread::sleep(Duration::from_millis(80));
        Ok(json!("done"))
    });

    let inflight = bus.emit::<WorkEvent>(TypedEvent::<WorkEvent>::new(EmptyPayload {}));
    thread::sleep(Duration::from_millis(5));

    let found = block_on(bus.find("work", false, Some(0.05), None));
    assert!(found.is_none());

    block_on(inflight.wait_completed());
    bus.stop();
}

#[test]
fn test_find_future_times_out_when_no_event_arrives() {
    let bus = EventBus::new(Some("FindFutureTimeoutBus".to_string()));

    let start = Instant::now();
    let found = block_on(bus.find("work", false, Some(0.05), None));

    assert!(found.is_none());
    assert!(start.elapsed() >= Duration::from_millis(30));
    bus.stop();
}

#[test]
fn test_find_past_false_future_false_returns_null_immediately() {
    let bus = EventBus::new(Some("FindNeitherBus".to_string()));

    let start = Instant::now();
    let found = block_on(bus.find("work", false, None, None));

    assert!(found.is_none());
    assert!(start.elapsed() < Duration::from_millis(100));
    bus.stop();
}

#[test]
fn test_find_past_future_returns_past_event_immediately() {
    let bus = EventBus::new(Some("FindPastFutureBus".to_string()));
    bus.on(
        "work",
        "complete",
        |_event| async move { Ok(json!("done")) },
    );

    let dispatched = bus.emit::<WorkEvent>(TypedEvent::<WorkEvent>::new(EmptyPayload {}));

    let start = Instant::now();
    let found = block_on(bus.find("work", true, Some(0.5), None)).expect("past event");

    let found_id = found.inner.lock().event_id.clone();
    let dispatched_id = dispatched.inner.inner.lock().event_id.clone();
    assert_eq!(found_id, dispatched_id);
    assert!(start.elapsed() < Duration::from_millis(100));
    bus.stop();
}

#[test]
fn test_find_past_future_waits_for_future_when_no_past_match() {
    let bus = EventBus::new(Some("FindPastFutureWaitBus".to_string()));
    let bus_for_emit = bus.clone();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(30));
        bus_for_emit.emit::<FutureEvent>(TypedEvent::<FutureEvent>::new(EmptyPayload {}));
    });

    let found = block_on(bus.find("future_event", true, Some(0.5), None));
    assert!(found.is_some());
    assert_eq!(found.unwrap().inner.lock().event_type, "future_event");
    bus.stop();
}

#[test]
fn test_find_past_future_windows_are_independent() {
    let bus = EventBus::new(Some("FindPastFutureWindowBus".to_string()));
    bus.on("work", "complete", |_event| async move { Ok(json!("ok")) });
    let bus_for_emit = bus.clone();

    let old_event = bus.emit::<WorkEvent>(TypedEvent::<WorkEvent>::new(EmptyPayload {}));
    block_on(old_event.wait_completed());
    old_event.inner.inner.lock().event_created_at = "2020-01-01T00:00:00.000Z".to_string();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(30));
        bus_for_emit.emit::<WorkEvent>(TypedEvent::<WorkEvent>::new(EmptyPayload {}));
    });

    let found = block_on(bus.find_with_options(
        "work",
        FindOptions {
            past: true,
            past_window: Some(0.01),
            future: Some(0.5),
            ..FindOptions::default()
        },
    ))
    .expect("future event should resolve when past window excludes old event");
    let found_id = found.inner.lock().event_id.clone();
    let old_id = old_event.inner.inner.lock().event_id.clone();
    assert_ne!(found_id, old_id);
    bus.stop();
}

#[test]
fn test_find_past_true_future_float_returns_old_event_immediately() {
    let bus = EventBus::new(Some("FindPastTrueFutureFloatBus".to_string()));
    let dispatched = bus.emit::<WorkEvent>(TypedEvent::<WorkEvent>::new(EmptyPayload {}));
    block_on(dispatched.wait_completed());

    let start = Instant::now();
    let found = block_on(bus.find("work", true, Some(0.5), None)).expect("past event");

    let found_id = found.inner.lock().event_id.clone();
    let dispatched_id = dispatched.inner.inner.lock().event_id.clone();
    assert_eq!(found_id, dispatched_id);
    assert!(start.elapsed() < Duration::from_millis(100));
    bus.stop();
}

#[test]
fn test_find_past_float_future_waits_for_new_event() {
    let bus = EventBus::new(Some("FindPastFloatFutureBus".to_string()));
    let bus_for_emit = bus.clone();

    let old_event = bus.emit::<WorkEvent>(TypedEvent::<WorkEvent>::new(EmptyPayload {}));
    block_on(old_event.wait_completed());
    old_event.inner.inner.lock().event_created_at = "2020-01-01T00:00:00.000Z".to_string();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(30));
        bus_for_emit.emit::<WorkEvent>(TypedEvent::<WorkEvent>::new(EmptyPayload {}));
    });

    let found = block_on(bus.find_with_options(
        "work",
        FindOptions {
            past: true,
            past_window: Some(0.01),
            future: Some(0.5),
            ..FindOptions::default()
        },
    ))
    .expect("future event");
    let found_id = found.inner.lock().event_id.clone();
    let old_id = old_event.inner.inner.lock().event_id.clone();
    assert_ne!(found_id, old_id);
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
fn test_find_supports_metadata_equality_filters_like_event_id_and_event_timeout() {
    let bus = EventBus::new(Some("FindEventFieldFilterBus".to_string()));

    let event_a = TypedEvent::<WorkEvent>::new(EmptyPayload {});
    event_a.inner.inner.lock().event_timeout = Some(11.0);
    let event_a = bus.emit(event_a);
    let event_b = TypedEvent::<WorkEvent>::new(EmptyPayload {});
    event_b.inner.inner.lock().event_timeout = Some(22.0);
    let event_b = bus.emit(event_b);
    block_on(event_a.wait_completed());
    block_on(event_b.wait_completed());

    let event_a_id = event_a.inner.inner.lock().event_id.clone();
    let found_a = block_on(bus.find_with_options(
        "work",
        FindOptions {
            past: true,
            where_filter: Some(HashMap::from([
                ("event_id".to_string(), json!(event_a_id.clone())),
                ("event_timeout".to_string(), json!(11.0)),
            ])),
            ..FindOptions::default()
        },
    ))
    .expect("event_a should match metadata filter");
    assert_eq!(found_a.inner.lock().event_id, event_a_id);

    let mismatch = block_on(bus.find_with_options(
        "work",
        FindOptions {
            past: true,
            where_filter: Some(HashMap::from([
                ("event_id".to_string(), json!(event_a_id)),
                ("event_timeout".to_string(), json!(22.0)),
            ])),
            ..FindOptions::default()
        },
    ));
    assert!(mismatch.is_none());
    bus.stop();
}

#[test]
fn test_find_respects_where_filter() {
    let bus = EventBus::new(Some("FindWhereBus".to_string()));
    bus.emit::<FilterEvent>(TypedEvent::new(FilterPayload {
        value: "wrong".to_string(),
        category: "alpha".to_string(),
    }));
    let target = bus.emit::<FilterEvent>(TypedEvent::new(FilterPayload {
        value: "right".to_string(),
        category: "beta".to_string(),
    }));
    block_on(bus.wait_until_idle(None));

    let found = block_on(bus.find_with_options(
        "filter_event",
        FindOptions {
            past: true,
            where_filter: Some(HashMap::from([
                ("value".to_string(), json!("right")),
                ("category".to_string(), json!("beta")),
            ])),
            ..FindOptions::default()
        },
    ))
    .expect("where-filtered event");

    let found_id = found.inner.lock().event_id.clone();
    let target_id = target.inner.inner.lock().event_id.clone();
    assert_eq!(found_id, target_id);
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
fn test_find_wildcard() {
    let bus = EventBus::new(Some("FindWildcardBus".to_string()));
    let first = bus.emit::<SystemEvent>(TypedEvent::new(EmptyPayload {}));
    thread::sleep(Duration::from_millis(5));
    let second = bus.emit::<UserActionEvent>(TypedEvent::new(FilterPayload {
        value: "clicked".to_string(),
        category: "user".to_string(),
    }));
    block_on(bus.wait_until_idle(None));

    let found = block_on(bus.find("*", true, None, None)).expect("wildcard match");
    let found_id = found.inner.lock().event_id.clone();
    let second_id = second.inner.inner.lock().event_id.clone();
    let first_id = first.inner.inner.lock().event_id.clone();
    assert_eq!(found_id, second_id);
    assert_ne!(found_id, first_id);
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

#[test]
fn test_find_with_past_float_and_where_filter() {
    let bus = EventBus::new(Some("FindPastFloatWhereBus".to_string()));
    let old = bus.emit::<FilterEvent>(TypedEvent::new(FilterPayload {
        value: "target".to_string(),
        category: "old".to_string(),
    }));
    block_on(old.wait_completed());
    old.inner.inner.lock().event_created_at = "2020-01-01T00:00:00.000Z".to_string();
    let fresh = bus.emit::<FilterEvent>(TypedEvent::new(FilterPayload {
        value: "target".to_string(),
        category: "fresh".to_string(),
    }));
    block_on(fresh.wait_completed());

    let found = block_on(bus.find_with_options(
        "filter_event",
        FindOptions {
            past: true,
            past_window: Some(1.0),
            where_filter: Some(HashMap::from([("value".to_string(), json!("target"))])),
            ..FindOptions::default()
        },
    ))
    .expect("fresh filtered event");
    let found_id = found.inner.lock().event_id.clone();
    let fresh_id = fresh.inner.inner.lock().event_id.clone();
    assert_eq!(found_id, fresh_id);
    bus.stop();
}

#[test]
fn test_find_wildcard_with_where_filter_works_for_future_waiting() {
    let bus = EventBus::new(Some("FindWildcardFutureBus".to_string()));
    let bus_for_emit = bus.clone();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(40));
        bus_for_emit.emit::<SystemEvent>(TypedEvent::new(EmptyPayload {}));
        bus_for_emit.emit::<UserActionEvent>(TypedEvent::new(FilterPayload {
            value: "normal".to_string(),
            category: "user".to_string(),
        }));
        bus_for_emit.emit::<UserActionEvent>(TypedEvent::new(FilterPayload {
            value: "special".to_string(),
            category: "user".to_string(),
        }));
    });

    let found = block_on(bus.find_with_options(
        "*",
        FindOptions {
            past: false,
            future: Some(0.5),
            where_filter: Some(HashMap::from([("value".to_string(), json!("special"))])),
            ..FindOptions::default()
        },
    ))
    .expect("future wildcard match");

    assert_eq!(found.inner.lock().event_type, "UserActionEvent");
    assert_eq!(
        found.inner.lock().payload.get("value"),
        Some(&json!("special"))
    );
    bus.stop();
}

#[test]
fn test_find_with_multiple_concurrent_waiters_resolves_correct_events() {
    let bus = EventBus::new(Some("FindConcurrentBus".to_string()));
    let bus_normal = bus.clone();
    let bus_special = bus.clone();
    let bus_system = bus.clone();
    let (tx, rx) = mpsc::channel();

    let tx_normal = tx.clone();
    thread::spawn(move || {
        let found = block_on(bus_normal.find_with_options(
            "UserActionEvent",
            FindOptions {
                past: false,
                future: Some(0.5),
                where_filter: Some(HashMap::from([("value".to_string(), json!("normal"))])),
                ..FindOptions::default()
            },
        ))
        .expect("normal event");
        tx_normal
            .send(("normal".to_string(), found.inner.lock().event_id.clone()))
            .expect("send normal");
    });

    let tx_special = tx.clone();
    thread::spawn(move || {
        let found = block_on(bus_special.find_with_options(
            "UserActionEvent",
            FindOptions {
                past: false,
                future: Some(0.5),
                where_filter: Some(HashMap::from([("value".to_string(), json!("special"))])),
                ..FindOptions::default()
            },
        ))
        .expect("special event");
        tx_special
            .send(("special".to_string(), found.inner.lock().event_id.clone()))
            .expect("send special");
    });

    let tx_system = tx.clone();
    thread::spawn(move || {
        let found =
            block_on(bus_system.find("SystemEvent", false, Some(0.5), None)).expect("system event");
        tx_system
            .send(("system".to_string(), found.inner.lock().event_id.clone()))
            .expect("send system");
    });

    thread::sleep(Duration::from_millis(50));
    let normal = bus.emit::<UserActionEvent>(TypedEvent::new(FilterPayload {
        value: "normal".to_string(),
        category: "user".to_string(),
    }));
    let system = bus.emit::<SystemEvent>(TypedEvent::new(EmptyPayload {}));
    let special = bus.emit::<UserActionEvent>(TypedEvent::new(FilterPayload {
        value: "special".to_string(),
        category: "user".to_string(),
    }));

    let mut resolved = HashMap::new();
    for _ in 0..3 {
        let (label, event_id) = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter should resolve");
        resolved.insert(label, event_id);
    }

    assert_eq!(
        resolved.get("normal"),
        Some(&normal.inner.inner.lock().event_id.clone())
    );
    assert_eq!(
        resolved.get("system"),
        Some(&system.inner.inner.lock().event_id.clone())
    );
    assert_eq!(
        resolved.get("special"),
        Some(&special.inner.inner.lock().event_id.clone())
    );
    bus.stop();
}

#[test]
fn test_find_child_of_returns_child_event() {
    let bus = EventBus::new(Some("FindChildBus".to_string()));
    let bus_for_parent = bus.clone();
    let child_id = Arc::new(Mutex::new(None::<String>));
    let child_id_for_parent = child_id.clone();

    bus.on("parent", "emit_child", move |_event| {
        let bus = bus_for_parent.clone();
        let child_id = child_id_for_parent.clone();
        async move {
            let child = bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            *child_id.lock().expect("child id lock") =
                Some(child.inner.inner.lock().event_id.clone());
            Ok(json!("parent"))
        }
    });
    bus.on("child", "complete_child", |_event| async move {
        Ok(json!("child"))
    });

    let parent = bus.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    let emitted_child_id = wait_for_string(&child_id);

    let child =
        block_on(bus.find("child", true, None, Some(parent.inner.clone()))).expect("child event");
    let found_child_id = child.inner.lock().event_id.clone();
    assert_eq!(found_child_id, emitted_child_id);
    assert_eq!(
        child.inner.lock().event_parent_id.as_deref(),
        Some(parent.inner.inner.lock().event_id.as_str())
    );
    bus.stop();
}

#[test]
fn test_find_child_of_returns_null_for_non_child() {
    let bus = EventBus::new(Some("FindNonChildBus".to_string()));

    let parent = bus.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    let unrelated = bus.emit::<UnrelatedEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(bus.wait_until_idle(Some(2.0)));

    let found = block_on(bus.find("unrelated", true, None, Some(parent.inner.clone())));
    assert!(found.is_none());
    assert_ne!(
        unrelated.inner.inner.lock().event_parent_id.as_deref(),
        Some(parent.inner.inner.lock().event_id.as_str())
    );
    bus.stop();
}

#[test]
fn test_find_child_of_returns_grandchild_event() {
    let bus = EventBus::new(Some("FindGrandchildBus".to_string()));
    let bus_for_parent = bus.clone();
    let bus_for_child = bus.clone();
    let child_id = Arc::new(Mutex::new(None::<String>));
    let child_id_for_parent = child_id.clone();

    bus.on("parent", "emit_child", move |_event| {
        let bus = bus_for_parent.clone();
        let child_id = child_id_for_parent.clone();
        async move {
            let child = bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            *child_id.lock().expect("child id lock") =
                Some(child.inner.inner.lock().event_id.clone());
            child.wait_completed().await;
            Ok(json!("parent"))
        }
    });
    bus.on("child", "emit_grandchild", move |_event| {
        let bus = bus_for_child.clone();
        async move {
            let grandchild = bus.emit_child::<GrandchildEvent>(TypedEvent::new(EmptyPayload {}));
            grandchild.wait_completed().await;
            Ok(json!("child"))
        }
    });
    bus.on("grandchild", "complete_grandchild", |_event| async move {
        Ok(json!("grandchild"))
    });

    let parent = bus.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(parent.wait_completed());

    let grandchild = block_on(bus.find("grandchild", true, None, Some(parent.inner.clone())))
        .expect("grandchild event");
    assert_eq!(
        grandchild.inner.lock().event_parent_id,
        child_id.lock().expect("child id lock").clone()
    );
    bus.stop();
}

#[test]
fn test_find_with_child_of_and_past_float() {
    let bus = EventBus::new(Some("FindChildPastFloatBus".to_string()));
    let bus_for_parent = bus.clone();

    bus.on("parent", "emit_child", move |_event| {
        let bus = bus_for_parent.clone();
        async move {
            let child = bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            child.wait_completed().await;
            Ok(json!("parent"))
        }
    });
    bus.on("child", "complete_child", |_event| async move {
        Ok(json!("child"))
    });

    let parent = bus.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(parent.wait_completed());

    let found = block_on(bus.find_with_options(
        "child",
        FindOptions {
            past: true,
            past_window: Some(1.0),
            child_of: Some(parent.inner.clone()),
            ..FindOptions::default()
        },
    ))
    .expect("child should be within past window");
    assert_eq!(
        found.inner.lock().event_parent_id.as_deref(),
        Some(parent.inner.inner.lock().event_id.as_str())
    );

    found.inner.lock().event_created_at = "2020-01-01T00:00:00.000Z".to_string();
    let stale = block_on(bus.find_with_options(
        "child",
        FindOptions {
            past: true,
            past_window: Some(0.01),
            child_of: Some(parent.inner.clone()),
            ..FindOptions::default()
        },
    ));
    assert!(stale.is_none());
    bus.stop();
}

#[test]
fn test_find_child_of_filters_to_correct_parent_among_siblings() {
    let bus = EventBus::new(Some("FindCorrectParentBus".to_string()));
    let bus_for_nav = bus.clone();

    bus.on("navigate", "create_tab", move |event| {
        let bus = bus_for_nav.clone();
        async move {
            let url = event
                .inner
                .lock()
                .payload
                .get("url")
                .and_then(|value| value.as_str())
                .expect("url")
                .to_string();
            let child = bus.emit_child::<TabCreatedEvent>(TypedEvent::new(TabPayload {
                tab_id: format!("tab_for_{url}"),
            }));
            child.wait_completed().await;
            Ok(json!("nav"))
        }
    });
    bus.on("tab_created", "complete_tab", |_event| async move {
        Ok(json!("tab"))
    });

    let nav_1 = bus.emit::<NavigateEvent>(TypedEvent::new(NavigatePayload {
        url: "site1".to_string(),
    }));
    let nav_2 = bus.emit::<NavigateEvent>(TypedEvent::new(NavigatePayload {
        url: "site2".to_string(),
    }));
    block_on(nav_1.wait_completed());
    block_on(nav_2.wait_completed());

    let tab_1 =
        block_on(bus.find("tab_created", true, None, Some(nav_1.inner.clone()))).expect("tab 1");
    let tab_2 =
        block_on(bus.find("tab_created", true, None, Some(nav_2.inner.clone()))).expect("tab 2");

    assert_eq!(
        tab_1.inner.lock().payload.get("tab_id"),
        Some(&json!("tab_for_site1"))
    );
    assert_eq!(
        tab_2.inner.lock().payload.get("tab_id"),
        Some(&json!("tab_for_site2"))
    );
    bus.stop();
}

#[test]
fn test_find_future_with_child_of_waits_for_matching_child() {
    let bus = EventBus::new(Some("FindFutureChildBus".to_string()));
    let bus_for_parent = bus.clone();

    bus.on("parent", "delayed_child", move |_event| {
        let bus = bus_for_parent.clone();
        async move {
            thread::sleep(Duration::from_millis(30));
            let child = bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            child.wait_completed().await;
            Ok(json!("parent"))
        }
    });
    bus.on("child", "complete_child", |_event| async move {
        Ok(json!("child"))
    });

    let parent = bus.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    let child = block_on(bus.find("child", false, Some(0.5), Some(parent.inner.clone())))
        .expect("future child");

    assert_eq!(
        child.inner.lock().event_parent_id.as_deref(),
        Some(parent.inner.inner.lock().event_id.as_str())
    );
    block_on(parent.wait_completed());
    bus.stop();
}

#[test]
fn test_find_catches_child_event_that_fired_during_parent_handler() {
    let bus = EventBus::new(Some("FindRaceConditionBus".to_string()));
    let bus_for_nav = bus.clone();
    let tab_event_id = Arc::new(Mutex::new(None::<String>));
    let tab_event_id_for_nav = tab_event_id.clone();

    bus.on("navigate", "create_tab", move |_event| {
        let bus = bus_for_nav.clone();
        let tab_event_id = tab_event_id_for_nav.clone();
        async move {
            let tab = bus.emit_child::<TabCreatedEvent>(TypedEvent::new(TabPayload {
                tab_id: "06bee4cf-9f51-7e5d-82d3-65f35169329c".to_string(),
            }));
            *tab_event_id.lock().expect("tab id lock") =
                Some(tab.inner.inner.lock().event_id.clone());
            tab.wait_completed().await;
            Ok(json!("nav"))
        }
    });
    bus.on("tab_created", "complete_tab", |_event| async move {
        Ok(json!("tab"))
    });

    let nav = bus.emit::<NavigateEvent>(TypedEvent::new(NavigatePayload {
        url: "https://example.com".to_string(),
    }));
    block_on(nav.wait_completed());
    let emitted_tab_id = wait_for_string(&tab_event_id);

    let found_tab =
        block_on(bus.find("tab_created", true, None, Some(nav.inner.clone()))).expect("found tab");
    let found_tab_id = found_tab.inner.lock().event_id.clone();
    assert_eq!(found_tab_id, emitted_tab_id);
    bus.stop();
}

#[test]
fn test_find_past_includes_in_progress_dispatched_events() {
    let bus = EventBus::new(Some("FindDispatchedPastBus".to_string()));

    bus.on("work", "slow", |_event| async move {
        thread::sleep(Duration::from_millis(80));
        Ok(json!("done"))
    });

    let dispatched = bus.emit::<WorkEvent>(TypedEvent::<WorkEvent>::new(EmptyPayload {}));
    thread::sleep(Duration::from_millis(10));

    let found = block_on(bus.find("work", true, None, None)).expect("in-progress event");
    let found_id = found.inner.lock().event_id.clone();
    let dispatched_id = dispatched.inner.inner.lock().event_id.clone();
    assert_eq!(found_id, dispatched_id);
    let found_status = found.inner.lock().event_status;
    assert_ne!(found_status, abxbus_rust::types::EventStatus::Completed);

    block_on(dispatched.wait_completed());
    bus.stop();
}

#[test]
fn test_find_future_resolves_on_dispatch_before_completion() {
    let bus = EventBus::new(Some("FindOnDispatchBus".to_string()));
    let bus_for_emit = bus.clone();

    bus.on("work", "slow", |_event| async move {
        thread::sleep(Duration::from_millis(80));
        Ok(json!("done"))
    });

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        bus_for_emit.emit::<WorkEvent>(TypedEvent::<WorkEvent>::new(EmptyPayload {}));
    });

    let found =
        block_on(bus.find("work", false, Some(0.5), None)).expect("future dispatched event");
    assert_eq!(
        found.inner.lock().event_status,
        abxbus_rust::types::EventStatus::Pending
    );

    block_on(found.event_completed());
    assert_eq!(
        found.inner.lock().event_status,
        abxbus_rust::types::EventStatus::Completed
    );
    bus.stop();
}

#[test]
fn test_find_with_all_parameters_combined() {
    let bus = EventBus::new(Some("FindAllParamsBus".to_string()));
    let bus_for_parent = bus.clone();
    let child_id = Arc::new(Mutex::new(None::<String>));
    let child_id_for_parent = child_id.clone();

    bus.on("parent", "emit_child", move |_event| {
        let bus = bus_for_parent.clone();
        let child_id = child_id_for_parent.clone();
        async move {
            let child = bus.emit_child::<FilterEvent>(TypedEvent::new(FilterPayload {
                value: "target-child".to_string(),
                category: "screenshot".to_string(),
            }));
            *child_id.lock().expect("child id lock") =
                Some(child.inner.inner.lock().event_id.clone());
            child.wait_completed().await;
            Ok(json!("parent"))
        }
    });
    bus.on("filter_event", "complete_child", |_event| async move {
        Ok(json!("child"))
    });

    let parent = bus.emit::<ParentEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(parent.wait_completed());
    let expected_child_id = child_id
        .lock()
        .expect("child id lock")
        .clone()
        .expect("captured child id");

    let found = block_on(bus.find_with_options(
        "filter_event",
        FindOptions {
            past: true,
            past_window: Some(5.0),
            future: None,
            child_of: Some(parent.inner.clone()),
            where_filter: Some(HashMap::from([(
                "value".to_string(),
                json!("target-child"),
            )])),
        },
    ))
    .expect("combined find match");

    assert_eq!(found.inner.lock().event_id, expected_child_id);
    bus.stop();
}
