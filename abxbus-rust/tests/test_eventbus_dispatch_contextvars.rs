use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use abxbus_rust::{
    event_bus::{EventBus, EventBusOptions},
    typed::{EventSpec, TypedEvent},
    types::EventHandlerConcurrencyMode,
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Serialize, Deserialize)]
struct EmptyPayload {}

struct SimpleEvent;
impl EventSpec for SimpleEvent {
    type Payload = EmptyPayload;
    type Result = String;
    const EVENT_TYPE: &'static str = "SimpleEvent";
}

struct ChildEvent;
impl EventSpec for ChildEvent {
    type Payload = EmptyPayload;
    type Result = String;
    const EVENT_TYPE: &'static str = "ChildEvent";
}

struct Level2Event;
impl EventSpec for Level2Event {
    type Payload = EmptyPayload;
    type Result = String;
    const EVENT_TYPE: &'static str = "Level2Event";
}

struct Level3Event;
impl EventSpec for Level3Event {
    type Payload = EmptyPayload;
    type Result = String;
    const EVENT_TYPE: &'static str = "Level3Event";
}

fn context(entries: &[(&str, &str)]) -> HashMap<String, Value> {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_string(), json!(value)))
        .collect()
}

fn context_str(key: &str) -> String {
    EventBus::context_get(key)
        .and_then(|value| value.as_str().map(ToString::to_string))
        .unwrap_or_else(|| "<unset>".to_string())
}

#[test]
fn test_contextvar_propagates_to_handler() {
    EventBus::context_clear();
    let bus = EventBus::new(Some("ContextTestBus".to_string()));
    let captured_values = Arc::new(Mutex::new(HashMap::<String, String>::new()));

    let captured_for_handler = captured_values.clone();
    bus.on_typed::<SimpleEvent, _, _>("handler", move |_event| {
        let captured = captured_for_handler.clone();
        async move {
            captured
                .lock()
                .expect("captured lock")
                .insert("request_id".to_string(), context_str("request_id"));
            captured
                .lock()
                .expect("captured lock")
                .insert("user_id".to_string(), context_str("user_id"));
            Ok("handled".to_string())
        }
    });

    let event = EventBus::with_context(
        context(&[("request_id", "req-12345"), ("user_id", "user-abc")]),
        || bus.emit::<SimpleEvent>(TypedEvent::new(EmptyPayload {})),
    );
    block_on(event.wait_completed());

    let captured = captured_values.lock().expect("captured lock");
    assert_eq!(
        captured.get("request_id").map(String::as_str),
        Some("req-12345")
    );
    assert_eq!(
        captured.get("user_id").map(String::as_str),
        Some("user-abc")
    );
    bus.stop();
    EventBus::context_clear();
}

#[test]
fn test_contextvar_propagates_through_nested_handlers() {
    EventBus::context_clear();
    let bus = EventBus::new(Some("NestedContextBus".to_string()));
    let captured_parent = Arc::new(Mutex::new(HashMap::<String, String>::new()));
    let captured_child = Arc::new(Mutex::new(HashMap::<String, String>::new()));

    let bus_for_parent = bus.clone();
    let parent_capture = captured_parent.clone();
    bus.on_typed::<SimpleEvent, _, _>("parent_handler", move |_event| {
        let bus = bus_for_parent.clone();
        let captured = parent_capture.clone();
        async move {
            captured
                .lock()
                .expect("parent capture lock")
                .insert("request_id".to_string(), context_str("request_id"));
            captured
                .lock()
                .expect("parent capture lock")
                .insert("trace_id".to_string(), context_str("trace_id"));
            let child = bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            child.wait_completed().await;
            Ok("parent_done".to_string())
        }
    });

    let child_capture = captured_child.clone();
    bus.on_typed::<ChildEvent, _, _>("child_handler", move |_event| {
        let captured = child_capture.clone();
        async move {
            captured
                .lock()
                .expect("child capture lock")
                .insert("request_id".to_string(), context_str("request_id"));
            captured
                .lock()
                .expect("child capture lock")
                .insert("trace_id".to_string(), context_str("trace_id"));
            Ok("child_done".to_string())
        }
    });

    let event = EventBus::with_context(
        context(&[("request_id", "req-nested-123"), ("trace_id", "trace-xyz")]),
        || bus.emit::<SimpleEvent>(TypedEvent::new(EmptyPayload {})),
    );
    block_on(event.wait_completed());

    let parent = captured_parent.lock().expect("parent capture lock");
    let child = captured_child.lock().expect("child capture lock");
    assert_eq!(
        parent.get("request_id").map(String::as_str),
        Some("req-nested-123")
    );
    assert_eq!(
        parent.get("trace_id").map(String::as_str),
        Some("trace-xyz")
    );
    assert_eq!(
        child.get("request_id").map(String::as_str),
        Some("req-nested-123")
    );
    assert_eq!(child.get("trace_id").map(String::as_str), Some("trace-xyz"));
    bus.stop();
    EventBus::context_clear();
}

#[test]
fn test_context_isolation_between_dispatches() {
    EventBus::context_clear();
    let bus = EventBus::new(Some("IsolationTestBus".to_string()));
    let captured_values = Arc::new(Mutex::new(Vec::<String>::new()));

    let captured_for_handler = captured_values.clone();
    bus.on_typed::<SimpleEvent, _, _>("handler", move |_event| {
        let captured = captured_for_handler.clone();
        async move {
            thread::sleep(Duration::from_millis(5));
            captured
                .lock()
                .expect("captured lock")
                .push(context_str("request_id"));
            Ok("handled".to_string())
        }
    });

    let event_a = EventBus::with_context(context(&[("request_id", "req-A")]), || {
        bus.emit::<SimpleEvent>(TypedEvent::new(EmptyPayload {}))
    });
    let event_b = EventBus::with_context(context(&[("request_id", "req-B")]), || {
        bus.emit::<SimpleEvent>(TypedEvent::new(EmptyPayload {}))
    });

    block_on(async {
        event_a.wait_completed().await;
        event_b.wait_completed().await;
    });

    let captured = captured_values.lock().expect("captured lock").clone();
    assert!(captured.contains(&"req-A".to_string()), "{captured:?}");
    assert!(captured.contains(&"req-B".to_string()), "{captured:?}");
    bus.stop();
    EventBus::context_clear();
}

#[test]
fn test_context_propagates_to_parallel_handler_concurrency() {
    EventBus::context_clear();
    let bus = EventBus::new_with_options(
        Some("ParallelContextBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    let captured_values = Arc::new(Mutex::new(Vec::<String>::new()));

    for handler_name in ["h1", "h2"] {
        let captured = captured_values.clone();
        bus.on_typed::<SimpleEvent, _, _>(handler_name, move |_event| {
            let captured = captured.clone();
            async move {
                captured
                    .lock()
                    .expect("captured lock")
                    .push(format!("{handler_name}:{}", context_str("request_id")));
                Ok(format!("{handler_name}_done"))
            }
        });
    }

    let event = EventBus::with_context(context(&[("request_id", "req-parallel")]), || {
        bus.emit::<SimpleEvent>(TypedEvent::new(EmptyPayload {}))
    });
    block_on(event.wait_completed());

    let captured = captured_values.lock().expect("captured lock").clone();
    assert!(captured.contains(&"h1:req-parallel".to_string()));
    assert!(captured.contains(&"h2:req-parallel".to_string()));
    bus.stop();
    EventBus::context_clear();
}

#[test]
fn test_context_propagates_through_event_forwarding() {
    EventBus::context_clear();
    let bus_a = EventBus::new(Some("BusA".to_string()));
    let bus_b = EventBus::new(Some("BusB".to_string()));
    let captured_bus_a = Arc::new(Mutex::new(HashMap::<String, String>::new()));
    let captured_bus_b = Arc::new(Mutex::new(HashMap::<String, String>::new()));

    let captured_a = captured_bus_a.clone();
    bus_a.on_typed::<SimpleEvent, _, _>("bus_a_handler", move |_event| {
        let captured = captured_a.clone();
        async move {
            captured
                .lock()
                .expect("captured a lock")
                .insert("request_id".to_string(), context_str("request_id"));
            Ok("bus_a_done".to_string())
        }
    });

    let bus_b_for_forward = bus_b.clone();
    bus_a.on("*", "forward_to_bus_b", move |event| {
        let bus_b = bus_b_for_forward.clone();
        async move {
            bus_b.emit_base(event);
            Ok(json!(null))
        }
    });

    let captured_b = captured_bus_b.clone();
    bus_b.on_typed::<SimpleEvent, _, _>("bus_b_handler", move |_event| {
        let captured = captured_b.clone();
        async move {
            captured
                .lock()
                .expect("captured b lock")
                .insert("request_id".to_string(), context_str("request_id"));
            Ok("bus_b_done".to_string())
        }
    });

    let event = EventBus::with_context(context(&[("request_id", "req-forwarded")]), || {
        bus_a.emit::<SimpleEvent>(TypedEvent::new(EmptyPayload {}))
    });
    block_on(async {
        event.wait_completed().await;
        assert!(bus_b.wait_until_idle(Some(1.0)).await);
    });

    assert_eq!(
        captured_bus_a
            .lock()
            .expect("captured a lock")
            .get("request_id")
            .map(String::as_str),
        Some("req-forwarded")
    );
    assert_eq!(
        captured_bus_b
            .lock()
            .expect("captured b lock")
            .get("request_id")
            .map(String::as_str),
        Some("req-forwarded")
    );
    bus_a.stop();
    bus_b.stop();
    EventBus::context_clear();
}

#[test]
fn test_forwarded_dispatch_context_does_not_leak_back_to_source_bus_handlers() {
    EventBus::context_clear();
    let bus_a = EventBus::new(Some("SourceContextBus".to_string()));
    let bus_b = EventBus::new(Some("ForwardedContextBus".to_string()));
    let captured = Arc::new(Mutex::new(HashMap::<String, String>::new()));

    let bus_b_for_first = bus_b.clone();
    let captured_first = captured.clone();
    bus_a.on_typed::<SimpleEvent, _, _>("source_first", move |event| {
        let bus_b = bus_b_for_first.clone();
        let captured = captured_first.clone();
        async move {
            captured
                .lock()
                .expect("captured lock")
                .insert("source_first".to_string(), context_str("request_id"));
            EventBus::context_set("request_id", json!("forwarded-context"));
            bus_b.emit_base(event.inner.clone());
            Ok("source_first_done".to_string())
        }
    });

    let captured_second = captured.clone();
    bus_a.on_typed::<SimpleEvent, _, _>("source_second", move |_event| {
        let captured = captured_second.clone();
        async move {
            captured
                .lock()
                .expect("captured lock")
                .insert("source_second".to_string(), context_str("request_id"));
            Ok("source_second_done".to_string())
        }
    });

    let captured_forwarded = captured.clone();
    bus_b.on_typed::<SimpleEvent, _, _>("forwarded_handler", move |_event| {
        let captured = captured_forwarded.clone();
        async move {
            captured
                .lock()
                .expect("captured lock")
                .insert("forwarded".to_string(), context_str("request_id"));
            Ok("forwarded_done".to_string())
        }
    });

    let event = EventBus::with_context(context(&[("request_id", "source-context")]), || {
        bus_a.emit::<SimpleEvent>(TypedEvent::new(EmptyPayload {}))
    });
    block_on(async {
        event.wait_completed().await;
        assert!(bus_b.wait_until_idle(Some(1.0)).await);
    });

    let captured = captured.lock().expect("captured lock");
    assert_eq!(
        captured.get("source_first").map(String::as_str),
        Some("source-context")
    );
    assert_eq!(
        captured.get("source_second").map(String::as_str),
        Some("source-context")
    );
    assert_eq!(
        captured.get("forwarded").map(String::as_str),
        Some("forwarded-context")
    );
    bus_a.stop();
    bus_b.stop();
    EventBus::context_clear();
}

#[test]
fn test_handler_can_modify_context_without_affecting_parent() {
    EventBus::context_clear();
    let bus = EventBus::new(Some("ModifyContextBus".to_string()));
    let parent_value_after_child = Arc::new(Mutex::new(String::new()));

    let bus_for_parent = bus.clone();
    let parent_value = parent_value_after_child.clone();
    bus.on_typed::<SimpleEvent, _, _>("parent_handler", move |_event| {
        let bus = bus_for_parent.clone();
        let parent_value = parent_value.clone();
        async move {
            EventBus::context_set("request_id", json!("parent-value"));
            let child = bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            child.wait_completed().await;
            *parent_value.lock().expect("parent value lock") = context_str("request_id");
            Ok("parent_done".to_string())
        }
    });

    bus.on_typed::<ChildEvent, _, _>("child_handler", move |_event| async move {
        EventBus::context_set("request_id", json!("child-modified"));
        Ok("child_done".to_string())
    });

    let event = bus.emit::<SimpleEvent>(TypedEvent::new(EmptyPayload {}));
    block_on(event.wait_completed());

    assert_eq!(
        parent_value_after_child
            .lock()
            .expect("parent value lock")
            .as_str(),
        "parent-value"
    );
    bus.stop();
    EventBus::context_clear();
}

#[test]
fn test_event_parent_id_tracking_still_works() {
    EventBus::context_clear();
    let bus = EventBus::new(Some("ParentIdTrackingBus".to_string()));
    let parent_event_id = Arc::new(Mutex::new(None::<String>));
    let child_event_parent_id = Arc::new(Mutex::new(None::<String>));

    let bus_for_parent = bus.clone();
    let parent_id_capture = parent_event_id.clone();
    bus.on_typed::<SimpleEvent, _, _>("parent_handler", move |event| {
        let bus = bus_for_parent.clone();
        let parent_id_capture = parent_id_capture.clone();
        async move {
            *parent_id_capture.lock().expect("parent id lock") =
                Some(event.inner.inner.lock().event_id.clone());
            let child = bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            child.wait_completed().await;
            Ok("parent_done".to_string())
        }
    });

    let child_parent_capture = child_event_parent_id.clone();
    bus.on_typed::<ChildEvent, _, _>("child_handler", move |event| {
        let child_parent_capture = child_parent_capture.clone();
        async move {
            *child_parent_capture.lock().expect("child parent id lock") =
                event.inner.inner.lock().event_parent_id.clone();
            Ok("child_done".to_string())
        }
    });

    let event = EventBus::with_context(context(&[("request_id", "req-parent-tracking")]), || {
        bus.emit::<SimpleEvent>(TypedEvent::new(EmptyPayload {}))
    });
    block_on(event.wait_completed());

    let parent_id = parent_event_id
        .lock()
        .expect("parent id lock")
        .clone()
        .expect("parent event id");
    let child_parent_id = child_event_parent_id
        .lock()
        .expect("child parent id lock")
        .clone()
        .expect("child event parent id");
    assert_eq!(child_parent_id, parent_id);
    bus.stop();
    EventBus::context_clear();
}

#[test]
fn test_dispatch_context_and_parent_id_both_work() {
    EventBus::context_clear();
    let bus = EventBus::new(Some("CombinedContextBus".to_string()));
    let results = Arc::new(Mutex::new(HashMap::<String, String>::new()));

    let bus_for_parent = bus.clone();
    let parent_results = results.clone();
    bus.on_typed::<SimpleEvent, _, _>("parent_handler", move |event| {
        let bus = bus_for_parent.clone();
        let results = parent_results.clone();
        async move {
            results
                .lock()
                .expect("results lock")
                .insert("parent_request_id".to_string(), context_str("request_id"));
            results.lock().expect("results lock").insert(
                "parent_event_id".to_string(),
                event.inner.inner.lock().event_id.clone(),
            );
            let child = bus.emit_child::<ChildEvent>(TypedEvent::new(EmptyPayload {}));
            child.wait_completed().await;
            Ok("parent_done".to_string())
        }
    });

    let child_results = results.clone();
    bus.on_typed::<ChildEvent, _, _>("child_handler", move |event| {
        let results = child_results.clone();
        async move {
            results
                .lock()
                .expect("results lock")
                .insert("child_request_id".to_string(), context_str("request_id"));
            let parent_id = event
                .inner
                .inner
                .lock()
                .event_parent_id
                .clone()
                .unwrap_or_default();
            results
                .lock()
                .expect("results lock")
                .insert("child_event_parent_id".to_string(), parent_id);
            Ok("child_done".to_string())
        }
    });

    let event = EventBus::with_context(context(&[("request_id", "req-combined-test")]), || {
        bus.emit::<SimpleEvent>(TypedEvent::new(EmptyPayload {}))
    });
    block_on(event.wait_completed());

    let results = results.lock().expect("results lock");
    assert_eq!(
        results.get("parent_request_id").map(String::as_str),
        Some("req-combined-test")
    );
    assert_eq!(
        results.get("child_request_id").map(String::as_str),
        Some("req-combined-test")
    );
    assert_eq!(
        results.get("child_event_parent_id"),
        results.get("parent_event_id")
    );
    bus.stop();
    EventBus::context_clear();
}

#[test]
fn test_deeply_nested_context_and_parent_tracking() {
    EventBus::context_clear();
    let bus = EventBus::new(Some("DeepNestingBus".to_string()));
    let results = Arc::new(Mutex::new(Vec::<HashMap<String, String>>::new()));

    let bus_for_level1 = bus.clone();
    let level1_results = results.clone();
    bus.on_typed::<SimpleEvent, _, _>("level1_handler", move |event| {
        let bus = bus_for_level1.clone();
        let results = level1_results.clone();
        async move {
            let event_id = event.inner.inner.lock().event_id.clone();
            results.lock().expect("results lock").push(HashMap::from([
                ("level".to_string(), "1".to_string()),
                ("request_id".to_string(), context_str("request_id")),
                ("event_id".to_string(), event_id),
                ("parent_id".to_string(), "<none>".to_string()),
            ]));
            let child = bus.emit_child::<Level2Event>(TypedEvent::new(EmptyPayload {}));
            child.wait_completed().await;
            Ok("level1_done".to_string())
        }
    });

    let bus_for_level2 = bus.clone();
    let level2_results = results.clone();
    bus.on_typed::<Level2Event, _, _>("level2_handler", move |event| {
        let bus = bus_for_level2.clone();
        let results = level2_results.clone();
        async move {
            let (event_id, parent_id) = {
                let inner = event.inner.inner.lock();
                (
                    inner.event_id.clone(),
                    inner.event_parent_id.clone().unwrap_or_default(),
                )
            };
            results.lock().expect("results lock").push(HashMap::from([
                ("level".to_string(), "2".to_string()),
                ("request_id".to_string(), context_str("request_id")),
                ("event_id".to_string(), event_id),
                ("parent_id".to_string(), parent_id),
            ]));
            let child = bus.emit_child::<Level3Event>(TypedEvent::new(EmptyPayload {}));
            child.wait_completed().await;
            Ok("level2_done".to_string())
        }
    });

    let level3_results = results.clone();
    bus.on_typed::<Level3Event, _, _>("level3_handler", move |event| {
        let results = level3_results.clone();
        async move {
            let inner = event.inner.inner.lock();
            let event_id = inner.event_id.clone();
            let parent_id = inner.event_parent_id.clone().unwrap_or_default();
            drop(inner);
            results.lock().expect("results lock").push(HashMap::from([
                ("level".to_string(), "3".to_string()),
                ("request_id".to_string(), context_str("request_id")),
                ("event_id".to_string(), event_id),
                ("parent_id".to_string(), parent_id),
            ]));
            Ok("level3_done".to_string())
        }
    });

    let event = EventBus::with_context(context(&[("request_id", "req-deep-nesting")]), || {
        bus.emit::<SimpleEvent>(TypedEvent::new(EmptyPayload {}))
    });
    block_on(event.wait_completed());

    let mut results = results.lock().expect("results lock").clone();
    results.sort_by_key(|row| row.get("level").cloned());
    assert_eq!(results.len(), 3);
    for row in &results {
        assert_eq!(
            row.get("request_id").map(String::as_str),
            Some("req-deep-nesting")
        );
    }
    assert_eq!(
        results[1].get("parent_id"),
        results[0].get("event_id"),
        "{results:?}"
    );
    assert_eq!(
        results[2].get("parent_id"),
        results[1].get("event_id"),
        "{results:?}"
    );
    bus.stop();
    EventBus::context_clear();
}
