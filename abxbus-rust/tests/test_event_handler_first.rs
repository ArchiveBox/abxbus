use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use abxbus_rust::{
    base_event::BaseEvent,
    event_bus::EventBus,
    typed::{EventSpec, TypedEvent},
    types::{EventHandlerCompletionMode, EventHandlerConcurrencyMode},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

#[derive(Clone, Serialize, Deserialize)]
struct EmptyPayload {}
#[derive(Clone, Serialize, Deserialize)]
struct WorkResult {
    value: String,
}
struct WorkEvent;
impl EventSpec for WorkEvent {
    type Payload = EmptyPayload;
    type Result = WorkResult;
    const EVENT_TYPE: &'static str = "work";
}

struct ValueEvent;
impl EventSpec for ValueEvent {
    type Payload = EmptyPayload;
    type Result = Value;
    const EVENT_TYPE: &'static str = "value";
}

#[test]
fn test_event_handler_first_serial_stops_after_first_success() {
    let bus = EventBus::new(Some("BusFirstSerial".to_string()));

    bus.on("work", "first", |_event| async move { Ok(json!("winner")) });
    bus.on("work", "second", |_event| async move {
        thread::sleep(Duration::from_millis(20));
        Ok(json!("late"))
    });

    let event = TypedEvent::<WorkEvent>::new(EmptyPayload {});
    {
        let mut inner = event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Serial);
    }
    let emitted = bus.emit(event);
    block_on(emitted.wait_completed());

    let results = emitted.inner.inner.lock().event_results.clone();
    assert_eq!(results.len(), 2);
    assert!(results.values().any(|result| {
        result.status == abxbus_rust::event_result::EventResultStatus::Completed
            && result.result == Some(json!("winner"))
    }));
    assert!(results.values().any(|result| {
        result.status == abxbus_rust::event_result::EventResultStatus::Error
            && result
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("Cancelled: first() resolved")
    }));
    bus.stop();
}

#[test]
fn test_event_first_skips_none_result_and_uses_next_winner() {
    let bus = EventBus::new(Some("BusFirstSkipsNull".to_string()));

    bus.on("value", "none", |_event| async move { Ok(Value::Null) });
    bus.on(
        "value",
        "winner",
        |_event| async move { Ok(json!("winner")) },
    );
    bus.on("value", "late", |_event| async move { Ok(json!("late")) });

    let event = TypedEvent::<ValueEvent>::new(EmptyPayload {});
    {
        let mut inner = event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Serial);
    }
    let emitted = bus.emit(event);
    block_on(emitted.wait_completed());

    let results = emitted.inner.inner.lock().event_results.clone();
    assert_eq!(results.len(), 3);
    assert_eq!(emitted.first_result(), Some(json!("winner")));
    assert!(results
        .values()
        .any(|result| result.result == Some(Value::Null)));
    assert!(results
        .values()
        .any(|result| result.result == Some(json!("winner"))));
    assert!(results.values().any(|result| {
        result.status == abxbus_rust::event_result::EventResultStatus::Error
            && result
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("Cancelled: first() resolved")
    }));
    bus.stop();
}

#[test]
fn test_event_first_preserves_false_and_empty_string_results() {
    let false_bus = EventBus::new(Some("BusFirstFalse".to_string()));
    false_bus.on(
        "value",
        "false_winner",
        |_event| async move { Ok(json!(false)) },
    );
    false_bus.on("value", "late", |_event| async move { Ok(json!("late")) });

    let false_event = TypedEvent::<ValueEvent>::new(EmptyPayload {});
    {
        let mut inner = false_event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Serial);
    }
    let false_event = false_bus.emit(false_event);
    block_on(false_event.wait_completed());
    assert_eq!(false_event.first_result(), Some(json!(false)));
    assert_eq!(false_event.inner.inner.lock().event_results.len(), 2);
    false_bus.stop();

    let empty_bus = EventBus::new(Some("BusFirstEmptyString".to_string()));
    empty_bus.on(
        "value",
        "empty_winner",
        |_event| async move { Ok(json!("")) },
    );
    empty_bus.on("value", "late", |_event| async move { Ok(json!("late")) });

    let empty_event = TypedEvent::<ValueEvent>::new(EmptyPayload {});
    {
        let mut inner = empty_event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Serial);
    }
    let empty_event = empty_bus.emit(empty_event);
    block_on(empty_event.wait_completed());
    assert_eq!(empty_event.first_result(), Some(json!("")));
    assert_eq!(empty_event.inner.inner.lock().event_results.len(), 2);
    empty_bus.stop();
}

#[test]
fn test_event_first_shortcut_sets_mode_and_returns_winner() {
    let bus = EventBus::new(Some("BusFirstShortcut".to_string()));

    bus.on(
        "value",
        "null_result",
        |_event| async move { Ok(Value::Null) },
    );
    bus.on(
        "value",
        "winner",
        |_event| async move { Ok(json!("winner")) },
    );
    bus.on("value", "late", |_event| async move { Ok(json!("late")) });

    let event = bus.emit::<ValueEvent>(TypedEvent::<ValueEvent>::new(EmptyPayload {}));
    assert_eq!(event.inner.inner.lock().event_handler_completion, None);
    let result = block_on(event.first());

    assert_eq!(result, Some(json!("winner")));
    assert_eq!(
        event.inner.inner.lock().event_handler_completion,
        Some(EventHandlerCompletionMode::First)
    );
    assert!(event
        .inner
        .to_json_value()
        .get("event_handler_completion")
        .is_some_and(|value| value == "first"));
    bus.stop();
}

#[test]
fn test_first_event_handler_completion_is_set_to_first_after_calling_first() {
    let bus = EventBus::new(Some("FirstFieldBus".to_string()));

    bus.on("value", "result_handler", |_event| async move {
        Ok(json!("result"))
    });

    let event = bus.emit::<ValueEvent>(TypedEvent::<ValueEvent>::new(EmptyPayload {}));
    assert_eq!(event.inner.inner.lock().event_handler_completion, None);

    let result = block_on(event.first());

    assert_eq!(result, Some(json!("result")));
    assert_eq!(
        event.inner.inner.lock().event_handler_completion,
        Some(EventHandlerCompletionMode::First)
    );
    bus.stop();
}

#[test]
fn test_first_event_handler_completion_appears_in_tojson_output() {
    let bus = EventBus::new(Some("FirstJsonBus".to_string()));

    bus.on("value", "json_handler", |_event| async move {
        Ok(json!("json result"))
    });

    let event = bus.emit::<ValueEvent>(TypedEvent::<ValueEvent>::new(EmptyPayload {}));
    block_on(event.first());

    let payload = event.inner.to_json_value();
    assert_eq!(payload["event_handler_completion"], "first");
    bus.stop();
}

#[test]
fn test_first_event_handler_completion_can_be_set_via_event_constructor() {
    let bus = EventBus::new(Some("FirstCtorBus".to_string()));

    bus.on("value", "slow_handler", |_event| async move {
        thread::sleep(Duration::from_millis(80));
        Ok(json!("slow handler"))
    });
    bus.on("value", "fast_handler", |_event| async move {
        thread::sleep(Duration::from_millis(10));
        Ok(json!("fast handler"))
    });

    let event = TypedEvent::<ValueEvent>::new(EmptyPayload {});
    {
        let mut inner = event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Parallel);
    }
    let event = bus.emit(event);
    let result = block_on(event.first());

    assert_eq!(result, Some(json!("fast handler")));
    assert_eq!(
        event.inner.inner.lock().event_handler_completion,
        Some(EventHandlerCompletionMode::First)
    );
    bus.stop();
}

#[test]
fn test_event_handler_first_parallel_returns_earliest_completed_non_null_result() {
    let bus = EventBus::new(Some("BusFirstParallelWinner".to_string()));

    bus.on("value", "slow_loser", |_event| async move {
        thread::sleep(Duration::from_millis(80));
        Ok(json!("slow"))
    });
    bus.on("value", "fast_winner", |_event| async move {
        thread::sleep(Duration::from_millis(10));
        Ok(json!("fast"))
    });

    let event = TypedEvent::<ValueEvent>::new(EmptyPayload {});
    {
        let mut inner = event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Parallel);
    }
    let event = bus.emit(event);
    block_on(event.wait_completed());

    assert_eq!(event.first_result(), Some(json!("fast")));
    let results = event.inner.inner.lock().event_results.clone();
    let fast_result = results
        .values()
        .find(|result| result.handler.handler_name == "fast_winner")
        .expect("fast result");
    assert_eq!(
        fast_result.status,
        abxbus_rust::event_result::EventResultStatus::Completed
    );
    let slow_result = results
        .values()
        .find(|result| result.handler.handler_name == "slow_loser")
        .expect("slow result");
    assert_eq!(
        slow_result.status,
        abxbus_rust::event_result::EventResultStatus::Error
    );
    assert!(slow_result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("Aborted: first() resolved"));
    assert_eq!(
        slow_result.to_flat_json_value()["error"],
        json!({
            "type": "EventHandlerAbortedError",
            "message": "Aborted: first() resolved",
        })
    );
    bus.stop();
}

#[test]
fn test_event_first_parallel_returns_before_slow_loser_finishes() {
    let bus = EventBus::new(Some("BusFirstParallelEarlyReturn".to_string()));
    let slow_completed = Arc::new(AtomicBool::new(false));

    bus.on("value", "fast_winner", |_event| async move {
        thread::sleep(Duration::from_millis(10));
        Ok(json!("fast"))
    });

    let slow_completed_for_handler = slow_completed.clone();
    bus.on("value", "slow_loser", move |_event| {
        let slow_completed = slow_completed_for_handler.clone();
        async move {
            thread::sleep(Duration::from_millis(400));
            slow_completed.store(true, Ordering::SeqCst);
            Ok(json!("slow"))
        }
    });

    let event = TypedEvent::<ValueEvent>::new(EmptyPayload {});
    {
        let mut inner = event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Parallel);
    }
    let event = bus.emit(event);

    let started = Instant::now();
    let result = block_on(event.first());

    assert_eq!(result, Some(json!("fast")));
    assert!(
        started.elapsed() < Duration::from_millis(200),
        "first() should resolve without waiting for slow losers"
    );
    assert!(!slow_completed.load(Ordering::SeqCst));
    let results = event.inner.inner.lock().event_results.clone();
    let slow_result = results
        .values()
        .find(|result| result.handler.handler_name == "slow_loser")
        .expect("slow result");
    assert_eq!(
        slow_result.status,
        abxbus_rust::event_result::EventResultStatus::Error
    );
    assert!(slow_result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("first() resolved"));
    bus.stop();
}

#[test]
fn test_first_returns_undefined_when_no_handlers_are_registered() {
    let bus = EventBus::new(Some("FirstNoHandlerBus".to_string()));

    let result = block_on(
        bus.emit::<ValueEvent>(TypedEvent::new(EmptyPayload {}))
            .first(),
    );

    assert_eq!(result, None);
    bus.stop();
}

#[test]
fn test_event_first_returns_zero_as_valid_first_result() {
    let bus = EventBus::new(Some("BusFirstZero".to_string()));

    bus.on("value", "zero_winner", |_event| async move { Ok(json!(0)) });
    bus.on("value", "late", |_event| async move { Ok(json!("late")) });

    let event = TypedEvent::<ValueEvent>::new(EmptyPayload {});
    {
        let mut inner = event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Serial);
    }
    let event = bus.emit(event);
    block_on(event.wait_completed());

    assert_eq!(event.first_result(), Some(json!(0)));
    bus.stop();
}

#[test]
fn test_first_returns_empty_string_as_a_valid_first_result() {
    let bus = EventBus::new(Some("FirstEmptyBus".to_string()));

    bus.on(
        "value",
        "empty_winner",
        |_event| async move { Ok(json!("")) },
    );

    let result = block_on(
        bus.emit::<ValueEvent>(TypedEvent::new(EmptyPayload {}))
            .first(),
    );

    assert_eq!(result, Some(json!("")));
    bus.stop();
}

#[test]
fn test_first_returns_false_as_a_valid_first_result() {
    let bus = EventBus::new(Some("FirstFalseBus".to_string()));

    bus.on(
        "value",
        "false_winner",
        |_event| async move { Ok(json!(false)) },
    );

    let result = block_on(
        bus.emit::<ValueEvent>(TypedEvent::new(EmptyPayload {}))
            .first(),
    );

    assert_eq!(result, Some(json!(false)));
    bus.stop();
}

#[test]
fn test_event_first_returns_none_when_all_handlers_return_null() {
    let bus = EventBus::new(Some("BusFirstAllNull".to_string()));

    bus.on("value", "null_a", |_event| async move { Ok(Value::Null) });
    bus.on("value", "null_b", |_event| async move { Ok(Value::Null) });

    let result = block_on(
        bus.emit::<ValueEvent>(TypedEvent::new(EmptyPayload {}))
            .first(),
    );

    assert_eq!(result, None);
    bus.stop();
}

#[test]
fn test_event_first_works_with_single_handler() {
    let bus = EventBus::new(Some("BusFirstSingle".to_string()));

    bus.on("value", "single", |_event| async move { Ok(json!(42)) });

    let result = block_on(
        bus.emit::<ValueEvent>(TypedEvent::new(EmptyPayload {}))
            .first(),
    );

    assert_eq!(result, Some(json!(42)));
    bus.stop();
}

#[test]
fn test_event_first_skips_base_event_json_result_and_uses_next_winner() {
    let bus = EventBus::new(Some("BusFirstSkipsBaseEventJson".to_string()));
    let third_handler_called = Arc::new(AtomicBool::new(false));

    bus.on("value", "base_event_result", |_event| async move {
        Ok(BaseEvent::new("FirstBaseEventSkipChild", Map::new()).to_json_value())
    });
    bus.on(
        "value",
        "winner",
        |_event| async move { Ok(json!("winner")) },
    );
    let third_called_for_handler = third_handler_called.clone();
    bus.on("value", "third", move |_event| {
        let third_handler_called = third_called_for_handler.clone();
        async move {
            third_handler_called.store(true, Ordering::SeqCst);
            Ok(json!("third"))
        }
    });

    let event = TypedEvent::<ValueEvent>::new(EmptyPayload {});
    {
        let mut inner = event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Serial);
    }
    let event = bus.emit(event);
    block_on(event.wait_completed());

    assert_eq!(event.first_result(), Some(json!("winner")));
    assert!(!third_handler_called.load(Ordering::SeqCst));
    let results = event.inner.inner.lock().event_results.clone();
    assert!(results
        .values()
        .any(|result| result.handler.handler_name == "base_event_result"
            && result.status == abxbus_rust::event_result::EventResultStatus::Completed
            && result.result.as_ref().is_some_and(
                |value| value.get("event_type") == Some(&json!("FirstBaseEventSkipChild"))
            )));
    assert!(results.values().any(|result| {
        result.handler.handler_name == "third"
            && result.status == abxbus_rust::event_result::EventResultStatus::Error
            && result
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("Cancelled: first() resolved")
    }));
    bus.stop();
}
