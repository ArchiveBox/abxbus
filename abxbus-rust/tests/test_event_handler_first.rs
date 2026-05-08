use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

use abxbus_rust::{
    base_event::BaseEvent,
    event_bus::EventBus,
    typed::{BaseEventHandle, EventSpec},
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
    type payload = EmptyPayload;
    type event_result_type = WorkResult;
    const event_type: &'static str = "work";
}

struct ValueEvent;
impl EventSpec for ValueEvent {
    type payload = EmptyPayload;
    type event_result_type = Value;
    const event_type: &'static str = "value";
}

struct ChildEvent;
impl EventSpec for ChildEvent {
    type payload = EmptyPayload;
    type event_result_type = Value;
    const event_type: &'static str = "child";
}

#[test]
fn test_event_handler_first_serial_stops_after_first_success() {
    let bus = EventBus::new(Some("BusFirstSerial".to_string()));

    bus.on_raw("work", "first", |_event| async move { Ok(json!("winner")) });
    bus.on_raw("work", "second", |_event| async move {
        thread::sleep(Duration::from_millis(20));
        Ok(json!("late"))
    });

    let event = BaseEventHandle::<WorkEvent>::new(EmptyPayload {});
    {
        let mut inner = event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Serial);
    }
    let emitted = bus.emit(event);
    block_on(emitted.done());

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

    bus.on_raw("value", "none", |_event| async move { Ok(Value::Null) });
    bus.on_raw(
        "value",
        "winner",
        |_event| async move { Ok(json!("winner")) },
    );
    bus.on_raw("value", "late", |_event| async move { Ok(json!("late")) });

    let event = BaseEventHandle::<ValueEvent>::new(EmptyPayload {});
    {
        let mut inner = event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Serial);
    }
    let emitted = bus.emit(event);
    block_on(emitted.done());

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
    false_bus.on_raw(
        "value",
        "false_winner",
        |_event| async move { Ok(json!(false)) },
    );
    false_bus.on_raw("value", "late", |_event| async move { Ok(json!("late")) });

    let false_event = BaseEventHandle::<ValueEvent>::new(EmptyPayload {});
    {
        let mut inner = false_event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Serial);
    }
    let false_event = false_bus.emit(false_event);
    block_on(false_event.done());
    assert_eq!(false_event.first_result(), Some(json!(false)));
    assert_eq!(false_event.inner.inner.lock().event_results.len(), 2);
    false_bus.stop();

    let empty_bus = EventBus::new(Some("BusFirstEmptyString".to_string()));
    empty_bus.on_raw(
        "value",
        "empty_winner",
        |_event| async move { Ok(json!("")) },
    );
    empty_bus.on_raw("value", "late", |_event| async move { Ok(json!("late")) });

    let empty_event = BaseEventHandle::<ValueEvent>::new(EmptyPayload {});
    {
        let mut inner = empty_event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Serial);
    }
    let empty_event = empty_bus.emit(empty_event);
    block_on(empty_event.done());
    assert_eq!(empty_event.first_result(), Some(json!("")));
    assert_eq!(empty_event.inner.inner.lock().event_results.len(), 2);
    empty_bus.stop();
}

#[test]
fn test_event_first_shortcut_sets_mode_and_returns_winner() {
    let bus = EventBus::new(Some("BusFirstShortcut".to_string()));

    bus.on_raw(
        "value",
        "null_result",
        |_event| async move { Ok(Value::Null) },
    );
    bus.on_raw(
        "value",
        "winner",
        |_event| async move { Ok(json!("winner")) },
    );
    bus.on_raw("value", "late", |_event| async move { Ok(json!("late")) });

    let event = bus.emit(BaseEventHandle::<ValueEvent>::new(EmptyPayload {}));
    assert_eq!(event.inner.inner.lock().event_handler_completion, None);
    let result = block_on(event.first()).expect("first result");

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

    bus.on_raw("value", "result_handler", |_event| async move {
        Ok(json!("result"))
    });

    let event = bus.emit(BaseEventHandle::<ValueEvent>::new(EmptyPayload {}));
    assert_eq!(event.inner.inner.lock().event_handler_completion, None);

    let result = block_on(event.first()).expect("first result");

    assert_eq!(result, Some(json!("result")));
    assert_eq!(
        event.inner.inner.lock().event_handler_completion,
        Some(EventHandlerCompletionMode::First)
    );
    bus.stop();
}

#[test]
fn test_first_event_handler_completion_is_set_to() {
    test_first_event_handler_completion_is_set_to_first_after_calling_first();
}

#[test]
fn test_first_event_handler_completion_appears_in_tojson_output() {
    let bus = EventBus::new(Some("FirstJsonBus".to_string()));

    bus.on_raw("value", "json_handler", |_event| async move {
        Ok(json!("json result"))
    });

    let event = bus.emit(BaseEventHandle::<ValueEvent>::new(EmptyPayload {}));
    block_on(event.first()).expect("first result");

    let payload = event.inner.to_json_value();
    assert_eq!(payload["event_handler_completion"], "first");
    bus.stop();
}

#[test]
fn test_first_event_handler_completion_can_be_set_via_event_constructor() {
    let bus = EventBus::new(Some("FirstCtorBus".to_string()));

    bus.on_raw("value", "slow_handler", |_event| async move {
        thread::sleep(Duration::from_millis(80));
        Ok(json!("slow handler"))
    });
    bus.on_raw("value", "fast_handler", |_event| async move {
        thread::sleep(Duration::from_millis(10));
        Ok(json!("fast handler"))
    });

    let event = BaseEventHandle::<ValueEvent>::new(EmptyPayload {});
    {
        let mut inner = event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Parallel);
    }
    let event = bus.emit(event);
    let result = block_on(event.first()).expect("first result");

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

    bus.on_raw("value", "slow_loser", |_event| async move {
        thread::sleep(Duration::from_millis(80));
        Ok(json!("slow"))
    });
    bus.on_raw("value", "fast_winner", |_event| async move {
        thread::sleep(Duration::from_millis(10));
        Ok(json!("fast"))
    });

    let event = BaseEventHandle::<ValueEvent>::new(EmptyPayload {});
    {
        let mut inner = event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Parallel);
    }
    let event = bus.emit(event);
    block_on(event.done());

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

    bus.on_raw("value", "fast_winner", |_event| async move {
        thread::sleep(Duration::from_millis(10));
        Ok(json!("fast"))
    });

    let slow_completed_for_handler = slow_completed.clone();
    bus.on_raw("value", "slow_loser", move |_event| {
        let slow_completed = slow_completed_for_handler.clone();
        async move {
            thread::sleep(Duration::from_millis(400));
            slow_completed.store(true, Ordering::SeqCst);
            Ok(json!("slow"))
        }
    });

    let event = BaseEventHandle::<ValueEvent>::new(EmptyPayload {});
    {
        let mut inner = event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Parallel);
    }
    let event = bus.emit(event);

    let started = Instant::now();
    let result = block_on(event.first()).expect("first result");

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
        bus.emit(BaseEventHandle::<ValueEvent>::new(EmptyPayload {}))
            .first(),
    )
    .expect("first result");

    assert_eq!(result, None);
    bus.stop();
}

#[test]
fn test_first_rejects_when_event_has_no_bus_attached() {
    let event = BaseEventHandle::<ValueEvent>::new(EmptyPayload {});

    let error = block_on(event.first()).expect_err("unemitted event should reject");

    assert_eq!(error, "event has no bus attached");
}

#[test]
fn test_first_re_raises_first_processing_error_when_all_handlers_throw() {
    let bus = EventBus::new(Some("FirstErrorBus".to_string()));

    bus.on_raw("value", "handler_1", |_event| async move {
        Err("handler 1 error".to_string())
    });
    bus.on_raw("value", "handler_2", |_event| async move {
        Err("handler 2 error".to_string())
    });

    let event = BaseEventHandle::<ValueEvent>::new(EmptyPayload {});
    {
        let mut inner = event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Parallel);
    }
    let error = block_on(bus.emit(event).first()).expect_err("first should surface handler error");

    assert!(
        error.contains("handler 1 error") || error.contains("handler 2 error"),
        "unexpected first error: {error}"
    );
    bus.stop();
}

#[test]
fn test_first_re_raises_processing_errors_even_when_another_handler_succeeds() {
    let bus = EventBus::new(Some("FirstMixBus".to_string()));

    bus.on_raw("value", "fast_failure", |_event| async move {
        Err("fast but fails".to_string())
    });
    bus.on_raw("value", "slow_success", |_event| async move {
        thread::sleep(Duration::from_millis(20));
        Ok(json!("slow but succeeds"))
    });

    let event = BaseEventHandle::<ValueEvent>::new(EmptyPayload {});
    {
        let mut inner = event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Parallel);
    }
    let event = bus.emit(event);
    let error = block_on(event.first()).expect_err("first should surface fast handler error");

    assert!(error.contains("fast but fails"));
    let has_success = event
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .any(|result| result.result == Some(json!("slow but succeeds")));
    assert!(has_success, "slow success should still be recorded");
    bus.stop();
}

#[test]
fn test_first_cancels_child_events_emitted_by_losing_handlers() {
    let bus = EventBus::new(Some("FirstChildBus".to_string()));
    let bus_for_slow_parent = bus.clone();
    let child_ref = Arc::new(Mutex::new(None::<Arc<BaseEvent>>));
    let child_ref_for_slow_parent = child_ref.clone();

    bus.on_raw("child", "slow_child", |_event| async move {
        thread::sleep(Duration::from_millis(500));
        Ok(json!("child result"))
    });
    bus.on_raw("value", "fast_parent", |_event| async move {
        thread::sleep(Duration::from_millis(30));
        Ok(json!("fast parent"))
    });
    bus.on_raw("value", "slow_parent_with_child", move |_event| {
        let bus = bus_for_slow_parent.clone();
        let child_ref = child_ref_for_slow_parent.clone();
        async move {
            let child = bus.emit_child(BaseEventHandle::<ChildEvent>::new(EmptyPayload {}));
            *child_ref.lock().expect("child ref lock") = Some(child.inner.clone());
            child.done().await;
            Ok(json!("slow parent with child"))
        }
    });

    let event = BaseEventHandle::<ValueEvent>::new(EmptyPayload {});
    {
        let mut inner = event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Parallel);
    }
    let result = block_on(bus.emit(event).first()).expect("first result");
    assert_eq!(result, Some(json!("fast parent")));

    let child = child_ref
        .lock()
        .expect("child ref lock")
        .clone()
        .expect("losing handler should have emitted child");
    let child_inner = child.inner.lock();
    assert!(child_inner.event_blocks_parent_completion);
    assert_eq!(
        child_inner.event_status,
        abxbus_rust::types::EventStatus::Completed
    );
    assert!(child_inner.event_results.values().any(|result| {
        result.status == abxbus_rust::event_result::EventResultStatus::Error
            && result
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("first() resolved")
    }));
    drop(child_inner);
    bus.stop();
}

#[test]
fn test_event_first_returns_zero_as_valid_first_result() {
    let bus = EventBus::new(Some("BusFirstZero".to_string()));

    bus.on_raw("value", "zero_winner", |_event| async move { Ok(json!(0)) });
    bus.on_raw("value", "late", |_event| async move { Ok(json!("late")) });

    let event = BaseEventHandle::<ValueEvent>::new(EmptyPayload {});
    {
        let mut inner = event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Serial);
    }
    let event = bus.emit(event);
    block_on(event.done());

    assert_eq!(event.first_result(), Some(json!(0)));
    bus.stop();
}

#[test]
fn test_first_returns_empty_string_as_a_valid_first_result() {
    let bus = EventBus::new(Some("FirstEmptyBus".to_string()));

    bus.on_raw(
        "value",
        "empty_winner",
        |_event| async move { Ok(json!("")) },
    );

    let result = block_on(
        bus.emit(BaseEventHandle::<ValueEvent>::new(EmptyPayload {}))
            .first(),
    )
    .expect("first result");

    assert_eq!(result, Some(json!("")));
    bus.stop();
}

#[test]
fn test_first_returns_false_as_a_valid_first_result() {
    let bus = EventBus::new(Some("FirstFalseBus".to_string()));

    bus.on_raw(
        "value",
        "false_winner",
        |_event| async move { Ok(json!(false)) },
    );

    let result = block_on(
        bus.emit(BaseEventHandle::<ValueEvent>::new(EmptyPayload {}))
            .first(),
    )
    .expect("first result");

    assert_eq!(result, Some(json!(false)));
    bus.stop();
}

#[test]
fn test_event_first_returns_none_when_all_handlers_return_null() {
    let bus = EventBus::new(Some("BusFirstAllNull".to_string()));

    bus.on_raw("value", "null_a", |_event| async move { Ok(Value::Null) });
    bus.on_raw("value", "null_b", |_event| async move { Ok(Value::Null) });

    let result = block_on(
        bus.emit(BaseEventHandle::<ValueEvent>::new(EmptyPayload {}))
            .first(),
    )
    .expect("first result");

    assert_eq!(result, None);
    bus.stop();
}

#[test]
fn test_event_first_works_with_single_handler() {
    let bus = EventBus::new(Some("BusFirstSingle".to_string()));

    bus.on_raw("value", "single", |_event| async move { Ok(json!(42)) });

    let result = block_on(
        bus.emit(BaseEventHandle::<ValueEvent>::new(EmptyPayload {}))
            .first(),
    )
    .expect("first result");

    assert_eq!(result, Some(json!(42)));
    bus.stop();
}

#[test]
fn test_event_first_skips_base_event_json_result_and_uses_next_winner() {
    let bus = EventBus::new(Some("BusFirstSkipsBaseEventJson".to_string()));
    let third_handler_called = Arc::new(AtomicBool::new(false));

    bus.on_raw("value", "base_event_result", |_event| async move {
        Ok(BaseEvent::new("FirstBaseEventSkipChild", Map::new()).to_json_value())
    });
    bus.on_raw(
        "value",
        "winner",
        |_event| async move { Ok(json!("winner")) },
    );
    let third_called_for_handler = third_handler_called.clone();
    bus.on_raw("value", "third", move |_event| {
        let third_handler_called = third_called_for_handler.clone();
        async move {
            third_handler_called.store(true, Ordering::SeqCst);
            Ok(json!("third"))
        }
    });

    let event = BaseEventHandle::<ValueEvent>::new(EmptyPayload {});
    {
        let mut inner = event.inner.inner.lock();
        inner.event_handler_completion = Some(EventHandlerCompletionMode::First);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Serial);
    }
    let event = bus.emit(event);
    block_on(event.done());

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

#[test]
fn test_first_returns_the_first_non_undefined_result_from_parallel_handlers() {
    test_event_handler_first_parallel_returns_earliest_completed_non_null_result();
}

#[test]
fn test_first_cancels_remaining_parallel_handlers_after_first_result() {
    test_event_first_parallel_returns_before_slow_loser_finishes();
}

#[test]
fn test_first_returns_the_first_non_undefined_result_from_serial_handlers() {
    test_event_handler_first_serial_stops_after_first_success();
}

#[test]
fn test_first_serial_mode_skips_first_handler_returning_undefined_takes_second() {
    test_event_first_skips_none_result_and_uses_next_winner();
}

#[test]
fn test_first_returns_undefined_when_all_handlers_return_undefined() {
    test_event_first_returns_none_when_all_handlers_return_null();
}

#[test]
fn test_first_re_raises_the_first_processing_error_when_all_handlers_throw() {
    test_first_re_raises_first_processing_error_when_all_handlers_throw();
}

#[test]
fn test_first_works_with_a_single_handler() {
    test_event_first_works_with_single_handler();
}

#[test]
fn test_first_skips_null_result_and_uses_the_next_handler_winner() {
    test_event_first_skips_none_result_and_uses_next_winner();
}

#[test]
fn test_first_returns_0_as_a_valid_first_result() {
    test_event_first_returns_zero_as_valid_first_result();
}

#[test]
fn test_first_skips_baseevent_return_values_and_uses_the_next_scalar_winner() {
    test_event_first_skips_base_event_json_result_and_uses_next_winner();
}
