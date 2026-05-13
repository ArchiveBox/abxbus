use std::{
    sync::{Arc, Mutex, MutexGuard, OnceLock},
    thread,
    time::{Duration, Instant},
};

use abxbus_rust::{
    base_event::{BaseEvent, EventResultOptions, EventWaitOptions},
    event,
    event_bus::{EventBus, EventBusOptions},
    event_handler::EventHandlerOptions,
    event_result::{EventResult, EventResultStatus},
    types::{EventHandlerCompletionMode, EventHandlerConcurrencyMode},
};
use futures::executor::block_on;
use serde_json::{json, Value};

event! {
    struct ValueEvent {
        event_result_type: Value,
        event_type: "value",
    }
}

static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn test_guard() -> MutexGuard<'static, ()> {
    TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn event_result_options(raise_if_any: bool, raise_if_none: bool) -> EventResultOptions {
    EventResultOptions {
        raise_if_any,
        raise_if_none,
        include: None,
    }
}

fn destroy_and_settle(bus: &Arc<EventBus>) {
    bus.destroy();
    thread::sleep(Duration::from_millis(10));
}

fn event_result_by_handler_name(event: &Arc<BaseEvent>, handler_name: &str) -> EventResult {
    event
        .inner
        .lock()
        .event_results
        .values()
        .find(|result| result.handler.handler_name == handler_name)
        .unwrap_or_else(|| panic!("missing event result for handler {handler_name}"))
        .clone()
}

fn event_results_except_handler_name(
    event: &Arc<BaseEvent>,
    handler_name: &str,
) -> Vec<EventResult> {
    event
        .inner
        .lock()
        .event_results
        .values()
        .filter(|result| result.handler.handler_name != handler_name)
        .cloned()
        .collect()
}

fn wait_for_error_results(event: &Arc<BaseEvent>, expected: usize, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let count = event
            .inner
            .lock()
            .event_results
            .values()
            .filter(|result| result.status == EventResultStatus::Error)
            .count();
        if count >= expected {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn is_base_event_json_for_test(value: &Value) -> bool {
    value
        .as_object()
        .is_some_and(|object| object.contains_key("event_type") && object.contains_key("event_id"))
}

#[test]
fn test_event_handler_completion_bus_default_first_serial() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("CompletionDefaultFirstBus".to_string()),
        EventBusOptions {
            event_handler_completion: EventHandlerCompletionMode::First,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let second_handler_called = Arc::new(Mutex::new(false));

    bus.on_raw("value", "first_handler", |_event| async move {
        Ok(json!("first"))
    });
    {
        let second_handler_called = second_handler_called.clone();
        bus.on_raw("value", "second_handler", move |_event| {
            let second_handler_called = second_handler_called.clone();
            async move {
                *second_handler_called.lock().unwrap() = true;
                Ok(json!("second"))
            }
        });
    }

    let emitted = bus.emit(ValueEvent::default());
    assert_eq!(emitted.inner.inner.lock().event_handler_completion, None);
    block_on(emitted.now()).expect("event should run");
    assert_eq!(emitted.inner.inner.lock().event_handler_completion, None);
    assert!(!*second_handler_called.lock().unwrap());

    let result = block_on(emitted.event_result_with_options(event_result_options(false, false)))
        .expect("event result");
    assert_eq!(result, Some(json!("first")));
    assert_eq!(
        event_result_by_handler_name(&emitted.inner, "first_handler").status,
        EventResultStatus::Completed
    );
    assert_eq!(
        event_result_by_handler_name(&emitted.inner, "second_handler").status,
        EventResultStatus::Error
    );
    destroy_and_settle(&bus);
}

#[test]
fn test_event_handler_completion_explicit_override_beats_bus_default() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("CompletionOverrideBus".to_string()),
        EventBusOptions {
            event_handler_completion: EventHandlerCompletionMode::First,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let second_handler_called = Arc::new(Mutex::new(false));

    bus.on_raw("value", "first_handler", |_event| async move {
        Ok(json!("first"))
    });
    {
        let second_handler_called = second_handler_called.clone();
        bus.on_raw("value", "second_handler", move |_event| {
            let second_handler_called = second_handler_called.clone();
            async move {
                *second_handler_called.lock().unwrap() = true;
                Ok(json!("second"))
            }
        });
    }

    let event = ValueEvent {
        event_handler_completion: Some(EventHandlerCompletionMode::All),
        ..Default::default()
    };
    let emitted = bus.emit(event);
    assert_eq!(
        emitted.inner.inner.lock().event_handler_completion,
        Some(EventHandlerCompletionMode::All)
    );
    block_on(emitted.now()).expect("event should run");
    assert!(*second_handler_called.lock().unwrap());
    destroy_and_settle(&bus);
}

#[test]
fn test_event_parallel_first_races_and_cancels_non_winners() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("CompletionParallelFirstBus".to_string()),
        EventBusOptions {
            event_handler_completion: EventHandlerCompletionMode::All,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let slow_started = Arc::new(Mutex::new(false));

    {
        let slow_started = slow_started.clone();
        bus.on_raw("value", "slow_handler_started", move |_event| {
            let slow_started = slow_started.clone();
            async move {
                *slow_started.lock().unwrap() = true;
                thread::sleep(Duration::from_millis(500));
                Ok(json!("slow-started"))
            }
        });
    }
    bus.on_raw("value", "fast_winner", |_event| async move {
        thread::sleep(Duration::from_millis(10));
        Ok(json!("winner"))
    });
    bus.on_raw(
        "value",
        "slow_handler_pending_or_started",
        |_event| async move {
            thread::sleep(Duration::from_millis(500));
            Ok(json!("slow-other"))
        },
    );

    let emitted = bus.emit(ValueEvent {
        event_handler_completion: Some(EventHandlerCompletionMode::First),
        event_handler_concurrency: Some(EventHandlerConcurrencyMode::Parallel),
        ..Default::default()
    });
    let started = Instant::now();
    block_on(emitted.now()).expect("event should run");
    assert!(started.elapsed() < Duration::from_millis(200));
    assert!(*slow_started.lock().unwrap());
    assert!(wait_for_error_results(
        &emitted.inner,
        2,
        Duration::from_millis(200)
    ));

    let winner = event_result_by_handler_name(&emitted.inner, "fast_winner");
    assert_eq!(winner.status, EventResultStatus::Completed);
    assert_eq!(winner.error, None);
    assert_eq!(winner.result, Some(json!("winner")));
    assert!(
        event_results_except_handler_name(&emitted.inner, "fast_winner")
            .iter()
            .all(|result| result.status == EventResultStatus::Error)
    );

    let result = block_on(emitted.event_result_with_options(event_result_options(false, true)))
        .expect("event result");
    assert_eq!(result, Some(json!("winner")));
    destroy_and_settle(&bus);
}

#[test]
fn test_event_handler_completion_explicit_first_cancels_parallel_losers() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("CompletionFirstShortcutBus".to_string()),
        EventBusOptions {
            event_handler_completion: EventHandlerCompletionMode::All,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    let slow_handler_completed = Arc::new(Mutex::new(false));

    bus.on_raw("value", "fast_handler", |_event| async move {
        thread::sleep(Duration::from_millis(10));
        Ok(json!("fast"))
    });
    {
        let slow_handler_completed = slow_handler_completed.clone();
        bus.on_raw("value", "slow_handler", move |_event| {
            let slow_handler_completed = slow_handler_completed.clone();
            async move {
                thread::sleep(Duration::from_millis(500));
                *slow_handler_completed.lock().unwrap() = true;
                Ok(json!("slow"))
            }
        });
    }

    let emitted = bus.emit(ValueEvent {
        event_handler_completion: Some(EventHandlerCompletionMode::First),
        ..Default::default()
    });
    block_on(emitted.now_with_options(EventWaitOptions {
        timeout: None,
        first_result: true,
    }))
    .expect("first result");
    let result = block_on(emitted.event_result_with_options(event_result_options(false, false)))
        .expect("event result");
    assert_eq!(result, Some(json!("fast")));
    assert_eq!(
        emitted.inner.inner.lock().event_handler_completion,
        Some(EventHandlerCompletionMode::First)
    );
    assert!(wait_for_error_results(
        &emitted.inner,
        1,
        Duration::from_millis(200)
    ));
    assert!(!*slow_handler_completed.lock().unwrap());
    destroy_and_settle(&bus);
}

#[test]
fn test_event_handler_completion_first_preserves_falsy_results() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("CompletionFalsyBus".to_string()),
        EventBusOptions {
            event_handler_completion: EventHandlerCompletionMode::All,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let second_handler_called = Arc::new(Mutex::new(false));

    bus.on_raw(
        "value",
        "zero_handler",
        |_event| async move { Ok(json!(0)) },
    );
    {
        let second_handler_called = second_handler_called.clone();
        bus.on_raw("value", "second_handler", move |_event| {
            let second_handler_called = second_handler_called.clone();
            async move {
                *second_handler_called.lock().unwrap() = true;
                Ok(json!(99))
            }
        });
    }

    let emitted = bus.emit(ValueEvent {
        event_handler_completion: Some(EventHandlerCompletionMode::First),
        ..Default::default()
    });
    block_on(emitted.now_with_options(EventWaitOptions {
        timeout: None,
        first_result: true,
    }))
    .expect("first result");
    let result = block_on(emitted.event_result_with_options(event_result_options(false, false)))
        .expect("event result");
    assert_eq!(result, Some(json!(0)));
    assert!(!*second_handler_called.lock().unwrap());
    destroy_and_settle(&bus);
}

#[test]
fn test_event_handler_completion_first_preserves_false_and_empty_string_results() {
    let _guard = test_guard();
    let bool_bus = EventBus::new_with_options(
        Some("CompletionFalsyFalseBus".to_string()),
        EventBusOptions {
            event_handler_completion: EventHandlerCompletionMode::All,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let bool_second_handler_called = Arc::new(Mutex::new(false));
    bool_bus.on_raw("value", "bool_first_handler", |_event| async move {
        Ok(json!(false))
    });
    {
        let bool_second_handler_called = bool_second_handler_called.clone();
        bool_bus.on_raw("value", "bool_second_handler", move |_event| {
            let bool_second_handler_called = bool_second_handler_called.clone();
            async move {
                *bool_second_handler_called.lock().unwrap() = true;
                Ok(json!(true))
            }
        });
    }
    let bool_event = bool_bus.emit(ValueEvent {
        event_handler_completion: Some(EventHandlerCompletionMode::First),
        ..Default::default()
    });
    block_on(bool_event.now_with_options(EventWaitOptions {
        timeout: None,
        first_result: true,
    }))
    .expect("first bool result");
    let bool_result =
        block_on(bool_event.event_result_with_options(event_result_options(false, false)))
            .expect("bool result");
    assert_eq!(bool_result, Some(json!(false)));
    assert!(!*bool_second_handler_called.lock().unwrap());
    destroy_and_settle(&bool_bus);

    let str_bus = EventBus::new_with_options(
        Some("CompletionFalsyEmptyStringBus".to_string()),
        EventBusOptions {
            event_handler_completion: EventHandlerCompletionMode::All,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let str_second_handler_called = Arc::new(Mutex::new(false));
    str_bus.on_raw("value", "str_first_handler", |_event| async move {
        Ok(json!(""))
    });
    {
        let str_second_handler_called = str_second_handler_called.clone();
        str_bus.on_raw("value", "str_second_handler", move |_event| {
            let str_second_handler_called = str_second_handler_called.clone();
            async move {
                *str_second_handler_called.lock().unwrap() = true;
                Ok(json!("second"))
            }
        });
    }
    let str_event = str_bus.emit(ValueEvent {
        event_handler_completion: Some(EventHandlerCompletionMode::First),
        ..Default::default()
    });
    block_on(str_event.now_with_options(EventWaitOptions {
        timeout: None,
        first_result: true,
    }))
    .expect("first string result");
    let str_result =
        block_on(str_event.event_result_with_options(event_result_options(false, false)))
            .expect("string result");
    assert_eq!(str_result, Some(json!("")));
    assert!(!*str_second_handler_called.lock().unwrap());
    destroy_and_settle(&str_bus);
}

#[test]
fn test_event_handler_completion_first_skips_none_result_and_uses_next_winner() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("CompletionNoneSkipBus".to_string()),
        EventBusOptions {
            event_handler_completion: EventHandlerCompletionMode::All,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let third_handler_called = Arc::new(Mutex::new(false));
    bus.on_raw(
        "value",
        "none_handler",
        |_event| async move { Ok(Value::Null) },
    );
    bus.on_raw("value", "winner_handler", |_event| async move {
        Ok(json!("winner"))
    });
    {
        let third_handler_called = third_handler_called.clone();
        bus.on_raw("value", "third_handler", move |_event| {
            let third_handler_called = third_handler_called.clone();
            async move {
                *third_handler_called.lock().unwrap() = true;
                Ok(json!("third"))
            }
        });
    }

    let emitted = bus.emit(ValueEvent {
        event_handler_completion: Some(EventHandlerCompletionMode::First),
        ..Default::default()
    });
    block_on(emitted.now_with_options(EventWaitOptions {
        timeout: None,
        first_result: true,
    }))
    .expect("first result");
    let result = block_on(emitted.event_result_with_options(event_result_options(false, false)))
        .expect("event result");
    assert_eq!(result, Some(json!("winner")));
    assert!(!*third_handler_called.lock().unwrap());
    assert_eq!(
        event_result_by_handler_name(&emitted.inner, "none_handler").result,
        Some(Value::Null)
    );
    assert_eq!(
        event_result_by_handler_name(&emitted.inner, "winner_handler").result,
        Some(json!("winner"))
    );
    destroy_and_settle(&bus);
}

#[test]
fn test_event_handler_completion_first_skips_baseevent_result_and_uses_next_winner() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("CompletionBaseEventSkipBus".to_string()),
        EventBusOptions {
            event_handler_completion: EventHandlerCompletionMode::All,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let third_handler_called = Arc::new(Mutex::new(false));
    bus.on_raw("value", "baseevent_handler", |_event| async move {
        Ok(BaseEvent::new("ChildCompletionEvent", Default::default()).to_json_value())
    });
    bus.on_raw("value", "winner_handler", |_event| async move {
        Ok(json!("winner"))
    });
    {
        let third_handler_called = third_handler_called.clone();
        bus.on_raw("value", "third_handler", move |_event| {
            let third_handler_called = third_handler_called.clone();
            async move {
                *third_handler_called.lock().unwrap() = true;
                Ok(json!("third"))
            }
        });
    }

    let emitted = bus.emit(ValueEvent {
        event_handler_completion: Some(EventHandlerCompletionMode::First),
        ..Default::default()
    });
    block_on(emitted.now_with_options(EventWaitOptions {
        timeout: None,
        first_result: true,
    }))
    .expect("first result");
    let result = block_on(emitted.event_result_with_options(event_result_options(false, false)))
        .expect("event result");
    assert_eq!(result, Some(json!("winner")));
    assert!(!*third_handler_called.lock().unwrap());
    assert!(is_base_event_json_for_test(
        event_result_by_handler_name(&emitted.inner, "baseevent_handler")
            .result
            .as_ref()
            .expect("baseevent result")
    ));
    destroy_and_settle(&bus);
}

#[test]
fn test_now_runs_all_handlers_and_event_result_returns_first_valid_result() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("CompletionNowAllBus".to_string()),
        EventBusOptions {
            event_handler_completion: EventHandlerCompletionMode::First,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let late_handler_called = Arc::new(Mutex::new(false));
    bus.on_raw("value", "baseevent_handler", |_event| async move {
        Ok(BaseEvent::new("NowAllChildEvent", Default::default()).to_json_value())
    });
    bus.on_raw(
        "value",
        "none_handler",
        |_event| async move { Ok(Value::Null) },
    );
    bus.on_raw("value", "winner_handler", |_event| async move {
        Ok(json!("winner"))
    });
    {
        let late_handler_called = late_handler_called.clone();
        bus.on_raw("value", "late_handler", move |_event| {
            let late_handler_called = late_handler_called.clone();
            async move {
                *late_handler_called.lock().unwrap() = true;
                Ok(json!("late"))
            }
        });
    }

    let emitted = bus.emit(ValueEvent {
        event_handler_completion: Some(EventHandlerCompletionMode::All),
        event_handler_concurrency: Some(EventHandlerConcurrencyMode::Serial),
        ..Default::default()
    });
    block_on(emitted.now()).expect("event should run");
    let result =
        block_on(emitted.event_result_with_options(EventResultOptions::default())).expect("result");
    assert_eq!(result, Some(json!("winner")));
    assert_eq!(
        emitted.inner.inner.lock().event_handler_completion,
        Some(EventHandlerCompletionMode::All)
    );
    assert!(*late_handler_called.lock().unwrap());
    destroy_and_settle(&bus);
}

#[test]
fn test_event_now_default_error_policy() {
    let _guard = test_guard();

    let no_handler_bus = EventBus::new(Some("CompletionNowNoHandlerBus".to_string()));
    let no_handler_event = block_on(no_handler_bus.emit(ValueEvent::default()).now())
        .expect("no handler event should complete");
    let no_handler_result =
        block_on(no_handler_event.event_result_with_options(event_result_options(false, false)))
            .expect("no handler result");
    assert_eq!(no_handler_result, None);
    destroy_and_settle(&no_handler_bus);

    let none_bus = EventBus::new(Some("CompletionNowNoneBus".to_string()));
    none_bus.on_raw(
        "value",
        "none_handler",
        |_event| async move { Ok(Value::Null) },
    );
    let none_event =
        block_on(none_bus.emit(ValueEvent::default()).now()).expect("none event should complete");
    let none_result =
        block_on(none_event.event_result_with_options(event_result_options(false, false)))
            .expect("none result");
    assert_eq!(none_result, None);
    destroy_and_settle(&none_bus);

    let all_error_bus = EventBus::new(Some("CompletionNowAllErrorBus".to_string()));
    all_error_bus.on_raw("value", "fail_one", |_event| async move {
        Err("now boom 1".to_string())
    });
    all_error_bus.on_raw("value", "fail_two", |_event| async move {
        Err("now boom 2".to_string())
    });
    let all_error_event = block_on(all_error_bus.emit(ValueEvent::default()).now())
        .expect("all-error event should complete");
    let all_error =
        block_on(all_error_event.event_result_with_options(EventResultOptions::default()))
            .expect_err("default event_result should surface handler errors");
    assert!(all_error.contains("2 handler error"));
    destroy_and_settle(&all_error_bus);

    let mixed_valid_bus = EventBus::new(Some("CompletionNowMixedValidBus".to_string()));
    mixed_valid_bus.on_raw("value", "fail_one", |_event| async move {
        Err("now boom 1".to_string())
    });
    mixed_valid_bus.on_raw(
        "value",
        "winner",
        |_event| async move { Ok(json!("winner")) },
    );
    let mixed_valid_event = block_on(mixed_valid_bus.emit(ValueEvent::default()).now())
        .expect("mixed-valid event should complete");
    let mixed_valid_result =
        block_on(mixed_valid_event.event_result_with_options(event_result_options(false, false)))
            .expect("mixed-valid result");
    assert_eq!(mixed_valid_result, Some(json!("winner")));
    destroy_and_settle(&mixed_valid_bus);

    let mixed_none_bus = EventBus::new(Some("CompletionNowMixedNoneBus".to_string()));
    mixed_none_bus.on_raw("value", "fail_one", |_event| async move {
        Err("now boom 1".to_string())
    });
    mixed_none_bus.on_raw(
        "value",
        "none_handler",
        |_event| async move { Ok(Value::Null) },
    );
    let mixed_none_event = block_on(mixed_none_bus.emit(ValueEvent::default()).now())
        .expect("mixed-none event should complete");
    let mixed_none_result =
        block_on(mixed_none_event.event_result_with_options(event_result_options(false, false)))
            .expect("mixed-none result");
    assert_eq!(mixed_none_result, None);
    destroy_and_settle(&mixed_none_bus);
}

#[test]
fn test_event_result_options_match_event_results_shape() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("CompletionNowOptionsBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );

    bus.on_raw("value", "fail_handler", |_event| async move {
        Err("now option boom".to_string())
    });
    bus.on_raw("value", "first_valid", |_event| async move {
        Ok(json!("first"))
    });
    bus.on_raw("value", "second_valid", |_event| async move {
        Ok(json!("second"))
    });

    let emitted = block_on(bus.emit(ValueEvent::default()).now()).expect("event should run");
    let error = block_on(emitted.event_result_with_options(event_result_options(true, false)))
        .expect_err("RaiseIfAny should surface handler errors");
    assert!(error.contains("now option boom"));

    let filtered = block_on(bus.emit(ValueEvent::default()).now()).expect("event should run");
    let result = block_on(filtered.event_result_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: false,
        include: Some(Arc::new(|result, event_result| {
            assert_eq!(result, event_result.result.as_ref());
            event_result.status == EventResultStatus::Completed && result == Some(&json!("second"))
        })),
    }))
    .expect("filtered result");
    assert_eq!(result, Some(json!("second")));
    destroy_and_settle(&bus);
}

#[test]
fn test_event_result_returns_first_valid_result_by_registration_order_after_now() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("CompletionNowRegistrationOrderBus".to_string()),
        EventBusOptions {
            event_handler_completion: EventHandlerCompletionMode::All,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );

    let registered_at = "2026-01-01T00:00:00.000Z".to_string();
    bus.on_raw_with_options(
        "value",
        "slow_handler",
        EventHandlerOptions {
            id: Some("00000000-0000-5000-8000-00000000000b".to_string()),
            handler_registered_at: Some(registered_at.clone()),
            ..EventHandlerOptions::default()
        },
        |_event| async move {
            thread::sleep(Duration::from_millis(50));
            Ok(json!("slow"))
        },
    );
    bus.on_raw_with_options(
        "value",
        "fast_handler",
        EventHandlerOptions {
            id: Some("00000000-0000-5000-8000-00000000000c".to_string()),
            handler_registered_at: Some(registered_at),
            ..EventHandlerOptions::default()
        },
        |_event| async move {
            thread::sleep(Duration::from_millis(1));
            Ok(json!("fast"))
        },
    );

    let emitted = block_on(
        bus.emit(ValueEvent {
            event_handler_completion: Some(EventHandlerCompletionMode::All),
            event_handler_concurrency: Some(EventHandlerConcurrencyMode::Parallel),
            ..Default::default()
        })
        .now(),
    )
    .expect("event should run");
    let result =
        block_on(emitted.event_result_with_options(EventResultOptions::default())).expect("result");
    assert_eq!(result, Some(json!("slow")));
    destroy_and_settle(&bus);
}

#[test]
fn test_event_handler_completion_first_returns_none_when_all_handlers_fail() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("CompletionAllFailBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    bus.on_raw("value", "fail_fast", |_event| async move {
        Err("boom1".to_string())
    });
    bus.on_raw("value", "fail_slow", |_event| async move {
        thread::sleep(Duration::from_millis(10));
        Err("boom2".to_string())
    });

    let emitted = bus.emit(ValueEvent {
        event_handler_completion: Some(EventHandlerCompletionMode::First),
        ..Default::default()
    });
    block_on(emitted.now_with_options(EventWaitOptions {
        timeout: None,
        first_result: true,
    }))
    .expect("all-fail event should complete");
    let result = block_on(emitted.event_result_with_options(event_result_options(false, false)))
        .expect("suppressed result");
    assert_eq!(result, None);
    destroy_and_settle(&bus);
}

#[test]
fn test_event_handler_completion_first_result_options_match_event_result_options() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("CompletionFirstOptionsBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    bus.on_raw("value", "fail_fast", |_event| async move {
        Err("first option boom".to_string())
    });
    bus.on_raw("value", "slow_winner", |_event| async move {
        thread::sleep(Duration::from_millis(10));
        Ok(json!("winner"))
    });

    let emitted = bus.emit(ValueEvent {
        event_handler_completion: Some(EventHandlerCompletionMode::First),
        ..Default::default()
    });
    block_on(emitted.now_with_options(EventWaitOptions {
        timeout: None,
        first_result: true,
    }))
    .expect("first result");
    let result = block_on(emitted.event_result_with_options(event_result_options(false, false)))
        .expect("suppressed result");
    assert_eq!(result, Some(json!("winner")));

    let error_event = bus.emit(ValueEvent {
        event_handler_completion: Some(EventHandlerCompletionMode::First),
        ..Default::default()
    });
    block_on(error_event.now_with_options(EventWaitOptions {
        timeout: None,
        first_result: true,
    }))
    .expect("first result");
    let error = block_on(error_event.event_result_with_options(event_result_options(true, false)))
        .expect_err("RaiseIfAny should surface handler errors");
    assert!(error.contains("first option boom"));

    let none_bus = EventBus::new(Some("CompletionFirstRaiseNoneBus".to_string()));
    none_bus.on_raw(
        "value",
        "none_handler",
        |_event| async move { Ok(Value::Null) },
    );
    let none_event = none_bus.emit(ValueEvent {
        event_handler_completion: Some(EventHandlerCompletionMode::First),
        ..Default::default()
    });
    block_on(none_event.now_with_options(EventWaitOptions {
        timeout: None,
        first_result: true,
    }))
    .expect("none event");
    let none_error =
        block_on(none_event.event_result_with_options(event_result_options(true, true)))
            .expect_err("RaiseIfNone should reject no-result first completion");
    assert!(none_error.contains("Expected at least one handler"));
    destroy_and_settle(&none_bus);
    destroy_and_settle(&bus);
}

#[test]
fn test_now_first_result_timeout_limits_processing_wait() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("CompletionFirstTimeoutBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_timeout: None,
            ..EventBusOptions::default()
        },
    );
    bus.on_raw("value", "slow_handler", |_event| async move {
        thread::sleep(Duration::from_millis(500));
        Ok(json!("slow"))
    });

    let emitted = bus.emit(ValueEvent {
        event_handler_completion: Some(EventHandlerCompletionMode::First),
        ..Default::default()
    });
    let error = block_on(emitted.now_with_options(EventWaitOptions {
        timeout: Some(0.01),
        first_result: true,
    }))
    .expect_err("now first_result should time out");
    assert!(error.contains("Timed out"));
    destroy_and_settle(&bus);
}

#[test]
fn test_event_result_include_callback_receives_result_and_event_result() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("CompletionFirstIncludeBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let seen = Arc::new(Mutex::new(Vec::new()));
    bus.on_raw(
        "value",
        "none_handler",
        |_event| async move { Ok(Value::Null) },
    );
    bus.on_raw("value", "second_handler", |_event| async move {
        Ok(json!("second"))
    });

    let emitted = bus.emit(ValueEvent {
        event_handler_completion: Some(EventHandlerCompletionMode::First),
        ..Default::default()
    });
    block_on(emitted.now_with_options(EventWaitOptions {
        timeout: None,
        first_result: true,
    }))
    .expect("first result");
    let seen_for_include = seen.clone();
    let result = block_on(emitted.event_result_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: false,
        include: Some(Arc::new(move |result, event_result| {
            assert_eq!(result, event_result.result.as_ref());
            seen_for_include
                .lock()
                .unwrap()
                .push((result.cloned(), event_result.handler.handler_name.clone()));
            event_result.status == EventResultStatus::Completed && result == Some(&json!("second"))
        })),
    }))
    .expect("filtered result");

    assert_eq!(result, Some(json!("second")));
    assert_eq!(
        seen.lock().unwrap().as_slice(),
        &[
            (Some(Value::Null), "none_handler".to_string()),
            (Some(json!("second")), "second_handler".to_string()),
        ]
    );
    destroy_and_settle(&bus);
}

#[test]
fn test_event_results_include_callback_receives_result_and_event_result() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("CompletionResultsListIncludeBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let seen = Arc::new(Mutex::new(Vec::new()));
    bus.on_raw("value", "keep_handler", |_event| async move {
        Ok(json!("keep"))
    });
    bus.on_raw("value", "drop_handler", |_event| async move {
        Ok(json!("drop"))
    });

    let emitted = block_on(bus.emit(ValueEvent::default()).now()).expect("event should run");
    let seen_for_include = seen.clone();
    let results = block_on(emitted.event_results_list_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: true,
        include: Some(Arc::new(move |result, event_result| {
            assert_eq!(result, event_result.result.as_ref());
            seen_for_include
                .lock()
                .unwrap()
                .push((result.cloned(), event_result.handler.handler_name.clone()));
            result == Some(&json!("keep"))
        })),
    }))
    .expect("filtered results");

    assert_eq!(results, vec![json!("keep")]);
    assert_eq!(
        seen.lock().unwrap().as_slice(),
        &[
            (Some(json!("keep")), "keep_handler".to_string()),
            (Some(json!("drop")), "drop_handler".to_string()),
        ]
    );
    destroy_and_settle(&bus);
}

#[test]
fn test_event_result_returns_first_current_result_with_first_result_wait() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("CompletionFirstCurrentResultBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    bus.on_raw("value", "slow_handler", |_event| async move {
        thread::sleep(Duration::from_millis(50));
        Ok(json!("slow"))
    });
    bus.on_raw("value", "fast_handler", |_event| async move {
        thread::sleep(Duration::from_millis(1));
        Ok(json!("fast"))
    });

    let emitted = bus.emit(ValueEvent::default());
    block_on(emitted.now_with_options(EventWaitOptions {
        timeout: None,
        first_result: true,
    }))
    .expect("first result wait");
    let result =
        block_on(emitted.event_result_with_options(EventResultOptions::default())).expect("result");
    assert_eq!(result, Some(json!("fast")));
    destroy_and_settle(&bus);
}

#[test]
fn test_event_result_raise_if_any_includes_first_mode_control_errors() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("CompletionFirstControlErrorBus".to_string()),
        EventBusOptions {
            event_handler_completion: EventHandlerCompletionMode::All,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    bus.on_raw("value", "fast_handler", |_event| async move {
        thread::sleep(Duration::from_millis(10));
        Ok(json!("fast"))
    });
    bus.on_raw("value", "slow_handler", |_event| async move {
        thread::sleep(Duration::from_millis(500));
        Ok(json!("slow"))
    });

    let emitted = bus.emit(ValueEvent {
        event_handler_completion: Some(EventHandlerCompletionMode::First),
        ..Default::default()
    });
    block_on(emitted.now_with_options(EventWaitOptions {
        timeout: None,
        first_result: true,
    }))
    .expect("first result");
    assert!(wait_for_error_results(
        &emitted.inner,
        1,
        Duration::from_millis(200)
    ));
    let error = block_on(emitted.event_result_with_options(event_result_options(true, false)))
        .expect_err("RaiseIfAny should include first-mode cancellation errors");
    assert!(error.contains("first result resolved"));
    let result = block_on(emitted.event_result_with_options(event_result_options(false, true)))
        .expect("suppressed result");
    assert_eq!(result, Some(json!("fast")));
    destroy_and_settle(&bus);
}
