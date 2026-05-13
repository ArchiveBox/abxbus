use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use abxbus_rust::{
    base_event::{BaseEvent, EventResultOptions, EventWaitOptions},
    event,
    event_bus::{EventBus, EventBusOptions},
    event_result::EventResultStatus,
    types::{EventHandlerCompletionMode, EventHandlerConcurrencyMode},
};
use futures::executor::block_on;
use serde_json::{json, Map, Value};

event! {
    struct CompletionEvent {
        event_result_type: Value,
    }
}

fn non_raising_result_options() -> EventResultOptions {
    EventResultOptions {
        raise_if_any: false,
        raise_if_none: false,
        include: None,
    }
}

event! {
    struct ConcurrencyEvent {
        event_result_type: Value,
    }
}

fn bump_in_flight(in_flight: &Arc<Mutex<i64>>, max_in_flight: &Arc<Mutex<i64>>) {
    let current = {
        let mut in_flight = in_flight.lock().expect("in_flight lock");
        *in_flight += 1;
        *in_flight
    };
    let mut max_seen = max_in_flight.lock().expect("max lock");
    *max_seen = (*max_seen).max(current);
}

fn drop_in_flight(in_flight: &Arc<Mutex<i64>>) {
    let mut in_flight = in_flight.lock().expect("in_flight lock");
    *in_flight -= 1;
}

fn emit_with_first_completion(
    bus: &Arc<EventBus>,
) -> abxbus_rust::typed::BaseEventHandle<CompletionEvent> {
    let event = CompletionEvent {
        event_handler_completion: Some(EventHandlerCompletionMode::First),
        ..Default::default()
    };
    let event = bus.emit(event);
    let _ = block_on(event.now_with_options(EventWaitOptions {
        timeout: None,
        first_result: true,
    }));
    event
}

#[test]
fn test_event_handler_completion_bus_default_first_serial() {
    let bus = EventBus::new_with_options(
        Some("CompletionDefaultFirstBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_handler_completion: EventHandlerCompletionMode::First,
            ..EventBusOptions::default()
        },
    );
    let second_handler_called = Arc::new(Mutex::new(false));

    bus.on_raw("CompletionEvent", "first_handler", |_event| async move {
        Ok(json!("first"))
    });
    let second_handler_called_for_handler = second_handler_called.clone();
    bus.on_raw("CompletionEvent", "second_handler", move |_event| {
        let second_handler_called = second_handler_called_for_handler.clone();
        async move {
            *second_handler_called.lock().expect("called lock") = true;
            Ok(json!("second"))
        }
    });

    let event = bus.emit(CompletionEvent {
        ..Default::default()
    });
    assert_eq!(event.inner.inner.lock().event_handler_completion, None);
    let _ = block_on(event.now());

    assert!(!*second_handler_called.lock().expect("called lock"));
    assert_eq!(
        block_on(event.event_result(non_raising_result_options())).expect("first result"),
        Some(json!("first"))
    );
    let results = event.inner.inner.lock().event_results.clone();
    let first_result = results
        .values()
        .find(|result| result.handler.handler_name == "first_handler")
        .expect("first result");
    let second_result = results
        .values()
        .find(|result| result.handler.handler_name == "second_handler")
        .expect("second result");
    assert_eq!(first_result.status, EventResultStatus::Completed);
    assert_eq!(second_result.status, EventResultStatus::Error);
    assert!(second_result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("Cancelled: first() resolved"));
    bus.stop();
}

#[test]
fn test_event_handler_completion_explicit_override_beats_bus_default() {
    let bus = EventBus::new_with_options(
        Some("CompletionOverrideBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_handler_completion: EventHandlerCompletionMode::First,
            ..EventBusOptions::default()
        },
    );
    let second_handler_called = Arc::new(Mutex::new(false));

    bus.on_raw("CompletionEvent", "first_handler", |_event| async move {
        Ok(json!("first"))
    });
    let second_handler_called_for_handler = second_handler_called.clone();
    bus.on_raw("CompletionEvent", "second_handler", move |_event| {
        let second_handler_called = second_handler_called_for_handler.clone();
        async move {
            *second_handler_called.lock().expect("called lock") = true;
            Ok(json!("second"))
        }
    });

    let event = CompletionEvent {
        event_handler_completion: Some(EventHandlerCompletionMode::All),
        ..Default::default()
    };
    let event = bus.emit(event);
    assert_eq!(
        event.inner.inner.lock().event_handler_completion,
        Some(EventHandlerCompletionMode::All)
    );
    let _ = block_on(event.now());

    assert!(*second_handler_called.lock().expect("called lock"));
    let results = event.inner.inner.lock().event_results.clone();
    assert!(results
        .values()
        .all(|result| result.status == EventResultStatus::Completed));
    bus.stop();
}

#[test]
fn test_event_parallel_first_races_and_cancels_non_winners() {
    let bus = EventBus::new_with_options(
        Some("CompletionParallelFirstBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_handler_completion: EventHandlerCompletionMode::All,
            ..EventBusOptions::default()
        },
    );
    let slow_started = Arc::new(Mutex::new(false));

    let slow_started_for_handler = slow_started.clone();
    bus.on_raw("CompletionEvent", "slow_handler_started", move |_event| {
        let slow_started = slow_started_for_handler.clone();
        async move {
            *slow_started.lock().expect("slow started lock") = true;
            thread::sleep(Duration::from_millis(500));
            Ok(json!("slow-started"))
        }
    });
    bus.on_raw("CompletionEvent", "fast_winner", |_event| async move {
        thread::sleep(Duration::from_millis(10));
        Ok(json!("winner"))
    });
    bus.on_raw(
        "CompletionEvent",
        "slow_handler_pending_or_started",
        |_event| async move {
            thread::sleep(Duration::from_millis(500));
            Ok(json!("slow-other"))
        },
    );

    let event = CompletionEvent {
        event_handler_concurrency: Some(EventHandlerConcurrencyMode::Parallel),
        event_handler_completion: Some(EventHandlerCompletionMode::First),
        ..Default::default()
    };
    let event = bus.emit(event);
    let started = std::time::Instant::now();
    let _ = block_on(event.now());

    assert!(started.elapsed() < Duration::from_millis(200));
    assert!(*slow_started.lock().expect("slow started lock"));
    assert_eq!(
        block_on(event.event_result(non_raising_result_options())).expect("first result"),
        Some(json!("winner"))
    );

    let results = event.inner.inner.lock().event_results.clone();
    let winner_result = results
        .values()
        .find(|result| result.handler.handler_name == "fast_winner")
        .expect("winner result");
    assert_eq!(winner_result.status, EventResultStatus::Completed);
    assert_eq!(winner_result.error, None);
    assert_eq!(winner_result.result, Some(json!("winner")));
    let loser_results: Vec<_> = results
        .values()
        .filter(|result| result.handler.handler_name != "fast_winner")
        .collect();
    assert_eq!(loser_results.len(), 2);
    assert!(loser_results
        .iter()
        .all(|result| result.status == EventResultStatus::Error));
    assert!(loser_results.iter().all(|result| result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("first() resolved")));
    bus.stop();
}

#[test]
fn test_event_handler_completion_first_cancels_parallel_losers() {
    let bus = EventBus::new_with_options(
        Some("CompletionFirstShortcutBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            event_handler_completion: EventHandlerCompletionMode::All,
            ..EventBusOptions::default()
        },
    );
    let slow_handler_completed = Arc::new(Mutex::new(false));

    bus.on_raw("CompletionEvent", "fast_handler", |_event| async move {
        thread::sleep(Duration::from_millis(10));
        Ok(json!("fast"))
    });
    let slow_handler_completed_for_handler = slow_handler_completed.clone();
    bus.on_raw("CompletionEvent", "slow_handler", move |_event| {
        let slow_handler_completed = slow_handler_completed_for_handler.clone();
        async move {
            thread::sleep(Duration::from_millis(500));
            *slow_handler_completed.lock().expect("slow completed lock") = true;
            Ok(json!("slow"))
        }
    });

    let event = bus.emit(CompletionEvent {
        event_handler_completion: Some(EventHandlerCompletionMode::First),
        ..Default::default()
    });
    assert_eq!(
        event.inner.inner.lock().event_handler_completion,
        Some(EventHandlerCompletionMode::First)
    );
    let _ = block_on(event.now());
    block_on(bus.wait_until_idle(Some(2.0)));
    for _ in 0..100 {
        if event
            .inner
            .inner
            .lock()
            .event_results
            .values()
            .any(|result| {
                result.status == EventResultStatus::Completed
                    && result.result == Some(json!("fast"))
            })
        {
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    let first_value = event
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .find(|result| {
            result.status == EventResultStatus::Completed && result.result == Some(json!("fast"))
        })
        .and_then(|result| result.result.clone());

    assert_eq!(first_value, Some(json!("fast")));
    assert_eq!(
        event.inner.inner.lock().event_handler_completion,
        Some(EventHandlerCompletionMode::First)
    );
    assert!(!*slow_handler_completed.lock().expect("slow completed lock"));
    assert!(event
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .any(|result| result.status == EventResultStatus::Error
            && result
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("first() resolved")));
    bus.stop();
}

#[test]
fn test_event_first_preserves_falsy_results() {
    let bus = EventBus::new_with_options(
        Some("CompletionFalsyBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_handler_completion: EventHandlerCompletionMode::All,
            ..EventBusOptions::default()
        },
    );
    let second_handler_called = Arc::new(Mutex::new(false));

    bus.on_raw("CompletionEvent", "zero_handler", |_event| async move {
        Ok(json!(0))
    });
    let second_handler_called_for_handler = second_handler_called.clone();
    bus.on_raw("CompletionEvent", "second_handler", move |_event| {
        let second_handler_called = second_handler_called_for_handler.clone();
        async move {
            *second_handler_called.lock().expect("called lock") = true;
            Ok(json!(99))
        }
    });

    let event = emit_with_first_completion(&bus);
    let result = block_on(event.event_result(non_raising_result_options())).expect("first result");
    assert_eq!(result, Some(json!(0)));
    assert!(!*second_handler_called.lock().expect("called lock"));
    bus.stop();
}

#[test]
fn test_event_first_preserves_false_and_empty_string_results() {
    let false_bus = EventBus::new_with_options(
        Some("CompletionFalsyFalseBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_handler_completion: EventHandlerCompletionMode::All,
            ..EventBusOptions::default()
        },
    );
    let false_second_called = Arc::new(Mutex::new(false));
    false_bus.on_raw("CompletionEvent", "false_handler", |_event| async move {
        Ok(json!(false))
    });
    let false_second_called_for_handler = false_second_called.clone();
    false_bus.on_raw("CompletionEvent", "second_handler", move |_event| {
        let false_second_called = false_second_called_for_handler.clone();
        async move {
            *false_second_called.lock().expect("called lock") = true;
            Ok(json!(true))
        }
    });
    let false_event = emit_with_first_completion(&false_bus);
    let false_result =
        block_on(false_event.event_result(non_raising_result_options())).expect("first result");
    assert_eq!(false_result, Some(json!(false)));
    assert!(!*false_second_called.lock().expect("called lock"));
    false_bus.stop();

    let str_bus = EventBus::new_with_options(
        Some("CompletionFalsyEmptyStringBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_handler_completion: EventHandlerCompletionMode::All,
            ..EventBusOptions::default()
        },
    );
    let str_second_called = Arc::new(Mutex::new(false));
    str_bus.on_raw("CompletionEvent", "empty_handler", |_event| async move {
        Ok(json!(""))
    });
    let str_second_called_for_handler = str_second_called.clone();
    str_bus.on_raw("CompletionEvent", "second_handler", move |_event| {
        let str_second_called = str_second_called_for_handler.clone();
        async move {
            *str_second_called.lock().expect("called lock") = true;
            Ok(json!("second"))
        }
    });
    let str_event = emit_with_first_completion(&str_bus);
    let str_result =
        block_on(str_event.event_result(non_raising_result_options())).expect("first result");
    assert_eq!(str_result, Some(json!("")));
    assert!(!*str_second_called.lock().expect("called lock"));
    str_bus.stop();
}

#[test]
fn test_event_first_skips_none_result_and_uses_next_winner() {
    let bus = EventBus::new_with_options(
        Some("CompletionNoneSkipBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_handler_completion: EventHandlerCompletionMode::All,
            ..EventBusOptions::default()
        },
    );
    let third_handler_called = Arc::new(Mutex::new(false));

    bus.on_raw("CompletionEvent", "none_handler", |_event| async move {
        Ok(Value::Null)
    });
    bus.on_raw("CompletionEvent", "winner_handler", |_event| async move {
        Ok(json!("winner"))
    });
    let third_handler_called_for_handler = third_handler_called.clone();
    bus.on_raw("CompletionEvent", "third_handler", move |_event| {
        let third_handler_called = third_handler_called_for_handler.clone();
        async move {
            *third_handler_called.lock().expect("called lock") = true;
            Ok(json!("third"))
        }
    });

    let event = emit_with_first_completion(&bus);
    let result = block_on(event.event_result(non_raising_result_options())).expect("first result");

    assert_eq!(result, Some(json!("winner")));
    assert!(!*third_handler_called.lock().expect("called lock"));
    let results = event.inner.inner.lock().event_results.clone();
    let none_result = results
        .values()
        .find(|result| result.handler.handler_name == "none_handler")
        .expect("none result");
    let winner_result = results
        .values()
        .find(|result| result.handler.handler_name == "winner_handler")
        .expect("winner result");
    assert_eq!(none_result.status, EventResultStatus::Completed);
    assert_eq!(none_result.result, Some(Value::Null));
    assert_eq!(winner_result.status, EventResultStatus::Completed);
    assert_eq!(winner_result.result, Some(json!("winner")));
    bus.stop();
}

#[test]
fn test_event_first_skips_baseevent_result_and_uses_next_winner() {
    let bus = EventBus::new_with_options(
        Some("CompletionBaseEventSkipBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_handler_completion: EventHandlerCompletionMode::All,
            ..EventBusOptions::default()
        },
    );
    let third_handler_called = Arc::new(Mutex::new(false));

    bus.on_raw("CompletionEvent",
        "baseevent_handler",
        |_event| async move { Ok(BaseEvent::new("ChildCompletionEvent", Map::new()).to_json_value())},
    );
    bus.on_raw("CompletionEvent", "winner_handler", |_event| async move {
        Ok(json!("winner"))
    });
    let third_handler_called_for_handler = third_handler_called.clone();
    bus.on_raw("CompletionEvent", "third_handler", move |_event| {
        let third_handler_called = third_handler_called_for_handler.clone();
        async move {
            *third_handler_called.lock().expect("called lock") = true;
            Ok(json!("third"))
        }
    });

    let event = emit_with_first_completion(&bus);
    let result = block_on(event.event_result(non_raising_result_options())).expect("first result");

    assert_eq!(result, Some(json!("winner")));
    assert!(!*third_handler_called.lock().expect("called lock"));
    let results = event.inner.inner.lock().event_results.clone();
    let baseevent_result = results
        .values()
        .find(|result| result.handler.handler_name == "baseevent_handler")
        .expect("baseevent result");
    assert_eq!(baseevent_result.status, EventResultStatus::Completed);
    assert!(baseevent_result
        .result
        .as_ref()
        .is_some_and(|value| value.get("event_type") == Some(&json!("ChildCompletionEvent"))));
    assert!(results.values().any(|result| {
        result.handler.handler_name == "third_handler"
            && result.status == EventResultStatus::Error
            && result
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("first() resolved")
    }));
    bus.stop();
}

#[test]
fn test_event_handler_concurrency_bus_default_remains_unset_on_dispatch() {
    let bus = EventBus::new_with_options(
        Some("ConcurrencyDefaultBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    bus.on_raw("ConcurrencyEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });

    let event = bus.emit(ConcurrencyEvent {
        ..Default::default()
    });
    assert_eq!(event.inner.inner.lock().event_handler_concurrency, None);
    let _ = block_on(event.now());
    bus.stop();
}

#[test]
fn test_event_handler_concurrency_per_event_override_controls_execution_mode() {
    let bus = EventBus::new_with_options(
        Some("ConcurrencyPerEventBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    let in_flight = Arc::new(Mutex::new(0));
    let max_in_flight = Arc::new(Mutex::new(0));

    for name in ["handler_1", "handler_2"] {
        let in_flight = in_flight.clone();
        let max_in_flight = max_in_flight.clone();
        bus.on_raw("ConcurrencyEvent", name, move |_event| {
            let in_flight = in_flight.clone();
            let max_in_flight = max_in_flight.clone();
            async move {
                bump_in_flight(&in_flight, &max_in_flight);
                thread::sleep(Duration::from_millis(20));
                drop_in_flight(&in_flight);
                Ok(json!("ok"))
            }
        });
    }

    let serial_event = ConcurrencyEvent {
        event_handler_concurrency: Some(EventHandlerConcurrencyMode::Serial),
        ..Default::default()
    };
    let serial_event = bus.emit(serial_event);
    let _ = block_on(serial_event.now());
    assert_eq!(*max_in_flight.lock().expect("max lock"), 1);

    *in_flight.lock().expect("in flight lock") = 0;
    *max_in_flight.lock().expect("max lock") = 0;
    let parallel_event = ConcurrencyEvent {
        event_handler_concurrency: Some(EventHandlerConcurrencyMode::Parallel),
        ..Default::default()
    };
    let parallel_event = bus.emit(parallel_event);
    let _ = block_on(parallel_event.now());
    assert!(*max_in_flight.lock().expect("max lock") >= 2);
    bus.stop();
}
