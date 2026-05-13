use std::{
    sync::{Mutex, MutexGuard, OnceLock},
    thread,
    time::Duration,
};

use abxbus_rust::{
    base_event::EventResultOptions,
    event,
    event_bus::{EventBus, EventBusOptions},
    event_handler::EventHandlerOptions,
    event_result::EventResultStatus,
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

fn destroy_and_settle(bus: &std::sync::Arc<EventBus>) {
    bus.destroy();
    thread::sleep(Duration::from_millis(10));
}

#[test]
fn test_event_handler_completion_first_stops_after_first_valid_result() {
    let _guard = test_guard();
    let bus = EventBus::new(Some("EventHandlerCompletionFirstShortcutBus".to_string()));

    bus.on_raw("value", "empty", |_event| async move { Ok(Value::Null) });
    bus.on_raw(
        "value",
        "winner",
        |_event| async move { Ok(json!("winner")) },
    );
    bus.on_raw("value", "late", |_event| async move { Ok(json!("late")) });

    let event = ValueEvent {
        event_handler_completion: Some(EventHandlerCompletionMode::First),
        event_handler_concurrency: Some(EventHandlerConcurrencyMode::Serial),
        ..Default::default()
    };
    let emitted = bus.emit(event);
    block_on(emitted.now()).expect("event should run");
    let result = block_on(emitted.event_result(EventResultOptions {
        raise_if_any: false,
        raise_if_none: false,
        include: None,
    }))
    .expect("event result");

    assert_eq!(result, Some(json!("winner")));
    assert_eq!(
        emitted.inner.inner.lock().event_handler_completion,
        Some(EventHandlerCompletionMode::First)
    );
    let results = emitted.inner.inner.lock().event_results.clone();
    assert!(!results.values().any(|result| {
        result.handler.handler_name == "late" && result.status == EventResultStatus::Completed
    }));
    destroy_and_settle(&bus);
}

#[test]
fn test_now_runs_all_handlers_and_event_result_returns_first_valid_result() {
    let _guard = test_guard();
    let bus = EventBus::new(Some("NowAllBus".to_string()));

    bus.on_raw("value", "base-event", |_event| async move {
        Ok(json!({"event_type": "ForwardedEvent", "event_id": "forwarded"}))
    });
    bus.on_raw("value", "none", |_event| async move { Ok(Value::Null) });
    bus.on_raw(
        "value",
        "winner",
        |_event| async move { Ok(json!("winner")) },
    );
    bus.on_raw("value", "late", |_event| async move { Ok(json!("late")) });

    let emitted = bus.emit(ValueEvent {
        event_handler_completion: Some(EventHandlerCompletionMode::All),
        event_handler_concurrency: Some(EventHandlerConcurrencyMode::Serial),
        ..Default::default()
    });
    let completed = block_on(emitted.now()).expect("event should run");
    let result = block_on(completed.event_result(EventResultOptions::default())).expect("result");

    assert_eq!(result, Some(json!("winner")));
    assert_eq!(
        completed.inner.inner.lock().event_handler_completion,
        Some(EventHandlerCompletionMode::All)
    );
    assert_eq!(completed.inner.inner.lock().event_results.len(), 4);
    destroy_and_settle(&bus);
}

#[test]
fn test_event_result_default_error_policy_raises_handler_errors() {
    let _guard = test_guard();
    let bus = EventBus::new(Some("EventResultErrorPolicyBus".to_string()));

    bus.on_raw("value", "fail", |_event| async move {
        Err("event result boom".to_string())
    });
    bus.on_raw(
        "value",
        "winner",
        |_event| async move { Ok(json!("winner")) },
    );

    let emitted = bus.emit(ValueEvent::default());
    let completed = block_on(emitted.now()).expect("event should run");
    let error = block_on(completed.event_result(EventResultOptions::default()))
        .expect_err("default EventResult should surface handler errors");
    assert!(error.contains("event result boom"));

    let result = block_on(completed.event_result(EventResultOptions {
        raise_if_any: false,
        raise_if_none: false,
        include: None,
    }))
    .expect("suppressed result");
    assert_eq!(result, Some(json!("winner")));
    destroy_and_settle(&bus);
}

#[test]
fn test_event_result_options_can_raise_and_filter_results() {
    let _guard = test_guard();
    let bus = EventBus::new(Some("EventResultOptionsBus".to_string()));

    bus.on_raw("value", "fail", |_event| async move {
        Err("event result option boom".to_string())
    });
    bus.on_raw("value", "first", |_event| async move { Ok(json!("first")) });
    bus.on_raw(
        "value",
        "second",
        |_event| async move { Ok(json!("second")) },
    );

    let emitted = block_on(
        bus.emit(ValueEvent {
            event_handler_completion: Some(EventHandlerCompletionMode::All),
            event_handler_concurrency: Some(EventHandlerConcurrencyMode::Serial),
            ..Default::default()
        })
        .now(),
    )
    .expect("event should run");
    let error = block_on(emitted.event_result(EventResultOptions {
        raise_if_any: true,
        raise_if_none: false,
        include: None,
    }))
    .expect_err("RaiseIfAny should surface handler errors");
    assert!(error.contains("event result option boom"));

    let result = block_on(emitted.event_result(EventResultOptions {
        raise_if_any: false,
        raise_if_none: false,
        include: Some(std::sync::Arc::new(|result, event_result| {
            event_result.status == EventResultStatus::Completed && result == Some(&json!("second"))
        })),
    }))
    .expect("filtered result");
    assert_eq!(result, Some(json!("second")));
    destroy_and_settle(&bus);
}

#[test]
fn test_event_result_include_callback_receives_result_and_event_result() {
    let _guard = test_guard();
    let bus = EventBus::new(Some("EventResultIncludeBus".to_string()));

    bus.on_raw(
        "value",
        "none_handler",
        |_event| async move { Ok(Value::Null) },
    );
    bus.on_raw("value", "second_handler", |_event| async move {
        Ok(json!("second"))
    });

    let emitted = block_on(
        bus.emit(ValueEvent {
            event_handler_completion: Some(EventHandlerCompletionMode::All),
            event_handler_concurrency: Some(EventHandlerConcurrencyMode::Serial),
            ..Default::default()
        })
        .now(),
    )
    .expect("event should run");
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen_for_include = seen.clone();
    let result = block_on(emitted.event_result(EventResultOptions {
        raise_if_any: true,
        raise_if_none: false,
        include: Some(std::sync::Arc::new(move |result, event_result| {
            seen_for_include
                .lock()
                .unwrap()
                .push(event_result.handler.handler_name.clone());
            result == Some(&json!("second"))
        })),
    }))
    .expect("filtered result");

    assert_eq!(result, Some(json!("second")));
    assert_eq!(
        seen.lock().unwrap().join(","),
        "none_handler,second_handler"
    );
    destroy_and_settle(&bus);
}

#[test]
fn test_event_result_returns_first_valid_result_by_registration_order() {
    let _guard = test_guard();
    let bus = EventBus::new_with_options(
        Some("EventResultHandlerOrderBus".to_string()),
        EventBusOptions {
            event_handler_completion: EventHandlerCompletionMode::All,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );

    let registered_at = "2026-01-01T00:00:00.000Z".to_string();
    bus.on_raw_with_options(
        "value",
        "slow",
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
        "fast",
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
            event_handler_concurrency: Some(EventHandlerConcurrencyMode::Serial),
            ..Default::default()
        })
        .now(),
    )
    .expect("event should run");
    thread::sleep(Duration::from_millis(60));
    let result = block_on(emitted.event_result(EventResultOptions::default())).expect("result");

    assert_eq!(result, Some(json!("slow")));
    destroy_and_settle(&bus);
}
