use std::{
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use abxbus_rust::{
    event_bus::{EventBus, EventBusOptions},
    event_handler::EventHandlerOptions,
    event_result::EventResultStatus,
    typed::{EventSpec, TypedEvent},
    types::{EventConcurrencyMode, EventHandlerConcurrencyMode},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Serialize, Deserialize)]
struct EmptyPayload {}
#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}
struct TimeoutEvent;
impl EventSpec for TimeoutEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "timeout";
}
struct ChildEvent;
impl EventSpec for ChildEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "child";
}
struct ParentEvent;
impl EventSpec for ParentEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "parent";
}
struct TailEvent;
impl EventSpec for TailEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "tail";
}
struct TimeoutDefaultsEvent;
impl EventSpec for TimeoutDefaultsEvent {
    type Payload = EmptyPayload;
    type Result = EmptyResult;
    const EVENT_TYPE: &'static str = "timeout_defaults";
    const EVENT_TIMEOUT: Option<f64> = Some(0.2);
    const EVENT_HANDLER_TIMEOUT: Option<f64> = Some(0.05);
}

fn wait_until_completed(event: &TypedEvent<ParentEvent>, timeout_ms: u64) {
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_millis(timeout_ms) {
        if event.inner.inner.lock().event_status == abxbus_rust::types::EventStatus::Completed {
            return;
        }
        thread::sleep(Duration::from_millis(5));
    }
    panic!("event did not complete within {timeout_ms}ms");
}

fn error_type(result: &abxbus_rust::event_result::EventResult) -> String {
    result.to_flat_json_value()["error"]["type"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

static TIMEOUT_TEST_MUTEX: Mutex<()> = Mutex::new(());

struct TimeoutTestGuard {
    _guard: std::sync::MutexGuard<'static, ()>,
}

impl Drop for TimeoutTestGuard {
    fn drop(&mut self) {
        thread::sleep(Duration::from_millis(250));
    }
}

fn timeout_test_guard() -> TimeoutTestGuard {
    TimeoutTestGuard {
        _guard: TIMEOUT_TEST_MUTEX
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
    }
}

#[test]
fn test_event_timeout_aborts_in_flight_handler_result() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new(Some("TimeoutBus".to_string()));

    bus.on("timeout", "slow", |_event| async move {
        thread::sleep(Duration::from_millis(50));
        Ok(json!("slow"))
    });

    let event = TypedEvent::<TimeoutEvent>::new(EmptyPayload {});
    event.inner.inner.lock().event_timeout = Some(0.01);

    let event = bus.emit(event);
    block_on(event.wait_completed());

    let result = event
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("missing result");
    assert_eq!(result.status, EventResultStatus::Error);
    assert!(result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("EventHandlerAbortedError"));
    assert_eq!(
        result.to_flat_json_value()["error"],
        json!({
            "type": "EventHandlerAbortedError",
            "message": "timeout",
        })
    );
    bus.stop();
}

#[test]
fn test_handler_completes_within_timeout() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new(Some("TimeoutOkBus".to_string()));

    bus.on("timeout", "fast", |_event| async move {
        thread::sleep(Duration::from_millis(5));
        Ok(json!("fast"))
    });

    let event = TypedEvent::<TimeoutEvent>::new(EmptyPayload {});
    event.inner.inner.lock().event_timeout = Some(0.5);

    let event = bus.emit(event);
    block_on(event.wait_completed());

    let result = event
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("missing result");
    assert_eq!(result.status, EventResultStatus::Completed);
    assert_eq!(result.result, Some(json!("fast")));
    bus.stop();
}

#[test]
fn test_event_timeouts_abort_handlers_across_concurrency_modes() {
    let _guard = timeout_test_guard();
    let event_modes = [
        EventConcurrencyMode::GlobalSerial,
        EventConcurrencyMode::BusSerial,
        EventConcurrencyMode::Parallel,
    ];
    let handler_modes = [
        EventHandlerConcurrencyMode::Serial,
        EventHandlerConcurrencyMode::Parallel,
    ];

    for event_mode in event_modes {
        for handler_mode in handler_modes {
            let bus = EventBus::new_with_options(
                Some(format!("TimeoutModeBus{event_mode:?}{handler_mode:?}")),
                EventBusOptions {
                    event_concurrency: event_mode,
                    event_handler_concurrency: handler_mode,
                    ..EventBusOptions::default()
                },
            );

            bus.on("timeout", "slow", |_event| async move {
                thread::sleep(Duration::from_millis(50));
                Ok(json!("slow"))
            });

            let event = TypedEvent::<TimeoutEvent>::new(EmptyPayload {});
            event.inner.inner.lock().event_timeout = Some(0.01);
            let event = bus.emit(event);
            block_on(event.wait_completed());

            let result = event
                .inner
                .inner
                .lock()
                .event_results
                .values()
                .next()
                .cloned()
                .expect("missing result");
            assert_eq!(
                result.status,
                EventResultStatus::Error,
                "expected timeout error for event={event_mode:?} handler={handler_mode:?}"
            );
            assert_eq!(
                result.to_flat_json_value()["error"],
                json!({
                    "type": "EventHandlerAbortedError",
                    "message": "timeout",
                }),
                "expected aborted error for event={event_mode:?} handler={handler_mode:?}"
            );
            bus.stop();
        }
    }
}

fn assert_event_timeout_does_not_relabel_preexisting_handler_timeout() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("EventTimeoutPreservesHandlerTimeoutBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );

    bus.on_with_options(
        "timeout",
        "handler_with_own_timeout",
        EventHandlerOptions {
            handler_timeout: Some(0.01),
            ..EventHandlerOptions::default()
        },
        |_event| async move {
            thread::sleep(Duration::from_millis(50));
            Ok(json!("own-timeout"))
        },
    );
    bus.on("timeout", "long_running_handler", |_event| async move {
        thread::sleep(Duration::from_millis(200));
        Ok(json!("long-running"))
    });

    let event = TypedEvent::<TimeoutEvent>::new(EmptyPayload {});
    event.inner.inner.lock().event_timeout = Some(0.05);
    let event = bus.emit(event);
    block_on(event.wait_completed());
    assert!(block_on(bus.wait_until_idle(Some(2.0))));

    let results: Vec<_> = event
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .cloned()
        .collect();
    assert_eq!(results.len(), 2);
    assert!(results
        .iter()
        .any(|result| error_type(result) == "EventHandlerTimeoutError"));
    assert!(results
        .iter()
        .any(|result| error_type(result) == "EventHandlerAbortedError"));
    bus.stop();
}

#[test]
fn test_event_timeout_does_not_relabel_preexisting_handler_timeout() {
    assert_event_timeout_does_not_relabel_preexisting_handler_timeout();
}

#[test]
fn test_event_timeout_does_not_relabel_pre_existing_handler_timeout_errors() {
    assert_event_timeout_does_not_relabel_preexisting_handler_timeout();
}

#[test]
fn test_timeout_still_marks_event_failed_when_other_handlers_finish() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("TimeoutParallelHandlers".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::Parallel,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    let completed = Arc::new(Mutex::new(Vec::new()));

    let completed_fast = completed.clone();
    bus.on("timeout", "fast", move |_event| {
        let completed = completed_fast.clone();
        async move {
            thread::sleep(Duration::from_millis(1));
            completed.lock().expect("completed lock").push("fast");
            Ok(json!("fast"))
        }
    });
    bus.on("timeout", "slow", |_event| async move {
        thread::sleep(Duration::from_millis(50));
        Ok(json!("slow"))
    });

    let event = TypedEvent::<TimeoutEvent>::new(EmptyPayload {});
    event.inner.inner.lock().event_timeout = Some(0.01);
    let event = bus.emit(event);
    block_on(event.wait_completed());

    let statuses: Vec<EventResultStatus> = event
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .map(|result| result.status)
        .collect();
    assert!(statuses.contains(&EventResultStatus::Completed));
    assert!(statuses.contains(&EventResultStatus::Error));
    assert_eq!(
        event.inner.inner.lock().event_status,
        abxbus_rust::types::EventStatus::Completed
    );
    assert_eq!(
        completed.lock().expect("completed lock").as_slice(),
        &["fast"]
    );
    bus.stop();
}

#[test]
fn test_event_timeout_is_hard_cap_in_parallel_mode() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("HardCapParallelBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );

    bus.on("timeout", "slow_a", |_event| async move {
        thread::sleep(Duration::from_millis(100));
        Ok(json!("a"))
    });
    bus.on("timeout", "slow_b", |_event| async move {
        thread::sleep(Duration::from_millis(100));
        Ok(json!("b"))
    });

    let event = TypedEvent::<TimeoutEvent>::new(EmptyPayload {});
    {
        let mut inner = event.inner.inner.lock();
        inner.event_timeout = Some(0.02);
        inner.event_concurrency = Some(EventConcurrencyMode::Parallel);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Parallel);
    }

    let started = Instant::now();
    let event = bus.emit(event);
    block_on(event.wait_completed());
    assert!(started.elapsed() < Duration::from_millis(90));

    let results: Vec<_> = event
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .cloned()
        .collect();
    assert_eq!(results.len(), 2);
    assert!(results
        .iter()
        .all(|result| result.status == EventResultStatus::Error));
    assert!(results
        .iter()
        .all(|result| error_type(result) == "EventHandlerAbortedError"));
    bus.stop();
}

#[test]
fn test_event_level_timeout_marks_started_parallel_handlers_as_aborted_or_timed_out() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("TimeoutParallelAbortedOnlyBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::Parallel,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    let started = Arc::new(Mutex::new(0usize));

    for handler_name in ["slow_a", "slow_b"] {
        let started = started.clone();
        bus.on("timeout", handler_name, move |_event| {
            let started = started.clone();
            async move {
                {
                    let mut count = started.lock().expect("started lock");
                    *count += 1;
                }
                thread::sleep(Duration::from_millis(200));
                Ok(json!(handler_name))
            }
        });
    }

    let event = TypedEvent::<TimeoutEvent>::new(EmptyPayload {});
    event.inner.inner.lock().event_timeout = Some(0.03);
    let event = bus.emit(event);
    for _ in 0..40 {
        if *started.lock().expect("started lock") == 2 {
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(*started.lock().expect("started lock"), 2);
    block_on(event.wait_completed());

    let results: Vec<_> = event
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .cloned()
        .collect();
    assert_eq!(results.len(), 2);
    assert!(results
        .iter()
        .all(|result| result.status == EventResultStatus::Error));
    assert!(results.iter().all(|result| {
        matches!(
            error_type(result).as_str(),
            "EventHandlerAbortedError" | "EventHandlerTimeoutError"
        )
    }));
    assert!(!results
        .iter()
        .any(|result| error_type(result) == "EventHandlerCancelledError"));
    bus.stop();
}

#[test]
fn test_event_level_concurrency_overrides_do_not_bypass_timeout_aborts() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("TimeoutEventOverrideBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::GlobalSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );

    bus.on("timeout", "slow", |_event| async move {
        thread::sleep(Duration::from_millis(50));
        Ok(json!("slow"))
    });

    let event = TypedEvent::<TimeoutEvent>::new(EmptyPayload {});
    {
        let mut inner = event.inner.inner.lock();
        inner.event_timeout = Some(0.01);
        inner.event_concurrency = Some(EventConcurrencyMode::Parallel);
        inner.event_handler_concurrency = Some(EventHandlerConcurrencyMode::Parallel);
    }
    let event = bus.emit(event);
    block_on(event.wait_completed());

    let result = event
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("result");
    assert_eq!(result.status, EventResultStatus::Error);
    assert_eq!(error_type(&result), "EventHandlerAbortedError");
    bus.stop();
}

#[test]
fn test_event_timeout_is_hard_cap_across_serial_handlers() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("EventHardCapBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );

    bus.on("timeout", "first_handler", |_event| async move {
        thread::sleep(Duration::from_millis(30));
        Ok(json!("first"))
    });
    bus.on("timeout", "second_handler", |_event| async move {
        thread::sleep(Duration::from_millis(30));
        Ok(json!("second"))
    });
    bus.on("timeout", "pending_handler", |_event| async move {
        Ok(json!("pending"))
    });

    let event = TypedEvent::<TimeoutEvent>::new(EmptyPayload {});
    event.inner.inner.lock().event_timeout = Some(0.05);
    let event = bus.emit(event);
    block_on(event.wait_completed());

    let results = event.inner.inner.lock().event_results.clone();
    let first_result = results
        .values()
        .find(|result| result.handler.handler_name == "first_handler")
        .expect("first result");
    let second_result = results
        .values()
        .find(|result| result.handler.handler_name == "second_handler")
        .expect("second result");
    let pending_result = results
        .values()
        .find(|result| result.handler.handler_name == "pending_handler")
        .expect("pending result");

    assert_eq!(first_result.status, EventResultStatus::Completed);
    assert_eq!(first_result.result, Some(json!("first")));
    assert_eq!(second_result.status, EventResultStatus::Error);
    assert!(second_result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("EventHandlerAbortedError"));
    assert_eq!(pending_result.status, EventResultStatus::Error);
    assert!(pending_result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("EventHandlerCancelledError"));
    bus.stop();
}

#[test]
fn test_forwarded_timeout_path_does_not_stall_followup_events() {
    let _guard = timeout_test_guard();
    let bus_a = EventBus::new(Some("TimeoutForwardRecoveryA".to_string()));
    let bus_b = EventBus::new(Some("TimeoutForwardRecoveryB".to_string()));
    let bus_a_tail_runs = Arc::new(Mutex::new(0usize));
    let bus_b_tail_runs = Arc::new(Mutex::new(0usize));
    let child_ref = Arc::new(Mutex::new(None));

    let bus_a_for_parent = bus_a.clone();
    let child_ref_for_parent = child_ref.clone();
    bus_a.on("parent", "parent_handler", move |_event| {
        let bus_a = bus_a_for_parent.clone();
        let child_ref = child_ref_for_parent.clone();
        async move {
            let child = TypedEvent::<ChildEvent>::new(EmptyPayload {});
            child.inner.inner.lock().event_timeout = Some(0.01);
            let child = bus_a.emit_child(child);
            *child_ref.lock().expect("child ref lock") = Some(child.inner.clone());
            child.wait_completed().await;
            Ok(json!("parent_done"))
        }
    });

    let bus_b_for_forward = bus_b.clone();
    bus_a.on("*", "forward_to_b", move |event| {
        let bus_b = bus_b_for_forward.clone();
        async move {
            bus_b.emit_base(event);
            Ok(json!(null))
        }
    });
    bus_b.on("child", "slow_child_handler", |_event| async move {
        thread::sleep(Duration::from_millis(50));
        Ok(json!("child_done"))
    });

    let bus_a_tail_runs_for_handler = bus_a_tail_runs.clone();
    bus_a.on("tail", "tail_handler_a", move |_event| {
        let runs = bus_a_tail_runs_for_handler.clone();
        async move {
            *runs.lock().expect("bus a tail runs lock") += 1;
            Ok(json!("tail_a"))
        }
    });
    let bus_b_tail_runs_for_handler = bus_b_tail_runs.clone();
    bus_b.on("tail", "tail_handler_b", move |_event| {
        let runs = bus_b_tail_runs_for_handler.clone();
        async move {
            *runs.lock().expect("bus b tail runs lock") += 1;
            Ok(json!("tail_b"))
        }
    });

    let parent = TypedEvent::<ParentEvent>::new(EmptyPayload {});
    parent.inner.inner.lock().event_timeout = Some(1.0);
    let parent = bus_a.emit(parent);
    block_on(parent.wait_completed());
    assert!(block_on(bus_a.wait_until_idle(Some(2.0))));
    assert!(block_on(bus_b.wait_until_idle(Some(2.0))));

    let parent_result = parent
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .find(|result| result.handler.handler_name == "parent_handler")
        .cloned()
        .expect("parent result");
    assert_eq!(parent_result.status, EventResultStatus::Completed);

    let child = child_ref
        .lock()
        .expect("child ref lock")
        .clone()
        .expect("child ref");
    let child_results: Vec<_> = child.inner.lock().event_results.values().cloned().collect();
    assert!(child_results.iter().any(|result| {
        matches!(
            error_type(result).as_str(),
            "EventHandlerAbortedError" | "EventHandlerTimeoutError"
        )
    }));

    let tail = TypedEvent::<TailEvent>::new(EmptyPayload {});
    tail.inner.inner.lock().event_timeout = Some(0.2);
    let tail = bus_a.emit(tail);
    block_on(tail.wait_completed());
    assert!(block_on(bus_a.wait_until_idle(Some(2.0))));
    assert!(block_on(bus_b.wait_until_idle(Some(2.0))));

    assert_eq!(
        tail.inner.inner.lock().event_status,
        abxbus_rust::types::EventStatus::Completed
    );
    assert_eq!(*bus_a_tail_runs.lock().expect("bus a tail runs lock"), 1);
    assert_eq!(*bus_b_tail_runs.lock().expect("bus b tail runs lock"), 1);
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_handler_timeout_marks_error_and_other_handlers_still_complete() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("TimeoutFocusedBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );

    bus.on("timeout", "slow_handler", |_event| async move {
        thread::sleep(Duration::from_millis(50));
        Ok(json!("slow"))
    });
    bus.on("timeout", "fast_handler", |_event| async move {
        Ok(json!("fast"))
    });

    let event = TypedEvent::<TimeoutEvent>::new(EmptyPayload {});
    {
        let mut inner = event.inner.inner.lock();
        inner.event_timeout = Some(0.2);
        inner.event_handler_timeout = Some(0.01);
    }
    let event = bus.emit(event);
    block_on(event.wait_completed());

    let results = event.inner.inner.lock().event_results.clone();
    let slow_result = results
        .values()
        .find(|result| result.handler.handler_name == "slow_handler")
        .expect("slow result");
    let fast_result = results
        .values()
        .find(|result| result.handler.handler_name == "fast_handler")
        .expect("fast result");

    assert_eq!(slow_result.status, EventResultStatus::Error);
    assert!(slow_result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("EventHandlerTimeoutError"));
    assert_eq!(fast_result.status, EventResultStatus::Completed);
    assert_eq!(fast_result.result, Some(json!("fast")));
    bus.stop();
}

#[test]
fn test_processing_time_timeout_defaults_do_not_mutate_event_fields() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("TimeoutDefaultsCopyBus".to_string()),
        EventBusOptions {
            event_timeout: Some(12.0),
            event_slow_timeout: Some(34.0),
            event_handler_slow_timeout: Some(56.0),
            ..EventBusOptions::default()
        },
    );

    bus.on(
        "timeout",
        "handler",
        |_event| async move { Ok(json!("ok")) },
    );

    let event = TypedEvent::<TimeoutEvent>::new(EmptyPayload {});
    {
        let inner = event.inner.inner.lock();
        assert_eq!(inner.event_timeout, None);
        assert_eq!(inner.event_handler_timeout, None);
        assert_eq!(inner.event_handler_slow_timeout, None);
        assert_eq!(inner.event_slow_timeout, None);
    }

    let event = bus.emit(event);
    {
        let inner = event.inner.inner.lock();
        assert_eq!(inner.event_timeout, None);
        assert_eq!(inner.event_handler_timeout, None);
        assert_eq!(inner.event_handler_slow_timeout, None);
        assert_eq!(inner.event_slow_timeout, None);
    }
    block_on(event.wait_completed());
    let result = event
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("handler result");
    assert_eq!(result.timeout, Some(12.0));
    bus.stop();
}

#[test]
fn test_parent_timeout_does_not_cancel_unawaited_child_with_own_timeout() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new(Some("ParentTimeoutBus".to_string()));
    let bus_for_handler = bus.clone();

    bus.on("child", "child_slow", |_event| async move {
        thread::sleep(Duration::from_millis(80));
        Ok(json!("child"))
    });

    bus.on("parent", "emit_child", move |_event| {
        let bus_local = bus_for_handler.clone();
        async move {
            let child = TypedEvent::<ChildEvent>::new(EmptyPayload {});
            child.inner.inner.lock().event_timeout = Some(1.0);
            bus_local.emit_child(child);
            thread::sleep(Duration::from_millis(80));
            Ok(json!("parent"))
        }
    });

    let parent = TypedEvent::<ParentEvent>::new(EmptyPayload {});
    parent.inner.inner.lock().event_timeout = Some(0.01);

    let parent = bus.emit(parent);
    wait_until_completed(&parent, 1000);
    thread::sleep(Duration::from_millis(120));

    let parent_result = parent
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("missing parent result");
    assert_eq!(parent_result.status, EventResultStatus::Error);

    let parent_id = parent.inner.inner.lock().event_id.clone();
    let payload = bus.runtime_payload_for_test();
    let child = payload
        .values()
        .find(|evt| evt.inner.lock().event_parent_id.as_deref() == Some(parent_id.as_str()))
        .cloned()
        .expect("missing child event");

    let child_inner = child.inner.lock();
    assert!(!child_inner.event_blocks_parent_completion);
    let is_completed = child_inner
        .event_results
        .values()
        .any(|r| r.status == EventResultStatus::Completed);
    assert!(is_completed);
    bus.stop();
}

#[test]
fn test_parent_timeout_does_not_cancel_unawaited_children_that_have_no_timeout_of_their_own() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("TimeoutBoundaryBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_timeout: None,
            ..EventBusOptions::default()
        },
    );
    let bus_for_parent = bus.clone();
    let child_ref = Arc::new(Mutex::new(None::<Arc<abxbus_rust::base_event::BaseEvent>>));
    let child_ref_for_parent = child_ref.clone();

    bus.on("child", "child_slow_handler", |_event| async move {
        thread::sleep(Duration::from_millis(80));
        Ok(json!("child_done"))
    });
    bus.on("parent", "parent_handler", move |_event| {
        let bus = bus_for_parent.clone();
        let child_ref = child_ref_for_parent.clone();
        async move {
            let child = TypedEvent::<ChildEvent>::new(EmptyPayload {});
            child.inner.inner.lock().event_timeout = None;
            let child = bus.emit_child(child);
            *child_ref.lock().expect("child ref lock") = Some(child.inner.clone());
            thread::sleep(Duration::from_millis(80));
            Ok(json!("parent_done"))
        }
    });

    let parent = TypedEvent::<ParentEvent>::new(EmptyPayload {});
    parent.inner.inner.lock().event_timeout = Some(0.03);
    let parent = bus.emit(parent);
    block_on(parent.wait_completed());
    assert!(block_on(bus.wait_until_idle(Some(2.0))));

    let parent_result = parent
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("parent result");
    assert_eq!(parent_result.status, EventResultStatus::Error);
    assert_eq!(error_type(&parent_result), "EventHandlerAbortedError");

    let parent_id = parent.inner.inner.lock().event_id.clone();
    let child = child_ref
        .lock()
        .expect("child ref lock")
        .clone()
        .expect("child event");
    let child_inner = child.inner.lock();
    assert_eq!(
        child_inner.event_status,
        abxbus_rust::types::EventStatus::Completed
    );
    assert_eq!(
        child_inner.event_parent_id.as_deref(),
        Some(parent_id.as_str())
    );
    assert!(!child_inner.event_blocks_parent_completion);
    let child_results: Vec<_> = child_inner.event_results.values().cloned().collect();
    assert_eq!(child_results.len(), 1);
    assert_eq!(child_results[0].status, EventResultStatus::Completed);
    assert_eq!(child_results[0].result, Some(json!("child_done")));
    bus.stop();
}

#[test]
fn test_parent_timeout_does_not_cancel_unawaited_child_handler_results_under_serial_handler_lock() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("TimeoutCancelBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_timeout: None,
            ..EventBusOptions::default()
        },
    );
    let bus_for_handler = bus.clone();

    bus.on("child", "child_first", |_event| async move {
        thread::sleep(Duration::from_millis(30));
        Ok(json!("first"))
    });
    bus.on("child", "child_second", |_event| async move {
        thread::sleep(Duration::from_millis(10));
        Ok(json!("second"))
    });

    bus.on("parent", "emit_unawaited_child", move |_event| {
        let bus = bus_for_handler.clone();
        async move {
            let child = TypedEvent::<ChildEvent>::new(EmptyPayload {});
            child.inner.inner.lock().event_timeout = Some(0.2);
            let child = bus.emit_child(child);
            assert!(!child.inner.inner.lock().event_blocks_parent_completion);
            thread::sleep(Duration::from_millis(50));
            Ok(json!("parent"))
        }
    });

    let parent = TypedEvent::<ParentEvent>::new(EmptyPayload {});
    parent.inner.inner.lock().event_timeout = Some(0.01);
    let parent = bus.emit(parent);
    block_on(parent.wait_completed());
    assert!(block_on(bus.wait_until_idle(Some(2.0))));

    let parent_result = parent
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("missing parent result");
    assert_eq!(parent_result.status, EventResultStatus::Error);

    let parent_id = parent.inner.inner.lock().event_id.clone();
    let payload = bus.runtime_payload_for_test();
    let child = payload
        .values()
        .find(|event| event.inner.lock().event_parent_id.as_deref() == Some(parent_id.as_str()))
        .cloned()
        .expect("missing child event");

    let child_inner = child.inner.lock();
    assert!(!child_inner.event_blocks_parent_completion);
    let child_results: Vec<EventResultStatus> = child_inner
        .event_results
        .values()
        .map(|result| result.status)
        .collect();
    assert_eq!(child_results.len(), 2);
    assert!(child_results
        .iter()
        .all(|status| *status == EventResultStatus::Completed));
    bus.stop();
}

#[test]
fn test_parent_timeout_cancels_awaited_child_handler_results() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("TimeoutAwaitedChildCancelBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_timeout: None,
            ..EventBusOptions::default()
        },
    );
    let bus_for_handler = bus.clone();

    bus.on("child", "child_slow", |_event| async move {
        thread::sleep(Duration::from_millis(80));
        Ok(json!("child"))
    });

    bus.on("parent", "emit_awaited_child", move |_event| {
        let bus = bus_for_handler.clone();
        async move {
            let child = TypedEvent::<ChildEvent>::new(EmptyPayload {});
            child.inner.inner.lock().event_timeout = Some(1.0);
            let child = bus.emit_child(child);
            child.wait_completed().await;
            Ok(json!("parent"))
        }
    });

    let parent = TypedEvent::<ParentEvent>::new(EmptyPayload {});
    parent.inner.inner.lock().event_timeout = Some(0.01);
    let parent = bus.emit(parent);
    block_on(parent.wait_completed());
    assert!(block_on(bus.wait_until_idle(Some(2.0))));

    let parent_result = parent
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("missing parent result");
    assert_eq!(parent_result.status, EventResultStatus::Error);

    let parent_id = parent.inner.inner.lock().event_id.clone();
    let payload = bus.runtime_payload_for_test();
    let child = payload
        .values()
        .find(|event| event.inner.lock().event_parent_id.as_deref() == Some(parent_id.as_str()))
        .cloned()
        .expect("missing child event");

    let child_inner = child.inner.lock();
    assert!(child_inner.event_blocks_parent_completion);
    let child_results: Vec<_> = child_inner.event_results.values().cloned().collect();
    assert_eq!(child_results.len(), 1);
    assert_eq!(child_results[0].status, EventResultStatus::Error);
    assert!(child_results[0]
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("EventHandlerAbortedError"));
    bus.stop();
}

#[test]
fn test_multi_bus_timeout_is_recorded_on_target_bus() {
    let _guard = timeout_test_guard();
    let bus_a = EventBus::new(Some("MultiTimeoutA".to_string()));
    let bus_b = EventBus::new(Some("MultiTimeoutB".to_string()));

    bus_b.on("timeout", "slow_target_handler", |_event| async move {
        thread::sleep(Duration::from_millis(50));
        Ok(json!("slow"))
    });

    let event = TypedEvent::<TimeoutEvent>::new(EmptyPayload {});
    event.inner.inner.lock().event_timeout = Some(0.01);
    let event = bus_a.emit(event);
    bus_b.emit_base(event.inner.clone());
    assert!(block_on(bus_b.wait_until_idle(Some(2.0))));

    let results = event.inner.inner.lock().event_results.clone();
    let bus_b_result = results
        .values()
        .find(|result| result.handler.eventbus_id == bus_b.id)
        .expect("bus_b result");
    assert_eq!(bus_b_result.status, EventResultStatus::Error);
    assert_eq!(error_type(bus_b_result), "EventHandlerAbortedError");
    assert_eq!(
        event.inner.inner.lock().event_path,
        vec![bus_a.label(), bus_b.label()]
    );
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_forwarded_event_timeout_aborts_apply_across_buses() {
    let _guard = timeout_test_guard();
    let bus_a = EventBus::new_with_options(
        Some("TimeoutForwardA".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );
    let bus_b = EventBus::new_with_options(
        Some("TimeoutForwardB".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            ..EventBusOptions::default()
        },
    );

    let bus_b_for_forward = bus_b.clone();
    bus_a.on("timeout", "forward_to_b", move |event| {
        let bus_b = bus_b_for_forward.clone();
        async move {
            bus_b.emit_base(event);
            Ok(json!(null))
        }
    });
    bus_b.on("timeout", "slow_target_handler", |_event| async move {
        thread::sleep(Duration::from_millis(50));
        Ok(json!("slow"))
    });

    let event = TypedEvent::<TimeoutEvent>::new(EmptyPayload {});
    event.inner.inner.lock().event_timeout = Some(0.01);
    let event = bus_a.emit(event);
    block_on(event.wait_completed());
    assert!(block_on(bus_b.wait_until_idle(Some(2.0))));

    let results = event.inner.inner.lock().event_results.clone();
    let bus_b_result = results
        .values()
        .find(|result| result.handler.eventbus_id == bus_b.id)
        .expect("bus_b result");
    assert_eq!(bus_b_result.status, EventResultStatus::Error);
    assert_eq!(error_type(bus_b_result), "EventHandlerAbortedError");
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_queue_jump_awaited_child_timeout_aborts_still_fire_across_buses() {
    let _guard = timeout_test_guard();
    let bus_a = EventBus::new_with_options(
        Some("TimeoutQueueJumpA".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::GlobalSerial,
            event_timeout: None,
            ..EventBusOptions::default()
        },
    );
    let bus_b = EventBus::new_with_options(
        Some("TimeoutQueueJumpB".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::GlobalSerial,
            event_timeout: None,
            ..EventBusOptions::default()
        },
    );
    let child_ref = Arc::new(Mutex::new(None));

    bus_b.on("child", "slow_child_handler", |_event| async move {
        thread::sleep(Duration::from_millis(50));
        Ok(json!("slow"))
    });

    let bus_a_for_parent = bus_a.clone();
    let bus_b_for_parent = bus_b.clone();
    let child_ref_for_parent = child_ref.clone();
    bus_a.on("parent", "parent_handler", move |_event| {
        let bus_a = bus_a_for_parent.clone();
        let bus_b = bus_b_for_parent.clone();
        let child_ref = child_ref_for_parent.clone();
        async move {
            let child = TypedEvent::<ChildEvent>::new(EmptyPayload {});
            child.inner.inner.lock().event_timeout = Some(0.01);
            let child = bus_a.emit_child(child);
            bus_b.emit_base(child.inner.clone());
            *child_ref.lock().expect("child ref lock") = Some(child.inner.clone());
            child.wait_completed().await;
            Ok(json!(null))
        }
    });

    let parent = TypedEvent::<ParentEvent>::new(EmptyPayload {});
    parent.inner.inner.lock().event_timeout = Some(2.0);
    let parent = bus_a.emit(parent);
    block_on(parent.wait_completed());
    assert!(block_on(bus_a.wait_until_idle(Some(2.0))));
    assert!(block_on(bus_b.wait_until_idle(Some(2.0))));

    let child = child_ref
        .lock()
        .expect("child ref lock")
        .clone()
        .expect("child ref");
    let child_results: Vec<_> = child.inner.lock().event_results.values().cloned().collect();
    assert!(
        child_results.iter().any(|result| {
            matches!(
                error_type(result).as_str(),
                "EventHandlerAbortedError" | "EventHandlerTimeoutError"
            )
        }),
        "expected child timeout/abort result, got results={:?} child={}",
        child_results
            .iter()
            .map(|result| result.to_flat_json_value())
            .collect::<Vec<_>>(),
        child.to_json_value()
    );
    bus_a.stop();
    bus_b.stop();
}

#[test]
fn test_followup_event_runs_after_parent_timeout_in_queue_jump_path() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new(Some("TimeoutQueueJumpFollowupBus".to_string()));
    let bus_for_parent = bus.clone();
    let tail_runs = Arc::new(Mutex::new(0usize));

    bus.on("child", "child_handler", |_event| async move {
        thread::sleep(Duration::from_millis(1));
        Ok(json!("child_done"))
    });
    bus.on("parent", "parent_handler", move |_event| {
        let bus = bus_for_parent.clone();
        async move {
            let child = TypedEvent::<ChildEvent>::new(EmptyPayload {});
            child.inner.inner.lock().event_timeout = Some(0.2);
            let child = bus.emit_child(child);
            child.wait_completed().await;
            thread::sleep(Duration::from_millis(50));
            Ok(json!("parent_done"))
        }
    });
    let tail_runs_for_handler = tail_runs.clone();
    bus.on("tail", "tail_handler", move |_event| {
        let tail_runs = tail_runs_for_handler.clone();
        async move {
            *tail_runs.lock().expect("tail runs lock") += 1;
            Ok(json!("tail_done"))
        }
    });

    let parent = TypedEvent::<ParentEvent>::new(EmptyPayload {});
    parent.inner.inner.lock().event_timeout = Some(0.02);
    let parent = bus.emit(parent);
    block_on(parent.wait_completed());
    assert!(block_on(bus.wait_until_idle(Some(2.0))));

    let parent_result = parent
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("parent result");
    assert_eq!(parent_result.status, EventResultStatus::Error);
    assert_eq!(error_type(&parent_result), "EventHandlerAbortedError");

    let tail = TypedEvent::<TailEvent>::new(EmptyPayload {});
    tail.inner.inner.lock().event_timeout = Some(0.2);
    let tail = bus.emit(tail);
    block_on(tail.wait_completed());
    assert_eq!(
        tail.inner.inner.lock().event_status,
        abxbus_rust::types::EventStatus::Completed
    );
    assert_eq!(*tail_runs.lock().expect("tail runs lock"), 1);
    bus.stop();
}

#[test]
fn test_event_timeout_null_falls_back_to_bus_default() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("TimeoutDefaultBus".to_string()),
        EventBusOptions {
            event_timeout: Some(0.01),
            ..EventBusOptions::default()
        },
    );

    bus.on("timeout", "slow", |_event| async move {
        thread::sleep(Duration::from_millis(50));
        Ok(json!("slow"))
    });

    let event = TypedEvent::<TimeoutEvent>::new(EmptyPayload {});
    event.inner.inner.lock().event_timeout = None;
    let event = bus.emit(event);
    block_on(event.wait_completed());

    let result = event
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("result");
    assert_eq!(result.status, EventResultStatus::Error);
    assert_eq!(error_type(&result), "EventHandlerAbortedError");
    bus.stop();
}

#[test]
fn test_bus_default_null_disables_timeouts_when_event_timeout_is_null() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("TimeoutDisabledBus".to_string()),
        EventBusOptions {
            event_timeout: None,
            ..EventBusOptions::default()
        },
    );

    bus.on("timeout", "slow", |_event| async move {
        thread::sleep(Duration::from_millis(20));
        Ok(json!("ok"))
    });

    let event = TypedEvent::<TimeoutEvent>::new(EmptyPayload {});
    event.inner.inner.lock().event_timeout = None;
    let event = bus.emit(event);
    block_on(event.wait_completed());

    let result = event
        .inner
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("result");
    assert_eq!(result.status, EventResultStatus::Completed);
    assert_eq!(result.result, Some(json!("ok")));
    bus.stop();
}

#[test]
fn test_handler_timeout_resolution_matches_ts_precedence() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("TimeoutPrecedenceBus".to_string()),
        EventBusOptions {
            event_timeout: Some(0.2),
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );

    bus.on("timeout_defaults", "default_handler", |_event| async move {
        Ok(json!("default"))
    });
    bus.on_with_options(
        "timeout_defaults",
        "overridden_handler",
        EventHandlerOptions {
            handler_timeout: Some(0.12),
            ..EventHandlerOptions::default()
        },
        |_event| async move { Ok(json!("override")) },
    );

    let event = TypedEvent::<TimeoutDefaultsEvent>::new(EmptyPayload {});
    let event = bus.emit(event);
    block_on(event.wait_completed());
    let results = event.inner.inner.lock().event_results.clone();
    let default_result = results
        .values()
        .find(|result| result.handler.handler_name == "default_handler")
        .expect("default handler result");
    let overridden_result = results
        .values()
        .find(|result| result.handler.handler_name == "overridden_handler")
        .expect("overridden handler result");
    assert_eq!(default_result.timeout, Some(0.05));
    assert_eq!(overridden_result.timeout, Some(0.12));

    let tighter_event_timeout = TypedEvent::<TimeoutDefaultsEvent>::new(EmptyPayload {});
    {
        let mut inner = tighter_event_timeout.inner.inner.lock();
        inner.event_timeout = Some(0.08);
        inner.event_handler_timeout = Some(0.2);
    }
    let tighter_event_timeout = bus.emit(tighter_event_timeout);
    block_on(tighter_event_timeout.wait_completed());
    let tighter_results = tighter_event_timeout
        .inner
        .inner
        .lock()
        .event_results
        .clone();
    assert!(tighter_results
        .values()
        .all(|result| result.timeout == Some(0.08)));

    bus.stop();
}

#[test]
fn test_event_handler_detect_file_paths_toggle() {
    let bus = EventBus::new_with_options(
        Some("NoDetectPathsBus".to_string()),
        EventBusOptions {
            event_handler_detect_file_paths: false,
            ..EventBusOptions::default()
        },
    );

    let entry = bus.on("timeout_defaults", "handler", |_event| async move {
        Ok(json!("ok"))
    });
    assert_eq!(entry.handler_file_path, None);
    bus.stop();
}
