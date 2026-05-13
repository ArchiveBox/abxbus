use abxbus_rust::event;
use std::{
    process::Command,
    sync::{mpsc, Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use abxbus_rust::{
    base_event::BaseEvent,
    event_bus::{EventBus, EventBusOptions},
    event_handler::EventHandlerOptions,
    event_result::{EventResult, EventResultStatus},
        types::{EventConcurrencyMode, EventHandlerConcurrencyMode, EventStatus},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}
event! {
    struct TimeoutEvent {
        event_result_type: EmptyResult,
        event_type: "timeout",
    }
}
event! {
    struct ChildEvent {
        event_result_type: EmptyResult,
        event_type: "child",
    }
}
event! {
    struct ParentEvent {
        event_result_type: EmptyResult,
        event_type: "parent",
    }
}
event! {
    struct TailEvent {
        event_result_type: EmptyResult,
        event_type: "tail",
    }
}
event! {
    struct GrandchildEvent {
        event_result_type: EmptyResult,
        event_type: "grandchild",
    }
}
event! {
    struct QueuedSiblingEvent {
        event_result_type: EmptyResult,
        event_type: "queued_sibling",
    }
}
event! {
    struct TimeoutDefaultsEvent {
        event_result_type: EmptyResult,
        event_type: "timeout_defaults",
        event_timeout: 0.2,
        event_handler_timeout: 0.05,
    }
}
event! {
    struct LateAfterTimeoutEvent {
        event_result_type: EmptyResult,
        event_type: "late_after_timeout",
    }
}
fn wait_until_completed(event: &ParentEvent, timeout_ms: u64) {
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_millis(timeout_ms) {
        if event.event_status.read() == abxbus_rust::types::EventStatus::Completed {
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

fn timeout_event(event_type: &str, timeout: Option<f64>) -> Arc<BaseEvent> {
    let event = BaseEvent::new(event_type, serde_json::Map::new());
    event.inner.lock().event_timeout = timeout;
    event
}

fn result_by_handler(event: &Arc<BaseEvent>, handler_name: &str) -> EventResult {
    event
        .inner
        .lock()
        .event_results
        .values()
        .find(|result| result.handler.handler_name == handler_name)
        .cloned()
        .unwrap_or_else(|| panic!("missing handler result {handler_name}"))
}

fn first_result_for_event(event: &Arc<BaseEvent>) -> EventResult {
    event
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("missing event result")
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

fn slow_warning_child_enabled() -> bool {
    std::env::var("ABXBUS_RUN_SLOW_WARNING_CHILD").as_deref() == Ok("1")
}

fn run_slow_warning_child(test_name: &str) -> String {
    let output = Command::new(std::env::current_exe().expect("current test binary"))
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .env("ABXBUS_RUN_SLOW_WARNING_CHILD", "1")
        .output()
        .expect("run slow warning child test");
    assert!(
        output.status.success(),
        "child test {test_name} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stderr).to_string()
}

fn run_slow_warning_event(
    event_slow_timeout: Option<f64>,
    event_handler_slow_timeout: Option<f64>,
) {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("SlowWarningChildBus".to_string()),
        EventBusOptions {
            event_timeout: Some(0.5),
            event_slow_timeout: Some(0.5),
            event_handler_slow_timeout: Some(0.5),
            ..EventBusOptions::default()
        },
    );
    bus.on_raw("timeout_defaults", "slow_handler", |_event| async move {
        thread::sleep(Duration::from_millis(30));
        eprintln!("slow warning child handler finishing");
        Ok(json!("ok"))
    });

    let event = bus.emit(TimeoutDefaultsEvent {
        event_slow_timeout,
        event_handler_slow_timeout,
        ..Default::default()
    });
    let _ = block_on(event.now());
    bus.destroy();
}

#[test]
fn test_event_timeout_aborts_in_flight_handler_result() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new(Some("TimeoutBus".to_string()));

    bus.on_raw("timeout", "slow", |_event| async move {
        thread::sleep(Duration::from_millis(50));
        Ok(json!("slow"))
    });

    let mut event = TimeoutEvent {
        ..Default::default()
    };
    event.event_timeout = Some(0.01);

    let event = bus.emit(event);
    let _ = block_on(event.now());

    let result = event
        ._inner_event().inner.lock()
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
    bus.destroy();
}

#[test]
fn test_handler_completes_within_timeout() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new(Some("TimeoutOkBus".to_string()));

    bus.on_raw("timeout", "fast", |_event| async move {
        thread::sleep(Duration::from_millis(5));
        Ok(json!("fast"))
    });

    let mut event = TimeoutEvent {
        ..Default::default()
    };
    event.event_timeout = Some(0.5);

    let event = bus.emit(event);
    let _ = block_on(event.now());

    let result = event
        ._inner_event().inner.lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("missing result");
    assert_eq!(result.status, EventResultStatus::Completed);
    assert_eq!(result.result, Some(json!("fast")));
    bus.destroy();
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

            bus.on_raw("timeout", "slow", |_event| async move {
                thread::sleep(Duration::from_millis(50));
                Ok(json!("slow"))
            });

            let mut event = TimeoutEvent {
                ..Default::default()
            };
            event.event_timeout = Some(0.01);
            let event = bus.emit(event);
            let _ = block_on(event.now());

            let result = event
                ._inner_event().inner.lock()
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
            bus.destroy();
        }
    }
}

#[test]
fn test_event_handler_errors_expose_event_result_cause_and_timeout_metadata() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("ErrorMetadataBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );

    bus.on_raw(
        "ErrorMetadataTimeout",
        "slow_timeout",
        |_event| async move {
            thread::sleep(Duration::from_millis(60));
            Ok(json!("slow"))
        },
    );

    let timed_out_event = bus.emit_base(timeout_event("ErrorMetadataTimeout", Some(0.02)));
    let _ = block_on(timed_out_event.now());

    let timeout_result = result_by_handler(&timed_out_event, "slow_timeout");
    let timeout_metadata = timeout_result
        .error_metadata_json(&timed_out_event)
        .expect("timeout metadata");
    assert_eq!(timeout_metadata["type"], "EventHandlerAbortedError");
    assert_eq!(
        timeout_metadata["cause"]["type"],
        "EventHandlerTimeoutError"
    );
    assert_eq!(
        timeout_metadata["event_result_id"],
        json!(timeout_result.id)
    );
    assert_eq!(
        timeout_metadata["event_id"],
        json!(timed_out_event.inner.lock().event_id.clone())
    );
    assert_eq!(timeout_metadata["event_type"], "ErrorMetadataTimeout");
    assert_eq!(timeout_metadata["handler_name"], "slow_timeout");
    assert_eq!(
        timeout_metadata["handler_id"],
        json!(timeout_result.handler.id)
    );
    assert_eq!(timeout_metadata["event_timeout"], json!(0.02));
    assert_eq!(timeout_metadata["timeout_seconds"], json!(0.02));
    assert_eq!(
        timeout_result.to_flat_json_value()["error"],
        json!({
            "type": "EventHandlerAbortedError",
            "message": "timeout",
        })
    );

    bus.on_raw(
        "ErrorMetadataAwaitedChild",
        "awaited_child_slow",
        |_event| async move {
            thread::sleep(Duration::from_millis(120));
            Ok(json!("awaited_child"))
        },
    );

    let awaited_child_ref = Arc::new(Mutex::new(None::<Arc<BaseEvent>>));
    let bus_for_awaited_parent = bus.clone();
    let awaited_child_ref_for_handler = awaited_child_ref.clone();
    bus.on_raw(
        "ErrorMetadataAwaitedParent",
        "awaits_child",
        move |_event| {
            let bus = bus_for_awaited_parent.clone();
            let awaited_child_ref = awaited_child_ref_for_handler.clone();
            async move {
                let child =
                    bus.emit_child_base(timeout_event("ErrorMetadataAwaitedChild", Some(0.5)));
                *awaited_child_ref.lock().expect("awaited child ref") = Some(child.clone());
                let _ = child.now().await;
                Ok(json!("parent"))
            }
        },
    );

    let awaited_parent = bus.emit_base(timeout_event("ErrorMetadataAwaitedParent", Some(0.05)));
    let _ = block_on(awaited_parent.now());
    assert!(block_on(bus.wait_until_idle(Some(2.0))));

    let awaited_child = awaited_child_ref
        .lock()
        .expect("awaited child ref")
        .clone()
        .expect("awaited child should be emitted");
    assert!(awaited_child.inner.lock().event_blocks_parent_completion);

    let awaited_parent_result = result_by_handler(&awaited_parent, "awaits_child");
    let parent_metadata = awaited_parent_result
        .error_metadata_json(&awaited_parent)
        .expect("parent timeout metadata");
    assert_eq!(parent_metadata["type"], "EventHandlerAbortedError");
    assert_eq!(parent_metadata["cause"]["type"], "EventHandlerTimeoutError");
    assert_eq!(parent_metadata["event_type"], "ErrorMetadataAwaitedParent");
    assert_eq!(parent_metadata["handler_name"], "awaits_child");
    assert_eq!(parent_metadata["event_timeout"], json!(0.05));

    let awaited_child_result = result_by_handler(&awaited_child, "awaited_child_slow");
    let child_metadata = awaited_child_result
        .error_metadata_json(&awaited_child)
        .expect("awaited child error metadata");
    assert_eq!(child_metadata["type"], "EventHandlerAbortedError");
    assert_eq!(child_metadata["cause"]["type"], "EventHandlerTimeoutError");
    assert!(child_metadata["cause"]["message"]
        .as_str()
        .unwrap_or_default()
        .contains("parent event timed out after 0.05s"));
    assert_eq!(
        child_metadata["event_result_id"],
        json!(awaited_child_result.id)
    );
    assert_eq!(
        child_metadata["event_id"],
        json!(awaited_child.inner.lock().event_id.clone())
    );
    assert_eq!(child_metadata["event_type"], "ErrorMetadataAwaitedChild");
    assert_eq!(child_metadata["handler_name"], "awaited_child_slow");
    assert_eq!(
        child_metadata["handler_id"],
        json!(awaited_child_result.handler.id)
    );
    assert_eq!(child_metadata["event_timeout"], json!(0.5));
    assert_eq!(child_metadata["timeout_seconds"], json!(0.5));

    bus.on_raw(
        "ErrorMetadataUnawaitedChild",
        "unawaited_child_fast",
        |_event| async move {
            thread::sleep(Duration::from_millis(10));
            Ok(json!("unawaited_child"))
        },
    );

    let unawaited_child_ref = Arc::new(Mutex::new(None::<Arc<BaseEvent>>));
    let bus_for_unawaited_parent = bus.clone();
    let unawaited_child_ref_for_handler = unawaited_child_ref.clone();
    bus.on_raw(
        "ErrorMetadataUnawaitedParent",
        "emits_unawaited_child",
        move |_event| {
            let bus = bus_for_unawaited_parent.clone();
            let unawaited_child_ref = unawaited_child_ref_for_handler.clone();
            async move {
                let child =
                    bus.emit_child_base(timeout_event("ErrorMetadataUnawaitedChild", Some(0.5)));
                *unawaited_child_ref.lock().expect("unawaited child ref") = Some(child);
                thread::sleep(Duration::from_millis(80));
                Ok(json!("parent"))
            }
        },
    );

    let unawaited_parent = bus.emit_base(timeout_event("ErrorMetadataUnawaitedParent", Some(0.02)));
    let _ = block_on(unawaited_parent.now());
    assert!(block_on(bus.wait_until_idle(Some(2.0))));

    let unawaited_child = unawaited_child_ref
        .lock()
        .expect("unawaited child ref")
        .clone()
        .expect("unawaited child should be emitted");
    assert!(!unawaited_child.inner.lock().event_blocks_parent_completion);
    assert_eq!(
        unawaited_child.inner.lock().event_status,
        EventStatus::Completed
    );
    let unawaited_child_result = first_result_for_event(&unawaited_child);
    assert_eq!(unawaited_child_result.status, EventResultStatus::Completed);
    assert_eq!(
        unawaited_child_result.result,
        Some(json!("unawaited_child"))
    );
    assert!(unawaited_child_result
        .error_metadata_json(&unawaited_child)
        .is_none());

    bus.destroy();
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

    bus.on_raw_with_options(
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
    bus.on_raw("timeout", "long_running_handler", |_event| async move {
        thread::sleep(Duration::from_millis(200));
        Ok(json!("long-running"))
    });

    let mut event = TimeoutEvent {
        ..Default::default()
    };
    event.event_timeout = Some(0.05);
    let event = bus.emit(event);
    let _ = block_on(event.now());
    assert!(block_on(bus.wait_until_idle(Some(2.0))));

    let results: Vec<_> = event
        ._inner_event().inner.lock()
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
    bus.destroy();
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
    bus.on_raw("timeout", "fast", move |_event| {
        let completed = completed_fast.clone();
        async move {
            thread::sleep(Duration::from_millis(1));
            completed.lock().expect("completed lock").push("fast");
            Ok(json!("fast"))
        }
    });
    bus.on_raw("timeout", "slow", |_event| async move {
        thread::sleep(Duration::from_millis(50));
        Ok(json!("slow"))
    });

    let mut event = TimeoutEvent {
        ..Default::default()
    };
    event.event_timeout = Some(0.01);
    let event = bus.emit(event);
    let _ = block_on(event.now());

    let statuses: Vec<EventResultStatus> = event
        ._inner_event().inner.lock()
        .event_results
        .values()
        .map(|result| result.status)
        .collect();
    assert!(statuses.contains(&EventResultStatus::Completed));
    assert!(statuses.contains(&EventResultStatus::Error));
    assert_eq!(
        event.event_status.read(),
        abxbus_rust::types::EventStatus::Completed
    );
    assert_eq!(
        completed.lock().expect("completed lock").as_slice(),
        &["fast"]
    );
    bus.destroy();
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

    bus.on_raw("timeout", "slow_a", |_event| async move {
        thread::sleep(Duration::from_millis(100));
        Ok(json!("a"))
    });
    bus.on_raw("timeout", "slow_b", |_event| async move {
        thread::sleep(Duration::from_millis(100));
        Ok(json!("b"))
    });

    let event = TimeoutEvent {
        event_timeout: Some(0.02),
        event_concurrency: Some(EventConcurrencyMode::Parallel),
        event_handler_concurrency: Some(EventHandlerConcurrencyMode::Parallel),
        ..Default::default()
    };

    let started = Instant::now();
    let event = bus.emit(event);
    let _ = block_on(event.now());
    assert!(started.elapsed() < Duration::from_millis(90));

    let results: Vec<_> = event
        ._inner_event().inner.lock()
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
    bus.destroy();
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
        bus.on_raw("timeout", handler_name, move |_event| {
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

    let mut event = TimeoutEvent {
        ..Default::default()
    };
    event.event_timeout = Some(0.03);
    let event = bus.emit(event);
    for _ in 0..40 {
        if *started.lock().expect("started lock") == 2 {
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(*started.lock().expect("started lock"), 2);
    let _ = block_on(event.now());

    let results: Vec<_> = event
        ._inner_event().inner.lock()
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
    bus.destroy();
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

    bus.on_raw("timeout", "slow", |_event| async move {
        thread::sleep(Duration::from_millis(50));
        Ok(json!("slow"))
    });

    let event = TimeoutEvent {
        event_timeout: Some(0.01),
        event_concurrency: Some(EventConcurrencyMode::Parallel),
        event_handler_concurrency: Some(EventHandlerConcurrencyMode::Parallel),
        ..Default::default()
    };
    let event = bus.emit(event);
    let _ = block_on(event.now());

    let result = event
        ._inner_event().inner.lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("result");
    assert_eq!(result.status, EventResultStatus::Error);
    assert_eq!(error_type(&result), "EventHandlerAbortedError");
    bus.destroy();
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

    bus.on_raw("timeout", "first_handler", |_event| async move {
        thread::sleep(Duration::from_millis(30));
        Ok(json!("first"))
    });
    bus.on_raw("timeout", "second_handler", |_event| async move {
        thread::sleep(Duration::from_millis(30));
        Ok(json!("second"))
    });
    bus.on_raw("timeout", "pending_handler", |_event| async move {
        Ok(json!("pending"))
    });

    let mut event = TimeoutEvent {
        ..Default::default()
    };
    event.event_timeout = Some(0.05);
    let event = bus.emit(event);
    let _ = block_on(event.now());

    let results = event.event_results.read();
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
    bus.destroy();
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
    bus_a.on_raw("parent", "parent_handler", move |_event| {
        let bus_a = bus_a_for_parent.clone();
        let child_ref = child_ref_for_parent.clone();
        async move {
            let mut child = ChildEvent {
                ..Default::default()
            };
            child.event_timeout = Some(0.01);
            let child = bus_a.emit_child(child);
            *child_ref.lock().expect("child ref lock") = Some(child._inner_event());
            let _ = child.now().await;
            Ok(json!("parent_done"))
        }
    });

    let bus_b_for_forward = bus_b.clone();
    bus_a.on_raw("*", "forward_to_b", move |event| {
        let bus_b = bus_b_for_forward.clone();
        async move {
            bus_b.emit_base(event);
            Ok(json!(null))
        }
    });
    bus_b.on_raw("child", "slow_child_handler", |_event| async move {
        thread::sleep(Duration::from_millis(50));
        Ok(json!("child_done"))
    });

    let bus_a_tail_runs_for_handler = bus_a_tail_runs.clone();
    bus_a.on_raw("tail", "tail_handler_a", move |_event| {
        let runs = bus_a_tail_runs_for_handler.clone();
        async move {
            *runs.lock().expect("bus a tail runs lock") += 1;
            Ok(json!("tail_a"))
        }
    });
    let bus_b_tail_runs_for_handler = bus_b_tail_runs.clone();
    bus_b.on_raw("tail", "tail_handler_b", move |_event| {
        let runs = bus_b_tail_runs_for_handler.clone();
        async move {
            *runs.lock().expect("bus b tail runs lock") += 1;
            Ok(json!("tail_b"))
        }
    });

    let mut parent = ParentEvent {
        ..Default::default()
    };
    parent.event_timeout = Some(1.0);
    let parent = bus_a.emit(parent);
    let _ = block_on(parent.now());
    assert!(block_on(bus_a.wait_until_idle(Some(2.0))));
    assert!(block_on(bus_b.wait_until_idle(Some(2.0))));

    let parent_result = parent
        ._inner_event().inner.lock()
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

    let mut tail = TailEvent {
        ..Default::default()
    };
    tail.event_timeout = Some(0.2);
    let tail = bus_a.emit(tail);
    let _ = block_on(tail.now());
    assert!(block_on(bus_a.wait_until_idle(Some(2.0))));
    assert!(block_on(bus_b.wait_until_idle(Some(2.0))));

    assert_eq!(
        tail.event_status.read(),
        abxbus_rust::types::EventStatus::Completed
    );
    assert_eq!(*bus_a_tail_runs.lock().expect("bus a tail runs lock"), 1);
    assert_eq!(*bus_b_tail_runs.lock().expect("bus b tail runs lock"), 1);
    bus_a.destroy();
    bus_b.destroy();
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

    bus.on_raw("timeout", "slow_handler", |_event| async move {
        thread::sleep(Duration::from_millis(50));
        Ok(json!("slow"))
    });
    bus.on_raw("timeout", "fast_handler", |_event| async move {
        Ok(json!("fast"))
    });

    let event = TimeoutEvent {
        event_timeout: Some(0.2),
        event_handler_timeout: Some(0.01),
        ..Default::default()
    };
    let event = bus.emit(event);
    let _ = block_on(event.now());

    let results = event.event_results.read();
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
    bus.destroy();
}

#[test]
fn test_handler_timeout_ignores_late_handler_result_and_late_emits() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("TimeoutIgnoresLateHandlerBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let late_handler_ran = Arc::new(Mutex::new(false));
    let (late_attempt_tx, late_attempt_rx) = mpsc::channel();

    let bus_for_slow = bus.clone();
    bus.on_raw("timeout", "slow_handler", move |_event| {
        let bus = bus_for_slow.clone();
        let late_attempt_tx = late_attempt_tx.clone();
        async move {
            thread::sleep(Duration::from_millis(40));
            let _ = late_attempt_tx.send(());
            bus.emit_child(LateAfterTimeoutEvent {
                ..Default::default()
            });
            Ok(json!("late success"))
        }
    });
    let late_handler_ran_for_handler = late_handler_ran.clone();
    bus.on_raw("late_after_timeout", "late_handler", move |_event| {
        let late_handler_ran = late_handler_ran_for_handler.clone();
        async move {
            *late_handler_ran.lock().expect("late handler lock") = true;
            Ok(json!("late child"))
        }
    });

    let event = TimeoutEvent {
        event_timeout: Some(0.2),
        event_handler_timeout: Some(0.01),
        ..Default::default()
    };
    let event = bus.emit(event);
    let _ = block_on(event.now());
    late_attempt_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("timed-out handler should still reach late emit attempt");
    thread::sleep(Duration::from_millis(30));
    block_on(bus.wait_until_idle(Some(1.0)));

    let slow_result = result_by_handler(&event._inner_event(), "slow_handler");
    assert_eq!(slow_result.status, EventResultStatus::Error);
    assert!(slow_result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("EventHandlerTimeoutError"));
    assert_eq!(slow_result.result, None);
    assert!(
        block_on(bus.find("late_after_timeout", true, None, None)).is_none(),
        "late emit from timed-out handler should not be queued or recorded"
    );
    assert!(
        !*late_handler_ran.lock().expect("late handler lock"),
        "late handler should not run after source handler timed out"
    );
    bus.destroy();
}

#[test]
fn test_event_timeout_ignores_late_handler_result_and_late_emits() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("TimeoutIgnoresLateEventBus".to_string()),
        EventBusOptions {
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let late_handler_ran = Arc::new(Mutex::new(false));
    let (late_attempt_tx, late_attempt_rx) = mpsc::channel();

    let bus_for_slow = bus.clone();
    bus.on_raw("timeout", "slow_handler", move |_event| {
        let bus = bus_for_slow.clone();
        let late_attempt_tx = late_attempt_tx.clone();
        async move {
            thread::sleep(Duration::from_millis(40));
            let _ = late_attempt_tx.send(());
            bus.emit_child(LateAfterTimeoutEvent {
                ..Default::default()
            });
            Ok(json!("late success"))
        }
    });
    let late_handler_ran_for_handler = late_handler_ran.clone();
    bus.on_raw("late_after_timeout", "late_handler", move |_event| {
        let late_handler_ran = late_handler_ran_for_handler.clone();
        async move {
            *late_handler_ran.lock().expect("late handler lock") = true;
            Ok(json!("late child"))
        }
    });

    let event = TimeoutEvent {
        event_timeout: Some(0.01),
        ..Default::default()
    };
    let event = bus.emit(event);
    let _ = block_on(event.now());
    late_attempt_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("timed-out handler should still reach late emit attempt");
    thread::sleep(Duration::from_millis(30));
    block_on(bus.wait_until_idle(Some(1.0)));

    let slow_result = result_by_handler(&event._inner_event(), "slow_handler");
    assert_eq!(slow_result.status, EventResultStatus::Error);
    assert!(slow_result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("EventHandlerAbortedError"));
    assert_eq!(slow_result.result, None);
    assert!(
        block_on(bus.find("late_after_timeout", true, None, None)).is_none(),
        "late emit from event-timed-out handler should not be queued or recorded"
    );
    assert!(
        !*late_handler_ran.lock().expect("late handler lock"),
        "late handler should not run after source handler timed out"
    );
    bus.destroy();
}

#[test]
fn test_processing_time_timeout_defaults_resolve_at_execution_time() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("TimeoutDefaultsResolveBus".to_string()),
        EventBusOptions {
            event_timeout: Some(12.0),
            event_slow_timeout: Some(34.0),
            event_handler_slow_timeout: Some(56.0),
            ..EventBusOptions::default()
        },
    );

    bus.on_raw(
        "timeout",
        "handler",
        |_event| async move { Ok(json!("ok")) },
    );

    let event = TimeoutEvent {
        ..Default::default()
    };
    {
        assert_eq!(event.event_timeout, None);
        assert_eq!(event.event_handler_timeout, None);
        assert_eq!(event.event_handler_slow_timeout, None);
        assert_eq!(event.event_slow_timeout, None);
    }

    let event = bus.emit(event);
    {
        let base = event._inner_event();
        let inner = base.inner.lock();
        assert_eq!(inner.event_timeout, None);
        assert_eq!(inner.event_handler_timeout, None);
        assert_eq!(inner.event_handler_slow_timeout, None);
        assert_eq!(inner.event_slow_timeout, None);
        assert_eq!(inner.event_concurrency, None);
        assert_eq!(inner.event_handler_concurrency, None);
        assert_eq!(inner.event_handler_completion, None);
    }
    let _ = block_on(event.now());
    {
        let base = event._inner_event();
        let inner = base.inner.lock();
        assert_eq!(inner.event_timeout, None);
        assert_eq!(inner.event_handler_slow_timeout, None);
        assert_eq!(inner.event_slow_timeout, None);
        assert_eq!(inner.event_concurrency, None);
        assert_eq!(inner.event_handler_concurrency, None);
        assert_eq!(inner.event_handler_completion, None);
    }
    let result = event
        ._inner_event().inner.lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("handler result");
    assert_eq!(result.timeout, Some(12.0));
    bus.destroy();
}

#[test]
fn test_parent_timeout_does_not_cancel_unawaited_child_with_own_timeout() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new(Some("ParentTimeoutBus".to_string()));
    let bus_for_handler = bus.clone();

    bus.on_raw("child", "child_slow", |_event| async move {
        thread::sleep(Duration::from_millis(80));
        Ok(json!("child"))
    });

    bus.on_raw("parent", "emit_child", move |_event| {
        let bus_local = bus_for_handler.clone();
        async move {
            let mut child = ChildEvent {
                ..Default::default()
            };
            child.event_timeout = Some(1.0);
            bus_local.emit_child(child);
            thread::sleep(Duration::from_millis(80));
            Ok(json!("parent"))
        }
    });

    let mut parent = ParentEvent {
        ..Default::default()
    };
    parent.event_timeout = Some(0.01);

    let parent = bus.emit(parent);
    wait_until_completed(&parent, 1000);
    thread::sleep(Duration::from_millis(120));

    let parent_result = parent
        ._inner_event().inner.lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("missing parent result");
    assert_eq!(parent_result.status, EventResultStatus::Error);

    let parent_id = parent.event_id.clone();
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
    bus.destroy();
}

#[test]
fn test_parent_timeout_does_not_cancel_unawaited_children_that_have_no_timeout_of_their_own() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("TimeoutBoundaryBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_timeout: Some(0.0),
            ..EventBusOptions::default()
        },
    );
    let bus_for_parent = bus.clone();
    let child_ref = Arc::new(Mutex::new(None::<Arc<abxbus_rust::base_event::BaseEvent>>));
    let child_ref_for_parent = child_ref.clone();

    bus.on_raw("child", "child_slow_handler", |_event| async move {
        thread::sleep(Duration::from_millis(80));
        Ok(json!("child_done"))
    });
    bus.on_raw("parent", "parent_handler", move |_event| {
        let bus = bus_for_parent.clone();
        let child_ref = child_ref_for_parent.clone();
        async move {
            let mut child = ChildEvent {
                ..Default::default()
            };
            child.event_timeout = None;
            let child = bus.emit_child(child);
            *child_ref.lock().expect("child ref lock") = Some(child._inner_event());
            thread::sleep(Duration::from_millis(80));
            Ok(json!("parent_done"))
        }
    });

    let mut parent = ParentEvent {
        ..Default::default()
    };
    parent.event_timeout = Some(0.03);
    let parent = bus.emit(parent);
    let _ = block_on(parent.now());
    assert!(block_on(bus.wait_until_idle(Some(2.0))));

    let parent_result = parent
        ._inner_event().inner.lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("parent result");
    assert_eq!(parent_result.status, EventResultStatus::Error);
    assert_eq!(error_type(&parent_result), "EventHandlerAbortedError");

    let parent_id = parent.event_id.clone();
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
    bus.destroy();
}

#[test]
fn test_parent_timeout_does_not_cancel_unawaited_child_handler_results_under_serial_handler_lock() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("TimeoutCancelBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_timeout: Some(0.0),
            ..EventBusOptions::default()
        },
    );
    let bus_for_handler = bus.clone();

    bus.on_raw("child", "child_first", |_event| async move {
        thread::sleep(Duration::from_millis(30));
        Ok(json!("first"))
    });
    bus.on_raw("child", "child_second", |_event| async move {
        thread::sleep(Duration::from_millis(10));
        Ok(json!("second"))
    });

    bus.on_raw("parent", "emit_unawaited_child", move |_event| {
        let bus = bus_for_handler.clone();
        async move {
            let mut child = ChildEvent {
                ..Default::default()
            };
            child.event_timeout = Some(0.2);
            let child = bus.emit_child(child);
            assert!(!child.event_blocks_parent_completion);
            thread::sleep(Duration::from_millis(50));
            Ok(json!("parent"))
        }
    });

    let mut parent = ParentEvent {
        ..Default::default()
    };
    parent.event_timeout = Some(0.01);
    let parent = bus.emit(parent);
    let _ = block_on(parent.now());
    assert!(block_on(bus.wait_until_idle(Some(2.0))));

    let parent_result = parent
        ._inner_event().inner.lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("missing parent result");
    assert_eq!(parent_result.status, EventResultStatus::Error);

    let parent_id = parent.event_id.clone();
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
    bus.destroy();
}

#[test]
fn test_parent_timeout_cancels_awaited_child_handler_results() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("TimeoutAwaitedChildCancelBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_timeout: Some(0.0),
            ..EventBusOptions::default()
        },
    );
    let bus_for_handler = bus.clone();

    bus.on_raw("child", "child_slow", |_event| async move {
        thread::sleep(Duration::from_millis(80));
        Ok(json!("child"))
    });

    bus.on_raw("parent", "emit_awaited_child", move |_event| {
        let bus = bus_for_handler.clone();
        async move {
            let mut child = ChildEvent {
                ..Default::default()
            };
            child.event_timeout = Some(1.0);
            let child = bus.emit_child(child);
            let _ = child.now().await;
            Ok(json!("parent"))
        }
    });

    let mut parent = ParentEvent {
        ..Default::default()
    };
    parent.event_timeout = Some(0.01);
    let parent = bus.emit(parent);
    let _ = block_on(parent.now());
    assert!(block_on(bus.wait_until_idle(Some(2.0))));

    let parent_result = parent
        ._inner_event().inner.lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("missing parent result");
    assert_eq!(parent_result.status, EventResultStatus::Error);

    let parent_id = parent.event_id.clone();
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
    bus.destroy();
}

#[test]
fn test_multi_bus_timeout_is_recorded_on_target_bus() {
    let _guard = timeout_test_guard();
    let bus_a = EventBus::new(Some("MultiTimeoutA".to_string()));
    let bus_b = EventBus::new(Some("MultiTimeoutB".to_string()));

    bus_b.on_raw("timeout", "slow_target_handler", |_event| async move {
        thread::sleep(Duration::from_millis(50));
        Ok(json!("slow"))
    });

    let mut event = TimeoutEvent {
        ..Default::default()
    };
    event.event_timeout = Some(0.01);
    let event = bus_a.emit(event);
    bus_b.emit(event.clone());
    assert!(block_on(bus_b.wait_until_idle(Some(2.0))));

    let results = event.event_results.read();
    let bus_b_result = results
        .values()
        .find(|result| result.handler.eventbus_id == bus_b.id)
        .expect("bus_b result");
    assert_eq!(bus_b_result.status, EventResultStatus::Error);
    assert_eq!(error_type(bus_b_result), "EventHandlerAbortedError");
    assert_eq!(
        event._inner_event().inner.lock().event_path,
        vec![bus_a.label(), bus_b.label()]
    );
    bus_a.destroy();
    bus_b.destroy();
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
    bus_a.on_raw("timeout", "forward_to_b", move |event| {
        let bus_b = bus_b_for_forward.clone();
        async move {
            bus_b.emit_base(event);
            Ok(json!(null))
        }
    });
    bus_b.on_raw("timeout", "slow_target_handler", |_event| async move {
        thread::sleep(Duration::from_millis(50));
        Ok(json!("slow"))
    });

    let mut event = TimeoutEvent {
        ..Default::default()
    };
    event.event_timeout = Some(0.01);
    let event = bus_a.emit(event);
    let _ = block_on(event.now());
    assert!(block_on(bus_b.wait_until_idle(Some(2.0))));

    let results = event.event_results.read();
    let bus_b_result = results
        .values()
        .find(|result| result.handler.eventbus_id == bus_b.id)
        .expect("bus_b result");
    assert_eq!(bus_b_result.status, EventResultStatus::Error);
    assert_eq!(error_type(bus_b_result), "EventHandlerAbortedError");
    bus_a.destroy();
    bus_b.destroy();
}

#[test]
fn test_queue_jump_awaited_child_timeout_aborts_still_fire_across_buses() {
    let _guard = timeout_test_guard();
    let bus_a = EventBus::new_with_options(
        Some("TimeoutQueueJumpA".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::GlobalSerial,
            event_timeout: Some(0.0),
            ..EventBusOptions::default()
        },
    );
    let bus_b = EventBus::new_with_options(
        Some("TimeoutQueueJumpB".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::GlobalSerial,
            event_timeout: Some(0.0),
            ..EventBusOptions::default()
        },
    );
    let child_ref = Arc::new(Mutex::new(None));

    bus_b.on_raw("child", "slow_child_handler", |_event| async move {
        thread::sleep(Duration::from_millis(50));
        Ok(json!("slow"))
    });

    let bus_a_for_parent = bus_a.clone();
    let bus_b_for_parent = bus_b.clone();
    let child_ref_for_parent = child_ref.clone();
    bus_a.on_raw("parent", "parent_handler", move |_event| {
        let bus_a = bus_a_for_parent.clone();
        let bus_b = bus_b_for_parent.clone();
        let child_ref = child_ref_for_parent.clone();
        async move {
            let mut child = ChildEvent {
                ..Default::default()
            };
            child.event_timeout = Some(0.01);
            let child = bus_a.emit_child(child);
            bus_b.emit(child.clone());
            *child_ref.lock().expect("child ref lock") = Some(child._inner_event());
            let _ = child.now().await;
            Ok(json!(null))
        }
    });

    let mut parent = ParentEvent {
        ..Default::default()
    };
    parent.event_timeout = Some(2.0);
    let parent = bus_a.emit(parent);
    let _ = block_on(parent.now());
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
    bus_a.destroy();
    bus_b.destroy();
}

#[test]
fn test_followup_event_runs_after_parent_timeout_in_queue_jump_path() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new(Some("TimeoutQueueJumpFollowupBus".to_string()));
    let bus_for_parent = bus.clone();
    let tail_runs = Arc::new(Mutex::new(0usize));

    bus.on_raw("child", "child_handler", |_event| async move {
        thread::sleep(Duration::from_millis(1));
        Ok(json!("child_done"))
    });
    bus.on_raw("parent", "parent_handler", move |_event| {
        let bus = bus_for_parent.clone();
        async move {
            let mut child = ChildEvent {
                ..Default::default()
            };
            child.event_timeout = Some(0.2);
            let child = bus.emit_child(child);
            let _ = child.now().await;
            thread::sleep(Duration::from_millis(50));
            Ok(json!("parent_done"))
        }
    });
    let tail_runs_for_handler = tail_runs.clone();
    bus.on_raw("tail", "tail_handler", move |_event| {
        let tail_runs = tail_runs_for_handler.clone();
        async move {
            *tail_runs.lock().expect("tail runs lock") += 1;
            Ok(json!("tail_done"))
        }
    });

    let mut parent = ParentEvent {
        ..Default::default()
    };
    parent.event_timeout = Some(0.02);
    let parent = bus.emit(parent);
    let _ = block_on(parent.now());
    assert!(block_on(bus.wait_until_idle(Some(2.0))));

    let parent_result = parent
        ._inner_event().inner.lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("parent result");
    assert_eq!(parent_result.status, EventResultStatus::Error);
    assert_eq!(error_type(&parent_result), "EventHandlerAbortedError");

    let mut tail = TailEvent {
        ..Default::default()
    };
    tail.event_timeout = Some(0.2);
    let tail = bus.emit(tail);
    let _ = block_on(tail.now());
    assert_eq!(
        tail.event_status.read(),
        abxbus_rust::types::EventStatus::Completed
    );
    assert_eq!(*tail_runs.lock().expect("tail runs lock"), 1);
    bus.destroy();
}

#[test]
fn test_regression_parent_timeout_while_reacquire_waits_behind_third_serial_handler_is_lock_safe_handler_mode_label(
) {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("TimeoutContentionBusSerial".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let tail_runs = Arc::new(Mutex::new(0usize));

    bus.on_raw("child", "child_handler", |_event| async move {
        thread::sleep(Duration::from_millis(20));
        Ok(json!("child_done"))
    });

    let bus_for_parent = bus.clone();
    bus.on_raw("parent", "parent_main", move |_event| {
        let bus = bus_for_parent.clone();
        async move {
            let mut child = ChildEvent {
                ..Default::default()
            };
            child.event_timeout = Some(0.2);
            let child = bus.emit_child(child);
            let _ = child.now().await;
            thread::sleep(Duration::from_millis(40));
            Ok(json!("parent_main"))
        }
    });
    bus.on_raw("parent", "parent_blocker", |_event| async move {
        thread::sleep(Duration::from_millis(40));
        Ok(json!("parent_blocker"))
    });

    let tail_runs_for_handler = tail_runs.clone();
    bus.on_raw("tail", "tail_handler", move |_event| {
        let tail_runs = tail_runs_for_handler.clone();
        async move {
            *tail_runs.lock().expect("tail runs lock") += 1;
            Ok(json!("tail_done"))
        }
    });

    let mut parent = ParentEvent {
        ..Default::default()
    };
    parent.event_timeout = Some(0.01);
    let parent = bus.emit(parent);
    let _ = block_on(parent.now());
    assert!(block_on(bus.wait_until_idle(Some(2.0))));

    let parent_results: Vec<_> = parent
        ._inner_event().inner.lock()
        .event_results
        .values()
        .cloned()
        .collect();
    assert!(parent_results.iter().any(|result| {
        result.status == EventResultStatus::Error
            && error_type(result) == "EventHandlerAbortedError"
    }));

    let mut tail = TailEvent {
        ..Default::default()
    };
    tail.event_timeout = Some(0.05);
    let tail = bus.emit(tail);
    let _ = block_on(tail.now());
    assert_eq!(tail.event_status.read(), EventStatus::Completed);
    assert_eq!(*tail_runs.lock().expect("tail runs lock"), 1);
    bus.destroy();
}

#[test]
fn test_regression_nested_queue_jump_with_timeout_cancellation_remains_lock_safe_handler_mode_label(
) {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("NestedPermitBusSerial".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            ..EventBusOptions::default()
        },
    );
    let queued_sibling_runs = Arc::new(Mutex::new(0usize));
    let tail_runs = Arc::new(Mutex::new(0usize));
    let queued_sibling_ref = Arc::new(Mutex::new(None::<Arc<BaseEvent>>));

    bus.on_raw("grandchild", "grandchild_handler", |_event| async move {
        thread::sleep(Duration::from_millis(1));
        Ok(json!("grandchild_done"))
    });

    let bus_for_child = bus.clone();
    bus.on_raw("child", "child_handler", move |_event| {
        let bus = bus_for_child.clone();
        async move {
            let mut grandchild = GrandchildEvent {
                ..Default::default()
            };
            grandchild.event_timeout = Some(0.2);
            let grandchild = bus.emit_child(grandchild);
            let _ = grandchild.now().await;
            thread::sleep(Duration::from_millis(40));
            Ok(json!("child_done"))
        }
    });

    let queued_sibling_runs_for_handler = queued_sibling_runs.clone();
    bus.on_raw("queued_sibling", "queued_sibling_handler", move |_event| {
        let queued_sibling_runs = queued_sibling_runs_for_handler.clone();
        async move {
            *queued_sibling_runs
                .lock()
                .expect("queued sibling runs lock") += 1;
            Ok(json!("queued_sibling_done"))
        }
    });

    let bus_for_parent = bus.clone();
    let queued_sibling_ref_for_parent = queued_sibling_ref.clone();
    bus.on_raw("parent", "parent_handler", move |_event| {
        let bus = bus_for_parent.clone();
        let queued_sibling_ref = queued_sibling_ref_for_parent.clone();
        async move {
            let mut queued_sibling = QueuedSiblingEvent {
                ..Default::default()
            };
            queued_sibling.event_timeout = Some(0.2);
            let queued_sibling = bus.emit_child(queued_sibling);
            *queued_sibling_ref.lock().expect("queued sibling ref lock") =
                Some(queued_sibling._inner_event());

            let mut child = ChildEvent {
                ..Default::default()
            };
            child.event_timeout = Some(0.02);
            let child = bus.emit_child(child);
            let _ = child.now().await;
            thread::sleep(Duration::from_millis(40));
            Ok(json!(null))
        }
    });

    let tail_runs_for_handler = tail_runs.clone();
    bus.on_raw("tail", "tail_handler", move |_event| {
        let tail_runs = tail_runs_for_handler.clone();
        async move {
            *tail_runs.lock().expect("tail runs lock") += 1;
            Ok(json!("tail_done"))
        }
    });

    let mut parent = ParentEvent {
        ..Default::default()
    };
    parent.event_timeout = Some(0.03);
    let parent = bus.emit(parent);
    let _ = block_on(parent.now());
    assert!(block_on(bus.wait_until_idle(Some(2.0))));

    let parent_result = first_result_for_event(&parent._inner_event());
    assert_eq!(parent_result.status, EventResultStatus::Error);
    assert_eq!(error_type(&parent_result), "EventHandlerAbortedError");

    let queued_sibling = queued_sibling_ref
        .lock()
        .expect("queued sibling ref lock")
        .clone()
        .expect("queued sibling ref");
    assert_eq!(
        queued_sibling.inner.lock().event_parent_id,
        Some(parent.event_id.clone())
    );
    assert!(!queued_sibling.inner.lock().event_blocks_parent_completion);
    assert_eq!(*queued_sibling_runs.lock().expect("runs lock"), 1);
    let queued_sibling_results: Vec<_> = queued_sibling
        .inner
        .lock()
        .event_results
        .values()
        .cloned()
        .collect();
    assert!(
        queued_sibling_results
            .iter()
            .all(|result| result.status == EventResultStatus::Completed),
        "queued sibling results should complete: {:?}",
        queued_sibling_results
            .iter()
            .map(|result| result.to_flat_json_value())
            .collect::<Vec<_>>()
    );

    let mut tail = TailEvent {
        ..Default::default()
    };
    tail.event_timeout = Some(0.05);
    let tail = bus.emit(tail);
    let _ = block_on(tail.now());
    assert_eq!(tail.event_status.read(), EventStatus::Completed);
    assert_eq!(*tail_runs.lock().expect("tail runs lock"), 1);
    bus.destroy();
}

#[test]
fn test_absent_event_timeout_falls_back_to_bus_default() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("TimeoutDefaultBus".to_string()),
        EventBusOptions {
            event_timeout: Some(0.01),
            ..EventBusOptions::default()
        },
    );

    bus.on_raw("timeout", "slow", |_event| async move {
        thread::sleep(Duration::from_millis(50));
        Ok(json!("slow"))
    });

    let event = TimeoutEvent {
        ..Default::default()
    };
    let event = bus.emit(event);
    let _ = block_on(event.now());

    let result = event
        ._inner_event().inner.lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("result");
    assert_eq!(result.status, EventResultStatus::Error);
    assert_eq!(error_type(&result), "EventHandlerAbortedError");
    bus.destroy();
}

#[test]
fn test_event_timeout_none_uses_bus_default_timeout_at_execution() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("TimeoutNoneUsesBusDefault".to_string()),
        EventBusOptions {
            event_timeout: Some(0.01),
            ..EventBusOptions::default()
        },
    );

    bus.on_raw("timeout", "slow", |_event| async move {
        thread::sleep(Duration::from_millis(20));
        Ok(json!("ok"))
    });

    let event = bus.emit(TimeoutEvent {
        event_timeout: None,
        ..Default::default()
    });
    let _ = block_on(event.now());

    let result = event
        ._inner_event().inner.lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("result");
    assert_eq!(event._inner_event().inner.lock().event_timeout, None);
    assert_eq!(result.status, EventResultStatus::Error);
    assert_eq!(error_type(&result), "EventHandlerAbortedError");
    bus.destroy();
}

#[test]
fn test_multi_level_timeout_cascade_with_mixed_cancellations() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("TimeoutCascadeBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_timeout: Some(0.0),
            ..EventBusOptions::default()
        },
    );

    let queued_child_ref = Arc::new(Mutex::new(None));
    let awaited_child_ref = Arc::new(Mutex::new(None));
    let immediate_grandchild_ref = Arc::new(Mutex::new(None));
    let queued_grandchild_ref = Arc::new(Mutex::new(None));
    let queued_child_runs = Arc::new(Mutex::new(0usize));
    let immediate_grandchild_runs = Arc::new(Mutex::new(0usize));
    let queued_grandchild_runs = Arc::new(Mutex::new(0usize));

    for (name, delay_ms) in [("queued_child_fast", 5_u64), ("queued_child_slow", 50_u64)] {
        let runs = queued_child_runs.clone();
        bus.on_raw("TimeoutCascadeQueuedChild", name, move |_event| {
            let runs = runs.clone();
            async move {
                *runs.lock().expect("queued child runs") += 1;
                thread::sleep(Duration::from_millis(delay_ms));
                Ok(json!(name))
            }
        });
    }

    bus.on_raw(
        "TimeoutCascadeAwaitedChild",
        "awaited_child_fast",
        |_event| async move {
            thread::sleep(Duration::from_millis(5));
            Ok(json!("awaited_fast"))
        },
    );
    let bus_for_awaited = bus.clone();
    let immediate_ref_for_handler = immediate_grandchild_ref.clone();
    let queued_gc_ref_for_handler = queued_grandchild_ref.clone();
    bus.on_raw(
        "TimeoutCascadeAwaitedChild",
        "awaited_child_slow",
        move |_event| {
            let bus = bus_for_awaited.clone();
            let immediate_ref = immediate_ref_for_handler.clone();
            let queued_gc_ref = queued_gc_ref_for_handler.clone();
            async move {
                let queued_grandchild = timeout_event("TimeoutCascadeQueuedGrandchild", Some(0.2));
                let immediate_grandchild =
                    timeout_event("TimeoutCascadeImmediateGrandchild", Some(0.2));
                bus.emit_child_base(queued_grandchild.clone());
                bus.emit_child_base(immediate_grandchild.clone());
                *queued_gc_ref.lock().expect("queued grandchild ref") = Some(queued_grandchild);
                *immediate_ref.lock().expect("immediate grandchild ref") =
                    Some(immediate_grandchild.clone());
                let _ = immediate_grandchild.now().await;
                thread::sleep(Duration::from_millis(100));
                Ok(json!("awaited_slow"))
            }
        },
    );

    for (name, delay_ms) in [
        ("immediate_grandchild_slow", 50_u64),
        ("immediate_grandchild_fast", 10_u64),
    ] {
        let runs = immediate_grandchild_runs.clone();
        bus.on_raw("TimeoutCascadeImmediateGrandchild", name, move |_event| {
            let runs = runs.clone();
            async move {
                *runs.lock().expect("immediate grandchild runs") += 1;
                thread::sleep(Duration::from_millis(delay_ms));
                Ok(json!(name))
            }
        });
    }

    for (name, delay_ms) in [
        ("queued_grandchild_slow", 50_u64),
        ("queued_grandchild_fast", 10_u64),
    ] {
        let runs = queued_grandchild_runs.clone();
        bus.on_raw("TimeoutCascadeQueuedGrandchild", name, move |_event| {
            let runs = runs.clone();
            async move {
                *runs.lock().expect("queued grandchild runs") += 1;
                thread::sleep(Duration::from_millis(delay_ms));
                Ok(json!(name))
            }
        });
    }

    let bus_for_top = bus.clone();
    let queued_child_ref_for_top = queued_child_ref.clone();
    let awaited_child_ref_for_top = awaited_child_ref.clone();
    bus.on_raw("TimeoutCascadeTop", "top_handler", move |_event| {
        let bus = bus_for_top.clone();
        let queued_child_ref = queued_child_ref_for_top.clone();
        let awaited_child_ref = awaited_child_ref_for_top.clone();
        async move {
            let queued_child = timeout_event("TimeoutCascadeQueuedChild", Some(0.2));
            let awaited_child = timeout_event("TimeoutCascadeAwaitedChild", Some(0.03));
            bus.emit_child_base(queued_child.clone());
            bus.emit_child_base(awaited_child.clone());
            *queued_child_ref.lock().expect("queued child ref") = Some(queued_child);
            *awaited_child_ref.lock().expect("awaited child ref") = Some(awaited_child.clone());
            let _ = awaited_child.now().await;
            thread::sleep(Duration::from_millis(80));
            Ok(json!(null))
        }
    });

    let top = timeout_event("TimeoutCascadeTop", Some(0.04));
    bus.emit_base(top.clone());
    let _ = block_on(top.now());
    assert!(block_on(bus.wait_until_idle(Some(2.0))));

    let top_result = top
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("top result");
    assert_eq!(top_result.status, EventResultStatus::Error);
    assert_eq!(error_type(&top_result), "EventHandlerAbortedError");

    let queued_child = queued_child_ref
        .lock()
        .expect("queued child ref")
        .clone()
        .expect("queued child");
    let queued_results: Vec<_> = queued_child
        .inner
        .lock()
        .event_results
        .values()
        .cloned()
        .collect();
    assert!(!queued_child.inner.lock().event_blocks_parent_completion);
    assert_eq!(*queued_child_runs.lock().expect("queued child runs"), 2);
    assert_eq!(queued_results.len(), 2);
    assert!(queued_results
        .iter()
        .all(|result| result.status == EventResultStatus::Completed));

    let awaited_child = awaited_child_ref
        .lock()
        .expect("awaited child ref")
        .clone()
        .expect("awaited child");
    let awaited_results: Vec<_> = awaited_child
        .inner
        .lock()
        .event_results
        .values()
        .cloned()
        .collect();
    assert_eq!(
        awaited_results
            .iter()
            .filter(|result| result.status == EventResultStatus::Completed)
            .count(),
        1
    );
    assert_eq!(
        awaited_results
            .iter()
            .filter(|result| error_type(result) == "EventHandlerAbortedError")
            .count(),
        1
    );

    let immediate_grandchild = immediate_grandchild_ref
        .lock()
        .expect("immediate grandchild ref")
        .clone()
        .expect("immediate grandchild");
    let immediate_results: Vec<_> = immediate_grandchild
        .inner
        .lock()
        .event_results
        .values()
        .cloned()
        .collect();
    assert_eq!(
        *immediate_grandchild_runs
            .lock()
            .expect("immediate grandchild runs"),
        1
    );
    assert_eq!(
        immediate_results
            .iter()
            .filter(|result| error_type(result) == "EventHandlerAbortedError")
            .count(),
        1
    );
    assert_eq!(
        immediate_results
            .iter()
            .filter(|result| error_type(result) == "EventHandlerCancelledError")
            .count(),
        1
    );

    let queued_grandchild = queued_grandchild_ref
        .lock()
        .expect("queued grandchild ref")
        .clone()
        .expect("queued grandchild");
    let queued_grandchild_results: Vec<_> = queued_grandchild
        .inner
        .lock()
        .event_results
        .values()
        .cloned()
        .collect();
    assert!(
        !queued_grandchild
            .inner
            .lock()
            .event_blocks_parent_completion
    );
    assert_eq!(
        *queued_grandchild_runs
            .lock()
            .expect("queued grandchild runs"),
        2
    );
    assert_eq!(queued_grandchild_results.len(), 2);
    assert!(queued_grandchild_results
        .iter()
        .all(|result| result.status == EventResultStatus::Completed));

    bus.destroy();
}

#[test]
fn test_unawaited_descendant_preserves_lineage_and_is_not_cancelled_by_ancestor_timeout() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("ErrorChainBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_timeout: Some(0.0),
            ..EventBusOptions::default()
        },
    );

    let inner_ref = Arc::new(Mutex::new(None));
    let deep_ref = Arc::new(Mutex::new(None));

    bus.on_raw("ErrorChainDeep", "deep_handler", |_event| async move {
        thread::sleep(Duration::from_millis(200));
        Ok(json!("deep_done"))
    });

    let bus_for_inner = bus.clone();
    let deep_ref_for_inner = deep_ref.clone();
    bus.on_raw("ErrorChainInner", "inner_handler", move |_event| {
        let bus = bus_for_inner.clone();
        let deep_ref = deep_ref_for_inner.clone();
        async move {
            let deep = timeout_event("ErrorChainDeep", Some(0.5));
            bus.emit_child_base(deep.clone());
            *deep_ref.lock().expect("deep ref") = Some(deep);
            thread::sleep(Duration::from_millis(200));
            Ok(json!("inner_done"))
        }
    });

    let bus_for_outer = bus.clone();
    let inner_ref_for_outer = inner_ref.clone();
    bus.on_raw("ErrorChainOuter", "outer_handler", move |_event| {
        let bus = bus_for_outer.clone();
        let inner_ref = inner_ref_for_outer.clone();
        async move {
            let inner = timeout_event("ErrorChainInner", Some(0.04));
            bus.emit_child_base(inner.clone());
            *inner_ref.lock().expect("inner ref") = Some(inner.clone());
            let _ = inner.now().await;
            thread::sleep(Duration::from_millis(200));
            Ok(json!("outer_done"))
        }
    });

    let outer = timeout_event("ErrorChainOuter", Some(0.15));
    bus.emit_base(outer.clone());
    let _ = block_on(outer.now());
    assert!(block_on(bus.wait_until_idle(Some(2.0))));

    let outer_result = outer
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("outer result");
    assert_eq!(outer_result.status, EventResultStatus::Error);
    assert_eq!(error_type(&outer_result), "EventHandlerAbortedError");

    let inner = inner_ref
        .lock()
        .expect("inner ref")
        .clone()
        .expect("inner event");
    let inner_result = inner
        .inner
        .lock()
        .event_results
        .values()
        .next()
        .cloned()
        .expect("inner result");
    assert_eq!(inner_result.status, EventResultStatus::Error);
    assert_eq!(error_type(&inner_result), "EventHandlerAbortedError");

    let deep = deep_ref
        .lock()
        .expect("deep ref")
        .clone()
        .expect("deep event");
    let deep_inner = deep.inner.lock();
    let inner_id = inner.inner.lock().event_id.clone();
    assert_eq!(
        deep_inner.event_parent_id.as_deref(),
        Some(inner_id.as_str())
    );
    assert!(!deep_inner.event_blocks_parent_completion);
    let deep_result = deep_inner
        .event_results
        .values()
        .next()
        .cloned()
        .expect("deep result");
    assert_eq!(deep_result.status, EventResultStatus::Completed);
    assert_eq!(deep_result.result, Some(json!("deep_done")));

    bus.destroy();
}

#[test]
fn test_three_level_timeout_cascade_with_per_level_timeouts_and_cascading_cancellation() {
    let _guard = timeout_test_guard();
    let bus = EventBus::new_with_options(
        Some("Cascade3LevelBus".to_string()),
        EventBusOptions {
            event_concurrency: EventConcurrencyMode::BusSerial,
            event_handler_concurrency: EventHandlerConcurrencyMode::Serial,
            event_timeout: Some(0.0),
            ..EventBusOptions::default()
        },
    );

    let execution_log = Arc::new(Mutex::new(Vec::<String>::new()));
    let child_ref = Arc::new(Mutex::new(None));
    let grandchild_ref = Arc::new(Mutex::new(None));
    let queued_grandchild_ref = Arc::new(Mutex::new(None));
    let sibling_ref = Arc::new(Mutex::new(None));

    let log = execution_log.clone();
    bus.on_raw("Cascade3LGrandchild", "gc_handler_a", move |_event| {
        let log = log.clone();
        async move {
            log.lock()
                .expect("execution log")
                .push("gc_a_start".to_string());
            thread::sleep(Duration::from_millis(500));
            log.lock()
                .expect("execution log")
                .push("gc_a_end".to_string());
            Ok(json!("gc_a_done"))
        }
    });
    let log = execution_log.clone();
    bus.on_raw("Cascade3LGrandchild", "gc_handler_b", move |_event| {
        let log = log.clone();
        async move {
            log.lock()
                .expect("execution log")
                .push("gc_b_complete".to_string());
            Ok(json!("gc_b_done"))
        }
    });
    for handler_name in ["gc_handler_c", "gc_handler_e"] {
        let log = execution_log.clone();
        bus.on_raw("Cascade3LGrandchild", handler_name, move |_event| {
            let log = log.clone();
            async move {
                log.lock()
                    .expect("execution log")
                    .push(format!("{handler_name}_start"));
                thread::sleep(Duration::from_millis(500));
                log.lock()
                    .expect("execution log")
                    .push(format!("{handler_name}_end"));
                Ok(json!(handler_name))
            }
        });
    }
    let log = execution_log.clone();
    bus.on_raw("Cascade3LGrandchild", "gc_handler_d", move |_event| {
        let log = log.clone();
        async move {
            log.lock()
                .expect("execution log")
                .push("gc_d_start".to_string());
            thread::sleep(Duration::from_millis(10));
            log.lock()
                .expect("execution log")
                .push("gc_d_complete".to_string());
            Ok(json!("gc_d_done"))
        }
    });

    let log = execution_log.clone();
    bus.on_raw("Cascade3LQueuedGC", "queued_gc_handler", move |_event| {
        let log = log.clone();
        async move {
            log.lock()
                .expect("execution log")
                .push("queued_gc_start".to_string());
            Ok(json!("queued_gc_done"))
        }
    });

    let bus_for_child = bus.clone();
    let log = execution_log.clone();
    let grandchild_ref_for_child = grandchild_ref.clone();
    let queued_gc_ref_for_child = queued_grandchild_ref.clone();
    bus.on_raw("Cascade3LChild", "child_handler", move |_event| {
        let bus = bus_for_child.clone();
        let log = log.clone();
        let grandchild_ref = grandchild_ref_for_child.clone();
        let queued_gc_ref = queued_gc_ref_for_child.clone();
        async move {
            log.lock()
                .expect("execution log")
                .push("child_start".to_string());
            let grandchild = timeout_event("Cascade3LGrandchild", Some(0.035));
            let queued_grandchild = timeout_event("Cascade3LQueuedGC", Some(0.5));
            bus.emit_child_base(grandchild.clone());
            bus.emit_child_base(queued_grandchild.clone());
            *grandchild_ref.lock().expect("grandchild ref") = Some(grandchild.clone());
            *queued_gc_ref.lock().expect("queued gc ref") = Some(queued_grandchild);
            let _ = grandchild.now().await;
            log.lock()
                .expect("execution log")
                .push("child_after_grandchild".to_string());
            thread::sleep(Duration::from_millis(300));
            log.lock()
                .expect("execution log")
                .push("child_end".to_string());
            Ok(json!("child_done"))
        }
    });

    let log = execution_log.clone();
    bus.on_raw("Cascade3LSibling", "sibling_handler", move |_event| {
        let log = log.clone();
        async move {
            log.lock()
                .expect("execution log")
                .push("sibling_start".to_string());
            Ok(json!("sibling_done"))
        }
    });

    let log = execution_log.clone();
    bus.on_raw("Cascade3LTop", "top_handler_fast", move |_event| {
        let log = log.clone();
        async move {
            log.lock()
                .expect("execution log")
                .push("top_fast_start".to_string());
            thread::sleep(Duration::from_millis(2));
            log.lock()
                .expect("execution log")
                .push("top_fast_complete".to_string());
            Ok(json!("top_fast_done"))
        }
    });

    let bus_for_top = bus.clone();
    let log = execution_log.clone();
    let child_ref_for_top = child_ref.clone();
    let sibling_ref_for_top = sibling_ref.clone();
    bus.on_raw("Cascade3LTop", "top_handler_main", move |_event| {
        let bus = bus_for_top.clone();
        let log = log.clone();
        let child_ref = child_ref_for_top.clone();
        let sibling_ref = sibling_ref_for_top.clone();
        async move {
            log.lock()
                .expect("execution log")
                .push("top_main_start".to_string());
            let child = timeout_event("Cascade3LChild", Some(0.15));
            let sibling = timeout_event("Cascade3LSibling", Some(0.5));
            bus.emit_child_base(child.clone());
            bus.emit_child_base(sibling.clone());
            *child_ref.lock().expect("child ref") = Some(child.clone());
            *sibling_ref.lock().expect("sibling ref") = Some(sibling);
            let _ = child.now().await;
            log.lock()
                .expect("execution log")
                .push("top_main_after_child".to_string());
            thread::sleep(Duration::from_millis(300));
            log.lock()
                .expect("execution log")
                .push("top_main_end".to_string());
            Ok(json!("top_main_done"))
        }
    });

    let top = timeout_event("Cascade3LTop", Some(0.25));
    bus.emit_base(top.clone());
    let _ = block_on(top.now());
    assert!(block_on(bus.wait_until_idle(Some(3.0))));

    let (top_status, top_result_count) = {
        let top_inner = top.inner.lock();
        (top_inner.event_status, top_inner.event_results.len())
    };
    assert_eq!(top_status, EventStatus::Completed);
    assert!(!top.event_errors().is_empty());
    assert_eq!(top_result_count, 2);

    let top_fast = result_by_handler(&top, "top_handler_fast");
    assert_eq!(top_fast.status, EventResultStatus::Completed);
    assert_eq!(top_fast.result, Some(json!("top_fast_done")));
    let top_main = result_by_handler(&top, "top_handler_main");
    assert_eq!(top_main.status, EventResultStatus::Error);
    assert_eq!(error_type(&top_main), "EventHandlerAbortedError");

    let child = child_ref
        .lock()
        .expect("child ref")
        .clone()
        .expect("child event");
    let child_result = result_by_handler(&child, "child_handler");
    assert_eq!(child.inner.lock().event_status, EventStatus::Completed);
    assert_eq!(child_result.status, EventResultStatus::Error);
    assert_eq!(error_type(&child_result), "EventHandlerAbortedError");

    let grandchild = grandchild_ref
        .lock()
        .expect("grandchild ref")
        .clone()
        .expect("grandchild event");
    assert_eq!(grandchild.inner.lock().event_status, EventStatus::Completed);
    let grandchild_results: Vec<_> = grandchild
        .inner
        .lock()
        .event_results
        .values()
        .cloned()
        .collect();
    assert_eq!(grandchild_results.len(), 5);
    assert_eq!(
        grandchild_results
            .iter()
            .filter(|result| error_type(result) == "EventHandlerAbortedError")
            .count(),
        1
    );
    assert_eq!(
        error_type(&result_by_handler(&grandchild, "gc_handler_a")),
        "EventHandlerAbortedError"
    );
    assert_eq!(
        grandchild_results
            .iter()
            .filter(|result| {
                matches!(
                    error_type(result).as_str(),
                    "EventHandlerCancelledError" | "EventHandlerAbortedError"
                )
            })
            .count(),
        5
    );

    let queued_grandchild = queued_grandchild_ref
        .lock()
        .expect("queued grandchild ref")
        .clone()
        .expect("queued grandchild");
    assert_eq!(
        queued_grandchild.inner.lock().event_status,
        EventStatus::Completed
    );
    assert!(
        !queued_grandchild
            .inner
            .lock()
            .event_blocks_parent_completion
    );
    let queued_gc_result = result_by_handler(&queued_grandchild, "queued_gc_handler");
    assert_eq!(queued_gc_result.status, EventResultStatus::Completed);
    assert_eq!(queued_gc_result.result, Some(json!("queued_gc_done")));

    let sibling = sibling_ref
        .lock()
        .expect("sibling ref")
        .clone()
        .expect("sibling event");
    assert_eq!(sibling.inner.lock().event_status, EventStatus::Completed);
    assert!(!sibling.inner.lock().event_blocks_parent_completion);
    let sibling_result = result_by_handler(&sibling, "sibling_handler");
    assert_eq!(sibling_result.status, EventResultStatus::Completed);
    assert_eq!(sibling_result.result, Some(json!("sibling_done")));

    let log = execution_log.lock().expect("execution log").clone();
    assert!(log.contains(&"top_fast_start".to_string()));
    assert!(log.contains(&"top_fast_complete".to_string()));
    assert!(log.contains(&"gc_a_start".to_string()));
    assert!(!log.contains(&"gc_a_end".to_string()));
    assert!(!log.contains(&"gc_b_complete".to_string()));
    assert!(!log.contains(&"gc_d_start".to_string()));
    assert!(!log.contains(&"gc_d_complete".to_string()));
    assert!(log.contains(&"top_main_start".to_string()));
    assert!(log.contains(&"child_start".to_string()));
    assert!(log.contains(&"child_after_grandchild".to_string()));
    assert!(log.contains(&"top_main_after_child".to_string()));
    assert!(!log.contains(&"child_end".to_string()));
    assert!(!log.contains(&"top_main_end".to_string()));
    assert!(log.contains(&"queued_gc_start".to_string()));
    assert!(log.contains(&"sibling_start".to_string()));

    let top_children: Vec<_> = top
        .inner
        .lock()
        .event_results
        .values()
        .flat_map(|result| result.event_children.clone())
        .collect();
    let child_id = child.inner.lock().event_id.clone();
    let sibling_id = sibling.inner.lock().event_id.clone();
    assert!(top_children.contains(&child_id));
    assert!(top_children.contains(&sibling_id));

    let child_children: Vec<_> = child
        .inner
        .lock()
        .event_results
        .values()
        .flat_map(|result| result.event_children.clone())
        .collect();
    let grandchild_id = grandchild.inner.lock().event_id.clone();
    let queued_grandchild_id = queued_grandchild.inner.lock().event_id.clone();
    assert!(child_children.contains(&grandchild_id));
    assert!(child_children.contains(&queued_grandchild_id));

    for event in [&top, &child, &grandchild, &queued_grandchild, &sibling] {
        assert!(
            event.inner.lock().event_completed_at.is_some(),
            "{} should have event_completed_at",
            event.inner.lock().event_type
        );
    }
    for result in top.inner.lock().event_results.values() {
        assert!(result.started_at.is_some());
        assert!(result.completed_at.is_some());
    }
    for result in grandchild.inner.lock().event_results.values() {
        if error_type(result) != "EventHandlerCancelledError" {
            assert!(result.started_at.is_some());
        }
        assert!(result.completed_at.is_some());
    }

    bus.destroy();
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

    bus.on_raw("timeout_defaults", "default_handler", |_event| async move {
        Ok(json!("default"))
    });
    bus.on_raw_with_options(
        "timeout_defaults",
        "overridden_handler",
        EventHandlerOptions {
            handler_timeout: Some(0.12),
            ..EventHandlerOptions::default()
        },
        |_event| async move { Ok(json!("override")) },
    );

    let event = TimeoutDefaultsEvent {
        ..Default::default()
    };
    let event = bus.emit(event);
    let _ = block_on(event.now());
    let results = event.event_results.read();
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

    let tighter_event_timeout = TimeoutDefaultsEvent {
        event_timeout: Some(0.08),
        event_handler_timeout: Some(0.2),
        ..Default::default()
    };
    let tighter_event_timeout = bus.emit(tighter_event_timeout);
    let _ = block_on(tighter_event_timeout.now());
    let tighter_results = tighter_event_timeout
        ._inner_event().inner.lock()
        .event_results
        .clone();
    assert!(tighter_results
        .values()
        .all(|result| result.timeout == Some(0.08)));

    bus.destroy();
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

    let entry = bus.on_raw("timeout_defaults", "handler", |_event| async move {
        Ok(json!("ok"))
    });
    assert_eq!(entry.handler_file_path, None);
    bus.destroy();
}

#[test]
fn test_handler_slow_warning_uses_event_handler_slow_timeout() {
    let stderr = run_slow_warning_child("_inner_event_slow_handler_warning_child");
    let slow_warning_index = stderr
        .to_lowercase()
        .find("slow event handler")
        .unwrap_or_else(|| panic!("expected slow handler warning in stderr, got: {stderr}"));
    let finish_marker_index = stderr
        .find("slow warning child handler finishing")
        .unwrap_or_else(|| panic!("expected finish marker in stderr, got: {stderr}"));
    assert!(
        slow_warning_index < finish_marker_index,
        "slow handler warning should be emitted while handler is still running, got: {stderr}"
    );
    assert!(
        !stderr.to_lowercase().contains("slow event processing"),
        "handler-only slow warning should not also emit event warning, got: {stderr}"
    );
}

#[test]
fn test_event_slow_warning_uses_event_slow_timeout() {
    let stderr = run_slow_warning_child("_inner_event_slow_event_warning_child");
    let slow_warning_index = stderr
        .to_lowercase()
        .find("slow event processing")
        .unwrap_or_else(|| panic!("expected slow event warning in stderr, got: {stderr}"));
    let finish_marker_index = stderr
        .find("slow warning child handler finishing")
        .unwrap_or_else(|| panic!("expected finish marker in stderr, got: {stderr}"));
    assert!(
        slow_warning_index < finish_marker_index,
        "slow event warning should be emitted while event is still running, got: {stderr}"
    );
    assert!(
        !stderr.to_lowercase().contains("slow event handler"),
        "event-only slow warning should not also emit handler warning, got: {stderr}"
    );
}

#[test]
fn test_slow_handler_and_slow_event_warnings_can_both_fire() {
    let stderr = run_slow_warning_child("_inner_event_slow_handler_and_event_warning_child");
    assert!(
        stderr.to_lowercase().contains("slow event handler"),
        "expected slow handler warning in stderr, got: {stderr}"
    );
    assert!(
        stderr.to_lowercase().contains("slow event processing"),
        "expected slow event warning in stderr, got: {stderr}"
    );
}

#[test]
fn test_zero_slow_warning_thresholds_disable_event_and_handler_slow_warnings() {
    let stderr = run_slow_warning_child("_inner_event_no_slow_warning_child");
    assert!(
        !stderr.to_lowercase().contains("slow event handler"),
        "handler slow warning should be disabled, got: {stderr}"
    );
    assert!(
        !stderr.to_lowercase().contains("slow event processing"),
        "event slow warning should be disabled, got: {stderr}"
    );
    assert!(
        stderr.contains("slow warning child handler finishing"),
        "child should still run to completion, got: {stderr}"
    );
}

#[test]
fn _inner_event_slow_handler_warning_child() {
    if !slow_warning_child_enabled() {
        return;
    }
    run_slow_warning_event(Some(0.0), Some(0.01));
}

#[test]
fn _inner_event_slow_event_warning_child() {
    if !slow_warning_child_enabled() {
        return;
    }
    run_slow_warning_event(Some(0.01), Some(0.0));
}

#[test]
fn _inner_event_slow_handler_and_event_warning_child() {
    if !slow_warning_child_enabled() {
        return;
    }
    run_slow_warning_event(Some(0.01), Some(0.01));
}

#[test]
fn _inner_event_no_slow_warning_child() {
    if !slow_warning_child_enabled() {
        return;
    }
    run_slow_warning_event(Some(0.0), Some(0.0));
}
