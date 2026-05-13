use std::{
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc, Mutex, MutexGuard, OnceLock,
    },
    time::{Duration, Instant},
};

use abxbus_rust::{
    base_event::BaseEvent,
    event_bus::{EventBus, EventBusOptions},
    event_handler::EventHandlerOptions,
    types::{EventHandlerCompletionMode, EventHandlerConcurrencyMode},
};
use futures::executor::block_on;
use futures_timer::Delay;
use serde_json::{json, Map, Value};

const PERFORMANCE_MAX_MS_PER_UNIT: f64 = 0.3;
static PERFORMANCE_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn performance_test_guard() -> MutexGuard<'static, ()> {
    PERFORMANCE_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn payload(entries: impl IntoIterator<Item = (&'static str, Value)>) -> Map<String, Value> {
    entries
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

fn wait_for_performance_batch(events: &[Arc<BaseEvent>]) {
    for event in events {
        let _ = block_on(event.now());
    }
}

fn assert_performance_budget(scenario: &str, total: usize, elapsed: Duration, unit: &str) {
    let ms_per_unit = elapsed.as_secs_f64() * 1000.0 / total as f64;
    let throughput = total as f64 / elapsed.as_secs_f64().max(1e-9);
    println!(
        "{scenario}: total={elapsed:?} latency={ms_per_unit:.3}ms/{unit} throughput={throughput:.0}/s"
    );
    assert!(
        ms_per_unit <= PERFORMANCE_MAX_MS_PER_UNIT,
        "{scenario} exceeded {PERFORMANCE_MAX_MS_PER_UNIT:.3}ms/{unit} budget: {ms_per_unit:.3}ms/{unit}"
    );
}

fn no_path_handler_options(id: impl Into<String>) -> EventHandlerOptions {
    EventHandlerOptions {
        id: Some(id.into()),
        detect_handler_file_path: Some(false),
        ..EventHandlerOptions::default()
    }
}

#[test]
fn test_performance_50k_events() {
    let _perf_guard = performance_test_guard();
    let total_events = 50_000usize;
    let batch_size = 512usize;
    let history_size = 512usize;
    let bus = EventBus::new_with_options(
        Some("Perf50kBus".to_string()),
        EventBusOptions {
            max_history_size: Some(history_size),
            max_history_drop: true,
            event_handler_detect_file_paths: false,
            ..EventBusOptions::default()
        },
    );

    let processed = Arc::new(AtomicI64::new(0));
    let checksum = Arc::new(AtomicI64::new(0));
    let processed_for_handler = processed.clone();
    let checksum_for_handler = checksum.clone();
    bus.on_raw_sync_with_options(
        "PerfSimpleEvent",
        "handler",
        no_path_handler_options("perf-simple-handler"),
        move |event| {
            processed_for_handler.fetch_add(1, Ordering::SeqCst);
            let inner = event.inner.lock();
            let value = inner
                .payload
                .get("value")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            let batch_id = inner
                .payload
                .get("batch_id")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            checksum_for_handler.fetch_add(value + batch_id, Ordering::SeqCst);
            Ok(Value::Null)
        },
    );

    let mut pending = Vec::with_capacity(batch_size);
    let mut expected_checksum = 0i64;
    let started = Instant::now();
    for index in 0..total_events {
        let batch_id = (index % 17) as i64;
        expected_checksum += index as i64 + batch_id;
        pending.push(bus.emit_base(BaseEvent::new(
            "PerfSimpleEvent",
            payload([
                ("value", json!(index as i64)),
                ("batch_id", json!(batch_id)),
            ]),
        )));
        if pending.len() >= batch_size {
            wait_for_performance_batch(&pending);
            pending.clear();
        }
    }
    if !pending.is_empty() {
        wait_for_performance_batch(&pending);
    }
    assert!(block_on(bus.wait_until_idle(Some(10.0))));
    let elapsed = started.elapsed();

    assert_eq!(processed.load(Ordering::SeqCst), total_events as i64);
    assert_eq!(checksum.load(Ordering::SeqCst), expected_checksum);
    assert!(bus.event_history_size() <= history_size);
    assert_performance_budget("50k events", total_events, elapsed, "event");
    bus.destroy();
}

#[test]
fn test_performance_ephemeral_buses() {
    let _perf_guard = performance_test_guard();
    let total_buses = 500usize;
    let events_per_bus = 100usize;
    let history_size = 128usize;
    let processed = Arc::new(AtomicI64::new(0));

    let started = Instant::now();
    for bus_index in 0..total_buses {
        let bus = EventBus::new_with_options(
            Some(format!("PerfEphemeralBus{bus_index}")),
            EventBusOptions {
                max_history_size: Some(history_size),
                max_history_drop: true,
                event_handler_detect_file_paths: false,
                ..EventBusOptions::default()
            },
        );
        let processed_for_handler = processed.clone();
        bus.on_raw_sync_with_options(
            "PerfEphemeralEvent",
            "handler",
            no_path_handler_options(format!("perf-ephemeral-handler-{bus_index}")),
            move |_event| {
                processed_for_handler.fetch_add(1, Ordering::SeqCst);
                Ok(Value::Null)
            },
        );

        let pending: Vec<_> = (0..events_per_bus)
            .map(|_| bus.emit_base(BaseEvent::new("PerfEphemeralEvent", Map::new())))
            .collect();
        wait_for_performance_batch(&pending);
        assert!(block_on(bus.wait_until_idle(Some(2.0))));
        bus.destroy();
    }
    let elapsed = started.elapsed();
    let total_events = total_buses * events_per_bus;
    assert_eq!(processed.load(Ordering::SeqCst), total_events as i64);
    assert_performance_budget("500 buses x 100 events", total_events, elapsed, "event");
}

#[test]
fn test_performance_single_event_many_parallel_handlers() {
    let _perf_guard = performance_test_guard();
    let total_handlers = 50_000usize;
    let bus = EventBus::new_with_options(
        Some("PerfFixedHandlersBus".to_string()),
        EventBusOptions {
            max_history_size: Some(128),
            max_history_drop: true,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            event_handler_completion: EventHandlerCompletionMode::All,
            event_handler_detect_file_paths: false,
            ..EventBusOptions::default()
        },
    );

    let handled = Arc::new(AtomicI64::new(0));
    for index in 0..total_handlers {
        let handled_for_handler = handled.clone();
        let handler_id = format!("perf-fixed-handler-{index:05}");
        bus.on_raw_sync_with_options(
            "PerfFixedHandlersEvent",
            &handler_id,
            no_path_handler_options(handler_id.clone()),
            move |_event| {
                handled_for_handler.fetch_add(1, Ordering::SeqCst);
                Ok(Value::Null)
            },
        );
    }

    let started = Instant::now();
    let event = bus.emit_base(BaseEvent::new("PerfFixedHandlersEvent", Map::new()));
    wait_for_performance_batch(&[event]);
    assert!(block_on(bus.wait_until_idle(Some(10.0))));
    let elapsed = started.elapsed();

    assert_eq!(handled.load(Ordering::SeqCst), total_handlers as i64);
    assert_performance_budget(
        "1 event x 50k parallel handlers",
        total_handlers,
        elapsed,
        "handler",
    );
    bus.destroy();
}

#[test]
fn test_performance_on_off_churn() {
    let _perf_guard = performance_test_guard();
    let total_events = 50_000usize;
    let bus = EventBus::new_with_options(
        Some("PerfOnOffChurnBus".to_string()),
        EventBusOptions {
            max_history_size: Some(128),
            max_history_drop: true,
            event_handler_detect_file_paths: false,
            ..EventBusOptions::default()
        },
    );

    let handled = Arc::new(AtomicI64::new(0));
    let started = Instant::now();
    for index in 0..total_events {
        let handled_for_handler = handled.clone();
        let handler_id = format!("perf-one-off-handler-{index:05}");
        let handler = bus.on_raw_sync_with_options(
            "PerfOneOffEvent",
            &handler_id,
            no_path_handler_options(handler_id.clone()),
            move |_event| {
                handled_for_handler.fetch_add(1, Ordering::SeqCst);
                Ok(Value::Null)
            },
        );
        let event = bus.emit_base(BaseEvent::new("PerfOneOffEvent", Map::new()));
        wait_for_performance_batch(&[event]);
        bus.off("PerfOneOffEvent", Some(&handler.id));
    }
    assert!(block_on(bus.wait_until_idle(Some(10.0))));
    let elapsed = started.elapsed();

    assert_eq!(handled.load(Ordering::SeqCst), total_events as i64);
    assert_performance_budget(
        "50k one-off handlers over 50k events",
        total_events,
        elapsed,
        "event",
    );
    bus.destroy();
}

#[test]
fn test_performance_worst_case_forwarding_queue_jump_timeouts() {
    let _perf_guard = performance_test_guard();
    let total_events = 2_000usize;
    let child_bus = EventBus::new_with_options(
        Some("PerfWorstChildBus".to_string()),
        EventBusOptions {
            max_history_size: Some(128),
            max_history_drop: true,
            event_timeout: Some(0.0001),
            event_handler_detect_file_paths: false,
            ..EventBusOptions::default()
        },
    );
    let parent_bus = EventBus::new_with_options(
        Some("PerfWorstParentBus".to_string()),
        EventBusOptions {
            max_history_size: Some(128),
            max_history_drop: true,
            event_handler_detect_file_paths: false,
            ..EventBusOptions::default()
        },
    );

    let parents = Arc::new(AtomicI64::new(0));
    let children = Arc::new(AtomicI64::new(0));
    let timed_out = Arc::new(AtomicI64::new(0));

    let parents_for_handler = parents.clone();
    let child_bus_for_handler = child_bus.clone();
    parent_bus.on_raw("WCParent", "forward", move |event| {
        let child_bus = child_bus_for_handler.clone();
        let parents = parents_for_handler.clone();
        async move {
            parents.fetch_add(1, Ordering::SeqCst);
            let iteration = event
                .inner
                .lock()
                .payload
                .get("iteration")
                .cloned()
                .unwrap_or_else(|| json!(0));
            let child = child_bus.emit_base(BaseEvent::new(
                "WCChild",
                payload([
                    ("parent", json!(event.inner.lock().event_id.clone())),
                    ("iteration", iteration),
                ]),
            ));
            let _ = child.now().await;
            Ok(Value::Null)
        }
    });

    let children_for_handler = children.clone();
    let timed_out_for_handler = timed_out.clone();
    child_bus.on_raw_with_options(
        "WCChild",
        "child",
        no_path_handler_options("perf-worst-child-handler"),
        move |event| {
            children_for_handler.fetch_add(1, Ordering::SeqCst);
            let iteration = event
                .inner
                .lock()
                .payload
                .get("iteration")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            let timed_out = timed_out_for_handler.clone();
            async move {
                if iteration % 10 == 0 {
                    timed_out.fetch_add(1, Ordering::SeqCst);
                    Delay::new(Duration::from_millis(2)).await;
                }
                Ok(json!("ok"))
            }
        },
    );

    let mut pending = Vec::with_capacity(128);
    let started = Instant::now();
    for index in 0..total_events {
        pending.push(parent_bus.emit_base(BaseEvent::new(
            "WCParent",
            payload([("iteration", json!(index as i64))]),
        )));
        if pending.len() >= pending.capacity() {
            wait_for_performance_batch(&pending);
            pending.clear();
        }
    }
    if !pending.is_empty() {
        wait_for_performance_batch(&pending);
    }
    assert!(block_on(parent_bus.wait_until_idle(Some(10.0))));
    assert!(block_on(child_bus.wait_until_idle(Some(10.0))));
    let elapsed = started.elapsed();

    assert_eq!(parents.load(Ordering::SeqCst), total_events as i64);
    assert!(children.load(Ordering::SeqCst) > 0);
    assert!(timed_out.load(Ordering::SeqCst) > 0);
    assert_performance_budget(
        "worst-case forwarding + timeouts",
        total_events,
        elapsed,
        "event",
    );
    parent_bus.destroy();
    child_bus.destroy();
}
