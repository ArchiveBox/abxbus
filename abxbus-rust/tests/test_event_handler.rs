use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use abxbus_rust::{
    event_bus::{EventBus, EventBusOptions},
    event_result::EventResultStatus,
    typed::{EventSpec, TypedEvent},
    types::{EventHandlerCompletionMode, EventHandlerConcurrencyMode},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Serialize, Deserialize)]
struct EmptyPayload {}

struct CompletionEvent;
impl EventSpec for CompletionEvent {
    type Payload = EmptyPayload;
    type Result = Value;
    const EVENT_TYPE: &'static str = "CompletionEvent";
}

struct ConcurrencyEvent;
impl EventSpec for ConcurrencyEvent {
    type Payload = EmptyPayload;
    type Result = Value;
    const EVENT_TYPE: &'static str = "ConcurrencyEvent";
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

    bus.on("CompletionEvent", "first_handler", |_event| async move {
        Ok(json!("first"))
    });
    let second_handler_called_for_handler = second_handler_called.clone();
    bus.on("CompletionEvent", "second_handler", move |_event| {
        let second_handler_called = second_handler_called_for_handler.clone();
        async move {
            *second_handler_called.lock().expect("called lock") = true;
            Ok(json!("second"))
        }
    });

    let event = bus.emit::<CompletionEvent>(TypedEvent::new(EmptyPayload {}));
    assert_eq!(event.inner.inner.lock().event_handler_completion, None);
    block_on(event.wait_completed());

    assert!(!*second_handler_called.lock().expect("called lock"));
    assert_eq!(event.first_result(), Some(json!("first")));
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

    bus.on("CompletionEvent", "first_handler", |_event| async move {
        Ok(json!("first"))
    });
    let second_handler_called_for_handler = second_handler_called.clone();
    bus.on("CompletionEvent", "second_handler", move |_event| {
        let second_handler_called = second_handler_called_for_handler.clone();
        async move {
            *second_handler_called.lock().expect("called lock") = true;
            Ok(json!("second"))
        }
    });

    let event = TypedEvent::<CompletionEvent>::new(EmptyPayload {});
    event.inner.inner.lock().event_handler_completion = Some(EventHandlerCompletionMode::All);
    let event = bus.emit(event);
    assert_eq!(
        event.inner.inner.lock().event_handler_completion,
        Some(EventHandlerCompletionMode::All)
    );
    block_on(event.wait_completed());

    assert!(*second_handler_called.lock().expect("called lock"));
    let results = event.inner.inner.lock().event_results.clone();
    assert!(results
        .values()
        .all(|result| result.status == EventResultStatus::Completed));
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
    bus.on("ConcurrencyEvent", "handler", |_event| async move {
        Ok(json!("ok"))
    });

    let event = bus.emit::<ConcurrencyEvent>(TypedEvent::new(EmptyPayload {}));
    assert_eq!(event.inner.inner.lock().event_handler_concurrency, None);
    block_on(event.wait_completed());
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
        bus.on("ConcurrencyEvent", name, move |_event| {
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

    let serial_event = TypedEvent::<ConcurrencyEvent>::new(EmptyPayload {});
    serial_event.inner.inner.lock().event_handler_concurrency =
        Some(EventHandlerConcurrencyMode::Serial);
    let serial_event = bus.emit(serial_event);
    block_on(serial_event.wait_completed());
    assert_eq!(*max_in_flight.lock().expect("max lock"), 1);

    *in_flight.lock().expect("in flight lock") = 0;
    *max_in_flight.lock().expect("max lock") = 0;
    let parallel_event = TypedEvent::<ConcurrencyEvent>::new(EmptyPayload {});
    parallel_event.inner.inner.lock().event_handler_concurrency =
        Some(EventHandlerConcurrencyMode::Parallel);
    let parallel_event = bus.emit(parallel_event);
    block_on(parallel_event.wait_completed());
    assert!(*max_in_flight.lock().expect("max lock") >= 2);
    bus.stop();
}
