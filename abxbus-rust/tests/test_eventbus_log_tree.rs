use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    thread,
    time::Duration,
};

use abxbus_rust::{
    event_bus::{EventBus, EventBusOptions},
    typed::{BaseEventHandle, EventSpec},
    types::{EventHandlerCompletionMode, EventHandlerConcurrencyMode},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Serialize, Deserialize)]
struct RootPayload {
    data: Option<String>,
}
#[derive(Clone, Serialize, Deserialize)]
struct ChildPayload {
    value: Option<i64>,
}
#[derive(Clone, Serialize, Deserialize)]
struct GrandchildPayload {
    nested: Option<std::collections::HashMap<String, i64>>,
}
#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}

struct RootEvent;
impl EventSpec for RootEvent {
    type payload = RootPayload;
    type event_result_type = EmptyResult;
    const event_type: &'static str = "RootEvent";
}
struct ChildEvent;
impl EventSpec for ChildEvent {
    type payload = ChildPayload;
    type event_result_type = EmptyResult;
    const event_type: &'static str = "ChildEvent";
}
struct GrandchildEvent;
impl EventSpec for GrandchildEvent {
    type payload = GrandchildPayload;
    type event_result_type = EmptyResult;
    const event_type: &'static str = "GrandchildEvent";
}
struct CancelledLogEvent;
impl EventSpec for CancelledLogEvent {
    type payload = RootPayload;
    type event_result_type = String;
    const event_type: &'static str = "CancelledLogEvent";
}

#[test]
fn test_log_tree_single_event() {
    let bus = EventBus::new(Some("SingleBus".to_string()));
    let event = bus.emit(BaseEventHandle::<RootEvent>::new(RootPayload {
        data: Some("test".to_string()),
    }));
    block_on(event.done());

    let output = bus.log_tree();
    assert!(output.contains("└── ✅ RootEvent#"));
    assert!(output.contains('[') && output.contains(']'));
    bus.stop();
}

#[test]
fn test_log_history_tree_single_event() {
    test_log_tree_single_event();
}

#[test]
fn test_logtree_single_event() {
    test_log_tree_single_event();
}

#[test]
fn test_log_tree_with_handler_results() {
    let bus = EventBus::new(Some("HandlerBus".to_string()));
    bus.on_raw("RootEvent", "test_handler", |_event| async move {
        Ok(json!("status: success"))
    });

    let event = bus.emit(BaseEventHandle::<RootEvent>::new(RootPayload {
        data: Some("test".to_string()),
    }));
    block_on(event.done());

    let output = bus.log_tree();
    assert!(output.contains("└── ✅ RootEvent#"));
    assert!(output.contains(&format!("{}.test_handler#", bus.label())));
    assert!(output.contains("\"status: success\""));
    bus.stop();
}

#[test]
fn test_log_history_tree_with_handlers() {
    test_log_tree_with_handler_results();
}

#[test]
fn test_logtree_with_handler_results() {
    test_log_tree_with_handler_results();
}

#[test]
fn test_log_tree_with_handler_errors() {
    let bus = EventBus::new(Some("ErrorBus".to_string()));
    bus.on_raw("RootEvent", "error_handler", |_event| async move {
        Err("ValueError: Test error message".to_string())
    });

    let event = bus.emit(BaseEventHandle::<RootEvent>::new(RootPayload {
        data: Some("test".to_string()),
    }));
    block_on(event.done());

    let output = bus.log_tree();
    assert!(output.contains(&format!("{}.error_handler#", bus.label())));
    assert!(output.contains("ValueError: Test error message"));
    bus.stop();
}

#[test]
fn test_log_history_tree_with_errors() {
    test_log_tree_with_handler_errors();
}

#[test]
fn test_logtree_with_handler_errors() {
    test_log_tree_with_handler_errors();
}

#[test]
fn test_log_tree_first_mode_control_cancellations_use_cancelled_icon() {
    let bus = EventBus::new_with_options(
        Some("CancelledLogBus".to_string()),
        EventBusOptions {
            event_timeout: None,
            event_handler_concurrency: EventHandlerConcurrencyMode::Parallel,
            ..EventBusOptions::default()
        },
    );
    bus.on_raw("CancelledLogEvent", "fast_handler", |_event| async move {
        thread::sleep(Duration::from_millis(5));
        Ok(json!("fast result"))
    });
    bus.on_raw("CancelledLogEvent", "slow_handler", |_event| async move {
        thread::sleep(Duration::from_millis(100));
        Ok(json!("slow result"))
    });

    let event = BaseEventHandle::<CancelledLogEvent>::new(RootPayload { data: None });
    event.inner.inner.lock().event_handler_completion = Some(EventHandlerCompletionMode::First);
    let event = bus.emit(event);
    let first = block_on(event.first()).expect("first result");
    assert_eq!(first, Some("fast result".to_string()));

    let output = bus.log_tree();
    assert!(output.contains(&format!("🚫 {}.slow_handler#", bus.label())));
    assert!(!output.contains(&format!("❌ {}.slow_handler#", bus.label())));
    assert!(output.contains("Aborted: Aborted: first() resolved"));
    bus.stop();
}

#[test]
fn test_logtree_first_mode_control_cancellations_use_cancelled_icon() {
    test_log_tree_first_mode_control_cancellations_use_cancelled_icon();
}

#[test]
fn test_log_tree_complex_nested() {
    let bus = EventBus::new(Some("ComplexBus".to_string()));
    let bus_for_root = bus.clone();
    let bus_for_child = bus.clone();

    bus.on_raw("RootEvent", "root_handler", move |_event| {
        let bus = bus_for_root.clone();
        async move {
            let child = bus.emit_child(BaseEventHandle::<ChildEvent>::new(ChildPayload {
                value: Some(100),
            }));
            child.done().await;
            Ok(json!("Root processed"))
        }
    });
    bus.on_raw("ChildEvent", "child_handler", move |_event| {
        let bus = bus_for_child.clone();
        async move {
            let grandchild =
                bus.emit_child(BaseEventHandle::<GrandchildEvent>::new(GrandchildPayload {
                    nested: None,
                }));
            grandchild.done().await;
            Ok(json!([1, 2, 3]))
        }
    });
    bus.on_raw(
        "GrandchildEvent",
        "grandchild_handler",
        |_event| async move { Ok(json!(null)) },
    );

    let root = bus.emit(BaseEventHandle::<RootEvent>::new(RootPayload {
        data: Some("root_data".to_string()),
    }));
    block_on(root.done());

    let output = bus.log_tree();
    assert!(output.contains("✅ RootEvent#"));
    assert!(output.contains(&format!("✅ {}.root_handler#", bus.label())));
    assert!(output.contains("✅ ChildEvent#"));
    assert!(output.contains(&format!("✅ {}.child_handler#", bus.label())));
    assert!(output.contains("✅ GrandchildEvent#"));
    assert!(output.contains(&format!("✅ {}.grandchild_handler#", bus.label())));
    assert!(output.contains("\"Root processed\""));
    assert!(output.contains("list(3 items)"));
    assert!(output.contains("None"));
    bus.stop();
}

#[test]
fn test_log_history_tree_complex_nested() {
    test_log_tree_complex_nested();
}

#[test]
fn test_logtree_complex_nested() {
    test_log_tree_complex_nested();
}

#[test]
fn test_log_tree_multiple_roots() {
    let bus = EventBus::new(Some("MultiBus".to_string()));

    let root_1 = bus.emit(BaseEventHandle::<RootEvent>::new(RootPayload {
        data: Some("first".to_string()),
    }));
    let root_2 = bus.emit(BaseEventHandle::<RootEvent>::new(RootPayload {
        data: Some("second".to_string()),
    }));
    block_on(root_1.done());
    block_on(root_2.done());

    let output = bus.log_tree();
    assert_eq!(output.matches("├── ✅ RootEvent#").count(), 1);
    assert_eq!(output.matches("└── ✅ RootEvent#").count(), 1);
    bus.stop();
}

#[test]
fn test_log_history_tree_multiple_roots() {
    test_log_tree_multiple_roots();
}

#[test]
fn test_logtree_multiple_roots() {
    test_log_tree_multiple_roots();
}

#[test]
fn test_log_tree_timing_info() {
    let bus = EventBus::new(Some("TimingBus".to_string()));
    bus.on_raw("RootEvent", "timed_handler", |_event| async move {
        thread::sleep(Duration::from_millis(5));
        Ok(json!("done"))
    });

    let event = bus.emit(BaseEventHandle::<RootEvent>::new(RootPayload {
        data: None,
    }));
    block_on(event.done());

    let output = bus.log_tree();
    assert!(output.contains('('));
    assert!(output.contains("s)"));
    bus.stop();
}

#[test]
fn test_log_history_tree_timing_info() {
    test_log_tree_timing_info();
}

#[test]
fn test_logtree_timing_info() {
    test_log_tree_timing_info();
}

#[test]
fn test_log_tree_running_handler() {
    let bus = EventBus::new(Some("RunningBus".to_string()));
    let (started_tx, started_rx) = mpsc::channel();
    let release_handler = Arc::new(AtomicBool::new(false));
    let release_handler_for_handler = release_handler.clone();

    bus.on_raw("RootEvent", "running_handler", move |_event| {
        let started_tx = started_tx.clone();
        let release_handler = release_handler_for_handler.clone();
        async move {
            let _ = started_tx.send(());
            while !release_handler.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(5));
            }
            Ok(json!("done"))
        }
    });

    let event = bus.emit(BaseEventHandle::<RootEvent>::new(RootPayload {
        data: None,
    }));
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("handler should start");

    let output = bus.log_tree();
    assert!(output.contains(&format!("{}.running_handler#", bus.label())));
    assert!(output.contains("🏃 RootEvent#"));
    release_handler.store(true, Ordering::SeqCst);
    block_on(event.done());
    bus.stop();
}

#[test]
fn test_log_history_tree_running_handler() {
    test_log_tree_running_handler();
}

#[test]
fn test_logtree_running_handler() {
    test_log_tree_running_handler();
}
