use abxbus_rust::event;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    thread,
    time::Duration,
};

use abxbus_rust::{
    base_event::{EventResultOptions, EventWaitOptions},
    event_bus::{EventBus, EventBusOptions},
    types::{EventHandlerCompletionMode, EventHandlerConcurrencyMode},
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Serialize, Deserialize)]
struct EmptyResult {}

event! {
    struct RootEvent {
        data: Option<String>,
        event_result_type: EmptyResult,
        event_type: "RootEvent",
    }
}
event! {
    struct ChildEvent {
        value: Option<i64>,
        event_result_type: EmptyResult,
        event_type: "ChildEvent",
    }
}
event! {
    struct GrandchildEvent {
        nested: Option<std::collections::HashMap<String, i64>>,
        event_result_type: EmptyResult,
        event_type: "GrandchildEvent",
    }
}
event! {
    struct CancelledLogEvent {
        data: Option<String>,
        event_result_type: String,
        event_type: "CancelledLogEvent",
    }
}
#[test]
fn test_log_tree_single_event() {
    let bus = EventBus::new(Some("SingleBus".to_string()));
    let event = bus.emit(RootEvent {
        data: Some("test".to_string()),
        ..Default::default()
    });
    let _ = block_on(event.now_with_options(EventWaitOptions {
        timeout: None,
        first_result: false,
    }));

    let output = bus.log_tree();
    assert!(output.contains("└── ✅ RootEvent#"));
    assert!(output.contains('[') && output.contains(']'));
    bus.destroy();
}

#[test]
fn test_log_tree_with_handler_results() {
    let bus = EventBus::new(Some("HandlerBus".to_string()));
    bus.on_raw("RootEvent", "test_handler", |_event| async move {
        Ok(json!("status: success"))
    });

    let event = bus.emit(RootEvent {
        data: Some("test".to_string()),
        ..Default::default()
    });
    let _ = block_on(event.now());

    let output = bus.log_tree();
    assert!(output.contains("└── ✅ RootEvent#"));
    assert!(output.contains(&format!("{}.test_handler#", bus.label())));
    assert!(output.contains("\"status: success\""));
    bus.destroy();
}

#[test]
fn test_log_tree_with_handler_errors() {
    let bus = EventBus::new(Some("ErrorBus".to_string()));
    bus.on_raw("RootEvent", "error_handler", |_event| async move {
        Err("ValueError: Test error message".to_string())
    });

    let event = bus.emit(RootEvent {
        data: Some("test".to_string()),
        ..Default::default()
    });
    let _ = block_on(event.now());

    let output = bus.log_tree();
    assert!(output.contains(&format!("{}.error_handler#", bus.label())));
    assert!(output.contains("ValueError: Test error message"));
    bus.destroy();
}

#[test]
fn test_log_tree_first_mode_control_cancellations_use_cancelled_icon() {
    let bus = EventBus::new_with_options(
        Some("CancelledLogBus".to_string()),
        EventBusOptions {
            event_timeout: Some(0.0),
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

    let event = CancelledLogEvent {
        data: None,
        event_handler_completion: Some(EventHandlerCompletionMode::First),
        ..Default::default()
    };
    let event = bus.emit(event);
    let _ = block_on(event.now());
    block_on(bus.wait_until_idle(Some(2.0)));
    let first = block_on(event.event_result_with_options(EventResultOptions {
        raise_if_any: false,
        raise_if_none: false,
        include: None,
    }))
    .expect("first result");
    assert_eq!(first, Some("fast result".to_string()));

    let output = bus.log_tree();
    assert!(output.contains(&format!("🚫 {}.slow_handler#", bus.label())));
    assert!(!output.contains(&format!("❌ {}.slow_handler#", bus.label())));
    assert!(output.contains("Aborted: Aborted: first result resolved"));
    bus.destroy();
}

#[test]
fn test_log_tree_complex_nested() {
    let bus = EventBus::new(Some("ComplexBus".to_string()));
    let bus_for_root = bus.clone();
    let bus_for_child = bus.clone();

    bus.on_raw("RootEvent", "root_handler", move |_event| {
        let bus = bus_for_root.clone();
        async move {
            let child = bus.emit_child(ChildEvent {
                value: Some(100),
                ..Default::default()
            });
            let _ = child.now().await;
            Ok(json!("Root processed"))
        }
    });
    bus.on_raw("ChildEvent", "child_handler", move |_event| {
        let bus = bus_for_child.clone();
        async move {
            let grandchild = bus.emit_child(GrandchildEvent {
                nested: None,
                ..Default::default()
            });
            let _ = grandchild.now().await;
            Ok(json!([1, 2, 3]))
        }
    });
    bus.on_raw(
        "GrandchildEvent",
        "grandchild_handler",
        |_event| async move { Ok(json!(null)) },
    );

    let root = bus.emit(RootEvent {
        data: Some("root_data".to_string()),
        ..Default::default()
    });
    let _ = block_on(root.now());

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
    bus.destroy();
}

#[test]
fn test_log_tree_multiple_roots() {
    let bus = EventBus::new(Some("MultiBus".to_string()));

    let root_1 = bus.emit(RootEvent {
        data: Some("first".to_string()),
        ..Default::default()
    });
    let root_2 = bus.emit(RootEvent {
        data: Some("second".to_string()),
        ..Default::default()
    });
    let _ = block_on(root_1.now());
    let _ = block_on(root_2.now());

    let output = bus.log_tree();
    assert_eq!(output.matches("├── ✅ RootEvent#").count(), 1);
    assert_eq!(output.matches("└── ✅ RootEvent#").count(), 1);
    bus.destroy();
}

#[test]
fn test_log_tree_timing_info() {
    let bus = EventBus::new(Some("TimingBus".to_string()));
    bus.on_raw("RootEvent", "timed_handler", |_event| async move {
        thread::sleep(Duration::from_millis(5));
        Ok(json!("done"))
    });

    let event = bus.emit(RootEvent {
        data: None,
        ..Default::default()
    });
    let _ = block_on(event.now());

    let output = bus.log_tree();
    assert!(output.contains('('));
    assert!(output.contains("s)"));
    bus.destroy();
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

    let event = bus.emit(RootEvent {
        data: None,
        ..Default::default()
    });
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("handler should start");

    let output = bus.log_tree();
    assert!(output.contains(&format!("{}.running_handler#", bus.label())));
    assert!(output.contains("🏃 RootEvent#"));
    release_handler.store(true, Ordering::SeqCst);
    let _ = block_on(event.now());
    bus.destroy();
}
