use abxbus::{
    base_event::BaseEvent,
    event_bus::{EventBus, EventBusOptions},
};
use futures::{channel::oneshot, executor::block_on, FutureExt};
use serde_json::json;

#[test]
fn test_capacity_does_not_double_count_queued_events() {
    for limit in [50, 100, 150] {
        let bus = EventBus::new_with_options(
            Some("CapacityAccounting".to_string()),
            EventBusOptions {
                max_history_size: Some(limit),
                ..EventBusOptions::default()
            },
        );
        let (release, gate) = oneshot::channel::<()>();
        let gate = gate.shared();
        bus.on_raw("CapacityEvent", "blocked_handler", move |_event| {
            let gate = gate.clone();
            async move {
                let _ = gate.await;
                Ok(json!(null))
            }
        });
        let events: Vec<_> = (0..limit)
            .map(|_| bus.emit_base(BaseEvent::new("CapacityEvent", serde_json::Map::new())))
            .collect();
        assert_eq!(bus.event_history_size(), limit);
        let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            bus.emit_base(BaseEvent::new("CapacityEvent", serde_json::Map::new()));
        }))
        .expect_err("full history must reject the next event");
        let message = rejected
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| rejected.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(
            message.contains(&format!("history limit reached: max_history_size={limit}")),
            "{message}"
        );
        assert_eq!(bus.event_history_size(), limit);
        release.send(()).unwrap();
        assert!(block_on(bus.wait_until_idle(Some(5.0))));
        for event in events {
            assert_eq!(
                event.inner.lock().event_status,
                abxbus::EventStatus::Completed
            );
        }
        bus.destroy();
    }
}
