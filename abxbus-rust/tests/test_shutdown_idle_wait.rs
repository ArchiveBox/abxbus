use abxbus::{
    base_event::BaseEvent,
    event_bus::{DestroyOptions, EventBus},
};
use futures::{channel::oneshot, executor::block_on, pin_mut, poll};
use serde_json::json;
use std::{
    sync::{mpsc, Arc, Mutex},
    task::Poll,
    time::Duration,
};

#[test]
fn destroy_releases_pending_idle_wait_and_stays_terminal() {
    for clear in [false, true] {
        let bus = EventBus::new(Some(format!("ShutdownIdleWait_{clear}")));
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let release = Arc::new(Mutex::new(Some(release_rx)));
        bus.on_raw("Slow", "blocked", move |_event| {
            let release_rx = release.lock().unwrap().take().unwrap();
            started_tx.send(()).unwrap();
            async move {
                let _ = release_rx.await;
                Ok(json!(null))
            }
        });
        bus.emit_base(BaseEvent::new("Slow", serde_json::Map::new()));
        bus.emit_base(BaseEvent::new("Queued", serde_json::Map::new()));
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("handler should start");
        block_on(async {
            let waiting = bus.wait_until_idle(None);
            pin_mut!(waiting);
            assert!(matches!(poll!(waiting.as_mut()), Poll::Pending));
            bus.destroy_with_options(DestroyOptions { clear });
            assert!(waiting.await, "destroy should release existing idle wait");
        });
        assert!(bus.is_destroyed_for_test());
        assert!(!bus.is_running_for_test());
        assert!(bus.is_idle_and_queue_empty());
        assert_destroyed(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || {
                bus.emit_base(BaseEvent::new("Late", serde_json::Map::new()));
            },
        )));
        assert_destroyed(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || {
                block_on(bus.wait_until_idle(None));
            },
        )));
        let _ = release_tx.send(());
    }
}

fn assert_destroyed(result: std::thread::Result<()>) {
    let payload = result.expect_err("destroyed bus must reject new operations");
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .expect("expected a destroyed-bus error message");
    assert!(
        message.contains("has been destroyed"),
        "unexpected panic: {message}"
    );
}
