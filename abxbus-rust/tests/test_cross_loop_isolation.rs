use abxbus::{base_event::BaseEvent, event_bus::EventBus};
use futures::{channel::oneshot, executor::block_on};
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc, Arc, Mutex,
};
use std::time::Duration;

// Rust's Send handlers are executor-independent. The shared contract is that an
// active await never drains an unrelated bus's queued events.
#[test]
fn test_now_does_not_drain_unrelated_bus() {
    let bus = EventBus::new(Some("AwaitingBus".into()));
    let other = EventBus::new(Some("UnrelatedBus".into()));
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let release = Arc::new(Mutex::new(Some(release_rx)));
    let seen = Arc::new(AtomicUsize::new(0));
    let own_seen = Arc::new(AtomicUsize::new(0));
    let own_count = own_seen.clone();
    bus.on_raw("Own", "own", move |_| {
        own_count.fetch_add(1, Ordering::SeqCst);
        async { Ok(json!(null)) }
    });
    other.on_raw("Blocker", "blocker", move |_| {
        let release_rx = release.lock().unwrap().take().unwrap();
        started_tx.send(()).unwrap();
        async move {
            let _ = release_rx.await;
            Ok(json!(null))
        }
    });
    let count = seen.clone();
    other.on_raw("Probe", "probe", move |_| {
        count.fetch_add(1, Ordering::SeqCst);
        async { Ok(json!(null)) }
    });
    other.emit_base(BaseEvent::new("Blocker", serde_json::Map::new()));
    let queued = other.emit_base(BaseEvent::new("Probe", serde_json::Map::new()));
    started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("blocker never started");
    block_on(
        bus.emit_base(BaseEvent::new("Own", serde_json::Map::new()))
            .now(),
    )
    .unwrap();
    let before_release = seen.load(Ordering::SeqCst);
    let _ = release_tx.send(());
    block_on(queued.wait()).unwrap();
    bus.destroy();
    other.destroy();
    assert_eq!(before_release, 0, "now drained unrelated queue");
    assert_eq!(seen.load(Ordering::SeqCst), 1);
    assert_eq!(own_seen.load(Ordering::SeqCst), 1);
}
