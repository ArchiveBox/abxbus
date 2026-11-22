use std::{
    fs::{self, OpenOptions},
    io::Write,
    sync::mpsc,
    time::Duration,
};

use abxbus::{base_event::BaseEvent, id::uuid_v7_string, jsonl_bridge::JsonlEventBridge};
use serde_json::{json, Value};

#[test]
fn test_jsonl_bridge_starts_at_eof_and_skips_invalid_events() {
    let root = std::env::temp_dir().join(format!("abxbus-rust-jsonl-{}", uuid_v7_string()));
    let path = root.join("events.jsonl");
    let sender = JsonlEventBridge::new(&path, None, None);
    let event = |label| {
        BaseEvent::new(
            "Ping",
            json!({"label": label, "event_parent_id": uuid_v7_string()})
                .as_object()
                .unwrap()
                .clone(),
        )
    };
    sender.emit_base(event("already present")).unwrap();

    let receiver = JsonlEventBridge::new(&path, Some(Duration::from_millis(5)), None);
    let (tx, rx) = mpsc::channel();
    receiver
        .on_raw_sync("Ping", "record", move |event| {
            tx.send(event.to_json_value())
                .map_err(|error| error.to_string())?;
            Ok(Value::Null)
        })
        .unwrap();

    let first = event("first");
    let good = event("leaf 🌱 café");
    let mut bad_id = good.to_json_value();
    bad_id["event_id"] = json!([]);
    let mut bad_ttl = good.to_json_value();
    bad_ttl["event_ttl"] = json!(-2);
    let mut bad_timeout = good.to_json_value();
    bad_timeout["event_timeout"] = json!(-2);
    let good_record = format!("{}\n", good.to_json_value());
    let split_at = good_record.find('🌱').unwrap() + 1;
    let (prefix, suffix) = good_record.as_bytes().split_at(split_at);
    let mut chunk = format!(
        "{}\n{bad_id}\n{bad_ttl}\n{bad_timeout}\n",
        first.to_json_value()
    )
    .into_bytes();
    chunk.extend_from_slice(prefix);
    let mut writer = OpenOptions::new().append(true).open(&path).unwrap();
    writer.write_all(&chunk).unwrap();

    let first_received = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(first_received["label"], "first");
    assert!(first_received["event_parent_id"].is_null());
    // The first real handler confirms that the partially written record was read.
    writer.write_all(suffix).unwrap();

    let sentinel = event("sentinel");
    sender.dispatch_base(sentinel.clone()).unwrap();
    for (original, expected) in [(&good, "leaf 🌱 café"), (&sentinel, "sentinel")] {
        let received = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(received["label"], expected);
        assert_eq!(received["event_type"], "Ping");
        assert_ne!(received["event_id"], original.to_json_value()["event_id"]);
        assert!(received["event_parent_id"].is_null());
    }
    receiver.close();
    receiver.close();
    assert!(receiver.start().is_err());
    assert!(rx.try_recv().is_err());
    sender.close();
    fs::remove_dir_all(root).unwrap();
}

abxbus::event! {
    struct BridgePing {
        label: String,
        event_result_type: Value,
    }
}

#[test]
fn test_jsonl_bridge_supports_typed_emit_and_handlers() {
    let root = std::env::temp_dir().join(format!("abxbus-rust-jsonl-{}", uuid_v7_string()));
    let path = root.join("events.jsonl");
    let sender = JsonlEventBridge::new(&path, None, None);
    let receiver = JsonlEventBridge::new(&path, Some(Duration::from_millis(5)), None);
    let (tx, rx) = mpsc::channel();
    receiver
        .on(BridgePing, move |event: BridgePing| {
            let tx = tx.clone();
            async move {
                tx.send(event.label).map_err(|error| error.to_string())?;
                Ok(Value::Null)
            }
        })
        .unwrap();
    let event = sender
        .emit(BridgePing {
            label: "typed café 🌱".to_string(),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(5)).unwrap(),
        event.label
    );
    receiver.close();
    sender.close();
    fs::remove_dir_all(root).unwrap();
}
