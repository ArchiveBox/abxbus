use std::env;
use std::fs;

use bubus_rust::base_event::BaseEvent;

fn main() {
    let input_path = env::var("BUBUS_RUST_INPUT_PATH")
        .expect("missing BUBUS_RUST_INPUT_PATH");
    let output_path = env::var("BUBUS_RUST_OUTPUT_PATH")
        .expect("missing BUBUS_RUST_OUTPUT_PATH");

    let raw = fs::read_to_string(&input_path)
        .unwrap_or_else(|e| panic!("failed to read {input_path}: {e}"));
    let items: Vec<serde_json::Value> = serde_json::from_str(&raw)
        .expect("expected JSON array");

    let roundtripped: Vec<serde_json::Value> = items
        .into_iter()
        .map(|item| {
            let event = BaseEvent::from_json_value(item);
            event.to_json_value()
        })
        .collect();

    let output = serde_json::to_string_pretty(&roundtripped)
        .expect("failed to serialize output");
    fs::write(&output_path, output)
        .unwrap_or_else(|e| panic!("failed to write {output_path}: {e}"));
}
