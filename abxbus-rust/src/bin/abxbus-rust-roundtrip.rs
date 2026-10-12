use std::{env, fs, process};

use abxbus::{base_event::BaseEvent, event_bus::EventBus};
use serde_json::Value;

fn usage() -> ! {
    eprintln!("usage: abxbus-rust-roundtrip <events|bus> <input.json> <output.json>");
    process::exit(2);
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        usage();
    }

    let mode = &args[1];
    let input_path = &args[2];
    let output_path = &args[3];

    let raw = fs::read_to_string(input_path).unwrap_or_else(|error| {
        eprintln!("failed to read {input_path}: {error}");
        process::exit(1);
    });
    let payload: Value = serde_json::from_str(&raw).unwrap_or_else(|error| {
        eprintln!("failed to parse {input_path} as JSON: {error}");
        process::exit(1);
    });

    let roundtripped = match mode.as_str() {
        "events" => {
            let Some(events) = payload.as_array() else {
                eprintln!("events mode requires an array payload");
                process::exit(1);
            };
            Value::Array(
                events
                    .iter()
                    .cloned()
                    .map(|event| BaseEvent::from_json_value(event).to_json_value())
                    .collect(),
            )
        }
        "bus" => EventBus::from_json_value(payload).to_json_value(),
        _ => usage(),
    };

    let output = serde_json::to_string_pretty(&roundtripped).unwrap_or_else(|error| {
        eprintln!("failed to serialize roundtrip payload: {error}");
        process::exit(1);
    });
    fs::write(output_path, output).unwrap_or_else(|error| {
        eprintln!("failed to write {output_path}: {error}");
        process::exit(1);
    });
}
