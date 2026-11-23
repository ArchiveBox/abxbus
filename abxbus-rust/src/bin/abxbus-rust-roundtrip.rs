use std::{env, fs, path::PathBuf, process, sync::mpsc, time::Duration};

use abxbus::{base_event::BaseEvent, event_bus::EventBus, jsonl_bridge::JsonlEventBridge};
use serde::Deserialize;
use serde_json::Value;

fn usage() -> ! {
    eprintln!("usage: abxbus-rust-roundtrip <events|bus> <input.json> <output.json>");
    eprintln!("       abxbus-rust-roundtrip <jsonl-listener|jsonl-sender> <config.json>");
    process::exit(2);
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() == 3 && matches!(args[1].as_str(), "jsonl-listener" | "jsonl-sender") {
        if let Err(error) = run_jsonl(&args[1], &args[2]) {
            eprintln!("JSONL worker failed: {error}");
            process::exit(1);
        }
        return;
    }
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

#[derive(Deserialize)]
struct JsonlWorkerConfig {
    path: PathBuf,
    ready_path: Option<PathBuf>,
    output_path: Option<PathBuf>,
    event_path: Option<PathBuf>,
    #[serde(default)]
    expected_count: usize,
}

fn run_jsonl(mode: &str, config_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let config: JsonlWorkerConfig = serde_json::from_slice(&fs::read(config_path)?)?;
    let bridge = JsonlEventBridge::new(config.path, Some(Duration::from_millis(50)), None);
    if mode == "jsonl-sender" {
        let event_path = config.event_path.ok_or("missing event_path")?;
        let event = BaseEvent::from_json_value(serde_json::from_slice(&fs::read(event_path)?)?);
        bridge.emit_base(event)?;
        return Ok(());
    }

    let (sender, receiver) = mpsc::channel();
    bridge.on_raw_sync("*", "capture", move |event| {
        sender
            .send(event.to_json_value())
            .map_err(|error| error.to_string())?;
        Ok(Value::Null)
    })?;
    fs::write(config.ready_path.ok_or("missing ready_path")?, b"ready")?;
    let count = config.expected_count.max(1);
    let events = (0..count)
        .map(|_| receiver.recv_timeout(Duration::from_secs(30)))
        .collect::<Result<Vec<_>, _>>()?;
    let output = if count == 1 {
        events.into_iter().next().unwrap()
    } else {
        Value::Array(events)
    };
    fs::write(
        config.output_path.ok_or("missing output_path")?,
        serde_json::to_vec(&output)?,
    )?;
    Ok(())
}
