# bubus-rust

Idiomatic Rust implementation of `bubus`, matching the Python/TypeScript event JSON surface and execution semantics as closely as possible.

## Current scope

Implemented core features:
- Base event model and event result model with serde JSON compatibility
- Async event bus with queueing and queue-jump behavior
- Event concurrency: `global-serial`, `bus-serial`, `parallel`
- Handler concurrency: `serial`, `parallel`
- Handler completion strategies: `all`, `first`
- Event path tracking and pending bus count

Not yet implemented in this crate revision:
- Bridges
- Middlewares (hook points are left in code comments)

## Quickstart

```rust
use bubus_rust::{base_event, event_bus};
use futures::executor::block_on;
use serde_json::{Map, json};

let bus = event_bus::new(Some("MainBus".to_string()));
bus.on("UserLoginEvent", "handle_login", |event| async move {
    Ok(json!({"ok": true, "event_id": event.inner.lock().event_id}))
});

let mut payload = Map::new();
payload.insert("username".to_string(), json!("alice"));
let event = base_event::new("UserLoginEvent", payload);
bus.emit(event.clone());

block_on(async {
    event.wait_completed().await;
    println!("{}", event.to_json_value());
});
```
