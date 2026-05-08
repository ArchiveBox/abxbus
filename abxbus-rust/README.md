# abxbus-rust

Idiomatic Rust implementation of `abxbus`, matching the Python/TypeScript event JSON surface and execution semantics as closely as possible.

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
use abxbus_rust::{event, BaseEvent, event_bus::EventBus};
use futures::executor::block_on;
use serde_json::json;

event! {
    struct UserLoginEvent {
        username: String,
        event_result_type: serde_json::Value,
    }
}

let bus = EventBus::new(Some("MainBus".to_string()));
bus.on::<UserLoginEvent, _, _>("handle_login", |event| async move {
    Ok(json!({"ok": true, "username": event.payload().username}))
});

let event = bus.emit::<UserLoginEvent>(BaseEvent::new(UserLoginEvent {
    username: "alice".to_string(),
}));

block_on(async {
    event.wait_completed().await;
    println!("{}", event.inner.to_json_value());
});
```
