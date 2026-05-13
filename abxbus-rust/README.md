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
use abxbus_rust::{event, event_bus::EventBus};
use futures::executor::block_on;
use serde_json::json;

event! {
    struct UserLoginEvent {
        username: String,
        event_result_type: serde_json::Value,
    }
}

let bus = EventBus::new(Some("MainBus".to_string()));
bus.on(UserLoginEvent, |event: UserLoginEvent| async move {
    Ok(json!({"ok": true, "username": event.username}))
});

let event = bus.emit(UserLoginEvent {
    username: "alice".to_string(),
    ..Default::default()
});

block_on(async {
    event.now().await?;
    println!("{}", event.inner.to_json_value());
    Ok::<(), String>(())
});
```

Use `event.now_with_options(EventWaitOptions { first_result: true, ..Default::default() })` when you want to resolve as soon as the first valid result is available while the remaining handlers continue running.
