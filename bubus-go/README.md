# bubus-go

Go implementation of the core bubus event bus behavior.

Implemented core features:
- EventBus / BaseEvent / EventHandler / EventResult
- event_concurrency, event_handler_concurrency, event_handler_completion
- queue-jump via `event.Done(ctx)`
- timeout handling with context propagation
- JSON-compatible snake_case wire format
- `ToJSON` / `FromJSON` roundtrips for EventBus, BaseEvent, EventHandler, EventResult

Not yet implemented:
- bridges
- middlewares (left intentionally unimplemented; add hook calls where parity is needed)
