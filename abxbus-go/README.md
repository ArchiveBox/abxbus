# `abxbus-go`

Go implementation of the core AbxBus event bus behavior.

Implemented core features:
- EventBus / BaseEvent / EventHandler / EventResult
- typed payload/result helpers for Go structs
- event_concurrency, event_handler_concurrency, event_handler_completion
- `find()` / `filter()` history and future lookup helpers
- queue-jump via `event.Done(ctx)`
- timeout handling with context propagation
- `event_result_type` JSON Schema enforcement for handler return values
- JSON-compatible snake_case wire format
- `ToJSON` / `FromJSON` roundtrips for EventBus, BaseEvent, EventHandler, EventResult
- Python/TS/Rust-compatible cross-runtime roundtrip helper: `tests/roundtrip_cli`
- `JSONLEventBridge`
- `OtelTracingMiddleware`

Intentionally not implemented yet:
- event_suck helpers
- retry decorator / retry middleware
- bridge implementations other than JSONLBridge
- middleware implementations other than OtelTracingMiddleware

## Development

```bash
go test ./...
go run ./tests/roundtrip_cli events input.json output.json
go run ./tests/roundtrip_cli bus input.json output.json
```

Cross-runtime parity tests live in the Python and TypeScript test suites. From the repo root:

```bash
uv run pytest tests/test_cross_runtime_roundtrip.py -q
pnpm --dir abxbus-ts exec node --expose-gc --test --import tsx tests/cross_runtime_roundtrip.test.ts
```
