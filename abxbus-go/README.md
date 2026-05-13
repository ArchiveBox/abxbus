# `abxbus-go`

Go implementation of the core AbxBus event bus behavior.

Implemented core features:
- EventBus / BaseEvent / EventHandler / EventResult
- typed payload/result helpers for Go structs
- event_concurrency, event_handler_concurrency, event_handler_completion
- `find()` / `filter()` history and future lookup helpers
- queue-jump via `event.Now()`; use `event.Wait()` for passive completion waits
- timeout handling with context propagation captured at emit time
- result helpers with no-arg defaults (`RaiseIfAny=true`, `RaiseIfNone=false`) and optional `EventResultOptions`
- `Destroy()` / `DestroyWithOptions(...)` lifecycle cleanup (`Clear=true` by default)
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

Result helpers are intentionally no-arg by default:

```go
value, err := event.EventResult()
values, err := event.EventResultsList(&abxbus.EventResultOptions{
	RaiseIfAny:  false,
	RaiseIfNone: false,
})
```

Only `EmitWithContext(...)` accepts a caller-provided context. `Now()`, `Wait()`, `EventResult()`, and `EventResultsList()` use the context snapshot captured at emit/handler-dispatch time plus their own native timeout options.

Destroy clears bus-owned state by default:

```go
bus.Destroy()
bus.DestroyWithOptions(&abxbus.EventBusDestroyOptions{Clear: false}) // preserve handlers/history for inspection
```

Cross-runtime parity tests live in the Python and TypeScript test suites. From the repo root:

```bash
uv run pytest tests/test_cross_runtime_roundtrip.py -q
pnpm --dir abxbus-ts exec node --expose-gc --test --import tsx tests/cross_runtime_roundtrip.test.ts
```
