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

Intentionally not implemented yet:
- event_suck helpers
- retry decorator / retry middleware
- bridge implementations other than JSONLBridge
- middleware implementations

## Development

Verify the local module from the repository checkout:

```bash
cd abxbus-go
test "$(go list -m -f '{{.Path}}')" = "github.com/ArchiveBox/abxbus/abxbus-go/v2"
go test .
```

Import the package as `abxbus "github.com/ArchiveBox/abxbus/abxbus-go/v2"`.

```bash
cd abxbus-go
go test ./tests/roundtrip_cli
```

Result helpers are intentionally no-arg by default:

<pre><code>
value, err := event.EventResult()
values, err := event.EventResultsList(&abxbus.EventResultOptions{
	RaiseIfAny:  false,
	RaiseIfNone: false,
})
</code></pre>

Only `EmitWithContext(...)` accepts a caller-provided context. `Now()`, `Wait()`, `EventResult()`, and `EventResultsList()` use the context snapshot captured at emit/handler-dispatch time plus their own native timeout options.

Destroy clears bus-owned state by default:

<pre><code>
bus.Destroy()
bus.DestroyWithOptions(&abxbus.EventBusDestroyOptions{Clear: false}) // still terminal; preserves handlers/history for inspection
</code></pre>

Cross-runtime parity tests live in the Python and TypeScript test suites. From the repo root:

```bash
test -f tests/test_cross_runtime_roundtrip.py
test -f abxbus-ts/tests/cross_runtime_roundtrip.test.ts
```
