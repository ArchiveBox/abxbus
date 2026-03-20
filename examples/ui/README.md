# abxbus Monitoring Dashboard UI

Minimal FastAPI Web UI application that reads the `events_log` and `event_results_log` tables produced by the `SQLiteHistoryMirrorMiddleware` and exposes them over HTTP/WebSocket for live monitoring by an administrator / developer.
For local debugging, this middleware-backed history is the most complete source because it includes lifecycle snapshots and handler result metadata.

## Quick start

```bash
git clone https://github.com/pirate/bbus.git
cd bbus
uv venv
uv pip install fastapi 'uvicorn[standard]'
```

```bash
# generate and save a live stream of test events (creates/appends to ./events.sqlite)
export EVENT_HISTORY_DB=./events.sqlite
uv run python -m examples.ui.test_events &
```

```bash
# run the UI backend server and then open the UI in your browser
uv run uvicorn examples.ui.main:app --reload
open http://localhost:8000
```

You should now see on [http://localhost:8000](http://localhost:8000) a simple dashboard that shows recent events and handler results in real-time (via WebSocket).

Replace `events.sqlite` with any db matching that schema to use in other codebases.

## Endpoints

- `GET /events?limit=20` – latest events (JSON)
- `GET /results?limit=20` – latest handler results (JSON)
- `GET /meta` – database path + table readiness flags
- `GET /` – minimal HTML dashboard
- `WS /ws/events` – pushes new rows as they arrive (`{"events": [...], "results": [...]}`)

This app is intentionally small so you can vibecode-extend it with additional metrics, authentication, or richer UI as needed.
