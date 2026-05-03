# Events, Decomplected
### *Causality, Transport, and the Function Call You Already Wrote*

> *"Most 'event-driven' code is just async/await with extra steps. And that's actually the right default."*

A 40-minute PyCon talk outline for [`abxbus`](https://github.com/ArchiveBox/abxbus).
Style: Rich-Hickey-style — definitional, decomplecting, sober, no live coding except one demo near the end.
Audience: junior + senior Python and full-stack devs, low prior exposure to event-driven systems.

**Total: 40 min.** Six parts. Backstory front-loaded, comparative meat in the middle, one demo, sober close.

---

## Part 0 — Who, why we're here (8 min)

### 0:00 – 0:30 · Title + me
- Single slide, no bio bullets. *"I make a thing called `abxbus`. I've been writing event systems for ~10 years and getting it wrong the whole time. This is what I think I finally figured out."*

### 0:30 – 5:30 · Backstory: four tries at the same problem (5 min)

Don't sell — *narrate the pattern you kept noticing*. One slide per project. Each ends with the same shape on the screen: arrows, parents, children.

1. **[redux-time](https://github.com/Monadical-SAS/redux-time) (2014, frontend animation)** — *"All state can be represented as layered patches that are a function of time."* A CSS animation engine where the animation queue lived in Redux. You could scrub time. Insight: **animations are events. So is every UI change. Why are they different code paths?**
2. **[OddSlingers.poker](https://github.com/Monadical-SAS/oddslingers.poker) (2016, multiplayer poker)** — Same idea, server-side. Every poker action — bet, fold, deal — was a redux-style action dispatched into the game engine, broadcast over Django Channels, replayed in the browser through redux-time. One stream of events drove logic, animation, replay, audit. It worked. *Then* I tried to add a second feature.
3. **[browser-use](https://github.com/browser-use/browser-use) (2025, AI browser agent)** — A fleet of Pydantic `Watchdog` classes (`CrashWatchdog`, `DOMWatchdog`, `ScreenshotWatchdog`...) listening for `NavigateToUrlEvent`, `TabCreatedEvent`, etc. The [`bubus`](https://github.com/browser-use/bubus) library was born here. *"Inspired by the simplicity of async and events in JS."*
4. **[ArchiveBox](https://github.com/ArchiveBox/ArchiveBox) (now)** — Big extractor pipeline (SingleFile, Chrome PDF, yt-dlp, Readability, git, ...) that fans out, retries, observes. The exact workload I'd been redesigning bus libraries for since 2014.

> **Pull quote slide**: *"I've shipped four event systems. Three of them I had to rewrite. The fourth one is `abxbus`."*

### 5:30 – 8:00 · The frustration

Every system I'd ever touched — Celery, NATS, Kafka, RabbitMQ, EventEmitter, Redux, Django signals, pyee, every internal "we'll just write our own bus" — was wrong **differently**. They each got *some* axis right. None got all of them.

> *"At some point you stop blaming the libraries and start asking what 'event' is supposed to mean."*

That question is the rest of the talk.

---

## Part 1 — The wedge (8 min)

### 8:00 – 10:30 · What "event-driven" promised

Slide: a list of words people use to sell event architectures.
- **Decoupling.**
- **Asynchrony.**
- **Resilience / retry.**
- **Observability.**
- **Scale.**
- **"It's just like Erlang."**

Hickey beat: *"All of these are real. None of them require a queue."*

Then the demo of the lie — a snippet from a real codebase (anonymized) where someone wrote:

```python
publish("user.created", {"id": 42})
# ...somewhere else, in another file, another process, another timezone...
@subscribe("user.created")
def send_welcome(payload): ...
```

Nobody ever found out if the email was sent. **We took a function call and we hid it from the type system.**

### 10:30 – 13:00 · The thesis, stated plainly

One slide. No animation.

> **The default for event-driven code should be a function call.**
> **A queue is a special case, not the base case.**

Junior takeaway (announced explicitly): *if you've used `await`, you already understand 90% of event systems.*

Senior takeaway (announced explicitly): *the parts of "event-driven architecture" worth keeping — causality, observability, retry, replay — are orthogonal to whether the call is queued. You've been buying a bundle.*

### 13:00 – 16:00 · Decomplect

Hickey's signature move. Whiteboard slide. We're going to take the word "event" apart.

When someone says **event-driven**, they actually mean **some subset of**:

| Concern | What it really is |
|---|---|
| **Identity** | This thing happened. It has an ID. It has a time. |
| **Shape** | The payload. Typed? Schemaless? Versioned? |
| **Causality** | What caused this? What did this cause? |
| **Transport** | In-memory? Cross-process? Cross-machine? |
| **Scheduling** | Now? Later? Strict order? Parallel? |
| **Durability** | Survives a crash? Replayable? |
| **Delivery** | Once? At-least-once? At-most-once? |

> *"These are seven different decisions. Most libraries make four of them for you, in the name."*

Celery picks: durable, queued, at-least-once, opaque. Period.
EventEmitter picks: in-memory, sync, no causality, no types. Period.
Kafka picks: durable, ordered-per-partition, at-least-once, opaque. Period.

**You don't get to mix.** That's the bug.

---

## Part 2 — What we conflated (10 min)

Three conflations. Three slides each: *the conflation, the cost, what `abxbus` does instead.*

### 16:00 – 19:00 · Conflation #1: Causality with Transport

> *"We invented queues to send events between machines, and then we forgot how to use events on one machine."*

The original event systems — Smalltalk-80, the DOM `EventTarget`, Erlang messages — were **causality machines**. "X happened, therefore Y should run." They didn't say a word about transport. The DOM doesn't put events on a queue across machines. It's just a function-call dispatcher with a registry.

When ops people built distributed systems, they bolted on a transport (Kafka, RabbitMQ) and called *that* the event system. Then app developers reverse-imported the queue model back into in-process code, where it never belonged.

`abxbus` separates them:
- **Causality** is `event_parent_id` + `event_path`. Always recorded. Free.
- **Transport** is a `Bridge`. Optional. Plug in Redis/NATS/Postgres if and only if you actually need to cross a machine boundary.

You can have causality without transport. You can. We forgot.

### 19:00 – 22:00 · Conflation #2: Scheduling with semantics

> *"The fact that something is queued is an implementation detail. We made it the API."*

Look at this code. Two snippets, side by side.

**Celery / Kafka / NATS shape:**
```python
charge_card.delay(order_id=42)   # fire and pray
# ... how do I get the receipt?
# ... I emit ReceiptEvent and find it later? subscribe? poll?
```

**`async`/`await` shape:**
```python
receipt = await charge_card(order_id=42)
```

The second one is what we want. The first one is what we got because somebody decided "events" meant "queued."

Now `abxbus`:

```python
async def on_checkout(event: CheckoutEvent) -> str:
    charge = event.emit(ChargeCardEvent(order_id=event.order_id))
    await charge                              # queue-jump, runs NOW
    return await charge.event_result()        # typed return value
```

`await child_event` inside a handler is **immediate-execution** — it queue-jumps the bus and runs synchronously *from the caller's point of view*. Same code, same call stack, same exception propagation. **It's a function call.**

But now `event_parent_id`, `event_children`, `event_path`, the WAL log, the timing, the OTel span — all of it — is recorded for free.

> *"You write async/await. You get an event system."*

The five-line rule (one slide):

```
event.emit(ChildEvent())         → linked, background
await event.emit(ChildEvent())   → linked, foreground (queue-jumps, RPC-style)  ← USE THIS 90%
bus.emit(TopLevelEvent())        → detached, background  ← THE RARE CASE
await bus.emit(TopLevelEvent())  → detached, foreground
```

The rare case **is rare**. Cron-style triggers. Outbound webhooks. Things that genuinely should outlive their caller. That's it. **The default is a function call.**

### 22:00 – 26:00 · Conflation #3: Shape with Delivery

> *"`{"id": 42}` is not a contract. It's a wish."*

Most bus libraries take `dict` or bytes. Then six months later you grep production logs trying to figure out what fields a 2019 event had.

`abxbus` events are Pydantic models (or Zod in TS). The same class is the schema, the validator, the IDE autocomplete, the JSON serializer for the bridge, and — critically — the **return-type annotation**:

```python
class ChargeCardEvent(BaseEvent[str]):   # BaseEvent[str] = handlers must return str
    order_id: str
```

Compile-time check. Runtime check. Cross-language check (Python ↔ TS over a bridge). Same model.

You don't pay for this. You just stop dropping it.

---

## Part 3 — The comparison table (6 min)

### 26:00 – 32:00 · One slide. Held for four minutes.

This is the slide people will photograph. Don't rush it.

| | **`asyncio` / `await`** | **EventEmitter / pyee** | **Celery / RQ** | **NATS** | **Kafka** | **`abxbus`** |
|---|---|---|---|---|---|---|
| In-process default | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ |
| Cross-process | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ (bridge) |
| Foreground (RPC-style) | ✅ | ✅ | ❌ | ⚠️ req/reply | ❌ | ✅ |
| Background queued | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ (rare) |
| Causality / parent-child | ❌ | ❌ | ⚠️ headers | ❌ | ❌ | ✅ |
| Typed payloads | ❌ | ❌ | ❌ | ❌ | ❌ schema reg | ✅ |
| Typed return values | ✅ | ❌ | ❌ | ⚠️ | ❌ | ✅ |
| Replay / WAL | ❌ | ❌ | ⚠️ | ❌ | ✅ | ✅ |
| Built-in observability | ❌ | ❌ | ⚠️ | ⚠️ | ⚠️ | ✅ |
| Forces durable storage | — | — | ✅ | ❌ | ✅ | ❌ (opt-in) |
| Lines of code to "hello world" | 4 | 5 | ~30 + broker | ~20 + broker | ~50 + cluster | 6 |

Hickey beats while it's up:

- *"Look at the row that says 'foreground RPC-style.' That's where most of your code lives. Look how many of these check that box."*
- *"Now look at 'causality.' That's what you actually wanted from 'event-driven.' Look how few check it."*
- *"Celery is a great Celery. It is not a great event system. We've been using a hammer to do typing."*
- *"Kafka is a great log. It is not an in-process function-call mechanism. Stop putting it in your monolith."*
- *"`asyncio` is almost the right answer. It's missing the registry, the lineage, and the cross-process door. `abxbus` is `asyncio` with those three things."*

Senior-dev sentence (called out): **`abxbus` doesn't replace Kafka. It replaces the in-process bus you wrote on top of Kafka because you couldn't figure out how else to coordinate work.**

---

## Part 4 — Demo (5 min)

### 32:00 – 37:00 · `log_tree()` + forwarding + concurrency knobs

One terminal, three scenes. Pre-record if live demos make you nervous; the talk doesn't depend on it being live.

**Scene 1 (90s) — Causality you can see.** Run the checkout example from [`examples/parent_child_tracking.py`](./examples/parent_child_tracking.py). Show the `bus.log_tree()` output:

```text
└── CheckoutEvent#b7c7 [10:10:54.522 (0.003s)]
    └── ✅ on_checkout#7a12 → 'reserve|charge:fraud-ok|receipt'
        ├── ReserveInventoryEvent#ca2f
        │   └── ✅ on_reserve → 'reserve:ord-123'
        ├── ChargeCardEvent#b746
        │   └── ✅ on_charge
        │       └── FraudCheckEvent#31e0
        │           └── ✅ on_fraud → 'fraud-ok:ord-123'
        └── SendReceiptEvent#c399
```

> *"That tree is from running normal `async`/`await` code. I didn't add any tracing. I didn't wrap anything. I wrote functions."*

**Scene 2 (90s) — Forwarding across buses.** Two `EventBus`es (`MainBus`, `AuthBus`). `main.on('*', auth.emit)`. Show `event_path = ['MainBus#ab12', 'AuthBus#cd34']`. The same call, same code, but now the auth event genuinely crossed a bus boundary — and lineage held.

> *"This is the door. On the other side of the door, you can put a Redis bridge. Or a NATS bridge. Or another process. The handler doesn't know."*

**Scene 3 (90s) — The concurrency dial.** Show [`examples/immediate_event_processing.py`](./examples/immediate_event_processing.py) running in two modes:

- `await child` — sibling waits, child runs first (queue-jump, foreground).
- `await child.event_completed()` — sibling runs first, child waits its turn (background, FIFO).

> *"Same code. One word changed. That's the whole knob."*

**Scene 4 (60s, only if time)** — `event_concurrency='parallel'` + `asyncio.gather` of three child events. Three handlers run simultaneously, lineage still tracked.

---

## Part 5 — The honest part (3 min)

### 37:00 – 38:00 · Brief gestures at the rest

Three things in 60 seconds, no more:
- **[Bridges](./docs/integrations/bridges.mdx)** (Redis/NATS/Postgres/SQLite/JSONL/HTTP/socket): when you actually need to cross a machine, the door is one line of code.
- **[Middleware](./docs/integrations/middlewares.mdx)** (`WAL`, `OtelTracing`, `SQLiteHistoryMirror`, `AutoReturn`, `AutoError`): replay, audit, tracing — opt in.
- **[`@retry`](./docs/api/retry.mdx)** and **[`events_suck.wrap()`](./docs/further-reading/events-suck.mdx)**: retry decorator and a wrapper that gives you `client.create(...)` instead of `bus.emit(...)` for teams who really do want it to look like a function call. *"Because sometimes events do suck."*

### 38:00 – 39:30 · When NOT to reach for `abxbus`

Hickey-honest. One slide.

- **You need durable, distributed, partition-tolerant ordering at scale.** Use Kafka. `abxbus` is in-process by default; the bridges are good but they are not Kafka.
- **You need cross-team contract governance with schema registries.** Buy or build that on top.
- **You need work to survive process restart with no loss.** WAL middleware helps; it's not a broker.
- **You're writing 50 lines of code.** Just write `async def`. That's literally the talk's thesis turned against me.

> *"The point isn't that `abxbus` is the only answer. The point is that for the slice of work where you've been reaching for Celery or Kafka and didn't actually need them — that slice is bigger than you think."*

### 39:30 – 40:00 · Close

One slide. Call back to the opening.

> *"Causality is not a queue.*
> *Transport is not semantics.*
> *Shape is not delivery.*
> *And the function call you already wrote is, almost always, the event system you wanted."*

`pip install abxbus` · `npm install abxbus` · [github.com/ArchiveBox/abxbus](https://github.com/ArchiveBox/abxbus)

Thank you.

---

## Production notes

- **Slide count target: ~32.** ~10 in Part 0, 6 in Part 1, 9 in Part 2, 1 in Part 3 (held), 4 demo, 3 close. Hickey averages ~30/40min — don't pad.
- **No bullets >5 words on any slide except the comparison table.** Force yourself to talk.
- **One recurring example throughout (checkout/charge/receipt/fraud).** Already in the repo. Don't introduce a second domain.
- **The phrase to repeat 3 times across the talk:** *"a queue is a special case, not the base case."* That's the bumper sticker.
- **The single image to make memorable:** the `log_tree()` output. It's the thing nothing else has.
- **Cut candidates if you run long:** Scene 4 of demo, Conflation #3 (shape) can be tightened to 2 min, the "promised" list in Part 1 can lose 30s.

---

# References

## Repos referenced in the backstory

- **[redux-time](https://github.com/Monadical-SAS/redux-time)** — frontend animation as a pure function of time over a Redux event log. *"All state can be represented as layered patches that are a function of time."*
- **[OddSlingers.poker](https://github.com/Monadical-SAS/oddslingers.poker)** — Django + Channels + React/Redux poker site; event-sourced game engine where every poker action is dispatched, broadcast, and replayed through redux-time on the client.
- **[browser-use](https://github.com/browser-use/browser-use)** — AI browser-automation agent. Watchdog/event architecture: [`browser_use/browser/events.py`](https://github.com/browser-use/browser-use/blob/main/browser_use/browser/events.py), [`browser_use/browser/watchdog_base.py`](https://github.com/browser-use/browser-use/blob/main/browser_use/browser/watchdog_base.py).
- **[bubus](https://github.com/browser-use/bubus)** — predecessor of `abxbus`, original async-Python event bus written for browser-use's watchdogs.
- **[ArchiveBox](https://github.com/ArchiveBox/ArchiveBox)** — self-hosted web-archiving tool; the production user of `abxbus`.
- **[abx-dl](https://github.com/ArchiveBox/abx-dl)** — newer ArchiveBox-family tool also built on `abxbus`.

## `abxbus` docs referenced in the talk

- [Overview](./docs/index.mdx) · [Quickstart](./docs/quickstart.mdx)
- [Immediate Execution (RPC-style)](./docs/concurrency/immediate-execution.mdx) — the queue-jump / foreground execution pattern that powers the talk's central thesis.
- [Parent-Child Tracking](./docs/features/parent-child-tracking.mdx) — `event_parent_id`, `event_children`, `event_path`, `bus.log_tree()`.
- [Forwarding Between Buses](./docs/features/forwarding-between-buses.mdx)
- [Find Events](./docs/features/find-events.mdx) · [Event Debouncing](./docs/features/event-debouncing.mdx)
- [Typed Events](./docs/features/typed-events.mdx) · [Return Value Handling](./docs/features/return-value-handling.mdx)
- Concurrency: [bus-serial](./docs/concurrency/events-bus-serial.mdx) · [global-serial](./docs/concurrency/events-global-serial.mdx) · [parallel](./docs/concurrency/events-parallel.mdx) · [handlers-serial](./docs/concurrency/handlers-serial.mdx) · [handlers-parallel](./docs/concurrency/handlers-parallel.mdx) · [completion-all](./docs/concurrency/handler-completion-all.mdx) · [completion-first](./docs/concurrency/handler-completion-first.mdx) · [timeouts](./docs/concurrency/timeouts.mdx) · [backpressure](./docs/concurrency/backpressure.mdx)
- [Bridges](./docs/integrations/bridges.mdx) — [HTTP](./docs/integrations/bridge-http.mdx) · [Socket](./docs/integrations/bridge-socket.mdx) · [Redis](./docs/integrations/bridge-redis.mdx) · [NATS](./docs/integrations/bridge-nats.mdx) · [Postgres](./docs/integrations/bridge-postgres.mdx) · [JSONL](./docs/integrations/bridge-jsonl.mdx) · [SQLite](./docs/integrations/bridge-sqlite.mdx)
- [Middlewares overview](./docs/integrations/middlewares.mdx) — [WAL](./docs/integrations/middleware-wal.mdx) · [Logger](./docs/integrations/middleware-logger.mdx) · [OTel Tracing](./docs/integrations/middleware-otel-tracing.mdx) · [SQLite History Mirror](./docs/integrations/middleware-sqlite-history-mirror.mdx) · [Auto-Return](./docs/integrations/middleware-auto-return.mdx) · [Auto-Error](./docs/integrations/middleware-auto-error.mdx) · [Auto-Handler-Change](./docs/integrations/middleware-auto-handler-change.mdx)
- [`@retry`](./docs/api/retry.mdx) · [`events_suck`](./docs/further-reading/events-suck.mdx) · [Similar projects](./docs/further-reading/similar-projects.mdx)

## Demo files used in Part 4

- [`examples/simple.py`](./examples/simple.py)
- [`examples/parent_child_tracking.py`](./examples/parent_child_tracking.py)
- [`examples/immediate_event_processing.py`](./examples/immediate_event_processing.py)
- [`examples/forwarding_between_busses.py`](./examples/forwarding_between_busses.py)
- [`examples/concurrency_options.py`](./examples/concurrency_options.py)
- [`examples/log_tree_demo.py`](./examples/log_tree_demo.py)

## External / inspirational reading

- Rich Hickey, *Simple Made Easy* (2011) — the source of the "decomplect" frame and the *simple ≠ easy* distinction the talk leans on.
- Rich Hickey, *The Value of Values* (2012) — useful for the "events are immutable values, queues are an implementation detail" framing.
- Rich Hickey, *Hammock Driven Development* — recommended priming for the audience.
- Cosmic Python, [Chapter 8: Events and the Message Bus](https://www.cosmicpython.com/book/chapter_08_events_and_message_bus.html) — clearest written treatment of in-process buses in Python.
- MDN, [`EventTarget`](https://developer.mozilla.org/en-US/docs/Web/API/EventTarget) — the original "good" event API. The DOM got it right before we forgot.
- Sindre Sorhus, [`emittery`](https://github.com/sindresorhus/emittery) — closest spiritual sibling in JS-land.
- pytest, [`pluggy`](https://github.com/pytest-dev/pluggy) — registry-based dispatch, in-process, typed-ish; another point on the same axis.
- Erlang, [`gen_server`](https://www.erlang.org/doc/design_principles/gen_server_concepts.html) — how the original "everything is a message" world handled causality without inventing Kafka.
- Ethereum Foundation, [`lahja`](https://github.com/ethereum/lahja) — multi-process Python event bus, good comparison point for "what does it take to add a transport tastefully."

---

# Extra context (not for the talk, for the speaker)

## Why this thesis matters more than the library

The library is a vehicle for the idea. If the audience walks out only remembering *"a queue is a special case, not the base case"* — and they go back to work and stop reaching for Celery for a 50ms in-process call — the talk succeeded, even if they never `pip install abxbus`.

The library exists because the idea, once you accept it, has follow-on requirements that nobody had bundled together:

1. The function-call shape (`await child_event`).
2. Causality recorded automatically (`event_parent_id`, `event_path`).
3. A typed-payload contract that survives JSON serialization (Pydantic / Zod).
4. A door to cross machines when — and only when — you need it (Bridges).
5. Observability that doesn't require new vocabulary (`log_tree()`, OTel middleware).

You can build #1–#5 yourself. You will build them badly the first three times. (See backstory.)

## What `abxbus` is *not* trying to be

- Not a broker. Bridges are connectors, not durable storage.
- Not a workflow engine (Temporal, Airflow, Prefect). No long-running stateful step orchestration.
- Not a CQRS framework. Events are just typed function calls with lineage; we don't prescribe read/write segregation.
- Not an actor system. No mailboxes, no `self()`, no PIDs.

## Likely audience objections (prepare answers)

| Objection | One-liner answer |
|---|---|
| *"This is just a function call with extra steps."* | *"Yes. That's the thesis. The 'extra steps' are observability, retry, lineage, and a transport door."* |
| *"Why not just use `asyncio` directly?"* | *"You should — until you want to add the registry, the parent tree, or the cross-process door. Then `abxbus` is `asyncio` plus those three things."* |
| *"Celery already does this."* | *"Celery is durable background work. We're talking about foreground work. Different job."* |
| *"How does this scale to N machines?"* | *"It doesn't, by itself. Bridges scale it. But most teams don't actually have N machines worth of in-process events; they have one machine and a fear of in-process events."* |
| *"What about backpressure?"* | *"`max_history_size`, `max_history_drop`, and FIFO concurrency modes. Slide deferred to docs — see [`backpressure.mdx`](./docs/concurrency/backpressure.mdx)."* |
| *"Pydantic is slow."* | *"Pydantic v2 is fast enough that the bus does ~0.2ms/event in the perf suite. Show the numbers in the README."* |
| *"What if my handler crashes?"* | *"Captured in `EventResult.error`, propagated to `await event` as an exception, lineage preserved. Same as a function call, because it is one."* |

## Rehearsal targets

- Backstory section ≤ 5:00. If it runs to 6:00, cut OddSlingers to one sentence.
- The decomplect table ≤ 90 seconds on screen. Don't read it row by row — make the audience read.
- The comparison table ≥ 4:00 on screen. This is the one slide that has to *stick*.
- Demo: rehearse the `log_tree()` output until you can narrate it cold. If the terminal hiccups live, the recording is the safety net.

## Possible Q&A primers

- **"Does it work with FastAPI / Django / Flask?"** Yes; ContextVar propagation is the FastAPI integration point. Request-scoped buses + `bus.stop(clear=True)` in the request finalizer.
- **"Cross-language: how strict is Python ↔ TS compatibility?"** Tested via `tests/test_cross_runtime_roundtrip.py`. Shape-compatible types (TypedDict / dataclass / model dicts) round-trip; fine-grained custom validators don't.
- **"OTel?"** [`OtelTracingMiddleware`](./docs/integrations/middleware-otel-tracing.mdx) emits parent-child-linked spans. Sentry ingests via OTel.
- **"Why not just use Temporal?"** Temporal is durable workflow orchestration. `abxbus` is in-process function-call replacement with optional transport. Different layer.

---

# Addendums

## Addendum A — One-paragraph version (for a conference blurb)

> Most "event-driven architecture" code is async/await with extra boilerplate, and most of the boilerplate is wrong. This talk argues that the default for event-driven code should be a function call, not a queue; that we conflated three orthogonal concerns (causality, transport, scheduling) into one word; and that once you separate them, you can have observability, lineage, replay, and cross-machine reach *without* paying the queueing tax on every call. We'll compare `asyncio`, EventEmitter, Celery, NATS, Kafka, and `abxbus` along seven axes, and demo a single recurring example that shows the resulting log tree, queue-jump behavior, and bus forwarding.

## Addendum B — Alternate structures considered (and rejected)

1. **"Build an event bus from scratch on stage"** — too much code, not enough idea. Rejected.
2. **"Tour of `abxbus` features"** — sales pitch, not a talk. Rejected.
3. **"The history of message passing from Smalltalk to Kafka"** — interesting, but the audience leaves with no actionable thesis. Rejected.
4. **The current shape: one thesis, four supporting decomplections, one comparison, one demo, sober close.** Adopted.

## Addendum C — If you only have 25 minutes

Cut order:
1. Backstory: 5:00 → 2:00. Just `redux-time` + `browser-use`, drop OddSlingers + ArchiveBox.
2. Conflation #3 (Shape with Delivery): cut entirely; reference docs.
3. Demo Scene 4: cut.
4. The `When NOT to reach for abxbus` slide: cut to one line (*"Use Kafka if you actually need Kafka"*).

Result: ~25 minutes, same thesis, same memorable image.

## Addendum D — If you only have 10 minutes (lightning talk)

1. *"The default for event-driven code should be a function call."* (1 min)
2. The five-line rule. (2 min)
3. The comparison table, abridged to 3 columns: `asyncio`, Celery, `abxbus`. (3 min)
4. `log_tree()` screenshot. (2 min)
5. Close: *"Causality is not a queue."* + URL. (1 min)
6. Buffer. (1 min)

## Addendum E — Talk title alternates

- *"Events, Decomplected"* (current — Hickey-style)
- *"The Function Call You Forgot You Wrote"*
- *"A Queue is a Special Case"*
- *"Causality is not a Queue"*
- *"Foreground Events"* (terse, technical)
- *"What's an Event, Really?"* (most Hickey)

## Addendum F — Things to *not* do on stage

- Do not say "paradigm shift."
- Do not say "best practice."
- Do not pitch the library before the thesis lands.
- Do not apologize for in-process by default. It is a feature.
- Do not show benchmarks before showing meaning.
- Do not live-code in front of 800 people unless rehearsed 10+ times.

## Addendum G — Suggested follow-up writing

If the talk lands, the natural follow-ups (each a separate blog post or future talk) are:

1. *"Causality without Transport"* — a short essay on `event_parent_id` as the single under-appreciated feature.
2. *"The Queue Was Never the Point"* — historical piece tying Smalltalk → DOM → Erlang → Redux → `abxbus`.
3. *"Cross-Language Events with Pydantic and Zod"* — a hands-on piece, more practical than philosophical.
4. *"When Your In-Process Bus Should Become a Real Bus"* — the bridges story, told as a migration guide rather than a feature list.
