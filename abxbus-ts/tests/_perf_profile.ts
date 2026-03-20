import { BaseEvent, EventBus } from '../src/index.js'

const SimpleEvent = BaseEvent.extend('SimpleEvent', {})

const total_events = 200_000
// Keep full history to avoid trimming inflight events during perf profiling.
const bus = new EventBus('PerfBus', { max_history_size: total_events })

let processed_count = 0
bus.on(SimpleEvent, () => {
  processed_count += 1
})

// Baseline memory
global.gc?.()
const mem_before = process.memoryUsage()
console.log(`Memory before: RSS=${(mem_before.rss / 1024 / 1024).toFixed(1)}MB, Heap=${(mem_before.heapUsed / 1024 / 1024).toFixed(1)}MB`)

// Phase 1: Dispatch all events (measure dispatch throughput)
const t0 = performance.now()
const pending: Array<ReturnType<typeof SimpleEvent>> = []
for (let i = 0; i < total_events; i++) {
  pending.push(bus.emit(SimpleEvent({})))
}
const t1 = performance.now()
console.log(`Dispatch ${total_events} events: ${(t1 - t0).toFixed(0)}ms (${(total_events / ((t1 - t0) / 1000)).toFixed(0)} events/s)`)

const mem_after_dispatch = process.memoryUsage()
console.log(
  `Memory after dispatch: RSS=${(mem_after_dispatch.rss / 1024 / 1024).toFixed(1)}MB, Heap=${(mem_after_dispatch.heapUsed / 1024 / 1024).toFixed(1)}MB`
)

// Phase 2: Wait for all to complete
const t2 = performance.now()
await Promise.all(pending.map((e) => e.done()))
await bus.waitUntilIdle()
const t3 = performance.now()
console.log(`Await completion: ${(t3 - t2).toFixed(0)}ms`)
console.log(`Total: ${(t3 - t0).toFixed(0)}ms (${(total_events / ((t3 - t0) / 1000)).toFixed(0)} events/s)`)

const mem_after = process.memoryUsage()
console.log(
  `Memory after complete: RSS=${(mem_after.rss / 1024 / 1024).toFixed(1)}MB, Heap=${(mem_after.heapUsed / 1024 / 1024).toFixed(1)}MB`
)

global.gc?.()
const mem_gc = process.memoryUsage()
console.log(`Memory after GC: RSS=${(mem_gc.rss / 1024 / 1024).toFixed(1)}MB, Heap=${(mem_gc.heapUsed / 1024 / 1024).toFixed(1)}MB`)

const total_ms = t3 - t0
console.log(
  `Per-event: time=${(total_ms / total_events).toFixed(4)}ms, ` +
    `heap=${((mem_after.heapUsed - mem_before.heapUsed) / total_events / 1024).toFixed(2)}KB, ` +
    `heap_gc=${((mem_gc.heapUsed - mem_before.heapUsed) / total_events / 1024).toFixed(2)}KB`
)

console.log(`\nProcessed: ${processed_count}/${total_events}`)
console.log(`History size: ${bus.event_history.size} (max: ${bus.event_history.max_history_size})`)
console.log(`Heap delta (before GC): +${((mem_after.heapUsed - mem_before.heapUsed) / 1024 / 1024).toFixed(1)}MB`)
console.log(`Heap delta (after GC): +${((mem_gc.heapUsed - mem_before.heapUsed) / 1024 / 1024).toFixed(1)}MB`)
