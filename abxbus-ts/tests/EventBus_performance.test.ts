import assert from 'node:assert/strict'
import { test } from 'node:test'

import { BaseEvent, EventBus, EventHandlerCancelledError, EventHandlerTimeoutError } from '../src/index.js'
import {
  runCleanupEquivalence,
  runPerf50kEvents,
  runPerfEphemeralBuses,
  runPerfSingleEventManyFixedHandlers,
  runPerfOnOffChurn,
  runPerfWorstCase,
} from './performance.scenarios.js'

const nodePerfInput = {
  runtimeName: 'node:test',
  api: { BaseEvent, EventBus, EventHandlerTimeoutError, EventHandlerCancelledError },
  now: () => performance.now(),
  sleep: (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)),
  log: (message: string) => console.log(message),
  getMemoryUsage: () => process.memoryUsage(),
  limits: {
    singleRunMs: 30_000,
    worstCaseMs: 60_000,
  },
}

test('processes 50k events within reasonable time', { timeout: 30_000 }, async () => {
  const result = await runPerf50kEvents(nodePerfInput)
  assert.equal(result.scenario, '50k events')
})

test('500 ephemeral buses with 100 events each', { timeout: 30_000 }, async () => {
  const result = await runPerfEphemeralBuses(nodePerfInput)
  assert.equal(result.scenario, '500 buses x 100 events')
})

test('1 event with 50k parallel handlers', { timeout: 30_000 }, async () => {
  const result = await runPerfSingleEventManyFixedHandlers(nodePerfInput)
  assert.equal(result.scenario, '1 event x 50k parallel handlers')
})

test('50k events with 50k one-off handlers on a single bus', { timeout: 30_000 }, async () => {
  const result = await runPerfOnOffChurn(nodePerfInput)
  assert.equal(result.scenario, '50k one-off handlers over 50k events')
})

test('worst-case: forwarding + queue-jump + timeouts + cancellation at scale', { timeout: 60_000 }, async () => {
  const result = await runPerfWorstCase(nodePerfInput)
  assert.equal(result.scenario, 'worst-case forwarding + timeouts')
})

test('cleanup equivalence: destroy() vs out-of-scope collection', { timeout: 60_000 }, async () => {
  const result = await runCleanupEquivalence(nodePerfInput)
  assert.equal(result.scenario, 'cleanup destroy vs scope equivalence')
  assert.equal(result.equivalent, true)
})
