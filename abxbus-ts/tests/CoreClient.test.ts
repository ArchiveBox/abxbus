import assert from 'node:assert/strict'
import { test } from 'node:test'

import { RustCoreClient } from '../src/index.js'

const busRecord = (): Record<string, unknown> => ({
  bus_id: 'ts-bus',
  name: 'TsBus',
  label: 'TsBus#0001',
  defaults: {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
    event_handler_completion: 'all',
    event_timeout: 60,
    event_slow_timeout: 300,
    event_handler_timeout: null,
    event_handler_slow_timeout: 30,
  },
  host_id: 'ts-host',
})

const handlerRecord = (): Record<string, unknown> => ({
  handler_id: 'ts-handler',
  bus_id: 'ts-bus',
  host_id: 'ts-host',
  event_pattern: 'TsEvent',
  handler_name: 'ts_handler',
  handler_file_path: null,
  handler_registered_at: '2026-05-08T00:00:00Z',
  handler_timeout: null,
  handler_slow_timeout: null,
  handler_concurrency: null,
  handler_completion: null,
})

const eventRecord = (): Record<string, unknown> => ({
  event_type: 'TsEvent',
  event_version: '0.0.1',
  event_id: 'ts-event',
  event_path: [],
  event_created_at: '2026-05-08T00:00:00Z',
  event_status: 'pending',
  event_blocks_parent_completion: false,
  value: 41,
})

test('RustCoreClient roundtrips versioned Tachyon protocol', async () => {
  const core = new RustCoreClient({ session_id: 'ts-test' })
  try {
    const responses = await core.request({ type: 'register_bus', bus: busRecord() })
    assert.ok(responses.length > 0)
    const first = responses[0]
    assert.equal(first.protocol_version, 1)
    assert.equal(first.session_id, 'ts-test')
    assert.equal(first.message.type, 'patch')
    assert.equal((first.message.patch as Record<string, unknown>).type, 'bus_registered')
  } finally {
    core.close()
  }
})

test('RustCoreClient drives native handler turn through core', async () => {
  const core = new RustCoreClient({ session_id: 'ts-turn' })
  try {
    await core.registerBus(busRecord())
    await core.registerHandler(handlerRecord())
    await core.emitEvent(eventRecord(), 'ts-bus')

    const messages = await core.processNextRoute('ts-bus')
    const invocation = messages.find((message) => message.type === 'invoke_handler')!
    assert.equal(invocation.handler_id, 'ts-handler')
    const event_snapshot = invocation.event_snapshot as { value: number }

    const completion = await core.completeHandler(invocation, event_snapshot.value + 1)
    assert.ok(
      completion.some((message) => message.type === 'patch' && (message.patch as Record<string, unknown>).type === 'result_completed')
    )

    const result_completed = completion.find(
      (message) => message.type === 'patch' && (message.patch as Record<string, unknown>).type === 'result_completed'
    )!.patch as { result: { result: number; route_id: string } }
    assert.equal(result_completed.result.result, 42)
    const route_id = result_completed.result.route_id
    const done = await core.processRoute(route_id)
    assert.ok(done.some((message) => message.type === 'patch' && (message.patch as Record<string, unknown>).type === 'event_completed'))
  } finally {
    core.close()
  }
})

test('RustCoreClient commits host error outcome', async () => {
  const core = new RustCoreClient({ session_id: 'ts-error' })
  try {
    const bus = { ...busRecord(), bus_id: 'ts-error-bus' }
    const handler = { ...handlerRecord(), bus_id: 'ts-error-bus', handler_id: 'ts-error-handler' }
    const event = { ...eventRecord(), event_id: 'ts-error-event' }
    await core.registerBus(bus)
    await core.registerHandler(handler)
    await core.emitEvent(event, 'ts-error-bus')
    const invocation = (await core.processNextRoute('ts-error-bus')).find((message) => message.type === 'invoke_handler')!
    const messages = await core.errorHandler(invocation, new Error('ts boom'))
    const patch = messages.find(
      (message) => message.type === 'patch' && (message.patch as Record<string, unknown>).type === 'result_completed'
    )!.patch as { result: { status: string; error: { kind: string; message: string } } }

    assert.equal(patch.result.status, 'error')
    assert.equal(patch.result.error.kind, 'host_error')
    assert.equal(patch.result.error.message, 'ts boom')
  } finally {
    core.close()
  }
})
