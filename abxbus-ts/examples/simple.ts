#!/usr/bin/env -S node --import tsx
// Run: node --import tsx examples/simple.ts

import assert from 'node:assert/strict'
import { BaseEvent, EventBus } from '../src/index.js'
import { z } from 'zod'

// 1) Define typed events with BaseEvent.extend(...)
const RegisterUserEvent = BaseEvent.extend('RegisterUserEvent', {
  email: z.string().email(),
  plan: z.enum(['free', 'pro']),
  // Handler return values for this event are validated against this schema.
  event_result_type: z.object({
    user_id: z.string(),
    welcome_email_sent: z.boolean(),
  }),
})

const AuditEvent = BaseEvent.extend('AuditEvent', {
  message: z.string(),
})

async function main(): Promise<void> {
  const bus = new EventBus('SimpleExampleBus')

  // 2) Register a wildcard handler to observe every event flowing through this bus.
  bus.on('*', (event: BaseEvent) => {
    console.log(`[wildcard] ${event.event_type}#${event.event_id.slice(-8)}`)
  })

  // 3) Register by EventClass/factory (best type inference for payload + return type).
  bus.on(RegisterUserEvent, async (event) => {
    console.log(`[class handler] Creating account for ${event.email} (${event.plan})`)
    return {
      user_id: `user_${event.email.split('@')[0]}`,
      welcome_email_sent: true,
    }
  })

  // 4) Register by string event type (more dynamic, weaker compile-time checks).
  bus.on('AuditEvent', (event: InstanceType<typeof AuditEvent>) => {
    console.log(`[string handler] Audit log: ${event.message}`)
  })

  // 5) Intentionally return an invalid result shape.
  //    This compiles because string-based registration is best-effort, but will fail
  //    at runtime because RegisterUserEvent has event_result_type enforcement.
  bus.on('RegisterUserEvent', () => {
    return { user_id: 123, welcome_email_sent: 'yes' } as unknown
  })

  // Dispatch a simple event handled by a string registration.
  await bus.emit(AuditEvent({ message: 'Starting simple abxbus example' })).now()

  // Dispatch the typed event; one handler returns valid data, one returns invalid data.
  const register_event = bus.emit(
    RegisterUserEvent({
      email: 'ada@example.com',
      plan: 'pro',
    })
  )
  await register_event.now()
  const schema_error_result = Array.from(register_event.event_results.values()).find(
    (result) => result.status === 'error' && result.error instanceof Error && result.error.name === 'EventHandlerResultSchemaError'
  )
  const valid_result = Array.from(register_event.event_results.values()).find(
    (result) =>
      result.status === 'completed' &&
      typeof (result.result as { user_id?: unknown } | undefined)?.user_id === 'string' &&
      typeof (result.result as { welcome_email_sent?: unknown } | undefined)?.welcome_email_sent === 'boolean'
  )
  assert.ok(schema_error_result, 'expected one handler to fail event_result_type validation')
  assert.ok(valid_result, 'expected one handler to return a valid typed result')

  // 6) Inspect per-handler results (completed vs error) from event.event_results.
  console.log('\nRegisterUserEvent handler outcomes:')
  for (const result of register_event.event_results.values()) {
    if (result.status === 'completed') {
      console.log(`- ${result.handler_name}: completed -> ${JSON.stringify(result.result)}`)
      continue
    }
    if (result.status === 'error') {
      const message = result.error instanceof Error ? result.error.message : String(result.error)
      console.log(`- ${result.handler_name}: error -> ${message}`)
      console.log(`  raw invalid return: ${JSON.stringify(result.raw_value)}`)
      continue
    }
    console.log(`- ${result.handler_name}: ${result.status}`)
  }

  // 7) Aggregate inspection helpers.
  console.log('\nFirst valid parsed result:', await register_event.eventResult({ raise_if_any: false, raise_if_none: true }))
  console.log(`Total event errors: ${register_event.event_errors.length}`)
  for (const [index, error] of register_event.event_errors.entries()) {
    const message = error instanceof Error ? error.message : String(error)
    console.log(`  ${index + 1}. ${message}`)
  }

  await bus.waitUntilIdle()
  console.log('\n=== bus.logTree() ===')
  console.log(bus.logTree())
}

main().catch((error) => {
  console.error('Example failed:', error)
  process.exitCode = 1
})
