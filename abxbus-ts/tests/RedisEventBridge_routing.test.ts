import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { mkdtemp, rm } from 'node:fs/promises'
import { createServer } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { z } from 'zod'

import { BaseEvent, EventBus } from '../src/index.js'
import { RedisEventBridge } from '../src/RedisEventBridge.js'

test('Redis bridge bidirectional routing preserves routing without echoes', { timeout: 10000 }, async () => {
  const socket = createServer()
  socket.listen(0, '127.0.0.1')
  await once(socket, 'listening')
  const address = socket.address()
  assert(address && typeof address !== 'string')
  const port = address.port
  await new Promise<void>((resolve) => socket.close(() => resolve()))
  const directory = await mkdtemp(join(tmpdir(), 'abxbus-redis-routing-'))
  const redis = spawn('redis-server', [
    '--port',
    String(port),
    '--bind',
    '127.0.0.1',
    '--save',
    '',
    '--appendonly',
    'no',
    '--dir',
    directory,
  ])
  const bridges = [
    new RedisEventBridge(`redis://127.0.0.1:${port}/0/routing`, undefined, 'SameBridgeName'),
    new RedisEventBridge(`redis://127.0.0.1:${port}/0/routing`, undefined, 'SameBridgeName'),
  ]
  const relayOut = new RedisEventBridge(`redis://127.0.0.1:${port}/0/relay`)
  const relayIn = new RedisEventBridge(`redis://127.0.0.1:${port}/0/relay`)
  const relayed: BaseEvent[] = []
  const buses = [new EventBus(), new EventBus()]
  const seen: BaseEvent[][] = [[], []]
  let expectedCount = 0
  let received: (() => void) | undefined
  const checkReceived = () => {
    if (seen.every((events) => events.length === expectedCount) && relayed.length === expectedCount) received?.()
  }
  try {
    await new Promise<void>((resolve, reject) => {
      redis.once('error', reject)
      redis.once('exit', () => reject(new Error('Redis exited before ready')))
      redis.stdout.on('data', (data: Buffer) => {
        if (data.toString().includes('Ready to accept connections')) resolve()
      })
    })
    await relayOut.start()
    await relayIn.start()
    relayIn.on('*', async (event: BaseEvent) => {
      relayed.push(event)
      checkReceived()
    })
    buses[1].on('*', relayOut.emit)
    for (let i = 0; i < 2; i++) {
      await bridges[i].start()
      bridges[i].on('*', buses[i].emit)
      buses[i].on('*', bridges[i].emit)
      buses[i].on('*', async (event: BaseEvent) => {
        seen[i].push(event)
        checkReceived()
      })
    }
    const FrontendEvent = BaseEvent.extend('FrontendEvent', { message: z.string() })
    for (let sender = 0; sender < 2; sender++) {
      expectedCount = sender + 1
      const delivery = new Promise<void>((resolve) => {
        received = resolve
      })
      const event = FrontendEvent({ message: 'Hello from frontend' })
      await buses[sender].emit(event).wait({ timeout: 2 })
      let deadline: ReturnType<typeof setTimeout> | undefined
      try {
        await Promise.race([
          delivery,
          new Promise<never>((_, reject) => {
            deadline = setTimeout(() => reject(new Error('Redis routing delivery timeout')), 2000)
          }),
        ])
      } finally {
        clearTimeout(deadline)
      }
      // A quiet observation window catches self-delivery and cascading pub/sub echoes.
      await sleep(100)
      assert.deepEqual(
        seen.map((events) => events.length),
        [sender + 1, sender + 1]
      )
      assert.equal(relayed.length, sender + 1)
      assert.equal(relayed[sender].toJSON().message, 'Hello from frontend')
      assert.notEqual(seen[1 - sender][sender].event_id, event.event_id)
      assert.equal(seen[1 - sender][sender].toJSON().message, 'Hello from frontend')
      assert(event.event_results.size > 0)
      assert(event.event_path.every((entry) => !entry.startsWith('SameBridgeName')))
      assert.equal(seen[1 - sender][sender].event_path.filter((entry) => entry.startsWith('SameBridgeName')).length, 2)
    }
    for (const bus of buses) await bus.waitUntilIdle()
    for (const events of seen)
      for (const event of events) {
        for (const result of event.event_results.values()) assert.equal(result.error, undefined)
      }
  } finally {
    for (const bridge of [...bridges, relayOut, relayIn]) await bridge.close()
    for (const bus of buses) await bus.destroy()
    redis.kill()
    await once(redis, 'exit')
    await rm(directory, { recursive: true })
  }
})
