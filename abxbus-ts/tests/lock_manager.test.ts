import assert from 'node:assert/strict'
import { test } from 'node:test'

import { AsyncLock, HandlerLock, LockManager, runWithLock } from '../src/lock_manager.js'

const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

test('AsyncLock(1): releasing to a queued waiter does not allow a new acquire to slip in', async () => {
  const lock = new AsyncLock(1)

  await lock.acquire() // Initial holder.

  const waiter = lock.acquire()
  assert.equal(lock.waiters.length, 1)

  // Transfer the permit to the waiter.
  lock.release()

  // A new acquire in the same tick must wait behind the queued waiter.
  let contender_acquired = false
  const contender = lock.acquire().then(() => {
    contender_acquired = true
  })
  assert.equal(lock.waiters.length, 1)

  await waiter
  await Promise.resolve()
  assert.equal(contender_acquired, false)
  lock.release() // waiter release
  await contender
  lock.release() // contender release

  assert.equal(lock.in_use, 0)
})

test('AsyncLock(Infinity): acquire/release is a no-op bypass', async () => {
  const lock = new AsyncLock(Infinity)
  await Promise.all([lock.acquire(), lock.acquire(), lock.acquire()])
  assert.equal(lock.in_use, 0)
  assert.equal(lock.waiters.length, 0)
  lock.release()
  assert.equal(lock.in_use, 0)
  assert.equal(lock.waiters.length, 0)
})

test('AsyncLock(size>1): enforces semaphore concurrency limit', async () => {
  const lock = new AsyncLock(2)
  let active = 0
  let max_active = 0

  await Promise.all(
    Array.from({ length: 6 }, async () => {
      await lock.acquire()
      active += 1
      max_active = Math.max(max_active, active)
      await delay(5)
      active -= 1
      lock.release()
    })
  )

  assert.equal(max_active, 2)
  assert.equal(lock.in_use, 0)
  assert.equal(lock.waiters.length, 0)
})

test('runWithLock(null): executes function directly and preserves errors', async () => {
  let called = 0
  const value = await runWithLock(null, async () => {
    called += 1
    return 'ok'
  })
  assert.equal(value, 'ok')
  assert.equal(called, 1)

  await assert.rejects(
    runWithLock(null, async () => {
      throw new Error('boom')
    }),
    /boom/
  )
})

test('HandlerLock.reclaimHandlerLockIfRunning: releases reclaimed permit if handler exits while waiting', async () => {
  const lock = new AsyncLock(1)
  await lock.acquire()
  const handler_lock = new HandlerLock(lock)

  assert.equal(handler_lock.yieldHandlerLockForChildRun(), true)
  await lock.acquire() // Occupy lock so reclaim waits.

  const reclaim_promise = handler_lock.reclaimHandlerLockIfRunning()
  await Promise.resolve()
  assert.equal(lock.waiters.length, 1)

  handler_lock.exitHandlerRun() // Handler exits while reclaim is pending.
  lock.release() // Let pending reclaim continue.

  const reclaimed = await reclaim_promise
  assert.equal(reclaimed, false)
  assert.equal(lock.in_use, 0)
  assert.equal(lock.waiters.length, 0)
})

test('HandlerLock.runQueueJump: yields permit during child run and reacquires before returning', async () => {
  const lock = new AsyncLock(1)
  await lock.acquire()
  const handler_lock = new HandlerLock(lock)

  let contender_acquired = false
  let release_contender: (() => void) | null = null
  const contender_can_release = new Promise<void>((resolve) => {
    release_contender = resolve
  })
  const contender = (async () => {
    await lock.acquire()
    contender_acquired = true
    await contender_can_release
    lock.release()
  })()
  await Promise.resolve()
  assert.equal(lock.waiters.length, 1)

  const result = await handler_lock.runQueueJump(async () => {
    while (!contender_acquired) {
      await Promise.resolve()
    }
    release_contender?.()
    return 'child-ok'
  })

  assert.equal(result, 'child-ok')
  assert.equal(lock.in_use, 1, 'parent handler lock should be reacquired on return')
  handler_lock.exitHandlerRun()
  await contender
  assert.equal(lock.in_use, 0)
})

test('LockManager pause is re-entrant and resumes waiters only at depth zero', async () => {
  let idle = true
  const bus = {
    isIdleAndQueueEmpty: () => idle,
    event_concurrency: 'bus-serial' as const,
    _lock_for_event_global_serial: new AsyncLock(1),
  }
  const locks = new LockManager(bus)

  const release_a = locks._requestRunloopPause()
  const release_b = locks._requestRunloopPause()
  assert.equal(locks._isPaused(), true)

  let resumed = false
  const resumed_promise = locks._waitUntilRunloopResumed().then(() => {
    resumed = true
  })
  await Promise.resolve()
  assert.equal(resumed, false)

  release_a()
  await Promise.resolve()
  assert.equal(resumed, false)
  assert.equal(locks._isPaused(), true)

  release_b()
  await resumed_promise
  assert.equal(resumed, true)
  assert.equal(locks._isPaused(), false)

  release_a()
  release_b()
  idle = false
})

test('LockManager waitForIdle uses two-check stability and supports timeout', async () => {
  let idle = false
  const bus = {
    isIdleAndQueueEmpty: () => idle,
    event_concurrency: 'bus-serial' as const,
    _lock_for_event_global_serial: new AsyncLock(1),
  }
  const timeout_locks = new LockManager(bus)

  // Busy bus should timeout.
  const timeout_false = await timeout_locks.waitForIdle(0.01)
  assert.equal(timeout_false, false)

  const locks = new LockManager(bus, { auto_schedule_idle_checks: false })

  let resolved: boolean | null = null
  const idle_promise = locks.waitForIdle(0.2).then((value) => {
    resolved = value
  })

  idle = true
  locks._notifyIdleListeners() // first stable-idle tick; should not resolve synchronously
  assert.equal(resolved, null)
  await delay(0)
  assert.equal(resolved, null)
  locks._notifyIdleListeners() // second stable-idle tick; now resolve
  await idle_promise
  assert.equal(resolved, true)
})
