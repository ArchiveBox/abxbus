import assert from 'node:assert/strict'
import { test } from 'node:test'

import { BaseEvent, EventBus, EventResult } from '../src/index.js'
import { AsyncLock, HandlerLock, LockManager, runWithLock } from '../src/LockManager.js'

const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

type ActiveCounter = {
  active: number
  max_active: number
}

const register_active_handler = (bus: EventBus, event_type: string, counter: ActiveCounter): void => {
  bus.on(event_type, async () => {
    counter.active += 1
    counter.max_active = Math.max(counter.max_active, counter.active)
    await delay(20)
    counter.active -= 1
    return 'ok'
  })
}

const reset_counter = (counter: ActiveCounter): void => {
  counter.active = 0
  counter.max_active = 0
}

test('test_async_lock_1_releasing_to_a_queued_waiter_does_not_allow_a_new_acquire_to_slip_in', async () => {
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

test('test_async_lock_infinity_acquire_release_is_a_no_op_bypass', async () => {
  const lock = new AsyncLock(Infinity)
  await Promise.all([lock.acquire(), lock.acquire(), lock.acquire()])
  assert.equal(lock.in_use, 0)
  assert.equal(lock.waiters.length, 0)
  lock.release()
  assert.equal(lock.in_use, 0)
  assert.equal(lock.waiters.length, 0)
})

test('test_async_lock_size_greater_than_one_enforces_semaphore_concurrency_limit', async () => {
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

test('test_run_with_lock_null_executes_function_directly_and_preserves_errors', async () => {
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

test('test_handler_lock_reclaim_handler_lock_if_running_releases_reclaimed_permit_if_handler_exits_while_waiting', async () => {
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

test('test_handler_lock_run_queue_jump_yields_permit_during_child_run_and_reacquires_before_returning', async () => {
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

test('test_lock_manager_pause_is_reentrant_and_resumes_waiters_only_at_depth_zero', async () => {
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

test('test_lock_manager_wait_for_idle_uses_two_check_stability_and_supports_timeout', async () => {
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

test('test_wait_until_idle_behaves_correctly', async () => {
  const bus = new EventBus('IdleBus')
  const IdleEvent = BaseEvent.extend('LockManagerIdleEvent', {})
  let calls = 0

  bus.on(IdleEvent, async () => {
    calls += 1
    return 'ok'
  })

  assert.equal(await bus.waitUntilIdle(0.01), true)
  const event = bus.emit(IdleEvent({}))
  assert.equal(await bus.waitUntilIdle(1), true)
  assert.equal(calls, 1)
  assert.equal(event.event_status, 'completed')
  assert.equal(event.event_results.size, 1)
  for (const result of event.event_results.values()) {
    assert.equal(result.status, 'completed')
    assert.equal(result.result, 'ok')
  }

  await bus.destroy()
})

test('test_lock_manager_get_lock_for_event_modes', async () => {
  const bus = new EventBus('LockManagerEventModesBus', { event_concurrency: 'bus-serial' })
  const event = new BaseEvent({ event_type: 'LockModesEvent' })

  assert.equal(bus.locks.getLockForEvent(event), bus.locks.bus_event_lock)

  event.event_concurrency = 'global-serial'
  assert.equal(bus.locks.getLockForEvent(event), bus._lock_for_event_global_serial)

  event.event_concurrency = 'parallel'
  assert.equal(bus.locks.getLockForEvent(event), null)

  await bus.destroy()

  const counter = { active: 0, max_active: 0 }

  const bus_serial = new EventBus('LockManagerEventModesBusSerial', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'parallel',
  })
  register_active_handler(bus_serial, 'LockModesRuntimeEvent', counter)
  bus_serial.emit(new BaseEvent({ event_type: 'LockModesRuntimeEvent' }))
  bus_serial.emit(new BaseEvent({ event_type: 'LockModesRuntimeEvent' }))
  assert.equal(await bus_serial.waitUntilIdle(1), true)
  assert.equal(counter.max_active, 1)
  await bus_serial.destroy()

  reset_counter(counter)
  const parallel_override_bus = new EventBus('LockManagerEventModesParallelOverrideBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'parallel',
  })
  register_active_handler(parallel_override_bus, 'LockModesRuntimeEvent', counter)
  parallel_override_bus.emit(new BaseEvent({ event_type: 'LockModesRuntimeEvent', event_concurrency: 'parallel' }))
  parallel_override_bus.emit(new BaseEvent({ event_type: 'LockModesRuntimeEvent', event_concurrency: 'parallel' }))
  assert.equal(await parallel_override_bus.waitUntilIdle(1), true)
  assert.equal(counter.max_active, 2)
  await parallel_override_bus.destroy()

  reset_counter(counter)
  const bus_a = new EventBus('LockManagerGlobalEventModesBusA', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
  })
  const bus_b = new EventBus('LockManagerGlobalEventModesBusB', {
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
  })
  register_active_handler(bus_a, 'LockModesRuntimeEvent', counter)
  register_active_handler(bus_b, 'LockModesRuntimeEvent', counter)
  bus_a.emit(new BaseEvent({ event_type: 'LockModesRuntimeEvent', event_concurrency: 'global-serial' }))
  bus_b.emit(new BaseEvent({ event_type: 'LockModesRuntimeEvent', event_concurrency: 'global-serial' }))
  assert.equal(await bus_a.waitUntilIdle(1), true)
  assert.equal(await bus_b.waitUntilIdle(1), true)
  assert.equal(counter.max_active, 1)
  await bus_a.destroy()
  await bus_b.destroy()
})

test('test_lock_manager_get_lock_for_event_handler_modes', async () => {
  const bus = new EventBus('LockManagerHandlerModesBus', { event_handler_concurrency: 'serial' })
  const event = new BaseEvent({ event_type: 'LockHandlerModesEvent' })

  assert.equal(event._getHandlerLock('parallel'), null)
  await bus.locks._runWithHandlerLock(event, bus.event_handler_concurrency, async (handler_lock) => {
    assert.notEqual(handler_lock, null)
    assert.equal(event._getHandlerLock(bus.event_handler_concurrency)?.in_use, 1)
  })

  event.event_handler_concurrency = 'parallel'
  await bus.locks._runWithHandlerLock(event, bus.event_handler_concurrency, async (handler_lock) => {
    assert.equal(handler_lock, null)
  })

  await bus.destroy()

  const counter = { active: 0, max_active: 0 }
  const serial_bus = new EventBus('LockManagerHandlerModesSerialBus', { event_handler_concurrency: 'serial' })
  register_active_handler(serial_bus, 'LockHandlerModesRuntimeEvent', counter)
  register_active_handler(serial_bus, 'LockHandlerModesRuntimeEvent', counter)

  serial_bus.emit(new BaseEvent({ event_type: 'LockHandlerModesRuntimeEvent' }))
  assert.equal(await serial_bus.waitUntilIdle(1), true)
  assert.equal(counter.max_active, 1)
  await serial_bus.destroy()

  reset_counter(counter)
  const parallel_override_bus = new EventBus('LockManagerHandlerModesParallelOverrideBus', {
    event_handler_concurrency: 'serial',
  })
  register_active_handler(parallel_override_bus, 'LockHandlerModesRuntimeEvent', counter)
  register_active_handler(parallel_override_bus, 'LockHandlerModesRuntimeEvent', counter)

  parallel_override_bus.emit(new BaseEvent({ event_type: 'LockHandlerModesRuntimeEvent', event_handler_concurrency: 'parallel' }))
  assert.equal(await parallel_override_bus.waitUntilIdle(1), true)
  assert.equal(counter.max_active, 2)
  await parallel_override_bus.destroy()
})

test('test_run_with_event_lock_and_handler_lock_respect_parallel_bypass', async () => {
  const bus = new EventBus('LockManagerBypassBus', {
    event_concurrency: 'bus-serial',
    event_handler_concurrency: 'serial',
  })
  const parallel_event = new BaseEvent({
    event_type: 'ParallelBypassEvent',
    event_concurrency: 'parallel',
    event_handler_concurrency: 'parallel',
  })

  await bus.locks._runWithEventLock(parallel_event, async () => {
    assert.equal(bus.locks.bus_event_lock.in_use, 0)
  })

  await bus.locks._runWithHandlerLock(parallel_event, bus.event_handler_concurrency, async (handler_lock) => {
    assert.equal(handler_lock, null)
  })

  const serial_event = new BaseEvent({ event_type: 'SerialAcquireEvent' })
  await bus.locks._runWithEventLock(serial_event, async () => {
    assert.equal(bus.locks.bus_event_lock.in_use, 1)
  })

  await bus.locks._runWithHandlerLock(serial_event, bus.event_handler_concurrency, async (handler_lock) => {
    assert.notEqual(handler_lock, null)
    assert.equal(serial_event._getHandlerLock(bus.event_handler_concurrency)?.in_use, 1)
  })

  assert.equal(serial_event._getHandlerLock(bus.event_handler_concurrency)?.in_use, 0)
  await bus.destroy()
})

test('test_handler_dispatch_context_marks_and_restores_active_handler_result', async () => {
  const bus = new EventBus('LockDispatchContextBus')
  const DispatchContextEvent = BaseEvent.extend('DispatchContextEvent', {})
  const event = DispatchContextEvent({})
  const handler = bus.on(DispatchContextEvent, async () => 'ok')
  const result = new EventResult({ event, handler })
  result.status = 'started'

  assert.deepEqual(bus.locks._getActiveHandlerResults(), [])
  await bus.locks._runWithHandlerDispatchContext(result, async () => {
    assert.equal(bus.locks._getActiveHandlerResultForCurrentAsyncContext(), result)
    assert.deepEqual(bus.locks._getActiveHandlerResults(), [result])
  })

  assert.equal(bus.locks._getActiveHandlerResultForCurrentAsyncContext(), undefined)
  assert.deepEqual(bus.locks._getActiveHandlerResults(), [])
  await bus.destroy()
})

test('test_run_with_event_lock_releases_and_reraises_on_exception', async () => {
  const bus = new EventBus('LockManagerEventErrorBus', { event_concurrency: 'bus-serial' })
  const event = new BaseEvent({ event_type: 'EventLockErrorEvent' })
  const lock = bus.locks.getLockForEvent(event)

  assert.notEqual(lock, null)
  assert.equal(lock?.in_use, 0)

  await assert.rejects(
    bus.locks._runWithEventLock(event, async () => {
      assert.equal(lock?.in_use, 1)
      throw new Error('event-lock-error')
    }),
    /event-lock-error/
  )

  assert.equal(lock?.in_use, 0)
  await bus.destroy()
})

test('test_run_with_handler_lock_releases_and_reraises_on_exception', async () => {
  const bus = new EventBus('LockManagerHandlerErrorBus', { event_handler_concurrency: 'serial' })
  const event = new BaseEvent({ event_type: 'HandlerLockErrorEvent' })

  await assert.rejects(
    bus.locks._runWithHandlerLock(event, bus.event_handler_concurrency, async () => {
      const lock = event._getHandlerLock(bus.event_handler_concurrency)
      assert.notEqual(lock, null)
      assert.equal(lock?.in_use, 1)
      throw new Error('handler-lock-error')
    }),
    /handler-lock-error/
  )

  assert.equal(event._getHandlerLock(bus.event_handler_concurrency)?.in_use, 0)
  await bus.destroy()
})
