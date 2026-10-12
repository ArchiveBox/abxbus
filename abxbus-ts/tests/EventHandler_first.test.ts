import assert from 'node:assert/strict'
import { test } from 'node:test'
import { z } from 'zod'

import { BaseEvent, EventBus } from '../src/index.js'

const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

test('test_event_handler_completion_bus_default_first_serial', async () => {
  const bus = new EventBus('CompletionDefaultFirstBus', {
    event_timeout: 0,
    event_handler_concurrency: 'serial',
    event_handler_completion: 'first',
  })
  const CompletionEvent = BaseEvent.extend('CompletionDefaultFirstEvent', { event_result_type: z.string() })
  let second_handler_called = false

  async function first_handler() {
    return 'first'
  }

  async function second_handler() {
    second_handler_called = true
    return 'second'
  }

  bus.on(CompletionEvent, first_handler)
  bus.on(CompletionEvent, second_handler)

  try {
    const event = bus.emit(CompletionEvent({}))
    assert.equal(event.event_handler_completion, undefined)

    await event.now()
    assert.equal(event.event_handler_completion, undefined)
    assert.equal(second_handler_called, false)
    assert.equal(await event.eventResult({ raise_if_any: false, raise_if_none: false }), 'first')

    const first_result = Array.from(event.event_results.values()).find((result) => result.handler_name === 'first_handler')
    const second_result = Array.from(event.event_results.values()).find((result) => result.handler_name === 'second_handler')
    assert.equal(first_result?.status, 'completed')
    assert.equal(second_result?.status, 'error')
  } finally {
    await bus.destroy()
  }
})

test('test_event_handler_completion_explicit_override_beats_bus_default', async () => {
  const bus = new EventBus('CompletionOverrideBus', {
    event_timeout: 0,
    event_handler_concurrency: 'serial',
    event_handler_completion: 'first',
  })
  const CompletionEvent = BaseEvent.extend('CompletionOverrideEvent', { event_result_type: z.string() })
  let second_handler_called = false

  bus.on(CompletionEvent, async () => 'first')
  bus.on(CompletionEvent, async () => {
    second_handler_called = true
    return 'second'
  })

  try {
    const event = bus.emit(CompletionEvent({ event_handler_completion: 'all' }))
    assert.equal(event.event_handler_completion, 'all')
    await event.now()
    assert.equal(second_handler_called, true)
  } finally {
    await bus.destroy()
  }
})

test('test_event_parallel_first_races_and_cancels_non_winners', async () => {
  const bus = new EventBus('CompletionParallelFirstBus', {
    event_timeout: 0,
    event_handler_concurrency: 'serial',
    event_handler_completion: 'all',
  })
  const CompletionEvent = BaseEvent.extend('CompletionParallelFirstEvent', { event_result_type: z.string() })
  let slow_completed = false

  async function slow_handler_started() {
    await delay(500)
    slow_completed = true
    return 'slow-started'
  }

  async function fast_winner() {
    await delay(10)
    return 'winner'
  }

  async function slow_handler_pending_or_started() {
    await delay(500)
    slow_completed = true
    return 'slow-other'
  }

  bus.on(CompletionEvent, slow_handler_started)
  bus.on(CompletionEvent, fast_winner)
  bus.on(CompletionEvent, slow_handler_pending_or_started)

  try {
    const event = bus.emit(
      CompletionEvent({
        event_handler_concurrency: 'parallel',
        event_handler_completion: 'first',
      })
    )
    assert.equal(event.event_handler_concurrency, 'parallel')
    assert.equal(event.event_handler_completion, 'first')

    await event.now()
    assert.equal(slow_completed, false)

    const winner_result = Array.from(event.event_results.values()).find((result) => result.handler_name === 'fast_winner')
    assert.equal(winner_result?.status, 'completed')
    assert.equal(winner_result?.error, undefined)
    assert.equal(winner_result?.result, 'winner')

    const loser_results = Array.from(event.event_results.values()).filter((result) => result.handler_name !== 'fast_winner')
    assert.equal(loser_results.length, 2)
    assert.equal(
      loser_results.every((result) => result.status === 'error'),
      true
    )
    assert.equal(await event.eventResult({ raise_if_any: false, raise_if_none: true }), 'winner')
  } finally {
    await bus.destroy()
  }
})

test('test_event_handler_completion_explicit_first_cancels_parallel_losers', async () => {
  const bus = new EventBus('CompletionFirstShortcutBus', {
    event_timeout: 0,
    event_handler_concurrency: 'parallel',
    event_handler_completion: 'all',
  })
  const CompletionEvent = BaseEvent.extend('CompletionFirstShortcutEvent', { event_result_type: z.string() })
  let slow_handler_completed = false

  bus.on(CompletionEvent, async () => {
    await delay(10)
    return 'fast'
  })
  bus.on(CompletionEvent, async () => {
    await delay(500)
    slow_handler_completed = true
    return 'slow'
  })

  try {
    const event = bus.emit(CompletionEvent({ event_handler_completion: 'first' }))
    assert.equal(event.event_handler_completion, 'first')

    const completed_event = await event.now({ first_result: true })
    assert.equal(await completed_event.eventResult({ raise_if_any: false }), 'fast')
    assert.equal(event.event_handler_completion, 'first')
    await event.wait({ timeout: 1 })
    assert.equal(slow_handler_completed, false)
    assert.equal(
      Array.from(event.event_results.values()).some((result) => result.status === 'error'),
      true
    )
  } finally {
    await bus.destroy()
  }
})

test('test_event_handler_completion_first_preserves_falsy_results', async () => {
  const bus = new EventBus('CompletionFalsyBus', {
    event_timeout: 0,
    event_handler_concurrency: 'serial',
    event_handler_completion: 'all',
  })
  const IntCompletionEvent = BaseEvent.extend('IntCompletionEvent', { event_result_type: z.number() })
  let second_handler_called = false

  bus.on(IntCompletionEvent, async () => 0)
  bus.on(IntCompletionEvent, async () => {
    second_handler_called = true
    return 99
  })

  try {
    const event = bus.emit(IntCompletionEvent({ event_handler_completion: 'first' }))
    assert.equal(await (await event.now({ first_result: true })).eventResult({ raise_if_any: false }), 0)
    assert.equal(second_handler_called, false)
  } finally {
    await bus.destroy()
  }
})

test('test_event_handler_completion_first_preserves_false_and_empty_string_results', async () => {
  const bool_bus = new EventBus('CompletionFalsyFalseBus', {
    event_timeout: 0,
    event_handler_concurrency: 'serial',
    event_handler_completion: 'all',
  })
  const BoolCompletionEvent = BaseEvent.extend('BoolCompletionEvent', { event_result_type: z.boolean() })
  let bool_second_handler_called = false

  bool_bus.on(BoolCompletionEvent, async () => false)
  bool_bus.on(BoolCompletionEvent, async () => {
    bool_second_handler_called = true
    return true
  })

  try {
    const bool_event = bool_bus.emit(BoolCompletionEvent({ event_handler_completion: 'first' }))
    assert.equal(await (await bool_event.now({ first_result: true })).eventResult({ raise_if_any: false }), false)
    assert.equal(bool_second_handler_called, false)
  } finally {
    await bool_bus.destroy()
  }

  const str_bus = new EventBus('CompletionFalsyEmptyStringBus', {
    event_timeout: 0,
    event_handler_concurrency: 'serial',
    event_handler_completion: 'all',
  })
  const StrCompletionEvent = BaseEvent.extend('StrCompletionEvent', { event_result_type: z.string() })
  let str_second_handler_called = false

  str_bus.on(StrCompletionEvent, async () => '')
  str_bus.on(StrCompletionEvent, async () => {
    str_second_handler_called = true
    return 'second'
  })

  try {
    const str_event = str_bus.emit(StrCompletionEvent({ event_handler_completion: 'first' }))
    assert.equal(await (await str_event.now({ first_result: true })).eventResult({ raise_if_any: false }), '')
    assert.equal(str_second_handler_called, false)
  } finally {
    await str_bus.destroy()
  }
})

test('test_event_handler_completion_first_skips_none_result_and_uses_next_winner', async () => {
  const bus = new EventBus('CompletionNoneSkipBus', {
    event_timeout: 0,
    event_handler_concurrency: 'serial',
    event_handler_completion: 'all',
  })
  const CompletionEvent = BaseEvent.extend('CompletionNoneSkipEvent', {})
  let third_handler_called = false

  async function none_handler() {
    return null
  }

  async function winner_handler() {
    return 'winner'
  }

  async function third_handler() {
    third_handler_called = true
    return 'third'
  }

  bus.on(CompletionEvent, none_handler)
  bus.on(CompletionEvent, winner_handler)
  bus.on(CompletionEvent, third_handler)

  try {
    const event = bus.emit(CompletionEvent({ event_handler_completion: 'first' }))
    assert.equal(await (await event.now({ first_result: true })).eventResult({ raise_if_any: false }), 'winner')
    assert.equal(third_handler_called, false)

    const none_result = Array.from(event.event_results.values()).find((result) => result.handler_name === 'none_handler')
    const winner_result = Array.from(event.event_results.values()).find((result) => result.handler_name === 'winner_handler')
    assert.equal(none_result?.status, 'completed')
    assert.equal(none_result?.result, null)
    assert.equal(winner_result?.status, 'completed')
    assert.equal(winner_result?.result, 'winner')
  } finally {
    await bus.destroy()
  }
})

test('test_event_handler_completion_first_skips_baseevent_result_and_uses_next_winner', async () => {
  const bus = new EventBus('CompletionBaseEventSkipBus', {
    event_timeout: 0,
    event_handler_concurrency: 'serial',
    event_handler_completion: 'all',
  })
  const CompletionEvent = BaseEvent.extend('CompletionBaseEventSkipEvent', { event_result_type: z.string() })
  const ChildCompletionEvent = BaseEvent.extend('ChildCompletionEvent', { event_result_type: z.string() })
  let third_handler_called = false

  async function baseevent_handler() {
    return ChildCompletionEvent({})
  }

  async function winner_handler() {
    return 'winner'
  }

  async function third_handler() {
    third_handler_called = true
    return 'third'
  }

  bus.on(CompletionEvent, baseevent_handler)
  bus.on(CompletionEvent, winner_handler)
  bus.on(CompletionEvent, third_handler)

  try {
    const event = bus.emit(CompletionEvent({ event_handler_completion: 'first' }))
    assert.equal(await (await event.now({ first_result: true })).eventResult({ raise_if_any: false }), 'winner')
    assert.equal(third_handler_called, false)

    const baseevent_result = Array.from(event.event_results.values()).find((result) => result.handler_name === 'baseevent_handler')
    assert.equal((baseevent_result?.result as unknown) instanceof BaseEvent, true)
  } finally {
    await bus.destroy()
  }
})

test('test_now_runs_all_handlers_and_event_result_returns_first_valid_result', async () => {
  const bus = new EventBus('CompletionNowAllBus', {
    event_timeout: 0,
    event_handler_concurrency: 'serial',
    event_handler_completion: 'first',
  })
  const CompletionEvent = BaseEvent.extend('CompletionNowAllEvent', { event_result_type: z.string() })
  const ChildCompletionEvent = BaseEvent.extend('CompletionNowAllChildEvent', {})
  let late_handler_called = false

  bus.on(CompletionEvent, async () => ChildCompletionEvent({}))
  bus.on(CompletionEvent, async () => null)
  bus.on(CompletionEvent, async () => 'winner')
  bus.on(CompletionEvent, async () => {
    late_handler_called = true
    return 'late'
  })

  try {
    const event = await bus.emit(CompletionEvent({ event_handler_completion: 'all' })).now()
    assert.equal(await event.eventResult({ raise_if_any: false }), 'winner')
    assert.equal(event.event_handler_completion, 'all')
    assert.equal(late_handler_called, true)
  } finally {
    await bus.destroy()
  }
})

test('test_event_now_default_error_policy', async () => {
  const no_handler_bus = new EventBus('CompletionNowNoHandlerBus', { event_timeout: 0 })
  const CompletionEvent = BaseEvent.extend('CompletionNowErrorPolicyEvent', { event_result_type: z.string() })

  try {
    const event = await no_handler_bus.emit(CompletionEvent({})).now()
    assert.equal(await event.eventResult({ raise_if_any: false, raise_if_none: false }), undefined)
  } finally {
    await no_handler_bus.destroy()
  }

  const none_bus = new EventBus('CompletionNowNoneBus', { event_timeout: 0 })
  none_bus.on(CompletionEvent, async () => null)
  try {
    const event = await none_bus.emit(CompletionEvent({})).now()
    assert.equal(await event.eventResult({ raise_if_any: false, raise_if_none: false }), undefined)
  } finally {
    await none_bus.destroy()
  }

  const all_error_bus = new EventBus('CompletionNowAllErrorBus', { event_timeout: 0 })
  all_error_bus.on(CompletionEvent, async () => {
    throw new Error('now boom 1')
  })
  all_error_bus.on(CompletionEvent, async () => {
    throw new Error('now boom 2')
  })
  try {
    const event = await all_error_bus.emit(CompletionEvent({})).now()
    await assert.rejects(() => event.eventResult(), AggregateError)
  } finally {
    await all_error_bus.destroy()
  }

  const mixed_valid_bus = new EventBus('CompletionNowMixedValidBus', { event_timeout: 0 })
  mixed_valid_bus.on(CompletionEvent, async () => {
    throw new Error('now boom 1')
  })
  mixed_valid_bus.on(CompletionEvent, async () => 'winner')
  try {
    const event = await mixed_valid_bus.emit(CompletionEvent({})).now()
    assert.equal(await event.eventResult({ raise_if_any: false }), 'winner')
  } finally {
    await mixed_valid_bus.destroy()
  }

  const mixed_none_bus = new EventBus('CompletionNowMixedNoneBus', { event_timeout: 0 })
  mixed_none_bus.on(CompletionEvent, async () => {
    throw new Error('now boom 1')
  })
  mixed_none_bus.on(CompletionEvent, async () => null)
  try {
    const event = await mixed_none_bus.emit(CompletionEvent({})).now()
    assert.equal(await event.eventResult({ raise_if_any: false, raise_if_none: false }), undefined)
  } finally {
    await mixed_none_bus.destroy()
  }
})

test('test_event_result_options_match_event_results_shape', async () => {
  const bus = new EventBus('CompletionNowOptionsBus', { event_timeout: 0, event_handler_concurrency: 'serial' })
  const CompletionEvent = BaseEvent.extend('CompletionNowOptionsEvent', { event_result_type: z.string() })

  bus.on(CompletionEvent, async () => {
    throw new Error('now option boom')
  })
  bus.on(CompletionEvent, async () => 'first')
  bus.on(CompletionEvent, async () => 'second')

  try {
    const event = await bus.emit(CompletionEvent({})).now()
    await assert.rejects(() => event.eventResult({ raise_if_any: true }), /now option boom/)

    const filtered_event = await bus.emit(CompletionEvent({})).now()
    assert.equal(
      await filtered_event.eventResult({
        raise_if_any: false,
        include: (result, event_result) => {
          assert.equal(result, event_result.result)
          return event_result.status === 'completed' && event_result.result === 'second'
        },
      }),
      'second'
    )
  } finally {
    await bus.destroy()
  }
})

test('test_event_result_returns_first_valid_result_by_registration_order_after_now', async () => {
  const bus = new EventBus('CompletionNowRegistrationOrderBus', { event_timeout: 0, event_handler_concurrency: 'parallel' })
  const CompletionEvent = BaseEvent.extend('CompletionNowRegistrationOrderEvent', { event_result_type: z.string() })

  bus.on(CompletionEvent, async () => {
    await delay(50)
    return 'slow'
  })
  bus.on(CompletionEvent, async () => {
    await delay(1)
    return 'fast'
  })

  try {
    const event = await bus.emit(CompletionEvent({})).now()
    assert.equal(await event.eventResult(), 'slow')
  } finally {
    await bus.destroy()
  }
})

test('test_event_handler_completion_first_returns_none_when_all_handlers_fail', async () => {
  const bus = new EventBus('CompletionAllFailBus', { event_timeout: 0, event_handler_concurrency: 'parallel' })
  const CompletionEvent = BaseEvent.extend('CompletionAllFailEvent', { event_result_type: z.string() })

  bus.on(CompletionEvent, async () => {
    throw new Error('boom1')
  })
  bus.on(CompletionEvent, async () => {
    await delay(10)
    throw new Error('boom2')
  })

  try {
    const event = await bus.emit(CompletionEvent({ event_handler_completion: 'first' })).now({ first_result: true })
    assert.equal(await event.eventResult({ raise_if_any: false, raise_if_none: false }), undefined)
  } finally {
    await bus.destroy()
  }
})

test('test_event_handler_completion_first_result_options_match_event_result_options', async () => {
  const bus = new EventBus('CompletionFirstOptionsBus', { event_timeout: 0, event_handler_concurrency: 'parallel' })
  const CompletionEvent = BaseEvent.extend('CompletionFirstOptionsEvent', { event_result_type: z.string() })

  bus.on(CompletionEvent, async () => {
    throw new Error('first option boom')
  })
  bus.on(CompletionEvent, async () => {
    await delay(10)
    return 'winner'
  })

  try {
    const event = await bus.emit(CompletionEvent({ event_handler_completion: 'first' })).now({ first_result: true })
    assert.equal(await event.eventResult({ raise_if_any: false }), 'winner')

    const error_event = await bus.emit(CompletionEvent({ event_handler_completion: 'first' })).now({ first_result: true })
    await assert.rejects(() => error_event.eventResult({ raise_if_any: true }), /first option boom/)
  } finally {
    await bus.destroy()
  }

  const none_bus = new EventBus('CompletionFirstRaiseNoneBus', { event_timeout: 0 })
  const NoneCompletionEvent = BaseEvent.extend('CompletionFirstRaiseNoneEvent', {})
  none_bus.on(NoneCompletionEvent, async () => null)
  try {
    const event = await none_bus.emit(NoneCompletionEvent({ event_handler_completion: 'first' })).now({ first_result: true })
    await assert.rejects(() => event.eventResult({ raise_if_none: true }), /Expected at least one handler/)
  } finally {
    await none_bus.destroy()
  }
})

test('test_now_first_result_timeout_limits_processing_wait', async () => {
  const bus = new EventBus('CompletionFirstTimeoutBus', { event_timeout: 0, event_handler_concurrency: 'serial' })
  const CompletionEvent = BaseEvent.extend('CompletionFirstTimeoutEvent', { event_result_type: z.string() })

  bus.on(CompletionEvent, async () => {
    await delay(500)
    return 'slow'
  })

  try {
    await assert.rejects(
      () =>
        bus.emit(CompletionEvent({ event_handler_completion: 'first' })).now({
          timeout: 0.01,
          first_result: true,
        }),
      /Timed out/
    )
  } finally {
    await bus.destroy()
  }
})

test('test_event_result_include_callback_receives_result_and_event_result', async () => {
  const bus = new EventBus('CompletionFirstIncludeBus', { event_timeout: 0, event_handler_concurrency: 'serial' })
  const CompletionEvent = BaseEvent.extend('CompletionFirstIncludeEvent', { event_result_type: z.string() })
  const seen_handler_names: Array<string | null> = []
  const seen_results: Array<string | undefined> = []

  async function none_handler() {
    return null
  }

  async function second_handler() {
    return 'second'
  }

  bus.on(CompletionEvent, none_handler)
  bus.on(CompletionEvent, second_handler)

  try {
    const event = await bus.emit(CompletionEvent({ event_handler_completion: 'first' })).now({ first_result: true })
    assert.equal(
      await event.eventResult({
        raise_if_any: false,
        include: (result, event_result) => {
          assert.equal(result, event_result.result)
          seen_results.push(result)
          seen_handler_names.push(event_result.handler_name)
          return event_result.status === 'completed' && result === 'second'
        },
      }),
      'second'
    )
    assert.deepEqual(seen_results.slice(-2), [undefined, 'second'])
    assert.deepEqual(seen_handler_names.slice(-2), ['none_handler', 'second_handler'])
  } finally {
    await bus.destroy()
  }
})

test('test_event_results_include_callback_receives_result_and_event_result', async () => {
  const bus = new EventBus('CompletionResultsListIncludeBus', { event_timeout: 0, event_handler_concurrency: 'serial' })
  const CompletionEvent = BaseEvent.extend('CompletionResultsListIncludeEvent', { event_result_type: z.string() })
  const seen_pairs: Array<[string | undefined, string | null]> = []

  async function keep_handler() {
    return 'keep'
  }

  async function drop_handler() {
    return 'drop'
  }

  bus.on(CompletionEvent, keep_handler)
  bus.on(CompletionEvent, drop_handler)

  try {
    const event = await bus.emit(CompletionEvent({})).now()
    assert.deepEqual(
      await event.eventResultsList({
        raise_if_any: false,
        raise_if_none: true,
        include: (result, event_result) => {
          assert.equal(result, event_result.result)
          seen_pairs.push([result, event_result.handler_name])
          return result === 'keep'
        },
      }),
      ['keep']
    )
    assert.deepEqual(seen_pairs, [
      ['keep', 'keep_handler'],
      ['drop', 'drop_handler'],
    ])
  } finally {
    await bus.destroy()
  }
})

test('test_event_result_returns_first_current_result_with_first_result_wait', async () => {
  const bus = new EventBus('CompletionFirstCurrentResultBus', { event_timeout: 0, event_handler_concurrency: 'parallel' })
  const CompletionEvent = BaseEvent.extend('CompletionFirstCurrentResultEvent', { event_result_type: z.string() })

  bus.on(CompletionEvent, async () => {
    await delay(50)
    return 'slow'
  })
  bus.on(CompletionEvent, async () => {
    await delay(1)
    return 'fast'
  })

  try {
    const event = await bus.emit(CompletionEvent({})).now({ first_result: true })
    assert.equal(await event.eventResult(), 'fast')
  } finally {
    await bus.destroy()
  }
})

test('test_event_result_raise_if_any_includes_first_mode_control_errors', async () => {
  const bus = new EventBus('CompletionFirstControlErrorBus', {
    event_timeout: 0,
    event_handler_concurrency: 'parallel',
    event_handler_completion: 'all',
  })
  const CompletionEvent = BaseEvent.extend('CompletionFirstControlErrorEvent', { event_result_type: z.string() })

  bus.on(CompletionEvent, async () => {
    await delay(10)
    return 'fast'
  })
  bus.on(CompletionEvent, async () => {
    await delay(500)
    return 'slow'
  })

  try {
    const event = await bus.emit(CompletionEvent({ event_handler_completion: 'first' })).now({ first_result: true })
    await assert.rejects(() => event.eventResult({ raise_if_any: true }), /first.*resolved|aborted/i)
    assert.equal(await event.eventResult({ raise_if_any: false, raise_if_none: true }), 'fast')
  } finally {
    await bus.destroy()
  }
})
