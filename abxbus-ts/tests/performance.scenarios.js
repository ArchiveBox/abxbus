const defaultNow = () => performance.now()
const defaultSleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

const assert = (condition, message) => {
  if (!condition) {
    throw new Error(message)
  }
}

const mb = (bytes) => (bytes / 1024 / 1024).toFixed(1)
const kb = (bytes) => bytes / 1024
const clampNonNegative = (value) => (value < 0 ? 0 : value)
const formatMsPerEvent = (value, unit = 'event') => `${value.toFixed(3)}ms/${unit}`
const formatKbPerEvent = (value) => `${value.toFixed(3)}kb/event`
const formatMs = (value) => `${value.toFixed(3)}ms`
const formatMb = (value) => `${value.toFixed(3)}mb`

const HISTORY_LIMIT_STREAM = 512
const HISTORY_LIMIT_ON_OFF = 128
const HISTORY_LIMIT_EPHEMERAL_BUS = 128
const HISTORY_LIMIT_FIXED_HANDLERS = 128
const HISTORY_LIMIT_WORST_CASE = 128
const TRIM_TARGET = 1
const WORST_CASE_IMMEDIATE_TIMEOUT_MS = 0.0001
const WORST_CASE_IMMEDIATE_TIMEOUT_SECONDS = WORST_CASE_IMMEDIATE_TIMEOUT_MS / 1000

const heapDeltaNoiseFloorMb = (runtimeName) => {
  if (runtimeName === 'bun') return 192.0
  if (runtimeName === 'deno') return 4.5
  return 3.0
}

const measureMemory = (hooks) => {
  if (typeof hooks.getMemoryUsage !== 'function') {
    return null
  }
  return hooks.getMemoryUsage()
}

const maybeForceGc = (hooks) => {
  if (typeof hooks.forceGc === 'function') {
    hooks.forceGc()
  }
}

const waitForRuntimeSettle = async (hooks) => {
  // Let normal runtime scheduling/GC progress naturally without explicit GC forcing.
  await hooks.sleep(50)
}

const measureStableHeapUsed = async (hooks, mode = 'max', rounds = 12) => {
  const heaps = []
  for (let i = 0; i < rounds; i += 1) {
    maybeForceGc(hooks)
    await hooks.sleep(12)
    const mem = measureMemory(hooks)
    if (mem) heaps.push(mem.heapUsed)
  }
  if (heaps.length === 0) return null
  return mode === 'min' ? Math.min(...heaps) : Math.max(...heaps)
}

const measureHeapDeltaAfterGc = async (hooks, baselineHeapUsed) => {
  if (baselineHeapUsed === null || baselineHeapUsed === undefined) return null
  await hooks.sleep(120)
  const endHeapUsed = await measureStableHeapUsed(hooks, 'min', 24)
  if (endHeapUsed === null) return null
  return (endHeapUsed - baselineHeapUsed) / 1024 / 1024
}

const trimBusHistoryToOneEvent = async (hooks, bus, TrimEvent) => {
  bus.event_history.max_history_size = TRIM_TARGET
  bus.event_history.max_history_drop = true
  let trimEvent = bus.emit(TrimEvent({}))
  await trimEvent.done()
  trimEvent = null
  await bus.waitUntilIdle()
  assert(bus.event_history.size <= TRIM_TARGET, `trim-to-1 failed for ${bus.toString()}: ${bus.event_history.size}/${TRIM_TARGET}`)
}

const waitForRegistrySize = async (hooks, EventBus, expectedSize, attempts = 150) => {
  for (let i = 0; i < attempts; i += 1) {
    await hooks.sleep(40)
    if (EventBus.all_instances.size <= expectedSize) {
      return true
    }
  }
  return EventBus.all_instances.size <= expectedSize
}

const runCleanupBurst = async ({ hooks, EventBus, CleanupEvent, TrimEvent, busesPerMode, eventsPerBus, destroyMode }) => {
  for (let i = 0; i < busesPerMode; i += 1) {
    let bus = new EventBus(`CleanupEq-${destroyMode ? 'destroy' : 'scope'}-${i}`, {
      max_history_size: HISTORY_LIMIT_EPHEMERAL_BUS,
      max_history_drop: true,
    })
    bus.on(CleanupEvent, () => {})

    const pending = []
    for (let e = 0; e < eventsPerBus; e += 1) {
      // Store completion promises (not event proxies) to avoid retaining bus-bound proxies across GC checks.
      pending.push(
        bus
          .emit(CleanupEvent({}))
          .done()
          .then(() => undefined)
      )
    }
    await Promise.all(pending)
    pending.length = 0
    await bus.waitUntilIdle()
    await trimBusHistoryToOneEvent(hooks, bus, TrimEvent)

    if (destroyMode) {
      bus.destroy()
    }
    bus = null
  }
}

const runWarmup = async (input) => {
  const hooks = withDefaults(input)
  const { BaseEvent, EventBus } = hooks.api
  const { PerfWarmupEvent: WarmEvent, PerfWarmupTrimEvent: WarmTrimEvent } = getEventClasses(BaseEvent)

  const bus = new EventBus('PerfWarmupBus', { max_history_size: 512, max_history_drop: true })
  bus.on(WarmEvent, () => {})

  for (let i = 0; i < 2048; i += 256) {
    const pending = []
    for (let j = 0; j < 256; j += 1) {
      pending.push(bus.emit(WarmEvent({})))
    }
    await Promise.all(pending.map((event) => event.done()))
    await bus.waitUntilIdle()
  }

  await trimBusHistoryToOneEvent(hooks, bus, WarmTrimEvent)
  bus.destroy()
  await waitForRuntimeSettle(hooks)
}

const createMemoryTracker = (hooks) => {
  const baselineRaw = measureMemory(hooks)
  if (!baselineRaw) {
    return {
      baseline: null,
      current: null,
      sample: () => null,
      peakHeapKbPerEvent: () => null,
      peakRssKbPerEvent: () => null,
    }
  }

  const baseline = { rss: baselineRaw.rss, heapUsed: baselineRaw.heapUsed }
  let current = baselineRaw
  let peakHeapUsed = baselineRaw.heapUsed
  let peakRss = baselineRaw.rss

  const sample = () => {
    const snapshot = measureMemory(hooks)
    if (!snapshot) return null
    current = snapshot
    if (snapshot.heapUsed > peakHeapUsed) peakHeapUsed = snapshot.heapUsed
    if (snapshot.rss > peakRss) peakRss = snapshot.rss
    return snapshot
  }

  const peakHeapKbPerEvent = (events) => {
    if (!events || !baseline) return null
    const deltaBytes = clampNonNegative(peakHeapUsed - baseline.heapUsed)
    return kb(deltaBytes) / events
  }

  const peakRssKbPerEvent = (events) => {
    if (!events || !baseline) return null
    const deltaBytes = clampNonNegative(peakRss - baseline.rss)
    return kb(deltaBytes) / events
  }

  return { baseline, current: () => current, sample, peakHeapKbPerEvent, peakRssKbPerEvent }
}

const record = (hooks, name, metrics) => {
  if (typeof hooks.log === 'function') {
    const perEventOnly = name === 'worst-case forwarding + timeouts'
    const parts = []
    if (!perEventOnly && typeof metrics.totalEvents === 'number') parts.push(`events=${metrics.totalEvents}`)
    if (!perEventOnly && typeof metrics.totalMs === 'number') parts.push(`total=${formatMs(metrics.totalMs)}`)
    if (typeof metrics.msPerEvent === 'number')
      parts.push(`latency=${formatMsPerEvent(metrics.msPerEvent, metrics.msPerEventUnit ?? 'event')}`)
    if (typeof metrics.peakHeapKbPerEvent === 'number') parts.push(`peak_heap=${formatKbPerEvent(metrics.peakHeapKbPerEvent)}`)
    if (typeof metrics.peakRssKbPerEvent === 'number') parts.push(`peak_rss=${formatKbPerEvent(metrics.peakRssKbPerEvent)}`)
    if (
      typeof metrics.ramKbPerEvent === 'number' &&
      typeof metrics.peakHeapKbPerEvent !== 'number' &&
      typeof metrics.peakRssKbPerEvent !== 'number'
    ) {
      parts.push(`ram=${formatKbPerEvent(metrics.ramKbPerEvent)}`)
    }
    if (typeof metrics.throughput === 'number') parts.push(`throughput=${metrics.throughput}/s`)
    if (typeof metrics.equivalent === 'boolean') parts.push(`equivalent=${metrics.equivalent ? 'yes' : 'no'}`)
    if (typeof metrics.timeoutCount === 'number') parts.push(`timeouts=${metrics.timeoutCount}`)
    if (typeof metrics.cancelCount === 'number') parts.push(`cancels=${metrics.cancelCount}`)
    if (typeof metrics.heapDeltaAfterGcMb === 'number') parts.push(`heap_delta_after_gc=${formatMb(metrics.heapDeltaAfterGcMb)}`)
    hooks.log(`[${hooks.runtimeName}] ${name}: ${parts.join(' ')}`)
  }
}

const withDefaults = (input) => {
  const hooks = {
    runtimeName: input.runtimeName ?? 'runtime',
    now: input.now ?? defaultNow,
    sleep: input.sleep ?? defaultSleep,
    log: input.log ?? (() => {}),
    getMemoryUsage: input.getMemoryUsage,
    forceGc: input.forceGc,
    limits: {
      singleRunMs: input.limits?.singleRunMs ?? 90_000,
      worstCaseMs: input.limits?.worstCaseMs ?? 180_000,
      maxHeapDeltaAfterGcMb: input.limits?.maxHeapDeltaAfterGcMb ?? null,
      heapDeltaNoiseFloorMb: input.limits?.heapDeltaNoiseFloorMb ?? heapDeltaNoiseFloorMb(input.runtimeName ?? 'runtime'),
    },
    api: input.api,
  }
  return hooks
}

const eventClassCache = new WeakMap()

const getEventClasses = (BaseEvent) => {
  const cached = eventClassCache.get(BaseEvent)
  if (cached) return cached

  const classes = {
    PerfSimpleEvent: BaseEvent.extend('PerfSimpleEvent', {}),
    PerfTrimEvent: BaseEvent.extend('PerfTrimEvent', {}),
    PerfTrimEventEphemeral: BaseEvent.extend('PerfTrimEventEphemeral', {}),
    PerfRequestEvent: BaseEvent.extend('PerfRequestEvent', {}),
    PerfTrimEventOnOff: BaseEvent.extend('PerfTrimEventOnOff', {}),
    PerfFixedHandlersEvent: BaseEvent.extend('PerfFixedHandlersEvent', {}),
    PerfTrimEventFixedHandlers: BaseEvent.extend('PerfTrimEventFixedHandlers', {}),
    WCParent: BaseEvent.extend('WCParent', {}),
    WCChild: BaseEvent.extend('WCChild', {}),
    WCGrandchild: BaseEvent.extend('WCGrandchild', {}),
    WCTrimEvent: BaseEvent.extend('WCTrimEvent', {}),
    CleanupEqEvent: BaseEvent.extend('CleanupEqEvent', {}),
    CleanupEqTrimEvent: BaseEvent.extend('CleanupEqTrimEvent', {}),
    PerfWarmupEvent: BaseEvent.extend('PerfWarmupEvent', {}),
    PerfWarmupTrimEvent: BaseEvent.extend('PerfWarmupTrimEvent', {}),
  }
  eventClassCache.set(BaseEvent, classes)
  return classes
}

export const runPerf50kEvents = async (input) => {
  const hooks = withDefaults(input)
  const { BaseEvent, EventBus } = hooks.api
  const totalEvents = 50_000
  const batchSize = 512
  const { PerfSimpleEvent: SimpleEvent, PerfTrimEvent: TrimEvent } = getEventClasses(BaseEvent)
  const bus = new EventBus('PerfBus', { max_history_size: HISTORY_LIMIT_STREAM, max_history_drop: true })

  let processedCount = 0
  const sampledEarlyEvents = []
  bus.on(SimpleEvent, () => {
    processedCount += 1
  })

  const memory = createMemoryTracker(hooks)
  const t0 = hooks.now()

  let dispatched = 0
  while (dispatched < totalEvents) {
    const pending = []
    const thisBatch = Math.min(batchSize, totalEvents - dispatched)
    for (let i = 0; i < thisBatch; i += 1) {
      const dispatchedEvent = bus.emit(SimpleEvent({}))
      pending.push(dispatchedEvent)
      if (sampledEarlyEvents.length < 64) {
        const original = dispatchedEvent._event_original ?? dispatchedEvent
        sampledEarlyEvents.push(original)
      }
      dispatched += 1
    }

    await Promise.all(pending.map((event) => event.done()))
    await bus.waitUntilIdle()
    if (dispatched % 2048 === 0) memory.sample()
  }

  const tDispatch = hooks.now()
  memory.sample()

  await trimBusHistoryToOneEvent(hooks, bus, TrimEvent)
  const tDone = hooks.now()
  await waitForRuntimeSettle(hooks)
  memory.sample()
  const memDone = measureMemory(hooks)

  const dispatchMs = tDispatch - t0
  const awaitMs = tDone - tDispatch
  const totalMs = tDone - t0
  const msPerEvent = totalMs / totalEvents
  const peakHeapKbPerEvent = memory.peakHeapKbPerEvent(totalEvents)
  const peakRssKbPerEvent = memory.peakRssKbPerEvent(totalEvents)

  assert(processedCount === totalEvents, `50k events processed ${processedCount}/${totalEvents}`)
  assert(totalMs < hooks.limits.singleRunMs, `50k events took ${Math.round(totalMs)}ms (limit ${hooks.limits.singleRunMs}ms)`)
  assert(
    bus.event_history.size <= bus.event_history.max_history_size,
    `50k events history exceeded limit: ${bus.event_history.size}/${bus.event_history.max_history_size}`
  )

  assert(sampledEarlyEvents.length > 0, 'expected sampled early events to be captured')

  let sampledEvictedCount = 0
  for (const event of sampledEarlyEvents) {
    const isStillInHistory = bus.event_history.has(event.event_id)
    assert(!isStillInHistory, `expected sampled early event to be evicted from history: ${event.event_id}`)
    sampledEvictedCount += 1
    assert(event.event_results.size === 0, `trimmed event still has event_results: ${event.event_id} (${event.event_results.size})`)
    assert(event.bus === undefined, `trimmed event still has bus reference: ${event.event_id}`)
  }
  assert(
    sampledEvictedCount === sampledEarlyEvents.length,
    `expected all sampled events to be evicted: ${sampledEvictedCount}/${sampledEarlyEvents.length}`
  )

  const result = {
    scenario: '50k events',
    totalEvents,
    totalMs,
    dispatchMs,
    awaitMs,
    msPerEvent,
    msPerEventLabel: formatMsPerEvent(msPerEvent),
    ramKbPerEvent: peakHeapKbPerEvent,
    ramKbPerEventLabel: peakHeapKbPerEvent === null ? null : formatKbPerEvent(peakHeapKbPerEvent),
    peakHeapKbPerEvent,
    peakHeapKbPerEventLabel: peakHeapKbPerEvent === null ? null : formatKbPerEvent(peakHeapKbPerEvent),
    peakRssKbPerEvent,
    peakRssKbPerEventLabel: peakRssKbPerEvent === null ? null : formatKbPerEvent(peakRssKbPerEvent),
    throughput: Math.round(totalEvents / (totalMs / 1000)),
    processedCount,
    sampledEvictedCount,
  }

  if (memory.baseline && memDone) {
    result.heapBeforeMb = Number(mb(memory.baseline.heapUsed))
    result.heapDoneMb = Number(mb(memDone.heapUsed))
  }

  bus.destroy()
  record(hooks, result.scenario, result)
  return result
}

export const runPerfEphemeralBuses = async (input) => {
  const hooks = withDefaults(input)
  const { BaseEvent, EventBus } = hooks.api
  const totalBuses = 500
  const eventsPerBus = 100
  const totalEvents = totalBuses * eventsPerBus
  const { PerfSimpleEvent: SimpleEvent, PerfTrimEventEphemeral: TrimEvent } = getEventClasses(BaseEvent)

  let processedCount = 0
  const memory = createMemoryTracker(hooks)
  const t0 = hooks.now()

  for (let b = 0; b < totalBuses; b += 1) {
    const bus = new EventBus(`ReqBus-${b}`, { max_history_size: HISTORY_LIMIT_EPHEMERAL_BUS, max_history_drop: true })
    bus.on(SimpleEvent, () => {
      processedCount += 1
    })

    const pending = []
    for (let i = 0; i < eventsPerBus; i += 1) {
      pending.push(bus.emit(SimpleEvent({})))
    }

    await Promise.all(pending.map((event) => event.done()))
    await bus.waitUntilIdle()
    await trimBusHistoryToOneEvent(hooks, bus, TrimEvent)
    bus.destroy()
    if (b % 10 === 0) memory.sample()
  }

  const totalMs = hooks.now() - t0
  await waitForRuntimeSettle(hooks)
  memory.sample()
  const msPerEvent = totalMs / totalEvents
  const peakHeapKbPerEvent = memory.peakHeapKbPerEvent(totalEvents)
  const peakRssKbPerEvent = memory.peakRssKbPerEvent(totalEvents)

  assert(processedCount === totalEvents, `500x100 buses processed ${processedCount}/${totalEvents}`)
  assert(totalMs < hooks.limits.singleRunMs, `500x100 buses took ${Math.round(totalMs)}ms (limit ${hooks.limits.singleRunMs}ms)`)
  assert(EventBus.all_instances.size === 0, `500x100 buses leaked instances: ${EventBus.all_instances.size}`)

  const result = {
    scenario: '500 buses x 100 events',
    totalEvents,
    totalMs,
    msPerEvent,
    msPerEventLabel: formatMsPerEvent(msPerEvent),
    ramKbPerEvent: peakHeapKbPerEvent,
    ramKbPerEventLabel: peakHeapKbPerEvent === null ? null : formatKbPerEvent(peakHeapKbPerEvent),
    peakHeapKbPerEvent,
    peakHeapKbPerEventLabel: peakHeapKbPerEvent === null ? null : formatKbPerEvent(peakHeapKbPerEvent),
    peakRssKbPerEvent,
    peakRssKbPerEventLabel: peakRssKbPerEvent === null ? null : formatKbPerEvent(peakRssKbPerEvent),
    throughput: Math.round(totalEvents / (totalMs / 1000)),
    processedCount,
  }
  record(hooks, result.scenario, result)
  return result
}

export const runPerfSingleEventManyFixedHandlers = async (input) => {
  const hooks = withDefaults(input)
  const { BaseEvent, EventBus } = hooks.api
  const totalEvents = 1
  const totalHandlers = 50_000
  const { PerfFixedHandlersEvent: FixedHandlersEvent, PerfTrimEventFixedHandlers: TrimEvent } = getEventClasses(BaseEvent)
  const bus = new EventBus('FixedHandlersBus', {
    max_history_size: HISTORY_LIMIT_FIXED_HANDLERS,
    max_history_drop: true,
    event_handler_concurrency: 'parallel',
  })

  let processedCount = 0
  for (let i = 0; i < totalHandlers; i += 1) {
    bus.on(
      FixedHandlersEvent,
      () => {
        processedCount += 1
      },
      { id: `fixed-handler-${i}` }
    )
    if (i % 1000 === 0) {
      // Keep memory sampling overhead bounded during massive registration.
      measureMemory(hooks)
    }
  }

  const memory = createMemoryTracker(hooks)
  const t0 = hooks.now()

  const event = bus.emit(FixedHandlersEvent({}))
  await event.done()
  await bus.waitUntilIdle()

  const totalMs = hooks.now() - t0
  await waitForRuntimeSettle(hooks)
  memory.sample()
  const msPerEvent = totalMs / (totalEvents * totalHandlers)
  const peakHeapKbPerEvent = memory.peakHeapKbPerEvent(totalEvents)
  const peakRssKbPerEvent = memory.peakRssKbPerEvent(totalEvents)

  assert(processedCount === totalHandlers, `fixed-handlers processed ${processedCount}/${totalHandlers}`)
  assert(totalMs < hooks.limits.singleRunMs, `fixed-handlers took ${Math.round(totalMs)}ms (limit ${hooks.limits.singleRunMs}ms)`)
  assert(bus.handlers.size === totalHandlers, `fixed-handlers expected ${totalHandlers} registered handlers, got ${bus.handlers.size}`)

  await trimBusHistoryToOneEvent(hooks, bus, TrimEvent)
  bus.destroy()

  const result = {
    scenario: '1 event x 50k parallel handlers',
    totalEvents,
    totalMs,
    msPerEvent,
    msPerEventLabel: formatMsPerEvent(msPerEvent, 'event/handler'),
    msPerEventUnit: 'event/handler',
    ramKbPerEvent: peakHeapKbPerEvent,
    ramKbPerEventLabel: peakHeapKbPerEvent === null ? null : formatKbPerEvent(peakHeapKbPerEvent),
    peakHeapKbPerEvent,
    peakHeapKbPerEventLabel: peakHeapKbPerEvent === null ? null : formatKbPerEvent(peakHeapKbPerEvent),
    peakRssKbPerEvent,
    peakRssKbPerEventLabel: peakRssKbPerEvent === null ? null : formatKbPerEvent(peakRssKbPerEvent),
    throughput: Math.round(totalEvents / (totalMs / 1000)),
    processedCount,
    totalHandlers,
  }
  record(hooks, result.scenario, result)
  return result
}

export const runPerfOnOffChurn = async (input) => {
  const hooks = withDefaults(input)
  const { BaseEvent, EventBus } = hooks.api
  const { PerfRequestEvent: RequestEvent, PerfTrimEventOnOff: TrimEvent } = getEventClasses(BaseEvent)

  const totalEvents = 50_000
  const bus = new EventBus('OneOffHandlerBus', { max_history_size: HISTORY_LIMIT_ON_OFF, max_history_drop: true })

  let processedCount = 0

  const memory = createMemoryTracker(hooks)
  const t0 = hooks.now()

  const dispatchWithEphemeralHandler = async () => {
    // Allocate/register exactly one handler for one event, then immediately remove it.
    // Avoid pre-building handler arrays so memory samples reflect runtime churn, not idle closures.
    const oneOffHandler = () => {
      processedCount += 1
    }
    bus.on(RequestEvent, oneOffHandler)

    const ev = bus.emit(RequestEvent({}))
    await ev.done()

    bus.off(RequestEvent, oneOffHandler)
  }

  for (let i = 0; i < totalEvents; i += 1) {
    await dispatchWithEphemeralHandler()
    if (i % 1000 === 0) memory.sample()
  }

  await bus.waitUntilIdle()
  const totalMs = hooks.now() - t0
  const msPerEvent = totalMs / totalEvents

  assert(processedCount === totalEvents, `50k one-off handlers processed ${processedCount}/${totalEvents}`)
  assert(totalMs < hooks.limits.singleRunMs, `50k on/off took ${Math.round(totalMs)}ms (limit ${hooks.limits.singleRunMs}ms)`)
  assert(bus.handlers.size === 0, `50k on/off leaked handlers: ${bus.handlers.size}`)

  await trimBusHistoryToOneEvent(hooks, bus, TrimEvent)
  await waitForRuntimeSettle(hooks)
  memory.sample()
  const peakHeapKbPerEvent = memory.peakHeapKbPerEvent(totalEvents)
  const peakRssKbPerEvent = memory.peakRssKbPerEvent(totalEvents)
  bus.destroy()

  const result = {
    scenario: '50k one-off handlers over 50k events',
    totalEvents,
    totalMs,
    msPerEvent,
    msPerEventLabel: formatMsPerEvent(msPerEvent),
    ramKbPerEvent: peakHeapKbPerEvent,
    ramKbPerEventLabel: peakHeapKbPerEvent === null ? null : formatKbPerEvent(peakHeapKbPerEvent),
    peakHeapKbPerEvent,
    peakHeapKbPerEventLabel: peakHeapKbPerEvent === null ? null : formatKbPerEvent(peakHeapKbPerEvent),
    peakRssKbPerEvent,
    peakRssKbPerEventLabel: peakRssKbPerEvent === null ? null : formatKbPerEvent(peakRssKbPerEvent),
    throughput: Math.round(totalEvents / (totalMs / 1000)),
    processedCount,
  }
  record(hooks, result.scenario, result)
  return result
}

export const runPerfWorstCase = async (input) => {
  const hooks = withDefaults(input)
  const { BaseEvent, EventBus, EventHandlerTimeoutError, EventHandlerCancelledError } = hooks.api

  const { WCParent: ParentEvent, WCChild: ChildEvent, WCGrandchild: GrandchildEvent, WCTrimEvent: TrimEvent } = getEventClasses(BaseEvent)

  const totalIterations = 500
  const historyLimit = HISTORY_LIMIT_WORST_CASE
  const busA = new EventBus('WCA', { max_history_size: historyLimit, max_history_drop: true })
  const busB = new EventBus('WCB', { max_history_size: historyLimit, max_history_drop: true })
  const busC = new EventBus('WCC', { max_history_size: historyLimit, max_history_drop: true })

  let parentHandledA = 0
  let parentHandledB = 0
  let childHandled = 0
  let grandchildHandled = 0
  let timeoutCount = 0
  let cancelCount = 0

  busB.on(ParentEvent, () => {
    parentHandledB += 1
  })

  busC.on(ChildEvent, async (event) => {
    childHandled += 1
    const gc = event.bus.emit(GrandchildEvent({}))
    busC.emit(gc)
    if (event.event_timeout !== null) {
      // Yield once so near-zero timeout paths execute without adding a large fixed delay.
      await hooks.sleep(0)
    }
    await gc.done()
  })

  busC.on(GrandchildEvent, () => {
    grandchildHandled += 1
  })

  const memory = createMemoryTracker(hooks)
  const t0 = hooks.now()

  for (let i = 0; i < totalIterations; i += 1) {
    const shouldTimeout = i % 5 === 0

    const ephemeralHandler = async (event) => {
      parentHandledA += 1
      const child = event.bus.emit(
        ChildEvent({
          // event_timeout is in seconds; use a near-zero timeout to exercise timeout handling overhead.
          event_timeout: shouldTimeout ? WORST_CASE_IMMEDIATE_TIMEOUT_SECONDS : null,
        })
      )
      busC.emit(child)
      try {
        await child.done()
      } catch {
        // Timeouts are expected for timeout iterations.
      }
    }

    busA.on(ParentEvent, ephemeralHandler)
    const parent = ParentEvent({})
    const evA = busA.emit(parent)
    busB.emit(parent)
    await evA.done()
    busA.off(ParentEvent, ephemeralHandler)

    if (i % 10 === 0) {
      busA.find(ParentEvent, { future: 0.001 })
    }
    if (i % 5 === 0) memory.sample()
  }

  await busA.waitUntilIdle()
  await busB.waitUntilIdle()
  await busC.waitUntilIdle()
  memory.sample()

  for (const event of busC.event_history.values()) {
    for (const result of event.event_results.values()) {
      if (result.error instanceof EventHandlerTimeoutError) timeoutCount += 1
      if (result.error instanceof EventHandlerCancelledError) cancelCount += 1
    }
  }

  const totalMs = hooks.now() - t0
  const estimatedEvents = totalIterations * 3
  const msPerEvent = totalMs / estimatedEvents

  assert(parentHandledA === totalIterations, `worst-case parentA ${parentHandledA}/${totalIterations}`)
  assert(parentHandledB === totalIterations, `worst-case parentB ${parentHandledB}/${totalIterations}`)
  assert(busA.handlers.size === 0, `worst-case leaked busA handlers: ${busA.handlers.size}`)
  assert(busA.event_history.size <= historyLimit, `worst-case busA history ${busA.event_history.size}/${historyLimit}`)
  assert(busB.event_history.size <= historyLimit, `worst-case busB history ${busB.event_history.size}/${historyLimit}`)
  assert(busC.event_history.size <= historyLimit, `worst-case busC history ${busC.event_history.size}/${historyLimit}`)
  assert(totalMs < hooks.limits.worstCaseMs, `worst-case took ${Math.round(totalMs)}ms (limit ${hooks.limits.worstCaseMs}ms)`)

  await trimBusHistoryToOneEvent(hooks, busA, TrimEvent)
  await trimBusHistoryToOneEvent(hooks, busB, TrimEvent)
  await trimBusHistoryToOneEvent(hooks, busC, TrimEvent)
  await waitForRuntimeSettle(hooks)
  const peakHeapKbPerEvent = memory.peakHeapKbPerEvent(estimatedEvents)
  const peakRssKbPerEvent = memory.peakRssKbPerEvent(estimatedEvents)
  busA.destroy()
  busB.destroy()
  busC.destroy()

  const result = {
    scenario: 'worst-case forwarding + timeouts',
    totalEvents: estimatedEvents,
    totalMs,
    msPerEvent,
    msPerEventLabel: formatMsPerEvent(msPerEvent),
    ramKbPerEvent: peakHeapKbPerEvent,
    ramKbPerEventLabel: peakHeapKbPerEvent === null ? null : formatKbPerEvent(peakHeapKbPerEvent),
    peakHeapKbPerEvent,
    peakHeapKbPerEventLabel: peakHeapKbPerEvent === null ? null : formatKbPerEvent(peakHeapKbPerEvent),
    peakRssKbPerEvent,
    peakRssKbPerEventLabel: peakRssKbPerEvent === null ? null : formatKbPerEvent(peakRssKbPerEvent),
    parentHandledA,
    parentHandledB,
    childHandled,
    grandchildHandled,
    timeoutCount,
    cancelCount,
  }
  record(hooks, result.scenario, result)
  assert(EventBus.all_instances.size === 0, `worst-case leaked instances: ${EventBus.all_instances.size}`)

  return result
}

export const runCleanupEquivalence = async (input) => {
  const hooks = withDefaults(input)
  const { BaseEvent, EventBus } = hooks.api
  const { CleanupEqEvent: CleanupEvent, CleanupEqTrimEvent: TrimEvent } = getEventClasses(BaseEvent)

  const busesPerMode = 80
  const eventsPerBus = 64
  const totalEvents = busesPerMode * eventsPerBus * 2
  const baselineRegistrySize = EventBus.all_instances.size

  const t0 = hooks.now()

  await runCleanupBurst({
    hooks,
    EventBus,
    CleanupEvent,
    TrimEvent,
    busesPerMode,
    eventsPerBus,
    destroyMode: true,
  })
  assert(
    EventBus.all_instances.size === baselineRegistrySize,
    `cleanup equivalence destroy branch leaked instances: ${EventBus.all_instances.size}/${baselineRegistrySize}`
  )

  await runCleanupBurst({
    hooks,
    EventBus,
    CleanupEvent,
    TrimEvent,
    busesPerMode,
    eventsPerBus,
    destroyMode: false,
  })

  const scopeCollectionAttempts = hooks.runtimeName === 'deno' ? 500 : 150
  let scopeCollected = await waitForRegistrySize(hooks, EventBus, baselineRegistrySize, scopeCollectionAttempts)
  let scopeEquivalentByState = false

  if (!scopeCollected) {
    const retained = Array.from(EventBus.all_instances)
    const allRetainedIdle = retained.every(
      (bus) =>
        bus.pending_event_queue.length === 0 &&
        bus.in_flight_event_ids.size === 0 &&
        bus.find_waiters.size === 0 &&
        bus.runloop_running === false &&
        bus.event_history.size <= TRIM_TARGET
    )
    assert(
      allRetainedIdle,
      `cleanup equivalence scope branch retained active deno instances: ${EventBus.all_instances.size}/${baselineRegistrySize}`
    )
    if (hooks.runtimeName === 'deno') {
      assert(
        retained.length <= 24,
        `cleanup equivalence scope branch retained too many deno instances: ${retained.length} (expected <= 24)`
      )
    } else {
      assert(
        retained.length <= busesPerMode,
        `cleanup equivalence scope branch retained too many non-gc-forced instances: ${retained.length} (expected <= ${busesPerMode})`
      )
    }
    scopeEquivalentByState = true

    // Some runtimes may defer finalizing weak refs even after explicit waits.
    // Destroy retained idle buses so following scenarios start from a clean baseline.
    for (const bus of retained) {
      bus.destroy()
    }
    scopeCollected = await waitForRegistrySize(hooks, EventBus, baselineRegistrySize, 100)
  }

  const equivalent = scopeCollected || scopeEquivalentByState
  assert(equivalent, `cleanup equivalence scope branch retained instances: ${EventBus.all_instances.size}/${baselineRegistrySize}`)

  const totalMs = hooks.now() - t0
  const msPerEvent = totalMs / totalEvents

  const result = {
    scenario: 'cleanup destroy vs scope equivalence',
    totalEvents,
    totalMs,
    msPerEvent,
    msPerEventLabel: formatMsPerEvent(msPerEvent),
    ramKbPerEvent: null,
    peakHeapKbPerEvent: null,
    peakRssKbPerEvent: null,
    equivalent,
  }
  record(hooks, result.scenario, result)
  return result
}

const runWithLeakCheck = async (input, scenarioId, scenarioFn) => {
  const hooks = withDefaults(input)
  let baselineHeapUsed = null
  if (typeof hooks.getMemoryUsage === 'function') {
    // Leak checks compare retained floor before/after work; min/min reduces allocator jitter noise.
    baselineHeapUsed = await measureStableHeapUsed(hooks, 'min', 8)
  }

  const result = await scenarioFn(input)

  if (baselineHeapUsed === null) {
    return result
  }

  const heapDeltaAfterGcMb = await measureHeapDeltaAfterGc(hooks, baselineHeapUsed)
  if (heapDeltaAfterGcMb === null) {
    return result
  }

  const normalizedHeapDeltaAfterGcMb = clampNonNegative(heapDeltaAfterGcMb)
  result.heapDeltaAfterGcMb = Number(normalizedHeapDeltaAfterGcMb.toFixed(3))
  if (typeof hooks.log === 'function') {
    hooks.log(`[${hooks.runtimeName}] ${result.scenario}: heap_delta_after_gc=${formatMb(result.heapDeltaAfterGcMb)}`)
  }

  const maxHeapDeltaAfterGcMb = hooks.limits.maxHeapDeltaAfterGcMb
  const heapNoiseFloorMb = hooks.limits.heapDeltaNoiseFloorMb
  if (typeof maxHeapDeltaAfterGcMb === 'number') {
    const allowedMb = maxHeapDeltaAfterGcMb + heapNoiseFloorMb
    assert(
      normalizedHeapDeltaAfterGcMb <= allowedMb,
      `${scenarioId} retained ${normalizedHeapDeltaAfterGcMb.toFixed(3)}mb heap after GC (limit ${allowedMb.toFixed(3)}mb = ${maxHeapDeltaAfterGcMb.toFixed(3)}mb + ${heapNoiseFloorMb.toFixed(3)}mb noise floor)`
    )
  }

  return result
}

const PERF_SCENARIO_RUNNERS = {
  '50k-events': runPerf50kEvents,
  '500-buses-x-100-events': runPerfEphemeralBuses,
  '1-event-x-50k-parallel-handlers': runPerfSingleEventManyFixedHandlers,
  '50k-one-off-handlers': runPerfOnOffChurn,
  'worst-case-forwarding-timeouts': runPerfWorstCase,
  'cleanup-equivalence': runCleanupEquivalence,
}

export const PERF_SCENARIO_IDS = Object.freeze(Object.keys(PERF_SCENARIO_RUNNERS))

export const runPerfScenarioById = async (input, scenarioId) => {
  const scenarioFn = PERF_SCENARIO_RUNNERS[scenarioId]
  if (!scenarioFn) {
    throw new Error(`unknown perf scenario "${scenarioId}", expected one of: ${PERF_SCENARIO_IDS.join(', ')}`)
  }
  await runWarmup(input)
  return runWithLeakCheck(input, scenarioId, scenarioFn)
}

export const runAllPerfScenarios = async (input) => {
  const results = []
  for (const scenarioId of PERF_SCENARIO_IDS) {
    results.push(await runPerfScenarioById(input, scenarioId))
  }
  return results
}
