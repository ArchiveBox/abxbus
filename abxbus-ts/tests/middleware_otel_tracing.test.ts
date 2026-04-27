import assert from 'node:assert/strict'
import { test } from 'node:test'

import {
  ROOT_CONTEXT,
  type Context,
  type Span,
  type SpanAttributes,
  type SpanAttributeValue,
  type SpanContext,
  type SpanOptions,
  type SpanStatus,
  type TimeInput,
  type Tracer,
} from '@opentelemetry/api'

import { BaseEvent, EventBus, OtelTracingMiddleware, type OtelTracingSpanFactoryInput } from '../src/index.js'

const flushHooks = async (ticks: number = 4): Promise<void> => {
  for (let i = 0; i < ticks; i += 1) {
    await Promise.resolve()
  }
}

type RecordingContext = {
  parent: Context
  span: RecordingSpan
}

class RecordingSpan implements Span {
  name: string
  options: SpanOptions | undefined
  parent_context: Context | undefined
  attributes: Record<string, SpanAttributeValue>
  status: SpanStatus | undefined
  exceptions: unknown[]
  ended: boolean
  end_time: TimeInput | undefined
  span_context: SpanContext
  parent_span_context: SpanContext | undefined

  constructor(
    name: string,
    options: SpanOptions | undefined,
    parent_context: Context | undefined,
    span_context: SpanContext = {
      traceId: '00000000000000000000000000000001',
      spanId: '0000000000000001',
      traceFlags: 1,
    },
    parent_span_context?: SpanContext
  ) {
    this.name = name
    this.options = options
    this.parent_context = parent_context
    this.span_context = span_context
    this.parent_span_context = parent_span_context
    this.attributes = {}
    for (const [key, value] of Object.entries(options?.attributes ?? {})) {
      if (value !== undefined) {
        this.attributes[key] = value
      }
    }
    this.exceptions = []
    this.ended = false
    this.end_time = undefined
  }

  spanContext(): SpanContext {
    return this.span_context
  }

  setAttribute(key: string, value: SpanAttributeValue): this {
    this.attributes[key] = value
    return this
  }

  setAttributes(attributes: SpanAttributes): this {
    for (const [key, value] of Object.entries(attributes)) {
      if (value !== undefined) {
        this.attributes[key] = value
      }
    }
    return this
  }

  addEvent(): this {
    return this
  }

  addLink(): this {
    return this
  }

  addLinks(): this {
    return this
  }

  setStatus(status: SpanStatus): this {
    this.status = status
    return this
  }

  updateName(name: string): this {
    this.name = name
    return this
  }

  end(end_time?: TimeInput): void {
    this.ended = true
    this.end_time = end_time
  }

  isRecording(): boolean {
    return true
  }

  recordException(exception: unknown): void {
    this.exceptions.push(exception)
  }
}

class RecordingTracer implements Tracer {
  spans: RecordingSpan[] = []

  startSpan(name: string, options?: SpanOptions, parent_context?: Context): Span {
    const span = new RecordingSpan(name, options, parent_context)
    this.spans.push(span)
    return span
  }

  startActiveSpan(): never {
    throw new Error('not implemented')
  }
}

test('OtelTracingMiddleware creates event and handler spans with child event parentage', async () => {
  const tracer = new RecordingTracer()
  const trace_api = {
    getTracer: () => tracer,
    setSpan: (parent: Context, span: Span): Context => ({ parent, span }) as unknown as Context,
  }
  const bus = new EventBus('OtelTracingBus', {
    middlewares: [new OtelTracingMiddleware({ tracer, trace_api })],
    max_history_size: null,
  })
  const ParentEvent = BaseEvent.extend('OtelTracingParentEvent', {})
  const ChildEvent = BaseEvent.extend('OtelTracingChildEvent', {})

  bus.on(ParentEvent, async (event) => {
    await event.emit(ChildEvent({ event_timeout: 0.2 })).done()
    return 'parent'
  })
  bus.on(ChildEvent, () => 'child')

  await bus.emit(ParentEvent({ event_timeout: 0.5 })).done()
  await flushHooks()

  const parent_event_span = tracer.spans.find((span) => span.name === 'abxbus.event OtelTracingParentEvent')
  const parent_handler_span = tracer.spans.find((span) => span.name.startsWith('abxbus.handler OtelTracingParentEvent '))
  const child_event_span = tracer.spans.find((span) => span.name === 'abxbus.event OtelTracingChildEvent')
  const child_handler_span = tracer.spans.find((span) => span.name.startsWith('abxbus.handler OtelTracingChildEvent '))
  const root_span = tracer.spans.find((span) => span.name === 'abxbus.trace OtelTracingBus')

  assert.ok(parent_event_span)
  assert.ok(parent_handler_span)
  assert.ok(child_event_span)
  assert.ok(child_handler_span)
  assert.ok(root_span)
  assert.equal(root_span.parent_context, ROOT_CONTEXT)
  assert.equal(recordingContext(parent_event_span.parent_context).span, root_span)
  assert.equal(parent_event_span.ended, true)
  assert.equal(parent_handler_span.ended, true)
  assert.equal(child_event_span.ended, true)
  assert.equal(child_handler_span.ended, true)
  assert.equal(recordingContext(parent_handler_span.parent_context).span, parent_event_span)
  assert.equal(recordingContext(child_event_span.parent_context).span, parent_handler_span)
  assert.equal(recordingContext(child_handler_span.parent_context).span, child_event_span)

  bus.destroy()
})

test('OtelTracingMiddleware supports named root spans with session attributes and non-zero duration', async () => {
  const tracer = new RecordingTracer()
  const trace_api = {
    getTracer: () => tracer,
    setSpan: (parent: Context, span: Span): Context => ({ parent, span }) as unknown as Context,
  }
  const bus = new EventBus('OtelTracingSessionBus', {
    middlewares: [
      new OtelTracingMiddleware({
        tracer,
        trace_api,
        root_span_name: () => 'StagehandSession session-123',
        root_span_attributes: { 'stagehand.session_id': 'session-123' },
      }),
    ],
  })
  const InstantEvent = BaseEvent.extend('OtelTracingInstantEvent', {})

  await bus.emit(InstantEvent({ session_id: 'event-session-456', event_timeout: 0.2 } as any)).eventCompleted()
  await flushHooks()

  const root_span = tracer.spans.find((span) => span.name === 'StagehandSession session-123')
  const event_span = tracer.spans.find((span) => span.name === 'abxbus.event OtelTracingInstantEvent')

  assert.ok(root_span)
  assert.ok(event_span)
  assert.equal(root_span.attributes['stagehand.session_id'], 'session-123')
  assert.equal(root_span.attributes['abxbus.root_event.session_id'], 'event-session-456')
  assert.equal(event_span.attributes['abxbus.event.session_id'], 'event-session-456')
  assert.equal(recordingContext(event_span.parent_context).span, root_span)
  assert.ok(root_span.end_time instanceof Date)
  assert.ok(event_span.end_time instanceof Date)
  assert.ok(root_span.options?.startTime instanceof Date)
  assert.ok(event_span.options?.startTime instanceof Date)
  assert.ok(root_span.end_time.getTime() > root_span.options.startTime.getTime())
  assert.ok(event_span.end_time.getTime() > event_span.options.startTime.getTime())

  bus.destroy()
})

test('OtelTracingMiddleware span_factory mirrors abxbus ids into stable parent and child span contexts', async () => {
  const spans: RecordingSpan[] = []
  const span_factory = (input: OtelTracingSpanFactoryInput): Span => {
    const span = new RecordingSpan(
      input.name,
      {
        attributes: input.attributes,
        startTime: input.start_time,
      },
      undefined,
      input.span_context,
      input.parent_span_context
    )
    spans.push(span)
    return span
  }
  const bus = new EventBus('OtelTracingManualIdsBus', {
    middlewares: [
      new OtelTracingMiddleware({
        span_factory,
        root_span_name: () => 'StagehandSession session-abc',
        root_span_attributes: { 'stagehand.session_id': 'session-abc' },
      }),
    ],
    max_history_size: null,
  })
  const ParentEvent = BaseEvent.extend('OtelTracingManualParentEvent', {})
  const ChildEvent = BaseEvent.extend('OtelTracingManualChildEvent', {})

  bus.on(ParentEvent, async (event) => {
    await event.emit(ChildEvent({ event_timeout: 0.2 })).done()
    return 'parent'
  })
  bus.on(ChildEvent, () => 'child')

  await bus.emit(ParentEvent({ session_id: 'session-abc', event_timeout: 0.5 } as any)).done()
  await flushHooks()

  const root_span = spans.find((span) => span.name === 'StagehandSession session-abc')
  const parent_event_span = spans.find((span) => span.name === 'abxbus.event OtelTracingManualParentEvent')
  const parent_handler_span = spans.find((span) => span.name.startsWith('abxbus.handler OtelTracingManualParentEvent '))
  const child_event_span = spans.find((span) => span.name === 'abxbus.event OtelTracingManualChildEvent')
  const child_handler_span = spans.find((span) => span.name.startsWith('abxbus.handler OtelTracingManualChildEvent '))

  assert.ok(root_span)
  assert.ok(parent_event_span)
  assert.ok(parent_handler_span)
  assert.ok(child_event_span)
  assert.ok(child_handler_span)

  const trace_id = root_span.spanContext().traceId
  assert.match(trace_id, /^[0-9a-f]{32}$/)
  assert.equal(parent_event_span.spanContext().traceId, trace_id)
  assert.equal(parent_handler_span.spanContext().traceId, trace_id)
  assert.equal(child_event_span.spanContext().traceId, trace_id)
  assert.equal(child_handler_span.spanContext().traceId, trace_id)

  assert.equal(root_span.parent_span_context, undefined)
  assert.equal(parent_event_span.parent_span_context?.spanId, root_span.spanContext().spanId)
  assert.equal(parent_handler_span.parent_span_context?.spanId, parent_event_span.spanContext().spanId)
  assert.equal(child_event_span.parent_span_context?.spanId, parent_handler_span.spanContext().spanId)
  assert.equal(child_handler_span.parent_span_context?.spanId, child_event_span.spanContext().spanId)

  const span_ids = new Set(spans.map((span) => span.spanContext().spanId))
  assert.equal(span_ids.size, spans.length)
  assert.ok([...span_ids].every((span_id) => /^[0-9a-f]{16}$/.test(span_id) && span_id !== '0000000000000000'))
  assert.equal(parent_event_span.attributes['abxbus.event.parent_id'], undefined)
  assert.equal(child_event_span.attributes['abxbus.event.parent_id'], parent_event_span.attributes['abxbus.event.id'])
  assert.equal(child_event_span.attributes['abxbus.event.emitted_by_handler_id'], parent_handler_span.attributes['abxbus.handler.id'])

  bus.destroy()
})

function recordingContext(value: Context | undefined): RecordingContext {
  assert.ok(value)
  return value as unknown as RecordingContext
}

test('OtelTracingMiddleware records handler errors on handler and event spans', async () => {
  const tracer = new RecordingTracer()
  const bus = new EventBus('OtelTracingErrorBus', {
    middlewares: [
      new OtelTracingMiddleware({
        tracer,
        trace_api: {
          getTracer: () => tracer,
          setSpan: (parent: Context, span: Span): Context => ({ parent, span }) as unknown as Context,
        },
      }),
    ],
  })
  const ErrorEvent = BaseEvent.extend('OtelTracingErrorEvent', {})
  const error = new Error('handler failed')

  bus.on(ErrorEvent, () => {
    throw error
  })

  await bus.emit(ErrorEvent({ event_timeout: 0.2 })).eventCompleted()
  await flushHooks()

  const event_span = tracer.spans.find((span) => span.name === 'abxbus.event OtelTracingErrorEvent')
  const handler_span = tracer.spans.find((span) => span.name.startsWith('abxbus.handler OtelTracingErrorEvent '))

  assert.ok(event_span)
  assert.ok(handler_span)
  assert.equal(event_span.status?.code, 2)
  assert.equal(handler_span.status?.code, 2)
  assert.equal(event_span.exceptions[0], error)
  assert.equal(handler_span.exceptions[0], error)

  bus.destroy()
})
