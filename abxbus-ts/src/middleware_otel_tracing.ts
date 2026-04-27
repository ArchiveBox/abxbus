import {
  ROOT_CONTEXT,
  SpanKind,
  SpanStatusCode,
  trace,
  type Context,
  type Span,
  type SpanAttributeValue,
  type SpanAttributes,
  type SpanContext,
  type Tracer,
} from '@opentelemetry/api'
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http'
import { resourceFromAttributes } from '@opentelemetry/resources'
import { BasicTracerProvider, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base'
import type { SpanLimits, SpanProcessor } from '@opentelemetry/sdk-trace-base'
import { SpanImpl } from '@opentelemetry/sdk-trace-base/build/src/Span.js'

import type { BaseEvent } from './base_event.js'
import type { EventBus } from './event_bus.js'
import type { EventResult } from './event_result.js'
import type { EventBusMiddleware } from './middlewares.js'
import type { EventStatus } from './types.js'

type OpenTelemetryTraceApi = Pick<typeof trace, 'getTracer' | 'setSpan'> & Partial<Pick<typeof trace, 'setSpanContext'>>

export type OtelTracingSpanFactoryInput = {
  name: string
  span_context: SpanContext
  parent_span_context?: SpanContext
  attributes: SpanAttributes
  start_time?: Date
}

export type OtelTracingSpanFactory = (input: OtelTracingSpanFactoryInput) => Span

type OtelTracingSpanProviderInternals = {
  _activeSpanProcessor?: SpanProcessor
  _config?: {
    resource?: unknown
    spanLimits?: SpanLimits
  }
  _resource?: unknown
}

export type OtelTracingSpanProvider = object

export type OtelTracingMiddlewareOptions = {
  tracer?: Tracer
  trace_api?: OpenTelemetryTraceApi
  span_provider?: OtelTracingSpanProvider
  span_factory?: OtelTracingSpanFactory
  otlp_endpoint?: string
  service_name?: string
  instrumentation_name?: string
  root_span_name?: string | ((eventbus: EventBus, event: BaseEvent) => string)
  root_span_attributes?: SpanAttributes | ((eventbus: EventBus, event: BaseEvent) => SpanAttributes)
}

export class OtelTracingMiddleware implements EventBusMiddleware {
  private readonly tracer: Tracer
  private readonly trace_api: OpenTelemetryTraceApi
  private readonly span_factory?: OtelTracingSpanFactory
  private readonly span_provider?: OtelTracingSpanProvider
  private readonly root_span_name: OtelTracingMiddlewareOptions['root_span_name']
  private readonly root_span_attributes: OtelTracingMiddlewareOptions['root_span_attributes']
  private readonly root_spans = new Map<string, Span>()
  private readonly root_contexts = new Map<string, Context>()
  private readonly event_spans = new Map<string, Span>()
  private readonly event_contexts = new Map<string, Context>()
  private readonly handler_spans = new Map<string, Span>()
  private readonly handler_contexts = new Map<string, Context>()

  constructor(options: OtelTracingMiddlewareOptions = {}) {
    this.trace_api = options.trace_api ?? trace
    this.tracer = options.tracer ?? this.trace_api.getTracer('abxbus')
    this.span_provider = options.span_provider ?? (options.otlp_endpoint ? createOtlpSpanProvider(options) : undefined)
    this.span_factory =
      options.span_factory ??
      (this.span_provider
        ? createProviderSpanFactory(this.trace_api, this.span_provider, options.instrumentation_name ?? 'abxbus')
        : undefined)
    this.root_span_name = options.root_span_name
    this.root_span_attributes = options.root_span_attributes
  }

  onEventChange(eventbus: EventBus, event: BaseEvent, status: EventStatus): void {
    if (status === 'started') {
      if (this.span_factory) {
        return
      }
      this.startEventSpan(eventbus, event)
      return
    }

    if (status === 'completed') {
      this.completeEventSpan(eventbus, event)
    }
  }

  onEventResultChange(eventbus: EventBus, event: BaseEvent, event_result: EventResult, status: EventStatus): void {
    if (status === 'started') {
      if (this.span_factory) {
        return
      }
      this.startHandlerSpan(eventbus, event, event_result)
      return
    }

    if (status === 'completed') {
      this.completeHandlerSpan(eventbus, event, event_result)
    }
  }

  private startEventSpan(eventbus: EventBus, event: BaseEvent): Span {
    const existing = this.event_spans.get(event.event_id)
    if (existing) {
      return existing
    }

    const parent_context = this.parentContextForEvent(event) ?? this.startRootSpan(eventbus, event)
    const start_time = dateFromIso(event.event_started_at)
    const span = this.tracer.startSpan(
      eventSpanName(eventbus, event),
      {
        attributes: compactAttributes({
          'abxbus.bus.id': eventbus.id,
          'abxbus.bus.name': eventbus.name,
          'abxbus.event.id': event.event_id,
          'abxbus.event.type': event.event_type,
          'abxbus.event.version': event.event_version,
          'abxbus.event.session_id': stringValue((event as { session_id?: unknown }).session_id),
          'abxbus.event.parent_id': event.event_parent_id,
          'abxbus.event.emitted_by_handler_id': event.event_emitted_by_handler_id,
          'abxbus.event.path': event.event_path.join(' '),
        }),
        startTime: start_time,
      },
      parent_context
    )
    const span_context = this.trace_api.setSpan(parent_context, span)
    this.event_spans.set(event.event_id, span)
    this.event_contexts.set(event.event_id, span_context)
    return span
  }

  private completeEventSpan(eventbus: EventBus, event: BaseEvent): void {
    if (this.span_factory) {
      this.completeEventSpanWithFactory(eventbus, event)
      return
    }

    const span = this.event_spans.get(event.event_id) ?? this.startEventSpan(eventbus, event)
    if (event.event_errors.length > 0) {
      recordSpanError(span, event.event_errors[0])
    } else {
      span.setStatus({ code: SpanStatusCode.OK })
    }
    span.setAttributes(
      compactAttributes({
        'abxbus.event.status': event.event_status,
        'abxbus.event.result_count': event.event_results.size,
        'abxbus.event.error_count': event.event_errors.length,
        'abxbus.event.child_count': event.event_children.length,
      })
    )
    const start_time = dateFromIso(event.event_started_at)
    const end_time = endTimeAfterStart(start_time, dateFromIso(event.event_completed_at))
    span.end(end_time)
    this.event_spans.delete(event.event_id)
    this.event_contexts.delete(event.event_id)
    this.completeRootSpan(event.event_id, start_time, end_time)
  }

  private startHandlerSpan(eventbus: EventBus, event: BaseEvent, event_result: EventResult): Span {
    const existing = this.handler_spans.get(event_result.id)
    if (existing) {
      return existing
    }

    const parent_context =
      this.event_contexts.get(event.event_id) ?? this.trace_api.setSpan(ROOT_CONTEXT, this.startEventSpan(eventbus, event))
    const span = this.tracer.startSpan(
      handlerSpanName(event, event_result),
      {
        attributes: compactAttributes({
          'abxbus.bus.id': eventbus.id,
          'abxbus.bus.name': eventbus.name,
          'abxbus.event.id': event.event_id,
          'abxbus.event.type': event.event_type,
          'abxbus.handler.id': event_result.handler_id,
          'abxbus.handler.name': event_result.handler_name,
          'abxbus.handler.file_path': event_result.handler_file_path,
          'abxbus.handler.event_pattern': event_result.handler.event_pattern,
          'abxbus.event_result.id': event_result.id,
        }),
        startTime: dateFromIso(event_result.started_at),
      },
      parent_context
    )
    const span_context = this.trace_api.setSpan(parent_context, span)
    this.handler_spans.set(event_result.id, span)
    this.handler_contexts.set(handlerSpanKey(event_result.event_id, event_result.handler_id), span_context)
    return span
  }

  private completeHandlerSpan(eventbus: EventBus, event: BaseEvent, event_result: EventResult): void {
    if (this.span_factory) {
      this.completeHandlerSpanWithFactory(eventbus, event, event_result)
      return
    }

    const span = this.handler_spans.get(event_result.id)
    if (!span) {
      return
    }

    if (event_result.error !== undefined) {
      recordSpanError(span, event_result.error)
    } else {
      span.setStatus({ code: SpanStatusCode.OK })
    }
    span.setAttributes(
      compactAttributes({
        'abxbus.event_result.status': event_result.status,
        'abxbus.handler.child_count': event_result.event_children.length,
      })
    )
    span.end(endTimeAfterStart(dateFromIso(event_result.started_at), dateFromIso(event_result.completed_at)))
    this.handler_spans.delete(event_result.id)
    this.handler_contexts.delete(handlerSpanKey(event_result.event_id, event_result.handler_id))
  }

  private startRootSpan(eventbus: EventBus, event: BaseEvent): Context {
    const existing = this.root_contexts.get(event.event_id)
    if (existing) {
      return existing
    }

    const session_id = stringValue((event as { session_id?: unknown }).session_id)
    const root_attributes = resolveAttributes(this.root_span_attributes, eventbus, event)
    const root_span = this.tracer.startSpan(
      resolveRootSpanName(this.root_span_name, eventbus, event),
      {
        attributes: compactAttributes({
          ...root_attributes,
          'abxbus.trace.root': true,
          'abxbus.bus.id': eventbus.id,
          'abxbus.bus.name': eventbus.name,
          'abxbus.root_event.id': event.event_id,
          'abxbus.root_event.type': event.event_type,
          'abxbus.root_event.session_id': session_id,
        }),
        startTime: dateFromIso(event.event_started_at),
      },
      ROOT_CONTEXT
    )
    const root_context = this.trace_api.setSpan(ROOT_CONTEXT, root_span)
    this.root_spans.set(event.event_id, root_span)
    this.root_contexts.set(event.event_id, root_context)
    return root_context
  }

  private completeRootSpan(event_id: string, start_time: Date | undefined, end_time: Date | undefined): void {
    const root_span = this.root_spans.get(event_id)
    if (!root_span) {
      return
    }

    root_span.setStatus({ code: SpanStatusCode.OK })
    root_span.end(endTimeAfterStart(start_time, end_time))
    this.root_spans.delete(event_id)
    this.root_contexts.delete(event_id)
  }

  private parentContextForEvent(event: BaseEvent): Context | undefined {
    if (event.event_parent_id && event.event_emitted_by_handler_id) {
      const handler_context = this.handler_contexts.get(handlerSpanKey(event.event_parent_id, event.event_emitted_by_handler_id))
      if (handler_context) {
        return handler_context
      }
    }

    return event.event_parent_id ? this.event_contexts.get(event.event_parent_id) : undefined
  }

  private completeEventSpanWithFactory(eventbus: EventBus, event: BaseEvent): void {
    const root_event = rootEventForEvent(eventbus, event)
    const trace_id = traceIdForRootEvent(root_event.event_id)
    const event_context = eventSpanContext(trace_id, event.event_id)
    const start_time = dateFromIso(event.event_started_at)
    const end_time = endTimeAfterStart(start_time, dateFromIso(event.event_completed_at))

    if (!event.event_parent_id) {
      const root_span = this.span_factory!({
        name: resolveRootSpanName(this.root_span_name, eventbus, event),
        span_context: rootSpanContext(trace_id, event.event_id),
        attributes: rootSpanAttributes(this.root_span_attributes, eventbus, event),
        start_time,
      })
      root_span.setStatus({ code: SpanStatusCode.OK })
      root_span.end(end_time)
    }

    const span = this.span_factory!({
      name: eventSpanName(eventbus, event),
      span_context: event_context,
      parent_span_context: parentSpanContextForEvent(event, trace_id),
      attributes: eventSpanAttributes(eventbus, event),
      start_time,
    })
    if (event.event_errors.length > 0) {
      recordSpanError(span, event.event_errors[0])
    } else {
      span.setStatus({ code: SpanStatusCode.OK })
    }
    span.end(end_time)
  }

  private completeHandlerSpanWithFactory(eventbus: EventBus, event: BaseEvent, event_result: EventResult): void {
    const root_event = rootEventForEvent(eventbus, event)
    const trace_id = traceIdForRootEvent(root_event.event_id)
    const start_time = dateFromIso(event_result.started_at)
    const span = this.span_factory!({
      name: handlerSpanName(event, event_result),
      span_context: handlerSpanContext(trace_id, event_result.event_id, event_result.handler_id),
      parent_span_context: eventSpanContext(trace_id, event.event_id),
      attributes: handlerSpanAttributes(eventbus, event, event_result),
      start_time,
    })

    if (event_result.error !== undefined) {
      recordSpanError(span, event_result.error)
    } else {
      span.setStatus({ code: SpanStatusCode.OK })
    }
    span.end(endTimeAfterStart(start_time, dateFromIso(event_result.completed_at)))
  }
}

function handlerSpanKey(event_id: string, handler_id: string): string {
  return `${event_id}:${handler_id}`
}

function eventSpanName(eventbus: EventBus, event: BaseEvent): string {
  return `${eventbus.name}.emit(${event.event_type})`
}

function handlerSpanName(event: BaseEvent, event_result: EventResult): string {
  return `${event_result.handler_name}(${event.event_type})`
}

function createOtlpSpanProvider(options: OtelTracingMiddlewareOptions): OtelTracingSpanProvider {
  return new BasicTracerProvider({
    resource: resourceFromAttributes({
      'service.name': options.service_name ?? 'abxbus',
    }),
    spanProcessors: [
      new SimpleSpanProcessor(
        new OTLPTraceExporter({
          url: normalizeOtlpTracesEndpoint(options.otlp_endpoint!),
        })
      ),
    ],
  })
}

function createProviderSpanFactory(
  trace_api: OpenTelemetryTraceApi,
  provider: OtelTracingSpanProvider,
  instrumentation_name: string
): OtelTracingSpanFactory {
  const provider_internals = provider as OtelTracingSpanProviderInternals
  return (input: OtelTracingSpanFactoryInput): Span => {
    const span_processor = provider_internals._activeSpanProcessor
    const span_limits = provider_internals._config?.spanLimits
    const resource = provider_internals._resource ?? provider_internals._config?.resource
    if (!span_processor || !span_limits || !resource) {
      throw new Error('OtelTracingMiddleware span_provider must be an OpenTelemetry SDK trace provider with active span internals')
    }

    const parent_context = input.parent_span_context
      ? (trace_api.setSpanContext ?? trace.setSpanContext)(ROOT_CONTEXT, input.parent_span_context)
      : ROOT_CONTEXT
    return new SpanImpl({
      resource,
      scope: { name: instrumentation_name },
      context: parent_context,
      spanContext: input.span_context,
      parentSpanContext: input.parent_span_context,
      name: input.name,
      kind: SpanKind.INTERNAL,
      attributes: input.attributes,
      startTime: input.start_time,
      spanProcessor: span_processor,
      spanLimits: span_limits,
    } as ConstructorParameters<typeof SpanImpl>[0])
  }
}

function normalizeOtlpTracesEndpoint(endpoint: string): string {
  const trimmed = endpoint.replace(/\/+$/, '')
  return trimmed.endsWith('/v1/traces') ? trimmed : `${trimmed}/v1/traces`
}

function eventSpanAttributes(eventbus: EventBus, event: BaseEvent): SpanAttributes {
  return compactAttributes({
    'abxbus.bus.id': eventbus.id,
    'abxbus.bus.name': eventbus.name,
    'abxbus.event.id': event.event_id,
    'abxbus.event.type': event.event_type,
    'abxbus.event.version': event.event_version,
    'abxbus.event.session_id': stringValue((event as { session_id?: unknown }).session_id),
    'abxbus.event.parent_id': event.event_parent_id,
    'abxbus.event.emitted_by_handler_id': event.event_emitted_by_handler_id,
    'abxbus.event.path': event.event_path.join(' '),
    'abxbus.event.status': event.event_status,
    'abxbus.event.result_count': event.event_results.size,
    'abxbus.event.error_count': event.event_errors.length,
    'abxbus.event.child_count': event.event_children.length,
  })
}

function handlerSpanAttributes(eventbus: EventBus, event: BaseEvent, event_result: EventResult): SpanAttributes {
  return compactAttributes({
    'abxbus.bus.id': eventbus.id,
    'abxbus.bus.name': eventbus.name,
    'abxbus.event.id': event.event_id,
    'abxbus.event.type': event.event_type,
    'abxbus.handler.id': event_result.handler_id,
    'abxbus.handler.name': event_result.handler_name,
    'abxbus.handler.file_path': event_result.handler_file_path,
    'abxbus.handler.event_pattern': event_result.handler.event_pattern,
    'abxbus.event_result.id': event_result.id,
    'abxbus.event_result.status': event_result.status,
    'abxbus.handler.child_count': event_result.event_children.length,
  })
}

function rootSpanAttributes(
  root_span_attributes: OtelTracingMiddlewareOptions['root_span_attributes'],
  eventbus: EventBus,
  event: BaseEvent
): SpanAttributes {
  const session_id = stringValue((event as { session_id?: unknown }).session_id)
  return compactAttributes({
    ...resolveAttributes(root_span_attributes, eventbus, event),
    'abxbus.trace.root': true,
    'abxbus.bus.id': eventbus.id,
    'abxbus.bus.name': eventbus.name,
    'abxbus.root_event.id': event.event_id,
    'abxbus.root_event.type': event.event_type,
    'abxbus.root_event.session_id': session_id,
    'abxbus.root_event.status': event.event_status,
    'abxbus.root_event.error_count': event.event_errors.length,
    'abxbus.root_event.child_count': event.event_children.length,
  })
}

function rootEventForEvent(eventbus: EventBus, event: BaseEvent): BaseEvent {
  let current = event._event_original ?? event
  const seen = new Set<string>()
  while (current.event_parent_id && !seen.has(current.event_id)) {
    seen.add(current.event_id)
    const parent = eventbus.findEventById(current.event_parent_id)
    if (!parent) {
      break
    }
    current = parent._event_original ?? parent
  }
  return current
}

function parentSpanContextForEvent(event: BaseEvent, trace_id: string): SpanContext {
  if (!event.event_parent_id) {
    return rootSpanContext(trace_id, event.event_id)
  }

  if (event.event_emitted_by_handler_id) {
    return handlerSpanContext(trace_id, event.event_parent_id, event.event_emitted_by_handler_id)
  }

  return eventSpanContext(trace_id, event.event_parent_id)
}

function rootSpanContext(trace_id: string, event_id: string): SpanContext {
  return {
    traceId: trace_id,
    spanId: deterministicSpanId(`abxbus.root:${event_id}`),
    traceFlags: 1,
  }
}

function eventSpanContext(trace_id: string, event_id: string): SpanContext {
  return {
    traceId: trace_id,
    spanId: deterministicSpanId(`abxbus.event:${event_id}`),
    traceFlags: 1,
  }
}

function handlerSpanContext(trace_id: string, event_id: string, handler_id: string): SpanContext {
  return {
    traceId: trace_id,
    spanId: deterministicSpanId(`abxbus.handler:${event_id}:${handler_id}`),
    traceFlags: 1,
  }
}

function traceIdForRootEvent(event_id: string): string {
  return `${fnv1a64Hex(`abxbus.trace.a:${event_id}`)}${fnv1a64Hex(`abxbus.trace.b:${event_id}`)}`
}

function deterministicSpanId(input: string): string {
  return fnv1a64Hex(input)
}

function fnv1a64Hex(input: string): string {
  let hash = 0xcbf29ce484222325n
  const prime = 0x100000001b3n
  const mask = 0xffffffffffffffffn
  for (let index = 0; index < input.length; index += 1) {
    hash ^= BigInt(input.charCodeAt(index))
    hash = (hash * prime) & mask
  }
  if (hash === 0n) {
    hash = 1n
  }
  return hash.toString(16).padStart(16, '0')
}

function dateFromIso(value: string | null | undefined): Date | undefined {
  if (value == null) {
    return undefined
  }
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? undefined : date
}

function endTimeAfterStart(start_time: Date | undefined, end_time: Date | undefined): Date | undefined {
  if (!start_time || !end_time) {
    return end_time
  }

  return end_time.getTime() > start_time.getTime() ? end_time : new Date(start_time.getTime() + 1)
}

function resolveRootSpanName(root_span_name: OtelTracingMiddlewareOptions['root_span_name'], eventbus: EventBus, event: BaseEvent): string {
  if (typeof root_span_name === 'function') {
    return root_span_name(eventbus, event)
  }
  return root_span_name ?? `abxbus.trace ${eventbus.name}`
}

function resolveAttributes(
  attributes: OtelTracingMiddlewareOptions['root_span_attributes'],
  eventbus: EventBus,
  event: BaseEvent
): SpanAttributes {
  return typeof attributes === 'function' ? attributes(eventbus, event) : (attributes ?? {})
}

function stringValue(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined
}

function compactAttributes(attributes: Record<string, SpanAttributeValue | null | undefined>): SpanAttributes {
  const compacted: SpanAttributes = {}
  for (const [key, value] of Object.entries(attributes)) {
    if (value !== null && value !== undefined) {
      compacted[key] = value
    }
  }
  return compacted
}

function recordSpanError(span: Span, error: unknown): void {
  if (error instanceof Error) {
    span.recordException(error)
    span.setStatus({ code: SpanStatusCode.ERROR, message: error.message })
    return
  }

  const message = typeof error === 'string' ? error : 'Unknown abxbus handler error'
  span.recordException(message)
  span.setStatus({ code: SpanStatusCode.ERROR, message })
}
