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
  type TimeInput,
  type Tracer,
} from '@opentelemetry/api'
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http'
import { resourceFromAttributes } from '@opentelemetry/resources'
import { BasicTracerProvider, BatchSpanProcessor } from '@opentelemetry/sdk-trace-base'
import type { SpanLimits, SpanProcessor } from '@opentelemetry/sdk-trace-base'
import { SpanImpl } from '@opentelemetry/sdk-trace-base/build/src/Span.js'

import type { BaseEvent } from './BaseEvent.js'
import type { EventBus } from './EventBus.js'
import type { EventResult } from './EventResult.js'
import type { EventBusMiddleware } from './EventBusMiddleware.js'
import type { EventStatus } from './types.js'

type OpenTelemetryTraceApi = Pick<typeof trace, 'getTracer' | 'setSpan'> & Partial<Pick<typeof trace, 'setSpanContext'>>

export type OtelTracingSpanFactoryInput = {
  name: string
  span_context: SpanContext
  parent_span_context?: SpanContext
  attributes: SpanAttributes
  start_time?: TimeInput
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
  root_span_attributes?: SpanAttributes | ((eventbus: EventBus, event: BaseEvent) => SpanAttributes)
}

export class OtelTracingMiddleware implements EventBusMiddleware {
  private readonly tracer: Tracer
  private readonly trace_api: OpenTelemetryTraceApi
  private readonly span_factory?: OtelTracingSpanFactory
  private readonly span_provider?: OtelTracingSpanProvider
  private readonly root_span_attributes: OtelTracingMiddlewareOptions['root_span_attributes']
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

    const parent_context = this.parentContextForEvent(event) ?? ROOT_CONTEXT
    const start_time = timeInputFromIso(event.event_started_at)
    const span = this.tracer.startSpan(
      eventSpanName(eventbus, event),
      {
        attributes: event.event_parent_id
          ? eventStartedSpanAttributes(eventbus, event)
          : topLevelEventStartedSpanAttributes(this.root_span_attributes, eventbus, event),
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
      event.event_parent_id ? eventSpanAttributes(eventbus, event) : topLevelEventSpanAttributes(this.root_span_attributes, eventbus, event)
    )
    const start_time = epochNsFromIso(event.event_started_at)
    const end_time = endTimeAfterStart(start_time, epochNsFromIso(event.event_completed_at))
    span.end(end_time)
    this.event_spans.delete(event.event_id)
    this.event_contexts.delete(event.event_id)
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
        attributes: handlerSpanAttributes(eventbus, event, event_result),
        startTime: timeInputFromIso(event_result.started_at),
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
    span.setAttributes(handlerSpanAttributes(eventbus, event, event_result))
    span.end(endTimeAfterStart(epochNsFromIso(event_result.started_at), epochNsFromIso(event_result.completed_at)))
    this.handler_spans.delete(event_result.id)
    this.handler_contexts.delete(handlerSpanKey(event_result.event_id, event_result.handler_id))
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
    if (event.event_parent_id) {
      return
    }

    const top_level_event = event._event_original ?? event
    const trace_id = traceIdForRootEvent(top_level_event.event_id)
    this.exportEventTreeWithFactory(eventbus, top_level_event, trace_id, undefined, new Set<string>())
  }

  private exportEventTreeWithFactory(
    eventbus: EventBus,
    event: BaseEvent,
    trace_id: string,
    parent_span_context: SpanContext | undefined,
    visited_event_ids: Set<string>
  ): void {
    const original_event = event._event_original ?? event
    if (visited_event_ids.has(original_event.event_id)) {
      return
    }
    visited_event_ids.add(original_event.event_id)

    const start_time = epochNsFromIso(original_event.event_started_at)
    const span_context = eventSpanContext(trace_id, original_event.event_id)
    const span = this.span_factory!({
      name: eventSpanName(eventbus, original_event),
      span_context,
      parent_span_context,
      attributes: original_event.event_parent_id
        ? eventSpanAttributes(eventbus, original_event)
        : topLevelEventSpanAttributes(this.root_span_attributes, eventbus, original_event),
      start_time: timeInputFromEpochNs(start_time),
    })
    if (original_event.event_errors.length > 0) {
      recordSpanError(span, original_event.event_errors[0])
    } else {
      span.setStatus({ code: SpanStatusCode.OK })
    }
    span.end(endTimeAfterStart(start_time, epochNsFromIso(original_event.event_completed_at)))

    for (const event_result of original_event.event_results.values()) {
      const handler_context = this.exportHandlerSpanWithFactory(eventbus, original_event, event_result, trace_id, span_context)
      for (const child of event_result.event_children) {
        this.exportEventTreeWithFactory(eventbus, child, trace_id, handler_context, visited_event_ids)
      }
    }
  }

  private exportHandlerSpanWithFactory(
    eventbus: EventBus,
    event: BaseEvent,
    event_result: EventResult,
    trace_id: string,
    parent_span_context: SpanContext
  ): SpanContext {
    const start_time = epochNsFromIso(event_result.started_at)
    const span_context = handlerSpanContext(trace_id, event_result.event_id, event_result.handler_id)
    const span = this.span_factory!({
      name: handlerSpanName(event, event_result),
      span_context,
      parent_span_context,
      attributes: handlerSpanAttributes(eventbus, event, event_result),
      start_time: timeInputFromEpochNs(start_time),
    })
    if (event_result.error !== undefined) {
      recordSpanError(span, event_result.error)
    } else {
      span.setStatus({ code: SpanStatusCode.OK })
    }
    span.end(endTimeAfterStart(start_time, epochNsFromIso(event_result.completed_at)))
    return span_context
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
      new BatchSpanProcessor(
        new OTLPTraceExporter({
          url: normalizeOtlpTracesEndpoint(options.otlp_endpoint!),
        }),
        {
          scheduledDelayMillis: 100,
        }
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

function eventStartedSpanAttributes(eventbus: EventBus, event: BaseEvent): SpanAttributes {
  return compactAttributes({
    'abxbus.event_bus.id': eventbus.id,
    'abxbus.event_bus.name': eventbus.name,
    'abxbus.event_id': event.event_id,
    'abxbus.event_type': event.event_type,
    'abxbus.event_version': event.event_version,
    'abxbus.session_id': stringValue((event as { session_id?: unknown }).session_id),
    'abxbus.event_parent_id': event.event_parent_id,
    'abxbus.event_emitted_by_handler_id': event.event_emitted_by_handler_id,
    'abxbus.event_path': event.event_path.join(' '),
  })
}

function eventSpanAttributes(eventbus: EventBus, event: BaseEvent): SpanAttributes {
  return compactAttributes({
    ...eventStartedSpanAttributes(eventbus, event),
    'abxbus.event_status': event.event_status,
  })
}

function handlerSpanAttributes(eventbus: EventBus, event: BaseEvent, event_result: EventResult): SpanAttributes {
  return compactAttributes({
    'abxbus.event_bus.id': eventbus.id,
    'abxbus.event_bus.name': eventbus.name,
    'abxbus.event_id': event.event_id,
    'abxbus.event_type': event.event_type,
    'abxbus.handler_id': event_result.handler_id,
    'abxbus.handler_name': event_result.handler_name,
    'abxbus.handler_file_path': event_result.handler_file_path,
    'abxbus.handler_event_pattern': event_result.handler.event_pattern,
    'abxbus.event_result_id': event_result.id,
    'abxbus.event_result_status': event_result.status,
  })
}

function topLevelEventStartedSpanAttributes(
  root_span_attributes: OtelTracingMiddlewareOptions['root_span_attributes'],
  eventbus: EventBus,
  event: BaseEvent
): SpanAttributes {
  return compactAttributes({
    ...eventStartedSpanAttributes(eventbus, event),
    ...resolveAttributes(root_span_attributes, eventbus, event),
    'abxbus.trace.root': true,
  })
}

function topLevelEventSpanAttributes(
  root_span_attributes: OtelTracingMiddlewareOptions['root_span_attributes'],
  eventbus: EventBus,
  event: BaseEvent
): SpanAttributes {
  return compactAttributes({
    ...eventSpanAttributes(eventbus, event),
    ...resolveAttributes(root_span_attributes, eventbus, event),
    'abxbus.trace.root': true,
  })
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

const ISO_EPOCH_NS_REGEX = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$/
const NS_PER_SECOND = 1_000_000_000n
const NS_PER_MS = 1_000_000n

function epochNsFromIso(value: string | null | undefined): bigint | undefined {
  if (value == null) {
    return undefined
  }
  const match = ISO_EPOCH_NS_REGEX.exec(value)
  if (!match) {
    return undefined
  }
  const [, base, fraction = '', timezone] = match
  const base_ms = Date.parse(`${base}.000${timezone}`)
  if (Number.isNaN(base_ms)) {
    return undefined
  }
  return BigInt(base_ms) * NS_PER_MS + BigInt(fraction.padEnd(9, '0'))
}

function timeInputFromIso(value: string | null | undefined): TimeInput | undefined {
  return timeInputFromEpochNs(epochNsFromIso(value))
}

function timeInputFromEpochNs(epoch_ns: bigint | undefined): TimeInput | undefined {
  if (epoch_ns === undefined) {
    return undefined
  }
  const seconds = epoch_ns / NS_PER_SECOND
  const nanos = epoch_ns % NS_PER_SECOND
  return [Number(seconds), Number(nanos)]
}

function endTimeAfterStart(start_time: bigint | undefined, end_time: bigint | undefined): TimeInput | undefined {
  if (start_time === undefined || end_time === undefined) {
    return timeInputFromEpochNs(end_time)
  }

  return timeInputFromEpochNs(end_time > start_time ? end_time : start_time + 1n)
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
