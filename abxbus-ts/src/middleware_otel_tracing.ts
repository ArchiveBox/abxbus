import { ROOT_CONTEXT, SpanStatusCode, trace, type Context, type Span, type Tracer } from '@opentelemetry/api'

import type { BaseEvent } from './base_event.js'
import type { EventBus } from './event_bus.js'
import type { EventResult } from './event_result.js'
import type { EventBusMiddleware } from './middlewares.js'
import type { EventStatus } from './types.js'

type OpenTelemetryTraceApi = Pick<typeof trace, 'getTracer' | 'setSpan'>

export type OtelTracingMiddlewareOptions = {
  tracer?: Tracer
  trace_api?: OpenTelemetryTraceApi
}

export class OtelTracingMiddleware implements EventBusMiddleware {
  private readonly tracer: Tracer
  private readonly trace_api: OpenTelemetryTraceApi
  private readonly event_spans = new Map<string, Span>()
  private readonly event_contexts = new Map<string, Context>()
  private readonly handler_spans = new Map<string, Span>()
  private readonly handler_contexts = new Map<string, Context>()

  constructor(options: OtelTracingMiddlewareOptions = {}) {
    this.trace_api = options.trace_api ?? trace
    this.tracer = options.tracer ?? this.trace_api.getTracer('abxbus')
  }

  onEventChange(eventbus: EventBus, event: BaseEvent, status: EventStatus): void {
    if (status === 'started') {
      this.startEventSpan(eventbus, event)
      return
    }

    if (status === 'completed') {
      this.completeEventSpan(eventbus, event)
    }
  }

  onEventResultChange(eventbus: EventBus, event: BaseEvent, event_result: EventResult, status: EventStatus): void {
    if (status === 'started') {
      this.startHandlerSpan(eventbus, event, event_result)
      return
    }

    if (status === 'completed') {
      this.completeHandlerSpan(event_result)
    }
  }

  private startEventSpan(eventbus: EventBus, event: BaseEvent): Span {
    const existing = this.event_spans.get(event.event_id)
    if (existing) {
      return existing
    }

    const parent_context = this.parentContextForEvent(event) ?? ROOT_CONTEXT
    const span = this.tracer.startSpan(
      `abxbus.event ${event.event_type}`,
      {
        attributes: compactAttributes({
          'abxbus.bus.id': eventbus.id,
          'abxbus.bus.name': eventbus.name,
          'abxbus.event.id': event.event_id,
          'abxbus.event.type': event.event_type,
          'abxbus.event.version': event.event_version,
          'abxbus.event.parent_id': event.event_parent_id,
          'abxbus.event.emitted_by_handler_id': event.event_emitted_by_handler_id,
          'abxbus.event.path': event.event_path.join(' '),
        }),
        startTime: dateFromIso(event.event_started_at),
      },
      parent_context
    )
    const span_context = this.trace_api.setSpan(parent_context, span)
    this.event_spans.set(event.event_id, span)
    this.event_contexts.set(event.event_id, span_context)
    return span
  }

  private completeEventSpan(eventbus: EventBus, event: BaseEvent): void {
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
    span.end(dateFromIso(event.event_completed_at))
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
      `abxbus.handler ${event.event_type} ${event_result.handler_name}`,
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

  private completeHandlerSpan(event_result: EventResult): void {
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
    span.end(dateFromIso(event_result.completed_at))
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
}

function handlerSpanKey(event_id: string, handler_id: string): string {
  return `${event_id}:${handler_id}`
}

function dateFromIso(value: string | null | undefined): Date | undefined {
  if (value == null) {
    return undefined
  }
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? undefined : date
}

function compactAttributes(
  attributes: Record<string, string | number | boolean | null | undefined>
): Record<string, string | number | boolean> {
  const compacted: Record<string, string | number | boolean> = {}
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
