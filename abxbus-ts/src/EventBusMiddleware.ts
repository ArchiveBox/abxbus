import type { BaseEvent } from './BaseEvent.js'
import type { EventBus } from './EventBus.js'
import type { EventHandler } from './EventHandler.js'
import type { EventResult } from './EventResult.js'
import type { EventStatus } from './types.js'

export type { EventStatus } from './types.js'

export interface EventBusMiddleware {
  onEventChange?(eventbus: EventBus, event: BaseEvent, status: EventStatus): void | Promise<void>
  onEventResultChange?(eventbus: EventBus, event: BaseEvent, event_result: EventResult, status: EventStatus): void | Promise<void>
  onBusHandlersChange?(eventbus: EventBus, handler: EventHandler, registered: boolean): void | Promise<void>
}

export type EventBusMiddlewareCtor = new () => EventBusMiddleware
export type EventBusMiddlewareInput = EventBusMiddleware | EventBusMiddlewareCtor
