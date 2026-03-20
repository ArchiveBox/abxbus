import { EventBus } from './event_bus.js'
import { BaseEvent } from './base_event.js'

import type { EventClass, EventResultType } from './types.js'

type EventMap = Record<string, EventClass<BaseEvent>>
type AnyFn = (...args: any[]) => any
type FunctionMap = Record<string, AnyFn>
type ExtraDict = Record<string, unknown>

type EventFieldsFromFn<TFunc extends AnyFn> =
  Parameters<TFunc> extends [infer TArg] ? (TArg extends Record<string, unknown> ? TArg : ExtraDict) : ExtraDict

type GeneratedEvent<TFunc extends AnyFn> = {
  (
    data: EventFieldsFromFn<TFunc> & ExtraDict
  ): BaseEvent & EventFieldsFromFn<TFunc> & { __event_result_type__?: Awaited<ReturnType<TFunc>> }
  new (
    data: EventFieldsFromFn<TFunc> & ExtraDict
  ): BaseEvent & EventFieldsFromFn<TFunc> & { __event_result_type__?: Awaited<ReturnType<TFunc>> }
  event_type?: string
}

export type GeneratedEvents<TEvents extends FunctionMap> = {
  by_name: { [K in keyof TEvents]: GeneratedEvent<TEvents[K]> }
} & {
  [K in keyof TEvents]: GeneratedEvent<TEvents[K]>
}

type EventInit<TEventClass extends EventClass<BaseEvent>> =
  ConstructorParameters<TEventClass> extends [infer TInit, ...unknown[]] ? TInit : never

type EventMethodArgs<TEventClass extends EventClass<BaseEvent>> =
  {} extends EventInit<TEventClass>
    ? [init?: EventInit<TEventClass>, extra?: Record<string, unknown>]
    : [init: EventInit<TEventClass>, extra?: Record<string, unknown>]

type EventMethodResult<TEventClass extends EventClass<BaseEvent>> = EventResultType<InstanceType<TEventClass>> | undefined

export type EventsSuckClient<TEvents extends EventMap> = {
  bus: EventBus
} & {
  [K in keyof TEvents]: (...args: EventMethodArgs<TEvents[K]>) => Promise<EventMethodResult<TEvents[K]>>
}

export type EventsSuckClientClass<TEvents extends EventMap> = new (bus?: EventBus) => EventsSuckClient<TEvents>

type DynamicWrappedClient = {
  bus: EventBus
} & Record<string, (...args: unknown[]) => Promise<unknown>>

export const make_events = <TEvents extends FunctionMap>(events: TEvents): GeneratedEvents<TEvents> => {
  const by_name = {} as { [K in keyof TEvents]: GeneratedEvent<TEvents[K]> }
  for (const [event_name] of Object.entries(events) as Array<[keyof TEvents, TEvents[keyof TEvents]]>) {
    if (!/^[A-Za-z_$][\w$]*$/.test(String(event_name))) {
      throw new Error(`Invalid event name: ${String(event_name)}`)
    }
    by_name[event_name] = BaseEvent.extend(String(event_name), {}) as unknown as GeneratedEvent<TEvents[keyof TEvents]>
  }
  return Object.assign({ by_name }, by_name) as GeneratedEvents<TEvents>
}

export const wrap = <TEvents extends EventMap>(class_name: string, methods: TEvents): EventsSuckClientClass<TEvents> => {
  class WrappedClient {
    bus: EventBus

    constructor(bus?: EventBus) {
      this.bus = bus ?? new EventBus(`${class_name}Bus`)
    }
  }

  Object.defineProperty(WrappedClient, 'name', { value: class_name })

  for (const [method_name, EventCtor] of Object.entries(methods)) {
    Object.defineProperty(WrappedClient.prototype, method_name, {
      value: async function (this: DynamicWrappedClient, init?: Record<string, unknown>, extra?: Record<string, unknown>) {
        const payload = { ...(init ?? {}), ...(extra ?? {}) }
        return await this.bus.emit(new EventCtor(payload)).first()
      },
      writable: true,
      configurable: true,
    })
  }

  return WrappedClient as unknown as EventsSuckClientClass<TEvents>
}

// Intentionally no make_event()/make_handler() helpers in TypeScript.
// Prefer the explicit inline pattern:
//   const FooCreateEvent = BaseEvent.extend('FooCreateEvent', {
//     id: z.string().nullable().optional(),
//     name: z.string(),
//     age: z.number(),
//   })
//   bus.on(FooCreateEvent, ({ id, name, age, ...extra }) => impl.create(id, { name, age }))
export const events_suck = { make_events, wrap } as const
