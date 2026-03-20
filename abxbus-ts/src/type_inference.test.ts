/* eslint-disable @typescript-eslint/no-unused-vars */
// Do not remove the unused type/const names below; they are used to test type inference at compile time.

import { z } from 'zod'

import { BaseEvent } from './base_event.js'
import { EventBus } from './event_bus.js'
import { events_suck } from './events_suck.js'
import type { EventResult } from './event_result.js'
import type { EventResultType } from './types.js'

type IsEqual<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false
type Assert<T extends true> = T

const InferableResultEvent = BaseEvent.extend('InferableResultEvent', {
  target_id: z.string(),
  event_result_type: z.object({ ok: z.boolean() }),
})

type InferableResult = EventResultType<InstanceType<typeof InferableResultEvent>>
type _assert_inferable_result = Assert<IsEqual<InferableResult, { ok: boolean }>>
type InferableEventResultEntry =
  InstanceType<typeof InferableResultEvent>['event_results'] extends Map<string, infer TResultEntry> ? TResultEntry : never
type _assert_inferable_event_result_entry = Assert<
  IsEqual<InferableEventResultEntry, EventResult<InstanceType<typeof InferableResultEvent>>>
>
type InferableEventResultValue = InferableEventResultEntry extends { result?: infer TResultValue } ? TResultValue : never
type _assert_inferable_event_result_value = Assert<IsEqual<InferableEventResultValue, { ok: boolean }>>

const NoSchemaEvent = BaseEvent.extend('NoSchemaEventForInference', {})
type NoSchemaResult = EventResultType<InstanceType<typeof NoSchemaEvent>>
type _assert_no_schema_result = Assert<IsEqual<NoSchemaResult, unknown>>

const ConstructorStringResultEvent = BaseEvent.extend('ConstructorStringResultEventForInference', {
  event_result_type: String,
})
type ConstructorStringResult = EventResultType<InstanceType<typeof ConstructorStringResultEvent>>
type _assert_constructor_string_result = Assert<IsEqual<ConstructorStringResult, string>>

const ConstructorNumberResultEvent = BaseEvent.extend('ConstructorNumberResultEventForInference', {
  event_result_type: Number,
})
type ConstructorNumberResult = EventResultType<InstanceType<typeof ConstructorNumberResultEvent>>
type _assert_constructor_number_result = Assert<IsEqual<ConstructorNumberResult, number>>

const ConstructorBooleanResultEvent = BaseEvent.extend('ConstructorBooleanResultEventForInference', {
  event_result_type: Boolean,
})
type ConstructorBooleanResult = EventResultType<InstanceType<typeof ConstructorBooleanResultEvent>>
type _assert_constructor_boolean_result = Assert<IsEqual<ConstructorBooleanResult, boolean>>

const ConstructorArrayResultEvent = BaseEvent.extend('ConstructorArrayResultEventForInference', {
  event_result_type: Array,
})
type ConstructorArrayResult = EventResultType<InstanceType<typeof ConstructorArrayResultEvent>>
type _assert_constructor_array_result = Assert<IsEqual<ConstructorArrayResult, unknown[]>>

const ConstructorObjectResultEvent = BaseEvent.extend('ConstructorObjectResultEventForInference', {
  event_result_type: Object,
})
type ConstructorObjectResult = EventResultType<InstanceType<typeof ConstructorObjectResultEvent>>
type _assert_constructor_object_result = Assert<IsEqual<ConstructorObjectResult, Record<string, unknown>>>

const bus = new EventBus('TypeInferenceBus')

const find_by_class_call = bus.find(InferableResultEvent, { past: true, future: false })
type FindByClassReturn = Awaited<typeof find_by_class_call>
type _assert_find_by_class_return = Assert<IsEqual<FindByClassReturn, InstanceType<typeof InferableResultEvent> | null>>

const find_by_class_with_where_call = bus.find(
  InferableResultEvent,
  (event) => {
    const target: string = event.target_id
    return target.length > 0
  },
  { past: true, future: false }
)
type FindByClassWithWhereReturn = Awaited<typeof find_by_class_with_where_call>
type _assert_find_by_class_with_where_return = Assert<IsEqual<FindByClassWithWhereReturn, InstanceType<typeof InferableResultEvent> | null>>

const find_history_by_class_call = bus.event_history.find(InferableResultEvent, (event) => event.target_id.length > 0, { past: true })
type FindHistoryByClassReturn = Awaited<typeof find_history_by_class_call>
type _assert_find_history_by_class_return = Assert<IsEqual<FindHistoryByClassReturn, InstanceType<typeof InferableResultEvent> | null>>

const find_by_wildcard_call = bus.find('*', { past: true, future: false })
type FindByWildcardReturn = Awaited<typeof find_by_wildcard_call>
type _assert_find_by_wildcard_return = Assert<IsEqual<FindByWildcardReturn, BaseEvent | null>>

bus.on(InferableResultEvent, (event) => {
  const target: string = event.target_id
  return { ok: true }
})

bus.on(InferableResultEvent, () => undefined)

// @ts-expect-error non-void return must match event_result_type for inferable event keys
bus.on(InferableResultEvent, () => 'not-ok')

// String/wildcard keys remain best-effort and do not strongly enforce return shapes.
bus.on('InferableResultEvent', () => 'anything')
bus.on('*', () => 123)

const WrappedClient = events_suck.wrap('WrappedClient', {
  create: InferableResultEvent,
  update: ConstructorBooleanResultEvent,
})

const wrapped_client = new WrappedClient(new EventBus('WrappedClientBus'))

const wrapped_create_call = wrapped_client.create({ target_id: 'abc-123' }, { debug_tag: 'create' })
type WrappedCreateReturn = Awaited<typeof wrapped_create_call>
type _assert_wrapped_create_return = Assert<IsEqual<WrappedCreateReturn, { ok: boolean } | undefined>>

const wrapped_update_call = wrapped_client.update()
type WrappedUpdateReturn = Awaited<typeof wrapped_update_call>
type _assert_wrapped_update_return = Assert<IsEqual<WrappedUpdateReturn, boolean | undefined>>

// @ts-expect-error missing required InferableResultEvent field
wrapped_client.create({})

const make_events_demo = events_suck.make_events({
  FooBarAPIObjEvent: (payload: { id: string; age?: number }) => payload.id.length > 0,
})

const generated_event = make_events_demo.FooBarAPIObjEvent({ id: 'abc' })
const _generated_event_id: string = generated_event.id
bus.on(make_events_demo.FooBarAPIObjEvent, (event) => {
  const id: string = event.id
  return id.length > 0
})
// @ts-expect-error event_result_type inferred from make_events() function return type (boolean)
bus.on(make_events_demo.FooBarAPIObjEvent, () => 'not-boolean')
