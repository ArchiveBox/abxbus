/* eslint-disable @typescript-eslint/no-unused-vars */
// Do not remove the unused type/const names below; they are used to test type inference at compile time.

import { z } from 'zod'

import { BaseEvent } from '../src/BaseEvent.js'
import { EventBus } from '../src/EventBus.js'
import { events_suck } from '../src/events_suck.js'
import type { EventResult } from '../src/EventResult.js'
import type { EventClass, EventResultType } from '../src/types.js'

type IsEqual<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false
type Assert<T extends true> = T

const InferableResultEvent = BaseEvent.extend('InferableResultEvent', {
  target_id: z.string(),
  event_result_type: z.object({ ok: z.boolean() }),
})

const InferableZodObjectResultEvent = BaseEvent.extend(
  'InferableZodObjectResultEvent',
  z.object({
    target_id: z.string(),
    event_result_type: z.object({ ok: z.boolean() }),
  })
)
const StaticSchemaField = z.literal('abc').default('abc')
const StaticSchemaResult = z.string()
const StaticSchemaTimeout = z.number().default(25)
const StaticSchemaEvent = BaseEvent.extend(
  'StaticSchemaEventForInference',
  z.object({
    some_field: StaticSchemaField,
    length: z.number().default(3),
    event_timeout: StaticSchemaTimeout,
    event_result_type: StaticSchemaResult,
  })
)
const StaticShortcutField = z.string().default('shortcut')
const StaticShortcutResult = z.number()
const StaticShortcutObjectField: { mode: 'exact'; nested: { count: number } } = { mode: 'exact', nested: { count: 2 } }
const StaticShortcutTupleField: ['ready', { retries: number }] = ['ready', { retries: 1 }]
const StaticShortcutEvent = BaseEvent.extend('StaticShortcutEventForInference', {
  shortcut_field: StaticShortcutField,
  raw_shortcut_field: 'abc',
  raw_object_shortcut_field: StaticShortcutObjectField,
  raw_tuple_shortcut_field: StaticShortcutTupleField,
  event_timeout: 2000,
  event_result_type: StaticShortcutResult,
})
const static_schema_default_event = StaticSchemaEvent()
const static_shortcut_default_event = StaticShortcutEvent()
const exported_static_schema_event_class: EventClass<InstanceType<typeof StaticSchemaEvent>> = StaticSchemaEvent
const exported_static_schema_event_instance: BaseEvent = static_schema_default_event
const exported_static_schema_event_instance_type: InstanceType<typeof BaseEvent> = static_schema_default_event

type InferableResult = EventResultType<InstanceType<typeof InferableResultEvent>>
type _assert_inferable_result = Assert<IsEqual<InferableResult, { ok: boolean }>>
type InferableZodObjectResult = EventResultType<InstanceType<typeof InferableZodObjectResultEvent>>
type _assert_inferable_zod_object_result = Assert<IsEqual<InferableZodObjectResult, { ok: boolean }>>
type _assert_static_schema_model_field = Assert<IsEqual<typeof StaticSchemaEvent.model_fields.some_field, typeof StaticSchemaField>>
type _assert_static_schema_model_length_field = Assert<IsEqual<typeof StaticSchemaEvent.model_fields.length, z.ZodDefault<z.ZodNumber>>>
type _assert_static_schema_model_builtin_override_field = Assert<
  IsEqual<typeof StaticSchemaEvent.model_fields.event_timeout, typeof StaticSchemaTimeout>
>
type _assert_static_schema_field = Assert<IsEqual<typeof StaticSchemaEvent.some_field, 'abc'>>
type _assert_static_schema_length_field = Assert<IsEqual<typeof StaticSchemaEvent.length, number>>
type _assert_static_schema_builtin_override_field = Assert<IsEqual<typeof StaticSchemaEvent.event_timeout, number>>
type _assert_static_schema_result_schema = Assert<IsEqual<typeof StaticSchemaEvent.event_result_type, typeof StaticSchemaResult>>
type _assert_static_schema_model_result_schema = Assert<
  IsEqual<typeof StaticSchemaEvent.model_fields.event_result_type, typeof StaticSchemaResult>
>
const _exported_event_class_schema: z.ZodTypeAny = exported_static_schema_event_class.event_schema
const _exported_event_class_model_fields: z.ZodRawShape = exported_static_schema_event_class.model_fields
const _exported_event_class_type: string | undefined = exported_static_schema_event_class.event_type
const _exported_event_class_version: string = exported_static_schema_event_class.event_version
const _exported_event_class_result_schema: z.ZodTypeAny | undefined = exported_static_schema_event_class.event_result_type
const _exported_event_class_from_json: InstanceType<typeof StaticSchemaEvent> = exported_static_schema_event_class.fromJSON(
  static_schema_default_event.toJSON()
)
type _assert_static_schema_instance_default = Assert<IsEqual<typeof static_schema_default_event.some_field, 'abc'>>
type _assert_static_schema_instance_length_default = Assert<IsEqual<typeof static_schema_default_event.length, number>>
type _assert_static_schema_instance_builtin_default = Assert<IsEqual<typeof static_schema_default_event.event_timeout, number | null>>
type _assert_static_shortcut_model_field = Assert<
  IsEqual<typeof StaticShortcutEvent.model_fields.shortcut_field, typeof StaticShortcutField>
>
type _assert_static_shortcut_raw_model_field = Assert<
  IsEqual<typeof StaticShortcutEvent.model_fields.raw_shortcut_field, z.ZodDefault<z.ZodLiteral<'abc'>>>
>
type _assert_static_shortcut_raw_object_model_field = Assert<
  IsEqual<typeof StaticShortcutEvent.model_fields.raw_object_shortcut_field, z.ZodDefault<z.ZodType<typeof StaticShortcutObjectField>>>
>
type _assert_static_shortcut_raw_tuple_model_field = Assert<
  IsEqual<typeof StaticShortcutEvent.model_fields.raw_tuple_shortcut_field, z.ZodDefault<z.ZodType<typeof StaticShortcutTupleField>>>
>
type _assert_static_shortcut_field = Assert<IsEqual<typeof StaticShortcutEvent.shortcut_field, string>>
type _assert_static_shortcut_raw_field = Assert<IsEqual<typeof StaticShortcutEvent.raw_shortcut_field, 'abc'>>
type _assert_static_shortcut_raw_object_field = Assert<
  IsEqual<typeof StaticShortcutEvent.raw_object_shortcut_field, typeof StaticShortcutObjectField>
>
type _assert_static_shortcut_raw_tuple_field = Assert<
  IsEqual<typeof StaticShortcutEvent.raw_tuple_shortcut_field, typeof StaticShortcutTupleField>
>
type _assert_static_shortcut_model_timeout = Assert<
  IsEqual<typeof StaticShortcutEvent.model_fields.event_timeout, z.ZodDefault<z.ZodNullable<z.ZodNumber>>>
>
type _assert_static_shortcut_timeout = Assert<IsEqual<typeof StaticShortcutEvent.event_timeout, 2000>>
type _assert_static_shortcut_result_schema = Assert<IsEqual<typeof StaticShortcutEvent.event_result_type, typeof StaticShortcutResult>>
type _assert_static_shortcut_model_result_schema = Assert<
  IsEqual<typeof StaticShortcutEvent.model_fields.event_result_type, typeof StaticShortcutResult>
>
type _assert_static_shortcut_instance_default = Assert<IsEqual<typeof static_shortcut_default_event.shortcut_field, string>>
type _assert_static_shortcut_raw_instance_default = Assert<IsEqual<typeof static_shortcut_default_event.raw_shortcut_field, 'abc'>>
type _assert_static_shortcut_raw_object_instance_default = Assert<
  IsEqual<typeof static_shortcut_default_event.raw_object_shortcut_field, typeof StaticShortcutObjectField>
>
type _assert_static_shortcut_raw_tuple_instance_default = Assert<
  IsEqual<typeof static_shortcut_default_event.raw_tuple_shortcut_field, typeof StaticShortcutTupleField>
>
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

bus.on(InferableZodObjectResultEvent, (event) => {
  const target: string = event.target_id
  return { ok: target.length > 0 }
})

// @ts-expect-error z.object event_result_type must also enforce handler return shape
bus.on(InferableZodObjectResultEvent, () => 'not-ok')

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

if (false) {
  // @ts-expect-error missing required InferableResultEvent field
  wrapped_client.create({})
}

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
