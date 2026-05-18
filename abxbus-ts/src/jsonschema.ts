import { z } from 'zod'

export type JsonSchema = boolean | z.core.JSONSchema.JSONSchema
type JsonSchemaObject = z.core.JSONSchema.JSONSchema

const isJsonSchemaObject = (value: unknown): value is JsonSchemaObject =>
  typeof value === 'object' && value !== null && !Array.isArray(value)

export const isJsonSchema = (value: unknown): value is JsonSchema => typeof value === 'boolean' || isJsonSchemaObject(value)

const nullUnionCandidates = (schema: Record<string, unknown>): Record<string, unknown>[] | null => {
  if (Array.isArray(schema.type) && schema.type.includes('null')) {
    const non_null_types = schema.type.filter((item): item is string => typeof item === 'string' && item !== 'null')
    if (non_null_types.length > 0) {
      return [{ type: non_null_types.length === 1 ? non_null_types[0] : non_null_types }, { type: 'null' }]
    }
  }

  const one_of = schema.oneOf
  if (Array.isArray(one_of) && one_of.length === 2) {
    const object_candidates = one_of.filter(isJsonSchemaObject)
    if (object_candidates.length === 2) {
      const null_candidates = object_candidates.filter((candidate) => candidate.type === 'null')
      const non_null_candidates = object_candidates.filter((candidate) => candidate.type !== 'null')
      if (null_candidates.length === 1 && non_null_candidates.length === 1) {
        return [non_null_candidates[0], { type: 'null' }]
      }
    }
  }

  return null
}

export const normalizeJsonSchema = (schema: JsonSchema): JsonSchema => {
  const normalized = normalizeJsonSchemaValue(schema)
  if (!isJsonSchemaObject(normalized)) {
    return normalized as JsonSchema
  }
  const schema_record = { ...normalized } as JsonSchemaObject
  const definitions = schema_record.$defs
  const root_ref = rootRefForSchema(schema_record, definitions)
  if (!root_ref || !definitions) {
    schema_record.$schema ??= 'https://json-schema.org/draft/2020-12/schema'
    return schema_record
  }
  const root_name = root_ref.slice('#/$defs/'.length)
  const root_schema = definitions[root_name]
  if (!isJsonSchemaObject(root_schema)) {
    schema_record.$schema ??= 'https://json-schema.org/draft/2020-12/schema'
    return schema_record
  }
  const rewritten_root = rewriteJsonSchemaRefs(root_schema, { [root_ref]: '#' }) as JsonSchemaObject
  const remaining_defs = Object.fromEntries(Object.entries(definitions).filter(([name]) => name !== root_name))
  if (Object.keys(remaining_defs).length > 0) {
    rewritten_root.$defs = rewriteJsonSchemaRefs(remaining_defs, { [root_ref]: '#' }) as Record<string, JsonSchemaObject>
  }
  rewritten_root.$schema ??= schema_record.$schema ?? 'https://json-schema.org/draft/2020-12/schema'
  setTitleFromInlinedRootDefinition(rewritten_root, root_name)
  return rewritten_root
}

const rootRefForSchema = (schema: JsonSchemaObject, definitions: Record<string, JsonSchemaObject> | undefined): string | null => {
  if (typeof schema.$ref === 'string' && schema.$ref.startsWith('#/$defs/')) {
    return schema.$ref
  }
  if (!definitions) {
    return null
  }
  const root = schemaWithoutSchemaAndDefinitions(schema)
  for (const [name, definition] of Object.entries(definitions)) {
    if (JSON.stringify(definition) === JSON.stringify(root)) {
      return `#/$defs/${name}`
    }
  }
  return null
}

const schemaWithoutSchemaAndDefinitions = (schema: JsonSchemaObject): Record<string, unknown> => {
  const root: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(schema)) {
    if (key !== '$schema' && key !== '$defs') {
      root[key] = value
    }
  }
  return root
}

const setTitleFromInlinedRootDefinition = (schema: JsonSchemaObject, root_name: string): void => {
  if (root_name.startsWith('__schema')) return
  schema.title ??= root_name
}

const rewriteJsonSchemaRefs = (schema: unknown, refs: Record<string, string>): unknown => {
  if (Array.isArray(schema)) {
    return schema.map((item) => rewriteJsonSchemaRefs(item, refs))
  }
  if (!isJsonSchemaObject(schema)) {
    return schema
  }
  const rewritten: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(schema)) {
    rewritten[key] = rewriteJsonSchemaRefs(value, refs)
  }
  if (typeof rewritten.$ref === 'string' && rewritten.$ref in refs) {
    rewritten.$ref = refs[rewritten.$ref]
  }
  return rewritten
}

const normalizeJsonSchemaValue = (schema: unknown): unknown => {
  if (Array.isArray(schema)) {
    return schema.map((item) => normalizeJsonSchemaValue(item))
  }
  if (!isJsonSchemaObject(schema)) {
    return schema
  }

  const normalized: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(schema)) {
    normalized[key] = normalizeJsonSchemaValue(value)
  }
  if (Array.isArray(normalized.required) && normalized.required.every((item) => typeof item === 'string')) {
    normalized.required = [...normalized.required].sort()
  }

  const null_union_candidates = nullUnionCandidates(normalized)
  if (null_union_candidates !== null) {
    const merged: Record<string, unknown> = { anyOf: normalizeJsonSchemaValue(null_union_candidates) }
    for (const [key, value] of Object.entries(normalized)) {
      if (key !== 'type' && key !== 'oneOf') {
        merged[key] = value
      }
    }
    return merged
  }

  return normalized
}

export const toJsonSchema = (schema: z.core.$ZodType): JsonSchema => {
  return normalizeJsonSchema(z.toJSONSchema(schema) as JsonSchema)
}

export const fromJsonSchema = (schema: JsonSchema): z.ZodTypeAny => {
  return z.fromJSONSchema(schema)
}
