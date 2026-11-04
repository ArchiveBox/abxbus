import { z } from 'zod'

export type JsonSchema = boolean | z.core.JSONSchema.JSONSchema
type JsonSchemaObject = z.core.JSONSchema.JSONSchema

const isJsonSchemaObject = (value: unknown): value is JsonSchemaObject => {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

export const isJsonSchema = (value: unknown): value is JsonSchema => {
  if (typeof value === 'boolean') {
    return true
  }
  if (!isJsonSchemaObject(value)) {
    return false
  }
  if ('_zod' in value || '_def' in value || '~standard' in value) {
    return false
  }
  return true
}

const typeUnionCandidates = (schema: Record<string, unknown>): Record<string, unknown>[] | null => {
  // Zod 4.6 compacts bare anyOf branches into type arrays. Keep our wire shape,
  // without replacing an independent anyOf constraint already on the schema.
  if (schema.anyOf !== undefined) return null
  if (Array.isArray(schema.type) && schema.type.includes('null')) {
    const non_null_types = schema.type.filter((item): item is string => typeof item === 'string' && item !== 'null')
    if (non_null_types.length > 0) {
      return [{ type: non_null_types.length === 1 ? non_null_types[0] : non_null_types }, { type: 'null' }]
    }
  }
  if (
    Array.isArray(schema.type) &&
    schema.type.length > 0 &&
    schema.type.every((item) => typeof item === 'string') &&
    Object.keys(schema).every((key) =>
      ['type', '$schema', 'title', 'description', 'default', 'examples', 'readOnly', 'writeOnly', 'deprecated'].includes(key)
    )
  ) {
    // Only bare type assertions were compacted. Keep constrained type arrays:
    // Zod's importer does not apply sibling type-specific constraints to anyOf.
    return schema.type.map((type) => ({ type }))
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

const normalizeJsonSchemaValue = (schema: unknown, schemaMap = false, nullableBranch = false): unknown => {
  if (Array.isArray(schema)) {
    return schema.map((item) => normalizeJsonSchemaValue(item, false, nullableBranch))
  }
  if (!isJsonSchemaObject(schema)) {
    return schema
  }

  const normalized: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(schema)) {
    // These keywords contain instance data, even when it resembles a schema.
    // A property *named* default/const/etc. still contains a schema, though.
    normalized[key] =
      !schemaMap && ['default', 'const', 'enum', 'examples'].includes(key)
        ? value
        : normalizeJsonSchemaValue(
            value,
            !schemaMap && ['$defs', 'definitions', 'properties', 'patternProperties', 'dependentSchemas'].includes(key),
            !schemaMap && key === 'anyOf' && Array.isArray(value) && value.some((item) => isJsonSchemaObject(item) && item.type === 'null')
          )
  }
  if (schemaMap) return normalized
  if (Array.isArray(normalized.required) && normalized.required.every((item) => typeof item === 'string')) {
    normalized.required = [...normalized.required].sort()
  }

  // New Zod imports emit explicit empty object/tuple keywords. Omit only the
  // redundant forms so shared Python/TS fixtures retain their canonical shape.
  if (normalized.type === 'object' && isJsonSchemaObject(normalized.properties) && Object.keys(normalized.properties).length === 0) {
    delete normalized.properties
  }
  if (
    normalized.type === 'array' &&
    Array.isArray(normalized.prefixItems) &&
    typeof normalized.maxItems === 'number' &&
    normalized.maxItems <= normalized.prefixItems.length &&
    isJsonSchemaObject(normalized.items) &&
    Object.keys(normalized.items).length === 0
  ) {
    // For an open tuple, items:{} marks extra items evaluated and can change
    // unevaluatedItems semantics. Here maxItems already forbids every extra item.
    delete normalized.items
  }

  // Python and existing fixtures group all non-null types into one nullable
  // branch. Preserve that group on subsequent normalization as well.
  const type_union_candidates = nullableBranch ? null : typeUnionCandidates(normalized)
  if (type_union_candidates !== null) {
    const merged: Record<string, unknown> = { anyOf: type_union_candidates }
    for (const [key, value] of Object.entries(normalized)) {
      if (key !== 'type') {
        merged[key] = value
      }
    }
    return merged
  }

  return normalized
}

export const toJsonSchema = (schema: z.core.$ZodType, params?: z.core.ToJSONSchemaParams): JsonSchema => {
  try {
    return normalizeJsonSchema(z.toJSONSchema(schema, params) as JsonSchema)
  } catch (error) {
    if (params?.io === 'input') {
      throw error
    }
    return normalizeJsonSchema(z.toJSONSchema(schema, { ...params, io: 'input' }) as JsonSchema)
  }
}

export const fromJsonSchema = (schema: JsonSchema): z.ZodTypeAny => {
  return z.fromJSONSchema(schema)
}
