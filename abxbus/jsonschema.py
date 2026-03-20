import inspect
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, TypeAlias, cast

from pydantic import BaseModel, Field, TypeAdapter, create_model

_SCHEMA_TYPE_REGISTRY: tuple[tuple[str, type[Any], str], ...] = (
    ('string', str, 'string'),
    ('integer', int, 'number'),  # note both integer and number are mapped to the same JSON Schema type
    ('number', float, 'number'),
    ('boolean', bool, 'boolean'),
    ('object', dict, 'object'),
    ('array', list, 'array'),
    ('null', type(None), 'null'),
)

TYPE_MAPPING: dict[str, type[Any]] = {schema_type: python_type for schema_type, python_type, _ in _SCHEMA_TYPE_REGISTRY}

CONSTRAINT_MAPPING: dict[str, str] = {
    'minimum': 'ge',
    'maximum': 'le',
    'exclusiveMinimum': 'gt',
    'exclusiveMaximum': 'lt',
    'inclusiveMinimum': 'ge',
    'inclusiveMaximum': 'le',
    'minItems': 'min_length',
    'maxItems': 'max_length',
}

_NON_PRIMITIVE_SCHEMA_TYPES = {'object', 'array'}

PRIMITIVE_TYPE_MAPPING: dict[str, type[Any]] = {
    schema_type: python_type
    for schema_type, python_type, _ in _SCHEMA_TYPE_REGISTRY
    if schema_type not in _NON_PRIMITIVE_SCHEMA_TYPES
}

IDENTIFIER_NORMALIZATION: dict[str, str] = {schema_type: identifier for schema_type, _, identifier in _SCHEMA_TYPE_REGISTRY}

JSON_SCHEMA_DRAFT = 'https://json-schema.org/draft/2020-12/schema'
_TYPE_ADAPTER_CACHE: dict[Any, TypeAdapter[Any]] = {}

FieldDefinition: TypeAlias = Any | tuple[Any, Any]


def _get_cached_type_adapter(result_type: Any) -> TypeAdapter[Any]:
    """Return a cached TypeAdapter for hashable result types."""
    try:
        cached = _TYPE_ADAPTER_CACHE.get(result_type)
    except TypeError:
        return TypeAdapter(result_type)
    if cached is not None:
        return cached
    adapter = TypeAdapter(result_type)
    _TYPE_ADAPTER_CACHE[result_type] = adapter
    return adapter


def _as_string_key_dict(value: object) -> dict[str, Any] | None:
    """Return a dict view with only string keys, otherwise None."""
    if not isinstance(value, Mapping):
        return None
    value_mapping = cast(Mapping[object, Any], value)
    normalized: dict[str, Any] = {}
    for raw_key, raw_value in value_mapping.items():
        if isinstance(raw_key, str):
            normalized[raw_key] = raw_value
    return normalized


def _as_non_string_sequence(value: object) -> Sequence[Any] | None:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return cast(Sequence[Any], value)
    return None


def _iter_string_key_dicts(value: object) -> Iterator[dict[str, Any]]:
    sequence_values = _as_non_string_sequence(value)
    if sequence_values is None:
        return
    for candidate_raw in sequence_values:
        candidate = _as_string_key_dict(candidate_raw)
        if candidate is not None:
            yield candidate


def _extract_non_null_json_schema_type(schema: Mapping[str, Any]) -> str | None:
    raw_type = schema.get('type')
    if isinstance(raw_type, str):
        return raw_type

    raw_type_values = _as_non_string_sequence(raw_type)
    if raw_type_values is not None:
        non_null_types = [item for item in raw_type_values if isinstance(item, str) and item != 'null']
        if len(non_null_types) == 1:
            return non_null_types[0]

    return None


def _json_schema_allows_null(schema: Mapping[str, Any]) -> bool:
    raw_type = schema.get('type')
    if raw_type == 'null':
        return True
    raw_type_values = _as_non_string_sequence(raw_type)
    if raw_type_values is not None:
        if any(item == 'null' for item in raw_type_values):
            return True

    for candidate in _iter_string_key_dicts(schema.get('anyOf')):
        if candidate.get('type') == 'null':
            return True
    return False


def _nullable_type(resolved_type: Any, *, nullable: bool) -> Any:
    if not nullable or resolved_type is type(None):
        return resolved_type
    return resolved_type | None


def normalize_result_dict(value: Any) -> dict[str, Any]:
    """Return a dict with only string keys from an arbitrary mapping-like value."""
    return _as_string_key_dict(value) or {}


def _json_schema_primitive_type(schema: dict[str, Any]) -> type[Any] | None:
    """Map simple JSON Schema primitive types to Python runtime types."""
    schema_type = _extract_non_null_json_schema_type(schema)
    return PRIMITIVE_TYPE_MAPPING.get(schema_type) if schema_type is not None else None


def _json_schema_identifier(schema: dict[str, Any]) -> str | None:
    schema_type = _extract_non_null_json_schema_type(schema)
    return IDENTIFIER_NORMALIZATION.get(schema_type) if schema_type is not None else None


def get_field_params_from_field_schema(field_schema: dict[str, Any]) -> dict[str, Any]:
    """Gets Pydantic field parameters from a JSON schema field."""
    field_params: dict[str, Any] = {}
    for constraint, constraint_value in CONSTRAINT_MAPPING.items():
        if constraint in field_schema:
            field_params[constraint_value] = field_schema[constraint]
    if 'description' in field_schema:
        field_params['description'] = field_schema['description']
    if 'default' in field_schema:
        field_params['default'] = field_schema['default']
    return field_params


def _json_schema_ref_name(schema: Mapping[str, Any]) -> str | None:
    raw_ref = schema.get('$ref')
    if raw_ref is None:
        return None
    reference = str(raw_ref).strip()
    if not reference:
        return None
    return reference.split('/')[-1]


def _build_model_fields_from_schema(
    schema: Mapping[str, Any],
    *,
    resolve_field_type: Callable[[dict[str, Any]], Any],
) -> dict[str, FieldDefinition]:
    fields: dict[str, FieldDefinition] = {}
    properties = _as_string_key_dict(schema.get('properties'))
    if properties is None:
        return fields
    required_raw = schema.get('required')
    required_fields: set[str] = set()
    required_values = _as_non_string_sequence(required_raw)
    if required_values is not None:
        required_fields = {name for name in required_values if isinstance(name, str)}

    for field_name, field_schema_raw in properties.items():
        field_schema = _as_string_key_dict(field_schema_raw)
        if field_schema is None:
            continue
        field_type = resolve_field_type(field_schema)
        field_params = get_field_params_from_field_schema(field_schema=field_schema)
        field_name_str = str(field_name)
        is_required = field_name_str in required_fields
        has_default = 'default' in field_params
        if not is_required and not has_default:
            relaxed_type = _nullable_type(field_type, nullable=True)
            fields[field_name_str] = (relaxed_type, Field(default=None, **field_params))
        else:
            fields[field_name_str] = (field_type, Field(**field_params))

    return fields


def _create_dynamic_model(
    *,
    model_name: str,
    model_schema: Mapping[str, Any],
    fields: Mapping[str, FieldDefinition],
) -> type[BaseModel]:
    field_definitions: dict[str, Any] = dict(fields)
    return create_model(
        model_name,
        __doc__=str(model_schema.get('description', '')),
        **field_definitions,
    )


def pydantic_model_from_json_schema(result_type: Any) -> Any:
    """Reconstruct runtime types from JSON Schema when possible."""
    if not isinstance(result_type, dict):
        return result_type
    normalized_schema = normalize_result_dict(result_type)
    definitions = _as_string_key_dict(normalized_schema.get('$defs')) or {}
    models: dict[str, type[BaseModel]] = {}
    model_build_stack: set[str] = set()

    def _combine_union_types(resolved_types: list[Any], *, nullable: bool) -> Any:
        if not resolved_types:
            return _nullable_type(Any, nullable=nullable)
        combined = resolved_types[0]
        for candidate_type in resolved_types[1:]:
            combined = combined | candidate_type
        return _nullable_type(combined, nullable=nullable)

    def _resolve_ref_model(model_reference: str) -> Any:
        if model_reference in models:
            return models[model_reference]
        if model_reference in model_build_stack:
            return Any
        model_schema_raw = definitions.get(model_reference)
        model_schema = _as_string_key_dict(model_schema_raw)
        if model_schema is None:
            return Any

        model_build_stack.add(model_reference)
        try:
            dynamic_model = _create_dynamic_model(
                model_name=model_reference,
                model_schema=model_schema,
                fields=_build_model_fields_from_schema(
                    model_schema,
                    resolve_field_type=_resolve_schema,
                ),
            )
            models[model_reference] = dynamic_model
            return dynamic_model
        finally:
            model_build_stack.remove(model_reference)

    def _resolve_array_schema(schema: dict[str, Any], *, nullable: bool) -> Any:
        prefix_items_raw = schema.get('prefixItems')
        prefix_items = _as_non_string_sequence(prefix_items_raw)
        if prefix_items is not None:
            tuple_items = [_resolve_schema(item) for item in prefix_items]
            if tuple_items:
                resolved_tuple = tuple.__class_getitem__(tuple(tuple_items))
                return _nullable_type(resolved_tuple, nullable=nullable)

        items_schema = _as_string_key_dict(schema.get('items'))
        if items_schema is None:
            return _nullable_type(list[Any], nullable=nullable)
        item_type = _resolve_schema(items_schema)
        if schema.get('uniqueItems') is True:
            return _nullable_type(set[item_type], nullable=nullable)
        return _nullable_type(list[item_type], nullable=nullable)

    def _resolve_object_schema(schema: dict[str, Any], *, nullable: bool) -> Any:
        properties = _as_string_key_dict(schema.get('properties'))
        if properties:
            dynamic_model = _create_dynamic_model(
                model_name=str(schema.get('title', 'InlineObject')),
                model_schema=schema,
                fields=_build_model_fields_from_schema(
                    schema,
                    resolve_field_type=_resolve_schema,
                ),
            )
            return _nullable_type(dynamic_model, nullable=nullable)

        additional_properties = _as_string_key_dict(schema.get('additionalProperties'))
        if additional_properties is not None:
            value_type = _resolve_schema(additional_properties)
            return _nullable_type(dict[str, value_type], nullable=nullable)
        return _nullable_type(dict[str, Any], nullable=nullable)

    def _resolve_schema(schema_raw: Any) -> Any:
        schema = normalize_result_dict(schema_raw)
        if not schema:
            return Any

        allows_null = _json_schema_allows_null(schema)
        model_reference = _json_schema_ref_name(schema)
        if model_reference is not None:
            return _nullable_type(_resolve_ref_model(model_reference), nullable=allows_null)

        primitive_type = _json_schema_primitive_type(schema)
        if primitive_type is not None:
            return _nullable_type(primitive_type, nullable=allows_null)

        any_of_candidates = _as_non_string_sequence(schema.get('anyOf'))
        if any_of_candidates is not None:
            resolved_types: list[Any] = []
            includes_null = allows_null
            for candidate in _iter_string_key_dicts(any_of_candidates):
                if candidate.get('type') == 'null':
                    includes_null = True
                    continue
                resolved_types.append(_resolve_schema(candidate))
            return _combine_union_types(resolved_types, nullable=includes_null)

        schema_type = _extract_non_null_json_schema_type(schema)
        if schema_type == 'null':
            return type(None)
        if schema_type == 'array':
            return _resolve_array_schema(schema, nullable=allows_null)
        if schema_type == 'object':
            return _resolve_object_schema(schema, nullable=allows_null)
        if isinstance(schema_type, str) and schema_type in TYPE_MAPPING:
            return _nullable_type(TYPE_MAPPING[schema_type], nullable=allows_null)
        return _nullable_type(Any, nullable=allows_null)

    for model_name in definitions:
        _resolve_ref_model(model_name)
    return _resolve_schema(normalized_schema)


def pydantic_model_to_json_schema(result_type: Any) -> dict[str, Any] | None:
    """Best-effort conversion of a Python result schema/type into JSON Schema."""
    if result_type is None:
        return None
    if isinstance(result_type, dict):
        schema = dict(cast(dict[str, Any], result_type))
        schema.setdefault('$schema', JSON_SCHEMA_DRAFT)
        return schema
    if isinstance(result_type, str):
        return None

    try:
        if inspect.isclass(result_type) and issubclass(result_type, BaseModel):
            schema = result_type.model_json_schema()
            schema.setdefault('$schema', JSON_SCHEMA_DRAFT)
            return schema
    except TypeError:
        pass

    try:
        schema = TypeAdapter(result_type).json_schema()
        normalized_schema = normalize_result_dict(schema)
        normalized_schema.setdefault('$schema', JSON_SCHEMA_DRAFT)
        return normalized_schema
    except Exception:
        return None


def result_type_identifier_from_schema(result_type: Any) -> str | None:
    if result_type is None:
        return None
    if isinstance(result_type, str):
        return result_type
    if isinstance(result_type, dict):
        return _json_schema_identifier(normalize_result_dict(result_type))

    if result_type is str:
        return 'string'
    if result_type in (int, float):
        return 'number'
    if result_type is bool:
        return 'boolean'

    derived_schema = pydantic_model_to_json_schema(result_type)
    if isinstance(derived_schema, dict):
        return _json_schema_identifier(derived_schema)
    return None


def validate_result_against_type(result_type: Any, result: Any) -> Any:
    if result_type is None:
        return result

    if isinstance(result_type, dict):
        result_type = pydantic_model_from_json_schema(result_type)

    if inspect.isclass(result_type) and issubclass(result_type, BaseModel):
        return result_type.model_validate(result)

    adapter = _get_cached_type_adapter(result_type)
    return adapter.validate_python(result)


__all__ = [
    'get_field_params_from_field_schema',
    'normalize_result_dict',
    'pydantic_model_from_json_schema',
    'pydantic_model_to_json_schema',
    'result_type_identifier_from_schema',
    'validate_result_against_type',
]
