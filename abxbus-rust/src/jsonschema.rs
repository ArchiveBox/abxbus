use serde_json::{json, Map, Value};

const JSON_SCHEMA_DRAFT: &str = "https://json-schema.org/draft/2020-12/schema";

fn normalize_json_schema_value(value: Value) -> Value {
    match value {
        Value::Array(items) => {
            Value::Array(items.into_iter().map(normalize_json_schema_value).collect())
        }
        Value::Object(mut object) => {
            for item in object.values_mut() {
                *item = normalize_json_schema_value(item.take());
            }
            sort_required_fields(&mut object);
            let Some(candidates) = null_union_candidates(&object) else {
                return Value::Object(object);
            };
            let mut merged = Map::new();
            merged.insert("anyOf".to_string(), normalize_json_schema_value(candidates));
            for (key, item) in object {
                if key != "type" {
                    merged.insert(key, item);
                }
            }
            Value::Object(merged)
        }
        other => other,
    }
}

fn sort_required_fields(object: &mut Map<String, Value>) {
    let Some(required) = object.get_mut("required").and_then(Value::as_array_mut) else {
        return;
    };
    if required.iter().all(Value::is_string) {
        required.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
    }
}

pub fn normalize_json_schema(value: Value) -> Value {
    let normalized = normalize_json_schema_value(value);
    let Value::Object(mut object) = normalized else {
        return normalized;
    };
    let Some(root_ref) = json_schema_root_ref(&object) else {
        object
            .entry("$schema".to_string())
            .or_insert_with(|| Value::String(JSON_SCHEMA_DRAFT.to_string()));
        return Value::Object(object);
    };
    let root_name = root_ref.trim_start_matches("#/$defs/").to_string();
    let Some(root_schema) = object
        .get("$defs")
        .and_then(|defs| defs.get(&root_name))
        .cloned()
    else {
        object
            .entry("$schema".to_string())
            .or_insert_with(|| Value::String(JSON_SCHEMA_DRAFT.to_string()));
        return Value::Object(object);
    };
    let mut refs = Map::new();
    refs.insert(root_ref.clone(), Value::String("#".to_string()));
    let Value::Object(mut rewritten_root) = rewrite_json_schema_refs(root_schema, &refs) else {
        object
            .entry("$schema".to_string())
            .or_insert_with(|| Value::String(JSON_SCHEMA_DRAFT.to_string()));
        return Value::Object(object);
    };
    let remaining_defs = object
        .get("$defs")
        .and_then(Value::as_object)
        .map(|defs| {
            defs.iter()
                .filter(|(name, _)| name.as_str() != root_name)
                .map(|(name, def)| (name.clone(), rewrite_json_schema_refs(def.clone(), &refs)))
                .collect::<Map<String, Value>>()
        })
        .unwrap_or_default();
    if !remaining_defs.is_empty() {
        rewritten_root.insert("$defs".to_string(), Value::Object(remaining_defs));
    }
    if let Some(schema_version) = object.get("$schema") {
        rewritten_root
            .entry("$schema".to_string())
            .or_insert_with(|| schema_version.clone());
    } else {
        rewritten_root
            .entry("$schema".to_string())
            .or_insert_with(|| Value::String(JSON_SCHEMA_DRAFT.to_string()));
    }
    set_title_from_inlined_root_definition(&mut rewritten_root, &root_name);
    Value::Object(rewritten_root)
}

fn json_schema_root_ref(object: &Map<String, Value>) -> Option<String> {
    if let Some(root_ref) = object.get("$ref").and_then(Value::as_str) {
        if root_ref.starts_with("#/$defs/") {
            return Some(root_ref.to_string());
        }
    }
    let definitions = object.get("$defs").and_then(Value::as_object)?;
    let root_name = schema_root_def_name(object, definitions)?;
    Some(format!("#/$defs/{root_name}"))
}

fn schema_root_def_name(
    object: &Map<String, Value>,
    definitions: &Map<String, Value>,
) -> Option<String> {
    let root = object
        .iter()
        .filter(|(key, _)| key.as_str() != "$schema" && key.as_str() != "$defs")
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Map<String, Value>>();
    definitions.iter().find_map(|(name, definition)| {
        if definition.as_object() == Some(&root) {
            Some(name.clone())
        } else {
            None
        }
    })
}

fn set_title_from_inlined_root_definition(schema: &mut Map<String, Value>, root_name: &str) {
    if root_name.starts_with("__schema") {
        return;
    }
    schema
        .entry("title".to_string())
        .or_insert_with(|| Value::String(root_name.to_string()));
}

fn rewrite_json_schema_refs(value: Value, refs: &Map<String, Value>) -> Value {
    match value {
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|item| rewrite_json_schema_refs(item, refs))
                .collect(),
        ),
        Value::Object(object) => {
            let mut rewritten = object
                .into_iter()
                .map(|(key, item)| (key, rewrite_json_schema_refs(item, refs)))
                .collect::<Map<String, Value>>();
            if let Some(reference) = rewritten.get("$ref").and_then(Value::as_str) {
                if let Some(replacement) = refs.get(reference) {
                    rewritten.insert("$ref".to_string(), replacement.clone());
                }
            }
            Value::Object(rewritten)
        }
        other => other,
    }
}

pub fn validate_json_schema_value(
    root_schema: &Value,
    schema: &Value,
    value: &Value,
    path: &str,
) -> Result<(), String> {
    if let Some(const_value) = schema.get("const") {
        if const_value != value {
            return Err(format!("{path} expected const value"));
        }
    }
    if let Some(enum_values) = schema.get("enum").and_then(Value::as_array) {
        if !enum_values.iter().any(|enum_value| enum_value == value) {
            return Err(format!("{path} expected enum value"));
        }
    }
    if let Some(not_schema) = schema.get("not") {
        if validate_json_schema_value(root_schema, not_schema, value, path).is_ok() {
            return Err(format!("{path} matched not schema"));
        }
    }

    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        let resolved = resolve_json_schema_ref(root_schema, reference)
            .ok_or_else(|| format!("{path} unresolved schema reference {reference}"))?;
        validate_json_schema_value(root_schema, resolved, value, path)?;
        if schema.as_object().is_some_and(|object| object.len() == 1) {
            return Ok(());
        }
    }

    if let Some(any_of) = schema.get("anyOf").and_then(Value::as_array) {
        if !any_of
            .iter()
            .any(|branch| validate_json_schema_value(root_schema, branch, value, path).is_ok())
        {
            return Err(format!("{path} did not match anyOf schema"));
        }
    }
    if let Some(one_of) = schema.get("oneOf").and_then(Value::as_array) {
        let matches = one_of
            .iter()
            .filter(|branch| validate_json_schema_value(root_schema, branch, value, path).is_ok())
            .count();
        if matches != 1 {
            return Err(format!("{path} matched {matches} oneOf schemas"));
        }
    }
    if let Some(all_of) = schema.get("allOf").and_then(Value::as_array) {
        for branch in all_of {
            validate_json_schema_value(root_schema, branch, value, path)?;
        }
    }
    let schema_type = schema.get("type");
    if let Some(Value::Array(types)) = schema_type {
        if types
            .iter()
            .any(|schema_type| json_schema_type_matches(schema_type, value))
        {
            return validate_json_schema_children(root_schema, schema, value, path);
        }
        return Err(format!("{path} did not match any allowed type"));
    }

    if let Some(schema_type) = schema_type {
        if !json_schema_type_matches(schema_type, value) {
            return Err(format!(
                "{path} expected {}",
                schema_type.as_str().unwrap_or("matching schema type")
            ));
        }
    } else if (schema.get("properties").is_some()
        || schema.get("required").is_some()
        || schema.get("additionalProperties").is_some())
        && !value.is_object()
    {
        return Err(format!("{path} expected object"));
    }

    validate_json_schema_scalar_constraints(schema, value, path)?;
    validate_json_schema_children(root_schema, schema, value, path)
}

fn validate_json_schema_scalar_constraints(
    schema: &Value,
    value: &Value,
    path: &str,
) -> Result<(), String> {
    if let Some(text) = value.as_str() {
        if let Some(min_length) = schema.get("minLength").and_then(Value::as_u64) {
            if text.chars().count() < min_length as usize {
                return Err(format!("{path} length below minLength"));
            }
        }
        if let Some(max_length) = schema.get("maxLength").and_then(Value::as_u64) {
            if text.chars().count() > max_length as usize {
                return Err(format!("{path} length above maxLength"));
            }
        }
        if let Some(pattern) = schema.get("pattern").and_then(Value::as_str) {
            let regex = regex::Regex::new(pattern)
                .map_err(|_| format!("{path} invalid pattern {pattern}"))?;
            if !regex.is_match(text) {
                return Err(format!("{path} does not match pattern"));
            }
        }
    }

    if let Some(number) = value.as_f64() {
        if let Some(multiple_of) = schema.get("multipleOf").and_then(Value::as_f64) {
            if multiple_of != 0.0 && !is_multiple_of(number, multiple_of) {
                return Err(format!("{path} is not multipleOf {multiple_of}"));
            }
        }
        if let Some(minimum) = schema.get("minimum").and_then(Value::as_f64) {
            if number < minimum {
                return Err(format!("{path} below minimum"));
            }
        }
        if let Some(maximum) = schema.get("maximum").and_then(Value::as_f64) {
            if number > maximum {
                return Err(format!("{path} above maximum"));
            }
        }
        if let Some(exclusive_minimum) = schema.get("exclusiveMinimum").and_then(Value::as_f64) {
            if number <= exclusive_minimum {
                return Err(format!("{path} below exclusiveMinimum"));
            }
        }
        if let Some(exclusive_maximum) = schema.get("exclusiveMaximum").and_then(Value::as_f64) {
            if number >= exclusive_maximum {
                return Err(format!("{path} above exclusiveMaximum"));
            }
        }
    }

    if let Some(items) = value.as_array() {
        if let Some(min_items) = schema.get("minItems").and_then(Value::as_u64) {
            if items.len() < min_items as usize {
                return Err(format!("{path} item count below minItems"));
            }
        }
        if let Some(max_items) = schema.get("maxItems").and_then(Value::as_u64) {
            if items.len() > max_items as usize {
                return Err(format!("{path} item count above maxItems"));
            }
        }
    }

    if let Some(object) = value.as_object() {
        if let Some(min_properties) = schema.get("minProperties").and_then(Value::as_u64) {
            if object.len() < min_properties as usize {
                return Err(format!("{path} property count below minProperties"));
            }
        }
        if let Some(max_properties) = schema.get("maxProperties").and_then(Value::as_u64) {
            if object.len() > max_properties as usize {
                return Err(format!("{path} property count above maxProperties"));
            }
        }
    }
    Ok(())
}

fn is_multiple_of(number: f64, multiple_of: f64) -> bool {
    let quotient = number / multiple_of;
    (quotient - quotient.round()).abs() < 1e-9
}

fn null_union_candidates(object: &Map<String, Value>) -> Option<Value> {
    if let Some(raw_types) = object.get("type").and_then(Value::as_array) {
        let mut non_null_types = Vec::new();
        let mut has_null = false;
        for raw_type in raw_types {
            if raw_type.as_str() == Some("null") {
                has_null = true;
            } else if raw_type.is_string() {
                non_null_types.push(raw_type.clone());
            }
        }
        if has_null && !non_null_types.is_empty() {
            let schema_type = if non_null_types.len() == 1 {
                non_null_types.remove(0)
            } else {
                Value::Array(non_null_types)
            };
            return Some(Value::Array(vec![
                json!({"type": schema_type}),
                json!({"type": "null"}),
            ]));
        }
    }

    None
}

fn resolve_json_schema_ref<'a>(root_schema: &'a Value, reference: &str) -> Option<&'a Value> {
    let pointer = reference.strip_prefix('#')?;
    if pointer.is_empty() {
        Some(root_schema)
    } else {
        root_schema.pointer(pointer)
    }
}

fn json_schema_type_matches(schema_type: &Value, value: &Value) -> bool {
    match schema_type.as_str() {
        Some("string") => value.is_string(),
        Some("number") => value.is_number(),
        Some("integer") => value
            .as_f64()
            .is_some_and(|number| number.is_finite() && number.fract() == 0.0),
        Some("boolean") => value.is_boolean(),
        Some("null") => value.is_null(),
        Some("array") => value.is_array(),
        Some("object") => value.is_object(),
        _ => true,
    }
}

fn validate_json_schema_children(
    root_schema: &Value,
    schema: &Value,
    value: &Value,
    path: &str,
) -> Result<(), String> {
    if let Some(items_schema) = schema.get("items") {
        if let Some(items) = value.as_array() {
            for (index, item) in items.iter().enumerate() {
                validate_json_schema_value(
                    root_schema,
                    items_schema,
                    item,
                    &format!("{path}[{index}]"),
                )?;
            }
        }
    }
    if let Some(prefix_items) = schema.get("prefixItems").and_then(Value::as_array) {
        if let Some(items) = value.as_array() {
            for (index, item_schema) in prefix_items.iter().enumerate() {
                let Some(item) = items.get(index) else {
                    break;
                };
                validate_json_schema_value(
                    root_schema,
                    item_schema,
                    item,
                    &format!("{path}[{index}]"),
                )?;
            }
        }
    }

    let Some(object) = value.as_object() else {
        return Ok(());
    };

    if let Some(required) = schema.get("required").and_then(Value::as_array) {
        for key in required.iter().filter_map(Value::as_str) {
            if !object.contains_key(key) {
                return Err(format!("{path}.{key} is required"));
            }
        }
    }

    let properties = schema.get("properties").and_then(Value::as_object);
    if let Some(properties) = properties {
        for (key, property_schema) in properties {
            if let Some(property_value) = object.get(key) {
                validate_json_schema_value(
                    root_schema,
                    property_schema,
                    property_value,
                    &format!("{path}.{key}"),
                )?;
            }
        }
    }

    match schema.get("additionalProperties") {
        Some(Value::Bool(false)) => {
            if let Some(properties) = properties {
                for key in object.keys() {
                    if !properties.contains_key(key) {
                        return Err(format!("{path}.{key} is not allowed"));
                    }
                }
            }
        }
        Some(additional_schema @ Value::Object(_)) => {
            for (key, item) in object {
                if properties.is_some_and(|props| props.contains_key(key)) {
                    continue;
                }
                validate_json_schema_value(
                    root_schema,
                    additional_schema,
                    item,
                    &format!("{path}.{key}"),
                )?;
            }
        }
        _ => {}
    }

    Ok(())
}
