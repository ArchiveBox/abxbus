package jsonschema

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"
)

const Draft202012 = "https://json-schema.org/draft/2020-12/schema"

// Normalize converts Go values into the same JSON-compatible shape used by
// encoding/json. This keeps validation behavior stable for structs, maps, and
// integer values before applying JSON Schema rules.
func Normalize(value any) any {
	data, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var normalized any
	if err := json.Unmarshal(data, &normalized); err != nil {
		return value
	}
	return normalized
}

// Validate checks value against schema. It intentionally implements the small
// JSON Schema subset abxbus relies on at runtime instead of pulling in a full
// validator dependency.
func Validate(schema map[string]any, value any) error {
	if schema == nil {
		return nil
	}
	return validateValue(schema, schema, Normalize(value), "$")
}

func validateValue(root map[string]any, schema any, value any, path string) error {
	schemaMap, ok := schema.(map[string]any)
	if !ok {
		return nil
	}
	if constValue, ok := schemaMap["const"]; ok && !reflect.DeepEqual(Normalize(constValue), Normalize(value)) {
		return fmt.Errorf("%s expected const value", path)
	}
	if enumValues, ok := schemaMap["enum"].([]any); ok {
		matched := false
		normalizedValue := Normalize(value)
		for _, enumValue := range enumValues {
			if reflect.DeepEqual(Normalize(enumValue), normalizedValue) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%s expected enum value", path)
		}
	}
	if notSchema, ok := schemaMap["not"]; ok {
		if validateValue(root, notSchema, value, path) == nil {
			return fmt.Errorf("%s matched not schema", path)
		}
	}
	if ref, ok := schemaMap["$ref"].(string); ok {
		resolved, ok := resolveRef(root, ref)
		if !ok {
			return fmt.Errorf("%s unresolved schema reference %s", path, ref)
		}
		if err := validateValue(root, resolved, value, path); err != nil {
			return err
		}
		if len(schemaMap) == 1 {
			return nil
		}
	}
	if anyOf, ok := schemaMap["anyOf"].([]any); ok {
		matched := false
		for _, branch := range anyOf {
			if validateValue(root, branch, value, path) == nil {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%s did not match anyOf schema", path)
		}
	}
	if oneOf, ok := schemaMap["oneOf"].([]any); ok {
		matches := 0
		for _, branch := range oneOf {
			if validateValue(root, branch, value, path) == nil {
				matches++
			}
		}
		if matches != 1 {
			return fmt.Errorf("%s matched %d oneOf schemas", path, matches)
		}
	}
	if allOf, ok := schemaMap["allOf"].([]any); ok {
		for _, branch := range allOf {
			if err := validateValue(root, branch, value, path); err != nil {
				return err
			}
		}
	}
	if schemaType, ok := schemaMap["type"]; ok {
		if types, ok := schemaType.([]any); ok {
			matched := false
			for _, allowed := range types {
				if typeMatches(allowed, value) {
					matched = true
					break
				}
			}
			if !matched {
				return fmt.Errorf("%s did not match any allowed type", path)
			}
		} else if !typeMatches(schemaType, value) {
			if label, ok := schemaType.(string); ok {
				return fmt.Errorf("%s expected %s", path, label)
			}
			return fmt.Errorf("%s expected matching schema type", path)
		}
	} else if schemaMap["properties"] != nil || schemaMap["required"] != nil || schemaMap["additionalProperties"] != nil {
		if _, ok := value.(map[string]any); !ok {
			return fmt.Errorf("%s expected object", path)
		}
	}
	if err := validateScalarConstraints(schemaMap, value, path); err != nil {
		return err
	}
	return validateChildren(root, schemaMap, value, path)
}

func resolveRef(root map[string]any, ref string) (any, bool) {
	if ref == "#" {
		return root, true
	}
	if !strings.HasPrefix(ref, "#/") {
		return nil, false
	}
	var current any = root
	for _, part := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
		part = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func typeMatches(schemaType any, value any) bool {
	label, ok := schemaType.(string)
	if !ok {
		return true
	}
	switch label {
	case "string":
		_, ok := value.(string)
		return ok
	case "number":
		number, ok := value.(float64)
		return ok && !math.IsNaN(number) && !math.IsInf(number, 0)
	case "integer":
		number, ok := value.(float64)
		return ok && !math.IsNaN(number) && !math.IsInf(number, 0) && math.Trunc(number) == number
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "null":
		return value == nil
	case "array":
		_, ok := value.([]any)
		return ok
	case "object":
		_, ok := value.(map[string]any)
		return ok
	default:
		return true
	}
}

func validateChildren(root map[string]any, schema map[string]any, value any, path string) error {
	if itemsSchema, ok := schema["items"]; ok {
		if items, ok := value.([]any); ok {
			for idx, item := range items {
				if err := validateValue(root, itemsSchema, item, fmt.Sprintf("%s[%d]", path, idx)); err != nil {
					return err
				}
			}
		}
	}
	if prefixItems, ok := schema["prefixItems"].([]any); ok {
		if items, ok := value.([]any); ok {
			for idx, itemSchema := range prefixItems {
				if idx >= len(items) {
					break
				}
				if err := validateValue(root, itemSchema, items[idx], fmt.Sprintf("%s[%d]", path, idx)); err != nil {
					return err
				}
			}
		}
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	if required, ok := schema["required"].([]any); ok {
		for _, keyValue := range required {
			key, ok := keyValue.(string)
			if !ok {
				continue
			}
			if _, exists := object[key]; !exists {
				return fmt.Errorf("%s.%s is required", path, key)
			}
		}
	}
	properties, _ := schema["properties"].(map[string]any)
	for key, propertySchema := range properties {
		if propertyValue, exists := object[key]; exists {
			if err := validateValue(root, propertySchema, propertyValue, path+"."+key); err != nil {
				return err
			}
		}
	}
	switch additional := schema["additionalProperties"].(type) {
	case bool:
		if !additional && properties != nil {
			for key := range object {
				if _, known := properties[key]; !known {
					return fmt.Errorf("%s.%s is not allowed", path, key)
				}
			}
		}
	case map[string]any:
		for key, item := range object {
			if properties != nil {
				if _, known := properties[key]; known {
					continue
				}
			}
			if err := validateValue(root, additional, item, path+"."+key); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateScalarConstraints(schema map[string]any, value any, path string) error {
	if text, ok := value.(string); ok {
		if minLength, ok := schemaNumber(schema["minLength"]); ok && float64(len([]rune(text))) < minLength {
			return fmt.Errorf("%s length below minLength", path)
		}
		if maxLength, ok := schemaNumber(schema["maxLength"]); ok && float64(len([]rune(text))) > maxLength {
			return fmt.Errorf("%s length above maxLength", path)
		}
		if pattern, ok := schema["pattern"].(string); ok {
			matched, err := regexp.MatchString(pattern, text)
			if err != nil {
				return fmt.Errorf("%s invalid pattern %s", path, pattern)
			}
			if !matched {
				return fmt.Errorf("%s does not match pattern", path)
			}
		}
	}
	if number, ok := value.(float64); ok {
		if multipleOf, ok := schemaNumber(schema["multipleOf"]); ok && multipleOf != 0 && !isMultipleOf(number, multipleOf) {
			return fmt.Errorf("%s is not multipleOf %v", path, multipleOf)
		}
		if minimum, ok := schemaNumber(schema["minimum"]); ok && number < minimum {
			return fmt.Errorf("%s below minimum", path)
		}
		if maximum, ok := schemaNumber(schema["maximum"]); ok && number > maximum {
			return fmt.Errorf("%s above maximum", path)
		}
		if exclusiveMinimum, ok := schemaNumber(schema["exclusiveMinimum"]); ok && number <= exclusiveMinimum {
			return fmt.Errorf("%s below exclusiveMinimum", path)
		}
		if exclusiveMaximum, ok := schemaNumber(schema["exclusiveMaximum"]); ok && number >= exclusiveMaximum {
			return fmt.Errorf("%s above exclusiveMaximum", path)
		}
	}
	if items, ok := value.([]any); ok {
		if minItems, ok := schemaNumber(schema["minItems"]); ok && float64(len(items)) < minItems {
			return fmt.Errorf("%s item count below minItems", path)
		}
		if maxItems, ok := schemaNumber(schema["maxItems"]); ok && float64(len(items)) > maxItems {
			return fmt.Errorf("%s item count above maxItems", path)
		}
	}
	if object, ok := value.(map[string]any); ok {
		if minProperties, ok := schemaNumber(schema["minProperties"]); ok && float64(len(object)) < minProperties {
			return fmt.Errorf("%s property count below minProperties", path)
		}
		if maxProperties, ok := schemaNumber(schema["maxProperties"]); ok && float64(len(object)) > maxProperties {
			return fmt.Errorf("%s property count above maxProperties", path)
		}
	}
	return nil
}

func isMultipleOf(number float64, multipleOf float64) bool {
	quotient := number / multipleOf
	return math.Abs(quotient-math.Round(quotient)) < 1e-9
}

func schemaNumber(value any) (float64, bool) {
	switch number := value.(type) {
	case float64:
		return number, true
	case float32:
		return float64(number), true
	case int:
		return float64(number), true
	case int8:
		return float64(number), true
	case int16:
		return float64(number), true
	case int32:
		return float64(number), true
	case int64:
		return float64(number), true
	case uint:
		return float64(number), true
	case uint8:
		return float64(number), true
	case uint16:
		return float64(number), true
	case uint32:
		return float64(number), true
	case uint64:
		return float64(number), true
	default:
		return 0, false
	}
}
