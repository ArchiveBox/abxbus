package abxbus_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestGoRoundtripCLIPreservesEventJSONShape(t *testing.T) {
	tempDir := t.TempDir()
	inputPath := filepath.Join(tempDir, "events.json")
	outputPath := filepath.Join(tempDir, "events.out.json")
	input := []byte(`[
  {
    "event_type": "RoundtripEvent",
    "event_version": "0.0.1",
    "event_timeout": null,
    "event_slow_timeout": null,
    "event_concurrency": null,
    "event_handler_timeout": null,
    "event_handler_slow_timeout": null,
    "event_handler_concurrency": null,
    "event_handler_completion": null,
    "event_blocks_parent_completion": false,
    "event_result_type": {"type": "array", "items": {"type": "string"}},
    "event_id": "018f8e40-1234-7000-8000-00000000abcd",
    "event_path": [],
    "event_parent_id": null,
    "event_emitted_by_handler_id": null,
    "event_pending_bus_count": 0,
    "event_created_at": "2026-01-01T00:00:00.000000000Z",
    "event_status": "pending",
    "event_started_at": null,
    "event_completed_at": null,
    "label": "go"
  }
]`)
	if err := os.WriteFile(inputPath, input, 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("go", "run", "../cmd/abxbus-go-roundtrip", "events", inputPath, outputPath)
	cmd.Dir = "."
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go roundtrip CLI failed: %v\n%s", err, string(output))
	}
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	var events []map[string]any
	if err := json.Unmarshal(data, &events); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0]["event_type"] != "RoundtripEvent" || events[0]["label"] != "go" {
		t.Fatalf("roundtrip event payload mismatch: %#v", events)
	}
	expectedSchema := map[string]any{
		"type":  "array",
		"items": map[string]any{"type": "string"},
	}
	if !reflect.DeepEqual(events[0]["event_result_type"], expectedSchema) {
		t.Fatalf("event_result_type schema did not roundtrip: %#v", events[0]["event_result_type"])
	}
}

func TestGoToOtherRuntimeToGoEventRoundtripsPreserveJSONShape(t *testing.T) {
	cases := roundtripEventCases()
	events := make([]any, 0, len(cases))
	for _, tc := range cases {
		events = append(events, tc.event)
	}
	for _, runtime := range []string{"python", "ts", "rust"} {
		t.Run(runtime, func(t *testing.T) {
			throughRuntime := runRuntimeRoundtrip(t, runtime, "events", events)
			assertEventRoundtripEqualAllowingSchemaNormalization(t, events, throughRuntime)
			assertGoResultSchemaSemantics(t, throughRuntime, cases)
			backThroughGo := runRuntimeRoundtrip(t, "go", "events", throughRuntime)
			assertJSONEqual(t, throughRuntime, backThroughGo)
			assertGoResultSchemaSemantics(t, backThroughGo, cases)
		})
	}
}

func TestOtherRuntimeToGoToSameRuntimeEventRoundtripsPreserveJSONShape(t *testing.T) {
	cases := roundtripEventCases()
	events := make([]any, 0, len(cases))
	for _, tc := range cases {
		events = append(events, tc.event)
	}
	for _, runtime := range []string{"python", "ts", "rust"} {
		t.Run(runtime, func(t *testing.T) {
			originRuntime := runRuntimeRoundtrip(t, runtime, "events", events)
			assertEventRoundtripEqualAllowingSchemaNormalization(t, events, originRuntime)
			assertGoResultSchemaSemantics(t, originRuntime, cases)

			throughGo := runRuntimeRoundtrip(t, "go", "events", originRuntime)
			assertJSONEqual(t, originRuntime, throughGo)
			assertGoResultSchemaSemantics(t, throughGo, cases)

			backThroughOrigin := runRuntimeRoundtrip(t, runtime, "events", throughGo)
			assertJSONEqual(t, originRuntime, backThroughOrigin)
			assertGoResultSchemaSemantics(t, backThroughOrigin, cases)
		})
	}
}

func TestGoToOtherRuntimeToGoBusRoundtripsPreserveJSONShape(t *testing.T) {
	bus := roundtripBusFixture()
	for _, runtime := range []string{"python", "ts", "rust"} {
		t.Run(runtime, func(t *testing.T) {
			throughRuntime := runRuntimeRoundtrip(t, runtime, "bus", bus)
			assertJSONEqual(t, bus, throughRuntime)
			backThroughGo := runRuntimeRoundtrip(t, "go", "bus", throughRuntime)
			assertJSONEqual(t, bus, backThroughGo)
		})
	}
}

func TestOtherRuntimeToGoToSameRuntimeBusRoundtripsPreserveJSONShape(t *testing.T) {
	bus := roundtripBusFixture()
	for _, runtime := range []string{"python", "ts", "rust"} {
		t.Run(runtime, func(t *testing.T) {
			originRuntime := runRuntimeRoundtrip(t, runtime, "bus", bus)
			assertJSONEqual(t, bus, originRuntime)

			throughGo := runRuntimeRoundtrip(t, "go", "bus", originRuntime)
			assertJSONEqual(t, originRuntime, throughGo)

			backThroughOrigin := runRuntimeRoundtrip(t, runtime, "bus", throughGo)
			assertJSONEqual(t, originRuntime, backThroughOrigin)
		})
	}
}

func runRuntimeRoundtrip(t *testing.T, runtime string, mode string, payload any) any {
	t.Helper()
	tempDir := t.TempDir()
	inputPath := filepath.Join(tempDir, runtime+"-"+mode+"-input.json")
	outputPath := filepath.Join(tempDir, runtime+"-"+mode+"-output.json")
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(inputPath, data, 0o644); err != nil {
		t.Fatal(err)
	}

	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	var cmd *exec.Cmd
	switch runtime {
	case "go":
		cmd = exec.Command("go", "run", "./cmd/abxbus-go-roundtrip", mode, inputPath, outputPath)
		cmd.Dir = filepath.Join(repoRoot, "abxbus-go")
	case "python":
		cmd = exec.Command("uv", "run", "python", "-c", pythonRoundtripScript, mode, inputPath, outputPath)
		cmd.Dir = repoRoot
	case "ts":
		cmd = exec.Command("pnpm", "--dir", filepath.Join(repoRoot, "abxbus-ts"), "exec", "node", "--import", "tsx", "-e", tsRoundtripScript, mode, inputPath, outputPath)
		cmd.Dir = repoRoot
	case "rust":
		rustRoot := filepath.Join(repoRoot, "abxbus-rust")
		cmd = exec.Command("cargo", "run", "--quiet", "--manifest-path", filepath.Join(rustRoot, "Cargo.toml"), "--target-dir", filepath.Join(tempDir, "rust-target"), "--bin", "abxbus-rust-roundtrip", "--", mode, inputPath, outputPath)
		cmd.Dir = repoRoot
	default:
		t.Fatalf("unknown runtime %q", runtime)
	}

	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%s %s roundtrip failed: %v\n%s", runtime, mode, err, string(output))
	}
	out, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	var result any
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func assertJSONEqual(t *testing.T, expected any, actual any) {
	t.Helper()
	var expectedJSON any
	expectedData, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(expectedData, &expectedJSON); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expectedJSON, actual) {
		expectedPretty, _ := json.MarshalIndent(expectedJSON, "", "  ")
		actualPretty, _ := json.MarshalIndent(actual, "", "  ")
		t.Fatalf("JSON shape changed\nexpected:\n%s\nactual:\n%s", expectedPretty, actualPretty)
	}
}

func assertEventRoundtripEqualAllowingSchemaNormalization(t *testing.T, expected any, actual any) {
	t.Helper()
	expectedEvents := normalizeEventList(t, expected)
	actualEvents := normalizeEventList(t, actual)
	if len(expectedEvents) != len(actualEvents) {
		t.Fatalf("event count changed: got %d want %d", len(actualEvents), len(expectedEvents))
	}
	for idx := range expectedEvents {
		expectedEvent := copyWithoutKey(expectedEvents[idx], "event_result_type")
		actualEvent := copyWithoutKey(actualEvents[idx], "event_result_type")
		if !reflect.DeepEqual(expectedEvent, actualEvent) {
			expectedPretty, _ := json.MarshalIndent(expectedEvent, "", "  ")
			actualPretty, _ := json.MarshalIndent(actualEvent, "", "  ")
			t.Fatalf("event fields changed at index %d\nexpected:\n%s\nactual:\n%s", idx, expectedPretty, actualPretty)
		}
		if _, ok := actualEvents[idx]["event_result_type"]; !ok {
			t.Fatalf("event_result_type missing after roundtrip at index %d", idx)
		}
	}
}

func normalizeEventList(t *testing.T, payload any) []map[string]any {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var events []map[string]any
	if err := json.Unmarshal(data, &events); err != nil {
		t.Fatal(err)
	}
	return events
}

func copyWithoutKey(in map[string]any, omittedKey string) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		if key != omittedKey {
			out[key] = value
		}
	}
	return out
}

type roundtripEventCase struct {
	event   map[string]any
	valid   []any
	invalid []any
}

func roundtripEventCases() []roundtripEventCase {
	return []roundtripEventCase{
		{
			event:   roundtripEventFixture("GoStringResultEvent", "go-string", map[string]any{"type": "string"}, 1),
			valid:   []any{"ok"},
			invalid: []any{123},
		},
		{
			event:   roundtripEventFixture("GoIntegerResultEvent", "go-integer", map[string]any{"type": "integer"}, 2),
			valid:   []any{42},
			invalid: []any{3.5, "42"},
		},
		{
			event:   roundtripEventFixture("GoBooleanResultEvent", "go-boolean", map[string]any{"type": "boolean"}, 3),
			valid:   []any{true},
			invalid: []any{"true"},
		},
		{
			event:   roundtripEventFixture("GoNullResultEvent", "go-null", map[string]any{"type": "null"}, 4),
			valid:   []any{nil},
			invalid: []any{false},
		},
		{
			event: roundtripEventFixture("GoArrayResultEvent", "go-array", map[string]any{
				"type":  "array",
				"items": map[string]any{"type": "string"},
			}, 5),
			valid:   []any{[]any{"a", "b"}},
			invalid: []any{[]any{"a", 2}},
		},
		{
			event: roundtripEventFixture("GoObjectResultEvent", "go-object", map[string]any{
				"type":                 "object",
				"required":             []any{"id", "count"},
				"additionalProperties": false,
				"properties": map[string]any{
					"id":    map[string]any{"type": "string"},
					"count": map[string]any{"type": "integer"},
				},
			}, 6),
			valid:   []any{map[string]any{"id": "item-1", "count": 2}},
			invalid: []any{map[string]any{"id": "item-1"}, map[string]any{"id": "item-1", "count": "2"}},
		},
		{
			event: roundtripEventFixture("GoAnyOfResultEvent", "go-anyof", map[string]any{
				"anyOf": []any{
					map[string]any{"type": "string"},
					map[string]any{"type": "integer"},
				},
			}, 7),
			valid:   []any{"ok", 7},
			invalid: []any{false},
		},
	}
}

func assertGoResultSchemaSemantics(t *testing.T, payload any, cases []roundtripEventCase) {
	t.Helper()
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var eventPayloads []map[string]any
	if err := json.Unmarshal(payloadJSON, &eventPayloads); err != nil {
		t.Fatal(err)
	}
	if len(eventPayloads) != len(cases) {
		t.Fatalf("schema semantics fixture count mismatch: got %d want %d", len(eventPayloads), len(cases))
	}

	casesByType := map[string]roundtripEventCase{}
	for _, tc := range cases {
		casesByType[fmt.Sprint(tc.event["event_type"])] = tc
	}
	for _, eventPayload := range eventPayloads {
		eventType := fmt.Sprint(eventPayload["event_type"])
		tc, ok := casesByType[eventType]
		if !ok {
			t.Fatalf("unexpected roundtrip event type %q", eventType)
		}
		for idx, valid := range tc.valid {
			assertGoHandlerResultAccepted(t, eventPayload, valid, fmt.Sprintf("%s valid[%d]", eventType, idx))
		}
		for idx, invalid := range tc.invalid {
			assertGoHandlerResultRejected(t, eventPayload, invalid, fmt.Sprintf("%s invalid[%d]", eventType, idx))
		}
	}
}

func assertGoHandlerResultAccepted(t *testing.T, eventPayload map[string]any, result any, contextLabel string) {
	t.Helper()
	event := hydrateEventPayload(t, eventPayload)
	bus := abxbus.NewEventBus("GoRoundtripSchemaAccepted", nil)
	bus.On(event.EventType, "valid", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return result, nil
	}, nil)
	if _, err := bus.Emit(event).EventResult(context.Background()); err != nil {
		t.Fatalf("%s should accept handler result %#v: %v", contextLabel, result, err)
	}
}

func assertGoHandlerResultRejected(t *testing.T, eventPayload map[string]any, result any, contextLabel string) {
	t.Helper()
	event := hydrateEventPayload(t, eventPayload)
	bus := abxbus.NewEventBus("GoRoundtripSchemaRejected", nil)
	bus.On(event.EventType, "invalid", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		return result, nil
	}, nil)
	if _, err := bus.Emit(event).EventResult(context.Background()); err == nil || !strings.Contains(err.Error(), "EventHandlerResultSchemaError") {
		t.Fatalf("%s should reject handler result %#v with schema error, got %v", contextLabel, result, err)
	}
}

func hydrateEventPayload(t *testing.T, eventPayload map[string]any) *abxbus.BaseEvent {
	t.Helper()
	data, err := json.Marshal(eventPayload)
	if err != nil {
		t.Fatal(err)
	}
	event, err := abxbus.BaseEventFromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func roundtripEventFixture(eventType string, label string, resultSchema map[string]any, idSuffix int) map[string]any {
	schema := map[string]any{"$schema": "https://json-schema.org/draft/2020-12/schema"}
	for key, value := range resultSchema {
		schema[key] = value
	}
	return map[string]any{
		"event_type":                     eventType,
		"event_version":                  "0.0.1",
		"event_timeout":                  nil,
		"event_slow_timeout":             nil,
		"event_concurrency":              nil,
		"event_handler_timeout":          nil,
		"event_handler_slow_timeout":     nil,
		"event_handler_concurrency":      nil,
		"event_handler_completion":       nil,
		"event_blocks_parent_completion": false,
		"event_result_type":              schema,
		"event_id":                       fmt.Sprintf("018f8e40-1234-7000-8000-%012d", idSuffix),
		"event_path":                     []any{},
		"event_parent_id":                nil,
		"event_emitted_by_handler_id":    nil,
		"event_pending_bus_count":        0,
		"event_created_at":               "2026-01-01T00:00:00.000000000Z",
		"event_status":                   "pending",
		"event_started_at":               nil,
		"event_completed_at":             nil,
		"label":                          label,
	}
}

func roundtripBusFixture() map[string]any {
	handlerID := "handler-one"
	eventID := "018f8e40-1234-7000-8000-00000000e001"
	busID := "018f8e40-1234-7000-8000-00000000cc33"
	event := roundtripEventFixture("GoCrossRuntimeResumeEvent", "go-bus", map[string]any{
		"type":  "array",
		"items": map[string]any{"type": "string"},
	}, 999)
	event["event_id"] = eventID
	event["event_results"] = map[string]any{
		handlerID: map[string]any{
			"id":                    "result-one",
			"status":                "pending",
			"event_id":              eventID,
			"handler_id":            handlerID,
			"handler_name":          "handler_one",
			"handler_file_path":     nil,
			"handler_timeout":       nil,
			"handler_slow_timeout":  nil,
			"handler_registered_at": "2025-01-02T03:04:05.000000000Z",
			"handler_event_pattern": "GoCrossRuntimeResumeEvent",
			"eventbus_name":         "GoCrossRuntimeBus",
			"eventbus_id":           busID,
			"started_at":            nil,
			"completed_at":          nil,
			"result":                nil,
			"error":                 nil,
			"event_children":        []any{},
		},
	}
	return map[string]any{
		"id":                              busID,
		"name":                            "GoCrossRuntimeBus",
		"max_history_size":                100,
		"max_history_drop":                false,
		"event_concurrency":               "bus-serial",
		"event_timeout":                   60.0,
		"event_slow_timeout":              nil,
		"event_handler_concurrency":       "serial",
		"event_handler_completion":        "all",
		"event_handler_slow_timeout":      nil,
		"event_handler_detect_file_paths": false,
		"handlers": map[string]any{
			handlerID: map[string]any{
				"id":                    handlerID,
				"event_pattern":         "GoCrossRuntimeResumeEvent",
				"handler_name":          "handler_one",
				"handler_file_path":     nil,
				"handler_timeout":       nil,
				"handler_slow_timeout":  nil,
				"handler_registered_at": "2025-01-02T03:04:05.000000000Z",
				"eventbus_name":         "GoCrossRuntimeBus",
				"eventbus_id":           busID,
			},
		},
		"handlers_by_key": map[string]any{
			"GoCrossRuntimeResumeEvent": []any{handlerID},
		},
		"event_history": map[string]any{
			eventID: event,
		},
		"pending_event_queue": []any{eventID},
	}
}

const pythonRoundtripScript = `
import json, sys
from abxbus import BaseEvent, EventBus
mode, input_path, output_path = sys.argv[1:4]
with open(input_path, encoding='utf-8') as f:
    payload = json.load(f)
if mode == 'events':
    result = [BaseEvent.model_validate(item).model_dump(mode='json') for item in payload]
elif mode == 'bus':
    result = EventBus.validate(payload).model_dump()
else:
    raise SystemExit(f'unknown mode: {mode}')
with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(result, f, indent=2)
`

const tsRoundtripScript = `
import { readFileSync, writeFileSync } from 'node:fs'
import { BaseEvent, EventBus } from './src/index.ts'
const [mode, inputPath, outputPath] = process.argv.slice(1)
const payload = JSON.parse(readFileSync(inputPath, 'utf8'))
let result
if (mode === 'events') {
  result = payload.map((item) => BaseEvent.fromJSON(item).toJSON())
} else if (mode === 'bus') {
  result = EventBus.fromJSON(payload).toJSON()
} else {
  throw new Error('unknown mode: ' + mode)
}
writeFileSync(outputPath, JSON.stringify(result, null, 2), 'utf8')
`
