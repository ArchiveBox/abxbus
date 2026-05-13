package abxbus_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	abxbus "github.com/ArchiveBox/abxbus/abxbus-go/v2"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var schemaAssertionEventID atomic.Uint64

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

	cmd := exec.Command("go", "run", "./roundtrip_cli", "events", inputPath, outputPath)
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
		cmd = exec.Command("go", "run", "./tests/roundtrip_cli", mode, inputPath, outputPath)
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
	event = resetEventForSchemaAssertion(t, event)
	isolateSchemaAssertionEvent(event)
	bus := abxbus.NewEventBus("GoRoundtripSchemaAccepted", nil)
	defer bus.Destroy()
	bus.OnEventName(event.EventType, "valid", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return result, nil
	}, nil)
	executed, err := bus.Emit(event).Now(&abxbus.EventWaitOptions{FirstResult: true})
	if err != nil {
		t.Fatalf("%s should accept handler result %#v: %v", contextLabel, result, err)
	}
	if _, err := executed.EventResult(); err != nil {
		t.Fatalf("%s should accept handler result %#v: %v", contextLabel, result, err)
	}
}

func assertGoHandlerResultRejected(t *testing.T, eventPayload map[string]any, result any, contextLabel string) {
	t.Helper()
	event := hydrateEventPayload(t, eventPayload)
	event = resetEventForSchemaAssertion(t, event)
	isolateSchemaAssertionEvent(event)
	bus := abxbus.NewEventBus("GoRoundtripSchemaRejected", nil)
	defer bus.Destroy()
	bus.OnEventName(event.EventType, "invalid", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return result, nil
	}, nil)
	executed, err := bus.Emit(event).Now(&abxbus.EventWaitOptions{FirstResult: true})
	if err == nil {
		_, err = executed.EventResult()
	}
	if err == nil || !strings.Contains(err.Error(), "EventHandlerResultSchemaError") {
		t.Fatalf("%s should reject handler result %#v with schema error, got %v", contextLabel, result, err)
	}
}

func resetEventForSchemaAssertion(t *testing.T, event *abxbus.BaseEvent) *abxbus.BaseEvent {
	t.Helper()
	reset, err := event.EventReset()
	if err != nil {
		t.Fatal(err)
	}
	return reset
}

func isolateSchemaAssertionEvent(event *abxbus.BaseEvent) {
	event.EventType = fmt.Sprintf("GoSchemaAssertionEvent%d", schemaAssertionEventID.Add(1))
	event.EventPath = []string{}
	event.EventParentID = nil
	event.EventEmittedByHandlerID = nil
	event.EventPendingBusCount = 0
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
		"event_slow_timeout":              300.0,
		"event_handler_concurrency":       "serial",
		"event_handler_completion":        "all",
		"event_handler_slow_timeout":      30.0,
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

// Folded from bridges_test.go to keep test layout class-based.
func TestJSONLEventBridgeForwardsEventsThroughFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	writer := abxbus.NewJSONLEventBridge(path, 0.01, "JSONLWriter")
	reader := abxbus.NewJSONLEventBridge(path, 0.01, "JSONLReader")
	defer writer.Close()
	defer reader.Close()

	received := make(chan *abxbus.BaseEvent, 1)
	reader.OnEventName("JSONLTestEvent", "capture", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		received <- event
		return "ok", nil
	}, nil)
	if err := reader.Start(); err != nil {
		t.Fatal(err)
	}

	event := abxbus.NewBaseEvent("JSONLTestEvent", map[string]any{"value": "hello"})
	if _, err := writer.Emit(event); err != nil {
		t.Fatal(err)
	}

	select {
	case inbound := <-received:
		if inbound.EventType != "JSONLTestEvent" || inbound.Payload["value"] != "hello" {
			t.Fatalf("unexpected inbound event: %#v", inbound)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for JSONL bridge event")
	}
}

func TestJSONLEventBridgeClampsTinyPollInterval(t *testing.T) {
	bridge := abxbus.NewJSONLEventBridge(filepath.Join(t.TempDir(), "events.jsonl"), 0.0000000001, "JSONLClamp")
	if bridge.PollInterval < time.Millisecond {
		t.Fatalf("poll interval should be clamped to a positive ticker-safe duration, got %s", bridge.PollInterval)
	}
}

func TestJSONLEventBridgeIgnoresMalformedLinesAndKeepsPolling(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	reader := abxbus.NewJSONLEventBridge(path, 0.01, "JSONLReaderMalformed")
	defer reader.Close()

	received := make(chan *abxbus.BaseEvent, 1)
	reader.OnEventName("ValidEvent", "capture", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		received <- event
		return nil, nil
	}, nil)

	if _, err := writerFile(path, []byte("{bad json}\n")); err != nil {
		t.Fatal(err)
	}
	writer := abxbus.NewJSONLEventBridge(path, 0.01, "JSONLWriterMalformed")
	defer writer.Close()
	if _, err := writer.EmitEventName("ValidEvent", map[string]any{"ok": true}); err != nil {
		t.Fatal(err)
	}

	select {
	case inbound := <-received:
		if inbound.Payload["ok"] != true {
			t.Fatalf("unexpected payload: %#v", inbound.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for valid event after malformed line")
	}
}

func writerFile(path string, data []byte) (int, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, err
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return 0, err
	}
	return len(data), nil
}

func TestJSONLEventBridgeRoundtripBetweenProcesses(t *testing.T) {
	tempDir := t.TempDir()
	jsonlPath := filepath.Join(tempDir, "events.jsonl")
	assertJSONLBridgeRoundtrip(t, jsonlPath)
	latencyMS := measureJSONLBridgeWarmLatencyMS(t, filepath.Join(tempDir, "events-latency.jsonl"))
	t.Logf("LATENCY go jsonl %.3fms", latencyMS)
}

func assertJSONLBridgeRoundtrip(t *testing.T, jsonlPath string) {
	t.Helper()
	tempDir := t.TempDir()
	readyPath := filepath.Join(tempDir, "worker.ready")
	outputPath := filepath.Join(tempDir, "received.json")
	configPath := filepath.Join(tempDir, "worker_config.json")
	config := bridgeWorkerConfig{
		Path:       jsonlPath,
		ReadyPath:  readyPath,
		OutputPath: outputPath,
	}
	writeJSONFile(t, configPath, config)

	worker := startBridgeWorker(t, configPath)
	defer stopProcess(worker)
	waitForPath(t, readyPath, worker, 30*time.Second)

	sender := abxbus.NewJSONLEventBridge(jsonlPath, 0.05, "JSONLSender")
	defer sender.Close()
	outbound := newIPCPingEvent("jsonl_ok")
	if _, err := sender.Emit(outbound); err != nil {
		t.Fatal(err)
	}

	waitForPath(t, outputPath, worker, 30*time.Second)
	var received map[string]any
	readJSONFile(t, outputPath, &received)
	var expected map[string]any
	if err := json.Unmarshal(mustJSON(t, outbound), &expected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(normalizeRoundtripPayload(received), normalizeRoundtripPayload(expected)) {
		t.Fatalf("JSONL bridge payload mismatch\nexpected: %#v\nactual: %#v", expected, received)
	}
}

func measureJSONLBridgeWarmLatencyMS(t *testing.T, jsonlPath string) float64 {
	t.Helper()
	sender := abxbus.NewJSONLEventBridge(jsonlPath, 0.001, "JSONLLatencySender")
	receiver := abxbus.NewJSONLEventBridge(jsonlPath, 0.001, "JSONLLatencyReceiver")
	defer sender.Close()
	defer receiver.Close()

	warmupPrefix := "warmup_" + time.Now().Format("150405.000000000") + "_"
	measuredPrefix := "measured_" + time.Now().Format("150405.000000000") + "_"
	const warmupTarget = 5
	const measuredTarget = 1000

	warmupSeen := make(chan struct{})
	measuredSeen := make(chan struct{})
	countsMu := sync.Mutex{}
	warmupCount := 0
	measuredCount := 0
	warmupOnce := sync.Once{}
	measuredOnce := sync.Once{}
	receiver.OnEventName("IPCPingEvent", "latency_capture", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		label, _ := event.Payload["label"].(string)
		countsMu.Lock()
		defer countsMu.Unlock()
		if strings.HasPrefix(label, warmupPrefix) {
			warmupCount++
			if warmupCount == warmupTarget {
				warmupOnce.Do(func() { close(warmupSeen) })
			}
		}
		if strings.HasPrefix(label, measuredPrefix) {
			measuredCount++
			if measuredCount == measuredTarget {
				measuredOnce.Do(func() { close(measuredSeen) })
			}
		}
		return nil, nil
	}, nil)
	if err := receiver.Start(); err != nil {
		t.Fatal(err)
	}
	if err := sender.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	emitBatch := func(prefix string, count int) {
		t.Helper()
		for i := 0; i < count; i++ {
			if _, err := sender.Emit(newIPCPingEvent(prefix + strconv.Itoa(i))); err != nil {
				t.Fatal(err)
			}
		}
	}
	emitBatch(warmupPrefix, warmupTarget)
	waitForSignal(t, warmupSeen, 60*time.Second, "warmup JSONL bridge events")

	start := time.Now()
	emitBatch(measuredPrefix, measuredTarget)
	waitForSignal(t, measuredSeen, 120*time.Second, "measured JSONL bridge events")
	return float64(time.Since(start).Microseconds()) / 1000.0 / measuredTarget
}

type bridgeWorkerConfig struct {
	Path       string `json:"path"`
	ReadyPath  string `json:"ready_path"`
	OutputPath string `json:"output_path"`
}

func newIPCPingEvent(label string) *abxbus.BaseEvent {
	event := abxbus.NewBaseEvent("IPCPingEvent", map[string]any{"label": label})
	event.EventResultType = map[string]any{
		"$schema": "https://json-schema.org/draft/2020-12/schema",
		"type":    "object",
		"properties": map[string]any{
			"ok":    map[string]any{"type": "boolean"},
			"score": map[string]any{"type": "number"},
			"tags":  map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
		},
		"required":             []any{"ok", "score", "tags"},
		"additionalProperties": false,
	}
	return event
}

func writeJSONFile(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func readJSONFile(t *testing.T, path string, value any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, value); err != nil {
		t.Fatal(err)
	}
}

type bridgeWorkerProcess struct {
	cmd    *exec.Cmd
	done   chan error
	stdout bytes.Buffer
	stderr bytes.Buffer
}

func startBridgeWorker(t *testing.T, configPath string) *bridgeWorkerProcess {
	t.Helper()
	cmd := exec.Command("go", "run", "./roundtrip_cli", "jsonl-listener", configPath)
	worker := &bridgeWorkerProcess{cmd: cmd, done: make(chan error, 1)}
	cmd.Stdout = &worker.stdout
	cmd.Stderr = &worker.stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		worker.done <- cmd.Wait()
	}()
	return worker
}

func stopProcess(worker *bridgeWorkerProcess) {
	if worker == nil || worker.cmd == nil || worker.cmd.Process == nil {
		return
	}
	select {
	case <-worker.done:
		return
	default:
	}
	_ = worker.cmd.Process.Signal(os.Interrupt)
	select {
	case <-worker.done:
	case <-time.After(250 * time.Millisecond):
		_ = worker.cmd.Process.Kill()
		<-worker.done
	}
}

func waitForPath(t *testing.T, path string, worker *bridgeWorkerProcess, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if worker != nil {
			select {
			case err := <-worker.done:
				worker.done <- err
				t.Fatalf("worker exited early: %v\nstdout:\n%s\nstderr:\n%s", err, worker.stdout.String(), worker.stderr.String())
			default:
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("path did not appear in time: %s", path)
}

func waitForSignal(t *testing.T, done <-chan struct{}, timeout time.Duration, label string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func normalizeRoundtripPayload(payload map[string]any) map[string]any {
	normalized := map[string]any{}
	for key, value := range payload {
		normalized[key] = value
	}
	delete(normalized, "event_id")
	delete(normalized, "event_path")
	delete(normalized, "event_results")
	normalized["event_pending_bus_count"] = 0
	if status, _ := normalized["event_status"].(string); status == "pending" || status == "started" {
		normalized["event_status"] = "pending"
		normalized["event_started_at"] = nil
		normalized["event_completed_at"] = nil
	}
	if normalized["event_concurrency"] == nil {
		normalized["event_concurrency"] = "bus-serial"
	}
	if normalized["event_handler_concurrency"] == nil {
		normalized["event_handler_concurrency"] = "serial"
	}
	if normalized["event_handler_completion"] == nil {
		normalized["event_handler_completion"] = "all"
	}
	return normalized
}
