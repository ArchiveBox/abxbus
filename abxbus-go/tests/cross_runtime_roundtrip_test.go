package abxbus_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
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
    "event_result_type": {"type": "object", "properties": {"ok": {"type": "boolean"}}},
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
	if schema, ok := events[0]["event_result_type"].(map[string]any); !ok || schema["type"] != "object" {
		t.Fatalf("event_result_type schema did not roundtrip: %#v", events[0]["event_result_type"])
	}
}
