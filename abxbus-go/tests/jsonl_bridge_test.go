package abxbus_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestJSONLEventBridgeForwardsEventsThroughFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	writer := abxbus.NewJSONLEventBridge(path, 0.01, "JSONLWriter")
	reader := abxbus.NewJSONLEventBridge(path, 0.01, "JSONLReader")
	defer writer.Close()
	defer reader.Close()

	received := make(chan *abxbus.BaseEvent, 1)
	reader.On("JSONLTestEvent", "capture", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
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

func TestJSONLEventBridgeIgnoresMalformedLinesAndKeepsPolling(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	reader := abxbus.NewJSONLEventBridge(path, 0.01, "JSONLReaderMalformed")
	defer reader.Close()

	received := make(chan *abxbus.BaseEvent, 1)
	reader.On("ValidEvent", "capture", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		received <- event
		return nil, nil
	}, nil)

	if _, err := writerFile(path, []byte("{bad json}\n")); err != nil {
		t.Fatal(err)
	}
	writer := abxbus.NewJSONLEventBridge(path, 0.01, "JSONLWriterMalformed")
	defer writer.Close()
	if _, err := writer.Emit(abxbus.NewBaseEvent("ValidEvent", map[string]any{"ok": true})); err != nil {
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
