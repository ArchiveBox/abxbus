package abxbus_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
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
	receiver.On("IPCPingEvent", "latency_capture", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
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

func TestJSONLBridgeListenerWorker(t *testing.T) {
	if os.Getenv("ABXBUS_GO_BRIDGE_WORKER") != "1" {
		t.Skip("bridge listener worker helper")
	}
	if len(os.Args) < 2 {
		t.Fatal("missing worker config path")
	}
	configPath := os.Args[len(os.Args)-1]
	var config bridgeWorkerConfig
	readJSONFile(t, configPath, &config)

	bridge := abxbus.NewJSONLEventBridge(config.Path, 0.05, "JSONLWorker")
	defer bridge.Close()

	received := make(chan struct{})
	receivedOnce := sync.Once{}
	bridge.On("*", "capture", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		if err := os.WriteFile(config.OutputPath, mustJSON(t, event), 0o644); err != nil {
			return nil, err
		}
		receivedOnce.Do(func() { close(received) })
		return nil, nil
	}, nil)
	if err := bridge.Start(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config.ReadyPath, []byte("ready"), 0o644); err != nil {
		t.Fatal(err)
	}

	select {
	case <-received:
	case <-time.After(30 * time.Second):
		t.Fatal("worker timed out waiting for bridge event")
	}
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
	cmd := exec.Command(os.Args[0], "-test.run=TestJSONLBridgeListenerWorker", "--", configPath)
	cmd.Env = append(os.Environ(), "ABXBUS_GO_BRIDGE_WORKER=1")
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
