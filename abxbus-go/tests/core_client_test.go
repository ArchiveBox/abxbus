package abxbus_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go/v2"
)

type GoClientEvent struct {
	Value int `json:"value"`
}

type GoClientErrorEvent struct {
	Value int `json:"value"`
}

func TestRustCoreClientRoundtripsVersionedTachyonProtocol(t *testing.T) {
	ctx, cancel := coreClientTestContext()
	defer cancel()

	client, err := abxbus.NewRustCoreClient(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	responses, err := client.Request(map[string]any{
		"type": "register_bus",
		"bus": map[string]any{
			"bus_id":  "go-bus",
			"name":    "GoBus",
			"label":   "GoBus#0001",
			"host_id": "go-host",
			"defaults": map[string]any{
				"event_concurrency":          "bus-serial",
				"event_handler_concurrency":  "serial",
				"event_handler_completion":   "all",
				"event_timeout":              60,
				"event_slow_timeout":         300,
				"event_handler_timeout":      nil,
				"event_handler_slow_timeout": 30,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) == 0 {
		t.Fatal("expected at least one response")
	}
	if responses[0].ProtocolVersion != abxbus.CoreProtocolVersion {
		t.Fatalf("unexpected protocol version: %d", responses[0].ProtocolVersion)
	}

	message := responses[0].Message
	patch, _ := message["patch"].(map[string]any)
	bus, _ := patch["bus"].(map[string]any)
	if message["type"] != "patch" || patch["type"] != "bus_registered" || bus["bus_id"] != "go-bus" {
		t.Fatalf("unexpected response message: %+v", message)
	}
}

func TestRustCoreClientDrivesNativeHandlerTurn(t *testing.T) {
	ctx, cancel := coreClientTestContext()
	defer cancel()

	client, err := abxbus.NewRustCoreClient(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.RegisterBus(coreClientBusRecord("go-turn-bus", "GoTurnBus#0001")); err != nil {
		t.Fatal(err)
	}
	if _, err := client.RegisterHandler(map[string]any{
		"handler_id":            "go-handler",
		"bus_id":                "go-turn-bus",
		"host_id":               "go-host",
		"event_pattern":         "GoClientEvent",
		"handler_name":          "go_handler",
		"handler_file_path":     nil,
		"handler_registered_at": "2026-05-08T00:00:00Z",
		"handler_timeout":       nil,
		"handler_slow_timeout":  nil,
		"handler_concurrency":   nil,
		"handler_completion":    nil,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.EmitEvent(coreClientEventRecord(t, GoClientEvent{Value: 41}), "go-turn-bus"); err != nil {
		t.Fatal(err)
	}

	responses, err := client.ProcessNextRoute("go-turn-bus")
	if err != nil {
		t.Fatal(err)
	}
	invocation := findMessage(t, responses, "invoke_handler")
	if invocation["handler_id"] != "go-handler" {
		t.Fatalf("unexpected invocation: %+v", invocation)
	}
	eventSnapshot := invocation["event_snapshot"].(map[string]any)
	value := int(eventSnapshot["value"].(float64)) + 1

	responses, err = client.CompleteHandler(invocation, value, false)
	if err != nil {
		t.Fatal(err)
	}
	completed := findPatch(t, responses, "result_completed")
	result := completed["result"].(map[string]any)
	if numericAsInt(result["result"]) != 42 {
		t.Fatalf("unexpected handler result: %+v", result)
	}
	routeID := result["route_id"].(string)

	responses, err = client.ProcessRoute(routeID)
	if err != nil {
		t.Fatal(err)
	}
	if findPatchMaybe(responses, "event_completed") == nil {
		snapshot, err := client.GetEvent(eventSnapshot["event_id"].(string))
		if err != nil {
			t.Fatal(err)
		}
		if snapshot == nil || snapshot["event_status"] != "completed" {
			t.Fatalf("event did not complete: responses=%+v snapshot=%+v", responses, snapshot)
		}
	}
}

func TestRustCoreClientCommitsHostErrorOutcome(t *testing.T) {
	ctx, cancel := coreClientTestContext()
	defer cancel()

	client, err := abxbus.NewRustCoreClient(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.RegisterBus(coreClientBusRecord("go-error-bus", "GoErrorBus#0001")); err != nil {
		t.Fatal(err)
	}
	handler := map[string]any{
		"handler_id":            "go-error-handler",
		"bus_id":                "go-error-bus",
		"host_id":               "go-host",
		"event_pattern":         "GoClientErrorEvent",
		"handler_name":          "go_handler",
		"handler_file_path":     nil,
		"handler_registered_at": "2026-05-08T00:00:00Z",
		"handler_timeout":       nil,
		"handler_slow_timeout":  nil,
		"handler_concurrency":   nil,
		"handler_completion":    nil,
	}
	if _, err := client.RegisterHandler(handler); err != nil {
		t.Fatal(err)
	}
	if _, err := client.EmitEvent(coreClientEventRecord(t, GoClientErrorEvent{Value: 41}), "go-error-bus"); err != nil {
		t.Fatal(err)
	}
	responses, err := client.ProcessNextRoute("go-error-bus")
	if err != nil {
		t.Fatal(err)
	}
	invocation := findMessage(t, responses, "invoke_handler")
	responses, err = client.ErrorHandler(invocation, errors.New("go boom"))
	if err != nil {
		t.Fatal(err)
	}
	patch := findPatch(t, responses, "result_completed")
	result := patch["result"].(map[string]any)
	errRecord := result["error"].(map[string]any)
	if result["status"] != "error" || errRecord["kind"] != "host_error" || errRecord["message"] != "go boom" {
		t.Fatalf("unexpected error result: %+v", result)
	}
}

func TestRustCoreClientQueriesCoreOwnedEventSnapshots(t *testing.T) {
	ctx, cancel := coreClientTestContext()
	defer cancel()

	client, err := abxbus.NewRustCoreClient(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.RegisterBus(coreClientBusRecord("go-query-bus", "GoQueryBus#0001")); err != nil {
		t.Fatal(err)
	}
	first := coreClientEventRecord(t, GoClientEvent{Value: 1})
	first["event_id"] = "018f8e40-1234-7000-8000-00000000abd0"
	second := coreClientEventRecord(t, GoClientEvent{Value: 2})
	second["event_id"] = "018f8e40-1234-7000-8000-00000000abd1"
	if _, err := client.EmitEvent(first, "go-query-bus"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.EmitEvent(second, "go-query-bus"); err != nil {
		t.Fatal(err)
	}

	snapshot, err := client.GetEvent("018f8e40-1234-7000-8000-00000000abd0")
	if err != nil {
		t.Fatal(err)
	}
	limit := 1
	events, err := client.ListEvents("GoClientEvent", &limit)
	if err != nil {
		t.Fatal(err)
	}

	if snapshot["event_id"] != "018f8e40-1234-7000-8000-00000000abd0" || int(snapshot["value"].(float64)) != 1 {
		t.Fatalf("unexpected event snapshot: %+v", snapshot)
	}
	if len(events) != 1 || events[0]["event_id"] != "018f8e40-1234-7000-8000-00000000abd1" {
		t.Fatalf("unexpected event list: %+v", events)
	}
}

func coreClientTestContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 180*time.Second)
}

func coreClientBusRecord(busID string, label string) map[string]any {
	return map[string]any{
		"bus_id":  busID,
		"name":    busID,
		"label":   label,
		"host_id": "go-host",
		"defaults": map[string]any{
			"event_concurrency":          "bus-serial",
			"event_handler_concurrency":  "serial",
			"event_handler_completion":   "all",
			"event_timeout":              60,
			"event_slow_timeout":         300,
			"event_handler_timeout":      nil,
			"event_handler_slow_timeout": 30,
		},
	}
}

func coreClientEventRecord(t *testing.T, input any) map[string]any {
	t.Helper()
	event, err := abxbus.Event(input)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	delete(record, "event_pending_bus_count")
	return record
}

func findMessage(t *testing.T, responses []abxbus.CoreProtocolEnvelope, messageType string) map[string]any {
	t.Helper()
	for _, response := range responses {
		message := response.Message
		if message["type"] == messageType {
			return message
		}
	}
	t.Fatalf("missing message type %s in %d responses", messageType, len(responses))
	return nil
}

func findPatch(t *testing.T, responses []abxbus.CoreProtocolEnvelope, patchType string) map[string]any {
	t.Helper()
	if patch := findPatchMaybe(responses, patchType); patch != nil {
		return patch
	}
	t.Fatalf("missing patch type %s in %d responses", patchType, len(responses))
	return nil
}

func findPatchMaybe(responses []abxbus.CoreProtocolEnvelope, patchType string) map[string]any {
	for _, response := range responses {
		message := response.Message
		if message["type"] != "patch" {
			continue
		}
		patch, ok := message["patch"].(map[string]any)
		if ok && patch["type"] == patchType {
			return patch
		}
	}
	return nil
}

func mustResponses(t *testing.T, responses []abxbus.CoreProtocolEnvelope, err error) []abxbus.CoreProtocolEnvelope {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	return responses
}

func numericAsInt(value any) int {
	switch v := value.(type) {
	case int:
		return v
	case int8:
		return int(v)
	case int16:
		return int(v)
	case int32:
		return int(v)
	case int64:
		return int(v)
	case uint:
		return int(v)
	case uint8:
		return int(v)
	case uint16:
		return int(v)
	case uint32:
		return int(v)
	case uint64:
		return int(v)
	case float32:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}
