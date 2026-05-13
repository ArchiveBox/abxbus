package abxbus_test

import (
	"context"
	"encoding/json"
	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
	"github.com/google/uuid"
	"testing"
)

func TestEventHandlerJSONRoundtrip(t *testing.T) {
	h := abxbus.NewEventHandler("Bus", "bus-id", "Evt", "h", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) { return "ok", nil })
	data, err := h.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(data, &wire); err != nil {
		t.Fatal(err)
	}
	if wire["id"] != h.ID || wire["event_pattern"] != h.EventPattern || wire["handler_name"] != h.HandlerName {
		t.Fatalf("unexpected wire payload values: %#v", wire)
	}
	if wire["eventbus_name"] != h.EventBusName || wire["eventbus_id"] != h.EventBusID {
		t.Fatalf("event bus metadata mismatch in wire payload: %#v", wire)
	}

	round, err := abxbus.EventHandlerFromJSON(data, nil)
	if err != nil {
		t.Fatal(err)
	}
	if round.ID != h.ID {
		t.Fatalf("id mismatch: %s vs %s", round.ID, h.ID)
	}
	if round.EventPattern != h.EventPattern {
		t.Fatalf("event pattern mismatch: %s vs %s", round.EventPattern, h.EventPattern)
	}
	if round.EventBusName != h.EventBusName || round.EventBusID != h.EventBusID {
		t.Fatalf("event bus fields mismatch after roundtrip")
	}
	if round.HandlerName != h.HandlerName {
		t.Fatalf("handler name mismatch: %s vs %s", round.HandlerName, h.HandlerName)
	}
	if round.HandlerRegisteredAt != h.HandlerRegisteredAt {
		t.Fatalf("registered_at mismatch: %s vs %s", round.HandlerRegisteredAt, h.HandlerRegisteredAt)
	}
}

func TestEventBusOnSupportsEventFirstOptionalContextHandlerSignatures(t *testing.T) {
	bus := abxbus.NewEventBus("HandlerSignatureBus", nil)
	seen := []string{}

	bus.On("HandlerSignatureEvent", "value_error", func(event *abxbus.BaseEvent) (any, error) {
		seen = append(seen, event.EventType+":value_error")
		return "value_error", nil
	}, nil)
	bus.On("HandlerSignatureEvent", "value_error_ctx", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		if ctx == nil {
			t.Fatal("context should be available when requested")
		}
		seen = append(seen, event.EventType+":value_error_ctx")
		return "value_error_ctx", nil
	}, nil)
	bus.On("HandlerSignatureEvent", "error_only", func(event *abxbus.BaseEvent) error {
		seen = append(seen, event.EventType+":error_only")
		return nil
	}, nil)
	bus.On("HandlerSignatureEvent", "error_only_ctx", func(event *abxbus.BaseEvent, ctx context.Context) error {
		if ctx == nil {
			t.Fatal("context should be available when requested")
		}
		seen = append(seen, event.EventType+":error_only_ctx")
		return nil
	}, nil)
	bus.On("HandlerSignatureEvent", "void", func(event *abxbus.BaseEvent) {
		seen = append(seen, event.EventType+":void")
	}, nil)
	bus.On("HandlerSignatureEvent", "void_ctx", func(event *abxbus.BaseEvent, ctx context.Context) {
		if ctx == nil {
			t.Fatal("context should be available when requested")
		}
		seen = append(seen, event.EventType+":void_ctx")
	}, nil)

	values, err := bus.Emit(abxbus.NewBaseEvent("HandlerSignatureEvent", nil)).EventResultsList(&abxbus.EventResultOptions{
		RaiseIfAny:  false,
		RaiseIfNone: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 || values[0] != "value_error" || values[1] != "value_error_ctx" {
		t.Fatalf("expected only non-null handler values, got %#v", values)
	}
	if len(seen) != 6 {
		t.Fatalf("expected every handler signature to run, got %#v", seen)
	}
}

type legacyContextFirstHandler func(context.Context, *abxbus.BaseEvent) (any, error)

func TestEventBusOnPreservesLegacyContextFirstHandlerSignatures(t *testing.T) {
	bus := abxbus.NewEventBus("LegacyContextFirstHandlerBus", nil)
	gotNamed := false
	gotVoid := false

	bus.On("LegacyContextFirstEvent", "value_error_ctx_first", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		if ctx == nil {
			t.Fatal("context should be available when requested")
		}
		return event.EventType + ":value", nil
	}, nil)
	bus.On("LegacyContextFirstEvent", "named_ctx_first", legacyContextFirstHandler(func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		gotNamed = ctx != nil && event.EventType == "LegacyContextFirstEvent"
		return nil, nil
	}), nil)
	bus.On("LegacyContextFirstEvent", "void_ctx_first", func(ctx context.Context, event *abxbus.BaseEvent) {
		gotVoid = ctx != nil && event.EventType == "LegacyContextFirstEvent"
	}, nil)

	values, err := bus.Emit(abxbus.NewBaseEvent("LegacyContextFirstEvent", nil)).EventResultsList(&abxbus.EventResultOptions{
		RaiseIfAny:  false,
		RaiseIfNone: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0] != "LegacyContextFirstEvent:value" {
		t.Fatalf("expected ctx-first handler value result, got %#v", values)
	}
	if !gotNamed || !gotVoid {
		t.Fatalf("expected named and void ctx-first handlers to run, got named=%v void=%v", gotNamed, gotVoid)
	}
}

// Folded from event_handler_ids_test.go to keep test layout class-based.
func TestBusAndEventIDsAreUUIDv7(t *testing.T) {
	bus := abxbus.NewEventBus("BusId", nil)
	busID, err := uuid.Parse(bus.ID)
	if err != nil {
		t.Fatalf("bus id must parse as uuid: %v", err)
	}
	if busID.Version() != 7 {
		t.Fatalf("expected bus id uuid version 7, got %d", busID.Version())
	}

	event := abxbus.NewBaseEvent("work", nil)
	eventID, err := uuid.Parse(event.EventID)
	if err != nil {
		t.Fatalf("event id must parse as uuid: %v", err)
	}
	if eventID.Version() != 7 {
		t.Fatalf("expected event id uuid version 7, got %d", eventID.Version())
	}
}

func TestHandlerIDUsesV5NamespaceSeedCompatibleWithPythonTSRust(t *testing.T) {
	filePath := "~/project/app.py:123"
	eventbusID := "018f8e40-1234-7000-8000-000000001234"
	handlerName := "pkg.module.handler"
	handlerRegisteredAt := "2025-01-02T03:04:05.678901000Z"
	eventPattern := "StandaloneEvent"
	expectedID := "19ea9fe8-cfbe-541e-8a35-2579e4e9efff"

	computedID := abxbus.ComputeHandlerID(eventbusID, handlerName, &filePath, handlerRegisteredAt, eventPattern)
	if computedID != expectedID {
		t.Fatalf("handler id mismatch: got %s want %s", computedID, expectedID)
	}
	parsed, err := uuid.Parse(computedID)
	if err != nil {
		t.Fatalf("handler id must parse as uuid: %v", err)
	}
	if parsed.Version() != 5 {
		t.Fatalf("expected handler id uuid version 5, got %d", parsed.Version())
	}
}

func TestOnRecomputesHandlerIDAfterMetadataOverrides(t *testing.T) {
	filePath := "~/project/app.py:123"
	eventbusID := "018f8e40-1234-7000-8000-000000001234"
	handlerName := "pkg.module.handler"
	handlerRegisteredAt := "2025-01-02T03:04:05.678901000Z"
	eventPattern := "StandaloneEvent"
	expectedID := "19ea9fe8-cfbe-541e-8a35-2579e4e9efff"

	bus := abxbus.NewEventBus("StandaloneBus", &abxbus.EventBusOptions{ID: eventbusID})
	entry := bus.On(eventPattern, "original_name", func(e *abxbus.BaseEvent, ctx context.Context) (any, error) {
		return "ok", nil
	}, &abxbus.EventHandler{
		HandlerName:         handlerName,
		HandlerFilePath:     &filePath,
		HandlerRegisteredAt: handlerRegisteredAt,
	})

	if entry.ID != expectedID {
		t.Fatalf("handler id should be recomputed from overridden metadata, got %s want %s", entry.ID, expectedID)
	}
	if entry.HandlerName != handlerName || entry.HandlerFilePath == nil || *entry.HandlerFilePath != filePath {
		t.Fatalf("handler metadata overrides were not preserved: %#v", entry)
	}
	if entry.HandlerRegisteredAt != handlerRegisteredAt {
		t.Fatalf("handler registered_at override mismatch: got %s", entry.HandlerRegisteredAt)
	}
}
