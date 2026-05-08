package abxbus_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

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
	entry := bus.On(eventPattern, "original_name", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
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
