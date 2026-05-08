package abxbus_test

import (
	"testing"

	"github.com/google/uuid"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestGeneratedRuntimeIDsUseExpectedUUIDVersions(t *testing.T) {
	busID, err := uuid.Parse(abxbus.NewEventBus("IDsBus", nil).ID)
	if err != nil {
		t.Fatal(err)
	}
	eventID, err := uuid.Parse(abxbus.NewBaseEvent("IDsEvent", nil).EventID)
	if err != nil {
		t.Fatal(err)
	}
	handlerID := abxbus.ComputeHandlerID(
		"018f8e40-1234-7000-8000-000000001234",
		"handler",
		nil,
		"2025-01-02T03:04:05.678901000Z",
		"IDsEvent",
	)
	parsedHandlerID, err := uuid.Parse(handlerID)
	if err != nil {
		t.Fatal(err)
	}
	if busID.Version() != 7 || eventID.Version() != 7 || parsedHandlerID.Version() != 5 {
		t.Fatalf("unexpected uuid versions: bus=%d event=%d handler=%d", busID.Version(), eventID.Version(), parsedHandlerID.Version())
	}
}
