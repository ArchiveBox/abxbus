package bubus_test

import (
	"testing"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestEventHistoryRemoveAndTrim(t *testing.T) {
	h := bubus.NewEventHistory(bubus.IntPtr(2), true)
	e1 := bubus.NewBaseEvent("A", nil)
	e2 := bubus.NewBaseEvent("B", nil)
	e3 := bubus.NewBaseEvent("C", nil)
	e1.EventStatus = "completed"
	e2.EventStatus = "completed"
	e3.EventStatus = "completed"
	h.AddEvent(e1)
	h.AddEvent(e2)
	h.AddEvent(e3)
	removed := h.TrimEventHistory(nil)
	if removed == 0 {
		t.Fatal("expected trim to remove overage")
	}
	if h.Size() > 2 {
		t.Fatal("expected history to be trimmed to max")
	}
	if !h.RemoveEvent(e2.EventID) {
		t.Fatal("expected remove event to succeed")
	}
}
