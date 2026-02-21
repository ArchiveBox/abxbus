package bubus_test

import (
	"testing"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestEventHistoryBasic(t *testing.T) {
	h := bubus.NewEventHistory(nil, false)
	e1 := bubus.NewBaseEvent("A", nil)
	e2 := bubus.NewBaseEvent("B", nil)
	h.AddEvent(e1)
	h.AddEvent(e2)
	if h.Size() != 2 {
		t.Fatalf("expected size 2, got %d", h.Size())
	}
	if !h.Has(e1.EventID) || h.GetEvent(e2.EventID) == nil {
		t.Fatal("has/get failed")
	}
	vals := h.Values()
	if len(vals) != 2 || vals[0].EventID != e1.EventID || vals[1].EventID != e2.EventID {
		t.Fatal("order mismatch")
	}
}
