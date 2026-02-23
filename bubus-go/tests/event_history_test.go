package bubus_test

import (
	"testing"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestEventHistory(t *testing.T) {
	max := 2
	h := bubus.NewEventHistory(&max, false)
	if h.Size() != 0 {
		t.Fatalf("new history should start empty, got %d", h.Size())
	}

	e1 := bubus.NewBaseEvent("A", nil)
	e2 := bubus.NewBaseEvent("B", nil)
	e3 := bubus.NewBaseEvent("C", nil)
	e1.EventStatus = "completed"
	e2.EventStatus = "started"
	e3.EventStatus = "completed"

	h.AddEvent(e1)
	h.AddEvent(e2)
	h.AddEvent(e3)

	if h.Size() != 2 {
		t.Fatalf("expected size 2 after trimming, got %d", h.Size())
	}
	if h.Has(e1.EventID) {
		t.Fatalf("expected oldest completed event to be trimmed")
	}
	if !h.Has(e2.EventID) || !h.Has(e3.EventID) {
		t.Fatalf("expected newer events to remain after trim")
	}
	if h.GetEvent("missing") != nil || h.Has("missing") {
		t.Fatalf("missing IDs should not be found")
	}

	vals := h.Values()
	if len(vals) != 2 || vals[0].EventID != e2.EventID || vals[1].EventID != e3.EventID {
		t.Fatalf("expected stable order after trim, got %#v", vals)
	}

	h.AddEvent(e2)
	if h.Size() != 2 {
		t.Fatalf("duplicate add should not change size, got %d", h.Size())
	}

	if !h.RemoveEvent(e2.EventID) {
		t.Fatalf("expected remove existing event to succeed")
	}
	if h.RemoveEvent("missing") {
		t.Fatalf("remove missing event should return false")
	}
	if h.Size() != 1 || !h.Has(e3.EventID) {
		t.Fatalf("unexpected final remove state")
	}
	if removed := h.TrimEventHistory(nil); removed != 0 {
		t.Fatalf("trim under max should remove 0 events, removed=%d", removed)
	}

	maxOne := 1
	noDrop := bubus.NewEventHistory(&maxOne, false)
	p1 := bubus.NewBaseEvent("P1", nil)
	p2 := bubus.NewBaseEvent("P2", nil)
	p1.EventStatus = "started"
	p2.EventStatus = "pending"
	noDrop.AddEvent(p1)
	noDrop.AddEvent(p2)
	if noDrop.Size() != 2 {
		t.Fatalf("max_history_drop=false should not force-drop in-progress events")
	}

	drop := bubus.NewEventHistory(&maxOne, true)
	d1 := bubus.NewBaseEvent("D1", nil)
	d2 := bubus.NewBaseEvent("D2", nil)
	d1.EventStatus = "started"
	d2.EventStatus = "pending"
	drop.AddEvent(d1)
	drop.AddEvent(d2)
	if drop.Size() != 1 || !drop.Has(d2.EventID) {
		t.Fatalf("max_history_drop=true should force-drop oldest when over max")
	}

	drop.Clear()
	if drop.Size() != 0 || len(drop.Values()) != 0 {
		t.Fatalf("clear should reset history")
	}
}
