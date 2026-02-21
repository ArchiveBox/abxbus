package bubus

import (
	"testing"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestEventHistoryFindMatchesPastAndChildOf(t *testing.T) {
	h := bubus.NewEventHistory(bubus.IntPtr(100), false)
	parent := bubus.NewBaseEvent("ParentEvent", nil)
	child := bubus.NewBaseEvent("ChildEvent", map[string]any{"k": "v"})
	child.EventParentID = &parent.EventID
	h.AddEvent(parent)
	h.AddEvent(child)

	found := h.Find("ChildEvent", nil, &bubus.EventHistoryFindOptions{Past: true, ChildOf: parent})
	if found == nil || found.EventID != child.EventID {
		t.Fatalf("expected child match, got %#v", found)
	}

	notFound := h.Find("ChildEvent", nil, &bubus.EventHistoryFindOptions{Past: false})
	if notFound != nil {
		t.Fatalf("expected nil when past=false, got %#v", notFound)
	}
}
