package abxbus_test

import (
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestEventHistoryFindCoversFiltersAndEdgeCases(t *testing.T) {
	maxHistorySize := 100
	h := abxbus.NewEventHistory(&maxHistorySize, false)
	parent := abxbus.NewBaseEvent("ParentEvent", map[string]any{"kind": "parent"})
	parent.EventStatus = "completed"
	child := abxbus.NewBaseEvent("ChildEvent", map[string]any{"kind": "child", "k": "v"})
	child.EventParentID = &parent.EventID
	child.EventStatus = "completed"
	child.EventCreatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	oldChild := abxbus.NewBaseEvent("ChildEvent", map[string]any{"kind": "child", "k": "old"})
	oldChild.EventParentID = &parent.EventID
	oldChild.EventStatus = "completed"
	oldChild.EventCreatedAt = time.Now().Add(-3 * time.Second).UTC().Format(time.RFC3339Nano)
	other := abxbus.NewBaseEvent("OtherEvent", map[string]any{"kind": "other"})
	other.EventStatus = "started"

	h.AddEvent(parent)
	h.AddEvent(oldChild)
	h.AddEvent(child)
	h.AddEvent(other)

	foundChild := h.Find("ChildEvent", nil, &abxbus.EventHistoryFindOptions{Past: true, ChildOf: parent})
	if foundChild == nil || foundChild.EventID != child.EventID {
		t.Fatalf("expected newest matching child, got %#v", foundChild)
	}

	recentChild := h.Find("ChildEvent", nil, &abxbus.EventHistoryFindOptions{Past: 1.0, ChildOf: parent, Equals: map[string]any{"k": "v"}})
	if recentChild == nil || recentChild.EventID != child.EventID {
		t.Fatalf("expected recent child match with equals filter, got %#v", recentChild)
	}

	whereMatch := h.Find("*", func(event *abxbus.BaseEvent) bool {
		return event.Payload["kind"] == "other" && event.EventStatus == "started"
	}, &abxbus.EventHistoryFindOptions{Past: true})
	if whereMatch == nil || whereMatch.EventID != other.EventID {
		t.Fatalf("expected wildcard+where filter to find other event, got %#v", whereMatch)
	}

	eventTypeMatch := h.Find("*", nil, &abxbus.EventHistoryFindOptions{Past: true, Equals: map[string]any{"event_type": "ParentEvent", "event_status": "completed"}})
	if eventTypeMatch == nil || eventTypeMatch.EventID != parent.EventID {
		t.Fatalf("expected event_type/event_status equals match, got %#v", eventTypeMatch)
	}

	notFound := h.Find("ChildEvent", nil, &abxbus.EventHistoryFindOptions{Past: false})
	if notFound != nil {
		t.Fatalf("expected nil when past=false, got %#v", notFound)
	}
}
