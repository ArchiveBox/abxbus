package abxbus_test

import (
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestEventResultJSONRoundtrip(t *testing.T) {
	bus := abxbus.NewEventBus("ResultBus", nil)
	h := abxbus.NewEventHandler(bus.Name, bus.ID, "Evt", "h", nil)
	e := abxbus.NewBaseEvent("Evt", nil)
	r := abxbus.NewEventResult(e, h)
	r.Status = abxbus.EventResultCompleted
	r.Result = "ok"
	r.Error = "boom"
	now := "2026-02-21T00:00:00.000000000Z"
	r.StartedAt = &now
	r.CompletedAt = &now

	data, err := r.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	round, err := abxbus.EventResultFromJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	if round.ID != r.ID {
		t.Fatalf("id mismatch: %s vs %s", round.ID, r.ID)
	}
	if round.Status != r.Status {
		t.Fatalf("status mismatch: %s vs %s", round.Status, r.Status)
	}
	if round.HandlerID != h.ID || round.EventID != e.EventID {
		t.Fatal("handler/event ID roundtrip mismatch")
	}
	if round.HandlerName != h.HandlerName || round.EventBusName != h.EventBusName || round.EventBusID != h.EventBusID {
		t.Fatal("handler/event bus metadata mismatch")
	}
	if round.Result != "ok" || round.Error != "boom" {
		t.Fatalf("result/error mismatch after roundtrip: %#v %#v", round.Result, round.Error)
	}
	if round.StartedAt == nil || *round.StartedAt != now || round.CompletedAt == nil || *round.CompletedAt != now {
		t.Fatalf("timestamp mismatch after roundtrip: started=%#v completed=%#v", round.StartedAt, round.CompletedAt)
	}
}

func TestEventResultWaitBlocksUntilSettled(t *testing.T) {
	bus := abxbus.NewEventBus("ResultBus", nil)
	h := abxbus.NewEventHandler(bus.Name, bus.ID, "Evt", "h", nil)
	e := abxbus.NewBaseEvent("Evt", nil)
	r := abxbus.NewEventResult(e, h)

	done := make(chan error, 1)
	go func() { done <- r.Wait() }()
	select {
	case err := <-done:
		t.Fatalf("wait should block while result is pending, got %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	r.Update(&abxbus.EventResultUpdateOptions{Status: abxbus.EventResultCompleted})
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for completed result")
	}
}

func TestEventResultUnmarshalResetsWaitStateForPendingResults(t *testing.T) {
	completed := []byte(`{
		"id":"result-1",
		"status":"completed",
		"event_id":"event-1",
		"handler_id":"handler-1",
		"handler_name":"handler",
		"handler_file_path":null,
		"handler_timeout":null,
		"handler_slow_timeout":null,
		"handler_registered_at":"2026-02-21T00:00:00.000000000Z",
		"handler_event_pattern":"Evt",
		"eventbus_name":"Bus",
		"eventbus_id":"bus-1",
		"started_at":null,
		"completed_at":"2026-02-21T00:00:00.000000000Z",
		"result":"ok",
		"error":null,
		"event_children":[]
	}`)
	pending := []byte(`{
		"id":"result-2",
		"status":"pending",
		"event_id":"event-2",
		"handler_id":"handler-2",
		"handler_name":"handler",
		"handler_file_path":null,
		"handler_timeout":null,
		"handler_slow_timeout":null,
		"handler_registered_at":"2026-02-21T00:00:00.000000000Z",
		"handler_event_pattern":"Evt",
		"eventbus_name":"Bus",
		"eventbus_id":"bus-1",
		"started_at":null,
		"completed_at":null,
		"result":null,
		"error":null,
		"event_children":[]
	}`)

	var result abxbus.EventResult
	if err := result.UnmarshalJSON(completed); err != nil {
		t.Fatal(err)
	}
	if err := result.Wait(); err != nil {
		t.Fatalf("completed result should wait immediately: %v", err)
	}
	if err := result.UnmarshalJSON(pending); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- result.Wait() }()
	select {
	case err := <-done:
		t.Fatalf("pending result should not inherit the closed wait channel from a previous unmarshal, got %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	result.Update(&abxbus.EventResultUpdateOptions{Status: abxbus.EventResultCompleted})
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for completed result")
	}
}

func TestEventResultUpdateKeepsConsistentOrderingSemanticsForStatusResultError(t *testing.T) {
	bus := abxbus.NewEventBus("StandaloneResultUpdateBus", nil)
	handler := abxbus.NewEventHandler(bus.Name, bus.ID, "StandaloneEvent", "handler", nil)
	event := abxbus.NewBaseEvent("StandaloneEvent", nil)
	result := abxbus.NewEventResult(event, handler)
	result.Error = "RuntimeError: existing"

	result.Update(&abxbus.EventResultUpdateOptions{Status: abxbus.EventResultCompleted})
	if result.Status != abxbus.EventResultCompleted {
		t.Fatalf("expected completed status, got %s", result.Status)
	}
	if result.Error != "RuntimeError: existing" {
		t.Fatalf("status-only update should preserve existing error, got %#v", result.Error)
	}

	result.Update(&abxbus.EventResultUpdateOptions{
		Status: abxbus.EventResultError,
		Result: "seeded",
	})
	if result.Result != "seeded" {
		t.Fatalf("result update should preserve seeded result, got %#v", result.Result)
	}
	if result.Status != abxbus.EventResultError {
		t.Fatalf("explicit status should apply after result, got %s", result.Status)
	}
}
