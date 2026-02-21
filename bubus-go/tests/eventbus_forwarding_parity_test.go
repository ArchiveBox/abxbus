package bubus

import (
	"context"
	"testing"
	"time"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestForwardingDoesNotSetSelfParentOnSameEvent(t *testing.T) {
	origin := bubus.NewEventBus("Origin", nil)
	target := bubus.NewEventBus("Target", nil)
	origin.On("*", "forward", func(ctx context.Context, e *bubus.BaseEvent) (any, error) { return target.Emit(e), nil }, nil)

	e := origin.Emit(bubus.NewBaseEvent("SelfParentForwardEvent", nil))
	if _, err := e.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	if e.EventParentID != nil {
		t.Fatalf("expected nil parent for forwarded same event, got %v", *e.EventParentID)
	}
	if len(e.EventPath) != 2 {
		t.Fatalf("expected both buses in event_path, got %v", e.EventPath)
	}
}

func TestForwardedEventUsesProcessingBusDefaults(t *testing.T) {
	busA := bubus.NewEventBus("ForwardDefaultsA", &bubus.EventBusOptions{EventHandlerConcurrency: bubus.EventHandlerConcurrencySerial})
	busB := bubus.NewEventBus("ForwardDefaultsB", &bubus.EventBusOptions{EventHandlerConcurrency: bubus.EventHandlerConcurrencyParallel})
	log := make(chan string, 16)

	h1 := func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		log <- e.Payload["mode"].(string) + ":b1_start"
		time.Sleep(15 * time.Millisecond)
		log <- e.Payload["mode"].(string) + ":b1_end"
		return "b1", nil
	}
	h2 := func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		log <- e.Payload["mode"].(string) + ":b2_start"
		time.Sleep(5 * time.Millisecond)
		log <- e.Payload["mode"].(string) + ":b2_end"
		return "b2", nil
	}
	trigger := func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		inherited := busA.Emit(bubus.NewBaseEvent("ForwardedDefaultsChildEvent", map[string]any{"mode": "inherited"}))
		busB.Emit(inherited)
		_, _ = inherited.Done(context.Background())

		override := busA.Emit(bubus.NewBaseEvent("ForwardedDefaultsChildEvent", map[string]any{"mode": "override"}))
		override.EventHandlerConcurrency = bubus.EventHandlerConcurrencySerial
		busB.Emit(override)
		_, _ = override.Done(context.Background())
		return nil, nil
	}
	busA.On("ForwardedDefaultsTriggerEvent", "trigger", trigger, nil)
	busB.On("ForwardedDefaultsChildEvent", "h1", h1, nil)
	busB.On("ForwardedDefaultsChildEvent", "h2", h2, nil)

	top := busA.Emit(bubus.NewBaseEvent("ForwardedDefaultsTriggerEvent", nil))
	_, _ = top.Done(context.Background())
	to := 1.0
	_ = busA.WaitUntilIdle(&to)
	_ = busB.WaitUntilIdle(&to)
	time.Sleep(20 * time.Millisecond)

	entries := []string{}
	for len(log) > 0 {
		entries = append(entries, <-log)
	}

	idx := func(s string) int {
		for i, v := range entries {
			if v == s {
				return i
			}
		}
		return -1
	}
	if !(idx("inherited:b2_start") < idx("inherited:b1_end")) {
		t.Fatalf("expected inherited mode parallel on processing bus, log=%v", entries)
	}
	if !(idx("override:b1_end") < idx("override:b2_start")) {
		t.Fatalf("expected override mode serial, log=%v", entries)
	}
}
