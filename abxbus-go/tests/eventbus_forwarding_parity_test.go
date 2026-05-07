package abxbus_test

import (
	"context"
	"sync"
	"testing"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestForwardingDoesNotSetSelfParentOnSameEvent(t *testing.T) {
	origin := abxbus.NewEventBus("Origin", nil)
	target := abxbus.NewEventBus("Target", nil)
	origin.On("*", "forward", func(ctx context.Context, e *abxbus.BaseEvent) (any, error) { return target.Emit(e), nil }, nil)

	e := origin.Emit(abxbus.NewBaseEvent("SelfParentForwardEvent", nil))
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
	busA := abxbus.NewEventBus("ForwardDefaultsA", &abxbus.EventBusOptions{EventHandlerConcurrency: abxbus.EventHandlerConcurrencySerial})
	busB := abxbus.NewEventBus("ForwardDefaultsB", &abxbus.EventBusOptions{EventHandlerConcurrency: abxbus.EventHandlerConcurrencyParallel})

	var mu sync.Mutex
	entries := []string{}
	appendEntry := func(v string) {
		mu.Lock()
		defer mu.Unlock()
		entries = append(entries, v)
	}

	h1StartedInherited := make(chan struct{}, 1)
	h1StartedOverride := make(chan struct{}, 1)
	h2StartedInherited := make(chan struct{}, 1)
	releaseInherited := make(chan struct{})
	releaseOverride := make(chan struct{})

	h1 := func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		mode := e.Payload["mode"].(string)
		appendEntry(mode + ":b1_start")
		switch mode {
		case "inherited":
			h1StartedInherited <- struct{}{}
			<-releaseInherited
		case "override":
			h1StartedOverride <- struct{}{}
			<-releaseOverride
		}
		appendEntry(mode + ":b1_end")
		return "b1", nil
	}
	h2 := func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		mode := e.Payload["mode"].(string)
		appendEntry(mode + ":b2_start")
		if mode == "inherited" {
			h2StartedInherited <- struct{}{}
		}
		appendEntry(mode + ":b2_end")
		return "b2", nil
	}
	trigger := func(ctx context.Context, e *abxbus.BaseEvent) (any, error) {
		inherited := busA.Emit(abxbus.NewBaseEvent("ForwardedDefaultsChildEvent", map[string]any{"mode": "inherited"}))
		busB.Emit(inherited)
		if _, err := inherited.Done(ctx); err != nil {
			return nil, err
		}

		override := busA.Emit(abxbus.NewBaseEvent("ForwardedDefaultsChildEvent", map[string]any{"mode": "override"}))
		override.EventHandlerConcurrency = abxbus.EventHandlerConcurrencySerial
		busB.Emit(override)
		if _, err := override.Done(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	}
	busA.On("ForwardedDefaultsTriggerEvent", "trigger", trigger, nil)
	busB.On("ForwardedDefaultsChildEvent", "h1", h1, nil)
	busB.On("ForwardedDefaultsChildEvent", "h2", h2, nil)

	top := busA.Emit(abxbus.NewBaseEvent("ForwardedDefaultsTriggerEvent", nil))
	select {
	case <-h1StartedInherited:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for inherited h1 start")
	}
	select {
	case <-h2StartedInherited:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for inherited h2 start before h1 release")
	}
	close(releaseInherited)

	select {
	case <-h1StartedOverride:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for override h1 start")
	}
	close(releaseOverride)

	if _, err := top.Done(context.Background()); err != nil {
		t.Fatal(err)
	}
	to := 2.0
	if !busA.WaitUntilIdle(&to) {
		t.Fatal("busA did not become idle")
	}
	if !busB.WaitUntilIdle(&to) {
		t.Fatal("busB did not become idle")
	}

	idx := func(s string) int {
		for i, v := range entries {
			if v == s {
				return i
			}
		}
		return -1
	}
	requireIndex := func(label string) int {
		i := idx(label)
		if i < 0 {
			t.Fatalf("missing required log entry %q, log=%v", label, entries)
		}
		return i
	}

	inheritedB2Start := requireIndex("inherited:b2_start")
	inheritedB1End := requireIndex("inherited:b1_end")
	if !(inheritedB2Start < inheritedB1End) {
		t.Fatalf("expected inherited mode parallel on processing bus, log=%v", entries)
	}
	overrideB1End := requireIndex("override:b1_end")
	overrideB2Start := requireIndex("override:b2_start")
	if !(overrideB1End < overrideB2Start) {
		t.Fatalf("expected override mode serial, log=%v", entries)
	}
}
