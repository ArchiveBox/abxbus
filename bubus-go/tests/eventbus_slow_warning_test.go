package bubus_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestSlowEventAndHandlerWarnings(t *testing.T) {
	event_slow := 0.01
	handler_slow := 0.005
	bus := bubus.NewEventBus("SlowWarnBus", &bubus.EventBusOptions{
		EventTimeout:            nil,
		EventSlowTimeout:        &event_slow,
		EventHandlerSlowTimeout: &handler_slow,
	})

	var mu sync.Mutex
	logs := []string{}
	original := bubus.SlowWarningLogger
	bubus.SlowWarningLogger = func(message string) {
		mu.Lock()
		logs = append(logs, message)
		mu.Unlock()
	}
	defer func() { bubus.SlowWarningLogger = original }()

	bus.On("Evt", "slow_handler", func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
		time.Sleep(30 * time.Millisecond)
		return "ok", nil
	}, nil)

	e := bus.Emit(bubus.NewBaseEvent("Evt", nil))
	_, err := e.Done(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(logs) == 0 {
		t.Fatal("expected slow warning logs")
	}
	seen_event := false
	seen_handler := false
	for _, line := range logs {
		if strings.Contains(line, "Slow event processing") {
			seen_event = true
		}
		if strings.Contains(line, "Slow event handler") {
			seen_handler = true
		}
	}
	if !seen_event || !seen_handler {
		t.Fatalf("expected both event and handler warnings, got event=%v handler=%v logs=%v", seen_event, seen_handler, logs)
	}
}
