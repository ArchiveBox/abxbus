package bubus_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	bubus "github.com/pirate/bbus/bubus-go"
)

func TestGlobalSerialAcrossBuses(t *testing.T) {
	b1 := bubus.NewEventBus("B1", &bubus.EventBusOptions{EventConcurrency: bubus.EventConcurrencyGlobalSerial})
	b2 := bubus.NewEventBus("B2", &bubus.EventBusOptions{EventConcurrency: bubus.EventConcurrencyGlobalSerial})

	var mu sync.Mutex
	inFlight := 0
	maxInFlight := 0
	order := []string{}
	h := func(busLabel string) func(context.Context, *bubus.BaseEvent) (any, error) {
		return func(ctx context.Context, e *bubus.BaseEvent) (any, error) {
			seq := int(e.Payload["n"].(int))
			mu.Lock()
			inFlight++
			if inFlight > maxInFlight {
				maxInFlight = inFlight
			}
			order = append(order, busLabel+":start:"+strconv.Itoa(seq))
			mu.Unlock()

			time.Sleep(5 * time.Millisecond)

			mu.Lock()
			order = append(order, busLabel+":end:"+strconv.Itoa(seq))
			inFlight--
			mu.Unlock()
			return nil, nil
		}
	}

	b1.On("Evt", "h1", h("b1"), nil)
	b2.On("Evt", "h2", h("b2"), nil)

	for i := 1; i <= 3; i++ {
		b1.Emit(bubus.NewBaseEvent("Evt", map[string]any{"n": i}))
		b2.Emit(bubus.NewBaseEvent("Evt", map[string]any{"n": i}))
	}

	timeout := 2.0
	if !b1.WaitUntilIdle(&timeout) {
		t.Fatal("b1 did not become idle")
	}
	if !b2.WaitUntilIdle(&timeout) {
		t.Fatal("b2 did not become idle")
	}

	if maxInFlight != 1 {
		t.Fatalf("expected strict global serial execution (max in flight=1), got %d, order=%v", maxInFlight, order)
	}

	seenB1 := []int{}
	seenB2 := []int{}
	for _, entry := range order {
		if len(entry) < 9 || entry[3:8] != "start" {
			continue
		}
		if entry[:2] == "b1" {
			seenB1 = append(seenB1, int(entry[len(entry)-1]-'0'))
		}
		if entry[:2] == "b2" {
			seenB2 = append(seenB2, int(entry[len(entry)-1]-'0'))
		}
	}
	if len(seenB1) != 3 || seenB1[0] != 1 || seenB1[1] != 2 || seenB1[2] != 3 {
		t.Fatalf("expected per-bus FIFO order for b1, got %v", seenB1)
	}
	if len(seenB2) != 3 || seenB2[0] != 1 || seenB2[1] != 2 || seenB2[2] != 3 {
		t.Fatalf("expected per-bus FIFO order for b2, got %v", seenB2)
	}

	b1.Destroy()
	b2.Destroy()
}
