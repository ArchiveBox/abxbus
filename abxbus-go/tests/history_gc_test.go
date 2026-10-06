package abxbus_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"weak"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go/v2"
)

func TestExpiredChildrenAreCollectibleUnderRetainedParent(t *testing.T) {
	directory := t.TempDir()
	bus := abxbus.NewEventBus("TTLReferenceRelease", &abxbus.EventBusOptions{EventTTL: ttlPtr(0)})
	t.Cleanup(bus.Destroy)
	var references []weak.Pointer[abxbus.BaseEvent]
	bus.On("SaveFile", "save", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		path := filepath.Join(directory, fmt.Sprint(len(references)))
		references = append(references, weak.Make(bus.EventHistory.GetEvent(event.EventID)))
		return nil, os.WriteFile(path, []byte("saved"), 0600)
	}, nil)
	bus.On("SaveFiles", "save_all", func(event *abxbus.BaseEvent, ctx context.Context) (any, error) {
		for i := 0; i < 10; i++ {
			if _, err := event.Emit(abxbus.NewBaseEvent("SaveFile", nil)).Now(); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}, nil)
	parent := abxbus.NewBaseEvent("SaveFiles", nil)
	parent.EventTTL = ttlPtr(-1)
	parent = bus.Emit(parent)
	if _, err := parent.Now(); err != nil {
		t.Fatal(err)
	}
	timeout := 5.0
	if !bus.WaitUntilIdle(&timeout) {
		t.Fatal("bus not idle")
	}
	if _, err := bus.Emit(abxbus.NewBaseEvent("Trim", nil)).Now(); err != nil {
		t.Fatal(err)
	}
	if !bus.WaitUntilIdle(&timeout) {
		t.Fatal("bus not idle")
	}
	runtime.GC()
	if bus.EventHistory.GetEvent(parent.EventID) == nil {
		t.Fatal("parent expired")
	}
	if len(references) != 10 {
		t.Fatalf("saved %d files", len(references))
	}
	for index, reference := range references {
		if reference.Value() != nil {
			t.Errorf("expired child %d retained", index)
		}
		contents, err := os.ReadFile(filepath.Join(directory, fmt.Sprint(index)))
		if err != nil || string(contents) != "saved" {
			t.Fatalf("file %d: %q %v", index, contents, err)
		}
	}
}
