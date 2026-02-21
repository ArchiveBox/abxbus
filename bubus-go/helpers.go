package bubus

import (
	"fmt"
	"sync"
	"time"
)

var monotonic_mu sync.Mutex
var monotonic_last = time.Now().UTC()

func monotonicDatetime(isoString ...string) string {
	if len(isoString) > 0 {
		t, err := time.Parse(time.RFC3339Nano, isoString[0])
		if err != nil {
			panic(fmt.Errorf("invalid ISO datetime: %w", err))
		}
		return t.UTC().Format(time.RFC3339Nano)
	}
	monotonic_mu.Lock()
	defer monotonic_mu.Unlock()
	now := time.Now().UTC()
	if !now.After(monotonic_last) {
		now = monotonic_last.Add(time.Nanosecond)
	}
	monotonic_last = now
	return now.Format(time.RFC3339Nano)
}

func ptr[T any](v T) *T { return &v }

func IntPtr(v int) *int { return &v }
