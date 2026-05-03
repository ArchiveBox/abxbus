package bubus

import (
	"fmt"
	"sync"
	"time"
)

var monotonic_mu sync.Mutex
var monotonic_last = time.Now().UTC()
var fixedRFC3339Nano = "2006-01-02T15:04:05.000000000Z07:00"

func monotonicDatetime(isoString ...string) string {
	if len(isoString) > 0 {
		t, err := time.Parse(time.RFC3339Nano, isoString[0])
		if err != nil {
			panic(fmt.Errorf("invalid ISO datetime: %w", err))
		}
		return t.UTC().Format(fixedRFC3339Nano)
	}
	monotonic_mu.Lock()
	defer monotonic_mu.Unlock()
	now := time.Now().UTC()
	if !now.After(monotonic_last) {
		now = monotonic_last.Add(time.Nanosecond)
	}
	monotonic_last = now
	return now.Format(fixedRFC3339Nano)
}

func ptr[T any](v T) *T { return &v }

func IntPtr(v int) *int { return &v }
