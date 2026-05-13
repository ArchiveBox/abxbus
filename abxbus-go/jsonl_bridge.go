package abxbus

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type JSONLEventBridge struct {
	Path         string
	PollInterval time.Duration
	Name         string

	inboundBus  *EventBus
	byteOffset  int64
	pendingLine string
	running     bool
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	mu          sync.Mutex
}

func NewJSONLEventBridge(path string, pollIntervalSeconds float64, name string) *JSONLEventBridge {
	if pollIntervalSeconds <= 0 {
		pollIntervalSeconds = 0.25
	}
	if name == "" {
		name = "JSONLEventBridge_" + suffix(newUUIDv7String(), 8)
	}
	pollInterval := time.Duration(pollIntervalSeconds * float64(time.Second))
	if pollInterval < time.Millisecond {
		pollInterval = time.Millisecond
	}
	zeroHistory := 0
	return &JSONLEventBridge{
		Path:         path,
		PollInterval: pollInterval,
		Name:         name,
		inboundBus:   NewEventBus(name, &EventBusOptions{MaxHistorySize: &zeroHistory}),
	}
}

func (b *JSONLEventBridge) On(eventPattern string, handlerName string, handler any, options *EventHandler) *EventHandler {
	_ = b.Start()
	return b.inboundBus.On(eventPattern, handlerName, handler, options)
}

func (b *JSONLEventBridge) Emit(event *BaseEvent) (*BaseEvent, error) {
	if err := b.Start(); err != nil {
		return nil, err
	}
	payload, err := event.ToJSON()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(b.Path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(b.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if _, err := file.Write(append(payload, '\n')); err != nil {
		return nil, err
	}
	return event, nil
}

func (b *JSONLEventBridge) Dispatch(event *BaseEvent) (*BaseEvent, error) {
	return b.Emit(event)
}

func (b *JSONLEventBridge) Start() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.running {
		return nil
	}
	if b.Path == "" {
		return errors.New("JSONLEventBridge path is required")
	}
	if err := os.MkdirAll(filepath.Dir(b.Path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(b.Path, os.O_CREATE|os.O_RDONLY, 0o644)
	if err != nil {
		return err
	}
	offset, err := file.Seek(0, 2)
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	b.byteOffset = offset
	b.pendingLine = ""
	b.running = true
	b.wg.Add(1)
	go b.listenLoop(ctx)
	return nil
}

func (b *JSONLEventBridge) Close() {
	b.mu.Lock()
	cancel := b.cancel
	b.running = false
	b.cancel = nil
	b.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	b.wg.Wait()
	b.inboundBus.Destroy()
}

func (b *JSONLEventBridge) listenLoop(ctx context.Context) {
	defer b.wg.Done()
	ticker := time.NewTicker(b.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = b.pollNewLines()
		}
	}
}

func (b *JSONLEventBridge) pollNewLines() error {
	chunk, nextOffset, err := b.readAppendedText()
	if err != nil {
		return err
	}
	b.mu.Lock()
	previousOffset := b.byteOffset
	b.byteOffset = nextOffset
	if nextOffset < previousOffset {
		b.pendingLine = ""
	}
	combined := b.pendingLine + chunk
	lines := strings.Split(combined, "\n")
	if len(lines) > 0 {
		b.pendingLine = lines[len(lines)-1]
		lines = lines[:len(lines)-1]
	}
	b.mu.Unlock()

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var payload json.RawMessage
		if err := json.Unmarshal([]byte(line), &payload); err != nil {
			continue
		}
		event, err := BaseEventFromJSON(payload)
		if err != nil {
			continue
		}
		resetInboundEvent(event)
		b.inboundBus.Emit(event)
	}
	return nil
}

func (b *JSONLEventBridge) readAppendedText() (string, int64, error) {
	b.mu.Lock()
	offset := b.byteOffset
	b.mu.Unlock()

	file, err := os.Open(b.Path)
	if os.IsNotExist(err) {
		return "", 0, nil
	}
	if err != nil {
		return "", offset, err
	}
	defer file.Close()

	size, err := file.Seek(0, 2)
	if err != nil {
		return "", offset, err
	}
	startOffset := offset
	if size < offset {
		startOffset = 0
	}
	if size == startOffset {
		return "", size, nil
	}
	if _, err := file.Seek(startOffset, 0); err != nil {
		return "", offset, err
	}
	data, err := io.ReadAll(file)
	if err != nil {
		return "", offset, err
	}
	return string(data), size, nil
}

func resetInboundEvent(event *BaseEvent) {
	event.EventStatus = "pending"
	event.EventStartedAt = nil
	event.EventCompletedAt = nil
	event.EventPendingBusCount = 0
	event.EventResults = map[string]*EventResult{}
	event.Bus = nil
	event.done_ch = make(chan struct{})
	event.done_once = sync.Once{}
}
