package abxbus

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type CoreHandler = EventHandlerCallable

type coreInvocationContextKey struct{}
type coreOutcomeBatchContextKey struct{}

type coreOutcomeBatch struct {
	mu       sync.Mutex
	outcomes []map[string]any
}

func (b *coreOutcomeBatch) add(outcome map[string]any) {
	if b == nil || outcome == nil {
		return
	}
	b.mu.Lock()
	b.outcomes = append(b.outcomes, outcome)
	b.mu.Unlock()
}

func (b *coreOutcomeBatch) snapshot() []map[string]any {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]map[string]any{}, b.outcomes...)
}

const hardEventDeadlineThresholdSeconds = 5.0
const fastEventDeadlineAbortThresholdSeconds = 0.00025
const coreRouteSliceLimit = 65_536
const sharedRustCoreRetireHandlerThreshold = 4096

var fastTimeoutHandlers = &fastTimeoutHandlerPool{}
var cancelledContextDone = func() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}()

type cancelledValueContext struct {
	parent context.Context
}

func (c cancelledValueContext) Deadline() (time.Time, bool) {
	if c.parent == nil {
		return time.Time{}, false
	}
	return c.parent.Deadline()
}

func (c cancelledValueContext) Done() <-chan struct{} {
	return cancelledContextDone
}

func (c cancelledValueContext) Err() error {
	return context.Canceled
}

func (c cancelledValueContext) Value(key any) any {
	if c.parent == nil {
		return nil
	}
	return c.parent.Value(key)
}

type fastTimeoutHandlerPool struct {
	once sync.Once
	jobs chan func()
}

func (p *fastTimeoutHandlerPool) submit(job func()) {
	if job == nil {
		return
	}
	p.once.Do(func() {
		workerCount := runtime.GOMAXPROCS(0)
		if workerCount < 1 {
			workerCount = 1
		}
		p.jobs = make(chan func(), workerCount*256)
		for range workerCount {
			go func() {
				for queued := range p.jobs {
					queued()
				}
			}()
		}
	})
	select {
	case p.jobs <- job:
	default:
		go job()
	}
}

type RustCoreEventBusOptions struct {
	Core                       *RustCoreClient
	ID                         string
	EventConcurrency           EventConcurrencyMode
	EventHandlerConcurrency    EventHandlerConcurrencyMode
	EventHandlerCompletion     EventHandlerCompletionMode
	EventTimeout               *float64
	EventTimeoutSet            bool
	EventSlowTimeout           *float64
	EventSlowTimeoutSet        bool
	EventHandlerTimeout        *float64
	EventHandlerSlowTimeout    *float64
	EventHandlerSlowTimeoutSet bool
	MaxHistorySize             *int
	MaxHistorySizeSet          bool
	MaxHistoryDrop             bool
}

type RustCoreHandlerOptions struct {
	HandlerName        string
	HandlerTimeout     *float64
	HandlerSlowTimeout *float64
	HandlerConcurrency EventHandlerConcurrencyMode
	HandlerCompletion  EventHandlerCompletionMode
}

type RustCoreEventBus struct {
	Core                     *RustCoreClient
	Name                     string
	BusID                    string
	Label                    string
	EventConcurrency         EventConcurrencyMode
	EventHandlerConcurrency  EventHandlerConcurrencyMode
	EventHandlerCompletion   EventHandlerCompletionMode
	EventTimeout             *float64
	EventSlowTimeout         *float64
	EventHandlerTimeout      *float64
	EventHandlerSlowTimeout  *float64
	MaxHistorySize           *int
	MaxHistoryDrop           bool
	registeredMaxHistorySize *int
	registeredMaxHistoryDrop bool
	handlerEntries           map[string]*EventHandler
	localEventStarts         map[string]time.Time
	mu                       sync.RWMutex
	closed                   chan struct{}
	ownsCore                 bool
	sharedCore               bool
}

var rustCoreEventBusRegistry = struct {
	sync.Mutex
	instances map[*RustCoreEventBus]struct{}
}{instances: map[*RustCoreEventBus]struct{}{}}

var sharedRustCore = struct {
	sync.Mutex
	client       *RustCoreClient
	refs         int
	releaseTimer *time.Timer
}{}

const sharedRustCoreIdleTimeout = 500 * time.Millisecond

func acquireSharedRustCore(ctx context.Context) (*RustCoreClient, error) {
	sharedRustCore.Lock()
	defer sharedRustCore.Unlock()
	if sharedRustCore.releaseTimer != nil {
		sharedRustCore.releaseTimer.Stop()
		sharedRustCore.releaseTimer = nil
	}
	if sharedRustCore.client != nil && !sharedRustCore.client.closed {
		if _, exited := sharedRustCore.client.wait.tryErr(); !exited {
			sharedRustCore.refs++
			return sharedRustCore.client, nil
		}
		_ = sharedRustCore.client.Disconnect()
		sharedRustCore.client = nil
		sharedRustCore.refs = 0
	}
	client, err := newRustCoreClientAtSocketWithCommandContext(
		ctx,
		context.WithoutCancel(ctx),
		filepath.Join(os.TempDir(), "abxbus-core-"+uuid.NewString()+".sock"),
		"",
	)
	if err != nil {
		return nil, err
	}
	sharedRustCore.client = client
	sharedRustCore.refs = 1
	return client, nil
}

func releaseSharedRustCore(client *RustCoreClient, retire ...bool) error {
	shouldRetire := len(retire) > 0 && retire[0]
	sharedRustCore.Lock()
	if sharedRustCore.client != client {
		sharedRustCore.Unlock()
		return nil
	}
	if sharedRustCore.refs > 0 {
		sharedRustCore.refs--
	}
	if sharedRustCore.refs > 0 {
		sharedRustCore.Unlock()
		return nil
	}
	if sharedRustCore.releaseTimer != nil {
		sharedRustCore.releaseTimer.Stop()
		sharedRustCore.releaseTimer = nil
	}
	if shouldRetire {
		sharedRustCore.client = nil
		sharedRustCore.Unlock()
		return client.Disconnect()
	}
	sharedRustCore.releaseTimer = time.AfterFunc(sharedRustCoreIdleTimeout, func() {
		sharedRustCore.Lock()
		if sharedRustCore.client != client || sharedRustCore.refs > 0 {
			sharedRustCore.Unlock()
			return
		}
		sharedRustCore.client = nil
		sharedRustCore.releaseTimer = nil
		sharedRustCore.Unlock()
		_ = client.Disconnect()
	})
	sharedRustCore.Unlock()
	return nil
}

func NewRustCoreEventBus(ctx context.Context, name string, optionList ...RustCoreEventBusOptions) (*RustCoreEventBus, error) {
	options := RustCoreEventBusOptions{}
	if len(optionList) > 0 {
		options = optionList[0]
	}
	if name == "" {
		name = "EventBus"
	}
	core := options.Core
	ownsCore := false
	sharedCore := false
	var err error
	if core == nil {
		core, err = acquireSharedRustCore(ctx)
		if err != nil {
			return nil, err
		}
		ownsCore = true
		sharedCore = true
	}
	eventTimeout := resolveRustCoreTimeoutOption(options.EventTimeout, options.EventTimeoutSet, ptr(60.0))
	eventSlowTimeout := resolveRustCoreTimeoutOption(options.EventSlowTimeout, options.EventSlowTimeoutSet, ptr(300.0))
	eventHandlerSlowTimeout := resolveRustCoreTimeoutOption(options.EventHandlerSlowTimeout, options.EventHandlerSlowTimeoutSet, ptr(30.0))
	maxHistorySize := cloneIntPtr(options.MaxHistorySize)
	if len(optionList) == 0 || !options.MaxHistorySizeSet {
		maxHistorySize = ptr(DefaultMaxHistorySize)
	}
	if len(optionList) == 0 {
		eventTimeout = ptr(60.0)
		eventSlowTimeout = ptr(300.0)
		eventHandlerSlowTimeout = ptr(30.0)
	}
	eventConcurrency := options.EventConcurrency
	if eventConcurrency == "" {
		eventConcurrency = EventConcurrencyBusSerial
	}
	eventHandlerConcurrency := options.EventHandlerConcurrency
	if eventHandlerConcurrency == "" {
		eventHandlerConcurrency = EventHandlerConcurrencySerial
	}
	eventHandlerCompletion := options.EventHandlerCompletion
	if eventHandlerCompletion == "" {
		eventHandlerCompletion = EventHandlerCompletionAll
	}
	busID := options.ID
	if busID == "" {
		busID = StableCoreBusID(name)
	}
	labelSuffix := busID
	if len(labelSuffix) > 4 {
		labelSuffix = labelSuffix[len(labelSuffix)-4:]
	}
	bus := &RustCoreEventBus{
		Core:                    core,
		Name:                    name,
		BusID:                   busID,
		Label:                   fmt.Sprintf("%s#%s", name, labelSuffix),
		EventConcurrency:        eventConcurrency,
		EventHandlerConcurrency: eventHandlerConcurrency,
		EventHandlerCompletion:  eventHandlerCompletion,
		EventTimeout:            eventTimeout,
		EventSlowTimeout:        eventSlowTimeout,
		EventHandlerTimeout:     options.EventHandlerTimeout,
		EventHandlerSlowTimeout: eventHandlerSlowTimeout,
		MaxHistorySize:          maxHistorySize,
		MaxHistoryDrop:          options.MaxHistoryDrop,
		handlerEntries:          map[string]*EventHandler{},
		localEventStarts:        map[string]time.Time{},
		closed:                  make(chan struct{}),
		ownsCore:                ownsCore,
		sharedCore:              sharedCore,
	}
	if _, err := bus.Core.RegisterBus(bus.Record()); err != nil {
		if ownsCore {
			if sharedCore {
				_ = releaseSharedRustCore(core)
			} else {
				_ = core.Close()
			}
		}
		return nil, err
	}
	bus.registeredMaxHistorySize = cloneIntPtr(bus.MaxHistorySize)
	bus.registeredMaxHistoryDrop = bus.MaxHistoryDrop
	rustCoreEventBusRegistry.Lock()
	rustCoreEventBusRegistry.instances[bus] = struct{}{}
	rustCoreEventBusRegistry.Unlock()
	return bus, nil
}

func rustCoreEventBusInstancesSnapshot() []*RustCoreEventBus {
	rustCoreEventBusRegistry.Lock()
	defer rustCoreEventBusRegistry.Unlock()
	instances := make([]*RustCoreEventBus, 0, len(rustCoreEventBusRegistry.instances))
	for bus := range rustCoreEventBusRegistry.instances {
		select {
		case <-bus.closed:
			continue
		default:
			instances = append(instances, bus)
		}
	}
	return instances
}

func (b *RustCoreEventBus) processAvailableAfterHandlerOutcome() bool {
	return true
}

func (b *RustCoreEventBus) localEventStart(eventID string) time.Time {
	now := time.Now()
	if eventID == "" {
		return now
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if startedAt, ok := b.localEventStarts[eventID]; ok {
		return startedAt
	}
	b.localEventStarts[eventID] = now
	return now
}

func resolveRustCoreTimeoutOption(value *float64, isSet bool, defaultValue *float64) *float64 {
	if isSet || value != nil {
		return value
	}
	return defaultValue
}

func cloneIntPtr(value *int) *int {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func intPtrEqual(a *int, b *int) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func StableCoreBusID(busName string) string {
	return uuid.NewSHA1(uuid.NameSpaceDNS, []byte("abxbus-core-bus:"+busName)).String()
}

func (b *RustCoreEventBus) DefaultsRecord() map[string]any {
	return map[string]any{
		"event_concurrency":          b.EventConcurrency,
		"event_handler_concurrency":  b.EventHandlerConcurrency,
		"event_handler_completion":   b.EventHandlerCompletion,
		"event_timeout":              b.EventTimeout,
		"event_slow_timeout":         b.EventSlowTimeout,
		"event_handler_timeout":      b.EventHandlerTimeout,
		"event_handler_slow_timeout": b.EventHandlerSlowTimeout,
	}
}

func (b *RustCoreEventBus) Record() map[string]any {
	return map[string]any{
		"bus_id":           b.BusID,
		"name":             b.Name,
		"label":            b.Label,
		"host_id":          b.Core.sessionID,
		"defaults":         b.DefaultsRecord(),
		"max_history_size": b.MaxHistorySize,
		"max_history_drop": b.MaxHistoryDrop,
	}
}

func (b *RustCoreEventBus) SyncHistoryPolicy(maxHistorySize *int, maxHistoryDrop bool) error {
	if maxHistorySize != nil && *maxHistorySize < 0 {
		maxHistorySize = nil
	}
	b.mu.Lock()
	if intPtrEqual(b.registeredMaxHistorySize, maxHistorySize) && b.registeredMaxHistoryDrop == maxHistoryDrop {
		b.MaxHistorySize = cloneIntPtr(maxHistorySize)
		b.MaxHistoryDrop = maxHistoryDrop
		b.mu.Unlock()
		return nil
	}
	b.MaxHistorySize = cloneIntPtr(maxHistorySize)
	b.MaxHistoryDrop = maxHistoryDrop
	record := b.Record()
	b.mu.Unlock()
	if _, err := b.Core.RegisterBus(record); err != nil {
		return err
	}
	b.mu.Lock()
	b.registeredMaxHistorySize = cloneIntPtr(maxHistorySize)
	b.registeredMaxHistoryDrop = maxHistoryDrop
	b.mu.Unlock()
	return nil
}

func (b *RustCoreEventBus) On(eventType string, handlerName string, handler any, options *EventHandler) *EventHandler {
	if handlerName == "" {
		handlerName = "handler"
	}
	normalizedHandler, err := normalizeEventHandlerCallable(handler)
	if err != nil {
		panic(err)
	}
	handlerEntry := NewEventHandler(b.Name, b.BusID, eventType, handlerName, normalizedHandler)
	if options != nil {
		if options.ID != "" {
			handlerEntry.ID = options.ID
		}
		if options.HandlerName != "" {
			handlerEntry.HandlerName = options.HandlerName
			handlerName = options.HandlerName
		}
		if options.HandlerRegisteredAt != "" {
			handlerEntry.HandlerRegisteredAt = options.HandlerRegisteredAt
		}
		if options.HandlerFilePath != nil {
			handlerEntry.HandlerFilePath = options.HandlerFilePath
		}
		handlerEntry.HandlerTimeout = options.HandlerTimeout
		handlerEntry.HandlerSlowTimeout = options.HandlerSlowTimeout
	}
	handlerID := handlerEntry.ID
	b.mu.Lock()
	b.handlerEntries[handlerID] = handlerEntry
	b.mu.Unlock()
	_, err = b.Core.RegisterHandler(map[string]any{
		"handler_id":            handlerID,
		"bus_id":                b.BusID,
		"host_id":               b.Core.sessionID,
		"event_pattern":         eventType,
		"handler_name":          handlerName,
		"handler_file_path":     handlerEntry.HandlerFilePath,
		"handler_registered_at": handlerEntry.HandlerRegisteredAt,
		"handler_timeout":       handlerEntry.HandlerTimeout,
		"handler_slow_timeout":  handlerEntry.HandlerSlowTimeout,
		"handler_concurrency":   nil,
		"handler_completion":    nil,
	})
	if err != nil {
		panic(err)
	}
	return handlerEntry
}

func (b *RustCoreEventBus) OnTyped(sample any, handlerName string, handler EventHandlerCallable, options *EventHandler) *EventHandler {
	event, err := baseEventFromAny(sample)
	if err != nil {
		panic(err)
	}
	return b.On(event.EventType, handlerName, handler, options)
}

func (b *RustCoreEventBus) Emit(input any) (*BaseEvent, error) {
	event, err := baseEventFromAny(input)
	if err != nil {
		return nil, err
	}
	if !slices.Contains(event.EventPath, b.Label) {
		event.EventPath = append(event.EventPath, b.Label)
	}
	event.EventStatus = "pending"
	eventRecord, err := rustCoreEventRecord(event)
	if err != nil {
		return nil, err
	}
	if _, err := b.Core.EmitEvent(eventRecord, b.BusID); err != nil {
		return nil, err
	}
	return b.runUntilEventCompleted(event)
}

func coreJSONCompatibleValue(value any) (any, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var decoded any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func rustCoreEventRecord(event *BaseEvent) (map[string]any, error) {
	if event == nil {
		return nil, fmt.Errorf("event is nil")
	}
	event.mu.Lock()
	eventResultType := event.eventResultTypeJSONValue()
	record := map[string]any{
		"event_type":                     event.EventType,
		"event_version":                  event.EventVersion,
		"event_timeout":                  event.EventTimeout,
		"event_slow_timeout":             event.EventSlowTimeout,
		"event_concurrency":              eventConcurrencyRecordValue(event.EventConcurrency),
		"event_handler_timeout":          event.EventHandlerTimeout,
		"event_handler_slow_timeout":     event.EventHandlerSlowTimeout,
		"event_handler_concurrency":      eventHandlerConcurrencyRecordValue(event.EventHandlerConcurrency),
		"event_handler_completion":       eventHandlerCompletionRecordValue(event.EventHandlerCompletion),
		"event_blocks_parent_completion": event.EventBlocksParentCompletion,
		"event_result_type":              eventResultType,
		"event_id":                       event.EventID,
		"event_path":                     append([]string{}, event.EventPath...),
		"event_parent_id":                event.EventParentID,
		"event_emitted_by_handler_id":    event.EventEmittedByHandlerID,
		"event_emitted_by_result_id":     event.EventEmittedByResultID,
		"event_created_at":               event.EventCreatedAt,
		"event_status":                   event.EventStatus,
		"event_started_at":               event.EventStartedAt,
		"event_completed_at":             event.EventCompletedAt,
	}
	payload := make(map[string]any, len(event.Payload))
	for key, value := range event.Payload {
		payload[key] = value
	}
	event.mu.Unlock()
	normalizedEventResultType, err := coreJSONCompatibleValue(eventResultType)
	if err != nil {
		return nil, err
	}
	record["event_result_type"] = normalizedEventResultType
	for key, value := range payload {
		normalizedValue, err := coreJSONCompatibleValue(value)
		if err != nil {
			return nil, err
		}
		record[key] = normalizedValue
	}
	delete(record, "event_pending_bus_count")
	delete(record, "event_results")
	return record, nil
}

func rustCoreEventControlOptions(event *BaseEvent) map[string]any {
	if event == nil {
		return nil
	}
	event.mu.Lock()
	defer event.mu.Unlock()
	options := map[string]any{}
	if event.EventTimeout != nil {
		options["event_timeout"] = *event.EventTimeout
	}
	if event.EventSlowTimeout != nil {
		options["event_slow_timeout"] = *event.EventSlowTimeout
	}
	if value := eventConcurrencyRecordValue(event.EventConcurrency); value != nil {
		options["event_concurrency"] = value
	}
	if event.EventHandlerTimeout != nil {
		options["event_handler_timeout"] = *event.EventHandlerTimeout
	}
	if event.EventHandlerSlowTimeout != nil {
		options["event_handler_slow_timeout"] = *event.EventHandlerSlowTimeout
	}
	if value := eventHandlerConcurrencyRecordValue(event.EventHandlerConcurrency); value != nil {
		options["event_handler_concurrency"] = value
	}
	if value := eventHandlerCompletionRecordValue(event.EventHandlerCompletion); value != nil {
		options["event_handler_completion"] = value
	}
	if event.EventBlocksParentCompletion {
		options["event_blocks_parent_completion"] = true
	}
	return options
}

func eventConcurrencyRecordValue(value EventConcurrencyMode) any {
	if value == "" {
		return nil
	}
	return value
}

func eventHandlerConcurrencyRecordValue(value EventHandlerConcurrencyMode) any {
	if value == "" {
		return nil
	}
	return value
}

func eventHandlerCompletionRecordValue(value EventHandlerCompletionMode) any {
	if value == "" {
		return nil
	}
	return value
}

func (b *RustCoreEventBus) RunUntilEventCompleted(event *BaseEvent) (*BaseEvent, error) {
	if event == nil {
		return nil, fmt.Errorf("event is nil")
	}
	return b.runUntilEventCompleted(event)
}

func (b *RustCoreEventBus) Find(eventPattern string, where func(event *BaseEvent) bool, options *FindOptions) (*BaseEvent, error) {
	limit := 1
	results, err := b.Filter(eventPattern, where, &FilterOptions{
		Past:    optionPast(options),
		Future:  optionFuture(options),
		ChildOf: optionChildOf(options),
		Equals:  optionEquals(options),
		Limit:   &limit,
	})
	if err != nil || len(results) == 0 {
		return nil, err
	}
	return results[0], nil
}

func (b *RustCoreEventBus) Filter(eventPattern string, where func(event *BaseEvent) bool, options *FilterOptions) ([]*BaseEvent, error) {
	if options == nil {
		options = &FilterOptions{}
	}
	if options.Limit != nil && *options.Limit <= 0 {
		return []*BaseEvent{}, nil
	}
	if eventPattern == "" {
		eventPattern = "*"
	}
	if where == nil {
		where = func(event *BaseEvent) bool { return true }
	}
	pastEnabled, pastWindow := normalizePast(options.Past)
	if !pastEnabled {
		return []*BaseEvent{}, nil
	}

	records, err := b.Core.ListBusEvents(b.BusID, eventPattern, nil)
	if err != nil {
		return nil, err
	}
	events := make([]*BaseEvent, 0, len(records))
	for _, record := range records {
		event, err := baseEventFromCoreRecord(record)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	results := []*BaseEvent{}
	for _, event := range events {
		if !b.eventBelongsToBus(event) {
			continue
		}
		if options.ChildOf != nil && !b.coreEventIsChildOf(event, options.ChildOf) {
			continue
		}
		if !eventMatchesEquals(event, options.Equals) {
			continue
		}
		if pastWindow != nil {
			createdAt, err := time.Parse(time.RFC3339Nano, event.EventCreatedAt)
			if err != nil || time.Since(createdAt) > time.Duration(*pastWindow*float64(time.Second)) {
				continue
			}
		}
		if !where(event) {
			continue
		}
		results = append(results, event)
		if options.Limit != nil && len(results) >= *options.Limit {
			break
		}
	}
	return results, nil
}

func (b *RustCoreEventBus) eventBelongsToBus(event *BaseEvent) bool {
	if event == nil {
		return false
	}
	if slices.Contains(event.EventPath, b.Label) {
		return true
	}
	for _, result := range event.EventResults {
		if result != nil && (result.EventBusID == b.BusID || result.EventBusName == b.Name) {
			return true
		}
	}
	return false
}

func (b *RustCoreEventBus) runUntilEventCompleted(event *BaseEvent) (*BaseEvent, error) {
	return b.runUntilEventCompletedWithInitial(event, nil)
}

func (b *RustCoreEventBus) runUntilEventCompletedWithInitial(event *BaseEvent, initial []CoreProtocolEnvelope) (*BaseEvent, error) {
	return b.runUntilEventCompletedWithInitialAndBatch(event, initial, nil)
}

func (b *RustCoreEventBus) runUntilEventCompletedWithInitialAndBatch(event *BaseEvent, initial []CoreProtocolEnvelope, batch *coreOutcomeBatch) (*BaseEvent, error) {
	return b.runUntilEventCompletedWithContextInitialAndBatchAndTargetMode(context.Background(), event, initial, batch, true)
}

func (b *RustCoreEventBus) runUntilEventCompletedWithContextInitialAndBatchAndTargetMode(ctx context.Context, event *BaseEvent, initial []CoreProtocolEnvelope, batch *coreOutcomeBatch, targetOnly bool) (*BaseEvent, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	targetEventID := ""
	targetRouteID := ""
	if targetOnly {
		targetEventID = event.EventID
		if event.Bus != nil {
			targetRouteID = event.Bus.coreRouteIDForEvent(event.EventID)
		}
	}
	rememberTargetRoute := func(messages []map[string]any) {
		if targetRouteID != "" || targetEventID == "" {
			return
		}
		for _, message := range messages {
			routeID, eventID := coreRouteIDFromMessage(message, b.BusID)
			if routeID != "" && eventID == targetEventID {
				targetRouteID = routeID
				if event.Bus != nil {
					event.Bus.mu.Lock()
					event.Bus.coreRouteIDs[targetEventID] = routeID
					event.Bus.mu.Unlock()
				}
				return
			}
		}
	}
	if len(initial) == 0 {
		if event.Bus != nil && event.Bus.core != nil {
			initial = append(initial, event.Bus.takeCoreInitial(event.EventID)...)
		}
	}
	if len(initial) > 0 {
		produced, err := b.producedMessagesFromResponsesWithBatchForTarget(ctx, initial, batch, targetEventID)
		if err != nil {
			return nil, err
		}
		rememberTargetRoute(produced)
		completed := false
		for _, message := range produced {
			applyCoreMessageToLocalEventBuses(message)
			b.applyCoreMessage(event, message)
			if isEventCompletedMessageFor(message, event.EventID) {
				completed = true
			}
		}
		if completed {
			return b.completedSnapshotOrEvent(event)
		}
	}
	for i := 0; i < 1000; i++ {
		progressed := false
		for _, bus := range b.coreDriveBusesForEvent(event) {
			processedInitial := false
			var initial []CoreProtocolEnvelope
			if event.Bus != nil && event.Bus.core == bus {
				initial = append(initial, event.Bus.takeCoreInitial(event.EventID)...)
			}
			if len(event.EventPath) > 1 {
				initial = append(initial, bus.takeCoreInitialForEvent(event.EventID, event.Bus)...)
			}
			if len(initial) > 0 {
				produced, err := bus.producedMessagesFromResponsesWithBatchForTarget(ctx, initial, batch, targetEventID)
				if err != nil {
					return nil, err
				}
				rememberTargetRoute(produced)
				if coreMessagesHaveActionableWork(produced) {
					progressed = true
				}
				processedInitial = true
				completed := false
				for _, message := range produced {
					applyCoreMessageToLocalEventBuses(message)
					b.applyCoreMessage(event, message)
					if isEventCompletedMessageFor(message, event.EventID) {
						completed = true
					}
				}
				if completed {
					return b.completedSnapshotOrEvent(event)
				}
			}
			if !processedInitial && !bus.canHostEvent(event) {
				continue
			}
			var responses []CoreProtocolEnvelope
			var err error
			if targetRouteID != "" && bus.BusID == b.BusID {
				responses, err = bus.Core.ProcessRouteCompact(targetRouteID, coreRouteSliceLimit)
			} else {
				responses, err = bus.Core.ProcessNextRouteLimited(bus.BusID, coreRouteSliceLimit)
			}
			if err != nil {
				return nil, err
			}
			produced, err := bus.producedMessagesFromResponsesWithBatchForTarget(ctx, responses, batch, targetEventID)
			if err != nil {
				return nil, err
			}
			rememberTargetRoute(produced)
			if coreMessagesHaveActionableWork(produced) {
				progressed = true
			}
			completed := false
			for _, message := range produced {
				applyCoreMessageToLocalEventBuses(message)
				b.applyCoreMessage(event, message)
				if isEventCompletedMessageFor(message, event.EventID) {
					completed = true
				}
			}
			if completed {
				return b.completedSnapshotOrEvent(event)
			}
		}
		if event.status() == "completed" {
			return b.completedSnapshotOrEvent(event)
		}
		if !progressed {
			record, err := b.Core.GetEvent(event.EventID)
			if err != nil {
				return nil, err
			}
			if record != nil {
				if status, _ := record["event_status"].(string); status != "completed" {
					runtime.Gosched()
					continue
				}
				snapshot, err := baseEventFromCoreRecord(record)
				if err != nil {
					return nil, err
				}
				b.applyEffectiveResultTimeouts(snapshot)
				applyCoreSnapshotToLocalEventBuses(snapshot)
				return snapshot, nil
			}
			runtime.Gosched()
		}
	}
	record, _ := b.Core.GetEvent(event.EventID)
	if record != nil {
		if status, _ := record["event_status"].(string); status == "completed" {
			snapshot, err := baseEventFromCoreRecord(record)
			if err != nil {
				return nil, err
			}
			b.applyEffectiveResultTimeouts(snapshot)
			applyCoreSnapshotToLocalEventBuses(snapshot)
			return snapshot, nil
		}
	}
	return nil, fmt.Errorf("event did not complete within iteration limit: %s snapshot=%s", event.EventID, compactJSONForError(record))
}

func isEventCompletedPatchFor(patch map[string]any, eventID string) bool {
	patchType, _ := patch["type"].(string)
	return (patchType == "event_completed" || patchType == "event_completed_compact") && patch["event_id"] == eventID
}

func isEventCompletedMessageFor(message map[string]any, eventID string) bool {
	if message["type"] == "event_completed" {
		return message["event_id"] == eventID
	}
	if message["type"] != "patch" {
		return false
	}
	patch, _ := message["patch"].(map[string]any)
	return isEventCompletedPatchFor(patch, eventID)
}

func coreMessagesHaveActionableWork(messages []map[string]any) bool {
	for _, message := range messages {
		messageType, _ := message["type"].(string)
		if messageType != "" && messageType != "heartbeat_ack" {
			return true
		}
	}
	return false
}

func coreRouteIDFromMessage(message map[string]any, busID string) (string, string) {
	if message == nil {
		return "", ""
	}
	if message["type"] == "invoke_handler" {
		eventID, _ := message["event_id"].(string)
		routeID, _ := message["route_id"].(string)
		messageBusID, _ := message["bus_id"].(string)
		if routeID != "" && eventID != "" && (busID == "" || messageBusID == "" || messageBusID == busID) {
			return routeID, eventID
		}
	}
	if message["type"] == "invoke_handlers_compact" {
		for _, invocation := range invocationMessagesFromCoreMessage(message) {
			if routeID, eventID := coreRouteIDFromMessage(invocation, busID); routeID != "" && eventID != "" {
				return routeID, eventID
			}
		}
	}
	if message["type"] != "patch" {
		return "", ""
	}
	patch, _ := message["patch"].(map[string]any)
	if patch == nil || patch["type"] != "event_emitted" {
		return "", ""
	}
	route, _ := patch["route"].(map[string]any)
	if route == nil {
		return "", ""
	}
	routeID, _ := route["route_id"].(string)
	eventID, _ := route["event_id"].(string)
	routeBusID, _ := route["bus_id"].(string)
	if routeID == "" || eventID == "" || (busID != "" && routeBusID != "" && routeBusID != busID) {
		return "", ""
	}
	return routeID, eventID
}

func (b *RustCoreEventBus) processAvailableRoutesOnce() (bool, error) {
	progressed := false
	for _, bus := range rustCoreEventBusInstancesSnapshot() {
		if bus == nil {
			continue
		}
		responses, err := bus.Core.ProcessNextRouteLimited(bus.BusID, coreRouteSliceLimit)
		if err != nil {
			return false, err
		}
		produced, err := bus.producedMessagesFromResponsesWithBatch(responses, nil)
		if err != nil {
			return false, err
		}
		if coreMessagesHaveActionableWork(produced) {
			progressed = true
		}
		for _, message := range produced {
			applyCoreMessageToLocalEventBuses(message)
		}
	}
	return progressed, nil
}

func applyCoreMessageToLocalEventBuses(message map[string]any) {
	if message["type"] == "event_completed" {
		eventID, _ := message["event_id"].(string)
		if eventID == "" {
			return
		}
		for _, bus := range eventBusInstancesForEvent(eventID) {
			event := bus.EventHistory.GetEvent(eventID)
			if event == nil || bus.core == nil {
				continue
			}
			bus.core.applyCoreMessage(event, message)
			if event.EventStatus == "completed" {
				bus.finalizeCoreEvent(event)
			}
		}
		return
	}
	if message["type"] != "patch" {
		return
	}
	patch, _ := message["patch"].(map[string]any)
	if patch == nil {
		return
	}
	if localOnly, _ := patch["local_only"].(bool); localOnly {
		return
	}
	eventID, _ := patch["event_id"].(string)
	if eventID == "" {
		result, _ := patch["result"].(map[string]any)
		eventID, _ = result["event_id"].(string)
	}
	if eventID == "" {
		return
	}
	for _, bus := range eventBusInstancesForEvent(eventID) {
		event := bus.EventHistory.GetEvent(eventID)
		if event == nil || bus.core == nil {
			continue
		}
		bus.core.applyCoreMessage(event, message)
		if event.EventStatus == "completed" {
			bus.finalizeCoreEvent(event)
		}
	}
}

func applyCoreSnapshotToLocalEventBuses(snapshot *BaseEvent) {
	if snapshot == nil {
		return
	}
	for _, bus := range eventBusInstancesForEvent(snapshot.EventID) {
		event := bus.EventHistory.GetEvent(snapshot.EventID)
		if event == nil || bus.core == nil {
			continue
		}
		copyBaseEventRuntime(event, snapshot, bus)
		if event.EventStatus == "completed" {
			bus.finalizeCoreEvent(event)
		}
	}
}

func compactJSONForError(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	if len(data) > 6000 {
		return string(data[:6000]) + "..."
	}
	return string(data)
}

func (b *RustCoreEventBus) canHostEvent(event *BaseEvent) bool {
	if event == nil {
		return false
	}
	if b == nil {
		return false
	}
	return slices.Contains(event.EventPath, b.Label)
}

func (b *RustCoreEventBus) coreDriveBusesForEvent(event *BaseEvent) []*RustCoreEventBus {
	if b == nil {
		return nil
	}
	if event == nil || len(event.EventPath) <= 1 {
		return []*RustCoreEventBus{b}
	}
	labels := map[string]bool{}
	for _, label := range event.EventPath {
		labels[label] = true
	}
	out := []*RustCoreEventBus{b}
	seen := map[*RustCoreEventBus]bool{b: true}
	for _, bus := range rustCoreEventBusInstancesSnapshot() {
		if bus == nil || seen[bus] {
			continue
		}
		if labels[bus.Label] {
			out = append(out, bus)
			seen[bus] = true
		}
	}
	return out
}

func (b *RustCoreEventBus) takeCoreInitialForEvent(eventID string, alreadyDrained *EventBus) []CoreProtocolEnvelope {
	if b == nil || eventID == "" {
		return nil
	}
	var initial []CoreProtocolEnvelope
	for _, wrapper := range eventBusInstancesForEvent(eventID) {
		if wrapper == nil || wrapper == alreadyDrained || wrapper.core != b {
			continue
		}
		initial = append(initial, wrapper.takeCoreInitial(eventID)...)
	}
	return initial
}

func (b *RustCoreEventBus) completedSnapshotOrEvent(event *BaseEvent) (*BaseEvent, error) {
	if event.status() == "completed" {
		completeLocalNonterminalResults(event)
		b.applyEffectiveResultTimeouts(event)
		return event, nil
	}
	record, err := b.Core.GetEvent(event.EventID)
	if err != nil || record == nil {
		return event, err
	}
	snapshot, err := baseEventFromCoreRecord(record)
	if err != nil {
		return event, nil
	}
	b.applyEffectiveResultTimeouts(snapshot)
	return snapshot, nil
}

func completeLocalNonterminalResults(event *BaseEvent) {
	if event == nil {
		return
	}
	event.mu.Lock()
	completionMode := event.EventHandlerCompletion
	results := make([]*EventResult, 0, len(event.EventResults))
	for _, result := range event.EventResults {
		results = append(results, result)
	}
	event.mu.Unlock()
	for _, result := range results {
		if result == nil {
			continue
		}
		status, _, _, _ := result.snapshot()
		if status != EventResultPending && status != EventResultStarted {
			continue
		}
		if completionMode == EventHandlerCompletionFirst {
			result.Update(&EventResultUpdateOptions{
				Error:    "Cancelled pending handler after first result",
				ErrorSet: true,
			})
			continue
		}
		result.Update(&EventResultUpdateOptions{
			ResultSet: true,
		})
	}
}

func (b *RustCoreEventBus) applyEffectiveResultTimeouts(event *BaseEvent) {
	if b == nil || event == nil {
		return
	}
	for _, result := range event.EventResults {
		if result == nil {
			continue
		}
		ownerBus, handler := coreHandlerEntryForResult(b, result.HandlerID)
		result.Timeout = effectiveResultTimeout(ownerBus, event, handler, result.Timeout, nil)
	}
}

func coreHandlerEntryForResult(preferred *RustCoreEventBus, handlerID string) (*RustCoreEventBus, *EventHandler) {
	if handlerID == "" {
		return preferred, nil
	}
	if preferred != nil {
		preferred.mu.RLock()
		handler := preferred.handlerEntries[handlerID]
		preferred.mu.RUnlock()
		if handler != nil {
			return preferred, handler
		}
	}
	for _, bus := range rustCoreEventBusInstancesSnapshot() {
		if bus == nil || bus == preferred {
			continue
		}
		bus.mu.RLock()
		handler := bus.handlerEntries[handlerID]
		bus.mu.RUnlock()
		if handler != nil {
			return bus, handler
		}
	}
	return preferred, nil
}

func (b *RustCoreEventBus) coreEventIsChildOf(event *BaseEvent, ancestor *BaseEvent) bool {
	if event == nil || ancestor == nil {
		return false
	}
	parentID := event.EventParentID
	visited := map[string]bool{}
	for parentID != nil {
		if *parentID == ancestor.EventID {
			return true
		}
		if visited[*parentID] {
			return false
		}
		visited[*parentID] = true
		record, err := b.Core.GetEvent(*parentID)
		if err != nil || record == nil {
			return false
		}
		parent, err := baseEventFromCoreRecord(record)
		if err != nil {
			return false
		}
		parentID = parent.EventParentID
	}
	return false
}

func optionPast(options *FindOptions) any {
	if options == nil {
		return nil
	}
	return options.Past
}

func optionFuture(options *FindOptions) any {
	if options == nil {
		return nil
	}
	return options.Future
}

func optionChildOf(options *FindOptions) *BaseEvent {
	if options == nil {
		return nil
	}
	return options.ChildOf
}

func optionEquals(options *FindOptions) map[string]any {
	if options == nil {
		return nil
	}
	return options.Equals
}

var coreEventKnownFields = map[string]struct{}{
	"event_id": {}, "event_created_at": {}, "event_type": {}, "event_version": {},
	"event_timeout": {}, "event_slow_timeout": {}, "event_handler_timeout": {}, "event_handler_slow_timeout": {},
	"event_parent_id": {}, "event_path": {}, "event_result_type": {}, "event_emitted_by_handler_id": {},
	"event_emitted_by_result_id": {}, "event_pending_bus_count": {}, "event_status": {}, "event_started_at": {},
	"event_completed_at": {}, "event_concurrency": {}, "event_handler_concurrency": {}, "event_handler_completion": {},
	"event_blocks_parent_completion": {}, "event_results": {},
}

func baseEventFromCoreRecord(record map[string]any) (*BaseEvent, error) {
	event := &BaseEvent{
		EventID:                     coreString(record["event_id"]),
		EventCreatedAt:              coreString(record["event_created_at"]),
		EventType:                   coreString(record["event_type"]),
		EventVersion:                coreString(record["event_version"]),
		EventTimeout:                coreFloatPtr(record["event_timeout"]),
		EventSlowTimeout:            coreFloatPtr(record["event_slow_timeout"]),
		EventHandlerTimeout:         coreFloatPtr(record["event_handler_timeout"]),
		EventHandlerSlowTimeout:     coreFloatPtr(record["event_handler_slow_timeout"]),
		EventParentID:               coreStringPtr(record["event_parent_id"]),
		EventPath:                   coreStringSlice(record["event_path"]),
		EventResultType:             nil,
		EventEmittedByHandlerID:     coreStringPtr(record["event_emitted_by_handler_id"]),
		EventEmittedByResultID:      coreStringPtr(record["event_emitted_by_result_id"]),
		EventPendingBusCount:        coreInt(record["event_pending_bus_count"]),
		EventStatus:                 coreString(record["event_status"]),
		EventStartedAt:              coreStringPtr(record["event_started_at"]),
		EventCompletedAt:            coreStringPtr(record["event_completed_at"]),
		EventConcurrency:            EventConcurrencyMode(coreString(record["event_concurrency"])),
		EventHandlerConcurrency:     EventHandlerConcurrencyMode(coreString(record["event_handler_concurrency"])),
		EventHandlerCompletion:      EventHandlerCompletionMode(coreString(record["event_handler_completion"])),
		EventBlocksParentCompletion: coreBool(record["event_blocks_parent_completion"]),
		Payload:                     map[string]any{},
		EventResults:                map[string]*EventResult{},
		eventResultOrder:            []string{},
		done_ch:                     make(chan struct{}),
		coreKnown:                   true,
	}
	if event.EventID == "" {
		return nil, fmt.Errorf("event_id required")
	}
	if resultType, ok := record["event_result_type"]; ok && resultType != nil {
		event.EventResultType = normalizeEventResultTypeSchema(normalizeCoreValue(resultType))
	}
	for key, value := range record {
		if _, known := coreEventKnownFields[key]; !known {
			event.Payload[key] = normalizeCoreValue(value)
		}
	}
	if rawResults, ok := record["event_results"].(map[string]any); ok && len(rawResults) > 0 {
		keys := make([]string, 0, len(rawResults))
		for key := range rawResults {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			resultRecord, _ := rawResults[key].(map[string]any)
			if resultRecord == nil {
				continue
			}
			result := eventResultFromCoreRecord(resultRecord)
			if result.HandlerID == "" {
				result.HandlerID = key
			}
			event.EventResults[result.HandlerID] = result
			event.noteEventResultOrder(result.HandlerID)
		}
	}
	if !eventResultRegisteredTimesAllEqual(event.EventResults) {
		event.rebuildEventResultOrderByMetadata()
	}
	if event.EventStatus == "completed" {
		event.done_once.Do(func() { close(event.done_ch) })
	}
	return event, nil
}

func eventResultFromCoreRecord(record map[string]any) *EventResult {
	resultID := coreString(record["result_id"])
	if resultID == "" {
		resultID = coreString(record["id"])
	}
	result := &EventResult{
		ID:                  resultID,
		Status:              EventResultStatus(coreString(record["status"])),
		EventID:             coreString(record["event_id"]),
		HandlerID:           coreString(record["handler_id"]),
		HandlerName:         coreString(record["handler_name"]),
		HandlerFilePath:     coreStringPtr(record["handler_file_path"]),
		HandlerTimeout:      coreFloatPtr(record["handler_timeout"]),
		HandlerSlowTimeout:  coreFloatPtr(record["handler_slow_timeout"]),
		Timeout:             coreFloatPtr(record["timeout"]),
		HandlerRegisteredAt: coreString(record["handler_registered_at"]),
		HandlerEventPattern: coreString(record["handler_event_pattern"]),
		EventBusName:        coreString(record["eventbus_name"]),
		EventBusID:          coreString(record["eventbus_id"]),
		StartedAt:           coreStringPtr(record["started_at"]),
		CompletedAt:         coreStringPtr(record["completed_at"]),
		Result:              normalizeCoreValue(record["result"]),
		Error:               normalizeCoreValue(record["error"]),
		EventChildren:       nil,
		EventChildIDs:       coreStringSlice(record["event_children"]),
		done_ch:             make(chan struct{}),
	}
	if result.Status == EventResultCompleted || result.Status == EventResultError {
		result.once.Do(func() { close(result.done_ch) })
	}
	return result
}

func coreString(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}

func coreStringPtr(value any) *string {
	text, ok := value.(string)
	if !ok {
		return nil
	}
	return &text
}

func coreFloatPtr(value any) *float64 {
	switch number := value.(type) {
	case float64:
		return &number
	case float32:
		out := float64(number)
		return &out
	case int:
		out := float64(number)
		return &out
	case int8:
		out := float64(number)
		return &out
	case int16:
		out := float64(number)
		return &out
	case int32:
		out := float64(number)
		return &out
	case int64:
		out := float64(number)
		return &out
	case uint:
		out := float64(number)
		return &out
	case uint8:
		out := float64(number)
		return &out
	case uint16:
		out := float64(number)
		return &out
	case uint32:
		out := float64(number)
		return &out
	case uint64:
		out := float64(number)
		return &out
	default:
		return nil
	}
}

func coreInt(value any) int {
	switch number := value.(type) {
	case int:
		return number
	case int8:
		return int(number)
	case int16:
		return int(number)
	case int32:
		return int(number)
	case int64:
		return int(number)
	case uint:
		return int(number)
	case uint8:
		return int(number)
	case uint16:
		return int(number)
	case uint32:
		return int(number)
	case uint64:
		return int(number)
	case float64:
		return int(number)
	case float32:
		return int(number)
	default:
		return 0
	}
}

func coreBool(value any) bool {
	flag, _ := value.(bool)
	return flag
}

func coreStringSlice(value any) []string {
	switch items := value.(type) {
	case []string:
		return append([]string{}, items...)
	case []any:
		out := make([]string, 0, len(items))
		for _, item := range items {
			if text, ok := item.(string); ok {
				out = append(out, text)
			}
		}
		return out
	default:
		return nil
	}
}

func normalizeCoreValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = normalizeCoreValue(item)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = normalizeCoreValue(item)
		}
		return out
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int8:
		return float64(typed)
	case int16:
		return float64(typed)
	case int32:
		return float64(typed)
	case int64:
		return float64(typed)
	case uint:
		return float64(typed)
	case uint8:
		return float64(typed)
	case uint16:
		return float64(typed)
	case uint32:
		return float64(typed)
	case uint64:
		return float64(typed)
	default:
		return value
	}
}

func restoreCoreJSONNumbers(event *BaseEvent) {
	if event == nil {
		return
	}
	event.mu.Lock()
	if payload, ok := restoreJSONNumbers(event.Payload).(map[string]any); ok {
		event.Payload = payload
	}
	results := make([]*EventResult, 0, len(event.EventResults))
	for _, result := range event.EventResults {
		results = append(results, result)
	}
	event.mu.Unlock()
	for _, result := range results {
		if result == nil {
			continue
		}
		result.mu.Lock()
		result.Result = restoreJSONNumbers(result.Result)
		result.Error = restoreJSONNumbers(result.Error)
		result.mu.Unlock()
	}
}

func applyInvocationEventSnapshot(event *BaseEvent, invocation map[string]any) {
	if event == nil || invocation == nil {
		return
	}
	eventSnapshot, _ := invocation["event_snapshot"].(map[string]any)
	if eventSnapshot == nil {
		return
	}
	status := coreString(eventSnapshot["event_status"])
	startedAt := coreStringPtr(eventSnapshot["event_started_at"])
	event.mu.Lock()
	if status != "" {
		event.EventStatus = status
	}
	if startedAt != nil {
		event.EventStartedAt = startedAt
	} else if (status == "started" || status == "pending" || status == "") && event.EventStartedAt == nil {
		now := monotonicDatetime()
		event.EventStartedAt = &now
	}
	if event.EventStatus == "" || event.EventStatus == "pending" {
		event.EventStatus = "started"
	}
	event.mu.Unlock()
}

func restoreJSONNumbers(value any) any {
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = restoreJSONNumbers(item)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = restoreJSONNumbers(item)
		}
		return out
	case float64:
		if v == float64(int(v)) {
			return int(v)
		}
		return v
	default:
		return value
	}
}

func coreEventIsChildOf(eventsByID map[string]*BaseEvent, event *BaseEvent, ancestor *BaseEvent) bool {
	if event == nil || ancestor == nil {
		return false
	}
	parentID := event.EventParentID
	visited := map[string]bool{}
	for parentID != nil {
		if *parentID == ancestor.EventID {
			return true
		}
		if visited[*parentID] {
			return false
		}
		visited[*parentID] = true
		parent := eventsByID[*parentID]
		if parent == nil {
			return false
		}
		parentID = parent.EventParentID
	}
	return false
}

func (b *RustCoreEventBus) Close() error {
	return b.Disconnect()
}

func (b *RustCoreEventBus) ForgetHandler(handlerID string) {
	if b == nil || handlerID == "" {
		return
	}
	b.mu.Lock()
	delete(b.handlerEntries, handlerID)
	b.mu.Unlock()
}

func (b *RustCoreEventBus) Disconnect() error {
	select {
	case <-b.closed:
	default:
		close(b.closed)
	}
	rustCoreEventBusRegistry.Lock()
	delete(rustCoreEventBusRegistry.instances, b)
	rustCoreEventBusRegistry.Unlock()
	b.mu.Lock()
	handlerCount := len(b.handlerEntries)
	b.handlerEntries = map[string]*EventHandler{}
	b.mu.Unlock()
	if b.Core == nil {
		return nil
	}
	_, _ = b.Core.UnregisterBus(b.BusID)
	if b.sharedCore {
		return releaseSharedRustCore(b.Core, handlerCount >= sharedRustCoreRetireHandlerThreshold)
	}
	if b.ownsCore {
		return b.Core.Disconnect()
	}
	return nil
}

func (b *RustCoreEventBus) Stop(ctx context.Context) error {
	if b.sharedCore || !b.ownsCore {
		return b.Disconnect()
	}
	select {
	case <-b.closed:
	default:
		close(b.closed)
	}
	rustCoreEventBusRegistry.Lock()
	delete(rustCoreEventBusRegistry.instances, b)
	rustCoreEventBusRegistry.Unlock()
	if b.Core == nil {
		return nil
	}
	_, _ = b.Core.UnregisterBus(b.BusID)
	err := b.Core.Stop(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (b *RustCoreEventBus) runInvocation(invocation map[string]any) ([]map[string]any, error) {
	return b.runInvocationWithOptions(context.Background(), invocation, b.processAvailableAfterHandlerOutcome())
}

func (b *RustCoreEventBus) runInvocationWithOptions(ctx context.Context, invocation map[string]any, processAvailableAfter bool) ([]map[string]any, error) {
	responses, err := b.runInvocationResponsesWithOptions(ctx, invocation, processAvailableAfter)
	if err != nil {
		return nil, err
	}
	return b.producedMessagesFromResponsesWithContext(ctx, responses)
}

func (b *RustCoreEventBus) runInvocationAsync(invocation map[string]any) {
	bus := b.busForInvocation(invocation)
	go func() {
		produced, err := bus.runInvocation(invocation)
		if err != nil {
			return
		}
		for _, message := range produced {
			applyCoreMessageToLocalEventBuses(message)
		}
	}()
}

func (b *RustCoreEventBus) busForInvocation(invocation map[string]any) *RustCoreEventBus {
	handlerID, _ := invocation["handler_id"].(string)
	busID, _ := invocation["bus_id"].(string)
	if b != nil && (busID == "" || b.BusID == busID) {
		b.mu.RLock()
		handlerEntry := b.handlerEntries[handlerID]
		b.mu.RUnlock()
		if handlerEntry != nil {
			return b
		}
	}
	for _, bus := range rustCoreEventBusInstancesSnapshot() {
		if bus == nil {
			continue
		}
		if busID != "" && bus.BusID != busID {
			continue
		}
		bus.mu.RLock()
		handlerEntry := bus.handlerEntries[handlerID]
		bus.mu.RUnlock()
		if handlerEntry != nil {
			return bus
		}
	}
	return b
}

func (b *RustCoreEventBus) runInvocationResponses(invocation map[string]any) ([]CoreProtocolEnvelope, error) {
	return b.runInvocationResponsesWithOptions(context.Background(), invocation, b.processAvailableAfterHandlerOutcome())
}

func (b *RustCoreEventBus) runInvocationResponsesWithBatch(invocation map[string]any, batch *coreOutcomeBatch) ([]CoreProtocolEnvelope, error) {
	return b.runInvocationResponsesWithBatchAndOptions(context.Background(), invocation, batch, b.processAvailableAfterHandlerOutcome())
}

func (b *RustCoreEventBus) runInvocationResponsesWithOptions(ctx context.Context, invocation map[string]any, processAvailableAfter bool) ([]CoreProtocolEnvelope, error) {
	return b.runInvocationResponsesWithBatchAndOptions(ctx, invocation, nil, processAvailableAfter)
}

func (b *RustCoreEventBus) runInvocationResponsesWithBatchAndOptions(parentCtx context.Context, invocation map[string]any, batch *coreOutcomeBatch, processAvailableAfter bool) ([]CoreProtocolEnvelope, error) {
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	handlerID, ok := invocation["handler_id"].(string)
	if !ok || handlerID == "" {
		return nil, fmt.Errorf("invalid core invocation handler_id: %T", invocation["handler_id"])
	}
	b.mu.RLock()
	handlerEntry, ok := b.handlerEntries[handlerID]
	b.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("missing handler: %s", handlerID)
	}
	event, err := b.eventForInvocation(invocation)
	if err != nil {
		return nil, err
	}
	applyInvocationEventSnapshot(event, invocation)
	b.overlayLocalEventConfig(event)
	event.markStarted()
	ctx := context.WithValue(parentCtx, coreInvocationContextKey{}, invocation)
	outcomeBatch := &coreOutcomeBatch{}
	if batch == nil {
		ctx = context.WithValue(ctx, coreOutcomeBatchContextKey{}, outcomeBatch)
	} else {
		ctx = context.WithValue(ctx, coreOutcomeBatchContextKey{}, batch)
	}
	type handlerOutcome struct {
		value any
		err   error
	}
	var outcomeMu sync.Mutex
	ignoreHandlerOutcome := false
	timeoutSeconds := (*float64)(nil)
	var deadline time.Time
	deadlineKind := ""
	if resolvedDeadline, kind, hasDeadline := b.localInvocationDeadline(event, handlerEntry); hasDeadline {
		deadline = resolvedDeadline
		deadlineKind = kind
		seconds := time.Until(deadline).Seconds()
		if seconds < 0 {
			seconds = 0
		}
		timeoutSeconds = &seconds
	}
	fastEventDeadline := timeoutSeconds != nil && deadlineKind == "event" && *timeoutSeconds <= fastEventDeadlineAbortThresholdSeconds
	var outcome handlerOutcome
	markTimeoutOutcome := func(err error) error {
		outcomeMu.Lock()
		ignoreHandlerOutcome = true
		outcome = handlerOutcome{err: err}
		outcomeMu.Unlock()
		return err
	}
	eventTimeoutError := func(seconds float64) error {
		if seconds <= fastEventDeadlineAbortThresholdSeconds {
			return &EventHandlerAbortedError{Message: "Aborted running handler: event timed out"}
		}
		return &EventHandlerAbortedError{
			Message: fmt.Sprintf("Aborted running handler: event timed out after %.3fs", seconds),
		}
	}
	timeoutError := func() error {
		seconds := 0.0
		if timeoutSeconds != nil {
			seconds = *timeoutSeconds
		}
		if deadlineKind == "event" {
			return markTimeoutOutcome(eventTimeoutError(seconds))
		}
		return markTimeoutOutcome(&EventHandlerTimeoutError{
			Message:        fmt.Sprintf("%s.on(%s, %s) timed out after %.3fs", b.Name, event.EventType, handlerEntry.HandlerName, seconds),
			TimeoutSeconds: seconds,
		})
	}
	runHandler := func(ctx context.Context) error {
		value, err := handlerEntry.Handle(ctx, event)
		outcomeMu.Lock()
		if !ignoreHandlerOutcome {
			outcome = handlerOutcome{value: value, err: err}
		}
		outcomeMu.Unlock()
		return err
	}
	var handlerSlowTimer *time.Timer
	if !fastEventDeadline {
		handlerSlowTimer = b.startHandlerSlowTimer(event, handlerEntry, invocation)
	}
	if timeoutSeconds != nil {
		if fastEventDeadline {
			cancelledCtx := cancelledValueContext{parent: ctx}
			fastTimeoutHandlers.submit(func() {
				_, _ = handlerEntry.Handle(cancelledCtx, event)
			})
			err = timeoutError()
		} else if *timeoutSeconds <= 0 {
			cancelledCtx := cancelledValueContext{parent: ctx}
			_ = runHandler(cancelledCtx)
			err = timeoutError()
		} else if *timeoutSeconds <= hardEventDeadlineThresholdSeconds {
			err = runWithTimeout(ctx, timeoutSeconds, timeoutError, runHandler)
		} else if !deadline.IsZero() {
			deadlineCtx, cancel := context.WithDeadline(ctx, deadline)
			err = runHandler(deadlineCtx)
			cancel()
		} else {
			err = runHandler(ctx)
		}
	} else {
		err = runHandler(ctx)
	}
	if handlerSlowTimer != nil {
		handlerSlowTimer.Stop()
	}
	if err != nil {
		outcomeMu.Lock()
		if !ignoreHandlerOutcome {
			outcome.err = err
		}
		outcomeMu.Unlock()
	}
	outcomeMu.Lock()
	finalOutcome := outcome
	outcomeMu.Unlock()
	var outcomeRecord map[string]any
	if finalOutcome.err != nil {
		outcomeRecord = b.Core.erroredHandlerOutcome(finalOutcome.err)
	} else {
		if validationErr := event.validateResultValue(finalOutcome.value); validationErr != nil {
			outcomeRecord = b.Core.erroredHandlerOutcome(validationErr)
		} else {
			outcomeValue, resultIsEventReference := completedOutcomeValue(finalOutcome.value)
			outcomeRecord = b.Core.completedHandlerOutcome(outcomeValue, resultIsEventReference)
		}
	}
	if batch != nil {
		batch.add(b.Core.handlerOutcomeRecord(invocation, outcomeRecord, processAvailableAfter))
		return localOutcomePatchEnvelopes(b, invocation, handlerEntry, event, outcomeRecord), nil
	}
	pendingOutcomes := outcomeBatch.snapshot()
	pendingOutcomes = append(pendingOutcomes, b.Core.handlerOutcomeRecord(invocation, outcomeRecord, processAvailableAfter))
	var responses []CoreProtocolEnvelope
	if len(pendingOutcomes) > 1 {
		responses, err = b.Core.completeHandlerOutcomes(pendingOutcomes)
	} else if finalOutcome.err != nil {
		responses, err = b.Core.errorHandler(invocation, finalOutcome.err, true, processAvailableAfter)
	} else {
		if validationErr := event.validateResultValue(finalOutcome.value); validationErr != nil {
			responses, err = b.Core.errorHandler(invocation, validationErr, true, processAvailableAfter)
		} else {
			outcomeValue, resultIsEventReference := completedOutcomeValue(finalOutcome.value)
			responses, err = b.Core.completeHandler(invocation, outcomeValue, resultIsEventReference, true, processAvailableAfter)
		}
	}
	if err != nil {
		if isStaleInvocationOutcomeError(err) {
			return []CoreProtocolEnvelope{eventCompletedEnvelopeForInvocation(invocation)}, nil
		}
		return nil, err
	}
	localPatch := localOutcomeResultPatchEnvelope(b, invocation, handlerEntry, event, outcomeRecord)
	return append([]CoreProtocolEnvelope{localPatch}, responses...), nil
}

func localOutcomeResultPatchEnvelope(b *RustCoreEventBus, invocation map[string]any, handlerEntry *EventHandler, event *BaseEvent, outcome map[string]any) CoreProtocolEnvelope {
	resultID, _ := invocation["result_id"].(string)
	status, _ := outcome["status"].(string)
	resultStatus := "completed"
	patchType := "result_completed"
	var resultValue any
	var errorValue any
	if status == "errored" {
		resultStatus = "error"
		patchType = "result_cancelled"
		errorValue = outcome["error"]
	} else {
		resultValue = outcome["value"]
	}
	var startedAt any
	completedAt := monotonicDatetime()
	childIDs := []string{}
	event.mu.Lock()
	localResult := event.EventResults[handlerEntry.ID]
	event.mu.Unlock()
	if localResult != nil {
		localResult.mu.Lock()
		if localResult.StartedAt != nil {
			startedAt = *localResult.StartedAt
		}
		if localResult.CompletedAt != nil {
			completedAt = *localResult.CompletedAt
		}
		childIDs = append(childIDs, localResult.EventChildIDs...)
		if len(childIDs) == 0 && len(localResult.EventChildren) > 0 {
			for _, child := range localResult.EventChildren {
				if child != nil {
					childIDs = append(childIDs, child.EventID)
				}
			}
		}
		localResult.mu.Unlock()
	}
	result := map[string]any{
		"result_id":                 resultID,
		"route_id":                  invocation["route_id"],
		"event_id":                  event.EventID,
		"handler_id":                handlerEntry.ID,
		"handler_name":              handlerEntry.HandlerName,
		"handler_file_path":         handlerEntry.HandlerFilePath,
		"handler_timeout":           handlerEntry.HandlerTimeout,
		"handler_slow_timeout":      handlerEntry.HandlerSlowTimeout,
		"timeout":                   effectiveResultTimeout(b, event, handlerEntry, nil, invocation),
		"handler_registered_at":     handlerEntry.HandlerRegisteredAt,
		"handler_event_pattern":     handlerEntry.EventPattern,
		"eventbus_name":             handlerEntry.EventBusName,
		"eventbus_id":               handlerEntry.EventBusID,
		"started_at":                startedAt,
		"completed_at":              completedAt,
		"status":                    resultStatus,
		"result":                    resultValue,
		"error":                     errorValue,
		"event_children":            childIDs,
		"result_is_event_reference": outcome["result_is_event_reference"],
		"result_is_undefined":       outcome["result_is_undefined"],
	}
	return coreEnvelopeFromMessage(map[string]any{
		"type": "patch",
		"patch": map[string]any{
			"type":       patchType,
			"local_only": true,
			"result":     result,
		},
	})
}

func localOutcomePatchEnvelopes(b *RustCoreEventBus, invocation map[string]any, handlerEntry *EventHandler, event *BaseEvent, outcome map[string]any) []CoreProtocolEnvelope {
	localResultPatch := localOutcomeResultPatchEnvelope(b, invocation, handlerEntry, event, outcome)
	completedAt := monotonicDatetime()
	event.mu.Lock()
	eventStartedAtValue := event.EventStartedAt
	event.mu.Unlock()
	eventStartedAt := completedAt
	if eventStartedAtValue != nil {
		eventStartedAt = *eventStartedAtValue
	}
	return []CoreProtocolEnvelope{
		localResultPatch,
		coreEnvelopeFromMessage(map[string]any{
			"type": "patch",
			"patch": map[string]any{
				"type":             "event_completed_compact",
				"event_id":         event.EventID,
				"completed_at":     completedAt,
				"event_started_at": eventStartedAt,
			},
		}),
	}
}

func effectiveResultTimeout(b *RustCoreEventBus, event *BaseEvent, handler *EventHandler, handlerTimeoutOverride *float64, invocation map[string]any) *float64 {
	handlerTimeout := handlerTimeoutOverride
	if handlerTimeout == nil && invocation != nil {
		if snapshot, _ := invocation["result_snapshot"].(map[string]any); snapshot != nil {
			handlerTimeout = coreFloatPtr(snapshot["timeout"])
		}
	}
	if handlerTimeout == nil && handler != nil {
		handlerTimeout = handler.HandlerTimeout
	}
	if handlerTimeout == nil && event != nil {
		handlerTimeout = event.EventHandlerTimeout
	}
	if handlerTimeout == nil && b != nil {
		handlerTimeout = b.EventHandlerTimeout
	}
	var eventTimeout *float64
	if event != nil {
		eventTimeout = event.EventTimeout
	}
	if eventTimeout == nil && b != nil {
		eventTimeout = b.EventTimeout
	}
	return minFloatPtr(handlerTimeout, eventTimeout)
}

func completedOutcomeValue(value any) (any, bool) {
	if event, ok := value.(*BaseEvent); ok && event != nil {
		return event.EventID, true
	}
	return value, false
}

func minFloatPtr(left *float64, right *float64) *float64 {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	if *left <= *right {
		return left
	}
	return right
}

func (b *RustCoreEventBus) overlayLocalEventConfig(event *BaseEvent) {
	if b == nil || event == nil {
		return
	}
	needsLocalOverlay := event.EventParentID != nil ||
		event.EventEmittedByResultID != nil ||
		event.EventEmittedByHandlerID != nil ||
		len(event.EventPath) > 1
	if !needsLocalOverlay {
		return
	}
	for _, wrapper := range eventBusInstancesForEvent(event.EventID) {
		if wrapper == nil || wrapper.EventHistory == nil {
			continue
		}
		local := wrapper.EventHistory.GetEvent(event.EventID)
		if local == nil || local == event {
			continue
		}
		local.mu.Lock()
		eventTimeout := local.EventTimeout
		eventSlowTimeout := local.EventSlowTimeout
		eventHandlerTimeout := local.EventHandlerTimeout
		eventHandlerSlowTimeout := local.EventHandlerSlowTimeout
		eventHandlerCompletion := local.EventHandlerCompletion
		eventBlocksParentCompletion := local.EventBlocksParentCompletion
		dispatchCtx := local.dispatchCtx
		local.mu.Unlock()
		event.mu.Lock()
		event.EventTimeout = eventTimeout
		event.EventSlowTimeout = eventSlowTimeout
		event.EventHandlerTimeout = eventHandlerTimeout
		event.EventHandlerSlowTimeout = eventHandlerSlowTimeout
		event.EventHandlerCompletion = eventHandlerCompletion
		event.EventBlocksParentCompletion = eventBlocksParentCompletion
		if event.dispatchCtx == nil {
			event.dispatchCtx = dispatchCtx
		}
		if event.Bus == nil {
			event.Bus = wrapper
		}
		event.mu.Unlock()
		return
	}
}

func (b *RustCoreEventBus) startHandlerSlowTimer(event *BaseEvent, handler *EventHandler, invocation map[string]any) *time.Timer {
	slowTimeout := handler.HandlerSlowTimeout
	if slowTimeout == nil && event != nil {
		slowTimeout = event.EventHandlerSlowTimeout
	}
	if slowTimeout == nil && event != nil {
		slowTimeout = event.EventSlowTimeout
	}
	if slowTimeout == nil && b != nil {
		slowTimeout = b.EventHandlerSlowTimeout
	}
	if slowTimeout == nil && b != nil {
		slowTimeout = b.EventSlowTimeout
	}
	if slowTimeout == nil {
		return nil
	}
	handlerTimeout := effectiveResultTimeout(b, event, handler, nil, invocation)
	if handlerTimeout != nil && *handlerTimeout <= *slowTimeout {
		return nil
	}
	eventType := ""
	if event != nil {
		eventType = event.EventType
	}
	busName := ""
	if b != nil {
		busName = b.Name
	}
	handlerName := ""
	if handler != nil {
		handlerName = handler.HandlerName
	}
	return time.AfterFunc(time.Duration(*slowTimeout*float64(time.Second)), func() {
		SlowWarningLogger(fmt.Sprintf("[abxbus] Slow event handler: %s.on(%s, %s) still running", busName, eventType, handlerName))
	})
}

func coreEnvelopeFromMessage(message map[string]any) CoreProtocolEnvelope {
	return CoreProtocolEnvelope{
		ProtocolVersion: CoreProtocolVersion,
		Message:         message,
	}
}

func eventCompletedEnvelopeForInvocation(invocation map[string]any) CoreProtocolEnvelope {
	eventID, _ := invocation["event_id"].(string)
	return coreEnvelopeFromMessage(map[string]any{"type": "event_completed", "event_id": eventID})
}

func isStaleInvocationOutcomeError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "stale invocation outcome") || strings.Contains(message, "missing result")
}

func stashCoreInitialForEvent(eventID string, envelope CoreProtocolEnvelope) bool {
	if eventID == "" {
		return false
	}
	for _, wrapper := range eventBusInstancesForEvent(eventID) {
		if wrapper == nil || wrapper.core == nil || wrapper.EventHistory == nil {
			continue
		}
		if wrapper.EventHistory.GetEvent(eventID) == nil {
			continue
		}
		wrapper.mu.Lock()
		wrapper.coreInitial[eventID] = append(wrapper.coreInitial[eventID], envelope)
		wrapper.mu.Unlock()
		return true
	}
	return false
}

func (b *RustCoreEventBus) invocationCanUseBatchedOutcome(invocation map[string]any) bool {
	busID, _ := invocation["bus_id"].(string)
	if busID != "" && busID != b.BusID {
		return false
	}
	handlerID, _ := invocation["handler_id"].(string)
	if handlerID == "" {
		return false
	}
	b.mu.RLock()
	handlerEntry := b.handlerEntries[handlerID]
	b.mu.RUnlock()
	if handlerEntry == nil || handlerEntry.EventPattern == "*" {
		return false
	}
	eventSnapshot, _ := invocation["event_snapshot"].(map[string]any)
	eventID, _ := invocation["event_id"].(string)
	var localEvent *BaseEvent
	if eventID != "" {
		for _, wrapper := range eventBusInstancesForEvent(eventID) {
			if wrapper == nil || wrapper.core != b || wrapper.EventHistory == nil {
				continue
			}
			if busID != "" && wrapper.ID != busID {
				continue
			}
			if event := wrapper.EventHistory.GetEvent(eventID); event != nil {
				localEvent = event
				break
			}
		}
	}
	if localEvent != nil {
		localEvent.mu.Lock()
		completion := localEvent.EventHandlerCompletion
		if completion == "" {
			completion = b.EventHandlerCompletion
		}
		localEvent.mu.Unlock()
		if completion == EventHandlerCompletionFirst {
			return false
		}
	}
	completion, _ := eventSnapshot["event_handler_completion"].(string)
	if completion == "" {
		completion = string(b.EventHandlerCompletion)
	}
	return completion != string(EventHandlerCompletionFirst)
}

func (b *RustCoreEventBus) runInvocationOutcome(invocation map[string]any) (map[string]any, CoreProtocolEnvelope, error) {
	return b.runInvocationOutcomeWithOptions(context.Background(), invocation, b.processAvailableAfterHandlerOutcome())
}

func (b *RustCoreEventBus) runInvocationOutcomeWithOptions(parentCtx context.Context, invocation map[string]any, processAvailableAfter bool) (map[string]any, CoreProtocolEnvelope, error) {
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	handlerID, _ := invocation["handler_id"].(string)
	b.mu.RLock()
	handlerEntry := b.handlerEntries[handlerID]
	b.mu.RUnlock()
	if handlerEntry == nil {
		return nil, CoreProtocolEnvelope{}, fmt.Errorf("missing handler: %s", handlerID)
	}
	event, err := b.eventForInvocation(invocation)
	if err != nil {
		return nil, CoreProtocolEnvelope{}, err
	}
	applyInvocationEventSnapshot(event, invocation)
	b.overlayLocalEventConfig(event)
	event.markStarted()
	ctx := context.WithValue(parentCtx, coreInvocationContextKey{}, invocation)
	type singleHandlerOutcome struct {
		value any
		err   error
	}
	var outcomeState singleHandlerOutcome
	var outcomeMu sync.Mutex
	ignoreHandlerOutcome := false
	markTimeoutOutcome := func(err error) error {
		outcomeMu.Lock()
		ignoreHandlerOutcome = true
		outcomeState = singleHandlerOutcome{err: err}
		outcomeMu.Unlock()
		return err
	}
	runHandler := func(ctx context.Context) error {
		handlerValue, err := handlerEntry.Handle(ctx, event)
		if err == nil {
			if validationErr := event.validateResultValue(handlerValue); validationErr != nil {
				err = validationErr
			}
		}
		outcomeMu.Lock()
		if !ignoreHandlerOutcome {
			outcomeState = singleHandlerOutcome{value: handlerValue, err: err}
		}
		outcomeMu.Unlock()
		return err
	}
	deadline, deadlineKind, hasDeadline := b.localInvocationDeadline(event, handlerEntry)
	seconds := 0.0
	if hasDeadline {
		seconds = time.Until(deadline).Seconds()
		if seconds < 0 {
			seconds = 0
		}
	}
	fastEventDeadline := hasDeadline && deadlineKind == "event" && seconds <= fastEventDeadlineAbortThresholdSeconds
	var handlerSlowTimer *time.Timer
	if !fastEventDeadline {
		handlerSlowTimer = b.startHandlerSlowTimer(event, handlerEntry, invocation)
	}
	if hasDeadline {
		timeoutError := func() error {
			if deadlineKind == "event" {
				if seconds <= fastEventDeadlineAbortThresholdSeconds {
					return markTimeoutOutcome(&EventHandlerAbortedError{Message: "Aborted running handler: event timed out"})
				}
				return markTimeoutOutcome(&EventHandlerAbortedError{
					Message: fmt.Sprintf("Aborted running handler: event timed out after %.3fs", seconds),
				})
			}
			return markTimeoutOutcome(&EventHandlerTimeoutError{
				Message:        fmt.Sprintf("%s.on(%s, %s) timed out after %.3fs", b.Name, event.EventType, handlerEntry.HandlerName, seconds),
				TimeoutSeconds: seconds,
			})
		}
		if fastEventDeadline {
			cancelledCtx := cancelledValueContext{parent: ctx}
			fastTimeoutHandlers.submit(func() {
				_, _ = handlerEntry.Handle(cancelledCtx, event)
			})
			err = timeoutError()
		} else if seconds <= 0 || !deadline.After(time.Now()) {
			cancelledCtx := cancelledValueContext{parent: ctx}
			_ = runHandler(cancelledCtx)
			err = timeoutError()
		} else if seconds <= hardEventDeadlineThresholdSeconds {
			err = runWithTimeout(ctx, &seconds, timeoutError, runHandler)
		} else {
			deadlineCtx, cancel := context.WithDeadline(ctx, deadline)
			err = runHandler(deadlineCtx)
			cancel()
		}
	} else {
		err = runHandler(ctx)
	}
	if handlerSlowTimer != nil {
		handlerSlowTimer.Stop()
	}
	if err != nil {
		outcomeMu.Lock()
		if !ignoreHandlerOutcome {
			outcomeState.err = err
		}
		outcomeMu.Unlock()
	}
	outcomeMu.Lock()
	finalOutcome := outcomeState
	outcomeMu.Unlock()
	var outcome map[string]any
	if finalOutcome.err != nil {
		outcome = b.Core.erroredHandlerOutcome(finalOutcome.err)
	} else {
		outcomeValue, resultIsEventReference := completedOutcomeValue(finalOutcome.value)
		outcome = b.Core.completedHandlerOutcome(outcomeValue, resultIsEventReference)
	}
	return b.Core.handlerOutcomeRecord(invocation, outcome, processAvailableAfter),
		localOutcomeResultPatchEnvelope(b, invocation, handlerEntry, event, outcome),
		nil
}

func (b *RustCoreEventBus) localInvocationDeadline(event *BaseEvent, handlerEntry *EventHandler) (time.Time, string, bool) {
	now := time.Now()
	var handlerDeadline time.Time
	hasHandlerDeadline := false
	if handlerEntry != nil {
		handlerTimeout := handlerEntry.HandlerTimeout
		if handlerTimeout == nil && event != nil {
			handlerTimeout = event.EventHandlerTimeout
		}
		if handlerTimeout == nil {
			handlerTimeout = b.EventHandlerTimeout
		}
		if handlerTimeout != nil && *handlerTimeout >= 0 {
			handlerDeadline = now.Add(time.Duration(*handlerTimeout * float64(time.Second)))
			hasHandlerDeadline = true
		}
	}
	var eventDeadline time.Time
	hasEventDeadline := false
	if event != nil {
		eventTimeout := event.EventTimeout
		if eventTimeout == nil {
			eventTimeout = b.EventTimeout
		}
		if eventTimeout != nil && *eventTimeout >= 0 {
			eventStart := b.localEventStart(event.EventID)
			if event.EventStartedAt != nil {
				if parsed, err := time.Parse(time.RFC3339Nano, *event.EventStartedAt); err == nil {
					eventStart = parsed
				}
			}
			eventDeadline = eventStart.Add(time.Duration(*eventTimeout * float64(time.Second)))
			hasEventDeadline = true
		}
	}
	if hasEventDeadline && (!hasHandlerDeadline || eventDeadline.Before(handlerDeadline) || eventDeadline.Equal(handlerDeadline)) {
		return eventDeadline, "event", true
	}
	if hasHandlerDeadline {
		return handlerDeadline, "handler", true
	}
	return time.Time{}, "", false
}

func (b *RustCoreEventBus) eventForInvocation(invocation map[string]any) (*BaseEvent, error) {
	eventID, _ := invocation["event_id"].(string)
	if eventID == "" {
		return nil, fmt.Errorf("missing event id for invocation")
	}
	eventSnapshot, _ := invocation["event_snapshot"].(map[string]any)
	if eventSnapshot != nil {
		event, err := baseEventFromCoreRecord(eventSnapshot)
		if err != nil {
			return nil, err
		}
		restoreCoreJSONNumbers(event)
		return event, nil
	}
	busID, _ := invocation["bus_id"].(string)
	for _, wrapper := range eventBusInstancesForEvent(eventID) {
		if wrapper == nil || wrapper.core != b {
			continue
		}
		if busID != "" && wrapper.ID != busID {
			continue
		}
		if event := wrapper.EventHistory.GetEvent(eventID); event != nil {
			return event, nil
		}
	}
	if eventSnapshot == nil {
		var err error
		eventSnapshot, err = b.Core.GetEvent(eventID)
		if err != nil {
			return nil, err
		}
		if eventSnapshot == nil {
			return nil, fmt.Errorf("missing event: %s", eventID)
		}
	}
	event, err := baseEventFromCoreRecord(eventSnapshot)
	if err != nil {
		return nil, err
	}
	restoreCoreJSONNumbers(event)
	return event, nil
}

func coreInvocationDeadlineAt(invocation map[string]any, key string) (time.Time, bool) {
	deadlineValue, _ := invocation[key].(string)
	if deadlineValue == "" {
		return time.Time{}, false
	}
	deadline, err := time.Parse(time.RFC3339Nano, deadlineValue)
	if err != nil {
		return time.Time{}, false
	}
	return deadline, true
}

func (b *RustCoreEventBus) processTimedOutInvocation(invocation map[string]any) ([]map[string]any, error) {
	resultSnapshot, _ := invocation["result_snapshot"].(map[string]any)
	routeID, _ := invocation["route_id"].(string)
	if routeID == "" {
		routeID, _ = resultSnapshot["route_id"].(string)
	}
	if routeID == "" {
		return b.producedMessagesFromResponses(nil)
	}
	var responses []CoreProtocolEnvelope
	var err error
	for i := 0; i < 5; i++ {
		responses, err = b.Core.ProcessRouteCompact(routeID, coreRouteSliceLimit)
		if err != nil || len(responses) > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil {
		return nil, err
	}
	return b.producedMessagesFromResponses(responses)
}

func (b *RustCoreEventBus) producedMessagesFromResponses(responses []CoreProtocolEnvelope) ([]map[string]any, error) {
	return b.producedMessagesFromResponsesWithBatch(responses, nil)
}

func (b *RustCoreEventBus) producedMessagesFromResponsesWithBatch(responses []CoreProtocolEnvelope, batch *coreOutcomeBatch) ([]map[string]any, error) {
	return b.producedMessagesFromResponsesWithBatchForTarget(context.Background(), responses, batch, "")
}

func (b *RustCoreEventBus) producedMessagesFromResponsesWithContext(ctx context.Context, responses []CoreProtocolEnvelope) ([]map[string]any, error) {
	return b.producedMessagesFromResponsesWithBatchForTarget(ctx, responses, nil, "")
}

func (b *RustCoreEventBus) producedMessagesFromResponsesWithBatchForTarget(ctx context.Context, responses []CoreProtocolEnvelope, batch *coreOutcomeBatch, targetEventID string) ([]map[string]any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	processAvailableAfter := b.processAvailableAfterHandlerOutcome()
	if targetEventID != "" {
		processAvailableAfter = false
	}
	produced := []map[string]any{}
	invocations := []map[string]any{}
	seenInvocations := map[string]bool{}
	completedRoutes := map[string]bool{}
	processedResultRoutes := map[string]bool{}
	enqueueInvocation := func(message map[string]any) {
		for _, invocation := range invocationMessagesFromCoreMessage(message) {
			invocationID, _ := invocation["invocation_id"].(string)
			if invocationID != "" {
				if seenInvocations[invocationID] {
					continue
				}
				seenInvocations[invocationID] = true
			}
			invocations = append(invocations, invocation)
		}
	}
	appendResponses := func(responses []CoreProtocolEnvelope) error {
		maxPatchSeq := uint64(0)
		for _, response := range responses {
			if response.LastPatchSeq != nil && *response.LastPatchSeq > maxPatchSeq {
				maxPatchSeq = *response.LastPatchSeq
			}
			message, err := decodeCoreMessage(response)
			if err != nil {
				return err
			}
			if message == nil {
				continue
			}
			if targetEventID != "" && len(invocationMessagesFromCoreMessage(message)) > 0 {
				matchingInvocations := []map[string]any{}
				allMatch := true
				for _, invocation := range invocationMessagesFromCoreMessage(message) {
					invocationEventID, _ := invocation["event_id"].(string)
					if invocationEventID != "" && invocationEventID != targetEventID {
						allMatch = false
						stashCoreInitialForEvent(invocationEventID, coreEnvelopeFromMessage(invocation))
						continue
					}
					matchingInvocations = append(matchingInvocations, invocation)
				}
				if len(matchingInvocations) == 0 {
					continue
				}
				if !allMatch {
					message = compactInvocationBatchMessage(matchingInvocations)
				}
			}
			produced = append(produced, message)
			enqueueInvocation(message)
		}
		if maxPatchSeq > 0 {
			b.Core.SetPatchSeq(maxPatchSeq)
		}
		return nil
	}
	if err := appendResponses(responses); err != nil {
		return nil, err
	}
	processCompletedResultRoutes := func(scanFrom int) error {
		for index := scanFrom; index < len(produced); index++ {
			message := produced[index]
			if message["type"] != "patch" {
				continue
			}
			patch, _ := message["patch"].(map[string]any)
			if patch == nil {
				continue
			}
			if patch["type"] == "route_completed" {
				if localOnly, _ := patch["local_only"].(bool); localOnly {
					continue
				}
				routeID, _ := patch["route_id"].(string)
				if routeID != "" {
					completedRoutes[routeID] = true
				}
				continue
			}
			if patch["type"] != "result_completed" {
				continue
			}
			result, _ := patch["result"].(map[string]any)
			routeID, _ := result["route_id"].(string)
			if routeID == "" || completedRoutes[routeID] || processedResultRoutes[routeID] {
				continue
			}
			processedResultRoutes[routeID] = true
			followups, err := b.Core.ProcessRouteCompact(routeID, coreRouteSliceLimit)
			if err != nil {
				return err
			}
			if err := appendResponses(followups); err != nil {
				return err
			}
		}
		return nil
	}
	if err := processCompletedResultRoutes(0); err != nil {
		return nil, err
	}
	for len(invocations) > 0 {
		current := invocations
		invocations = []map[string]any{}
		canBatch := len(current) > 1
		for _, invocation := range current {
			invocationBus := b.busForInvocation(invocation)
			if !invocationBus.invocationCanUseBatchedOutcome(invocation) {
				canBatch = false
				break
			}
		}
		if canBatch {
			outcomes := make([]map[string]any, len(current))
			localPatches := make([]CoreProtocolEnvelope, len(current))
			errCh := make(chan error, len(current))
			var wg sync.WaitGroup
			workerCount := len(current)
			if workerCount > 1024 {
				workerCount = runtime.GOMAXPROCS(0) * 4
				if workerCount < 1 {
					workerCount = 1
				}
				if workerCount > len(current) {
					workerCount = len(current)
				}
			}
			jobs := make(chan int, workerCount)
			for worker := 0; worker < workerCount; worker++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range jobs {
						invocation := current[i]
						invocationBus := b.busForInvocation(invocation)
						outcome, localPatch, err := invocationBus.runInvocationOutcomeWithOptions(ctx, invocation, processAvailableAfter)
						if err != nil {
							errCh <- err
							return
						}
						outcomes[i] = outcome
						localPatches[i] = localPatch
					}
				}()
			}
			for i := range current {
				jobs <- i
			}
			close(jobs)
			wg.Wait()
			close(errCh)
			if err := <-errCh; err != nil {
				return nil, err
			}
			responses, err := b.Core.completeHandlerOutcomes(outcomes)
			if err != nil {
				return nil, err
			}
			if err := appendResponses(localPatches); err != nil {
				return nil, err
			}
			if err := appendResponses(responses); err != nil {
				return nil, err
			}
			if err := processCompletedResultRoutes(0); err != nil {
				return nil, err
			}
			continue
		}
		if len(current) == 1 {
			invocationBus := b.busForInvocation(current[0])
			responses, err := invocationBus.runInvocationResponsesWithBatchAndOptions(ctx, current[0], batch, processAvailableAfter)
			if err != nil {
				return nil, err
			}
			if responses == nil {
				nested, err := invocationBus.runInvocationWithOptions(ctx, current[0], processAvailableAfter)
				if err != nil {
					return nil, err
				}
				for _, message := range nested {
					produced = append(produced, message)
					enqueueInvocation(message)
				}
				if err := processCompletedResultRoutes(0); err != nil {
					return nil, err
				}
				continue
			}
			if err := appendResponses(responses); err != nil {
				return nil, err
			}
			if err := processCompletedResultRoutes(0); err != nil {
				return nil, err
			}
			continue
		}
		nestedByInvocation := make([][]map[string]any, len(current))
		errCh := make(chan error, len(current))
		var wg sync.WaitGroup
		for i, invocation := range current {
			i, invocation := i, invocation
			wg.Add(1)
			go func() {
				defer wg.Done()
				invocationBus := b.busForInvocation(invocation)
				nested, err := invocationBus.runInvocationWithOptions(ctx, invocation, processAvailableAfter)
				if err != nil {
					errCh <- err
					return
				}
				nestedByInvocation[i] = nested
			}()
		}
		wg.Wait()
		close(errCh)
		if err := <-errCh; err != nil {
			return nil, err
		}
		for _, nested := range nestedByInvocation {
			for _, message := range nested {
				produced = append(produced, message)
				enqueueInvocation(message)
			}
		}
		if err := processCompletedResultRoutes(0); err != nil {
			return nil, err
		}
	}
	for len(invocations) > 0 {
		current := invocations
		invocations = []map[string]any{}
		for _, invocation := range current {
			invocationBus := b.busForInvocation(invocation)
			nested, err := invocationBus.runInvocationWithOptions(ctx, invocation, processAvailableAfter)
			if err != nil {
				return nil, err
			}
			for _, message := range nested {
				produced = append(produced, message)
				enqueueInvocation(message)
			}
			if err := processCompletedResultRoutes(0); err != nil {
				return nil, err
			}
		}
	}
	return produced, nil
}

func (b *RustCoreEventBus) applyCoreMessage(event *BaseEvent, message map[string]any) {
	if message["type"] == "event_completed" {
		if message["event_id"] == event.EventID {
			if event.EventStatus == "pending" {
				event.markStarted()
			}
			event.markCompleted()
			completeLocalNonterminalResults(event)
		}
		return
	}
	if message["type"] != "patch" {
		return
	}
	patch, _ := message["patch"].(map[string]any)
	if patch == nil {
		return
	}
	if localOnly, _ := patch["local_only"].(bool); !localOnly {
		if len(b.Core.FilterUnseenPatchMessages([]map[string]any{message})) == 0 {
			return
		}
		defer b.Core.AckPatchMessages([]map[string]any{message})
	}
	switch patch["type"] {
	case "event_started":
		if patch["event_id"] == event.EventID {
			event.markStarted()
		}
	case "result_pending":
		b.applyResultRecord(event, patch["result"], EventResultPending)
	case "result_started":
		resultID, _ := patch["result_id"].(string)
		startedAt, _ := patch["started_at"].(string)
		for _, result := range event.EventResults {
			if result.ID == resultID {
				if eventResultIsTerminal(result) {
					return
				}
				result.Update(&EventResultUpdateOptions{Status: EventResultStarted})
				if startedAt != "" {
					result.StartedAt = &startedAt
				}
				return
			}
		}
	case "result_completed", "result_cancelled", "result_timed_out":
		b.applyResultRecord(event, patch["result"], "")
	case "event_completed", "event_completed_compact":
		if patch["event_id"] == event.EventID {
			startedAt, _ := patch["event_started_at"].(string)
			if event.EventStatus == "pending" {
				if startedAt != "" {
					event.EventStatus = "started"
					event.EventStartedAt = &startedAt
				} else {
					event.markStarted()
				}
			} else if startedAt != "" {
				event.EventStartedAt = &startedAt
			}
			completedAt, _ := patch["completed_at"].(string)
			if completedAt != "" {
				event.markCompletedAt(&completedAt)
			} else {
				event.markCompleted()
			}
			completeLocalNonterminalResults(event)
		}
	}
}

func (b *RustCoreEventBus) applyResultRecord(event *BaseEvent, value any, statusOverride EventResultStatus) {
	record, _ := value.(map[string]any)
	if record == nil {
		return
	}
	eventID, _ := record["event_id"].(string)
	handlerID, _ := record["handler_id"].(string)
	if eventID != event.EventID || handlerID == "" {
		return
	}
	ownerBus, handlerEntry := coreHandlerEntryForResult(b, handlerID)
	if handlerEntry == nil {
		return
	}
	if statusOverride == EventResultPending || statusOverride == EventResultStarted {
		event.mu.Lock()
		existing := event.EventResults[handlerID]
		event.mu.Unlock()
		if eventResultIsTerminal(existing) {
			return
		}
	}
	options := &BaseEventResultUpdateOptions{}
	if statusOverride != "" {
		options.Status = statusOverride
	} else if status, _ := record["status"].(string); status == "completed" {
		options.Result = normalizeCoreValue(record["result"])
		if resultIsUndefined, _ := record["result_is_undefined"].(bool); resultIsUndefined {
			options.ResultSet = false
		} else {
			options.ResultSet = true
		}
	} else if status == "error" || status == "cancelled" || status == "timed_out" {
		options.Error = normalizeCoreValue(record["error"])
		options.ErrorSet = true
	}
	result := event.EventResultUpdate(handlerEntry, options)
	resultID, _ := record["result_id"].(string)
	if resultID == "" {
		resultID, _ = record["id"].(string)
	}
	if resultID != "" {
		result.ID = resultID
	}
	childIDs := coreStringSlice(record["event_children"])
	if len(childIDs) > 0 {
		result.mu.Lock()
		result.EventChildIDs = append([]string{}, childIDs...)
		result.EventChildren = result.EventChildren[:0]
		if event.Bus != nil && event.Bus.EventHistory != nil {
			for _, childID := range childIDs {
				child := event.Bus.EventHistory.GetEvent(childID)
				if child != nil {
					result.EventChildren = append(result.EventChildren, child)
				}
			}
		}
		result.mu.Unlock()
	}
	result.Timeout = effectiveResultTimeout(ownerBus, event, handlerEntry, coreFloatPtr(record["timeout"]), nil)
}

func eventResultIsTerminal(result *EventResult) bool {
	if result == nil {
		return false
	}
	status, _, _, _ := result.snapshot()
	return status == EventResultCompleted || status == EventResultError
}

func decodeCoreMessage(envelope CoreProtocolEnvelope) (map[string]any, error) {
	return envelope.Message, nil
}

func invocationMessagesFromCoreMessage(message map[string]any) []map[string]any {
	if message == nil {
		return nil
	}
	if message["type"] == "invoke_handler" {
		return []map[string]any{message}
	}
	if message["type"] == "invoke_handlers_compact" {
		routeID, _ := message["route_id"].(string)
		eventID, _ := message["event_id"].(string)
		busID, _ := message["bus_id"].(string)
		eventDeadlineAt, _ := message["event_deadline_at"].(string)
		routePaused, _ := message["route_paused"].(bool)
		raw, _ := message["invocations"].([]any)
		invocations := make([]map[string]any, 0, len(raw))
		for _, item := range raw {
			fields, ok := item.([]any)
			if !ok || len(fields) < 5 {
				continue
			}
			invocationID, _ := fields[0].(string)
			resultID, _ := fields[1].(string)
			handlerID, _ := fields[2].(string)
			fence := uint64(coreInt(fields[3]))
			deadlineAt, _ := fields[4].(string)
			invocation := map[string]any{
				"type":            "invoke_handler",
				"invocation_id":   invocationID,
				"result_id":       resultID,
				"route_id":        routeID,
				"event_id":        eventID,
				"bus_id":          busID,
				"handler_id":      handlerID,
				"fence":           fence,
				"route_paused":    routePaused,
				"deadline_at":     nil,
				"event_snapshot":  nil,
				"result_snapshot": nil,
			}
			if deadlineAt != "" {
				invocation["deadline_at"] = deadlineAt
			}
			if eventDeadlineAt != "" {
				invocation["event_deadline_at"] = eventDeadlineAt
			}
			invocations = append(invocations, invocation)
		}
		return invocations
	}
	return nil
}

func compactInvocationBatchMessage(invocations []map[string]any) map[string]any {
	if len(invocations) == 0 {
		return nil
	}
	if len(invocations) == 1 {
		return invocations[0]
	}
	first := invocations[0]
	message := map[string]any{
		"type":         "invoke_handlers_compact",
		"route_id":     first["route_id"],
		"event_id":     first["event_id"],
		"bus_id":       first["bus_id"],
		"route_paused": first["route_paused"],
	}
	if eventDeadlineAt, ok := first["event_deadline_at"].(string); ok && eventDeadlineAt != "" {
		message["event_deadline_at"] = eventDeadlineAt
	}
	raw := make([]any, 0, len(invocations))
	for _, invocation := range invocations {
		raw = append(raw, []any{
			invocation["invocation_id"],
			invocation["result_id"],
			invocation["handler_id"],
			invocation["fence"],
			invocation["deadline_at"],
		})
	}
	message["invocations"] = raw
	return message
}

func eventBusInstancesForEvent(eventID string) []*EventBus {
	if eventID == "" {
		return nil
	}
	var buses []*EventBus
	for _, bus := range eventBusInstancesSnapshot() {
		if bus != nil && bus.EventHistory != nil && bus.EventHistory.GetEvent(eventID) != nil {
			buses = append(buses, bus)
		}
	}
	return buses
}

func copyBaseEventRuntime(target *BaseEvent, source *BaseEvent, bus *EventBus) {
	if target == nil || source == nil {
		return
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	source.mu.Lock()
	defer source.mu.Unlock()

	target.EventCreatedAt = source.EventCreatedAt
	target.EventType = source.EventType
	target.EventVersion = source.EventVersion
	target.EventTimeout = source.EventTimeout
	target.EventSlowTimeout = source.EventSlowTimeout
	target.EventHandlerTimeout = source.EventHandlerTimeout
	target.EventHandlerSlowTimeout = source.EventHandlerSlowTimeout
	target.EventParentID = source.EventParentID
	target.EventPath = append([]string{}, source.EventPath...)
	target.EventResultType = source.EventResultType
	target.EventEmittedByHandlerID = source.EventEmittedByHandlerID
	target.EventEmittedByResultID = source.EventEmittedByResultID
	target.EventPendingBusCount = source.EventPendingBusCount
	target.EventStatus = source.EventStatus
	target.EventStartedAt = source.EventStartedAt
	target.EventCompletedAt = source.EventCompletedAt
	target.EventConcurrency = source.EventConcurrency
	target.EventHandlerConcurrency = source.EventHandlerConcurrency
	target.EventHandlerCompletion = source.EventHandlerCompletion
	target.EventBlocksParentCompletion = source.EventBlocksParentCompletion
	target.Payload = cloneMap(source.Payload)
	target.EventResults = cloneEventResults(source.EventResults, target)
	target.eventResultOrder = append([]string{}, source.eventResultOrder...)
	target.eventResultTypeRaw = append([]byte{}, source.eventResultTypeRaw...)
	target.coreKnown = source.coreKnown
	if bus != nil {
		target.Bus = bus
	}
}

func cloneMap(values map[string]any) map[string]any {
	if values == nil {
		return nil
	}
	cloned := make(map[string]any, len(values))
	for key, value := range values {
		cloned[key] = normalizeCoreValue(value)
	}
	return cloned
}

func cloneEventResults(results map[string]*EventResult, event *BaseEvent) map[string]*EventResult {
	cloned := make(map[string]*EventResult, len(results))
	for key, result := range results {
		if result == nil {
			continue
		}
		copy := *result
		copy.Event = event
		copy.Handler = nil
		copy.EventChildren = append([]*BaseEvent{}, result.EventChildren...)
		copy.EventChildIDs = append([]string{}, result.EventChildIDs...)
		copy.done_ch = make(chan struct{})
		copy.once = sync.Once{}
		if copy.Status == EventResultCompleted || copy.Status == EventResultError {
			copy.once.Do(func() { close(copy.done_ch) })
		}
		cloned[key] = &copy
	}
	return cloned
}

func eventResultRegisteredTimesAllEqual(results map[string]*EventResult) bool {
	first := ""
	seen := false
	for _, result := range results {
		if result == nil {
			continue
		}
		if !seen {
			first = result.HandlerRegisteredAt
			seen = true
			continue
		}
		if result.HandlerRegisteredAt != first {
			return false
		}
	}
	return true
}

func (b *EventBus) coreRouteIDForEvent(eventID string) string {
	if b == nil || eventID == "" {
		return ""
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.coreRouteIDs == nil {
		return ""
	}
	return b.coreRouteIDs[eventID]
}

func (b *EventBus) takeCoreInitial(eventID string) []CoreProtocolEnvelope {
	if b == nil || eventID == "" {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.coreInitial == nil {
		return nil
	}
	initial := append([]CoreProtocolEnvelope{}, b.coreInitial[eventID]...)
	delete(b.coreInitial, eventID)
	return initial
}
