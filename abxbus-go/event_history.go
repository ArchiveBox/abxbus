package abxbus

import (
	"sync"
	"time"
)

const DefaultMaxHistorySize = 100
const UnlimitedHistorySize = -1

type EventHistory struct {
	MaxHistorySize *int
	MaxHistoryDrop bool
	events         map[string]*BaseEvent
	order          []string
	mu             sync.RWMutex
	onAdd          func(eventID string)
	onRemove       func(eventID string)
}

type EventHistoryFindOptions struct {
	Past    any
	ChildOf *BaseEvent
	Equals  map[string]any
	Limit   *int
}

func NewEventHistory(max_history_size *int, max_history_drop bool) *EventHistory {
	if max_history_size != nil && *max_history_size < 0 {
		max_history_size = nil
	}
	return &EventHistory{MaxHistorySize: max_history_size, MaxHistoryDrop: max_history_drop, events: map[string]*BaseEvent{}, order: []string{}}
}

func (h *EventHistory) AddEvent(event *BaseEvent) {
	h.mu.Lock()
	added := false
	if _, exists := h.events[event.EventID]; !exists {
		h.order = append(h.order, event.EventID)
		added = true
	}
	h.events[event.EventID] = event
	removedIDs := h.trimLocked(nil)
	onAdd := h.onAdd
	onRemove := h.onRemove
	h.mu.Unlock()
	if added && onAdd != nil {
		onAdd(event.EventID)
	}
	if onRemove != nil {
		for _, eventID := range removedIDs {
			onRemove(eventID)
		}
	}
}

func (h *EventHistory) GetEvent(event_id string) *BaseEvent {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.events[event_id]
}

func (h *EventHistory) Has(event_id string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := h.events[event_id]
	return ok
}

func (h *EventHistory) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.events)
}

func (h *EventHistory) Values() []*BaseEvent {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]*BaseEvent, 0, len(h.order))
	for _, id := range h.order {
		if e := h.events[id]; e != nil {
			out = append(out, e)
		}
	}
	return out
}

func (h *EventHistory) Clear() {
	h.mu.Lock()
	removedIDs := append([]string{}, h.order...)
	onRemove := h.onRemove
	h.events = map[string]*BaseEvent{}
	h.order = []string{}
	h.mu.Unlock()
	if onRemove != nil {
		for _, eventID := range removedIDs {
			onRemove(eventID)
		}
	}
}

func (h *EventHistory) Find(event_pattern string, where func(event *BaseEvent) bool, options *EventHistoryFindOptions) *BaseEvent {
	matches := h.Filter(event_pattern, where, options)
	if len(matches) == 0 {
		return nil
	}
	return matches[0]
}

func (h *EventHistory) Filter(event_pattern string, where func(event *BaseEvent) bool, options *EventHistoryFindOptions) []*BaseEvent {
	if event_pattern == "" {
		event_pattern = "*"
	}
	if where == nil {
		where = func(event *BaseEvent) bool { return true }
	}
	if options == nil {
		options = &EventHistoryFindOptions{}
	}
	past_enabled, past_window := normalizePast(options.Past)
	if !past_enabled {
		return []*BaseEvent{}
	}
	if options.Limit != nil && *options.Limit <= 0 {
		return []*BaseEvent{}
	}
	matches := func(event *BaseEvent) bool {
		if event_pattern != "*" && event.EventType != event_pattern {
			return false
		}
		if options.ChildOf != nil && !EventIsChildOfStatic(h, event, options.ChildOf) {
			return false
		}
		if !eventMatchesEquals(event, options.Equals) {
			return false
		}
		if !where(event) {
			return false
		}
		if past_window != nil {
			created_at, err := time.Parse(time.RFC3339Nano, event.EventCreatedAt)
			if err != nil {
				return false
			}
			if time.Since(created_at) > time.Duration(*past_window*float64(time.Second)) {
				return false
			}
		}
		return true
	}
	values := h.Values()
	out := []*BaseEvent{}
	for i := len(values) - 1; i >= 0; i-- {
		event := values[i]
		if matches(event) {
			out = append(out, event)
			if options.Limit != nil && len(out) >= *options.Limit {
				return out
			}
		}
	}
	return out
}

func EventIsChildOfStatic(h *EventHistory, event *BaseEvent, ancestor *BaseEvent) bool {
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
		parent := h.GetEvent(*parentID)
		if parent == nil {
			return false
		}
		parentID = parent.EventParentID
	}
	return false
}

func (h *EventHistory) RemoveEvent(event_id string) bool {
	h.mu.Lock()
	onRemove := h.onRemove
	if _, ok := h.events[event_id]; !ok {
		h.mu.Unlock()
		return false
	}
	delete(h.events, event_id)
	for i := len(h.order) - 1; i >= 0; i-- {
		if h.order[i] == event_id {
			h.order = append(h.order[:i], h.order[i+1:]...)
			break
		}
	}
	h.mu.Unlock()
	if onRemove != nil {
		onRemove(event_id)
	}
	return true
}

func (h *EventHistory) TrimEventHistory(is_event_complete func(event *BaseEvent) bool) int {
	h.mu.Lock()
	removedIDs := h.trimLocked(is_event_complete)
	onRemove := h.onRemove
	h.mu.Unlock()
	if onRemove != nil {
		for _, eventID := range removedIDs {
			onRemove(eventID)
		}
	}
	return len(removedIDs)
}

func (h *EventHistory) trimLocked(is_event_complete func(event *BaseEvent) bool) []string {
	if h.MaxHistorySize == nil {
		return nil
	}
	max := *h.MaxHistorySize
	overage := len(h.events) - max
	if overage <= 0 {
		return nil
	}
	if is_event_complete == nil {
		is_event_complete = func(event *BaseEvent) bool { return event.EventStatus == "completed" }
	}
	removedIDs := []string{}
	for overage > 0 {
		removed_any := false
		for i := 0; i < len(h.order) && overage > 0; i++ {
			eid := h.order[i]
			e := h.events[eid]
			if e == nil || !is_event_complete(e) {
				continue
			}
			delete(h.events, eid)
			h.order = append(h.order[:i], h.order[i+1:]...)
			i--
			overage--
			removedIDs = append(removedIDs, eid)
			removed_any = true
		}
		if removed_any {
			continue
		}
		break
	}
	return removedIDs
}
