package bubus

import (
	"reflect"
	"sync"
	"time"
)

type EventHistory struct {
	MaxHistorySize *int
	MaxHistoryDrop bool
	events         map[string]*BaseEvent
	order          []string
	mu             sync.RWMutex
}

type EventHistoryFindOptions struct {
	Past    any
	ChildOf *BaseEvent
	Equals  map[string]any
}

func NewEventHistory(max_history_size *int, max_history_drop bool) *EventHistory {
	if max_history_size == nil {
		max_history_size = ptr(100)
	}
	return &EventHistory{MaxHistorySize: max_history_size, MaxHistoryDrop: max_history_drop, events: map[string]*BaseEvent{}, order: []string{}}
}

func (h *EventHistory) AddEvent(event *BaseEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.events[event.EventID]; !exists {
		h.order = append(h.order, event.EventID)
	}
	h.events[event.EventID] = event
	h.trimLocked(nil)
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
	defer h.mu.Unlock()
	h.events = map[string]*BaseEvent{}
	h.order = []string{}
}

func (h *EventHistory) Find(event_pattern string, where func(event *BaseEvent) bool, options *EventHistoryFindOptions) *BaseEvent {
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
		return nil
	}
	matches := func(event *BaseEvent) bool {
		if event_pattern != "*" && event.EventType != event_pattern {
			return false
		}
		if options.ChildOf != nil && !EventIsChildOfStatic(h, event, options.ChildOf) {
			return false
		}
		if len(options.Equals) > 0 {
			for key, value := range options.Equals {
				switch key {
				case "event_status":
					if !reflect.DeepEqual(event.EventStatus, value) {
						return false
					}
				case "event_type":
					if !reflect.DeepEqual(event.EventType, value) {
						return false
					}
				default:
					payload_v, ok := event.Payload[key]
					if !ok || !reflect.DeepEqual(payload_v, value) {
						return false
					}
				}
			}
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
	for _, event := range h.Values() {
		if matches(event) {
			return event
		}
	}
	return nil
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
	defer h.mu.Unlock()
	if _, ok := h.events[event_id]; !ok {
		return false
	}
	delete(h.events, event_id)
	for i := len(h.order) - 1; i >= 0; i-- {
		if h.order[i] == event_id {
			h.order = append(h.order[:i], h.order[i+1:]...)
			break
		}
	}
	return true
}

func (h *EventHistory) TrimEventHistory(is_event_complete func(event *BaseEvent) bool) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.trimLocked(is_event_complete)
}

func (h *EventHistory) trimLocked(is_event_complete func(event *BaseEvent) bool) int {
	if h.MaxHistorySize == nil {
		return 0
	}
	max := *h.MaxHistorySize
	overage := len(h.events) - max
	if overage <= 0 {
		return 0
	}
	if is_event_complete == nil {
		is_event_complete = func(event *BaseEvent) bool { return event.EventStatus == "completed" }
	}
	removed := 0
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
			removed++
			removed_any = true
		}
		if removed_any {
			continue
		}
		if !h.MaxHistoryDrop || len(h.order) == 0 {
			break
		}
		eid := h.order[0]
		delete(h.events, eid)
		h.order = h.order[1:]
		overage--
		removed++
	}
	return removed
}
