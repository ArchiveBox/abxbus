package abxbus

import "encoding/json"

func NewTypedEvent[T any](eventType string, payload T) (*BaseEvent, error) {
	normalized := map[string]any{}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	if string(data) != "null" {
		if err := json.Unmarshal(data, &normalized); err != nil {
			return nil, err
		}
	}
	return NewBaseEvent(eventType, normalized), nil
}

func MustNewTypedEvent[T any](eventType string, payload T) *BaseEvent {
	event, err := NewTypedEvent(eventType, payload)
	if err != nil {
		panic(err)
	}
	return event
}

func EventPayloadAs[T any](event *BaseEvent) (T, error) {
	var payload T
	data, err := json.Marshal(event.Payload)
	if err != nil {
		return payload, err
	}
	err = json.Unmarshal(data, &payload)
	return payload, err
}

func EventResultAs[T any](result any) (T, error) {
	var typed T
	data, err := json.Marshal(result)
	if err != nil {
		return typed, err
	}
	err = json.Unmarshal(data, &typed)
	return typed, err
}
