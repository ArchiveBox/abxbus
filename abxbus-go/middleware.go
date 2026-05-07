package abxbus

type EventBusMiddleware interface {
	OnEventChange(eventbus *EventBus, event *BaseEvent, status string)
	OnEventResultChange(eventbus *EventBus, event *BaseEvent, eventResult *EventResult, status string)
}

type EventBusMiddlewareBase struct{}

func (EventBusMiddlewareBase) OnEventChange(eventbus *EventBus, event *BaseEvent, status string) {}

func (EventBusMiddlewareBase) OnEventResultChange(eventbus *EventBus, event *BaseEvent, eventResult *EventResult, status string) {
}
