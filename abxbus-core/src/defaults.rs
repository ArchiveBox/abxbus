use crate::{
    store::{BusRecord, EventRecord, EventResultRecord, HandlerRecord},
    types::{EventConcurrency, HandlerCompletion, HandlerConcurrency},
};

pub fn min_non_null(values: impl IntoIterator<Item = Option<f64>>) -> Option<f64> {
    values
        .into_iter()
        .flatten()
        .filter(|value| value.is_finite() && *value >= 0.0)
        .min_by(|left, right| left.total_cmp(right))
}

pub fn timeout_override(value: Option<f64>) -> Option<Option<f64>> {
    value.map(|value| {
        if value.is_finite() && value > 0.0 {
            Some(value)
        } else {
            None
        }
    })
}

pub fn resolve_event_concurrency(event: &EventRecord, bus: &BusRecord) -> EventConcurrency {
    event
        .event_concurrency
        .unwrap_or(bus.defaults.event_concurrency)
}

pub fn resolve_event_timeout(event: &EventRecord, bus: &BusRecord) -> Option<f64> {
    event
        .event_timeout
        .or(bus.defaults.event_timeout)
        .filter(|value| value.is_finite() && *value > 0.0)
}

pub fn resolve_event_slow_timeout(event: &EventRecord, bus: &BusRecord) -> Option<f64> {
    timeout_override(event.event_slow_timeout)
        .unwrap_or_else(|| timeout_override(bus.defaults.event_slow_timeout).unwrap_or(None))
}

pub fn resolve_handler_concurrency(
    event: &EventRecord,
    result: &EventResultRecord,
    handler: &HandlerRecord,
    bus: &BusRecord,
) -> HandlerConcurrency {
    result
        .handler_concurrency
        .or(handler.handler_concurrency)
        .or(event.event_handler_concurrency)
        .unwrap_or(bus.defaults.event_handler_concurrency)
}

pub fn resolve_handler_completion(
    event: &EventRecord,
    result: &EventResultRecord,
    handler: &HandlerRecord,
    bus: &BusRecord,
) -> HandlerCompletion {
    result
        .handler_completion
        .or(handler.handler_completion)
        .or(event.event_handler_completion)
        .unwrap_or(bus.defaults.event_handler_completion)
}

pub fn resolve_handler_timeout(
    event: &EventRecord,
    handler: &HandlerRecord,
    bus: &BusRecord,
) -> Option<f64> {
    handler
        .handler_timeout
        .or(event.event_handler_timeout)
        .or(bus.defaults.event_handler_timeout)
        .filter(|value| value.is_finite() && *value > 0.0)
}

pub fn resolve_effective_handler_timeout(
    event: &EventRecord,
    handler: &HandlerRecord,
    bus: &BusRecord,
) -> Option<f64> {
    min_non_null([
        resolve_handler_timeout(event, handler, bus),
        resolve_event_timeout(event, bus),
    ])
}

pub fn resolve_handler_slow_timeout(
    event: &EventRecord,
    handler: &HandlerRecord,
    bus: &BusRecord,
) -> Option<f64> {
    handler
        .handler_slow_timeout
        .map(|value| {
            if value.is_finite() && value > 0.0 {
                Some(value)
            } else {
                None
            }
        })
        .unwrap_or_else(|| {
            timeout_override(event.event_handler_slow_timeout).unwrap_or_else(|| {
                timeout_override(event.event_slow_timeout).unwrap_or_else(|| {
                    timeout_override(bus.defaults.event_handler_slow_timeout).unwrap_or_else(|| {
                        timeout_override(bus.defaults.event_slow_timeout).unwrap_or(None)
                    })
                })
            })
        })
}
