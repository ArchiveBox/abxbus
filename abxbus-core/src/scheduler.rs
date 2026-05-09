use std::collections::BTreeSet;

use crate::{
    defaults::{resolve_handler_completion, resolve_handler_concurrency},
    store::{
        BusRecord, EventRecord, EventResultRecord, EventRouteRecord, HandlerRecord, InMemoryStore,
    },
    types::{HandlerCompletion, HandlerConcurrency, ResultId, ResultStatus, RouteId},
};

pub fn is_first_mode_winner(result: &EventResultRecord) -> bool {
    result.status == ResultStatus::Completed
        && result.error.is_none()
        && result.result.as_ref().is_some_and(|value| !value.is_null())
        && !result.result_is_event_reference
}

pub fn route_has_first_winner(store: &InMemoryStore, route_id: &str) -> bool {
    store.route_results(route_id).any(is_first_mode_winner)
}

pub fn result_is_eligible(
    event: &EventRecord,
    route: &EventRouteRecord,
    result: &EventResultRecord,
    handler: &HandlerRecord,
    bus: &BusRecord,
    route_has_first_winner: bool,
) -> bool {
    if result.status != ResultStatus::Pending {
        return false;
    }
    if route_has_first_winner {
        return false;
    }

    match resolve_handler_concurrency(event, result, handler, bus) {
        HandlerConcurrency::Parallel => true,
        HandlerConcurrency::Serial => {
            result.handler_seq == route.handler_cursor && route.serial_handler_paused_by.is_none()
        }
    }
}

pub fn eligible_results(store: &InMemoryStore, route_id: &str) -> Vec<ResultId> {
    let Some(route) = store.routes.get(route_id) else {
        return Vec::new();
    };
    let Some(event) = store.events.get(&route.event_id) else {
        return Vec::new();
    };
    let Some(bus) = store.buses.get(&route.bus_id) else {
        return Vec::new();
    };
    let has_winner = resolved_completion_for_route(store, &route.route_id)
        .is_some_and(|completion| completion == HandlerCompletion::First)
        && route_has_first_winner(store, route_id);

    let mut eligible = Vec::new();
    for result in store.route_results(route_id) {
        let Some(handler) = store.handlers.get(&result.handler_id) else {
            continue;
        };
        if result_is_eligible(event, route, result, handler, bus, has_winner) {
            eligible.push(result.result_id.clone());
        }
    }
    eligible
}

pub fn resolved_completion_for_route(
    store: &InMemoryStore,
    route_id: &RouteId,
) -> Option<HandlerCompletion> {
    let route = store.routes.get(route_id)?;
    let event = store.events.get(&route.event_id)?;
    let bus = store.buses.get(&route.bus_id)?;
    let first_result = store.route_results(route_id).next()?;
    let handler = store.handlers.get(&first_result.handler_id)?;
    Some(resolve_handler_completion(
        event,
        first_result,
        handler,
        bus,
    ))
}

pub fn route_is_terminal(store: &InMemoryStore, route_id: &str) -> bool {
    let Some(route) = store.routes.get(route_id) else {
        return false;
    };
    if !matches!(
        route.status,
        crate::types::RouteStatus::Completed | crate::types::RouteStatus::Cancelled
    ) {
        return false;
    }
    let result_ids = store
        .results_by_route
        .get(route_id)
        .cloned()
        .unwrap_or_default();
    if result_ids.is_empty() {
        let Some(event) = store.events.get(&route.event_id) else {
            return false;
        };
        if store
            .handlers_for_bus(&route.bus_id)
            .into_iter()
            .any(|handler| {
                handler.event_pattern == "*" || handler.event_pattern == event.event_type
            })
        {
            return false;
        }
    }
    result_ids.iter().all(|result_id| {
        store
            .results
            .get(result_id)
            .is_some_and(|result| result.status.is_terminal())
    })
}

pub fn event_can_complete(store: &InMemoryStore, event_id: &str) -> bool {
    if !store.events.contains_key(event_id) {
        return false;
    }
    let route_ids = store
        .routes_by_event
        .get(event_id)
        .cloned()
        .unwrap_or_default();
    if !route_ids
        .iter()
        .all(|route_id| route_is_terminal(store, route_id))
    {
        return false;
    }
    blocking_children_complete_for_event(store, event_id)
}

pub fn blocking_children_complete_for_event(store: &InMemoryStore, event_id: &str) -> bool {
    let Some(event) = store.events.get(event_id) else {
        return false;
    };
    blocking_children_complete(store, event, &mut BTreeSet::new())
}

fn blocking_children_complete(
    store: &InMemoryStore,
    event: &EventRecord,
    visited: &mut BTreeSet<String>,
) -> bool {
    if !visited.insert(event.event_id.clone()) {
        return true;
    }

    for route_id in store
        .routes_by_event
        .get(&event.event_id)
        .into_iter()
        .flatten()
    {
        for result_id in store.results_by_route.get(route_id).into_iter().flatten() {
            let Some(result) = store.results.get(result_id) else {
                continue;
            };
            for child_id in &result.event_children {
                let Some(child) = store.events.get(child_id) else {
                    continue;
                };
                if !child.event_blocks_parent_completion {
                    continue;
                }
                if child.event_status != crate::types::EventStatus::Completed {
                    return false;
                }
                if !blocking_children_complete(store, child, visited) {
                    return false;
                }
            }
        }
    }
    true
}
