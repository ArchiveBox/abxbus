use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{
    defaults::{
        resolve_effective_handler_timeout, resolve_event_concurrency, resolve_event_slow_timeout,
        resolve_event_timeout, resolve_handler_slow_timeout, resolve_handler_timeout,
    },
    envelope::EventEnvelope,
    id::uuid_v7_string,
    locks::{event_lock_resource_for_route, LockAcquire, LockLease},
    protocol::{
        CompactInvokeHandler, CoreEventResultSnapshot, CoreEventSnapshot, CorePatch,
        CoreToHostMessage, HostToCoreMessage,
    },
    scheduler::{
        eligible_results, event_can_complete, is_first_mode_winner, resolved_completion_for_route,
        result_is_eligible,
    },
    store::{
        BusRecord, EventRecord, EventResultRecord, EventRouteRecord, HandlerRecord, InMemoryStore,
        InvocationState,
    },
    time::{now_timestamp, timestamp_is_past, timestamp_plus_seconds},
    types::{
        CancelReason, CoreError, CoreErrorKind, CoreErrorState, EventConcurrency, EventId,
        EventStatus, HandlerId, InvocationId, ResultId, ResultStatus, RouteId, RouteStatus,
        Timestamp,
    },
};

const ROUTE_INVOCATION_SLICE_LIMIT: usize = 65_536;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InvokeHandler {
    pub invocation_id: InvocationId,
    pub result_id: ResultId,
    pub route_id: RouteId,
    pub event_id: String,
    pub bus_id: String,
    pub handler_id: String,
    pub fence: u64,
    pub deadline_at: Option<String>,
    pub event_deadline_at: Option<String>,
    #[serde(default)]
    pub route_paused: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_snapshot: Option<EventEnvelope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_snapshot: Option<EventResultRecord>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueJumpContext {
    pub parent_invocation_id: InvocationId,
    pub initiating_bus_id: String,
    pub borrowed_event_locks: Vec<LockLease>,
    pub depth: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum HandlerOutcome {
    Completed {
        value: Value,
        result_is_event_reference: bool,
        #[serde(default)]
        result_is_undefined: bool,
    },
    Errored {
        error: CoreError,
    },
    Cancelled {
        reason: CancelReason,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RouteStep {
    NoWork {
        route_id: String,
    },
    WaitingForEventLock {
        route_id: String,
        position: usize,
    },
    InvokeHandlers {
        route_id: String,
        invocations: Vec<InvokeHandler>,
    },
    RouteCompleted {
        route_id: String,
        event_id: String,
        event_completed: bool,
    },
}

fn deadline_index_insert(
    index: &mut BTreeMap<Timestamp, BTreeSet<String>>,
    deadline: Timestamp,
    id: String,
) {
    index.entry(deadline).or_default().insert(id);
}

fn deadline_index_remove(
    index: &mut BTreeMap<Timestamp, BTreeSet<String>>,
    deadline: &str,
    id: &str,
) {
    let mut should_remove_key = false;
    if let Some(ids) = index.get_mut(deadline) {
        ids.remove(id);
        should_remove_key = ids.is_empty();
    }
    if should_remove_key {
        index.remove(deadline);
    }
}

fn compact_invoke_for_same_host(
    mut invocation: InvokeHandler,
    compact_response: bool,
) -> InvokeHandler {
    if compact_response {
        invocation.event_snapshot = None;
        invocation.result_snapshot = None;
    }
    invocation
}

#[derive(Default)]
pub struct Core {
    pub store: InMemoryStore,
    pub locks: crate::locks::LockManager,
    pub host_inboxes: BTreeMap<String, VecDeque<InvokeHandler>>,
    pub host_notifications: BTreeMap<String, VecDeque<CoreToHostMessage>>,
    pub active_hosts: BTreeSet<String>,
    pub host_session_counts: BTreeMap<String, usize>,
    pub event_slow_warnings_sent: BTreeSet<String>,
    pub result_deadline_index: BTreeMap<Timestamp, BTreeSet<String>>,
    pub result_slow_deadline_index: BTreeMap<Timestamp, BTreeSet<String>>,
    pub event_deadline_index: BTreeMap<Timestamp, BTreeSet<String>>,
    pub event_slow_deadline_index: BTreeMap<Timestamp, BTreeSet<String>>,
    pub result_deadlines_by_id: BTreeMap<String, (Option<Timestamp>, Option<Timestamp>)>,
    pub event_deadlines_by_id: BTreeMap<String, (Option<Timestamp>, Option<Timestamp>)>,
    pub paused_serial_routes_by_bus: BTreeMap<String, BTreeSet<String>>,
}

impl Core {
    pub fn insert_bus(&mut self, bus: BusRecord) {
        let bus_id = bus.bus_id.clone();
        self.store
            .append_patch(CorePatch::BusRegistered { bus: bus.clone() });
        self.store.insert_bus(bus);
        self.apply_history_policy_for_bus(&bus_id);
    }

    pub fn insert_handler(&mut self, handler: HandlerRecord) {
        self.store.append_patch(CorePatch::HandlerRegistered {
            handler: handler.clone(),
        });
        self.store.insert_handler(handler);
    }

    pub fn import_bus_snapshot(
        &mut self,
        bus: BusRecord,
        handlers: Vec<HandlerRecord>,
        events: Vec<Value>,
        pending_event_ids: Vec<String>,
    ) -> Result<(), CoreErrorState> {
        self.insert_bus(bus.clone());
        for handler in handlers {
            self.insert_handler(handler);
        }
        for raw_event in events {
            let Value::Object(mut event_map) = raw_event else {
                continue;
            };
            let raw_results = event_map
                .remove("event_results")
                .and_then(|value| value.as_object().cloned())
                .unwrap_or_default();
            event_map.remove("event_pending_bus_count");
            let event_envelope: EventEnvelope = serde_json::from_value(Value::Object(event_map))
                .map_err(|error| CoreErrorState::InvalidEnvelope(error.to_string()))?;
            let mut event_record = event_envelope
                .into_record()
                .map_err(|error| CoreErrorState::InvalidEnvelope(error.to_string()))?;
            if pending_event_ids
                .iter()
                .any(|event_id| event_id == &event_record.event_id)
            {
                event_record.event_status = EventStatus::Pending;
                event_record.event_started_at = None;
                event_record.event_completed_at = None;
            }
            let event_id = event_record.event_id.clone();
            self.remove_existing_routes_for_event_bus(&event_id, &bus.bus_id);
            self.store.insert_event(event_record.clone());
            self.store.add_event_to_bus_history(&bus.bus_id, &event_id);

            if !pending_event_ids
                .iter()
                .any(|pending_id| pending_id == &event_id)
                && raw_results.is_empty()
            {
                continue;
            }

            let route_id = uuid_v7_string();
            let route_seq = self
                .store
                .routes_by_event
                .get(&event_id)
                .map(|routes| routes.len() as u64)
                .unwrap_or(0);
            let mut route = EventRouteRecord {
                route_id: route_id.clone(),
                event_id: event_id.clone(),
                bus_id: bus.bus_id.clone(),
                route_seq,
                status: RouteStatus::Pending,
                handler_cursor: 0,
                serial_handler_paused_by: None,
                event_lock_lease: None,
            };
            let mut imported_results =
                self.import_event_results_for_route(&event_record, &bus, &route_id, raw_results);
            if pending_event_ids
                .iter()
                .any(|pending_id| pending_id == &event_id)
            {
                self.add_missing_imported_pending_results(
                    &event_record,
                    &bus,
                    &route_id,
                    &mut imported_results,
                );
            }
            imported_results.sort_by_key(|result| result.handler_seq);
            if !imported_results.is_empty() {
                route.handler_cursor = next_imported_handler_cursor(&imported_results);
            }
            self.store.insert_route(route);
            for result in imported_results {
                self.store.insert_result(result);
            }
            if self.route_ready_to_complete(&route_id) {
                if let Some(route) = self.store.routes.get_mut(&route_id) {
                    route.status = RouteStatus::Completed;
                }
                self.remove_route_from_bus_schedule_index(&route_id);
                let _ = self.complete_event_if_done(&event_id)?;
            }
        }
        self.apply_history_policy_for_bus(&bus.bus_id);
        Ok(())
    }

    fn remove_existing_routes_for_event_bus(&mut self, event_id: &str, bus_id: &str) {
        let route_ids = self
            .store
            .routes_by_event
            .get(event_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|route_id| {
                self.store
                    .routes
                    .get(route_id)
                    .is_some_and(|route| route.bus_id == bus_id)
            })
            .collect::<BTreeSet<_>>();
        if route_ids.is_empty() {
            return;
        }

        for route_id in &route_ids {
            let _ = self.release_route_event_lock(route_id);
            self.locks.remove_owner(&format!("route:{route_id}"));
            self.remove_results_for_route(route_id);
            self.store.routes.remove(route_id);
        }
        if let Some(ids) = self.store.routes_by_event.get_mut(event_id) {
            ids.retain(|route_id| !route_ids.contains(route_id));
            if ids.is_empty() {
                self.store.routes_by_event.remove(event_id);
            }
        }
        if let Some(ids) = self.store.routes_by_bus.get_mut(bus_id) {
            ids.retain(|route_id| !route_ids.contains(route_id));
            if ids.is_empty() {
                self.store.routes_by_bus.remove(bus_id);
            }
        }
        for inbox in self.host_inboxes.values_mut() {
            inbox.retain(|invocation| !route_ids.contains(&invocation.route_id));
        }
    }

    fn remove_existing_routes_for_event(&mut self, event_id: &str) {
        let route_ids = self
            .store
            .routes_by_event
            .get(event_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect::<BTreeSet<_>>();
        if route_ids.is_empty() {
            return;
        }

        let bus_ids = route_ids
            .iter()
            .filter_map(|route_id| self.store.routes.get(route_id))
            .map(|route| route.bus_id.clone())
            .collect::<BTreeSet<_>>();

        for route_id in &route_ids {
            let _ = self.release_route_event_lock(route_id);
            self.locks.remove_owner(&format!("route:{route_id}"));
            self.remove_results_for_route(route_id);
            self.store.routes.remove(route_id);
        }
        self.store.routes_by_event.remove(event_id);
        for bus_id in bus_ids {
            if let Some(ids) = self.store.routes_by_bus.get_mut(&bus_id) {
                ids.retain(|route_id| !route_ids.contains(route_id));
                if ids.is_empty() {
                    self.store.routes_by_bus.remove(&bus_id);
                }
            }
        }
        for inbox in self.host_inboxes.values_mut() {
            inbox.retain(|invocation| !route_ids.contains(&invocation.route_id));
        }
    }

    fn import_event_results_for_route(
        &self,
        event: &EventRecord,
        bus: &BusRecord,
        route_id: &str,
        raw_results: Map<String, Value>,
    ) -> Vec<EventResultRecord> {
        let mut handler_order = self
            .store
            .handlers_for_bus(&bus.bus_id)
            .into_iter()
            .enumerate()
            .filter(|handler| {
                handler.1.event_pattern == "*" || handler.1.event_pattern == event.event_type
            })
            .collect::<Vec<_>>();
        handler_order.sort_by_key(|(idx, handler)| (handler.event_pattern == "*", *idx));
        let handler_order = handler_order
            .into_iter()
            .map(|(_, handler)| handler.handler_id.clone())
            .collect::<Vec<_>>();
        let mut imported = Vec::new();
        for (fallback_handler_id, raw_result) in raw_results {
            let Value::Object(result_map) = raw_result else {
                continue;
            };
            let handler_id = result_map
                .get("handler_id")
                .and_then(Value::as_str)
                .unwrap_or(&fallback_handler_id)
                .to_string();
            let Some(handler) = self.store.handlers.get(&handler_id) else {
                continue;
            };
            let handler_seq = handler_order
                .iter()
                .position(|candidate| candidate == &handler_id)
                .unwrap_or(imported.len());
            let result_id = result_map
                .get("id")
                .or_else(|| result_map.get("result_id"))
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(uuid_v7_string);
            let status = match result_map.get("status").and_then(Value::as_str) {
                Some("completed") => ResultStatus::Completed,
                Some("error") => ResultStatus::Error,
                Some("cancelled") => ResultStatus::Cancelled,
                _ => ResultStatus::Pending,
            };
            let result_value = result_map
                .get("result")
                .cloned()
                .filter(|value| !value.is_null());
            let error = result_map
                .get("error")
                .cloned()
                .filter(|value| !value.is_null())
                .and_then(|value| serde_json::from_value::<CoreError>(value).ok());
            let timeout = resolve_effective_handler_timeout(event, handler, bus);
            let slow_timeout = resolve_handler_slow_timeout(event, handler, bus);
            imported.push(EventResultRecord {
                result_id,
                event_id: event.event_id.clone(),
                route_id: route_id.to_string(),
                bus_id: bus.bus_id.clone(),
                handler_id,
                handler_seq,
                status,
                result: result_value,
                result_is_event_reference: false,
                result_set: result_map.contains_key("result"),
                result_is_undefined: false,
                error,
                started_at: result_map
                    .get("started_at")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                completed_at: result_map
                    .get("completed_at")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                timeout,
                slow_timeout,
                slow_warning_at: None,
                slow_warning_sent: false,
                event_children: result_map
                    .get("event_children")
                    .and_then(Value::as_array)
                    .map(|children| {
                        children
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_string)
                            .collect()
                    })
                    .unwrap_or_default(),
                handler_concurrency: handler.handler_concurrency,
                handler_completion: handler.handler_completion,
                invocation: None,
            });
        }
        imported
    }

    fn add_missing_imported_pending_results(
        &self,
        event: &EventRecord,
        bus: &BusRecord,
        route_id: &str,
        imported_results: &mut Vec<EventResultRecord>,
    ) {
        let existing_handler_ids = imported_results
            .iter()
            .map(|result| result.handler_id.clone())
            .collect::<BTreeSet<_>>();
        let mut handlers = self
            .store
            .handlers_for_bus(&bus.bus_id)
            .into_iter()
            .enumerate()
            .filter(|handler| {
                handler.1.event_pattern == "*" || handler.1.event_pattern == event.event_type
            })
            .map(|(idx, handler)| (idx, handler.clone()))
            .collect::<Vec<_>>();
        handlers.sort_by_key(|(idx, handler)| (handler.event_pattern == "*", *idx));
        for (handler_seq, (_, handler)) in handlers.iter().enumerate() {
            if existing_handler_ids.contains(&handler.handler_id) {
                continue;
            }
            let timeout = resolve_effective_handler_timeout(event, handler, bus);
            let slow_timeout = resolve_handler_slow_timeout(event, handler, bus);
            imported_results.push(EventResultRecord::pending(
                uuid_v7_string(),
                event.event_id.clone(),
                route_id.to_string(),
                bus.bus_id.clone(),
                handler.handler_id.clone(),
                handler_seq,
                timeout,
                slow_timeout,
            ));
        }
    }

    pub fn unregister_handler(&mut self, handler_id: &str) {
        if let Some(handler) = self.store.handlers.remove(handler_id) {
            if let Some(ids) = self.store.handlers_by_bus.get_mut(&handler.bus_id) {
                ids.retain(|candidate| candidate != handler_id);
                if ids.is_empty() && !self.store.buses.contains_key(&handler.bus_id) {
                    self.store.handlers_by_bus.remove(&handler.bus_id);
                }
            }
            self.store.append_patch(CorePatch::HandlerUnregistered {
                handler_id: handler_id.to_string(),
            });
        }
    }

    pub fn unregister_bus(&mut self, bus_id: &str) {
        let mut affected_event_ids = BTreeSet::new();
        self.paused_serial_routes_by_bus.remove(bus_id);
        let route_ids = self
            .store
            .routes_by_bus
            .remove(bus_id)
            .unwrap_or_default()
            .into_iter()
            .collect::<BTreeSet<_>>();

        for route_id in &route_ids {
            if let Some(event_id) = self
                .store
                .routes
                .get(route_id)
                .map(|route| route.event_id.clone())
            {
                self.compact_completed_event_routes(&event_id);
            }
            let _ = self.release_route_event_lock(route_id);
            self.locks.remove_owner(&format!("route:{route_id}"));
            if let Some(route) = self.store.routes.remove(route_id) {
                affected_event_ids.insert(route.event_id.clone());
                if let Some(ids) = self.store.routes_by_event.get_mut(&route.event_id) {
                    ids.retain(|candidate| candidate != route_id);
                    if ids.is_empty() {
                        self.store.routes_by_event.remove(&route.event_id);
                    }
                }
            }
            self.remove_results_for_route(route_id);
        }

        if let Some(history) = self.store.bus_event_history.remove(bus_id) {
            affected_event_ids.extend(history);
        }
        if let Some(members) = self.store.bus_event_history_members.remove(bus_id) {
            affected_event_ids.extend(members);
        }
        if let Some(emissions) = self.store.bus_event_emissions.remove(bus_id) {
            affected_event_ids.extend(emissions);
        }

        if let Some(handler_ids) = self.store.handlers_by_bus.remove(bus_id) {
            for handler_id in handler_ids {
                self.store.handlers.remove(&handler_id);
            }
        }
        self.store.buses.remove(bus_id);
        for inbox in self.host_inboxes.values_mut() {
            inbox.retain(|invocation| {
                invocation.bus_id != bus_id && !route_ids.contains(&invocation.route_id)
            });
        }
        for event_id in affected_event_ids {
            self.prune_event_if_unreferenced(&event_id);
        }
        self.clear_patch_log_if_store_empty();
    }

    fn clear_patch_log_if_store_empty(&mut self) {
        if self.store.buses.is_empty()
            && self.store.handlers.is_empty()
            && self.store.handlers_by_bus.is_empty()
            && self.store.events.is_empty()
            && self.store.routes.is_empty()
            && self.store.routes_by_event.is_empty()
            && self.store.routes_by_bus.is_empty()
            && self.store.bus_event_history.is_empty()
            && self.store.bus_event_history_members.is_empty()
            && self.store.bus_event_emissions.is_empty()
            && self.store.results.is_empty()
            && self.store.results_by_route.is_empty()
        {
            self.store.clear_patches();
            self.host_inboxes.clear();
            self.event_slow_warnings_sent.clear();
            self.result_deadline_index.clear();
            self.result_slow_deadline_index.clear();
            self.event_deadline_index.clear();
            self.event_slow_deadline_index.clear();
            self.result_deadlines_by_id.clear();
            self.event_deadlines_by_id.clear();
            self.paused_serial_routes_by_bus.clear();
            self.locks = crate::locks::LockManager::default();
        }
    }

    fn remove_results_for_route(&mut self, route_id: &str) {
        if let Some(result_ids) = self.store.results_by_route.remove(route_id) {
            for result_id in result_ids {
                self.clear_result_deadline_indexes(&result_id);
                self.store.results.remove(&result_id);
            }
        }
    }

    pub fn connect_host_session(&mut self, host_id: &str) {
        self.active_hosts.insert(host_id.to_string());
        *self
            .host_session_counts
            .entry(host_id.to_string())
            .or_default() += 1;
    }

    pub fn disconnect_host_session(&mut self, host_id: &str) -> bool {
        let Some(count) = self.host_session_counts.get_mut(host_id) else {
            return false;
        };
        if *count > 1 {
            *count -= 1;
            return false;
        }
        self.host_session_counts.remove(host_id);
        self.disconnect_host(host_id);
        true
    }

    pub fn close_transport_session(&mut self, host_id: &str) -> bool {
        let Some(count) = self.host_session_counts.get_mut(host_id) else {
            return false;
        };
        if *count > 1 {
            *count -= 1;
            return false;
        }
        self.host_session_counts.remove(host_id);
        self.active_hosts.remove(host_id);
        false
    }

    pub fn disconnect_host(&mut self, host_id: &str) {
        self.active_hosts.remove(host_id);
        if let Some(notifications) = self.host_notifications.get_mut(host_id) {
            notifications.push_back(CoreToHostMessage::Disconnected {
                host_id: host_id.to_string(),
            });
        }
        self.host_inboxes.remove(host_id);
        let handler_ids = self
            .store
            .handlers
            .values()
            .filter(|handler| handler.host_id == host_id)
            .map(|handler| handler.handler_id.clone())
            .collect::<Vec<_>>();
        for handler_id in &handler_ids {
            self.unregister_handler(handler_id);
        }
        let mut patches = Vec::new();
        let mut affected_results = Vec::new();
        for result in self.store.results.values_mut() {
            if !handler_ids
                .iter()
                .any(|handler_id| handler_id == &result.handler_id)
            {
                continue;
            }
            if result.status.is_terminal() {
                continue;
            }
            result.status = ResultStatus::Cancelled;
            result.error = Some(CoreError::new(
                CoreErrorKind::HandlerCancelled,
                format!("handler host disconnected: {host_id}"),
            ));
            result.completed_at = Some(now_timestamp());
            result.invocation = None;
            patches.push(CorePatch::ResultCancelled {
                result: result.clone(),
                reason: CancelReason::HostDisconnected,
            });
            affected_results.push(result.result_id.clone());
        }
        for result_id in affected_results {
            self.clear_result_deadline_indexes(&result_id);
            self.advance_route_after_result(&result_id);
        }
        for patch in patches {
            self.store.append_patch(patch);
        }
    }

    pub fn patch_messages_since(&self, last_patch_seq: u64) -> Vec<CoreToHostMessage> {
        self.store
            .patches_since(last_patch_seq)
            .into_iter()
            .map(|(patch_seq, patch)| CoreToHostMessage::Patch { patch_seq, patch })
            .collect()
    }

    pub fn patch_messages_since_optional(
        &self,
        last_patch_seq: Option<u64>,
    ) -> Vec<CoreToHostMessage> {
        last_patch_seq
            .map(|last_patch_seq| self.patch_messages_since(last_patch_seq))
            .unwrap_or_default()
    }

    pub fn drain_patches(&mut self) -> Vec<CorePatch> {
        self.store
            .patches
            .drain(..)
            .map(|(_patch_seq, patch)| patch)
            .collect()
    }

    pub fn latest_patch_seq(&self) -> u64 {
        self.store.latest_patch_seq()
    }

    pub fn event_snapshot(
        &self,
        event_id: &str,
    ) -> Result<Option<CoreEventSnapshot>, CoreErrorState> {
        self.store
            .events
            .get(event_id)
            .map(|event| self.core_event_snapshot(event))
            .transpose()
    }

    pub fn list_event_snapshots(
        &self,
        event_pattern: &str,
        limit: Option<usize>,
    ) -> Result<Vec<CoreEventSnapshot>, CoreErrorState> {
        let mut events = Vec::new();
        for event in self.store.events.values().rev() {
            if event_pattern != "*" && event.event_type != event_pattern {
                continue;
            }
            events.push(self.core_event_snapshot(event)?);
            if limit.is_some_and(|limit| events.len() >= limit) {
                break;
            }
        }
        Ok(events)
    }

    pub fn list_bus_event_snapshots(
        &self,
        bus_id: &str,
        event_pattern: &str,
        limit: Option<usize>,
    ) -> Result<Vec<CoreEventSnapshot>, CoreErrorState> {
        let mut events = Vec::new();
        let Some(history) = self.store.bus_event_history.get(bus_id) else {
            return Ok(events);
        };
        for event_id in history.iter().rev() {
            let Some(event) = self.store.events.get(event_id) else {
                continue;
            };
            if event_pattern != "*" && event.event_type != event_pattern {
                continue;
            }
            events.push(self.core_event_snapshot(event)?);
            if limit.is_some_and(|limit| events.len() >= limit) {
                break;
            }
        }
        Ok(events)
    }

    pub fn list_event_ids(
        &self,
        event_pattern: &str,
        limit: Option<usize>,
        statuses: Option<&[EventStatus]>,
    ) -> Vec<EventId> {
        let mut event_ids = Vec::new();
        for event in self.store.events.values().rev() {
            if event_pattern != "*" && event.event_type != event_pattern {
                continue;
            }
            if statuses.is_some_and(|statuses| !statuses.contains(&event.event_status)) {
                continue;
            }
            event_ids.push(event.event_id.clone());
            if limit.is_some_and(|limit| event_ids.len() >= limit) {
                break;
            }
        }
        event_ids
    }

    pub fn list_bus_event_ids(
        &self,
        bus_id: &str,
        event_pattern: &str,
        limit: Option<usize>,
        statuses: Option<&[EventStatus]>,
    ) -> Vec<EventId> {
        let mut event_ids = Vec::new();
        let Some(history) = self.store.bus_event_history.get(bus_id) else {
            return event_ids;
        };
        for event_id in history.iter().rev() {
            let Some(event) = self.store.events.get(event_id) else {
                continue;
            };
            if event_pattern != "*" && event.event_type != event_pattern {
                continue;
            }
            if statuses.is_some_and(|statuses| !statuses.contains(&event.event_status)) {
                continue;
            }
            event_ids.push(event_id.clone());
            if limit.is_some_and(|limit| event_ids.len() >= limit) {
                break;
            }
        }
        event_ids
    }

    pub fn unseen_bus_event_snapshots(
        &self,
        bus_id: &str,
        event_pattern: &str,
        seen_event_ids: &BTreeSet<EventId>,
        after_event_id: Option<&str>,
        after_created_at: Option<&str>,
    ) -> Result<Vec<CoreEventSnapshot>, CoreErrorState> {
        let bus = self
            .store
            .buses
            .get(bus_id)
            .ok_or_else(|| CoreErrorState::MissingBus(bus_id.to_string()))?;
        let Some(emissions) = self.store.bus_event_emissions.get(bus_id) else {
            return Ok(Vec::new());
        };
        let mut snapshots = Vec::new();
        let mut after_seen = after_event_id.is_none();
        for event_id in emissions {
            if !after_seen {
                if after_event_id.is_some_and(|after_id| event_id == after_id) {
                    after_seen = true;
                }
                continue;
            }
            let Some(event) = self.store.events.get(event_id) else {
                continue;
            };
            if after_created_at.is_some_and(|cutoff| event.event_created_at.as_str() < cutoff) {
                continue;
            }
            if after_event_id.is_some_and(|after_id| {
                after_created_at.is_some_and(|cutoff| event.event_created_at.as_str() == cutoff)
                    && event.event_id.as_str() <= after_id
            }) {
                continue;
            }
            if seen_event_ids.contains(event_id) && after_created_at.is_none() {
                continue;
            }
            if event_pattern != "*" && event.event_type != event_pattern {
                continue;
            }
            if !event.event_path.iter().any(|label| label == &bus.label) {
                continue;
            }
            snapshots.push(self.core_event_snapshot(event)?);
        }
        Ok(snapshots)
    }

    fn core_event_snapshot(
        &self,
        event: &EventRecord,
    ) -> Result<CoreEventSnapshot, CoreErrorState> {
        let mut event_results = event.completed_result_snapshots.clone();
        event_results.extend(
            self.store
                .routes_by_event
                .get(&event.event_id)
                .into_iter()
                .flatten()
                .filter_map(|route_id| self.store.results_by_route.get(route_id))
                .flatten()
                .filter_map(|result_id| self.store.results.get(result_id))
                .filter_map(|result| self.core_result_snapshot(result))
                .map(|result| (result.handler_id.clone(), result))
                .collect::<BTreeMap<_, _>>(),
        );
        Ok(CoreEventSnapshot {
            event: EventEnvelope::from_record(event)
                .map_err(|error| CoreErrorState::InvalidEnvelope(error.to_string()))?,
            event_results,
        })
    }

    fn core_result_snapshot(&self, result: &EventResultRecord) -> Option<CoreEventResultSnapshot> {
        let handler = self.store.handlers.get(&result.handler_id)?;
        let bus = self.store.buses.get(&result.bus_id)?;
        let status = match result.status {
            ResultStatus::Pending => "pending",
            ResultStatus::Started => "started",
            ResultStatus::Completed => "completed",
            ResultStatus::Error | ResultStatus::Cancelled => "error",
        }
        .to_string();
        Some(CoreEventResultSnapshot {
            id: result.result_id.clone(),
            status,
            event_id: result.event_id.clone(),
            handler_id: result.handler_id.clone(),
            handler_seq: result.handler_seq,
            handler_name: handler.handler_name.clone(),
            handler_file_path: handler.handler_file_path.clone(),
            handler_timeout: handler.handler_timeout,
            handler_slow_timeout: handler.handler_slow_timeout,
            timeout: result.timeout,
            handler_registered_at: handler.handler_registered_at.clone(),
            handler_event_pattern: handler.event_pattern.clone(),
            eventbus_name: bus.name.clone(),
            eventbus_id: bus.bus_id.clone(),
            started_at: result.started_at.clone(),
            completed_at: result.completed_at.clone(),
            result_set: result.result_set,
            result_is_undefined: result.result_is_undefined,
            result_is_event_reference: result.result_is_event_reference,
            result: result.result.clone(),
            error: result.error.clone(),
            event_children: result.event_children.clone(),
        })
    }

    pub fn emit_to_bus(
        &mut self,
        event: EventRecord,
        bus_id: &str,
    ) -> Result<String, CoreErrorState> {
        self.emit_to_bus_with_patch(event, bus_id, true)
    }

    pub fn emit_to_bus_with_patch(
        &mut self,
        event: EventRecord,
        bus_id: &str,
        emit_patch: bool,
    ) -> Result<String, CoreErrorState> {
        let resolved_bus_id = self
            .store
            .buses
            .get(bus_id)
            .ok_or_else(|| CoreErrorState::MissingBus(bus_id.to_string()))?
            .bus_id
            .clone();
        let route_id = uuid_v7_string();
        let event_id = event.event_id.clone();
        self.ensure_bus_history_has_room(&resolved_bus_id, &event_id)?;
        let existing_route_ids = self
            .store
            .routes_by_event
            .get(&event_id)
            .cloned()
            .unwrap_or_default();
        let existing_routes_terminal = !existing_route_ids.is_empty()
            && existing_route_ids.iter().all(|route_id| {
                self.store.routes.get(route_id).is_none_or(|route| {
                    matches!(
                        route.status,
                        RouteStatus::Completed | RouteStatus::Cancelled
                    )
                })
            });
        let replace_terminal_event =
            existing_routes_terminal && event.event_status == EventStatus::Pending;
        if replace_terminal_event {
            self.remove_existing_routes_for_event(&event_id);
        }
        let existing_route_id = self
            .store
            .routes_by_event
            .get(&event_id)
            .into_iter()
            .flatten()
            .find(|route_id| {
                self.store.routes.get(*route_id).is_some_and(|route| {
                    route.bus_id == resolved_bus_id
                        && !matches!(
                            route.status,
                            RouteStatus::Completed | RouteStatus::Cancelled
                        )
                })
            })
            .cloned();
        if replace_terminal_event {
            self.store.insert_event(event);
        } else if let Some(existing) = self.store.events.get_mut(&event_id) {
            let mut clear_completed_deadlines = false;
            if existing.event_status == EventStatus::Completed
                && event.event_status == EventStatus::Pending
            {
                existing.event_status = EventStatus::Pending;
                existing.event_started_at = None;
                existing.event_completed_at = None;
                clear_completed_deadlines = true;
            }
            for label in &event.event_path {
                if !existing
                    .event_path
                    .iter()
                    .any(|existing_label| existing_label == label)
                {
                    existing.event_path.push(label.clone());
                }
            }
            if existing.event_parent_id.is_none() {
                existing.event_parent_id = event.event_parent_id.clone();
            }
            if existing.event_emitted_by_result_id.is_none() {
                existing.event_emitted_by_result_id = event.event_emitted_by_result_id.clone();
            }
            if existing.event_emitted_by_handler_id.is_none() {
                existing.event_emitted_by_handler_id = event.event_emitted_by_handler_id.clone();
            }
            if event.event_timeout.is_some() {
                existing.event_timeout = event.event_timeout;
            }
            if event.event_slow_timeout.is_some() {
                existing.event_slow_timeout = event.event_slow_timeout;
            }
            if event.event_concurrency.is_some() {
                existing.event_concurrency = event.event_concurrency;
            }
            if event.event_handler_timeout.is_some() {
                existing.event_handler_timeout = event.event_handler_timeout;
            }
            if event.event_handler_slow_timeout.is_some() {
                existing.event_handler_slow_timeout = event.event_handler_slow_timeout;
            }
            if event.event_handler_concurrency.is_some() {
                existing.event_handler_concurrency = event.event_handler_concurrency;
            }
            if event.event_handler_completion.is_some() {
                existing.event_handler_completion = event.event_handler_completion;
            }
            existing.event_blocks_parent_completion |= event.event_blocks_parent_completion;
            if clear_completed_deadlines {
                self.clear_event_deadline_indexes(&event_id);
                self.event_slow_warnings_sent.remove(&event_id);
            }
        } else {
            self.store.insert_event(event);
        }
        self.link_event_to_emitting_result(&event_id);
        if let Some(route_id) = existing_route_id {
            if self
                .store
                .events
                .get(&event_id)
                .is_some_and(|event| event.event_status == EventStatus::Started)
            {
                self.index_started_event_deadlines(&event_id);
            }
            return Ok(route_id);
        }
        let route_seq = self
            .store
            .routes_by_event
            .get(&event_id)
            .map(|routes| routes.len() as u64)
            .unwrap_or(0);
        let route = EventRouteRecord {
            route_id: route_id.clone(),
            event_id: event_id.clone(),
            bus_id: resolved_bus_id.clone(),
            route_seq,
            status: RouteStatus::Pending,
            handler_cursor: 0,
            serial_handler_paused_by: None,
            event_lock_lease: None,
        };
        self.store.insert_route(route.clone());
        self.store
            .add_event_to_bus_history(&resolved_bus_id, &event_id);
        self.store
            .record_event_emitted_to_bus(&resolved_bus_id, &event_id);
        if self
            .store
            .events
            .get(&event_id)
            .is_some_and(|event| event.event_status == EventStatus::Started)
        {
            self.index_started_event_deadlines(&event_id);
        }
        if emit_patch {
            let event_for_patch = self
                .store
                .events
                .get(&event_id)
                .cloned()
                .ok_or_else(|| CoreErrorState::MissingEvent(event_id.clone()))?;
            self.store.append_patch(CorePatch::EventEmitted {
                event: event_for_patch,
                route,
            });
        }
        Ok(route_id)
    }

    fn ensure_bus_history_has_room(
        &self,
        bus_id: &str,
        event_id: &str,
    ) -> Result<(), CoreErrorState> {
        let Some(bus) = self.store.buses.get(bus_id) else {
            return Err(CoreErrorState::MissingBus(bus_id.to_string()));
        };
        let Some(max_history_size) = bus.max_history_size else {
            return Ok(());
        };
        if max_history_size == 0 {
            return Ok(());
        }
        if bus.max_history_drop {
            return Ok(());
        }
        let current = self
            .store
            .bus_event_history
            .get(bus_id)
            .map(|history| history.len())
            .unwrap_or(0);
        let already_visible = self
            .store
            .bus_event_history
            .get(bus_id)
            .is_some_and(|history| history.iter().any(|existing| existing == event_id));
        if !already_visible && current >= max_history_size {
            return Err(CoreErrorState::HistoryLimitReached {
                current,
                max: max_history_size,
            });
        }
        Ok(())
    }

    fn apply_history_policy_for_bus(&mut self, bus_id: &str) {
        let Some(bus) = self.store.buses.get(bus_id) else {
            return;
        };
        let Some(max_history_size) = bus.max_history_size else {
            return;
        };
        if !bus.max_history_drop && max_history_size > 0 {
            return;
        }

        loop {
            let should_pop_front = {
                let Some(history) = self.store.bus_event_history.get(bus_id) else {
                    break;
                };
                if history.len() <= max_history_size {
                    break;
                }
                let Some(event_id) = history.front() else {
                    break;
                };
                self.store
                    .events
                    .get(event_id)
                    .is_some_and(|event| event.event_status == EventStatus::Completed)
            };
            if !should_pop_front {
                break;
            }

            let Some(event_id) = self.store.pop_front_bus_history_event(bus_id) else {
                break;
            };
            self.compact_completed_event_routes(&event_id);
            self.prune_event_if_unreferenced(&event_id);
        }
    }

    fn prune_event_if_unreferenced(&mut self, event_id: &str) {
        if self
            .store
            .routes_by_event
            .get(event_id)
            .is_some_and(|route_ids| !route_ids.is_empty())
        {
            return;
        }
        if self
            .store
            .bus_event_history_members
            .values()
            .any(|members| members.contains(event_id))
        {
            return;
        }
        if !self
            .store
            .events
            .get(event_id)
            .is_some_and(|event| event.event_status == EventStatus::Completed)
        {
            return;
        }
        self.clear_event_deadline_indexes(event_id);
        self.event_slow_warnings_sent.remove(event_id);
        self.store.events.remove(event_id);
    }

    fn link_event_to_emitting_result(&mut self, event_id: &str) {
        let Some(parent_result_id) = self
            .store
            .events
            .get(event_id)
            .and_then(|event| event.event_emitted_by_result_id.clone())
        else {
            return;
        };
        let Some(result) = self.store.results.get_mut(&parent_result_id) else {
            return;
        };
        if !result
            .event_children
            .iter()
            .any(|child_event_id| child_event_id == event_id)
        {
            result.event_children.push(event_id.to_string());
        }
    }

    pub fn handle_host_message(
        &mut self,
        msg: HostToCoreMessage,
    ) -> Result<Vec<CoreToHostMessage>, CoreErrorState> {
        self.handle_host_message_for_host_since("host", msg, Some(0))
    }

    pub fn handle_host_message_for_host(
        &mut self,
        session_id: &str,
        msg: HostToCoreMessage,
    ) -> Result<Vec<CoreToHostMessage>, CoreErrorState> {
        self.handle_host_message_for_host_since(session_id, msg, Some(0))
    }

    pub fn handle_host_message_for_host_since(
        &mut self,
        session_id: &str,
        msg: HostToCoreMessage,
        last_patch_seq: Option<u64>,
    ) -> Result<Vec<CoreToHostMessage>, CoreErrorState> {
        let mut out = Vec::new();
        let mut compact_response = false;
        if matches!(
            &msg,
            HostToCoreMessage::ProcessNextRoute { .. }
                | HostToCoreMessage::ProcessRoute { .. }
                | HostToCoreMessage::AwaitEvent { .. }
                | HostToCoreMessage::QueueJumpEvent { .. }
                | HostToCoreMessage::Heartbeat
        ) {
            self.maintenance_tick();
        }
        match msg {
            HostToCoreMessage::DisconnectHost { host_id } => {
                let host_id = host_id.unwrap_or_else(|| session_id.to_string());
                self.host_session_counts.remove(&host_id);
                self.disconnect_host(&host_id);
                out.push(CoreToHostMessage::Disconnected { host_id });
            }
            HostToCoreMessage::CloseSession => {
                out.push(CoreToHostMessage::SessionClosed {
                    session_id: session_id.to_string(),
                });
            }
            HostToCoreMessage::StopCore => {
                out.push(CoreToHostMessage::CoreStopped);
            }
            HostToCoreMessage::Hello { .. } => {
                self.active_hosts.insert(session_id.to_string());
                out.push(CoreToHostMessage::HelloAck {
                    core_id: "abxbus-core-embedded".to_string(),
                });
            }
            HostToCoreMessage::RegisterBus { bus } => {
                self.active_hosts.insert(session_id.to_string());
                self.insert_bus(bus);
            }
            HostToCoreMessage::UnregisterBus { bus_id } => {
                self.active_hosts.insert(session_id.to_string());
                self.unregister_bus(&bus_id);
            }
            HostToCoreMessage::RegisterHandler { handler } => {
                self.active_hosts.insert(session_id.to_string());
                self.insert_handler(handler);
            }
            HostToCoreMessage::ImportBusSnapshot {
                bus,
                handlers,
                events,
                pending_event_ids,
            } => {
                self.active_hosts.insert(session_id.to_string());
                self.import_bus_snapshot(bus, handlers, events, pending_event_ids)?;
            }
            HostToCoreMessage::UnregisterHandler { handler_id } => {
                self.active_hosts.insert(session_id.to_string());
                self.unregister_handler(&handler_id);
            }
            HostToCoreMessage::EmitEvent {
                event,
                bus_id,
                defer_start,
                compact_response: compact,
                parent_invocation_id,
                block_parent_completion,
                pause_parent_route,
            } => {
                compact_response = compact;
                self.active_hosts.insert(session_id.to_string());
                let event_id = event.event_id.clone();
                let record = match event.into_record() {
                    Ok(record) => record,
                    Err(error) => {
                        out.push(CoreToHostMessage::Error {
                            message: error.to_string(),
                        });
                        out.extend(self.patch_messages_since_optional(last_patch_seq));
                        return Ok(out);
                    }
                };
                let route_id = self.emit_to_bus_with_patch(record, &bus_id, !compact_response)?;
                if let Some(parent_invocation_id) = parent_invocation_id {
                    for step in self.process_queue_jump_event(
                        &parent_invocation_id,
                        &event_id,
                        block_parent_completion,
                        pause_parent_route,
                    )? {
                        self.push_route_step_messages_for_host(
                            session_id,
                            step,
                            &mut out,
                            compact_response,
                        );
                    }
                } else if !defer_start {
                    self.drain_host_invocations(session_id, Some(&bus_id), None, &mut out);
                    let step = self.process_route_step_with_options(
                        &route_id,
                        None,
                        Some(ROUTE_INVOCATION_SLICE_LIMIT),
                        !compact_response,
                    )?;
                    self.push_route_step_messages_for_host(
                        session_id,
                        step,
                        &mut out,
                        compact_response,
                    );
                }
            }
            HostToCoreMessage::EmitEvents {
                events,
                bus_id,
                defer_start,
                compact_response: compact,
            } => {
                compact_response = compact;
                self.active_hosts.insert(session_id.to_string());
                let mut route_ids = Vec::with_capacity(events.len());
                for event in events {
                    let record = match event.into_record() {
                        Ok(record) => record,
                        Err(error) => {
                            out.push(CoreToHostMessage::Error {
                                message: error.to_string(),
                            });
                            out.extend(self.patch_messages_since_optional(last_patch_seq));
                            return Ok(out);
                        }
                    };
                    route_ids.push(self.emit_to_bus_with_patch(
                        record,
                        &bus_id,
                        !compact_response,
                    )?);
                }
                if !defer_start {
                    self.drain_host_invocations(session_id, Some(&bus_id), None, &mut out);
                    for route_id in route_ids {
                        let step = self.process_route_step_with_options(
                            &route_id,
                            None,
                            Some(ROUTE_INVOCATION_SLICE_LIMIT),
                            !compact_response,
                        )?;
                        self.push_route_step_messages_for_host(
                            session_id,
                            step,
                            &mut out,
                            compact_response,
                        );
                    }
                }
            }
            HostToCoreMessage::UpdateEventOptions {
                event_id,
                event_handler_completion,
                event_blocks_parent_completion,
            } => {
                self.active_hosts.insert(session_id.to_string());
                let (event_handler_completion, event_blocks_parent_completion) = {
                    let event = self
                        .store
                        .events
                        .get_mut(&event_id)
                        .ok_or_else(|| CoreErrorState::MissingEvent(event_id.clone()))?;
                    if let Some(completion) = event_handler_completion {
                        event.event_handler_completion = Some(completion);
                    }
                    if let Some(blocks_parent) = event_blocks_parent_completion {
                        event.event_blocks_parent_completion = blocks_parent;
                    }
                    (
                        event.event_handler_completion,
                        event.event_blocks_parent_completion,
                    )
                };
                self.store.append_patch(CorePatch::EventUpdated {
                    event_id,
                    event_handler_completion,
                    event_blocks_parent_completion: Some(event_blocks_parent_completion),
                });
            }
            HostToCoreMessage::ForwardEvent {
                event_id,
                bus_id,
                defer_start,
                compact_response: compact,
                parent_invocation_id,
                block_parent_completion,
                pause_parent_route,
                event_timeout,
                event_slow_timeout,
                event_concurrency,
                event_handler_timeout,
                event_handler_slow_timeout,
                event_handler_concurrency,
                event_handler_completion,
                event_blocks_parent_completion,
            } => {
                compact_response = compact;
                self.active_hosts.insert(session_id.to_string());
                {
                    let event = self
                        .store
                        .events
                        .get_mut(&event_id)
                        .ok_or_else(|| CoreErrorState::MissingEvent(event_id.clone()))?;
                    if event_timeout.is_some() {
                        event.event_timeout = event_timeout;
                    }
                    if event_slow_timeout.is_some() {
                        event.event_slow_timeout = event_slow_timeout;
                    }
                    if event_concurrency.is_some() {
                        event.event_concurrency = event_concurrency;
                    }
                    if event_handler_timeout.is_some() {
                        event.event_handler_timeout = event_handler_timeout;
                    }
                    if event_handler_slow_timeout.is_some() {
                        event.event_handler_slow_timeout = event_handler_slow_timeout;
                    }
                    if event_handler_concurrency.is_some() {
                        event.event_handler_concurrency = event_handler_concurrency;
                    }
                    if event_handler_completion.is_some() {
                        event.event_handler_completion = event_handler_completion;
                    }
                    if event_blocks_parent_completion == Some(true) {
                        event.event_blocks_parent_completion = true;
                    }
                }
                let mut event = self
                    .store
                    .events
                    .get(&event_id)
                    .cloned()
                    .ok_or_else(|| CoreErrorState::MissingEvent(event_id.clone()))?;
                let bus_label = self
                    .store
                    .buses
                    .get(&bus_id)
                    .ok_or_else(|| CoreErrorState::MissingBus(bus_id.clone()))?
                    .label
                    .clone();
                if !event.event_path.iter().any(|label| label == &bus_label) {
                    event.event_path.push(bus_label);
                }
                let route_id = self.emit_to_bus_with_patch(event, &bus_id, !compact_response)?;
                if let Some(parent_invocation_id) = parent_invocation_id {
                    for step in self.process_queue_jump_event(
                        &parent_invocation_id,
                        &event_id,
                        block_parent_completion,
                        pause_parent_route,
                    )? {
                        self.push_route_step_messages_for_host(
                            session_id,
                            step,
                            &mut out,
                            compact_response,
                        );
                    }
                } else if !defer_start {
                    self.drain_host_invocations(session_id, Some(&bus_id), None, &mut out);
                    let step = self.process_route_step_with_options(
                        &route_id,
                        None,
                        Some(ROUTE_INVOCATION_SLICE_LIMIT),
                        !compact_response,
                    )?;
                    self.push_route_step_messages_for_host(
                        session_id,
                        step,
                        &mut out,
                        compact_response,
                    );
                }
            }
            HostToCoreMessage::ProcessRoute {
                route_id,
                limit,
                compact_response: compact,
            } => {
                compact_response = compact;
                self.active_hosts.insert(session_id.to_string());
                if let Some(completed_patch) = self.completed_route_event_patch(&route_id)? {
                    self.store.append_patch(completed_patch);
                    out.extend(
                        self.patches_since_for_response_optional(last_patch_seq, compact_response)
                            .into_iter()
                            .map(|(patch_seq, patch)| CoreToHostMessage::Patch {
                                patch_seq,
                                patch,
                            }),
                    );
                    return Ok(out);
                }
                let step = self.process_route_step_with_options(
                    &route_id,
                    None,
                    Some(limit.unwrap_or(ROUTE_INVOCATION_SLICE_LIMIT)),
                    !compact_response,
                )?;
                self.push_route_step_messages_for_host(
                    session_id,
                    step,
                    &mut out,
                    compact_response,
                );
            }
            HostToCoreMessage::ProcessNextRoute {
                bus_id,
                limit,
                compact_response: compact,
            } => {
                compact_response = compact;
                self.active_hosts.insert(session_id.to_string());
                self.maintenance_tick();
                self.drain_host_invocations(session_id, Some(&bus_id), limit, &mut out);
                if !limit.is_some_and(|limit| limit == 0) {
                    for step in self.process_available_routes_for_bus_limited_with_options(
                        &bus_id,
                        limit,
                        !compact_response,
                    )? {
                        self.push_route_step_messages_for_host(
                            session_id,
                            step,
                            &mut out,
                            compact_response,
                        );
                    }
                }
            }
            HostToCoreMessage::WaitInvocations { bus_id, limit } => {
                self.active_hosts.insert(session_id.to_string());
                self.collect_host_invocations(session_id, bus_id.as_deref(), limit, &mut out)?;
            }
            HostToCoreMessage::AwaitEvent {
                event_id,
                parent_invocation_id,
            } => {
                self.active_hosts.insert(session_id.to_string());
                if let Some(parent_invocation_id) = parent_invocation_id {
                    self.mark_child_blocks_parent_completion(&parent_invocation_id, &event_id)?;
                    self.pause_serial_route_for_invocation(&parent_invocation_id)?;
                }
            }
            HostToCoreMessage::QueueJumpEvent {
                event_id,
                parent_invocation_id,
                block_parent_completion,
                pause_parent_route,
            } => {
                self.active_hosts.insert(session_id.to_string());
                for step in self.process_queue_jump_event(
                    &parent_invocation_id,
                    &event_id,
                    block_parent_completion,
                    pause_parent_route,
                )? {
                    self.push_route_step_messages_for_host(session_id, step, &mut out, false);
                }
            }
            HostToCoreMessage::WaitEventCompleted { event_id } => {
                self.active_hosts.insert(session_id.to_string());
                if self
                    .store
                    .events
                    .get(&event_id)
                    .is_some_and(|event| event.event_status == EventStatus::Completed)
                {
                    out.push(CoreToHostMessage::EventCompleted { event_id });
                }
            }
            HostToCoreMessage::WaitEventEmitted {
                bus_id,
                event_pattern,
                seen_event_ids,
                after_event_id,
                after_created_at,
            } => {
                self.active_hosts.insert(session_id.to_string());
                let seen_event_ids = seen_event_ids.into_iter().collect::<BTreeSet<_>>();
                let events = self.unseen_bus_event_snapshots(
                    &bus_id,
                    &event_pattern,
                    &seen_event_ids,
                    after_event_id.as_deref(),
                    after_created_at.as_deref(),
                )?;
                if !events.is_empty() {
                    out.push(CoreToHostMessage::EventList { events });
                }
            }
            HostToCoreMessage::WaitBusIdle { bus_id, .. } => {
                self.active_hosts.insert(session_id.to_string());
                if self.is_bus_idle(&bus_id) {
                    out.push(CoreToHostMessage::BusIdle { bus_id });
                } else {
                    out.push(CoreToHostMessage::BusBusy { bus_id });
                }
            }
            HostToCoreMessage::GetEvent { event_id } => {
                self.active_hosts.insert(session_id.to_string());
                if let Some(event) = self.event_snapshot(&event_id)? {
                    out.push(CoreToHostMessage::EventSnapshot { event });
                }
            }
            HostToCoreMessage::ListEvents {
                event_pattern,
                limit,
                bus_id,
            } => {
                self.active_hosts.insert(session_id.to_string());
                out.push(CoreToHostMessage::EventList {
                    events: if let Some(bus_id) = bus_id {
                        self.list_bus_event_snapshots(&bus_id, &event_pattern, limit)?
                    } else {
                        self.list_event_snapshots(&event_pattern, limit)?
                    },
                });
            }
            HostToCoreMessage::ListEventIds {
                event_pattern,
                limit,
                bus_id,
                statuses,
            } => {
                self.active_hosts.insert(session_id.to_string());
                let statuses = statuses.as_deref();
                out.push(CoreToHostMessage::EventIdList {
                    event_ids: if let Some(bus_id) = bus_id {
                        self.list_bus_event_ids(&bus_id, &event_pattern, limit, statuses)
                    } else {
                        self.list_event_ids(&event_pattern, limit, statuses)
                    },
                });
            }
            HostToCoreMessage::ListPendingEventIds { bus_id } => {
                self.active_hosts.insert(session_id.to_string());
                out.push(CoreToHostMessage::EventIdList {
                    event_ids: self.pending_event_ids_for_bus(&bus_id),
                });
            }
            HostToCoreMessage::HandlerOutcome {
                result_id,
                invocation_id,
                fence,
                outcome,
                process_route_after,
                process_available_after,
                compact_response: compact,
            } => {
                compact_response = compact;
                self.active_hosts.insert(session_id.to_string());
                let route_id =
                    self.accept_handler_outcome(&result_id, &invocation_id, fence, outcome)?;
                self.drain_host_invocations(session_id, None, Some(0), &mut out);
                if !process_route_after {
                    return Ok(self
                        .patches_since_for_response_optional(last_patch_seq, compact_response)
                        .into_iter()
                        .map(|(patch_seq, patch)| CoreToHostMessage::Patch { patch_seq, patch })
                        .collect());
                }
                let bus_id = self
                    .store
                    .routes
                    .get(&route_id)
                    .map(|route| route.bus_id.clone());
                let event_id = self
                    .store
                    .routes
                    .get(&route_id)
                    .map(|route| route.event_id.clone());
                let route_already_terminal = self.store.routes.get(&route_id).is_none_or(|route| {
                    matches!(
                        route.status,
                        RouteStatus::Completed | RouteStatus::Cancelled
                    )
                });
                let step = if route_already_terminal {
                    RouteStep::NoWork {
                        route_id: route_id.clone(),
                    }
                } else {
                    self.process_route_step_with_options(
                        &route_id,
                        None,
                        Some(ROUTE_INVOCATION_SLICE_LIMIT),
                        !compact_response,
                    )?
                };
                let route_completed =
                    route_already_terminal || matches!(step, RouteStep::RouteCompleted { .. });
                self.push_route_step_messages_for_host(
                    session_id,
                    step,
                    &mut out,
                    compact_response,
                );
                if route_completed && process_available_after {
                    if let Some(bus_id) = bus_id {
                        for step in self.process_available_routes_for_bus_limited_with_options(
                            &bus_id,
                            Some(ROUTE_INVOCATION_SLICE_LIMIT),
                            !compact_response,
                        )? {
                            self.push_route_step_messages_for_host(
                                session_id,
                                step,
                                &mut out,
                                compact_response,
                            );
                        }
                    }
                }
                if route_completed && process_available_after {
                    if let Some(event_id) = event_id {
                        for step in self
                            .process_available_routes_for_event_path_limited_with_options(
                                &event_id,
                                Some(ROUTE_INVOCATION_SLICE_LIMIT),
                                !compact_response,
                            )?
                        {
                            self.push_route_step_messages_for_host(
                                session_id,
                                step,
                                &mut out,
                                compact_response,
                            );
                        }
                    }
                }
            }
            HostToCoreMessage::HandlerOutcomes {
                outcomes,
                compact_response: compact,
            } => {
                compact_response = compact;
                self.active_hosts.insert(session_id.to_string());
                let mut routes_to_process = BTreeMap::<String, bool>::new();
                let mut route_bus_ids = BTreeMap::<String, String>::new();
                let mut route_event_ids = BTreeMap::<String, String>::new();
                for handler_outcome in outcomes {
                    let process_available_after = handler_outcome.process_available_after;
                    let route_id = match self.accept_handler_outcome(
                        &handler_outcome.result_id,
                        &handler_outcome.invocation_id,
                        handler_outcome.fence,
                        handler_outcome.outcome,
                    ) {
                        Ok(route_id) => route_id,
                        Err(CoreErrorState::MissingResult(_))
                        | Err(CoreErrorState::StaleInvocationOutcome) => continue,
                        Err(error) => return Err(error),
                    };
                    if let Some(route) = self.store.routes.get(&route_id) {
                        route_bus_ids.insert(route_id.clone(), route.bus_id.clone());
                        route_event_ids.insert(route_id.clone(), route.event_id.clone());
                    }
                    routes_to_process
                        .entry(route_id)
                        .and_modify(|existing| *existing |= process_available_after)
                        .or_insert(process_available_after);
                }
                let mut routes_to_process = routes_to_process.into_iter().collect::<Vec<_>>();
                routes_to_process.sort_by(|(left, _), (right, _)| {
                    self.route_event_depth(right)
                        .cmp(&self.route_event_depth(left))
                        .then_with(|| left.cmp(right))
                });
                for (route_id, process_available_after) in routes_to_process {
                    if !self.store.routes.contains_key(&route_id) {
                        continue;
                    }
                    let route_already_terminal =
                        self.store.routes.get(&route_id).is_none_or(|route| {
                            matches!(
                                route.status,
                                RouteStatus::Completed | RouteStatus::Cancelled
                            )
                        });
                    let step = if route_already_terminal {
                        RouteStep::NoWork {
                            route_id: route_id.clone(),
                        }
                    } else {
                        self.process_route_step_with_options(
                            &route_id,
                            None,
                            Some(ROUTE_INVOCATION_SLICE_LIMIT),
                            !compact_response,
                        )?
                    };
                    let route_completed =
                        route_already_terminal || matches!(step, RouteStep::RouteCompleted { .. });
                    self.push_route_step_messages_for_host(
                        session_id,
                        step,
                        &mut out,
                        compact_response,
                    );
                    if route_completed && process_available_after {
                        if let Some(bus_id) = route_bus_ids.get(&route_id).cloned().or_else(|| {
                            self.store
                                .routes
                                .get(&route_id)
                                .map(|route| route.bus_id.clone())
                        }) {
                            for step in self.process_available_routes_for_bus_limited_with_options(
                                &bus_id,
                                Some(ROUTE_INVOCATION_SLICE_LIMIT),
                                !compact_response,
                            )? {
                                self.push_route_step_messages_for_host(
                                    session_id,
                                    step,
                                    &mut out,
                                    compact_response,
                                );
                            }
                        }
                        if let Some(event_id) =
                            route_event_ids.get(&route_id).cloned().or_else(|| {
                                self.store
                                    .routes
                                    .get(&route_id)
                                    .map(|route| route.event_id.clone())
                            })
                        {
                            for step in self
                                .process_available_routes_for_event_path_limited_with_options(
                                    &event_id,
                                    Some(ROUTE_INVOCATION_SLICE_LIMIT),
                                    !compact_response,
                                )?
                            {
                                self.push_route_step_messages_for_host(
                                    session_id,
                                    step,
                                    &mut out,
                                    compact_response,
                                );
                            }
                        }
                    }
                }
            }
            HostToCoreMessage::Heartbeat => {
                self.active_hosts.insert(session_id.to_string());
                out.push(CoreToHostMessage::HeartbeatAck);
            }
        }
        out.extend(
            self.patches_since_for_response_optional(last_patch_seq, compact_response)
                .into_iter()
                .map(|(patch_seq, patch)| CoreToHostMessage::Patch { patch_seq, patch }),
        );
        Ok(out)
    }

    pub fn handle_handler_outcome_for_host_no_patches(
        &mut self,
        session_id: &str,
        result_id: &str,
        invocation_id: &str,
        fence: u64,
        last_patch_seq: Option<u64>,
        outcome: HandlerOutcome,
        process_route_after: bool,
        process_available_after: bool,
        compact_response: bool,
        include_patches: bool,
    ) -> Result<Vec<CoreToHostMessage>, CoreErrorState> {
        let mut out = Vec::new();
        let route_id = self.accept_handler_outcome(result_id, invocation_id, fence, outcome)?;
        if !process_route_after {
            if let Some(event_id) = self.store.routes.get(&route_id).and_then(|route| {
                self.store.events.get(&route.event_id).and_then(|event| {
                    (event.event_status == EventStatus::Completed).then(|| route.event_id.clone())
                })
            }) {
                out.push(CoreToHostMessage::EventCompleted { event_id });
            }
        }
        self.drain_host_invocations(session_id, None, Some(0), &mut out);
        if !process_route_after {
            if include_patches {
                return Ok(self
                    .patches_since_for_response_optional(last_patch_seq, compact_response)
                    .into_iter()
                    .map(|(patch_seq, patch)| CoreToHostMessage::Patch { patch_seq, patch })
                    .collect());
            }
            return Ok(out);
        }
        let bus_id = self
            .store
            .routes
            .get(&route_id)
            .map(|route| route.bus_id.clone());
        let event_id = self
            .store
            .routes
            .get(&route_id)
            .map(|route| route.event_id.clone());
        let step = self.process_route_step_with_options(
            &route_id,
            None,
            Some(ROUTE_INVOCATION_SLICE_LIMIT),
            !compact_response,
        )?;
        let route_completed = matches!(step, RouteStep::RouteCompleted { .. });
        self.push_route_step_messages_for_host(session_id, step, &mut out, compact_response);
        if route_completed && process_available_after {
            if let Some(bus_id) = bus_id {
                for step in self.process_available_routes_for_bus_limited_with_options(
                    &bus_id,
                    Some(ROUTE_INVOCATION_SLICE_LIMIT),
                    !compact_response,
                )? {
                    self.push_route_step_messages_for_host(
                        session_id,
                        step,
                        &mut out,
                        compact_response,
                    );
                }
            }
        }
        if route_completed && process_available_after {
            if let Some(event_id) = event_id {
                for step in self.process_available_routes_for_event_path_limited_with_options(
                    &event_id,
                    Some(ROUTE_INVOCATION_SLICE_LIMIT),
                    !compact_response,
                )? {
                    self.push_route_step_messages_for_host(
                        session_id,
                        step,
                        &mut out,
                        compact_response,
                    );
                }
            }
        }
        if include_patches {
            out.extend(
                self.patches_since_for_response_optional(last_patch_seq, compact_response)
                    .into_iter()
                    .map(|(patch_seq, patch)| CoreToHostMessage::Patch { patch_seq, patch }),
            );
        }
        Ok(out)
    }

    pub fn queue_jump_event_for_host_no_patches(
        &mut self,
        session_id: &str,
        parent_invocation_id: &str,
        event_id: &str,
        last_patch_seq: Option<u64>,
        block_parent_completion: bool,
        pause_parent_route: bool,
        compact_response: bool,
        include_patches: bool,
    ) -> Result<Vec<CoreToHostMessage>, CoreErrorState> {
        let mut out = Vec::new();
        for step in self.process_queue_jump_event(
            parent_invocation_id,
            event_id,
            block_parent_completion,
            pause_parent_route,
        )? {
            self.push_route_step_messages_for_host(session_id, step, &mut out, compact_response);
        }
        if self
            .store
            .events
            .get(event_id)
            .is_some_and(|event| event.event_status == EventStatus::Completed)
        {
            out.push(CoreToHostMessage::EventCompleted {
                event_id: event_id.to_string(),
            });
        }
        if include_patches {
            out.extend(
                self.patches_since_for_response_optional(last_patch_seq, compact_response)
                    .into_iter()
                    .map(|(patch_seq, patch)| CoreToHostMessage::Patch { patch_seq, patch }),
            );
        }
        Ok(out)
    }

    fn patches_since(&self, last_patch_seq: u64) -> Vec<(u64, CorePatch)> {
        self.store.patches_since(last_patch_seq)
    }

    fn patches_since_for_response(
        &self,
        last_patch_seq: u64,
        compact_response: bool,
    ) -> Vec<(u64, CorePatch)> {
        let patches = self.patches_since(last_patch_seq);
        if !compact_response {
            return patches;
        }
        let mut compacted = Vec::new();
        let mut latest_seq = last_patch_seq;
        for (patch_seq, patch) in patches {
            latest_seq = patch_seq;
            if let Some(patch) = Self::compact_response_patch(patch) {
                compacted.push((patch_seq, patch));
            }
        }
        if compacted.len() > ROUTE_INVOCATION_SLICE_LIMIT {
            compacted.drain(0..compacted.len() - ROUTE_INVOCATION_SLICE_LIMIT);
        }
        if let Some((patch_seq, _)) = compacted.last_mut() {
            *patch_seq = latest_seq;
        }
        compacted
    }

    fn patches_since_for_response_optional(
        &self,
        last_patch_seq: Option<u64>,
        compact_response: bool,
    ) -> Vec<(u64, CorePatch)> {
        last_patch_seq
            .map(|last_patch_seq| self.patches_since_for_response(last_patch_seq, compact_response))
            .unwrap_or_default()
    }

    fn compact_response_patch(patch: CorePatch) -> Option<CorePatch> {
        match patch {
            CorePatch::EventEmitted { .. }
            | CorePatch::EventStarted { .. }
            | CorePatch::RouteStarted { .. }
            | CorePatch::RouteCompleted { .. }
            | CorePatch::ResultPending { .. }
            | CorePatch::ResultStarted { .. }
            | CorePatch::ResultCompleted { .. } => None,
            CorePatch::EventCompleted {
                event_id,
                completed_at,
                event_started_at,
                ..
            } => Some(CorePatch::EventCompletedCompact {
                event_id,
                completed_at,
                event_started_at,
            }),
            other => Some(other),
        }
    }

    fn event_result_snapshots_for_event(
        &self,
        event_id: &str,
    ) -> BTreeMap<HandlerId, CoreEventResultSnapshot> {
        let mut snapshots = self
            .store
            .events
            .get(event_id)
            .map(|event| event.completed_result_snapshots.clone())
            .unwrap_or_default();
        snapshots.extend(
            self.store
                .routes_by_event
                .get(event_id)
                .into_iter()
                .flatten()
                .filter_map(|route_id| self.store.results_by_route.get(route_id))
                .flatten()
                .filter_map(|result_id| self.store.results.get(result_id))
                .filter_map(|result| self.core_result_snapshot(result))
                .map(|result| (result.handler_id.clone(), result))
                .collect::<BTreeMap<_, _>>(),
        );
        snapshots
    }

    fn event_completed_patch_for_event(
        &self,
        event_id: &str,
        completed_at: String,
    ) -> Result<CorePatch, CoreErrorState> {
        let event = self
            .store
            .events
            .get(event_id)
            .ok_or_else(|| CoreErrorState::MissingEvent(event_id.to_string()))?;
        Ok(CorePatch::EventCompleted {
            event_id: event.event_id.clone(),
            completed_at,
            event_path: event.event_path.clone(),
            event_results: self.event_result_snapshots_for_event(&event.event_id),
            event_parent_id: event.event_parent_id.clone(),
            event_emitted_by_result_id: event.event_emitted_by_result_id.clone(),
            event_emitted_by_handler_id: event.event_emitted_by_handler_id.clone(),
            event_blocks_parent_completion: event.event_blocks_parent_completion,
            event_started_at: event.event_started_at.clone(),
        })
    }

    pub fn is_bus_idle(&self, bus_id: &str) -> bool {
        let bus_label = self.store.buses.get(bus_id).map(|bus| bus.label.as_str());
        let routes_idle = self.store.routes.values().all(|route| {
            route.bus_id != bus_id
                || matches!(
                    route.status,
                    RouteStatus::Completed | RouteStatus::Cancelled
                )
        });
        let results_idle = self
            .store
            .results
            .values()
            .all(|result| result.bus_id != bus_id || result.status.is_terminal());
        let inboxes_idle = self
            .host_inboxes
            .values()
            .all(|inbox| inbox.iter().all(|invocation| invocation.bus_id != bus_id));
        let events_idle = self.store.events.values().all(|event| {
            bus_label.is_none_or(|label| !event.event_path.iter().any(|entry| entry == label))
                || event.event_status == EventStatus::Completed
        });
        routes_idle && results_idle && inboxes_idle && events_idle
    }

    fn push_route_step_messages_for_host(
        &mut self,
        session_id: &str,
        step: RouteStep,
        out: &mut Vec<CoreToHostMessage>,
        compact_response: bool,
    ) {
        match step {
            RouteStep::InvokeHandlers { invocations, .. } => {
                let mut local_invocations = Vec::new();
                for invocation in invocations {
                    let handler_host = self
                        .store
                        .handlers
                        .get(&invocation.handler_id)
                        .map(|handler| handler.host_id.clone());
                    let should_queue_remote = handler_host.as_deref() != Some(session_id)
                        && handler_host.as_ref().is_some_and(|host_id| {
                            self.host_session_counts.contains_key(host_id)
                                || self.active_hosts.contains(host_id)
                        });
                    if should_queue_remote {
                        let host_id = handler_host.expect("checked active remote host");
                        self.host_inboxes
                            .entry(host_id)
                            .or_default()
                            .push_back(invocation);
                    } else if compact_response {
                        local_invocations
                            .push(compact_invoke_for_same_host(invocation, compact_response));
                    } else {
                        out.push(CoreToHostMessage::InvokeHandler(
                            compact_invoke_for_same_host(invocation, compact_response),
                        ));
                    }
                }
                match local_invocations.len() {
                    0 => {}
                    1 => out.push(CoreToHostMessage::InvokeHandler(
                        local_invocations
                            .pop()
                            .expect("local invocation length checked"),
                    )),
                    _ => {
                        let first = local_invocations
                            .first()
                            .expect("local invocation length checked");
                        out.push(CoreToHostMessage::InvokeHandlersCompact {
                            route_id: first.route_id.clone(),
                            event_id: first.event_id.clone(),
                            bus_id: first.bus_id.clone(),
                            event_deadline_at: first.event_deadline_at.clone(),
                            route_paused: first.route_paused,
                            invocations: local_invocations
                                .into_iter()
                                .map(|invocation| {
                                    CompactInvokeHandler(
                                        invocation.invocation_id,
                                        invocation.result_id,
                                        invocation.handler_id,
                                        invocation.fence,
                                        invocation.deadline_at,
                                    )
                                })
                                .collect(),
                        })
                    }
                }
            }
            RouteStep::RouteCompleted {
                route_id: _,
                event_id,
                event_completed,
            } => {
                if event_completed {
                    out.push(CoreToHostMessage::EventCompleted { event_id });
                }
            }
            RouteStep::NoWork { .. } | RouteStep::WaitingForEventLock { .. } => {}
        }
    }

    pub fn drain_host_invocations(
        &mut self,
        session_id: &str,
        bus_id: Option<&str>,
        limit: Option<usize>,
        out: &mut Vec<CoreToHostMessage>,
    ) {
        if let Some(notifications) = self.host_notifications.get_mut(session_id) {
            while let Some(message) = notifications.pop_front() {
                out.push(message);
            }
        }
        let Some(inbox) = self.host_inboxes.get_mut(session_id) else {
            return;
        };
        let mut remaining = VecDeque::new();
        while let Some(invocation) = inbox.pop_front() {
            let matches_bus = bus_id.is_none_or(|bus_id| invocation.bus_id == bus_id);
            let under_limit = limit.is_none_or(|limit| {
                limit == 0 || {
                    out.iter()
                        .filter(|message| matches!(message, CoreToHostMessage::InvokeHandler(_)))
                        .count()
                        < limit
                }
            });
            if matches_bus && under_limit && !limit.is_some_and(|limit| limit == 0) {
                out.push(CoreToHostMessage::InvokeHandler(invocation));
            } else {
                remaining.push_back(invocation);
            }
        }
        *inbox = remaining;
    }

    pub fn collect_host_invocations(
        &mut self,
        session_id: &str,
        bus_id: Option<&str>,
        limit: Option<usize>,
        out: &mut Vec<CoreToHostMessage>,
    ) -> Result<(), CoreErrorState> {
        self.drain_host_invocations(session_id, bus_id, limit, out);
        if limit.is_some_and(|limit| limit == 0)
            || out
                .iter()
                .any(|message| matches!(message, CoreToHostMessage::InvokeHandler(_)))
        {
            return Ok(());
        }
        let Some(bus_id) = bus_id else {
            return Ok(());
        };
        for step in self.process_available_routes_for_bus_limited(bus_id, limit)? {
            self.push_route_step_messages_for_host(session_id, step, out, false);
        }
        self.drain_host_invocations(session_id, Some(bus_id), limit, out);
        Ok(())
    }

    fn enqueue_host_notification(&mut self, host_id: &str, message: CoreToHostMessage) {
        self.host_notifications
            .entry(host_id.to_string())
            .or_default()
            .push_back(message);
    }

    fn enqueue_cancel_for_result(&mut self, result: &EventResultRecord, reason: CancelReason) {
        let Some(invocation) = result.invocation.as_ref() else {
            return;
        };
        let Some(handler) = self.store.handlers.get(&result.handler_id) else {
            return;
        };
        self.enqueue_host_notification(
            &handler.host_id.clone(),
            CoreToHostMessage::CancelInvocation {
                invocation_id: invocation.invocation_id.clone(),
                reason,
            },
        );
    }

    fn clear_result_deadline_indexes(&mut self, result_id: &str) {
        let Some((deadline, slow_deadline)) = self.result_deadlines_by_id.remove(result_id) else {
            return;
        };
        if let Some(deadline) = deadline {
            deadline_index_remove(&mut self.result_deadline_index, &deadline, result_id);
        }
        if let Some(deadline) = slow_deadline {
            deadline_index_remove(&mut self.result_slow_deadline_index, &deadline, result_id);
        }
    }

    fn index_started_result_deadlines(&mut self, result: &EventResultRecord) {
        self.clear_result_deadline_indexes(&result.result_id);
        if result.status != ResultStatus::Started {
            return;
        }
        let deadline = result
            .invocation
            .as_ref()
            .and_then(|invocation| invocation.deadline_at.clone());
        let slow_deadline = (!result.slow_warning_sent)
            .then(|| result.slow_warning_at.clone())
            .flatten();
        if let Some(deadline) = deadline.clone() {
            deadline_index_insert(
                &mut self.result_deadline_index,
                deadline,
                result.result_id.clone(),
            );
        }
        if let Some(deadline) = slow_deadline.clone() {
            deadline_index_insert(
                &mut self.result_slow_deadline_index,
                deadline,
                result.result_id.clone(),
            );
        }
        if deadline.is_some() || slow_deadline.is_some() {
            self.result_deadlines_by_id
                .insert(result.result_id.clone(), (deadline, slow_deadline));
        }
    }

    fn clear_event_deadline_indexes(&mut self, event_id: &str) {
        let Some((deadline, slow_deadline)) = self.event_deadlines_by_id.remove(event_id) else {
            return;
        };
        if let Some(deadline) = deadline {
            deadline_index_remove(&mut self.event_deadline_index, &deadline, event_id);
        }
        if let Some(deadline) = slow_deadline {
            deadline_index_remove(&mut self.event_slow_deadline_index, &deadline, event_id);
        }
    }

    fn event_deadline_anchor_at(&self, event_id: &str) -> Option<Timestamp> {
        let mut anchor = None;
        for route_id in self
            .store
            .routes_by_event
            .get(event_id)
            .into_iter()
            .flatten()
        {
            for result_id in self
                .store
                .results_by_route
                .get(route_id)
                .into_iter()
                .flatten()
            {
                let Some(started_at) = self
                    .store
                    .results
                    .get(result_id)
                    .and_then(|result| result.started_at.clone())
                else {
                    continue;
                };
                if anchor.as_ref().is_none_or(|current| started_at < *current) {
                    anchor = Some(started_at);
                }
            }
        }
        anchor
    }

    fn index_started_event_deadlines(&mut self, event_id: &str) {
        self.clear_event_deadline_indexes(event_id);
        let Some((deadline, slow_deadline)) = self.store.events.get(event_id).and_then(|event| {
            if event.event_status != EventStatus::Started {
                return None;
            }
            let started_at = self.event_deadline_anchor_at(event_id)?;
            let deadline = self
                .resolved_event_timeout_for_event(event)
                .and_then(|timeout| timestamp_plus_seconds(&started_at, timeout));
            let slow_deadline = (!self.event_slow_warnings_sent.contains(event_id))
                .then(|| {
                    self.resolved_event_slow_timeout_for_event(event)
                        .and_then(|timeout| timestamp_plus_seconds(&started_at, timeout))
                })
                .flatten();
            Some((deadline, slow_deadline))
        }) else {
            return;
        };
        if let Some(deadline) = deadline.clone() {
            deadline_index_insert(
                &mut self.event_deadline_index,
                deadline,
                event_id.to_string(),
            );
        }
        if let Some(deadline) = slow_deadline.clone() {
            deadline_index_insert(
                &mut self.event_slow_deadline_index,
                deadline,
                event_id.to_string(),
            );
        }
        if deadline.is_some() || slow_deadline.is_some() {
            self.event_deadlines_by_id
                .insert(event_id.to_string(), (deadline, slow_deadline));
        }
    }

    fn remove_route_from_bus_schedule_index(&mut self, route_id: &str) {
        let Some(bus_id) = self
            .store
            .routes
            .get(route_id)
            .map(|route| route.bus_id.clone())
        else {
            return;
        };
        if let Some(route_ids) = self.paused_serial_routes_by_bus.get_mut(&bus_id) {
            route_ids.remove(route_id);
            if route_ids.is_empty() {
                self.paused_serial_routes_by_bus.remove(&bus_id);
            }
        }
        let routes = &self.store.routes;
        if let Some(route_ids) = self.store.routes_by_bus.get_mut(&bus_id) {
            while route_ids.front().is_some_and(|candidate| {
                candidate == route_id
                    || routes.get(candidate).is_none_or(|route| {
                        matches!(
                            route.status,
                            RouteStatus::Completed | RouteStatus::Cancelled
                        )
                    })
            }) {
                route_ids.pop_front();
            }
            if route_ids.iter().any(|candidate| candidate == route_id) {
                route_ids.retain(|candidate| candidate != route_id);
            }
            if route_ids.is_empty() {
                self.store.routes_by_bus.remove(&bus_id);
            }
        }
    }

    pub fn maintenance_tick(&mut self) -> bool {
        let event_slow_warnings = self.emit_due_event_slow_warnings();
        let slow_warnings = self.emit_due_slow_warnings();
        let pruned_deadlines = self.prune_stale_deadline_heads();
        !event_slow_warnings.is_empty() || !slow_warnings.is_empty() || pruned_deadlines
    }

    fn due_deadline_ids(index: &BTreeMap<Timestamp, BTreeSet<String>>) -> Vec<String> {
        let mut ids = Vec::new();
        for (deadline, deadline_ids) in index {
            if !timestamp_is_past(deadline) {
                break;
            }
            ids.extend(deadline_ids.iter().cloned());
        }
        ids
    }

    fn prune_stale_deadline_heads(&mut self) -> bool {
        let mut changed = false;

        loop {
            let Some((deadline, result_id)) = self
                .result_deadline_index
                .first_key_value()
                .and_then(|(deadline, ids)| {
                    ids.iter().next().map(|id| (deadline.clone(), id.clone()))
                })
            else {
                break;
            };
            let valid = self.store.results.get(&result_id).is_some_and(|result| {
                result.status == ResultStatus::Started
                    && result
                        .invocation
                        .as_ref()
                        .and_then(|invocation| invocation.deadline_at.as_deref())
                        == Some(deadline.as_str())
            });
            if valid {
                break;
            }
            deadline_index_remove(&mut self.result_deadline_index, &deadline, &result_id);
            changed = true;
        }

        loop {
            let Some((deadline, result_id)) = self
                .result_slow_deadline_index
                .first_key_value()
                .and_then(|(deadline, ids)| {
                    ids.iter().next().map(|id| (deadline.clone(), id.clone()))
                })
            else {
                break;
            };
            let valid = self.store.results.get(&result_id).is_some_and(|result| {
                result.status == ResultStatus::Started
                    && !result.slow_warning_sent
                    && result.slow_warning_at.as_deref() == Some(deadline.as_str())
            });
            if valid {
                break;
            }
            deadline_index_remove(&mut self.result_slow_deadline_index, &deadline, &result_id);
            changed = true;
        }

        loop {
            let Some((deadline, event_id)) =
                self.event_deadline_index
                    .first_key_value()
                    .and_then(|(deadline, ids)| {
                        ids.iter().next().map(|id| (deadline.clone(), id.clone()))
                    })
            else {
                break;
            };
            let valid = self
                .store
                .events
                .get(&event_id)
                .is_some_and(|event| event.event_status == EventStatus::Started)
                && self
                    .event_deadlines_by_id
                    .get(&event_id)
                    .and_then(|(deadline, _)| deadline.as_deref())
                    == Some(deadline.as_str());
            if valid {
                break;
            }
            deadline_index_remove(&mut self.event_deadline_index, &deadline, &event_id);
            changed = true;
        }

        loop {
            let Some((deadline, event_id)) = self
                .event_slow_deadline_index
                .first_key_value()
                .and_then(|(deadline, ids)| {
                    ids.iter().next().map(|id| (deadline.clone(), id.clone()))
                })
            else {
                break;
            };
            let valid = self.store.events.get(&event_id).is_some_and(|event| {
                event.event_status == EventStatus::Started
                    && !self.event_slow_warnings_sent.contains(&event_id)
            }) && self
                .event_deadlines_by_id
                .get(&event_id)
                .and_then(|(_, deadline)| deadline.as_deref())
                == Some(deadline.as_str());
            if valid {
                break;
            }
            deadline_index_remove(&mut self.event_slow_deadline_index, &deadline, &event_id);
            changed = true;
        }

        changed
    }

    pub fn next_deadline_at(&self) -> Option<String> {
        [
            self.result_slow_deadline_index
                .first_key_value()
                .map(|(deadline, _)| deadline),
            self.event_slow_deadline_index
                .first_key_value()
                .map(|(deadline, _)| deadline),
        ]
        .into_iter()
        .flatten()
        .cloned()
        .min()
    }

    pub fn acquire_route_event_lock(
        &mut self,
        route_id: &str,
        owner: InvocationId,
        qj: Option<&QueueJumpContext>,
    ) -> Result<Option<LockAcquire>, CoreErrorState> {
        let route = self
            .store
            .routes
            .get(route_id)
            .cloned()
            .ok_or_else(|| CoreErrorState::MissingRoute(route_id.to_string()))?;
        let event = self
            .store
            .events
            .get(&route.event_id)
            .cloned()
            .ok_or_else(|| CoreErrorState::MissingEvent(route.event_id.clone()))?;
        let bus = self
            .store
            .buses
            .get(&route.bus_id)
            .cloned()
            .ok_or_else(|| CoreErrorState::MissingBus(route.bus_id.clone()))?;
        let resource = event_lock_resource_for_route(&event, &bus);

        if let (Some(resource), Some(qj)) = (&resource, qj) {
            let can_borrow = qj.initiating_bus_id == route.bus_id
                || qj
                    .borrowed_event_locks
                    .iter()
                    .any(|lease| !lease.suspended && &lease.resource == resource);
            if can_borrow {
                return Ok(None);
            }
        }

        if let Some(resource) = &resource {
            if self
                .locks
                .current(resource)
                .is_some_and(|lease| lease.owner != owner)
            {
                return Ok(Some(LockAcquire::Waiting { position: 0 }));
            }
        }

        let acquire = self.locks.acquire(resource, owner, None);
        if let Some(LockAcquire::Granted(lease)) = &acquire {
            if let Some(route) = self.store.routes.get_mut(route_id) {
                route.event_lock_lease = Some(lease.clone());
            }
            self.store.append_patch(CorePatch::RouteStarted {
                route_id: route_id.to_string(),
                event_lock_lease: Some(lease.clone()),
            });
        }
        Ok(acquire)
    }

    pub fn release_route_event_lock(&mut self, route_id: &str) -> Result<(), CoreErrorState> {
        self.locks.remove_waiter(&format!("route:{route_id}"));
        let lease = self
            .store
            .routes
            .get(route_id)
            .ok_or_else(|| CoreErrorState::MissingRoute(route_id.to_string()))?
            .event_lock_lease
            .clone();
        let Some(lease) = lease else {
            return Ok(());
        };
        self.locks.release(&lease)?;
        if let Some(route) = self.store.routes.get_mut(route_id) {
            route.event_lock_lease = None;
        }
        Ok(())
    }

    fn mark_event_and_route_started(&mut self, route_id: &str) -> Result<(), CoreErrorState> {
        let event_id = self
            .store
            .routes
            .get(route_id)
            .map(|route| route.event_id.clone())
            .ok_or_else(|| CoreErrorState::MissingRoute(route_id.to_string()))?;
        let event = self
            .store
            .events
            .get_mut(&event_id)
            .ok_or_else(|| CoreErrorState::MissingEvent(event_id.clone()))?;
        if event.event_status == EventStatus::Pending {
            event.event_status = EventStatus::Started;
            let started_at = now_timestamp();
            event.event_started_at = Some(started_at.clone());
            self.store.append_patch(CorePatch::EventStarted {
                event_id: event_id.clone(),
                started_at,
            });
            self.index_started_event_deadlines(&event_id);
        }
        if let Some(route) = self.store.routes.get_mut(route_id) {
            if route.status == RouteStatus::Pending {
                route.status = RouteStatus::Started;
            }
        }
        Ok(())
    }

    pub fn create_missing_result_rows(&mut self, route_id: &str) -> Result<(), CoreErrorState> {
        self.create_missing_result_rows_limited(route_id, None)
    }

    pub fn create_missing_result_rows_limited(
        &mut self,
        route_id: &str,
        limit: Option<usize>,
    ) -> Result<(), CoreErrorState> {
        self.create_missing_result_rows_with_options(route_id, limit, true)
    }

    fn create_missing_result_rows_with_options(
        &mut self,
        route_id: &str,
        limit: Option<usize>,
        append_lifecycle_patches: bool,
    ) -> Result<(), CoreErrorState> {
        let route = self
            .store
            .routes
            .get(route_id)
            .cloned()
            .ok_or_else(|| CoreErrorState::MissingRoute(route_id.to_string()))?;
        let event = self
            .store
            .events
            .get(&route.event_id)
            .cloned()
            .ok_or_else(|| CoreErrorState::MissingEvent(route.event_id.clone()))?;
        let bus = self
            .store
            .buses
            .get(&route.bus_id)
            .cloned()
            .ok_or_else(|| CoreErrorState::MissingBus(route.bus_id.clone()))?;
        let mut handlers = self
            .store
            .handlers_for_bus(&route.bus_id)
            .into_iter()
            .enumerate()
            .filter(|handler| {
                handler.1.event_pattern == "*" || handler.1.event_pattern == event.event_type
            })
            .map(|(idx, handler)| (idx, handler.clone()))
            .collect::<Vec<_>>();
        handlers.sort_by_key(|(idx, handler)| (handler.event_pattern == "*", *idx));

        let existing_result_ids = self
            .store
            .results_by_route
            .get(route_id)
            .cloned()
            .unwrap_or_default();
        let existing_count = existing_result_ids.len();
        if existing_count >= handlers.len() {
            return Ok(());
        }
        let mut existing_handler_ids = BTreeSet::new();
        for result_id in &existing_result_ids {
            if let Some(result) = self.store.results.get(result_id) {
                existing_handler_ids.insert(result.handler_id.clone());
            }
        }

        let mut created = 0usize;
        for (handler_seq, (_, handler)) in handlers.iter().enumerate().skip(existing_count) {
            if existing_handler_ids.contains(&handler.handler_id) {
                continue;
            }
            if limit.is_some_and(|limit| created >= limit) {
                break;
            }
            let result_id = uuid_v7_string();
            let timeout = resolve_effective_handler_timeout(&event, handler, &bus);
            let slow_timeout = resolve_handler_slow_timeout(&event, handler, &bus);
            let result = EventResultRecord::pending(
                result_id,
                event.event_id.clone(),
                route.route_id.clone(),
                bus.bus_id.clone(),
                handler.handler_id.clone(),
                handler_seq,
                timeout,
                slow_timeout,
            );
            self.store.insert_result(result.clone());
            if append_lifecycle_patches {
                self.store.append_patch(CorePatch::ResultPending { result });
            }
            existing_handler_ids.insert(handler.handler_id.clone());
            created += 1;
        }
        Ok(())
    }

    pub fn start_eligible_results(
        &mut self,
        route_id: &str,
    ) -> Result<Vec<InvokeHandler>, CoreErrorState> {
        self.start_eligible_results_limited(route_id, None, true)
    }

    pub fn start_eligible_results_limited(
        &mut self,
        route_id: &str,
        limit: Option<usize>,
        include_snapshots: bool,
    ) -> Result<Vec<InvokeHandler>, CoreErrorState> {
        if self
            .store
            .results_by_route
            .get(route_id)
            .is_none_or(|result_ids| result_ids.is_empty())
        {
            self.create_missing_result_rows_with_options(route_id, limit, include_snapshots)?;
        }
        let mut eligible = eligible_results(&self.store, route_id);
        if let Some(limit) = limit {
            eligible.truncate(limit);
        }
        let route = self
            .store
            .routes
            .get(route_id)
            .cloned()
            .ok_or_else(|| CoreErrorState::MissingRoute(route_id.to_string()))?;
        let event_record = self
            .store
            .events
            .get(&route.event_id)
            .cloned()
            .ok_or_else(|| CoreErrorState::MissingEvent(route.event_id.clone()))?;
        let bus_record = self
            .store
            .buses
            .get(&route.bus_id)
            .cloned()
            .ok_or_else(|| CoreErrorState::MissingBus(route.bus_id.clone()))?;
        let event_snapshot = if include_snapshots {
            Some(
                EventEnvelope::from_record(&event_record)
                    .map_err(|error| CoreErrorState::InvalidEnvelope(error.to_string()))?,
            )
        } else {
            None
        };
        let mut invocations = Vec::new();
        let mut patches = Vec::new();
        let mut event_deadline_anchor_at = self.event_deadline_anchor_at(&event_record.event_id);
        let resolved_event_timeout = resolve_event_timeout(&event_record, &bus_record);
        for result_id in eligible {
            let invocation_id = uuid_v7_string();
            let fence = 1;
            let handler_timeout = self.store.results.get(&result_id).and_then(|result| {
                self.store
                    .handlers
                    .get(&result.handler_id)
                    .and_then(|handler| {
                        resolve_handler_timeout(&event_record, handler, &bus_record)
                    })
            });
            let result_snapshot = {
                let result = self
                    .store
                    .results
                    .get_mut(&result_id)
                    .ok_or_else(|| CoreErrorState::MissingResult(result_id.clone()))?;
                if result.status != ResultStatus::Pending {
                    continue;
                }
                let started_at = now_timestamp();
                let deadline_at = handler_timeout
                    .and_then(|timeout| timestamp_plus_seconds(&started_at, timeout));
                let slow_warning_at = result
                    .slow_timeout
                    .and_then(|timeout| timestamp_plus_seconds(&started_at, timeout));
                result.status = ResultStatus::Started;
                result.started_at = Some(started_at);
                result.slow_warning_at = slow_warning_at;
                result.slow_warning_sent = false;
                result.invocation = Some(InvocationState {
                    invocation_id: invocation_id.clone(),
                    fence,
                    deadline_at: deadline_at.clone(),
                    cancel_reason: None,
                });
                result.clone()
            };
            self.index_started_result_deadlines(&result_snapshot);
            let deadline_at = result_snapshot
                .invocation
                .as_ref()
                .and_then(|invocation| invocation.deadline_at.clone());
            if let Some(started_at) = result_snapshot.started_at.as_ref() {
                if event_deadline_anchor_at
                    .as_ref()
                    .is_none_or(|current| started_at < current)
                {
                    event_deadline_anchor_at = Some(started_at.clone());
                }
            }
            let event_deadline_at = event_deadline_anchor_at.as_ref().and_then(|started_at| {
                resolved_event_timeout
                    .and_then(|timeout| timestamp_plus_seconds(started_at, timeout))
            });
            patches.push(CorePatch::ResultStarted {
                result_id: result_snapshot.result_id.clone(),
                invocation_id: invocation_id.clone(),
                started_at: result_snapshot
                    .started_at
                    .clone()
                    .unwrap_or_else(now_timestamp),
                deadline_at: deadline_at.clone(),
            });
            invocations.push(InvokeHandler {
                invocation_id,
                result_id: result_snapshot.result_id.clone(),
                route_id: result_snapshot.route_id.clone(),
                event_id: result_snapshot.event_id.clone(),
                bus_id: result_snapshot.bus_id.clone(),
                handler_id: result_snapshot.handler_id.clone(),
                fence,
                deadline_at,
                event_deadline_at: event_deadline_at.clone(),
                route_paused: false,
                event_snapshot: event_snapshot.clone(),
                result_snapshot: include_snapshots.then_some(result_snapshot),
            });
        }
        if !invocations.is_empty() {
            self.index_started_event_deadlines(&event_record.event_id);
        }
        if include_snapshots {
            for patch in patches {
                self.store.append_patch(patch);
            }
        }
        if let Some(route) = self.store.routes.get_mut(route_id) {
            route.status = RouteStatus::Started;
        }
        let route_paused = self
            .store
            .routes
            .get(route_id)
            .is_some_and(|route| route.serial_handler_paused_by.is_some());
        for invocation in invocations.iter_mut() {
            invocation.route_paused = route_paused;
        }
        Ok(invocations)
    }

    pub fn process_route_step(
        &mut self,
        route_id: &str,
        qj: Option<&QueueJumpContext>,
    ) -> Result<RouteStep, CoreErrorState> {
        self.process_route_step_limited(route_id, qj, None)
    }

    pub fn process_route_step_limited(
        &mut self,
        route_id: &str,
        qj: Option<&QueueJumpContext>,
        invocation_limit: Option<usize>,
    ) -> Result<RouteStep, CoreErrorState> {
        self.process_route_step_with_options(route_id, qj, invocation_limit, true)
    }

    fn process_route_step_with_options(
        &mut self,
        route_id: &str,
        qj: Option<&QueueJumpContext>,
        invocation_limit: Option<usize>,
        include_snapshots: bool,
    ) -> Result<RouteStep, CoreErrorState> {
        let route = self
            .store
            .routes
            .get(route_id)
            .cloned()
            .ok_or_else(|| CoreErrorState::MissingRoute(route_id.to_string()))?;
        if matches!(
            route.status,
            RouteStatus::Completed | RouteStatus::Cancelled
        ) {
            if route.status == RouteStatus::Completed {
                let event_id = route.event_id.clone();
                let event_completed = self
                    .store
                    .events
                    .get(&event_id)
                    .is_some_and(|event| event.event_status == EventStatus::Completed);
                return Ok(RouteStep::RouteCompleted {
                    route_id: route_id.to_string(),
                    event_id,
                    event_completed,
                });
            }
            return Ok(RouteStep::NoWork {
                route_id: route_id.to_string(),
            });
        }

        let should_acquire_event_lock =
            route.status == RouteStatus::Pending && route.event_lock_lease.is_none();
        if should_acquire_event_lock {
            match self.acquire_route_event_lock(route_id, format!("route:{route_id}"), qj)? {
                Some(LockAcquire::Waiting { position }) => {
                    return Ok(RouteStep::WaitingForEventLock {
                        route_id: route_id.to_string(),
                        position,
                    });
                }
                Some(LockAcquire::Granted(_)) | None => {}
            }
        }

        self.mark_event_and_route_started(route_id)?;
        self.create_missing_result_rows_with_options(
            route_id,
            invocation_limit,
            include_snapshots,
        )?;

        let invocations =
            self.start_eligible_results_limited(route_id, invocation_limit, include_snapshots)?;
        if !invocations.is_empty() {
            return Ok(RouteStep::InvokeHandlers {
                route_id: route_id.to_string(),
                invocations,
            });
        }

        if self.route_ready_to_complete(route_id) {
            self.create_missing_result_rows_with_options(
                route_id,
                invocation_limit,
                include_snapshots,
            )?;
        }

        if self.route_ready_to_complete(route_id) {
            self.release_route_event_lock(route_id)?;
            if let Some(route) = self.store.routes.get_mut(route_id) {
                if route.status != RouteStatus::Completed {
                    route.status = RouteStatus::Completed;
                    self.store.append_patch(CorePatch::RouteCompleted {
                        route_id: route_id.to_string(),
                    });
                }
            }
            self.remove_route_from_bus_schedule_index(route_id);
            let event_id = self
                .store
                .routes
                .get(route_id)
                .map(|route| route.event_id.clone())
                .ok_or_else(|| CoreErrorState::MissingRoute(route_id.to_string()))?;
            let event_completed = self.complete_event_if_route_done(&event_id, route_id)?;
            return Ok(RouteStep::RouteCompleted {
                route_id: route_id.to_string(),
                event_id,
                event_completed,
            });
        }

        Ok(RouteStep::NoWork {
            route_id: route_id.to_string(),
        })
    }

    fn completed_route_event_patch(
        &self,
        route_id: &str,
    ) -> Result<Option<CorePatch>, CoreErrorState> {
        let Some(route) = self.store.routes.get(route_id) else {
            return Err(CoreErrorState::MissingRoute(route_id.to_string()));
        };
        if route.status != RouteStatus::Completed {
            return Ok(None);
        }
        let Some(event) = self.store.events.get(&route.event_id) else {
            return Err(CoreErrorState::MissingEvent(route.event_id.clone()));
        };
        if event.event_status != EventStatus::Completed {
            return Ok(None);
        }
        let Some(completed_at) = event.event_completed_at.clone() else {
            return Ok(None);
        };
        self.event_completed_patch_for_event(&route.event_id, completed_at)
            .map(Some)
    }

    pub fn process_next_route_for_bus(
        &mut self,
        bus_id: &str,
    ) -> Result<Option<RouteStep>, CoreErrorState> {
        self.process_next_route_for_bus_limited(bus_id, None)
    }

    pub fn process_next_route_for_bus_limited(
        &mut self,
        bus_id: &str,
        invocation_limit: Option<usize>,
    ) -> Result<Option<RouteStep>, CoreErrorState> {
        self.process_next_route_for_bus_with_options(bus_id, invocation_limit, true)
    }

    fn process_next_route_for_bus_with_options(
        &mut self,
        bus_id: &str,
        invocation_limit: Option<usize>,
        include_snapshots: bool,
    ) -> Result<Option<RouteStep>, CoreErrorState> {
        if !self.store.buses.contains_key(bus_id) {
            return Err(CoreErrorState::MissingBus(bus_id.to_string()));
        }
        let len = self
            .store
            .routes_by_bus
            .get(bus_id)
            .map(|route_ids| route_ids.len())
            .unwrap_or(0);
        for index in 0..len {
            let route_id = self
                .store
                .routes_by_bus
                .get(bus_id)
                .and_then(|route_ids| route_ids.get(index))
                .cloned();
            let Some(route_id) = route_id else {
                continue;
            };
            let Some(route) = self.store.routes.get(&route_id) else {
                continue;
            };
            if matches!(
                route.status,
                RouteStatus::Completed | RouteStatus::Cancelled
            ) {
                continue;
            }
            if !self.route_may_have_work(&route_id)? {
                continue;
            }
            let step = self.process_route_step_with_options(
                &route_id,
                None,
                invocation_limit,
                include_snapshots,
            )?;
            if !matches!(step, RouteStep::NoWork { .. }) {
                return Ok(Some(step));
            }
        }
        Ok(None)
    }

    pub fn pending_event_ids_for_bus(&self, bus_id: &str) -> Vec<String> {
        let mut seen = BTreeSet::new();
        let mut event_ids = Vec::new();
        let Some(route_ids) = self.store.routes_by_bus.get(bus_id) else {
            return event_ids;
        };
        for route_id in route_ids {
            let Some(route) = self.store.routes.get(route_id) else {
                continue;
            };
            if matches!(
                route.status,
                RouteStatus::Completed | RouteStatus::Cancelled
            ) {
                continue;
            }
            let Some(event) = self.store.events.get(&route.event_id) else {
                continue;
            };
            if !matches!(
                event.event_status,
                EventStatus::Pending | EventStatus::Started
            ) {
                continue;
            }
            if seen.insert(route.event_id.clone()) {
                event_ids.push(route.event_id.clone());
            }
        }
        event_ids
    }

    pub fn process_available_routes_for_bus(
        &mut self,
        bus_id: &str,
    ) -> Result<Vec<RouteStep>, CoreErrorState> {
        self.process_available_routes_for_bus_limited(bus_id, None)
    }

    pub fn process_available_routes_for_bus_limited(
        &mut self,
        bus_id: &str,
        limit: Option<usize>,
    ) -> Result<Vec<RouteStep>, CoreErrorState> {
        self.process_available_routes_for_bus_limited_with_options(bus_id, limit, true)
    }

    fn process_available_routes_for_bus_limited_with_options(
        &mut self,
        bus_id: &str,
        limit: Option<usize>,
        include_snapshots: bool,
    ) -> Result<Vec<RouteStep>, CoreErrorState> {
        if !self.store.buses.contains_key(bus_id) {
            return Err(CoreErrorState::MissingBus(bus_id.to_string()));
        }

        let max_steps = self
            .store
            .routes_by_bus
            .get(bus_id)
            .map(|routes| routes.len().saturating_add(1))
            .unwrap_or(1)
            .min(limit.unwrap_or(usize::MAX))
            .max(1);
        let mut steps = Vec::new();

        for _ in 0..max_steps {
            let Some(step) =
                self.process_next_route_for_bus_with_options(bus_id, limit, include_snapshots)?
            else {
                break;
            };
            let continue_after_step = match &step {
                RouteStep::InvokeHandlers { route_id, .. } => self
                    .route_event_concurrency(route_id)?
                    .is_some_and(|mode| mode == EventConcurrency::Parallel),
                RouteStep::RouteCompleted { .. } => limit.is_some(),
                RouteStep::WaitingForEventLock { .. } | RouteStep::NoWork { .. } => false,
            };
            steps.push(step);
            if !continue_after_step {
                break;
            }
        }

        Ok(steps)
    }

    fn process_available_routes_for_event_path_limited_with_options(
        &mut self,
        event_id: &str,
        limit: Option<usize>,
        include_snapshots: bool,
    ) -> Result<Vec<RouteStep>, CoreErrorState> {
        let route_ids = self.routes_for_event_path_order(event_id);
        let mut steps = Vec::new();
        for route_id in route_ids {
            if !self.store.routes.contains_key(&route_id) {
                continue;
            }
            if !self.route_may_have_work(&route_id)? {
                continue;
            }
            let step =
                self.process_route_step_with_options(&route_id, None, limit, include_snapshots)?;
            let route_terminal = self.store.routes.get(&route_id).is_none_or(|route| {
                matches!(
                    route.status,
                    RouteStatus::Completed | RouteStatus::Cancelled
                )
            });
            if !matches!(step, RouteStep::NoWork { .. }) {
                steps.push(step);
            }
            if !route_terminal {
                break;
            }
        }
        Ok(steps)
    }

    fn route_may_have_work(&self, route_id: &str) -> Result<bool, CoreErrorState> {
        let route = self
            .store
            .routes
            .get(route_id)
            .ok_or_else(|| CoreErrorState::MissingRoute(route_id.to_string()))?;
        let bus_has_paused_serial_route = self
            .paused_serial_routes_by_bus
            .get(&route.bus_id)
            .is_some_and(|route_ids| {
                route_ids.iter().any(|candidate_id| {
                    candidate_id != route_id
                        && self
                            .store
                            .routes
                            .get(candidate_id)
                            .is_some_and(|candidate| candidate.serial_handler_paused_by.is_some())
                })
            });
        if bus_has_paused_serial_route
            && self.route_event_concurrency(route_id)? != Some(EventConcurrency::Parallel)
        {
            return Ok(false);
        }
        if route.status == RouteStatus::Pending {
            return Ok(true);
        }
        let result_ids = self
            .store
            .results_by_route
            .get(route_id)
            .cloned()
            .unwrap_or_default();
        if result_ids.is_empty() {
            return Ok(true);
        }
        let Some(event) = self.store.events.get(&route.event_id) else {
            return Ok(false);
        };
        let expected_handlers = self
            .store
            .handlers_for_bus(&route.bus_id)
            .into_iter()
            .filter(|handler| {
                handler.event_pattern == "*" || handler.event_pattern == event.event_type
            })
            .count();
        if result_ids.len() < expected_handlers {
            return Ok(true);
        }
        let mut all_terminal = true;
        let Some(bus) = self.store.buses.get(&route.bus_id) else {
            return Ok(false);
        };
        for result_id in result_ids {
            let Some(result) = self.store.results.get(&result_id) else {
                continue;
            };
            if !result.status.is_terminal() {
                all_terminal = false;
            }
            if result.status != ResultStatus::Pending {
                continue;
            }
            let Some(handler) = self.store.handlers.get(&result.handler_id) else {
                continue;
            };
            if result_is_eligible(event, route, result, handler, bus, false) {
                return Ok(true);
            }
        }
        Ok(all_terminal)
    }

    fn route_event_concurrency(
        &self,
        route_id: &str,
    ) -> Result<Option<EventConcurrency>, CoreErrorState> {
        let Some(route) = self.store.routes.get(route_id) else {
            return Ok(None);
        };
        let event = self
            .store
            .events
            .get(&route.event_id)
            .ok_or_else(|| CoreErrorState::MissingEvent(route.event_id.clone()))?;
        let bus = self
            .store
            .buses
            .get(&route.bus_id)
            .ok_or_else(|| CoreErrorState::MissingBus(route.bus_id.clone()))?;
        Ok(Some(resolve_event_concurrency(event, bus)))
    }

    pub fn accept_handler_outcome(
        &mut self,
        result_id: &str,
        invocation_id: &str,
        fence: u64,
        outcome: HandlerOutcome,
    ) -> Result<String, CoreErrorState> {
        let route_id_for_late_outcome = {
            let result = self
                .store
                .results
                .get(result_id)
                .ok_or_else(|| CoreErrorState::MissingResult(result_id.to_string()))?;
            result.route_id.clone()
        };
        let event_timeout_abort = self
            .store
            .results
            .get(result_id)
            .filter(|result| result.status == ResultStatus::Started)
            .and_then(|result| {
                let invocation = result.invocation.as_ref()?;
                (invocation.invocation_id == invocation_id && invocation.fence == fence)
                    .then_some(result.event_id.clone())
            })
            .filter(|_| handler_outcome_is_event_timeout_abort(&outcome))
            .and_then(|event_id| {
                let timeout = self
                    .store
                    .events
                    .get(&event_id)
                    .and_then(|event| self.resolved_event_timeout_for_event(event))
                    .or_else(|| handler_outcome_timeout_seconds(&outcome))?;
                Some((event_id, timeout))
            });
        if let Some((event_id, timeout)) = event_timeout_abort {
            self.finalize_event_timeout(&event_id, timeout);
            return Ok(route_id_for_late_outcome);
        }
        let mut pre_completion_patches = Vec::new();
        let (route_id, event_id, winner) = {
            let result = self
                .store
                .results
                .get_mut(result_id)
                .ok_or_else(|| CoreErrorState::MissingResult(result_id.to_string()))?;
            let Some(invocation) = &result.invocation else {
                return Err(CoreErrorState::StaleInvocationOutcome);
            };
            if invocation.invocation_id != invocation_id || invocation.fence != fence {
                return Err(CoreErrorState::StaleInvocationOutcome);
            }
            if result.status != ResultStatus::Started {
                return Err(CoreErrorState::StaleInvocationOutcome);
            }

            if handler_slow_warning_is_due(result) {
                result.slow_warning_sent = true;
                pre_completion_patches.push(CorePatch::ResultSlowWarning {
                    result_id: result.result_id.clone(),
                    invocation_id: invocation.invocation_id.clone(),
                    warned_at: now_timestamp(),
                });
            }

            match outcome {
                HandlerOutcome::Completed {
                    value,
                    result_is_event_reference,
                    result_is_undefined,
                } => {
                    result.status = ResultStatus::Completed;
                    result.result_set = true;
                    result.result_is_undefined = result_is_undefined;
                    result.result = Some(value);
                    result.result_is_event_reference = result_is_event_reference;
                }
                HandlerOutcome::Errored { error } => {
                    result.status = ResultStatus::Error;
                    result.result_set = false;
                    result.result_is_undefined = false;
                    result.result = None;
                    result.error = Some(error);
                }
                HandlerOutcome::Cancelled { reason } => {
                    result.status = ResultStatus::Cancelled;
                    result.result_set = false;
                    result.result_is_undefined = false;
                    result.result = None;
                    result.error = Some(CoreError::new(
                        CoreErrorKind::HandlerCancelled,
                        format!("handler cancelled: {reason:?}"),
                    ));
                }
            }
            result.completed_at = Some(now_timestamp());
            result.invocation = None;
            (
                result.route_id.clone(),
                result.event_id.clone(),
                is_first_mode_winner(result),
            )
        };
        self.emit_due_event_slow_warning_for_event(&event_id);
        for patch in pre_completion_patches {
            self.store.append_patch(patch);
        }
        self.clear_result_deadline_indexes(result_id);
        self.unpause_serial_route(&route_id, invocation_id)?;
        self.advance_route_after_result(result_id);
        let should_cancel_first_losers = winner
            && resolved_completion_for_route(&self.store, &route_id)
                .is_some_and(|completion| completion == crate::types::HandlerCompletion::First);
        if should_cancel_first_losers {
            self.cancel_remaining_first_mode_results(result_id)?;
        }
        if let Some(result) = self.store.results.get(result_id).cloned() {
            self.store
                .append_patch(CorePatch::ResultCompleted { result });
        }
        self.advance_route_after_result(result_id);
        Ok(route_id)
    }

    pub fn cancel_remaining_first_mode_results(
        &mut self,
        winner_result_id: &str,
    ) -> Result<(), CoreErrorState> {
        let winner = self
            .store
            .results
            .get(winner_result_id)
            .cloned()
            .ok_or_else(|| CoreErrorState::MissingResult(winner_result_id.to_string()))?;
        if !is_first_mode_winner(&winner) {
            return Ok(());
        }
        let route_result_ids = self.store.route_results_mut_ids(&winner.route_id);
        let mut patches = Vec::new();
        let mut cancelled_result_ids = Vec::new();
        let mut child_event_ids = Vec::new();
        for result_id in route_result_ids {
            if result_id == winner.result_id {
                continue;
            }
            let Some(result) = self.store.results.get_mut(&result_id) else {
                continue;
            };
            if result.status.is_terminal() {
                continue;
            }
            let cancel_snapshot = result.clone();
            child_event_ids.extend(cancel_snapshot.event_children.iter().cloned());
            result.status = ResultStatus::Cancelled;
            let error_kind = if cancel_snapshot.status == ResultStatus::Started {
                CoreErrorKind::HandlerAborted
            } else {
                CoreErrorKind::HandlerCancelled
            };
            let error_message = if cancel_snapshot.status == ResultStatus::Started {
                "first result resolved"
            } else {
                "first result resolved"
            };
            result.error = Some(CoreError::new(error_kind, error_message));
            result.completed_at = Some(now_timestamp());
            result.invocation = None;
            patches.push(CorePatch::ResultCancelled {
                result: result.clone(),
                reason: CancelReason::FirstResultWon,
            });
            cancelled_result_ids.push(result_id.clone());
            self.enqueue_cancel_for_result(&cancel_snapshot, CancelReason::FirstResultWon);
        }
        for patch in patches {
            self.store.append_patch(patch);
        }
        for result_id in cancelled_result_ids {
            self.clear_result_deadline_indexes(&result_id);
            self.advance_route_after_result(&result_id);
        }
        for child_event_id in child_event_ids {
            let mut visited = BTreeSet::new();
            self.finalize_event_parent_timeout(&child_event_id, &winner.event_id, &mut visited);
        }
        self.advance_route_after_result(&winner.result_id);
        Ok(())
    }

    pub fn expire_timed_out_invocations(&mut self) -> Vec<ResultId> {
        let timed_out: Vec<ResultId> = Self::due_deadline_ids(&self.result_deadline_index)
            .into_iter()
            .filter(|result_id| {
                self.store.results.get(result_id).is_some_and(|result| {
                    result.status == ResultStatus::Started
                        && result
                            .invocation
                            .as_ref()
                            .and_then(|invocation| invocation.deadline_at.as_deref())
                            .is_some_and(timestamp_is_past)
                })
            })
            .collect();

        for result_id in &timed_out {
            let mut patch = None;
            let mut cancel_snapshot = None;
            if let Some(result) = self.store.results.get_mut(result_id) {
                cancel_snapshot = Some(result.clone());
                result.status = ResultStatus::Error;
                result.error = Some(CoreError::new(
                    CoreErrorKind::HandlerTimeout,
                    format!("handler timed out after {:?}s", result.timeout),
                ));
                result.completed_at = Some(now_timestamp());
                if let Some(invocation) = &mut result.invocation {
                    invocation.cancel_reason = Some(CancelReason::HandlerTimeout);
                }
                result.invocation = None;
                patch = Some(CorePatch::ResultTimedOut {
                    result: result.clone(),
                });
            }
            if let Some(patch) = patch {
                self.store.append_patch(patch);
            }
            if let Some(result) = cancel_snapshot {
                self.enqueue_cancel_for_result(&result, CancelReason::HandlerTimeout);
            }
            self.clear_result_deadline_indexes(result_id);
            self.advance_route_after_result(result_id);
        }
        timed_out
    }

    pub fn emit_due_slow_warnings(&mut self) -> Vec<ResultId> {
        let due: Vec<ResultId> = Self::due_deadline_ids(&self.result_slow_deadline_index)
            .into_iter()
            .filter(|result_id| {
                self.store.results.get(result_id).is_some_and(|result| {
                    result.status == ResultStatus::Started && handler_slow_warning_is_due(result)
                })
            })
            .collect();

        for result_id in &due {
            let mut patch = None;
            let mut updated = None;
            if let Some(result) = self.store.results.get_mut(result_id) {
                result.slow_warning_sent = true;
                if let Some(invocation) = result.invocation.as_ref() {
                    patch = Some(CorePatch::ResultSlowWarning {
                        result_id: result.result_id.clone(),
                        invocation_id: invocation.invocation_id.clone(),
                        warned_at: now_timestamp(),
                    });
                }
                updated = Some(result.clone());
            }
            self.clear_result_deadline_indexes(result_id);
            if let Some(result) = updated.as_ref() {
                self.index_started_result_deadlines(result);
            }
            if let Some(patch) = patch {
                self.store.append_patch(patch);
            }
        }
        due
    }

    pub fn emit_due_event_slow_warnings(&mut self) -> Vec<String> {
        let due: Vec<String> = Self::due_deadline_ids(&self.event_slow_deadline_index)
            .into_iter()
            .filter(|event_id| {
                self.store.events.get(event_id).is_some_and(|event| {
                    event.event_status == EventStatus::Started
                        && !self.event_slow_warnings_sent.contains(event_id)
                        && self
                            .event_deadlines_by_id
                            .get(event_id)
                            .and_then(|(_, deadline)| deadline.as_deref())
                            .is_some_and(timestamp_is_past)
                })
            })
            .collect();

        for event_id in &due {
            self.event_slow_warnings_sent.insert(event_id.clone());
            self.clear_event_deadline_indexes(event_id);
            self.index_started_event_deadlines(event_id);
            self.store.append_patch(CorePatch::EventSlowWarning {
                event_id: event_id.clone(),
                warned_at: now_timestamp(),
            });
        }
        due
    }

    fn emit_due_event_slow_warning_for_event(&mut self, event_id: &str) -> bool {
        if self.event_slow_warnings_sent.contains(event_id) {
            return false;
        }
        let due = self.store.events.get(event_id).is_some_and(|event| {
            if event.event_status != EventStatus::Started {
                return false;
            }
            let Some(timeout) = self.resolved_event_slow_timeout_for_event(event) else {
                return false;
            };
            let Some(started_at) = event.event_started_at.as_deref() else {
                return false;
            };
            let Some(warning_at) = timestamp_plus_seconds(started_at, timeout) else {
                return false;
            };
            timestamp_is_past(&warning_at)
        });
        if !due {
            return false;
        }
        self.event_slow_warnings_sent.insert(event_id.to_string());
        self.clear_event_deadline_indexes(event_id);
        self.index_started_event_deadlines(event_id);
        self.store.append_patch(CorePatch::EventSlowWarning {
            event_id: event_id.to_string(),
            warned_at: now_timestamp(),
        });
        true
    }

    pub fn expire_timed_out_events(&mut self) -> Vec<String> {
        let timed_out: Vec<(String, f64)> = Self::due_deadline_ids(&self.event_deadline_index)
            .into_iter()
            .filter_map(|event_id| {
                let event = self.store.events.get(&event_id)?;
                if event.event_status != EventStatus::Started {
                    return None;
                }
                let deadline_due = self
                    .event_deadlines_by_id
                    .get(&event_id)
                    .and_then(|(deadline, _)| deadline.as_deref())
                    .is_some_and(timestamp_is_past);
                if !deadline_due {
                    return None;
                }
                let timeout = self.resolved_event_timeout_for_event(event)?;
                Some((event_id, timeout))
            })
            .collect();

        for (event_id, timeout) in &timed_out {
            self.finalize_event_timeout(event_id, *timeout);
        }
        timed_out
            .into_iter()
            .map(|(event_id, _timeout)| event_id)
            .collect()
    }

    fn resolved_event_timeout_for_event(&self, event: &EventRecord) -> Option<f64> {
        self.store
            .routes_by_event
            .get(&event.event_id)
            .into_iter()
            .flatten()
            .filter_map(|route_id| self.store.routes.get(route_id))
            .filter_map(|route| self.store.buses.get(&route.bus_id))
            .filter_map(|bus| resolve_event_timeout(event, bus))
            .min_by(|left, right| left.total_cmp(right))
    }

    fn resolved_event_slow_timeout_for_event(&self, event: &EventRecord) -> Option<f64> {
        self.store
            .routes_by_event
            .get(&event.event_id)
            .into_iter()
            .flatten()
            .filter_map(|route_id| self.store.routes.get(route_id))
            .filter_map(|route| self.store.buses.get(&route.bus_id))
            .filter_map(|bus| resolve_event_slow_timeout(event, bus))
            .min_by(|left, right| left.total_cmp(right))
    }

    fn finalize_event_timeout(&mut self, event_id: &str, timeout: f64) {
        self.clear_event_deadline_indexes(event_id);
        let blocking_child_ids = self.blocking_child_event_ids(event_id);
        let route_ids = self
            .store
            .routes_by_event
            .get(event_id)
            .cloned()
            .unwrap_or_default();
        let mut result_patches = Vec::new();
        let mut cancel_snapshots = Vec::new();
        for route_id in &route_ids {
            let result_ids = self
                .store
                .results_by_route
                .get(route_id)
                .cloned()
                .unwrap_or_default();
            for result_id in result_ids {
                let Some(result) = self.store.results.get_mut(&result_id) else {
                    continue;
                };
                match result.status {
                    ResultStatus::Pending => {
                        result.status = ResultStatus::Cancelled;
                        result.error = Some(CoreError::new(
                            CoreErrorKind::HandlerCancelled,
                            format!("Cancelled pending handler: event timed out after {timeout}s"),
                        ));
                        result.completed_at = Some(now_timestamp());
                        result_patches.push(CorePatch::ResultCancelled {
                            result: result.clone(),
                            reason: CancelReason::EventTimeout,
                        });
                    }
                    ResultStatus::Started => {
                        cancel_snapshots.push(result.clone());
                        result.status = ResultStatus::Error;
                        result.error = Some(CoreError::new(
                            CoreErrorKind::HandlerAborted,
                            format!("Aborted running handler: event timed out after {timeout}s"),
                        ));
                        result.completed_at = Some(now_timestamp());
                        if let Some(invocation) = &mut result.invocation {
                            invocation.cancel_reason = Some(CancelReason::EventTimeout);
                        }
                        result.invocation = None;
                        result_patches.push(CorePatch::ResultCancelled {
                            result: result.clone(),
                            reason: CancelReason::EventTimeout,
                        });
                    }
                    ResultStatus::Completed | ResultStatus::Error | ResultStatus::Cancelled => {}
                }
                self.clear_result_deadline_indexes(&result_id);
                self.advance_route_after_result(&result_id);
            }
            let _ = self.release_route_event_lock(route_id);
            if let Some(route) = self.store.routes.get_mut(route_id) {
                if route.status != RouteStatus::Completed {
                    route.status = RouteStatus::Completed;
                    self.store.append_patch(CorePatch::RouteCompleted {
                        route_id: route_id.clone(),
                    });
                }
            }
            self.remove_route_from_bus_schedule_index(route_id);
        }
        for result in &cancel_snapshots {
            self.enqueue_cancel_for_result(result, CancelReason::EventTimeout);
        }
        for patch in result_patches {
            self.store.append_patch(patch);
        }
        let mut completed_patch_at = None;
        if let Some(event) = self.store.events.get_mut(event_id) {
            if event.event_status != EventStatus::Completed {
                event.event_status = EventStatus::Completed;
                let completed_at = now_timestamp();
                event.event_completed_at = Some(completed_at.clone());
                completed_patch_at = Some(completed_at);
            }
        }
        if let Some(completed_at) = completed_patch_at {
            if let Ok(completed_patch) =
                self.event_completed_patch_for_event(event_id, completed_at)
            {
                self.store.append_patch(completed_patch);
            }
        }
        for child_id in blocking_child_ids {
            let mut visited = BTreeSet::new();
            self.finalize_event_parent_timeout(&child_id, event_id, &mut visited);
        }
    }

    fn blocking_child_event_ids(&self, event_id: &str) -> Vec<String> {
        self.store
            .routes_by_event
            .get(event_id)
            .into_iter()
            .flatten()
            .filter_map(|route_id| self.store.results_by_route.get(route_id))
            .flatten()
            .filter_map(|result_id| self.store.results.get(result_id))
            .flat_map(|result| result.event_children.iter())
            .filter_map(|child_id| self.store.events.get(child_id))
            .filter(|child| child.event_blocks_parent_completion)
            .filter(|child| child.event_status != EventStatus::Completed)
            .map(|child| child.event_id.clone())
            .collect()
    }

    fn finalize_event_parent_timeout(
        &mut self,
        event_id: &str,
        parent_event_id: &str,
        visited: &mut BTreeSet<String>,
    ) {
        if !visited.insert(event_id.to_string()) {
            return;
        }
        self.clear_event_deadline_indexes(event_id);
        let blocking_child_ids = self.blocking_child_event_ids(event_id);
        let route_ids = self
            .store
            .routes_by_event
            .get(event_id)
            .cloned()
            .unwrap_or_default();
        let mut result_patches = Vec::new();
        let mut cancel_snapshots = Vec::new();
        for route_id in &route_ids {
            let result_ids = self
                .store
                .results_by_route
                .get(route_id)
                .cloned()
                .unwrap_or_default();
            for result_id in result_ids {
                let Some(result) = self.store.results.get_mut(&result_id) else {
                    continue;
                };
                match result.status {
                    ResultStatus::Pending => {
                        result.status = ResultStatus::Cancelled;
                        result.error = Some(CoreError::new(
                            CoreErrorKind::HandlerCancelled,
                            format!(
                                "Cancelled pending handler: parent event {parent_event_id} timed out before child completed"
                            ),
                        ));
                        result.completed_at = Some(now_timestamp());
                        result_patches.push(CorePatch::ResultCancelled {
                            result: result.clone(),
                            reason: CancelReason::ParentTimeout,
                        });
                    }
                    ResultStatus::Started => {
                        cancel_snapshots.push(result.clone());
                        result.status = ResultStatus::Error;
                        result.error = Some(CoreError::new(
                            CoreErrorKind::HandlerAborted,
                            format!(
                                "Aborted running handler: parent event {parent_event_id} timed out before child completed"
                            ),
                        ));
                        result.completed_at = Some(now_timestamp());
                        if let Some(invocation) = &mut result.invocation {
                            invocation.cancel_reason = Some(CancelReason::ParentTimeout);
                        }
                        result.invocation = None;
                        result_patches.push(CorePatch::ResultCancelled {
                            result: result.clone(),
                            reason: CancelReason::ParentTimeout,
                        });
                    }
                    ResultStatus::Completed | ResultStatus::Error | ResultStatus::Cancelled => {}
                }
                self.clear_result_deadline_indexes(&result_id);
                self.advance_route_after_result(&result_id);
            }
            let _ = self.release_route_event_lock(route_id);
            if let Some(route) = self.store.routes.get_mut(route_id) {
                if route.status != RouteStatus::Completed {
                    route.status = RouteStatus::Completed;
                    self.store.append_patch(CorePatch::RouteCompleted {
                        route_id: route_id.clone(),
                    });
                }
            }
            self.remove_route_from_bus_schedule_index(route_id);
        }
        for result in cancel_snapshots {
            self.enqueue_cancel_for_result(&result, CancelReason::ParentTimeout);
        }
        for patch in result_patches {
            self.store.append_patch(patch);
        }
        let mut completed_patch_at = None;
        if let Some(event) = self.store.events.get_mut(event_id) {
            if event.event_status != EventStatus::Completed {
                event.event_status = EventStatus::Completed;
                let completed_at = now_timestamp();
                event.event_completed_at = Some(completed_at.clone());
                completed_patch_at = Some(completed_at);
            }
        }
        if let Some(completed_at) = completed_patch_at {
            if let Ok(completed_patch) =
                self.event_completed_patch_for_event(event_id, completed_at)
            {
                self.store.append_patch(completed_patch);
            }
        }
        for child_id in blocking_child_ids {
            self.finalize_event_parent_timeout(&child_id, event_id, visited);
        }
    }

    pub fn mark_child_blocks_parent_completion(
        &mut self,
        parent_invocation_id: &str,
        child_event_id: &str,
    ) -> Result<(), CoreErrorState> {
        let parent_result_id = self
            .store
            .results
            .values()
            .find(|result| {
                result
                    .invocation
                    .as_ref()
                    .is_some_and(|invocation| invocation.invocation_id == parent_invocation_id)
            })
            .map(|result| result.result_id.clone())
            .ok_or_else(|| CoreErrorState::MissingResult(parent_invocation_id.to_string()))?;

        let child = self
            .store
            .events
            .get_mut(child_event_id)
            .ok_or_else(|| CoreErrorState::MissingEvent(child_event_id.to_string()))?;
        child.event_blocks_parent_completion = true;
        if child.event_emitted_by_result_id.is_none() {
            child.event_emitted_by_result_id = Some(parent_result_id.clone());
        }

        let result = self
            .store
            .results
            .get_mut(&parent_result_id)
            .ok_or_else(|| CoreErrorState::MissingResult(parent_result_id.clone()))?;
        if !result
            .event_children
            .iter()
            .any(|event_id| event_id == child_event_id)
        {
            result.event_children.push(child_event_id.to_string());
        }
        self.store.append_patch(CorePatch::ChildBlocksParent {
            parent_invocation_id: parent_invocation_id.to_string(),
            child_event_id: child_event_id.to_string(),
        });
        Ok(())
    }

    pub fn build_queue_jump_context(
        &self,
        parent_invocation_id: &str,
        depth: usize,
    ) -> Result<QueueJumpContext, CoreErrorState> {
        let parent_result = self
            .store
            .results
            .values()
            .find(|result| {
                result
                    .invocation
                    .as_ref()
                    .is_some_and(|invocation| invocation.invocation_id == parent_invocation_id)
            })
            .ok_or_else(|| CoreErrorState::MissingResult(parent_invocation_id.to_string()))?;
        let borrowed_event_locks = self
            .store
            .routes
            .get(&parent_result.route_id)
            .and_then(|route| route.event_lock_lease.clone())
            .into_iter()
            .collect();
        Ok(QueueJumpContext {
            parent_invocation_id: parent_invocation_id.to_string(),
            initiating_bus_id: parent_result.bus_id.clone(),
            borrowed_event_locks,
            depth,
        })
    }

    pub fn process_queue_jump_event(
        &mut self,
        parent_invocation_id: &str,
        child_event_id: &str,
        block_parent_completion: bool,
        pause_parent_route: bool,
    ) -> Result<Vec<RouteStep>, CoreErrorState> {
        if block_parent_completion {
            self.mark_child_blocks_parent_completion(parent_invocation_id, child_event_id)?;
        }
        if pause_parent_route {
            self.pause_serial_route_for_invocation(parent_invocation_id)?;
        }
        let qj = self.build_queue_jump_context(parent_invocation_id, 0)?;
        let route_ids = self.queue_jump_route_ids(parent_invocation_id, child_event_id)?;
        let mut steps = Vec::new();
        for route_id in route_ids {
            let step = self.process_route_step_limited(
                &route_id,
                Some(&qj),
                Some(ROUTE_INVOCATION_SLICE_LIMIT),
            )?;
            let route_terminal = self.store.routes.get(&route_id).is_none_or(|route| {
                matches!(
                    route.status,
                    RouteStatus::Completed | RouteStatus::Cancelled
                )
            });
            if !matches!(step, RouteStep::NoWork { .. }) {
                steps.push(step);
            }
            if !route_terminal {
                break;
            }
        }
        if block_parent_completion || pause_parent_route {
            for step in self.process_independent_host_routes_for_queue_jump(
                &qj,
                child_event_id,
                Some(ROUTE_INVOCATION_SLICE_LIMIT),
            )? {
                steps.push(step);
            }
        }
        Ok(steps)
    }

    fn process_independent_host_routes_for_queue_jump(
        &mut self,
        qj: &QueueJumpContext,
        child_event_id: &str,
        limit: Option<usize>,
    ) -> Result<Vec<RouteStep>, CoreErrorState> {
        let host_id = self
            .store
            .buses
            .get(&qj.initiating_bus_id)
            .map(|bus| bus.host_id.clone())
            .ok_or_else(|| CoreErrorState::MissingBus(qj.initiating_bus_id.clone()))?;
        let child_bus_ids = self
            .routes_for_event_path_order(child_event_id)
            .into_iter()
            .filter_map(|route_id| {
                self.store
                    .routes
                    .get(&route_id)
                    .map(|route| route.bus_id.clone())
            })
            .collect::<BTreeSet<_>>();
        let mut bus_ids = self
            .store
            .buses
            .values()
            .filter(|bus| {
                bus.host_id == host_id
                    && bus.bus_id != qj.initiating_bus_id
                    && !child_bus_ids.contains(&bus.bus_id)
            })
            .map(|bus| (bus.label.clone(), bus.bus_id.clone()))
            .collect::<Vec<_>>();
        bus_ids.sort();

        let mut steps = Vec::new();
        for (_, bus_id) in bus_ids {
            for step in self.process_available_routes_for_bus_limited(&bus_id, limit)? {
                steps.push(step);
            }
        }
        Ok(steps)
    }

    fn queue_jump_route_ids(
        &self,
        parent_invocation_id: &str,
        child_event_id: &str,
    ) -> Result<Vec<String>, CoreErrorState> {
        let mut route_ids = self.routes_for_event_path_order(child_event_id);
        if !route_ids
            .iter()
            .any(|route_id| self.route_is_parallel_event(route_id).unwrap_or(false))
        {
            return Ok(route_ids);
        }

        let parent_result_id = self
            .store
            .results
            .values()
            .find(|result| {
                result
                    .invocation
                    .as_ref()
                    .is_some_and(|invocation| invocation.invocation_id == parent_invocation_id)
            })
            .map(|result| result.result_id.clone())
            .ok_or_else(|| CoreErrorState::MissingResult(parent_invocation_id.to_string()))?;
        let mut seen = route_ids.iter().cloned().collect::<BTreeSet<_>>();
        let mut sibling_event_ids = self
            .store
            .events
            .values()
            .filter(|event| {
                event.event_id != child_event_id
                    && event.event_emitted_by_result_id.as_deref()
                        == Some(parent_result_id.as_str())
                    && event.event_status != EventStatus::Completed
            })
            .map(|event| event.event_id.clone())
            .collect::<Vec<_>>();
        sibling_event_ids.sort();

        for sibling_event_id in sibling_event_ids {
            for route_id in self.routes_for_event_path_order(&sibling_event_id) {
                if !self.route_is_parallel_event(&route_id)? {
                    continue;
                }
                if seen.insert(route_id.clone()) {
                    route_ids.push(route_id);
                }
            }
        }
        Ok(route_ids)
    }

    fn route_is_parallel_event(&self, route_id: &str) -> Result<bool, CoreErrorState> {
        Ok(self
            .route_event_concurrency(route_id)?
            .is_some_and(|mode| mode == EventConcurrency::Parallel))
    }

    pub fn pause_serial_route_for_invocation(
        &mut self,
        invocation_id: &str,
    ) -> Result<Option<String>, CoreErrorState> {
        let route_id = self
            .store
            .results
            .values()
            .find(|result| {
                result
                    .invocation
                    .as_ref()
                    .is_some_and(|invocation| invocation.invocation_id == invocation_id)
            })
            .map(|result| result.route_id.clone());
        let Some(route_id) = route_id else {
            return Ok(None);
        };
        self.pause_serial_route(&route_id, invocation_id.to_string())?;
        Ok(Some(route_id))
    }

    pub fn routes_for_event_path_order(&self, event_id: &str) -> Vec<String> {
        let Some(event) = self.store.events.get(event_id) else {
            return Vec::new();
        };
        let mut route_ids = self
            .store
            .routes_by_event
            .get(event_id)
            .cloned()
            .unwrap_or_default();
        route_ids.sort_by_key(|route_id| {
            let Some(route) = self.store.routes.get(route_id) else {
                return (usize::MAX, u64::MAX);
            };
            let label_index = self
                .store
                .buses
                .get(&route.bus_id)
                .and_then(|bus| {
                    event
                        .event_path
                        .iter()
                        .position(|label| label == &bus.label)
                })
                .unwrap_or(usize::MAX);
            (label_index, route.route_seq)
        });
        route_ids
    }

    pub fn pause_serial_route(
        &mut self,
        route_id: &str,
        invocation_id: InvocationId,
    ) -> Result<(), CoreErrorState> {
        let route = self
            .store
            .routes
            .get_mut(route_id)
            .ok_or_else(|| CoreErrorState::MissingRoute(route_id.to_string()))?;
        let bus_id = route.bus_id.clone();
        route.serial_handler_paused_by = Some(invocation_id.clone());
        self.paused_serial_routes_by_bus
            .entry(bus_id)
            .or_default()
            .insert(route_id.to_string());
        self.store.append_patch(CorePatch::SerialRoutePaused {
            route_id: route_id.to_string(),
            invocation_id,
        });
        Ok(())
    }

    pub fn unpause_serial_route(
        &mut self,
        route_id: &str,
        invocation_id: &str,
    ) -> Result<(), CoreErrorState> {
        let route = self
            .store
            .routes
            .get_mut(route_id)
            .ok_or_else(|| CoreErrorState::MissingRoute(route_id.to_string()))?;
        if route.serial_handler_paused_by.as_deref() == Some(invocation_id) {
            let bus_id = route.bus_id.clone();
            route.serial_handler_paused_by = None;
            if let Some(route_ids) = self.paused_serial_routes_by_bus.get_mut(&bus_id) {
                route_ids.remove(route_id);
                if route_ids.is_empty() {
                    self.paused_serial_routes_by_bus.remove(&bus_id);
                }
            }
            self.store.append_patch(CorePatch::SerialRouteResumed {
                route_id: route_id.to_string(),
                invocation_id: invocation_id.to_string(),
            });
        }
        Ok(())
    }

    pub fn complete_event_if_done(&mut self, event_id: &str) -> Result<bool, CoreErrorState> {
        if !event_can_complete(&self.store, event_id) {
            return Ok(false);
        }
        self.complete_event(event_id)
    }

    fn complete_event_if_route_done(
        &mut self,
        event_id: &str,
        route_id: &str,
    ) -> Result<bool, CoreErrorState> {
        let route_result_ids = self
            .store
            .results_by_route
            .get(route_id)
            .cloned()
            .unwrap_or_default();
        if !route_result_ids.iter().all(|result_id| {
            self.store
                .results
                .get(result_id)
                .is_some_and(|result| result.status.is_terminal())
        }) {
            return Ok(false);
        }
        if !event_can_complete(&self.store, event_id) {
            return Ok(false);
        }
        let completed = self.complete_event(event_id)?;
        self.complete_parent_events_unblocked_by(event_id)?;
        Ok(completed)
    }

    fn event_slow_warning_is_due_by(&self, event_id: &str, now: &str) -> bool {
        if self.event_slow_warnings_sent.contains(event_id) {
            return false;
        }
        let Some(event) = self.store.events.get(event_id) else {
            return false;
        };
        if event.event_status != EventStatus::Started {
            return false;
        }
        let Some(timeout) = self.resolved_event_slow_timeout_for_event(event) else {
            return false;
        };
        let Some(started_at) = event.event_started_at.as_deref() else {
            return false;
        };
        let Some(warning_at) = timestamp_plus_seconds(started_at, timeout) else {
            return false;
        };
        warning_at.as_str() < now
    }

    fn complete_event(&mut self, event_id: &str) -> Result<bool, CoreErrorState> {
        let has_results = self
            .store
            .routes_by_event
            .get(event_id)
            .into_iter()
            .flatten()
            .any(|route_id| {
                self.store
                    .results_by_route
                    .get(route_id)
                    .is_some_and(|result_ids| !result_ids.is_empty())
            });
        let completed_at = if has_results {
            now_timestamp()
        } else {
            self.store
                .events
                .get(event_id)
                .and_then(|event| event.event_started_at.clone())
                .unwrap_or_else(now_timestamp)
        };
        if self.event_slow_warning_is_due_by(event_id, &completed_at) {
            self.event_slow_warnings_sent.insert(event_id.to_string());
            self.store.append_patch(CorePatch::EventSlowWarning {
                event_id: event_id.to_string(),
                warned_at: completed_at.clone(),
            });
        }
        self.clear_event_deadline_indexes(event_id);
        let event = self
            .store
            .events
            .get_mut(event_id)
            .ok_or_else(|| CoreErrorState::MissingEvent(event_id.to_string()))?;
        event.event_status = EventStatus::Completed;
        if event.event_started_at.is_none() {
            event.event_started_at = Some(completed_at.clone());
        }
        event.event_completed_at = Some(completed_at.clone());
        let completed_patch = self.event_completed_patch_for_event(event_id, completed_at)?;
        self.store.append_patch(completed_patch);
        let affected_bus_ids = self
            .store
            .routes_by_event
            .get(event_id)
            .into_iter()
            .flatten()
            .filter_map(|route_id| self.store.routes.get(route_id))
            .map(|route| route.bus_id.clone())
            .collect::<BTreeSet<_>>();
        for bus_id in affected_bus_ids {
            self.apply_history_policy_for_bus(&bus_id);
        }
        self.prune_event_if_unreferenced(event_id);
        Ok(true)
    }

    fn complete_parent_events_unblocked_by(
        &mut self,
        child_event_id: &str,
    ) -> Result<(), CoreErrorState> {
        let mut current_event_id = Some(child_event_id.to_string());
        let mut visited = BTreeSet::new();
        while let Some(event_id) = current_event_id {
            if !visited.insert(event_id.clone()) {
                break;
            }
            let Some(parent_event_id) = self
                .store
                .events
                .get(&event_id)
                .and_then(|event| event.event_parent_id.clone())
            else {
                break;
            };
            if self
                .store
                .events
                .get(&parent_event_id)
                .is_none_or(|event| event.event_status == EventStatus::Completed)
            {
                break;
            }
            self.complete_ready_routes_for_event(&parent_event_id)?;
            if !event_can_complete(&self.store, &parent_event_id) {
                break;
            }
            self.complete_event(&parent_event_id)?;
            current_event_id = Some(parent_event_id);
        }
        Ok(())
    }

    fn complete_ready_routes_for_event(&mut self, event_id: &str) -> Result<(), CoreErrorState> {
        let route_ids = self
            .store
            .routes_by_event
            .get(event_id)
            .cloned()
            .unwrap_or_default();
        for route_id in route_ids {
            let Some(route) = self.store.routes.get(&route_id) else {
                continue;
            };
            if matches!(
                route.status,
                RouteStatus::Completed | RouteStatus::Cancelled
            ) {
                continue;
            }
            if !self.route_ready_to_complete(&route_id) {
                continue;
            }
            self.release_route_event_lock(&route_id)?;
            if let Some(route) = self.store.routes.get_mut(&route_id) {
                if route.status != RouteStatus::Completed {
                    route.status = RouteStatus::Completed;
                    self.store.append_patch(CorePatch::RouteCompleted {
                        route_id: route_id.clone(),
                    });
                }
            }
            self.remove_route_from_bus_schedule_index(&route_id);
        }
        Ok(())
    }

    fn compact_completed_event_routes(&mut self, event_id: &str) {
        let route_ids = self
            .store
            .routes_by_event
            .get(event_id)
            .cloned()
            .unwrap_or_default();
        if route_ids.is_empty() {
            return;
        }
        let snapshots = self.event_result_snapshots_for_event(event_id);
        if let Some(event) = self.store.events.get_mut(event_id) {
            event.completed_result_snapshots.extend(snapshots);
        }
        for route_id in &route_ids {
            let _ = self.release_route_event_lock(route_id);
            self.locks.remove_owner(&format!("route:{route_id}"));
            self.remove_route_from_bus_schedule_index(route_id);
            self.remove_results_for_route(route_id);
            self.store.routes.remove(route_id);
        }
        self.store.routes_by_event.remove(event_id);
    }

    fn route_ready_to_complete(&self, route_id: &str) -> bool {
        let Some(route) = self.store.routes.get(route_id) else {
            return false;
        };
        let Some(event) = self.store.events.get(&route.event_id) else {
            return false;
        };
        let expected_handlers = self
            .store
            .handlers_for_bus(&route.bus_id)
            .into_iter()
            .filter(|handler| {
                handler.event_pattern == "*" || handler.event_pattern == event.event_type
            })
            .count();
        let result_ids = self
            .store
            .results_by_route
            .get(route_id)
            .cloned()
            .unwrap_or_default();
        if result_ids.len() < expected_handlers {
            return false;
        }
        result_ids.iter().all(|result_id| {
            self.store
                .results
                .get(result_id)
                .is_some_and(|result| result.status.is_terminal())
        })
    }

    fn route_event_depth(&self, route_id: &str) -> usize {
        let mut depth = 0;
        let mut visited = BTreeSet::new();
        let mut current_event_id = self
            .store
            .routes
            .get(route_id)
            .map(|route| route.event_id.clone());
        while let Some(event_id) = current_event_id {
            if !visited.insert(event_id.clone()) {
                break;
            }
            let Some(parent_id) = self
                .store
                .events
                .get(&event_id)
                .and_then(|event| event.event_parent_id.clone())
            else {
                break;
            };
            depth += 1;
            current_event_id = Some(parent_id);
        }
        depth
    }

    fn advance_route_after_result(&mut self, result_id: &str) {
        let Some(result) = self.store.results.get(result_id).cloned() else {
            return;
        };
        let Some(route) = self.store.routes.get_mut(&result.route_id) else {
            return;
        };
        if result.handler_seq == route.handler_cursor && result.status.is_terminal() {
            route.handler_cursor += 1;
        }
    }
}

fn handler_slow_warning_is_due(result: &EventResultRecord) -> bool {
    if result.slow_warning_sent {
        return false;
    }
    if let Some(deadline) = result.slow_warning_at.as_deref() {
        return timestamp_is_past(deadline);
    }
    let (Some(started_at), Some(slow_timeout)) =
        (result.started_at.as_deref(), result.slow_timeout)
    else {
        return false;
    };
    timestamp_plus_seconds(started_at, slow_timeout)
        .as_deref()
        .is_some_and(timestamp_is_past)
}

fn handler_outcome_is_event_timeout_abort(outcome: &HandlerOutcome) -> bool {
    let HandlerOutcome::Errored { error } = outcome else {
        return false;
    };
    if error.kind != CoreErrorKind::HandlerAborted {
        return false;
    }
    let message = error.message.to_lowercase();
    if message.contains("parent error") || message.contains("parent event") {
        return false;
    }
    if message.contains("event timeout") || message.contains("event timed out") {
        return true;
    }
    let Some(details) = error
        .details
        .as_ref()
        .and_then(|details| details.as_object())
    else {
        return false;
    };
    details
        .get("cause_name")
        .and_then(|value| value.as_str())
        .is_some_and(|name| name == "EventHandlerTimeoutError")
}

fn handler_outcome_timeout_seconds(outcome: &HandlerOutcome) -> Option<f64> {
    let HandlerOutcome::Errored { error } = outcome else {
        return None;
    };
    let details = error.details.as_ref()?.as_object()?;
    details
        .get("timeout_seconds")
        .or_else(|| details.get("cause_timeout_seconds"))
        .and_then(|value| value.as_f64())
}

fn next_imported_handler_cursor(results: &[EventResultRecord]) -> usize {
    let mut cursor = 0;
    for result in results {
        if result.handler_seq != cursor {
            break;
        }
        if result.status.is_terminal() {
            cursor += 1;
        } else {
            break;
        }
    }
    cursor
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        envelope::EventEnvelope,
        protocol::{CoreToHostMessage, HostHandlerOutcome, HostToCoreMessage},
        store::{BusDefaults, BusRecord, HandlerRecord},
        types::{EventConcurrency, HandlerCompletion, HandlerConcurrency},
    };
    use serde_json::json;

    #[test]
    fn parallel_fanout_progresses_across_bounded_slices() {
        let mut core = Core::default();
        let bus = BusRecord {
            bus_id: "bus".to_string(),
            name: "Bus".to_string(),
            label: "Bus".to_string(),
            defaults: BusDefaults {
                event_concurrency: EventConcurrency::BusSerial,
                event_handler_concurrency: HandlerConcurrency::Parallel,
                event_handler_completion: HandlerCompletion::All,
                event_timeout: Some(60.0),
                event_slow_timeout: Some(300.0),
                event_handler_timeout: None,
                event_handler_slow_timeout: Some(30.0),
            },
            host_id: "host".to_string(),
            max_history_size: Some(128),
            max_history_drop: true,
        };
        core.insert_bus(bus.clone());
        for i in 0..2_000 {
            core.insert_handler(HandlerRecord {
                handler_id: format!("handler-{i}"),
                bus_id: bus.bus_id.clone(),
                host_id: "host".to_string(),
                event_pattern: "Fanout".to_string(),
                handler_name: format!("handler-{i}"),
                handler_file_path: None,
                handler_registered_at: now_timestamp(),
                handler_timeout: None,
                handler_slow_timeout: None,
                handler_concurrency: Some(HandlerConcurrency::Parallel),
                handler_completion: Some(HandlerCompletion::All),
            });
        }

        let event = EventEnvelope {
            event_type: "Fanout".to_string(),
            event_version: "0.0.1".to_string(),
            event_timeout: None,
            event_slow_timeout: None,
            event_concurrency: None,
            event_handler_timeout: None,
            event_handler_slow_timeout: None,
            event_handler_concurrency: Some(HandlerConcurrency::Parallel),
            event_handler_completion: Some(HandlerCompletion::All),
            event_blocks_parent_completion: false,
            event_result_type: None,
            event_id: "event".to_string(),
            event_path: vec!["Bus".to_string()],
            event_parent_id: None,
            event_emitted_by_result_id: None,
            event_emitted_by_handler_id: None,
            event_created_at: now_timestamp(),
            event_status: EventStatus::Pending,
            event_started_at: None,
            event_completed_at: None,
            payload: Default::default(),
        };
        let route_id = core
            .emit_to_bus_with_patch(event.into_record().unwrap(), &bus.bus_id, false)
            .unwrap();
        let mut seen = 0usize;

        loop {
            let step = core
                .process_next_route_for_bus_limited(&bus.bus_id, Some(512))
                .unwrap()
                .unwrap_or_else(|| {
                    let route = core.store.routes.get(&route_id).cloned();
                    let result_count = core
                        .store
                        .results_by_route
                        .get(&route_id)
                        .map(|ids| ids.len())
                        .unwrap_or(0);
                    panic!(
                        "route should progress; seen={seen} result_count={result_count} route={route:?}"
                    )
                });
            match step {
                RouteStep::InvokeHandlers { invocations, .. } => {
                    seen += invocations.len();
                    for invocation in invocations {
                        core.accept_handler_outcome(
                            &invocation.result_id,
                            &invocation.invocation_id,
                            invocation.fence,
                            HandlerOutcome::Completed {
                                value: json!(null),
                                result_is_event_reference: false,
                                result_is_undefined: true,
                            },
                        )
                        .unwrap();
                    }
                    if matches!(
                        core.process_route_step_limited(&route_id, None, Some(0))
                            .unwrap(),
                        RouteStep::RouteCompleted { .. }
                    ) {
                        break;
                    }
                }
                RouteStep::RouteCompleted {
                    event_completed, ..
                } => {
                    assert!(event_completed);
                    break;
                }
                other => panic!("unexpected route step: {other:?}"),
            }
        }

        assert_eq!(seen, 2_000);
        assert_eq!(
            core.store.events.get("event").unwrap().event_status,
            EventStatus::Completed
        );
    }

    #[test]
    fn protocol_compact_fanout_progresses_across_bounded_slices() {
        let mut core = Core::default();
        let bus = BusRecord {
            bus_id: "bus".to_string(),
            name: "Bus".to_string(),
            label: "Bus".to_string(),
            defaults: BusDefaults {
                event_concurrency: EventConcurrency::BusSerial,
                event_handler_concurrency: HandlerConcurrency::Parallel,
                event_handler_completion: HandlerCompletion::All,
                event_timeout: Some(60.0),
                event_slow_timeout: Some(300.0),
                event_handler_timeout: None,
                event_handler_slow_timeout: Some(30.0),
            },
            host_id: "host".to_string(),
            max_history_size: Some(128),
            max_history_drop: true,
        };
        core.handle_host_message_for_host_since(
            "host",
            HostToCoreMessage::RegisterBus { bus: bus.clone() },
            Some(0),
        )
        .unwrap();
        let mut patch_seq = core.latest_patch_seq();
        for i in 0..2_000 {
            core.handle_host_message_for_host_since(
                "host",
                HostToCoreMessage::RegisterHandler {
                    handler: HandlerRecord {
                        handler_id: format!("handler-{i}"),
                        bus_id: bus.bus_id.clone(),
                        host_id: "host".to_string(),
                        event_pattern: "Fanout".to_string(),
                        handler_name: format!("handler-{i}"),
                        handler_file_path: None,
                        handler_registered_at: now_timestamp(),
                        handler_timeout: None,
                        handler_slow_timeout: None,
                        handler_concurrency: Some(HandlerConcurrency::Parallel),
                        handler_completion: Some(HandlerCompletion::All),
                    },
                },
                Some(patch_seq),
            )
            .unwrap();
            patch_seq = core.latest_patch_seq();
        }
        let event = EventEnvelope {
            event_type: "Fanout".to_string(),
            event_version: "0.0.1".to_string(),
            event_timeout: None,
            event_slow_timeout: None,
            event_concurrency: None,
            event_handler_timeout: None,
            event_handler_slow_timeout: None,
            event_handler_concurrency: Some(HandlerConcurrency::Parallel),
            event_handler_completion: Some(HandlerCompletion::All),
            event_blocks_parent_completion: false,
            event_result_type: None,
            event_id: "event".to_string(),
            event_path: vec!["Bus".to_string()],
            event_parent_id: None,
            event_emitted_by_result_id: None,
            event_emitted_by_handler_id: None,
            event_created_at: now_timestamp(),
            event_status: EventStatus::Pending,
            event_started_at: None,
            event_completed_at: None,
            payload: Default::default(),
        };
        core.handle_host_message_for_host_since(
            "host",
            HostToCoreMessage::EmitEvent {
                event,
                bus_id: bus.bus_id.clone(),
                defer_start: true,
                compact_response: true,
                parent_invocation_id: None,
                block_parent_completion: false,
                pause_parent_route: false,
            },
            Some(patch_seq),
        )
        .unwrap();
        patch_seq = core.latest_patch_seq();
        let mut seen = 0usize;
        let extract_invocations = |messages: Vec<CoreToHostMessage>| {
            messages
                .into_iter()
                .flat_map(|message| match message {
                    CoreToHostMessage::InvokeHandler(invocation) => vec![(
                        invocation.invocation_id,
                        invocation.result_id,
                        invocation.fence,
                    )],
                    CoreToHostMessage::InvokeHandlersCompact { invocations, .. } => invocations
                        .into_iter()
                        .map(|invocation| (invocation.0, invocation.1, invocation.3))
                        .collect(),
                    _ => Vec::new(),
                })
                .collect::<Vec<_>>()
        };
        let mut pending_invocations = Vec::new();

        loop {
            if pending_invocations.is_empty() {
                let messages = core
                    .handle_host_message_for_host_since(
                        "host",
                        HostToCoreMessage::ProcessNextRoute {
                            bus_id: bus.bus_id.clone(),
                            limit: Some(512),
                            compact_response: true,
                        },
                        Some(patch_seq),
                    )
                    .unwrap();
                patch_seq = core.latest_patch_seq();
                pending_invocations = extract_invocations(messages);
                if pending_invocations.is_empty() {
                    break;
                }
            }
            seen += pending_invocations.len();
            let outcomes = pending_invocations
                .drain(..)
                .map(|(invocation_id, result_id, fence)| HostHandlerOutcome {
                    result_id,
                    invocation_id,
                    fence,
                    outcome: HandlerOutcome::Completed {
                        value: json!(null),
                        result_is_event_reference: false,
                        result_is_undefined: true,
                    },
                    process_available_after: false,
                })
                .collect::<Vec<_>>();
            let messages = core
                .handle_host_message_for_host_since(
                    "host",
                    HostToCoreMessage::HandlerOutcomes {
                        outcomes,
                        compact_response: true,
                    },
                    Some(patch_seq),
                )
                .unwrap();
            patch_seq = core.latest_patch_seq();
            pending_invocations = extract_invocations(messages);
        }

        assert_eq!(seen, 2_000);
        assert_eq!(
            core.store.events.get("event").unwrap().event_status,
            EventStatus::Completed
        );
    }
}
