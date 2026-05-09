use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    locks::LockLease,
    protocol::{CoreEventResultSnapshot, CorePatch},
    types::{
        BusId, CoreError, EventConcurrency, EventId, EventStatus, HandlerCompletion,
        HandlerConcurrency, HandlerId, InvocationId, ResultId, ResultStatus, RouteId, RouteStatus,
        Timestamp,
    },
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BusDefaults {
    pub event_concurrency: EventConcurrency,
    pub event_handler_concurrency: HandlerConcurrency,
    pub event_handler_completion: HandlerCompletion,
    pub event_timeout: Option<f64>,
    pub event_slow_timeout: Option<f64>,
    pub event_handler_timeout: Option<f64>,
    pub event_handler_slow_timeout: Option<f64>,
}

impl Default for BusDefaults {
    fn default() -> Self {
        Self {
            event_concurrency: EventConcurrency::BusSerial,
            event_handler_concurrency: HandlerConcurrency::Serial,
            event_handler_completion: HandlerCompletion::All,
            event_timeout: Some(60.0),
            event_slow_timeout: Some(300.0),
            event_handler_timeout: None,
            event_handler_slow_timeout: Some(30.0),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BusRecord {
    pub bus_id: BusId,
    pub name: String,
    pub label: String,
    pub defaults: BusDefaults,
    pub host_id: String,
    #[serde(default = "default_max_history_size")]
    pub max_history_size: Option<usize>,
    #[serde(default)]
    pub max_history_drop: bool,
}

fn default_max_history_size() -> Option<usize> {
    Some(100)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HandlerRecord {
    pub handler_id: HandlerId,
    pub bus_id: BusId,
    pub host_id: String,
    pub event_pattern: String,
    pub handler_name: String,
    pub handler_file_path: Option<String>,
    pub handler_registered_at: Timestamp,
    pub handler_timeout: Option<f64>,
    pub handler_slow_timeout: Option<f64>,
    pub handler_concurrency: Option<HandlerConcurrency>,
    pub handler_completion: Option<HandlerCompletion>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventRecord {
    pub event_id: EventId,
    pub event_type: String,
    pub event_version: String,
    pub payload: Value,
    pub event_result_schema: Option<Value>,

    pub event_timeout: Option<f64>,
    pub event_slow_timeout: Option<f64>,
    pub event_concurrency: Option<EventConcurrency>,
    pub event_handler_timeout: Option<f64>,
    pub event_handler_slow_timeout: Option<f64>,
    pub event_handler_concurrency: Option<HandlerConcurrency>,
    pub event_handler_completion: Option<HandlerCompletion>,

    pub event_parent_id: Option<EventId>,
    pub event_emitted_by_result_id: Option<ResultId>,
    pub event_emitted_by_handler_id: Option<HandlerId>,
    pub event_blocks_parent_completion: bool,
    pub event_path: Vec<String>,
    pub event_created_at: Timestamp,
    pub event_started_at: Option<Timestamp>,
    pub event_completed_at: Option<Timestamp>,
    pub event_status: EventStatus,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub completed_result_snapshots: BTreeMap<HandlerId, CoreEventResultSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventRouteRecord {
    pub route_id: RouteId,
    pub event_id: EventId,
    pub bus_id: BusId,
    pub route_seq: u64,
    pub status: RouteStatus,
    pub handler_cursor: usize,
    pub serial_handler_paused_by: Option<InvocationId>,
    pub event_lock_lease: Option<LockLease>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InvocationState {
    pub invocation_id: InvocationId,
    pub fence: u64,
    pub deadline_at: Option<Timestamp>,
    pub cancel_reason: Option<crate::types::CancelReason>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventResultRecord {
    pub result_id: ResultId,
    pub event_id: EventId,
    pub route_id: RouteId,
    pub bus_id: BusId,
    pub handler_id: HandlerId,
    pub handler_seq: usize,

    pub status: ResultStatus,
    pub result: Option<Value>,
    #[serde(default)]
    pub result_set: bool,
    #[serde(default)]
    pub result_is_undefined: bool,
    pub result_is_event_reference: bool,
    pub error: Option<CoreError>,
    pub started_at: Option<Timestamp>,
    pub completed_at: Option<Timestamp>,
    pub timeout: Option<f64>,
    pub slow_timeout: Option<f64>,
    pub slow_warning_at: Option<Timestamp>,
    pub slow_warning_sent: bool,
    pub event_children: Vec<EventId>,
    pub handler_concurrency: Option<HandlerConcurrency>,
    pub handler_completion: Option<HandlerCompletion>,
    pub invocation: Option<InvocationState>,
}

impl EventResultRecord {
    pub fn pending(
        result_id: ResultId,
        event_id: EventId,
        route_id: RouteId,
        bus_id: BusId,
        handler_id: HandlerId,
        handler_seq: usize,
        timeout: Option<f64>,
        slow_timeout: Option<f64>,
    ) -> Self {
        Self {
            result_id,
            event_id,
            route_id,
            bus_id,
            handler_id,
            handler_seq,
            status: ResultStatus::Pending,
            result: None,
            result_set: false,
            result_is_undefined: false,
            result_is_event_reference: false,
            error: None,
            started_at: None,
            completed_at: None,
            timeout,
            slow_timeout,
            slow_warning_at: None,
            slow_warning_sent: false,
            event_children: Vec::new(),
            handler_concurrency: None,
            handler_completion: None,
            invocation: None,
        }
    }
}

#[derive(Default)]
pub struct InMemoryStore {
    pub buses: BTreeMap<BusId, BusRecord>,
    pub handlers: BTreeMap<HandlerId, HandlerRecord>,
    pub handlers_by_bus: BTreeMap<BusId, Vec<HandlerId>>,
    pub events: BTreeMap<EventId, EventRecord>,
    pub routes: BTreeMap<RouteId, EventRouteRecord>,
    pub routes_by_event: BTreeMap<EventId, Vec<RouteId>>,
    pub routes_by_bus: BTreeMap<BusId, VecDeque<RouteId>>,
    pub bus_event_history: BTreeMap<BusId, VecDeque<EventId>>,
    pub bus_event_history_members: BTreeMap<BusId, BTreeSet<EventId>>,
    pub bus_event_emissions: BTreeMap<BusId, VecDeque<EventId>>,
    pub results: BTreeMap<ResultId, EventResultRecord>,
    pub results_by_route: BTreeMap<RouteId, Vec<ResultId>>,
    pub patches: Vec<(u64, CorePatch)>,
    pub next_patch_seq: u64,
}

impl InMemoryStore {
    pub fn insert_bus(&mut self, bus: BusRecord) {
        self.buses.insert(bus.bus_id.clone(), bus);
    }

    pub fn insert_handler(&mut self, handler: HandlerRecord) {
        if let Some(existing) = self.handlers.get(&handler.handler_id) {
            if let Some(ids) = self.handlers_by_bus.get_mut(&existing.bus_id) {
                ids.retain(|handler_id| handler_id != &handler.handler_id);
            }
        }
        let ids = self
            .handlers_by_bus
            .entry(handler.bus_id.clone())
            .or_default();
        if !ids
            .iter()
            .any(|handler_id| handler_id == &handler.handler_id)
        {
            ids.push(handler.handler_id.clone());
        }
        self.handlers.insert(handler.handler_id.clone(), handler);
    }

    pub fn insert_event(&mut self, event: EventRecord) {
        self.events.insert(event.event_id.clone(), event);
    }

    pub fn insert_route(&mut self, route: EventRouteRecord) {
        self.routes_by_event
            .entry(route.event_id.clone())
            .or_default()
            .push(route.route_id.clone());
        self.routes_by_bus
            .entry(route.bus_id.clone())
            .or_default()
            .push_back(route.route_id.clone());
        self.routes.insert(route.route_id.clone(), route);
    }

    pub fn add_event_to_bus_history(&mut self, bus_id: &str, event_id: &str) {
        let members = self
            .bus_event_history_members
            .entry(bus_id.to_string())
            .or_default();
        if !members.insert(event_id.to_string()) {
            return;
        }
        let history = self
            .bus_event_history
            .entry(bus_id.to_string())
            .or_default();
        history.push_back(event_id.to_string());
    }

    pub fn record_event_emitted_to_bus(&mut self, bus_id: &str, event_id: &str) {
        let emissions = self
            .bus_event_emissions
            .entry(bus_id.to_string())
            .or_default();
        if !emissions.iter().any(|existing| existing == event_id) {
            emissions.push_back(event_id.to_string());
        }
    }

    pub fn remove_event_from_bus_history(&mut self, bus_id: &str, event_id: &str) {
        if let Some(members) = self.bus_event_history_members.get_mut(bus_id) {
            members.remove(event_id);
            if members.is_empty() {
                self.bus_event_history_members.remove(bus_id);
            }
        }
        if let Some(history) = self.bus_event_history.get_mut(bus_id) {
            history.retain(|existing| existing != event_id);
            if history.is_empty() {
                self.bus_event_history.remove(bus_id);
            }
        }
        if let Some(emissions) = self.bus_event_emissions.get_mut(bus_id) {
            emissions.retain(|existing| existing != event_id);
            if emissions.is_empty() {
                self.bus_event_emissions.remove(bus_id);
            }
        }
    }

    pub fn pop_front_bus_history_event(&mut self, bus_id: &str) -> Option<EventId> {
        let event_id = {
            let history = self.bus_event_history.get_mut(bus_id)?;
            let event_id = history.pop_front()?;
            if history.is_empty() {
                self.bus_event_history.remove(bus_id);
            }
            event_id
        };

        if let Some(members) = self.bus_event_history_members.get_mut(bus_id) {
            members.remove(&event_id);
            if members.is_empty() {
                self.bus_event_history_members.remove(bus_id);
            }
        }
        if let Some(emissions) = self.bus_event_emissions.get_mut(bus_id) {
            if emissions
                .front()
                .is_some_and(|candidate| candidate == &event_id)
            {
                emissions.pop_front();
            }
            if emissions.is_empty() {
                self.bus_event_emissions.remove(bus_id);
            }
        }

        Some(event_id)
    }

    pub fn insert_result(&mut self, result: EventResultRecord) {
        self.results_by_route
            .entry(result.route_id.clone())
            .or_default()
            .push(result.result_id.clone());
        self.results.insert(result.result_id.clone(), result);
    }

    pub fn route_results(&self, route_id: &str) -> impl Iterator<Item = &EventResultRecord> {
        self.results_by_route
            .get(route_id)
            .into_iter()
            .flatten()
            .filter_map(|result_id| self.results.get(result_id))
    }

    pub fn route_results_mut_ids(&self, route_id: &str) -> Vec<ResultId> {
        self.results_by_route
            .get(route_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn handlers_for_bus(&self, bus_id: &str) -> Vec<&HandlerRecord> {
        self.handlers_by_bus
            .get(bus_id)
            .into_iter()
            .flatten()
            .filter_map(|handler_id| self.handlers.get(handler_id))
            .collect()
    }

    pub fn append_patch(&mut self, patch: CorePatch) {
        self.next_patch_seq = self.next_patch_seq.saturating_add(1);
        self.patches.push((self.next_patch_seq, patch));
    }

    pub fn clear_patches(&mut self) {
        self.patches = Vec::new();
    }

    pub fn latest_patch_seq(&self) -> u64 {
        self.next_patch_seq
    }

    pub fn patches_since(&self, last_patch_seq: u64) -> Vec<(u64, CorePatch)> {
        let start = self
            .patches
            .partition_point(|(seq, _patch)| *seq <= last_patch_seq);
        self.patches[start..].to_vec()
    }
}
