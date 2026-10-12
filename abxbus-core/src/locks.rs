use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};

use crate::{
    defaults::resolve_event_concurrency,
    store::{BusRecord, EventRecord},
    types::{BusId, CoreErrorState, EventConcurrency, EventId, InvocationId, Timestamp},
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum LockResource {
    EventGlobal,
    EventBus(BusId),
    HandlerEvent(EventId),
    Custom(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockLease {
    pub resource: LockResource,
    pub owner: InvocationId,
    pub fence: u64,
    pub lease_expires_at: Option<Timestamp>,
    pub suspended: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockAcquire {
    Granted(LockLease),
    Waiting { position: usize },
}

#[derive(Debug, Clone, Default)]
struct LockState {
    current: Option<LockLease>,
    waiters: VecDeque<InvocationId>,
    waiter_members: BTreeSet<InvocationId>,
    next_fence: u64,
}

#[derive(Debug, Clone, Default)]
pub struct LockManager {
    locks: BTreeMap<LockResource, LockState>,
}

pub fn event_lock_resource(mode: EventConcurrency, bus_id: &str) -> Option<LockResource> {
    match mode {
        EventConcurrency::GlobalSerial => Some(LockResource::EventGlobal),
        EventConcurrency::BusSerial => Some(LockResource::EventBus(bus_id.to_string())),
        EventConcurrency::Parallel => None,
    }
}

pub fn handler_lock_resource(
    mode: crate::types::HandlerConcurrency,
    event_id: &str,
) -> Option<LockResource> {
    match mode {
        crate::types::HandlerConcurrency::Serial => {
            Some(LockResource::HandlerEvent(event_id.to_string()))
        }
        crate::types::HandlerConcurrency::Parallel => None,
    }
}

pub fn event_lock_resource_for_route(event: &EventRecord, bus: &BusRecord) -> Option<LockResource> {
    event_lock_resource(resolve_event_concurrency(event, bus), &bus.bus_id)
}

impl LockManager {
    pub fn acquire(
        &mut self,
        resource: Option<LockResource>,
        owner: InvocationId,
        lease_expires_at: Option<Timestamp>,
    ) -> Option<LockAcquire> {
        let resource = resource?;
        let state = self.locks.entry(resource.clone()).or_default();

        if let Some(current) = &state.current {
            if current.owner == owner {
                return Some(LockAcquire::Granted(current.clone()));
            }
            if state.waiter_members.insert(owner.clone()) {
                let position = state.waiters.len();
                state.waiters.push_back(owner.clone());
                return Some(LockAcquire::Waiting { position });
            }
            let position = state
                .waiters
                .iter()
                .position(|candidate| candidate == &owner)
                .unwrap_or(0);
            return Some(LockAcquire::Waiting { position });
        }

        if let Some(front) = state.waiters.front() {
            if front != &owner {
                if state.waiter_members.insert(owner.clone()) {
                    let position = state.waiters.len();
                    state.waiters.push_back(owner.clone());
                    return Some(LockAcquire::Waiting { position });
                }
                let position = state
                    .waiters
                    .iter()
                    .position(|candidate| candidate == &owner)
                    .unwrap_or(0);
                return Some(LockAcquire::Waiting { position });
            }
            if let Some(front) = state.waiters.pop_front() {
                state.waiter_members.remove(&front);
            }
        }

        state.next_fence += 1;
        let lease = LockLease {
            resource: resource.clone(),
            owner,
            fence: state.next_fence,
            lease_expires_at,
            suspended: false,
        };
        state.current = Some(lease.clone());
        Some(LockAcquire::Granted(lease))
    }

    pub fn release(&mut self, lease: &LockLease) -> Result<(), CoreErrorState> {
        let Some(state) = self.locks.get_mut(&lease.resource) else {
            return Err(CoreErrorState::LockLeaseMismatch);
        };
        let Some(current) = &state.current else {
            return Err(CoreErrorState::LockLeaseMismatch);
        };
        if current.owner != lease.owner || current.fence != lease.fence {
            return Err(CoreErrorState::LockLeaseMismatch);
        }
        state.current = None;
        Ok(())
    }

    pub fn remove_waiter(&mut self, owner: &str) {
        for state in self.locks.values_mut() {
            if state.waiter_members.remove(owner) {
                state.waiters.retain(|candidate| candidate != owner);
            }
        }
    }

    pub fn suspend(&mut self, lease: &LockLease) -> Result<LockLease, CoreErrorState> {
        let Some(state) = self.locks.get_mut(&lease.resource) else {
            return Err(CoreErrorState::LockLeaseMismatch);
        };
        let Some(current) = &mut state.current else {
            return Err(CoreErrorState::LockLeaseMismatch);
        };
        if current.owner != lease.owner || current.fence != lease.fence {
            return Err(CoreErrorState::LockLeaseMismatch);
        }
        current.suspended = true;
        Ok(current.clone())
    }

    pub fn reclaim(&mut self, lease: &LockLease) -> Result<LockLease, CoreErrorState> {
        let Some(state) = self.locks.get_mut(&lease.resource) else {
            return Err(CoreErrorState::LockLeaseMismatch);
        };
        let Some(current) = &mut state.current else {
            return Err(CoreErrorState::LockLeaseMismatch);
        };
        if current.owner != lease.owner || current.fence != lease.fence {
            return Err(CoreErrorState::LockLeaseMismatch);
        }
        current.suspended = false;
        Ok(current.clone())
    }

    pub fn current(&self, resource: &LockResource) -> Option<&LockLease> {
        self.locks
            .get(resource)
            .and_then(|state| state.current.as_ref())
    }

    pub fn waiters(&self, resource: &LockResource) -> Vec<InvocationId> {
        self.locks
            .get(resource)
            .map(|state| state.waiters.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn remove_owner(&mut self, owner: &str) {
        let mut empty_resources = Vec::new();
        for (resource, state) in &mut self.locks {
            if state
                .current
                .as_ref()
                .is_some_and(|lease| lease.owner == owner)
            {
                state.current = None;
            }
            if state.waiter_members.remove(owner) {
                state.waiters.retain(|candidate| candidate != owner);
            }
            if state.current.is_none() && state.waiters.is_empty() {
                empty_resources.push(resource.clone());
            }
        }
        for resource in empty_resources {
            self.locks.remove(&resource);
        }
    }
}
