use crate::{
    time::{now_timestamp, timestamp_plus_seconds},
    Core, CorePatch, EventStatus,
};

impl Core {
    pub(crate) fn track_bus_retention(&mut self, bus_id: &str) {
        let event_ids = self
            .store
            .bus_event_history
            .get(bus_id)
            .cloned()
            .unwrap_or_default();
        for event_id in event_ids {
            self.track_event_retention(bus_id, &event_id);
        }
    }

    fn clear_retention_deadline(&mut self, bus_id: &str, event_id: &str) {
        let key = (bus_id.to_string(), event_id.to_string());
        if let Some(deadline) = self.retention_deadlines.remove(&key) {
            if let Some(entries) = self.retention_queue.get_mut(&deadline) {
                entries.remove(&key);
                if entries.is_empty() {
                    self.retention_queue.remove(&deadline);
                }
            }
        }
    }

    pub(crate) fn track_event_retention(&mut self, bus_id: &str, event_id: &str) {
        self.clear_retention_deadline(bus_id, event_id);
        let Some(bus) = self.store.buses.get(bus_id) else {
            return;
        };
        let Some(event) = self.store.events.get(event_id) else {
            return;
        };
        if event.event_status != EventStatus::Completed {
            return;
        }
        let Some(completed_at) = event.event_completed_at.as_deref() else {
            return;
        };
        let result_default = event.event_result_ttl.or(bus.defaults.event_result_ttl);
        let mut ttl = event
            .event_ttl
            .or(bus.defaults.event_ttl)
            .filter(|ttl| *ttl >= 0.0);
        let mut consider = |candidate: Option<f64>| {
            if let Some(candidate) = candidate.filter(|ttl| *ttl >= 0.0) {
                ttl = Some(ttl.map_or(candidate, |ttl| ttl.min(candidate)));
            }
        };
        for result in event
            .completed_result_snapshots
            .values()
            .filter(|result| result.eventbus_id == bus_id)
        {
            consider(result.handler_result_ttl.or(result_default));
        }
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
                if let Some(result) = self
                    .store
                    .results
                    .get(result_id)
                    .filter(|result| result.bus_id == bus_id)
                {
                    consider(
                        self.store
                            .handlers
                            .get(&result.handler_id)
                            .and_then(|handler| handler.handler_result_ttl)
                            .or(result_default),
                    );
                }
            }
        }
        if let Some(deadline) = ttl.and_then(|ttl| timestamp_plus_seconds(completed_at, ttl)) {
            let key = (bus_id.to_string(), event_id.to_string());
            self.retention_deadlines
                .insert(key.clone(), deadline.clone());
            self.retention_queue
                .entry(deadline)
                .or_default()
                .insert(key);
        }
    }

    pub(crate) fn expire_history(&mut self) {
        let now = now_timestamp();
        while self
            .retention_queue
            .first_key_value()
            .is_some_and(|(deadline, _)| deadline <= &now)
        {
            let (_, entries) = self.retention_queue.pop_first().unwrap();
            for (bus_id, event_id) in entries {
                self.retention_deadlines
                    .remove(&(bus_id.clone(), event_id.clone()));
                let Some(bus) = self.store.buses.get(&bus_id) else {
                    continue;
                };
                let Some(event) = self.store.events.get(&event_id) else {
                    continue;
                };
                if event.event_status != EventStatus::Completed {
                    continue;
                }
                let Some(completed_at) = event.event_completed_at.as_deref() else {
                    continue;
                };
                let due = |ttl: Option<f64>| {
                    ttl.filter(|ttl| *ttl >= 0.0)
                        .and_then(|ttl| timestamp_plus_seconds(completed_at, ttl))
                        .is_some_and(|deadline| deadline <= now)
                };
                let event_expired = due(event.event_ttl.or(bus.defaults.event_ttl));
                let result_default = event.event_result_ttl.or(bus.defaults.event_result_ttl);
                let mut handler_ids: Vec<_> = event
                    .completed_result_snapshots
                    .values()
                    .filter(|result| {
                        result.eventbus_id == bus_id
                            && due(result.handler_result_ttl.or(result_default))
                    })
                    .map(|result| result.handler_id.clone())
                    .collect();
                let mut expired_results = Vec::new();
                for route_id in self
                    .store
                    .routes_by_event
                    .get(&event_id)
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
                        if let Some(result) = self
                            .store
                            .results
                            .get(result_id)
                            .filter(|result| result.bus_id == bus_id)
                        {
                            let ttl = self
                                .store
                                .handlers
                                .get(&result.handler_id)
                                .and_then(|handler| handler.handler_result_ttl)
                                .or(result_default);
                            if due(ttl) {
                                handler_ids.push(result.handler_id.clone());
                                expired_results.push((route_id.clone(), result_id.clone()));
                            }
                        }
                    }
                }
                if !handler_ids.is_empty() {
                    let event = self.store.events.get_mut(&event_id).unwrap();
                    for handler_id in &handler_ids {
                        event.completed_result_snapshots.remove(handler_id);
                    }
                    for (route_id, result_id) in expired_results {
                        self.store.results.remove(&result_id);
                        if let Some(results) = self.store.results_by_route.get_mut(&route_id) {
                            results.retain(|id| id != &result_id);
                        }
                    }
                    self.store.append_patch(CorePatch::ResultsExpired {
                        event_id: event_id.clone(),
                        handler_ids,
                    });
                }
                if event_expired {
                    self.evict_history_event(&bus_id, &event_id);
                } else {
                    self.track_event_retention(&bus_id, &event_id);
                }
            }
        }
    }

    pub(crate) fn evict_history_event(&mut self, bus_id: &str, event_id: &str) {
        self.clear_retention_deadline(bus_id, event_id);
        self.store.remove_event_from_bus_history(bus_id, event_id);
        self.store.append_patch(CorePatch::HistoryEvicted {
            bus_id: bus_id.to_string(),
            event_id: event_id.to_string(),
        });
        self.compact_completed_event_routes(event_id);
        self.prune_event_if_unreferenced(event_id);
    }
}
