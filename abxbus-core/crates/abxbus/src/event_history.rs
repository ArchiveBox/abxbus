use std::sync::Arc;

use indexmap::IndexMap;
use tracing::warn;

use crate::event::BaseEvent;
use crate::monotonic_dt::monotonic_datetime;
use crate::types::EventStatus;

/// Ordered event history map with query and trim helpers.
///
/// Events are stored in insertion order (FIFO) using an IndexMap.
/// Equivalent to Python's `EventHistory(dict[str, BaseEvent])`.
pub struct EventHistory {
    /// Event storage: event_id -> event.
    events: IndexMap<String, Arc<BaseEvent>>,
    /// Maximum number of events to keep in history.
    /// None = unlimited, 0 = keep only in-flight events.
    pub max_history_size: Option<usize>,
    /// If true, drop oldest events when full. If false, reject new events.
    pub max_history_drop: bool,
    /// Whether we've warned about dropping uncompleted events.
    warned_about_dropping: bool,
}

impl EventHistory {
    pub fn new(max_history_size: Option<usize>, max_history_drop: bool) -> Self {
        Self {
            events: IndexMap::new(),
            max_history_size,
            max_history_drop,
            warned_about_dropping: false,
        }
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn add_event(&mut self, event: Arc<BaseEvent>) {
        self.events.insert(event.event_id.clone(), event);
    }

    pub fn get_event(&self, event_id: &str) -> Option<&Arc<BaseEvent>> {
        self.events.get(event_id)
    }

    pub fn remove_event(&mut self, event_id: &str) -> bool {
        self.events.shift_remove(event_id).is_some()
    }

    pub fn has_event(&self, event_id: &str) -> bool {
        self.events.contains_key(event_id)
    }

    pub fn clear(&mut self) {
        self.events.clear();
    }

    pub fn values(&self) -> impl Iterator<Item = &Arc<BaseEvent>> {
        self.events.values()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Arc<BaseEvent>)> {
        self.events.iter()
    }

    /// Normalize an event pattern to a string key.
    pub fn normalize_event_pattern(event_pattern: &str) -> &str {
        if event_pattern == "*" {
            "*"
        } else {
            event_pattern
        }
    }

    /// Check if `event` is a descendant of `ancestor` by walking the parent chain.
    pub fn event_is_child_of(&self, event: &BaseEvent, ancestor: &BaseEvent) -> bool {
        let mut current_id = event.event_parent_id.as_deref();
        let mut visited = std::collections::HashSet::new();

        while let Some(cid) = current_id {
            if visited.contains(cid) {
                break;
            }
            if cid == ancestor.event_id {
                return true;
            }
            visited.insert(cid.to_string());
            current_id = self
                .events
                .get(cid)
                .and_then(|e| e.event_parent_id.as_deref());
        }

        false
    }

    /// Find the most recent event matching criteria in history.
    ///
    /// - `event_key`: event type string or "*" for all
    /// - `past`: true/false or seconds lookback window
    /// - `where_fn`: optional predicate filter
    /// - `child_of`: optional ancestor constraint
    /// - `field_filters`: exact-match filters on extra_fields
    pub fn find_in_past(
        &self,
        event_key: &str,
        past_seconds: Option<f64>,
        where_fn: Option<&dyn Fn(&BaseEvent) -> bool>,
        child_of: Option<&BaseEvent>,
        field_filters: &[(&str, &serde_json::Value)],
    ) -> Option<Arc<BaseEvent>> {
        let cutoff = past_seconds.map(|secs| {
            let cutoff_dt = chrono::Utc::now() - chrono::Duration::milliseconds((secs * 1000.0) as i64);
            monotonic_datetime(Some(&cutoff_dt.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string()))
                .unwrap_or_default()
        });

        // Iterate in reverse (most recent first).
        for event in self.events.values().rev() {
            // Cutoff check
            if let Some(ref cutoff_str) = cutoff {
                if event.event_created_at < *cutoff_str {
                    continue;
                }
            }

            // Pattern match
            if event_key != "*" && event.event_type != event_key {
                continue;
            }

            // Child-of check
            if let Some(ancestor) = child_of {
                if !self.event_is_child_of(event, ancestor) {
                    continue;
                }
            }

            // Field filters
            let mut fields_match = true;
            for (field_name, expected_value) in field_filters {
                let actual = event.extra_fields.get(*field_name);
                if actual != Some(expected_value) {
                    // Also check core fields
                    let core_match = match *field_name {
                        "event_type" => serde_json::Value::String(event.event_type.clone()) == **expected_value,
                        "event_id" => serde_json::Value::String(event.event_id.clone()) == **expected_value,
                        "event_status" => serde_json::Value::String(event.event_status.to_string()) == **expected_value,
                        _ => false,
                    };
                    if !core_match {
                        fields_match = false;
                        break;
                    }
                }
            }
            if !fields_match {
                continue;
            }

            // Custom predicate
            if let Some(pred) = where_fn {
                if !pred(event) {
                    continue;
                }
            }

            return Some(event.clone());
        }

        None
    }

    /// Trim event history to stay within max_history_size.
    /// Returns the number of events removed.
    pub fn trim_event_history(&mut self, owner_label: Option<&str>) -> usize {
        let max_size = match self.max_history_size {
            None => return 0,
            Some(0) => {
                // max_history_size=0: remove all completed events.
                let completed_ids: Vec<String> = self
                    .events
                    .iter()
                    .filter(|(_, e)| e.event_status == EventStatus::Completed)
                    .map(|(id, _)| id.clone())
                    .collect();
                let count = completed_ids.len();
                for id in completed_ids {
                    self.events.shift_remove(&id);
                }
                return count;
            }
            Some(size) => size,
        };

        if !self.max_history_drop || self.events.len() <= max_size {
            return 0;
        }

        let mut remaining_overage = self.events.len() - max_size;
        let mut removed_count = 0;

        // First pass: remove completed events.
        let completed_ids: Vec<String> = self
            .events
            .iter()
            .filter(|(_, e)| e.event_status == EventStatus::Completed)
            .take(remaining_overage)
            .map(|(id, _)| id.clone())
            .collect();

        for id in completed_ids {
            self.events.shift_remove(&id);
            removed_count += 1;
            remaining_overage -= 1;
            if remaining_overage == 0 {
                return removed_count;
            }
        }

        // Second pass: remove any events (including uncompleted).
        if remaining_overage > 0 {
            let all_ids: Vec<String> = self
                .events
                .keys()
                .take(remaining_overage)
                .cloned()
                .collect();
            let dropped_uncompleted = all_ids
                .iter()
                .filter(|id| {
                    self.events
                        .get(*id)
                        .is_some_and(|e| e.event_status != EventStatus::Completed)
                })
                .count();

            for id in all_ids {
                self.events.shift_remove(&id);
                removed_count += 1;
            }

            if dropped_uncompleted > 0 && !self.warned_about_dropping {
                self.warned_about_dropping = true;
                let owner = owner_label.unwrap_or("EventBus");
                warn!(
                    "[abxbus] ⚠️ Bus {} has exceeded max_history_size={} and is dropping oldest history entries \
                     (even uncompleted events). Increase max_history_size or set max_history_drop=false to reject.",
                    owner, max_size
                );
            }
        }

        removed_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event(event_type: &str) -> Arc<BaseEvent> {
        Arc::new(BaseEvent::new(event_type))
    }

    #[test]
    fn test_add_and_get() {
        let mut history = EventHistory::new(Some(100), false);
        let event = make_event("TestEvent");
        let id = event.event_id.clone();
        history.add_event(event.clone());
        assert!(history.has_event(&id));
        assert_eq!(history.get_event(&id).unwrap().event_type, "TestEvent");
    }

    #[test]
    fn test_find_in_past() {
        let mut history = EventHistory::new(Some(100), false);
        let e1 = make_event("TypeA");
        let e2 = make_event("TypeB");
        let e3 = make_event("TypeA");
        history.add_event(e1);
        history.add_event(e2);
        history.add_event(e3.clone());

        let found = history.find_in_past("TypeA", None, None, None, &[]);
        assert!(found.is_some());
        // Should find the most recent TypeA (e3).
        assert_eq!(found.unwrap().event_id, e3.event_id);

        let found_b = history.find_in_past("TypeB", None, None, None, &[]);
        assert!(found_b.is_some());

        let found_none = history.find_in_past("TypeC", None, None, None, &[]);
        assert!(found_none.is_none());

        // Wildcard
        let found_all = history.find_in_past("*", None, None, None, &[]);
        assert!(found_all.is_some());
    }

    #[test]
    fn test_trim_completed() {
        let mut history = EventHistory::new(Some(2), true);
        for i in 0..5 {
            let mut event = BaseEvent::new(format!("Event{}", i));
            event.event_status = EventStatus::Completed;
            event.is_complete = true;
            history.add_event(Arc::new(event));
        }
        assert_eq!(history.len(), 5);
        let removed = history.trim_event_history(Some("TestBus"));
        assert!(removed > 0);
        assert!(history.len() <= 2);
    }
}
