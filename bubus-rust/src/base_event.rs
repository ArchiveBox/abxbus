use std::{collections::HashMap, sync::Arc};

use event_listener::Event;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{
    event_result::EventResult,
    id::uuid_v7_string,
    types::{
        EventConcurrencyMode, EventHandlerCompletionMode, EventHandlerConcurrencyMode, EventStatus,
    },
};

pub fn now_iso() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}.{:09}Z", dur.as_secs(), dur.subsec_nanos())
}

#[derive(Clone, Serialize, Deserialize)]
pub struct BaseEventData {
    pub event_type: String,
    pub event_version: String,
    pub event_timeout: Option<f64>,
    pub event_slow_timeout: Option<f64>,
    pub event_concurrency: Option<EventConcurrencyMode>,
    pub event_handler_timeout: Option<f64>,
    pub event_handler_slow_timeout: Option<f64>,
    pub event_handler_concurrency: Option<EventHandlerConcurrencyMode>,
    pub event_handler_completion: Option<EventHandlerCompletionMode>,
    pub event_result_type: Option<Value>,
    pub event_id: String,
    pub event_path: Vec<String>,
    pub event_parent_id: Option<String>,
    pub event_emitted_by_handler_id: Option<String>,
    pub event_pending_bus_count: usize,
    pub event_created_at: String,
    pub event_status: EventStatus,
    pub event_started_at: Option<String>,
    pub event_completed_at: Option<String>,
    pub event_results: HashMap<String, EventResult>,
    #[serde(flatten)]
    pub payload: Map<String, Value>,
}

pub struct BaseEvent {
    pub inner: Mutex<BaseEventData>,
    pub completed: Event,
}

impl BaseEvent {
    pub fn new(event_type: impl Into<String>, payload: Map<String, Value>) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(BaseEventData {
                event_type: event_type.into(),
                event_version: "0.0.1".to_string(),
                event_timeout: None,
                event_slow_timeout: None,
                event_concurrency: None,
                event_handler_timeout: None,
                event_handler_slow_timeout: None,
                event_handler_concurrency: None,
                event_handler_completion: None,
                event_result_type: None,
                event_id: uuid_v7_string(),
                event_path: vec![],
                event_parent_id: None,
                event_emitted_by_handler_id: None,
                event_pending_bus_count: 0,
                event_created_at: now_iso(),
                event_status: EventStatus::Pending,
                event_started_at: None,
                event_completed_at: None,
                event_results: HashMap::new(),
                payload,
            }),
            completed: Event::new(),
        })
    }

    pub async fn wait_completed(self: &Arc<Self>) {
        loop {
            let listener = self.completed.listen();
            {
                let event = self.inner.lock();
                if event.event_status == EventStatus::Completed {
                    return;
                }
            }
            listener.await;
        }
    }

    pub fn mark_started(&self) {
        let mut event = self.inner.lock();
        if event.event_started_at.is_none() {
            event.event_started_at = Some(now_iso());
        }
        event.event_status = EventStatus::Started;
    }

    pub fn mark_completed(&self) {
        let mut event = self.inner.lock();
        event.event_status = EventStatus::Completed;
        if event.event_completed_at.is_none() {
            event.event_completed_at = Some(now_iso());
        }
        self.completed.notify(usize::MAX);
    }

    pub fn to_json_value(&self) -> Value {
        serde_json::to_value(&*self.inner.lock()).unwrap_or(Value::Null)
    }

    pub fn from_json_value(value: Value) -> Arc<Self> {
        let parsed: BaseEventData = serde_json::from_value(value).expect("invalid base_event json");
        Arc::new(Self {
            inner: Mutex::new(parsed),
            completed: Event::new(),
        })
    }
}
