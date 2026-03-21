use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{event_handler::EventHandler, id::uuid_v7_string};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventResultStatus {
    Pending,
    Started,
    Completed,
    Error,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EventResult {
    pub id: String,
    pub status: EventResultStatus,
    pub event_id: String,
    pub handler: EventHandler,
    pub timeout: Option<f64>,
    pub started_at: Option<String>,
    pub result: Option<Value>,
    pub error: Option<String>,
    pub completed_at: Option<String>,
    pub event_children: Vec<String>,
}

impl EventResult {
    pub fn new(event_id: String, handler: EventHandler, timeout: Option<f64>) -> Self {
        Self {
            id: uuid_v7_string(),
            status: EventResultStatus::Pending,
            event_id,
            handler,
            timeout,
            started_at: None,
            result: None,
            error: None,
            completed_at: None,
            event_children: vec![],
        }
    }
}
