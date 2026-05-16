use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{
    id::uuid_v7_string,
    store::EventRecord,
    time::now_timestamp,
    types::{
        EventConcurrency, EventId, EventStatus, HandlerCompletion, HandlerConcurrency, HandlerId,
        ResultId, Timestamp,
    },
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_type: String,
    #[serde(default = "default_event_version")]
    pub event_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_timeout: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_slow_timeout: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_concurrency: Option<EventConcurrency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_handler_timeout: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_handler_slow_timeout: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_handler_concurrency: Option<HandlerConcurrency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_handler_completion: Option<HandlerCompletion>,
    #[serde(default)]
    pub event_blocks_parent_completion: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_result_type: Option<Value>,
    #[serde(default = "uuid_v7_string")]
    pub event_id: EventId,
    #[serde(default)]
    pub event_path: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_parent_id: Option<EventId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_emitted_by_result_id: Option<ResultId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_emitted_by_handler_id: Option<HandlerId>,
    #[serde(default = "now_timestamp")]
    pub event_created_at: Timestamp,
    #[serde(default)]
    pub event_status: EventStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_started_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_completed_at: Option<Timestamp>,
    #[serde(flatten)]
    pub payload: Map<String, Value>,
}

fn default_event_version() -> String {
    "0.0.1".to_string()
}

const RESERVED_PAYLOAD_FIELDS: &[&str] = &[
    "bus",
    "emit",
    "first",
    "done",
    "eventCompleted",
    "event_completed",
    "event_result",
    "event_results_list",
    "toJSON",
    "model_dump",
];

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EnvelopeError {
    #[error("reserved runtime field is not allowed in payload: {0}")]
    ReservedRuntimeField(String),
    #[error("unrecognized event_ field in payload: {0}")]
    UnknownEventField(String),
    #[error("model_ fields are reserved for host model internals: {0}")]
    ReservedModelField(String),
    #[error("flat event payload must be a JSON object")]
    PayloadMustBeObject,
}

impl EventEnvelope {
    pub fn from_record(record: &EventRecord) -> Result<Self, EnvelopeError> {
        let payload = match &record.payload {
            Value::Object(payload) => payload.clone(),
            _ => return Err(EnvelopeError::PayloadMustBeObject),
        };
        validate_payload_fields(&payload)?;
        Ok(Self {
            event_type: record.event_type.clone(),
            event_version: record.event_version.clone(),
            event_timeout: record.event_timeout,
            event_slow_timeout: record.event_slow_timeout,
            event_concurrency: record.event_concurrency,
            event_handler_timeout: record.event_handler_timeout,
            event_handler_slow_timeout: record.event_handler_slow_timeout,
            event_handler_concurrency: record.event_handler_concurrency,
            event_handler_completion: record.event_handler_completion,
            event_blocks_parent_completion: record.event_blocks_parent_completion,
            event_result_type: record.event_result_schema.clone(),
            event_id: record.event_id.clone(),
            event_path: record.event_path.clone(),
            event_parent_id: record.event_parent_id.clone(),
            event_emitted_by_result_id: record.event_emitted_by_result_id.clone(),
            event_emitted_by_handler_id: record.event_emitted_by_handler_id.clone(),
            event_created_at: record.event_created_at.clone(),
            event_status: record.event_status,
            event_started_at: record.event_started_at.clone(),
            event_completed_at: record.event_completed_at.clone(),
            payload,
        })
    }

    pub fn into_record(self) -> Result<EventRecord, EnvelopeError> {
        validate_payload_fields(&self.payload)?;
        Ok(EventRecord {
            event_id: self.event_id,
            event_type: self.event_type,
            event_version: self.event_version,
            payload: Value::Object(self.payload),
            event_result_schema: self.event_result_type,
            event_timeout: self.event_timeout,
            event_slow_timeout: self.event_slow_timeout,
            event_concurrency: self.event_concurrency,
            event_handler_timeout: self.event_handler_timeout,
            event_handler_slow_timeout: self.event_handler_slow_timeout,
            event_handler_concurrency: self.event_handler_concurrency,
            event_handler_completion: self.event_handler_completion,
            event_parent_id: self.event_parent_id,
            event_emitted_by_result_id: self.event_emitted_by_result_id,
            event_emitted_by_handler_id: self.event_emitted_by_handler_id,
            event_blocks_parent_completion: self.event_blocks_parent_completion,
            event_path: self.event_path,
            event_created_at: self.event_created_at,
            event_started_at: self.event_started_at,
            event_completed_at: self.event_completed_at,
            event_status: self.event_status,
            completed_result_snapshots: Default::default(),
        })
    }
}

fn validate_payload_fields(payload: &Map<String, Value>) -> Result<(), EnvelopeError> {
    for key in payload.keys() {
        if RESERVED_PAYLOAD_FIELDS.contains(&key.as_str()) {
            return Err(EnvelopeError::ReservedRuntimeField(key.clone()));
        }
        if key.starts_with("event_") {
            return Err(EnvelopeError::UnknownEventField(key.clone()));
        }
        if key.starts_with("model_") {
            return Err(EnvelopeError::ReservedModelField(key.clone()));
        }
    }
    Ok(())
}
