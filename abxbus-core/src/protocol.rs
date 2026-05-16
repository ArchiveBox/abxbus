use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    core::{HandlerOutcome, InvokeHandler},
    envelope::EventEnvelope,
    locks::LockLease,
    store::{BusRecord, EventRecord, EventResultRecord, EventRouteRecord, HandlerRecord},
    types::{
        CancelReason, EventConcurrency, EventId, EventStatus, HandlerCompletion,
        HandlerConcurrency, HandlerId, InvocationId, ResultId, RouteId, Timestamp,
    },
};

pub const CORE_PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProtocolEnvelope<T> {
    pub protocol_version: u16,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_patch_seq: Option<u64>,
    pub message: T,
}

impl<T> ProtocolEnvelope<T> {
    pub fn new(session_id: impl Into<String>, message: T) -> Self {
        Self {
            protocol_version: CORE_PROTOCOL_VERSION,
            session_id: session_id.into(),
            request_id: None,
            last_patch_seq: None,
            message,
        }
    }

    pub fn with_request_id(
        session_id: impl Into<String>,
        request_id: impl Into<String>,
        message: T,
    ) -> Self {
        Self {
            protocol_version: CORE_PROTOCOL_VERSION,
            session_id: session_id.into(),
            request_id: Some(request_id.into()),
            last_patch_seq: None,
            message,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HostToCoreMessage {
    Hello {
        host_id: String,
        language: String,
    },
    RegisterBus {
        bus: BusRecord,
    },
    UnregisterBus {
        bus_id: String,
    },
    RegisterHandler {
        handler: HandlerRecord,
    },
    ImportBusSnapshot {
        bus: BusRecord,
        handlers: Vec<HandlerRecord>,
        events: Vec<Value>,
        pending_event_ids: Vec<EventId>,
    },
    UnregisterHandler {
        handler_id: HandlerId,
    },
    DisconnectHost {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        host_id: Option<String>,
    },
    CloseSession,
    StopCore,
    EmitEvent {
        event: EventEnvelope,
        bus_id: String,
        #[serde(default)]
        defer_start: bool,
        #[serde(default)]
        compact_response: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent_invocation_id: Option<InvocationId>,
        #[serde(default)]
        block_parent_completion: bool,
        #[serde(default)]
        pause_parent_route: bool,
    },
    EmitEvents {
        events: Vec<EventEnvelope>,
        bus_id: String,
        #[serde(default)]
        defer_start: bool,
        #[serde(default)]
        compact_response: bool,
    },
    UpdateEventOptions {
        event_id: EventId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_handler_completion: Option<HandlerCompletion>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_blocks_parent_completion: Option<bool>,
    },
    ProcessRoute {
        route_id: RouteId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<usize>,
        #[serde(default)]
        compact_response: bool,
    },
    ProcessNextRoute {
        bus_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<usize>,
        #[serde(default)]
        compact_response: bool,
    },
    WaitInvocations {
        bus_id: Option<String>,
        limit: Option<usize>,
    },
    ForwardEvent {
        event_id: EventId,
        bus_id: String,
        #[serde(default)]
        defer_start: bool,
        #[serde(default)]
        compact_response: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent_invocation_id: Option<InvocationId>,
        #[serde(default)]
        block_parent_completion: bool,
        #[serde(default)]
        pause_parent_route: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_timeout: Option<f64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_slow_timeout: Option<f64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_concurrency: Option<EventConcurrency>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_handler_timeout: Option<f64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_handler_slow_timeout: Option<f64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_handler_concurrency: Option<HandlerConcurrency>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_handler_completion: Option<HandlerCompletion>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_blocks_parent_completion: Option<bool>,
    },
    AwaitEvent {
        event_id: EventId,
        parent_invocation_id: Option<InvocationId>,
    },
    QueueJumpEvent {
        event_id: EventId,
        parent_invocation_id: InvocationId,
        #[serde(default = "default_true")]
        block_parent_completion: bool,
        #[serde(default = "default_true")]
        pause_parent_route: bool,
    },
    WaitEventCompleted {
        event_id: EventId,
    },
    WaitEventEmitted {
        bus_id: String,
        event_pattern: String,
        #[serde(default)]
        seen_event_ids: Vec<EventId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after_event_id: Option<EventId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after_created_at: Option<Timestamp>,
    },
    WaitBusIdle {
        bus_id: String,
        timeout: Option<f64>,
    },
    GetEvent {
        event_id: EventId,
    },
    ListEvents {
        event_pattern: String,
        limit: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bus_id: Option<String>,
    },
    ListEventIds {
        event_pattern: String,
        limit: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bus_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        statuses: Option<Vec<EventStatus>>,
    },
    ListPendingEventIds {
        bus_id: String,
    },
    HandlerOutcome {
        result_id: ResultId,
        invocation_id: InvocationId,
        fence: u64,
        outcome: HandlerOutcome,
        #[serde(default = "default_true")]
        process_route_after: bool,
        #[serde(default = "default_true")]
        process_available_after: bool,
        #[serde(default)]
        compact_response: bool,
    },
    HandlerOutcomes {
        outcomes: Vec<HostHandlerOutcome>,
        #[serde(default)]
        compact_response: bool,
    },
    Heartbeat,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HostHandlerOutcome {
    pub result_id: ResultId,
    pub invocation_id: InvocationId,
    pub fence: u64,
    pub outcome: HandlerOutcome,
    #[serde(default = "default_true")]
    pub process_available_after: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompactInvokeHandler(
    pub InvocationId,
    pub ResultId,
    pub HandlerId,
    pub u64,
    pub Option<String>,
);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CoreToHostMessage {
    HelloAck {
        core_id: String,
    },
    InvokeHandler(InvokeHandler),
    InvokeHandlersCompact {
        route_id: RouteId,
        event_id: EventId,
        bus_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_deadline_at: Option<String>,
        #[serde(default)]
        route_paused: bool,
        invocations: Vec<CompactInvokeHandler>,
    },
    CancelInvocation {
        invocation_id: InvocationId,
        reason: CancelReason,
    },
    Patch {
        patch_seq: u64,
        patch: CorePatch,
    },
    EventCompleted {
        event_id: EventId,
    },
    EventSnapshot {
        event: CoreEventSnapshot,
    },
    EventList {
        events: Vec<CoreEventSnapshot>,
    },
    EventIdList {
        event_ids: Vec<EventId>,
    },
    BusIdle {
        bus_id: String,
    },
    BusBusy {
        bus_id: String,
    },
    Error {
        message: String,
    },
    SessionClosed {
        session_id: String,
    },
    Disconnected {
        host_id: String,
    },
    CoreStopped,
    HeartbeatAck,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CoreEventSnapshot {
    #[serde(flatten)]
    pub event: EventEnvelope,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub event_results: BTreeMap<HandlerId, CoreEventResultSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CoreEventResultSnapshot {
    pub id: ResultId,
    pub status: String,
    pub event_id: EventId,
    pub handler_id: HandlerId,
    pub handler_seq: usize,
    pub handler_name: String,
    pub handler_file_path: Option<String>,
    pub handler_timeout: Option<f64>,
    pub handler_slow_timeout: Option<f64>,
    pub timeout: Option<f64>,
    pub handler_registered_at: String,
    pub handler_event_pattern: String,
    pub eventbus_name: String,
    pub eventbus_id: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub result_set: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub result_is_undefined: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub result_is_event_reference: bool,
    pub result: Option<Value>,
    pub error: Option<crate::types::CoreError>,
    pub event_children: Vec<EventId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CorePatch {
    BusRegistered {
        bus: BusRecord,
    },
    HandlerRegistered {
        handler: HandlerRecord,
    },
    HandlerUnregistered {
        handler_id: HandlerId,
    },
    EventEmitted {
        event: EventRecord,
        route: EventRouteRecord,
    },
    EventUpdated {
        event_id: EventId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_handler_completion: Option<HandlerCompletion>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_blocks_parent_completion: Option<bool>,
    },
    EventStarted {
        event_id: EventId,
        started_at: String,
    },
    EventSlowWarning {
        event_id: EventId,
        warned_at: String,
    },
    EventCompleted {
        event_id: EventId,
        completed_at: String,
        event_path: Vec<String>,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        event_results: BTreeMap<HandlerId, CoreEventResultSnapshot>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_parent_id: Option<EventId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_emitted_by_result_id: Option<ResultId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_emitted_by_handler_id: Option<HandlerId>,
        #[serde(default)]
        event_blocks_parent_completion: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_started_at: Option<String>,
    },
    EventCompletedCompact {
        event_id: EventId,
        completed_at: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_started_at: Option<String>,
    },
    RouteStarted {
        route_id: RouteId,
        event_lock_lease: Option<LockLease>,
    },
    RouteCompleted {
        route_id: RouteId,
    },
    ResultPending {
        result: EventResultRecord,
    },
    ResultStarted {
        result_id: ResultId,
        invocation_id: InvocationId,
        started_at: String,
        deadline_at: Option<String>,
    },
    ResultSlowWarning {
        result_id: ResultId,
        invocation_id: InvocationId,
        warned_at: String,
    },
    ResultCompleted {
        result: EventResultRecord,
    },
    ResultCancelled {
        result: EventResultRecord,
        reason: CancelReason,
    },
    ResultTimedOut {
        result: EventResultRecord,
    },
    SerialRoutePaused {
        route_id: RouteId,
        invocation_id: InvocationId,
    },
    SerialRouteResumed {
        route_id: RouteId,
        invocation_id: InvocationId,
    },
    ChildBlocksParent {
        parent_invocation_id: InvocationId,
        child_event_id: EventId,
    },
}
