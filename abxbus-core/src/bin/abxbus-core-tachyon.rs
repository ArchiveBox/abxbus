use std::{
    collections::BTreeSet,
    env, fs,
    fs::OpenOptions,
    io::Write,
    path::PathBuf,
    process,
    sync::{Arc, Condvar, Mutex},
    thread,
    time::{Duration, Instant},
};

use abxbus_core::{
    store::{BusRecord, HandlerRecord},
    Core, CoreError, CoreErrorKind, CoreToHostMessage, EventStatus, HandlerOutcome,
    HostToCoreMessage, ProtocolEnvelope, CORE_PROTOCOL_VERSION,
};
use chrono::{DateTime, Utc};
use rmp_serde::{decode as rmp_decode, encode as rmp_encode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tachyon_ipc::RpcBus;

const DEFAULT_CAPACITY: usize = 16 << 20;
const REQUEST_TYPE_ID: u32 = 1;
const RESPONSE_TYPE_ID: u32 = 2;
const FAST_HANDLER_COMPLETED_TYPE_ID: u32 = 3;
const FAST_ACK_RESPONSE_TYPE_ID: u32 = 4;
const FAST_ERROR_RESPONSE_TYPE_ID: u32 = 5;
const FAST_MESSAGES_RESPONSE_TYPE_ID: u32 = 6;
const FAST_QUEUE_JUMP_TYPE_ID: u32 = 7;
const FAST_REGISTER_HANDLER_TYPE_ID: u32 = 8;
const FAST_UNREGISTER_HANDLER_TYPE_ID: u32 = 9;
const FAST_REGISTER_BUS_TYPE_ID: u32 = 10;
const FAST_UNREGISTER_BUS_TYPE_ID: u32 = 11;
const FAST_HANDLER_OUTCOME_ERROR_FLAG: u16 = 0b0100_0000;
const SPIN_THRESHOLD: u32 = 50_000_000;

fn main() {
    start_owner_watchdog();
    let args = env::args().skip(1).collect::<Vec<_>>();
    let daemon_path = args
        .first()
        .filter(|arg| arg.as_str() == "--daemon")
        .and_then(|_| args.get(1))
        .cloned()
        .or_else(|| env::var("ABXBUS_CORE_DAEMON").ok());
    let path = daemon_path
        .clone()
        .or_else(|| args.first().cloned())
        .or_else(|| env::var("ABXBUS_CORE_TACHYON").ok());
    let capacity = env::var("ABXBUS_CORE_TACHYON_CAPACITY")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(DEFAULT_CAPACITY);

    if let Some(path) = daemon_path {
        run_daemon(PathBuf::from(path), capacity);
        return;
    }

    let path = path.unwrap_or_else(|| {
        eprintln!("usage: abxbus-core-tachyon [--daemon] <socket-path>");
        process::exit(2);
    });
    run_direct(&path, capacity);
}

fn start_owner_watchdog() {
    let Ok(raw_pid) = env::var("ABXBUS_CORE_OWNER_PID") else {
        return;
    };
    let Ok(owner_pid) = raw_pid.parse::<u32>() else {
        return;
    };
    if owner_pid == 0 || owner_pid == process::id() {
        return;
    }
    thread::spawn(move || loop {
        thread::sleep(Duration::from_millis(250));
        if !process_is_running(owner_pid) {
            process::exit(0);
        }
    });
}

fn run_direct(path: &str, capacity: usize) {
    let _ = fs::remove_file(&path);
    let bus = match RpcBus::listen(&path, capacity, capacity) {
        Ok(bus) => bus,
        Err(error) => {
            eprintln!("failed to listen on Tachyon socket {path}: {error}");
            process::exit(1);
        }
    };
    let core = Arc::new(SharedCore::default());
    start_core_timer(Arc::clone(&core));
    serve_tachyon_session(bus, core);
}

#[derive(Default)]
struct SharedCore {
    core: Mutex<Core>,
    changed: Condvar,
}

fn serve_tachyon_session(bus: RpcBus, core: Arc<SharedCore>) {
    let mut session_ids = BTreeSet::new();
    let mut graceful_close = false;
    loop {
        let request = match bus.serve(SPIN_THRESHOLD) {
            Ok(request) => request,
            Err(error) => {
                eprintln!("tachyon serve error: {error}");
                break;
            }
        };
        let correlation_id = request.correlation_id;
        let request_type = request.type_id & 0xffff;
        let request_bytes = request.data().to_vec();
        let (response, response_type, action) = if request_type == FAST_HANDLER_COMPLETED_TYPE_ID {
            let (response, response_type) =
                handle_fast_handler_completed(&core, &request_bytes, &mut session_ids);
            (response, response_type, SessionAction::Continue)
        } else if request_type == FAST_QUEUE_JUMP_TYPE_ID {
            let (response, response_type) =
                handle_fast_queue_jump(&core, &request_bytes, &mut session_ids);
            (response, response_type, SessionAction::Continue)
        } else if request_type == FAST_REGISTER_HANDLER_TYPE_ID {
            let (response, response_type) =
                handle_fast_register_handler(&core, &request_bytes, &mut session_ids);
            (response, response_type, SessionAction::Continue)
        } else if request_type == FAST_UNREGISTER_HANDLER_TYPE_ID {
            let (response, response_type) =
                handle_fast_unregister_handler(&core, &request_bytes, &mut session_ids);
            (response, response_type, SessionAction::Continue)
        } else if request_type == FAST_REGISTER_BUS_TYPE_ID {
            let (response, response_type) =
                handle_fast_register_bus(&core, &request_bytes, &mut session_ids);
            (response, response_type, SessionAction::Continue)
        } else if request_type == FAST_UNREGISTER_BUS_TYPE_ID {
            let (response, response_type) =
                handle_fast_unregister_bus(&core, &request_bytes, &mut session_ids);
            (response, response_type, SessionAction::Continue)
        } else {
            let (response, action) = handle_request(&core, &request_bytes, &mut session_ids);
            (response, RESPONSE_TYPE_ID, action)
        };
        let _ = request.commit();
        if let Err(error) = bus.reply(correlation_id, &response, response_type) {
            eprintln!("tachyon reply error: {error}");
            break;
        }
        if matches!(action, SessionAction::CloseSession) {
            graceful_close = true;
            break;
        }
        if matches!(action, SessionAction::StopCore) {
            break;
        }
    }
    if !session_ids.is_empty() {
        let mut guard = core.core.lock().expect("core mutex poisoned");
        let mut changed = false;
        for session_id in session_ids {
            changed |= if graceful_close {
                guard.close_transport_session(&session_id)
            } else {
                guard.disconnect_host_session(&session_id)
            };
        }
        if changed {
            core.changed.notify_all();
        }
    }
}

#[derive(Debug)]
struct FastHandlerCompletedRequest {
    session_id: String,
    result_id: String,
    invocation_id: String,
    fence: u64,
    last_patch_seq: u64,
    outcome: HandlerOutcome,
    process_route_after: bool,
    process_available_after: bool,
    compact_response: bool,
    include_patches: bool,
}

#[derive(Debug)]
struct FastQueueJumpRequest {
    session_id: String,
    parent_invocation_id: String,
    event_id: String,
    last_patch_seq: u64,
    block_parent_completion: bool,
    pause_parent_route: bool,
    compact_response: bool,
    include_patches: bool,
}

#[derive(Debug)]
struct FastRegisterHandlerRequest {
    session_id: String,
    last_patch_seq: u64,
    handler: HandlerRecord,
}

#[derive(Debug)]
struct FastUnregisterHandlerRequest {
    session_id: String,
    last_patch_seq: u64,
    handler_id: String,
}

#[derive(Debug)]
struct FastRegisterBusRequest {
    session_id: String,
    last_patch_seq: u64,
    bus: BusRecord,
}

#[derive(Debug)]
struct FastUnregisterBusRequest {
    session_id: String,
    last_patch_seq: u64,
    bus_id: String,
}

#[derive(Clone, Copy)]
enum WireFormat {
    MessagePack,
    Json,
}

fn handle_fast_handler_completed(
    shared: &SharedCore,
    request_bytes: &[u8],
    session_ids: &mut BTreeSet<String>,
) -> (Vec<u8>, u32) {
    let request = match parse_fast_handler_completed_request(request_bytes) {
        Ok(request) => request,
        Err(error) => {
            return (
                serialize_fast_error(&format!("invalid fast handler completion: {error}")),
                FAST_ERROR_RESPONSE_TYPE_ID,
            )
        }
    };
    let (response, patch_seq) = {
        let mut core = shared.core.lock().expect("core mutex poisoned");
        if session_ids.insert(request.session_id.clone()) {
            core.connect_host_session(&request.session_id);
        }
        core.active_hosts.insert(request.session_id.clone());
        let response = core.handle_handler_outcome_for_host_no_patches(
            &request.session_id,
            &request.result_id,
            &request.invocation_id,
            request.fence,
            Some(request.last_patch_seq),
            request.outcome,
            request.process_route_after,
            request.process_available_after,
            request.compact_response,
            request.include_patches,
        );
        (response, core.latest_patch_seq())
    };
    shared.changed.notify_all();
    match response {
        Ok(messages) if messages.is_empty() => {
            (serialize_fast_ack(patch_seq), FAST_ACK_RESPONSE_TYPE_ID)
        }
        Ok(messages) => (
            serialize_fast_messages(messages, patch_seq),
            FAST_MESSAGES_RESPONSE_TYPE_ID,
        ),
        Err(error) => (
            serialize_fast_error(&error.to_string()),
            FAST_ERROR_RESPONSE_TYPE_ID,
        ),
    }
}

fn handle_fast_queue_jump(
    shared: &SharedCore,
    request_bytes: &[u8],
    session_ids: &mut BTreeSet<String>,
) -> (Vec<u8>, u32) {
    let request = match parse_fast_queue_jump_request(request_bytes) {
        Ok(request) => request,
        Err(error) => {
            return (
                serialize_fast_error(&format!("invalid fast queue jump: {error}")),
                FAST_ERROR_RESPONSE_TYPE_ID,
            )
        }
    };
    let (response, patch_seq) = {
        let mut core = shared.core.lock().expect("core mutex poisoned");
        if session_ids.insert(request.session_id.clone()) {
            core.connect_host_session(&request.session_id);
        }
        core.active_hosts.insert(request.session_id.clone());
        let response = core.queue_jump_event_for_host_no_patches(
            &request.session_id,
            &request.parent_invocation_id,
            &request.event_id,
            Some(request.last_patch_seq),
            request.block_parent_completion,
            request.pause_parent_route,
            request.compact_response,
            request.include_patches,
        );
        (response, core.latest_patch_seq())
    };
    shared.changed.notify_all();
    match response {
        Ok(messages) if messages.is_empty() => {
            (serialize_fast_ack(patch_seq), FAST_ACK_RESPONSE_TYPE_ID)
        }
        Ok(messages) => (
            serialize_fast_messages(messages, patch_seq),
            FAST_MESSAGES_RESPONSE_TYPE_ID,
        ),
        Err(error) => (
            serialize_fast_error(&error.to_string()),
            FAST_ERROR_RESPONSE_TYPE_ID,
        ),
    }
}

fn handle_fast_register_handler(
    shared: &SharedCore,
    request_bytes: &[u8],
    session_ids: &mut BTreeSet<String>,
) -> (Vec<u8>, u32) {
    let request = match parse_fast_register_handler_request(request_bytes) {
        Ok(request) => request,
        Err(error) => {
            return (
                serialize_fast_error(&format!("invalid fast register handler: {error}")),
                FAST_ERROR_RESPONSE_TYPE_ID,
            )
        }
    };
    let patch_seq = {
        let mut core = shared.core.lock().expect("core mutex poisoned");
        if session_ids.insert(request.session_id.clone()) {
            core.connect_host_session(&request.session_id);
        }
        core.active_hosts.insert(request.session_id.clone());
        let _ = request.last_patch_seq;
        core.insert_handler(request.handler);
        core.latest_patch_seq()
    };
    shared.changed.notify_all();
    (serialize_fast_ack(patch_seq), FAST_ACK_RESPONSE_TYPE_ID)
}

fn handle_fast_unregister_handler(
    shared: &SharedCore,
    request_bytes: &[u8],
    session_ids: &mut BTreeSet<String>,
) -> (Vec<u8>, u32) {
    let request = match parse_fast_unregister_handler_request(request_bytes) {
        Ok(request) => request,
        Err(error) => {
            return (
                serialize_fast_error(&format!("invalid fast unregister handler: {error}")),
                FAST_ERROR_RESPONSE_TYPE_ID,
            )
        }
    };
    let patch_seq = {
        let mut core = shared.core.lock().expect("core mutex poisoned");
        if session_ids.insert(request.session_id.clone()) {
            core.connect_host_session(&request.session_id);
        }
        core.active_hosts.insert(request.session_id.clone());
        let _ = request.last_patch_seq;
        core.unregister_handler(&request.handler_id);
        core.latest_patch_seq()
    };
    shared.changed.notify_all();
    (serialize_fast_ack(patch_seq), FAST_ACK_RESPONSE_TYPE_ID)
}

fn handle_fast_register_bus(
    shared: &SharedCore,
    request_bytes: &[u8],
    session_ids: &mut BTreeSet<String>,
) -> (Vec<u8>, u32) {
    let request = match parse_fast_register_bus_request(request_bytes) {
        Ok(request) => request,
        Err(error) => {
            return (
                serialize_fast_error(&format!("invalid fast register bus: {error}")),
                FAST_ERROR_RESPONSE_TYPE_ID,
            )
        }
    };
    let patch_seq = {
        let mut core = shared.core.lock().expect("core mutex poisoned");
        if session_ids.insert(request.session_id.clone()) {
            core.connect_host_session(&request.session_id);
        }
        core.active_hosts.insert(request.session_id.clone());
        let _ = request.last_patch_seq;
        core.insert_bus(request.bus);
        core.latest_patch_seq()
    };
    shared.changed.notify_all();
    (serialize_fast_ack(patch_seq), FAST_ACK_RESPONSE_TYPE_ID)
}

fn handle_fast_unregister_bus(
    shared: &SharedCore,
    request_bytes: &[u8],
    session_ids: &mut BTreeSet<String>,
) -> (Vec<u8>, u32) {
    let request = match parse_fast_unregister_bus_request(request_bytes) {
        Ok(request) => request,
        Err(error) => {
            return (
                serialize_fast_error(&format!("invalid fast unregister bus: {error}")),
                FAST_ERROR_RESPONSE_TYPE_ID,
            )
        }
    };
    let patch_seq = {
        let mut core = shared.core.lock().expect("core mutex poisoned");
        if session_ids.insert(request.session_id.clone()) {
            core.connect_host_session(&request.session_id);
        }
        core.active_hosts.insert(request.session_id.clone());
        let _ = request.last_patch_seq;
        core.unregister_bus(&request.bus_id);
        core.latest_patch_seq()
    };
    shared.changed.notify_all();
    (serialize_fast_ack(patch_seq), FAST_ACK_RESPONSE_TYPE_ID)
}

fn parse_fast_handler_completed_request(
    bytes: &[u8],
) -> Result<FastHandlerCompletedRequest, String> {
    const HEADER_LEN: usize = 2 + 2 + 8 + 8 + 2 + 2 + 2 + 4;
    if bytes.len() < HEADER_LEN {
        return Err("payload too short".to_string());
    }
    let mut offset = 0usize;
    let version = read_u16_le(bytes, &mut offset)?;
    if version != CORE_PROTOCOL_VERSION {
        return Err(format!(
            "unsupported protocol version {version}, expected {CORE_PROTOCOL_VERSION}"
        ));
    }
    let flags = read_u16_le(bytes, &mut offset)?;
    let fence = read_u64_le(bytes, &mut offset)?;
    let last_patch_seq = read_u64_le(bytes, &mut offset)?;
    let session_len = read_u16_le(bytes, &mut offset)? as usize;
    let result_len = read_u16_le(bytes, &mut offset)? as usize;
    let invocation_len = read_u16_le(bytes, &mut offset)? as usize;
    let value_len = read_u32_le(bytes, &mut offset)? as usize;
    let fixed_len = HEADER_LEN
        .checked_add(session_len)
        .and_then(|size| size.checked_add(result_len))
        .and_then(|size| size.checked_add(invocation_len))
        .and_then(|size| size.checked_add(value_len))
        .ok_or_else(|| "payload length overflow".to_string())?;
    if bytes.len() != fixed_len {
        return Err(format!(
            "payload length mismatch: got {}, expected {fixed_len}",
            bytes.len()
        ));
    }
    let session_id = read_utf8(bytes, &mut offset, session_len, "session_id")?;
    let result_id = read_utf8(bytes, &mut offset, result_len, "result_id")?;
    let invocation_id = read_utf8(bytes, &mut offset, invocation_len, "invocation_id")?;
    let value_bytes = take(bytes, &mut offset, value_len)?;
    let value: Value = if value_len == 0 {
        Value::Null
    } else {
        rmp_decode::from_slice(value_bytes)
            .map_err(|error| format!("invalid MessagePack result value: {error}"))?
    };
    let outcome = if flags & FAST_HANDLER_OUTCOME_ERROR_FLAG != 0 {
        HandlerOutcome::Errored {
            error: fast_core_error_from_value(value),
        }
    } else {
        HandlerOutcome::Completed {
            value,
            result_is_event_reference: flags & 0b0000_0001 != 0,
            result_is_undefined: flags & 0b0000_0010 != 0,
        }
    };
    Ok(FastHandlerCompletedRequest {
        session_id,
        result_id,
        invocation_id,
        fence,
        last_patch_seq,
        outcome,
        process_route_after: flags & 0b0000_0100 != 0,
        process_available_after: flags & 0b0000_1000 != 0,
        compact_response: flags & 0b0001_0000 != 0,
        include_patches: flags & 0b0010_0000 != 0,
    })
}

fn fast_core_error_from_value(value: Value) -> CoreError {
    let kind = value
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("host_error");
    let message = value
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let kind = match kind {
        "handler_timeout" => CoreErrorKind::HandlerTimeout,
        "handler_cancelled" => CoreErrorKind::HandlerCancelled,
        "handler_aborted" => CoreErrorKind::HandlerAborted,
        "event_timeout" => CoreErrorKind::EventTimeout,
        "schema_error" => CoreErrorKind::SchemaError,
        "lease_lost" => CoreErrorKind::LeaseLost,
        "stale_write" => CoreErrorKind::StaleWrite,
        _ => CoreErrorKind::HostError,
    };
    CoreError::new(kind, message)
}

fn parse_fast_queue_jump_request(bytes: &[u8]) -> Result<FastQueueJumpRequest, String> {
    const HEADER_LEN: usize = 2 + 2 + 8 + 2 + 2 + 2;
    if bytes.len() < HEADER_LEN {
        return Err("payload too short".to_string());
    }
    let mut offset = 0usize;
    let version = read_u16_le(bytes, &mut offset)?;
    if version != CORE_PROTOCOL_VERSION {
        return Err(format!(
            "unsupported protocol version {version}, expected {CORE_PROTOCOL_VERSION}"
        ));
    }
    let flags = read_u16_le(bytes, &mut offset)?;
    let last_patch_seq = read_u64_le(bytes, &mut offset)?;
    let session_len = read_u16_le(bytes, &mut offset)? as usize;
    let parent_invocation_len = read_u16_le(bytes, &mut offset)? as usize;
    let event_len = read_u16_le(bytes, &mut offset)? as usize;
    let fixed_len = HEADER_LEN
        .checked_add(session_len)
        .and_then(|size| size.checked_add(parent_invocation_len))
        .and_then(|size| size.checked_add(event_len))
        .ok_or_else(|| "payload length overflow".to_string())?;
    if bytes.len() != fixed_len {
        return Err(format!(
            "payload length mismatch: got {}, expected {fixed_len}",
            bytes.len()
        ));
    }
    let session_id = read_utf8(bytes, &mut offset, session_len, "session_id")?;
    let parent_invocation_id = read_utf8(
        bytes,
        &mut offset,
        parent_invocation_len,
        "parent_invocation_id",
    )?;
    let event_id = read_utf8(bytes, &mut offset, event_len, "event_id")?;
    Ok(FastQueueJumpRequest {
        session_id,
        parent_invocation_id,
        event_id,
        last_patch_seq,
        block_parent_completion: flags & 0b0000_0001 != 0,
        pause_parent_route: flags & 0b0000_0010 != 0,
        compact_response: flags & 0b0000_0100 != 0,
        include_patches: flags & 0b0000_1000 != 0,
    })
}

fn parse_fast_register_handler_request(bytes: &[u8]) -> Result<FastRegisterHandlerRequest, String> {
    const HEADER_LEN: usize = 2 + 8 + 2 + 4;
    if bytes.len() < HEADER_LEN {
        return Err("payload too short".to_string());
    }
    let mut offset = 0usize;
    let version = read_u16_le(bytes, &mut offset)?;
    if version != CORE_PROTOCOL_VERSION {
        return Err(format!(
            "unsupported protocol version {version}, expected {CORE_PROTOCOL_VERSION}"
        ));
    }
    let last_patch_seq = read_u64_le(bytes, &mut offset)?;
    let session_len = read_u16_le(bytes, &mut offset)? as usize;
    let handler_len = read_u32_le(bytes, &mut offset)? as usize;
    let fixed_len = HEADER_LEN
        .checked_add(session_len)
        .and_then(|size| size.checked_add(handler_len))
        .ok_or_else(|| "payload length overflow".to_string())?;
    if bytes.len() != fixed_len {
        return Err(format!(
            "payload length mismatch: got {}, expected {fixed_len}",
            bytes.len()
        ));
    }
    let session_id = read_utf8(bytes, &mut offset, session_len, "session_id")?;
    let handler_bytes = take(bytes, &mut offset, handler_len)?;
    let handler = rmp_decode::from_slice::<HandlerRecord>(handler_bytes)
        .map_err(|error| format!("invalid MessagePack handler record: {error}"))?;
    Ok(FastRegisterHandlerRequest {
        session_id,
        last_patch_seq,
        handler,
    })
}

fn parse_fast_unregister_handler_request(
    bytes: &[u8],
) -> Result<FastUnregisterHandlerRequest, String> {
    const HEADER_LEN: usize = 2 + 8 + 2 + 2;
    if bytes.len() < HEADER_LEN {
        return Err("payload too short".to_string());
    }
    let mut offset = 0usize;
    let version = read_u16_le(bytes, &mut offset)?;
    if version != CORE_PROTOCOL_VERSION {
        return Err(format!(
            "unsupported protocol version {version}, expected {CORE_PROTOCOL_VERSION}"
        ));
    }
    let last_patch_seq = read_u64_le(bytes, &mut offset)?;
    let session_len = read_u16_le(bytes, &mut offset)? as usize;
    let handler_id_len = read_u16_le(bytes, &mut offset)? as usize;
    let fixed_len = HEADER_LEN
        .checked_add(session_len)
        .and_then(|size| size.checked_add(handler_id_len))
        .ok_or_else(|| "payload length overflow".to_string())?;
    if bytes.len() != fixed_len {
        return Err(format!(
            "payload length mismatch: got {}, expected {fixed_len}",
            bytes.len()
        ));
    }
    let session_id = read_utf8(bytes, &mut offset, session_len, "session_id")?;
    let handler_id = read_utf8(bytes, &mut offset, handler_id_len, "handler_id")?;
    Ok(FastUnregisterHandlerRequest {
        session_id,
        last_patch_seq,
        handler_id,
    })
}

fn parse_fast_register_bus_request(bytes: &[u8]) -> Result<FastRegisterBusRequest, String> {
    const HEADER_LEN: usize = 2 + 8 + 2 + 4;
    if bytes.len() < HEADER_LEN {
        return Err("payload too short".to_string());
    }
    let mut offset = 0usize;
    let version = read_u16_le(bytes, &mut offset)?;
    if version != CORE_PROTOCOL_VERSION {
        return Err(format!(
            "unsupported protocol version {version}, expected {CORE_PROTOCOL_VERSION}"
        ));
    }
    let last_patch_seq = read_u64_le(bytes, &mut offset)?;
    let session_len = read_u16_le(bytes, &mut offset)? as usize;
    let bus_len = read_u32_le(bytes, &mut offset)? as usize;
    let fixed_len = HEADER_LEN
        .checked_add(session_len)
        .and_then(|size| size.checked_add(bus_len))
        .ok_or_else(|| "payload length overflow".to_string())?;
    if bytes.len() != fixed_len {
        return Err(format!(
            "payload length mismatch: got {}, expected {fixed_len}",
            bytes.len()
        ));
    }
    let session_id = read_utf8(bytes, &mut offset, session_len, "session_id")?;
    let bus_bytes = take(bytes, &mut offset, bus_len)?;
    let bus = rmp_decode::from_slice::<BusRecord>(bus_bytes)
        .map_err(|error| format!("invalid MessagePack bus record: {error}"))?;
    Ok(FastRegisterBusRequest {
        session_id,
        last_patch_seq,
        bus,
    })
}

fn parse_fast_unregister_bus_request(bytes: &[u8]) -> Result<FastUnregisterBusRequest, String> {
    const HEADER_LEN: usize = 2 + 8 + 2 + 2;
    if bytes.len() < HEADER_LEN {
        return Err("payload too short".to_string());
    }
    let mut offset = 0usize;
    let version = read_u16_le(bytes, &mut offset)?;
    if version != CORE_PROTOCOL_VERSION {
        return Err(format!(
            "unsupported protocol version {version}, expected {CORE_PROTOCOL_VERSION}"
        ));
    }
    let last_patch_seq = read_u64_le(bytes, &mut offset)?;
    let session_len = read_u16_le(bytes, &mut offset)? as usize;
    let bus_id_len = read_u16_le(bytes, &mut offset)? as usize;
    let fixed_len = HEADER_LEN
        .checked_add(session_len)
        .and_then(|size| size.checked_add(bus_id_len))
        .ok_or_else(|| "payload length overflow".to_string())?;
    if bytes.len() != fixed_len {
        return Err(format!(
            "payload length mismatch: got {}, expected {fixed_len}",
            bytes.len()
        ));
    }
    let session_id = read_utf8(bytes, &mut offset, session_len, "session_id")?;
    let bus_id = read_utf8(bytes, &mut offset, bus_id_len, "bus_id")?;
    Ok(FastUnregisterBusRequest {
        session_id,
        last_patch_seq,
        bus_id,
    })
}

fn read_u16_le(bytes: &[u8], offset: &mut usize) -> Result<u16, String> {
    let raw = take(bytes, offset, 2)?;
    Ok(u16::from_le_bytes([raw[0], raw[1]]))
}

fn read_u32_le(bytes: &[u8], offset: &mut usize) -> Result<u32, String> {
    let raw = take(bytes, offset, 4)?;
    Ok(u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]))
}

fn read_u64_le(bytes: &[u8], offset: &mut usize) -> Result<u64, String> {
    let raw = take(bytes, offset, 8)?;
    Ok(u64::from_le_bytes([
        raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
    ]))
}

fn read_utf8(bytes: &[u8], offset: &mut usize, len: usize, label: &str) -> Result<String, String> {
    let raw = take(bytes, offset, len)?;
    std::str::from_utf8(raw)
        .map(str::to_string)
        .map_err(|error| format!("invalid UTF-8 {label}: {error}"))
}

fn take<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8], String> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| "payload offset overflow".to_string())?;
    if end > bytes.len() {
        return Err("payload ended early".to_string());
    }
    let out = &bytes[*offset..end];
    *offset = end;
    Ok(out)
}

fn serialize_fast_ack(last_patch_seq: u64) -> Vec<u8> {
    last_patch_seq.to_le_bytes().to_vec()
}

fn serialize_fast_error(message: &str) -> Vec<u8> {
    message.as_bytes().to_vec()
}

fn serialize_fast_messages(messages: Vec<CoreToHostMessage>, last_patch_seq: u64) -> Vec<u8> {
    let encoded = encode_msgpack(&messages);
    let mut output = Vec::with_capacity(8 + encoded.len());
    output.extend_from_slice(&last_patch_seq.to_le_bytes());
    output.extend_from_slice(&encoded);
    output
}

#[derive(Debug, Deserialize)]
struct ControlRequest {
    #[serde(default)]
    socket_path: Option<String>,
    #[serde(default)]
    stop: bool,
}

#[derive(Debug, Serialize)]
struct ControlAck {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stopped: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlAction {
    Continue,
    Stop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionAction {
    Continue,
    CloseSession,
    StopCore,
}

fn run_daemon(registry_path: PathBuf, capacity: usize) {
    let lock_path = registry_path.with_extension("sock.lock");
    let lock = match acquire_daemon_lock(&registry_path, &lock_path) {
        Ok(lock) => lock,
        Err(_) => {
            return;
        }
    };
    let _lock = DaemonLockGuard { lock_path, lock };

    let core = Arc::new(SharedCore::default());
    start_core_timer(Arc::clone(&core));
    loop {
        let _ = fs::remove_file(&registry_path);
        let control =
            match RpcBus::listen(registry_path.to_string_lossy().as_ref(), capacity, capacity) {
                Ok(control) => control,
                Err(error) => {
                    eprintln!(
                        "failed to listen on Tachyon control socket {}: {error}",
                        registry_path.display()
                    );
                    process::exit(1);
                }
            };
        if serve_control_session(control, &registry_path, capacity, Arc::clone(&core))
            == ControlAction::Stop
        {
            let _ = fs::remove_file(&registry_path);
            return;
        }
    }
}

struct DaemonLockGuard {
    lock_path: PathBuf,
    #[allow(dead_code)]
    lock: fs::File,
}

impl Drop for DaemonLockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.lock_path);
    }
}

fn acquire_daemon_lock(
    registry_path: &PathBuf,
    lock_path: &PathBuf,
) -> Result<fs::File, std::io::Error> {
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(lock_path)
    {
        Ok(mut lock) => {
            write_daemon_lock_owner(&mut lock)?;
            Ok(lock)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            match daemon_lock_owner_state(lock_path) {
                LockOwnerState::Running => return Err(error),
                LockOwnerState::Unknown if daemon_lock_is_recent(lock_path) => return Err(error),
                LockOwnerState::Unknown | LockOwnerState::Dead => {}
            }
            let _ = fs::remove_file(lock_path);
            let _ = fs::remove_file(registry_path);
            let mut lock = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(lock_path)?;
            write_daemon_lock_owner(&mut lock)?;
            Ok(lock)
        }
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn process_is_running(pid: u32) -> bool {
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if rc == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(not(unix))]
fn process_is_running(_pid: u32) -> bool {
    false
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LockOwnerState {
    Running,
    Dead,
    Unknown,
}

fn write_daemon_lock_owner(lock: &mut fs::File) -> Result<(), std::io::Error> {
    lock.set_len(0)?;
    writeln!(lock, "{}", process::id())?;
    lock.sync_all()
}

fn daemon_lock_owner_state(lock_path: &PathBuf) -> LockOwnerState {
    let Ok(raw_pid) = fs::read_to_string(lock_path) else {
        return LockOwnerState::Unknown;
    };
    let Ok(pid) = raw_pid.trim().parse::<u32>() else {
        return LockOwnerState::Unknown;
    };
    if pid == 0 {
        return LockOwnerState::Unknown;
    }
    if process_is_running(pid) {
        LockOwnerState::Running
    } else {
        LockOwnerState::Dead
    }
}

fn daemon_lock_is_recent(lock_path: &PathBuf) -> bool {
    fs::metadata(lock_path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|modified| modified.elapsed().ok())
        .is_some_and(|age| age < Duration::from_secs(30))
}

fn start_core_timer(shared: Arc<SharedCore>) {
    thread::spawn(move || {
        let mut guard = shared.core.lock().expect("core mutex poisoned");
        loop {
            let wait = next_core_deadline_wait(&guard).unwrap_or(Duration::from_secs(3600));
            let (next_guard, timeout) = shared
                .changed
                .wait_timeout(guard, wait)
                .expect("core mutex poisoned");
            guard = next_guard;
            if timeout.timed_out() {
                if guard.maintenance_tick() {
                    shared.changed.notify_all();
                } else {
                    let (next_guard, _) = shared
                        .changed
                        .wait_timeout(guard, Duration::from_millis(10))
                        .expect("core mutex poisoned");
                    guard = next_guard;
                }
            }
        }
    });
}

fn next_core_deadline_wait(core: &Core) -> Option<Duration> {
    let now = Utc::now();
    core.next_deadline_at()
        .as_deref()
        .and_then(|deadline| DateTime::parse_from_rfc3339(deadline).ok())
        .map(|deadline| deadline.with_timezone(&Utc))
        .map(|deadline| {
            if deadline <= now {
                Duration::from_millis(1)
            } else {
                (deadline - now)
                    .to_std()
                    .unwrap_or(Duration::from_millis(1))
                    .max(Duration::from_millis(1))
            }
        })
}

fn spawn_session_listener(socket_path: String, capacity: usize, core: Arc<SharedCore>) {
    thread::spawn(move || {
        let _ = fs::remove_file(&socket_path);
        let bus = match RpcBus::listen(&socket_path, capacity, capacity) {
            Ok(bus) => bus,
            Err(error) => {
                eprintln!("failed to listen on Tachyon session socket {socket_path}: {error}");
                return;
            }
        };
        serve_tachyon_session(bus, core);
        let _ = fs::remove_file(&socket_path);
    });
}

fn serve_control_session(
    control: RpcBus,
    registry_path: &PathBuf,
    capacity: usize,
    core: Arc<SharedCore>,
) -> ControlAction {
    let request = match control.serve(SPIN_THRESHOLD) {
        Ok(request) => request,
        Err(error) => {
            eprintln!("tachyon control serve error: {error}");
            return ControlAction::Continue;
        }
    };
    let correlation_id = request.correlation_id;
    let request_bytes = request.data().to_vec();
    let mut action = ControlAction::Continue;
    let response = match rmp_decode::from_slice::<ControlRequest>(&request_bytes) {
        Ok(control_request) if control_request.stop => {
            action = ControlAction::Stop;
            encode_msgpack(&ControlAck {
                ok: true,
                error: None,
                stopped: Some(true),
            })
        }
        Ok(control_request) => {
            if let Some(socket_path) = control_request.socket_path {
                spawn_session_listener(socket_path, capacity, core);
                encode_msgpack(&ControlAck {
                    ok: true,
                    error: None,
                    stopped: None,
                })
            } else {
                encode_msgpack(&ControlAck {
                    ok: false,
                    error: Some("missing socket_path".to_string()),
                    stopped: None,
                })
            }
        }
        Err(error) => encode_msgpack(&ControlAck {
            ok: false,
            error: Some(format!("invalid session request: {error}")),
            stopped: None,
        }),
    };
    let _ = request.commit();
    let _ = fs::remove_file(registry_path);
    if let Err(error) = control.reply(correlation_id, &response, RESPONSE_TYPE_ID) {
        eprintln!("tachyon control reply error: {error}");
    }
    action
}

fn parse_protocol_request(
    request_bytes: &[u8],
) -> Result<(ProtocolEnvelope<HostToCoreMessage>, WireFormat), String> {
    match rmp_decode::from_slice::<ProtocolEnvelope<HostToCoreMessage>>(request_bytes) {
        Ok(request) => Ok((request, WireFormat::MessagePack)),
        Err(msgpack_error) => {
            match serde_json::from_slice::<ProtocolEnvelope<HostToCoreMessage>>(request_bytes) {
                Ok(request) => Ok((request, WireFormat::Json)),
                Err(json_error) => Err(format!(
                    "{msgpack_error}; json fallback failed: {json_error}"
                )),
            }
        }
    }
}

fn handle_request(
    shared: &SharedCore,
    request_bytes: &[u8],
    session_ids: &mut BTreeSet<String>,
) -> (Vec<u8>, SessionAction) {
    let (request, wire_format) = match parse_protocol_request(request_bytes) {
        Ok(parsed) => parsed,
        Err(error) => {
            return (
                serialize_response(
                    "unknown",
                    None,
                    vec![CoreToHostMessage::Error {
                        message: format!("invalid protocol message: {error}"),
                    }],
                    None,
                ),
                SessionAction::Continue,
            )
        }
    };
    if request.protocol_version != CORE_PROTOCOL_VERSION {
        return (
            serialize_response_with_format(
                wire_format,
                &request.session_id,
                request.request_id.as_deref(),
                vec![CoreToHostMessage::Error {
                    message: format!(
                        "unsupported protocol version {}, expected {}",
                        request.protocol_version, CORE_PROTOCOL_VERSION
                    ),
                }],
                None,
            ),
            SessionAction::Continue,
        );
    }
    let session_id = request.session_id.clone();
    let request_id = request.request_id.clone();
    let last_patch_seq = match (wire_format, request.last_patch_seq) {
        (WireFormat::Json, None) => Some(0),
        (_, last_patch_seq) => last_patch_seq,
    };
    if session_ids.insert(session_id.clone()) {
        let mut guard = shared.core.lock().expect("core mutex poisoned");
        guard.connect_host_session(&session_id);
        shared.changed.notify_all();
    }
    if matches!(request.message, HostToCoreMessage::CloseSession) {
        return (
            serialize_response_with_format(
                wire_format,
                &session_id,
                request_id.as_deref(),
                vec![CoreToHostMessage::SessionClosed {
                    session_id: session_id.clone(),
                }],
                None,
            ),
            SessionAction::CloseSession,
        );
    }
    let action = if matches!(request.message, HostToCoreMessage::StopCore) {
        SessionAction::StopCore
    } else {
        SessionAction::Continue
    };
    match request.message {
        HostToCoreMessage::WaitInvocations { bus_id, limit } => {
            return (
                wait_for_invocations(
                    shared,
                    &session_id,
                    request_id.as_deref(),
                    bus_id.as_deref(),
                    limit,
                    last_patch_seq,
                    wire_format,
                ),
                SessionAction::Continue,
            );
        }
        HostToCoreMessage::WaitEventCompleted { event_id } => {
            return (
                wait_for_event_completed(
                    shared,
                    &session_id,
                    request_id.as_deref(),
                    &event_id,
                    last_patch_seq,
                    wire_format,
                ),
                SessionAction::Continue,
            );
        }
        HostToCoreMessage::WaitEventEmitted {
            bus_id,
            event_pattern,
            seen_event_ids,
            after_event_id,
            after_created_at,
        } => {
            return (
                wait_for_event_emitted(
                    shared,
                    &session_id,
                    request_id.as_deref(),
                    &bus_id,
                    &event_pattern,
                    seen_event_ids,
                    after_event_id,
                    after_created_at,
                    last_patch_seq,
                    wire_format,
                ),
                SessionAction::Continue,
            );
        }
        HostToCoreMessage::WaitBusIdle { bus_id, timeout } => {
            return (
                wait_for_bus_idle(
                    shared,
                    &session_id,
                    request_id.as_deref(),
                    &bus_id,
                    timeout,
                    last_patch_seq,
                    wire_format,
                ),
                SessionAction::Continue,
            );
        }
        message => {
            let response = {
                let mut core = shared.core.lock().expect("core mutex poisoned");
                core.handle_host_message_for_host_since(&session_id, message, last_patch_seq)
                    .map(|messages| (messages, core.latest_patch_seq()))
            };
            shared.changed.notify_all();
            match response {
                Ok((messages, patch_seq)) => (
                    serialize_response_with_format(
                        wire_format,
                        &session_id,
                        request_id.as_deref(),
                        messages,
                        Some(patch_seq),
                    ),
                    action,
                ),
                Err(error) => (
                    serialize_response_with_format(
                        wire_format,
                        &session_id,
                        request_id.as_deref(),
                        vec![CoreToHostMessage::Error {
                            message: error.to_string(),
                        }],
                        None,
                    ),
                    SessionAction::Continue,
                ),
            }
        }
    }
}

fn wait_for_event_emitted(
    shared: &SharedCore,
    session_id: &str,
    request_id: Option<&str>,
    bus_id: &str,
    event_pattern: &str,
    seen_event_ids: Vec<String>,
    after_event_id: Option<String>,
    after_created_at: Option<String>,
    last_patch_seq: Option<u64>,
    wire_format: WireFormat,
) -> Vec<u8> {
    let seen_event_ids = seen_event_ids.into_iter().collect::<BTreeSet<_>>();
    let mut core = shared.core.lock().expect("core mutex poisoned");
    loop {
        if !core.active_hosts.contains(session_id) {
            return serialize_response_with_format(
                wire_format,
                session_id,
                request_id,
                vec![CoreToHostMessage::Disconnected {
                    host_id: session_id.to_string(),
                }],
                Some(core.latest_patch_seq()),
            );
        }
        match core.unseen_bus_event_snapshots(
            bus_id,
            event_pattern,
            &seen_event_ids,
            after_event_id.as_deref(),
            after_created_at.as_deref(),
        ) {
            Ok(events) if !events.is_empty() => {
                let mut messages = core.patch_messages_since_optional(last_patch_seq);
                messages.push(CoreToHostMessage::EventList { events });
                return serialize_response_with_format(
                    wire_format,
                    session_id,
                    request_id,
                    messages,
                    Some(core.latest_patch_seq()),
                );
            }
            Ok(_) => {
                core = shared.changed.wait(core).expect("core mutex poisoned");
            }
            Err(error) => {
                return serialize_response_with_format(
                    wire_format,
                    session_id,
                    request_id,
                    vec![CoreToHostMessage::Error {
                        message: error.to_string(),
                    }],
                    Some(core.latest_patch_seq()),
                );
            }
        }
    }
}

fn wait_for_bus_idle(
    shared: &SharedCore,
    session_id: &str,
    request_id: Option<&str>,
    bus_id: &str,
    timeout: Option<f64>,
    last_patch_seq: Option<u64>,
    wire_format: WireFormat,
) -> Vec<u8> {
    let deadline =
        timeout.map(|seconds| Instant::now() + Duration::from_secs_f64(seconds.max(0.0)));
    let mut core = shared.core.lock().expect("core mutex poisoned");
    loop {
        if !core.active_hosts.contains(session_id) {
            return serialize_response_with_format(
                wire_format,
                session_id,
                request_id,
                vec![CoreToHostMessage::Disconnected {
                    host_id: session_id.to_string(),
                }],
                Some(core.latest_patch_seq()),
            );
        }
        if core.is_bus_idle(bus_id) {
            let mut messages = core.patch_messages_since_optional(last_patch_seq);
            messages.push(CoreToHostMessage::BusIdle {
                bus_id: bus_id.to_string(),
            });
            return serialize_response_with_format(
                wire_format,
                session_id,
                request_id,
                messages,
                Some(core.latest_patch_seq()),
            );
        }
        if core.maintenance_tick() {
            shared.changed.notify_all();
            continue;
        }
        let core_deadline_wait = next_core_deadline_wait(&core);
        if let Some(deadline) = deadline {
            let now = Instant::now();
            if now >= deadline {
                return serialize_response_with_format(
                    wire_format,
                    session_id,
                    request_id,
                    vec![CoreToHostMessage::BusBusy {
                        bus_id: bus_id.to_string(),
                    }],
                    Some(core.latest_patch_seq()),
                );
            }
            let wait = core_deadline_wait
                .map(|core_wait| core_wait.min(deadline.saturating_duration_since(now)))
                .unwrap_or_else(|| deadline.saturating_duration_since(now));
            let (next_core, timeout_result) = shared
                .changed
                .wait_timeout(core, wait)
                .expect("core mutex poisoned");
            core = next_core;
            if timeout_result.timed_out() && !core.is_bus_idle(bus_id) {
                return serialize_response_with_format(
                    wire_format,
                    session_id,
                    request_id,
                    vec![CoreToHostMessage::BusBusy {
                        bus_id: bus_id.to_string(),
                    }],
                    Some(core.latest_patch_seq()),
                );
            }
        } else {
            if let Some(wait) = core_deadline_wait {
                let (next_core, _) = shared
                    .changed
                    .wait_timeout(core, wait)
                    .expect("core mutex poisoned");
                core = next_core;
            } else {
                core = shared.changed.wait(core).expect("core mutex poisoned");
            }
        }
    }
}

fn wait_for_invocations(
    shared: &SharedCore,
    session_id: &str,
    request_id: Option<&str>,
    bus_id: Option<&str>,
    limit: Option<usize>,
    last_patch_seq: Option<u64>,
    wire_format: WireFormat,
) -> Vec<u8> {
    let mut core = shared.core.lock().expect("core mutex poisoned");
    loop {
        if !core.active_hosts.contains(session_id) {
            return serialize_response_with_format(
                wire_format,
                session_id,
                request_id,
                vec![CoreToHostMessage::Disconnected {
                    host_id: session_id.to_string(),
                }],
                Some(core.latest_patch_seq()),
            );
        }
        let mut messages = Vec::new();
        if let Err(error) = core.collect_host_invocations(session_id, bus_id, limit, &mut messages)
        {
            return serialize_response_with_format(
                wire_format,
                session_id,
                request_id,
                vec![CoreToHostMessage::Error {
                    message: error.to_string(),
                }],
                Some(core.latest_patch_seq()),
            );
        }
        messages.extend(core.patch_messages_since_optional(last_patch_seq));
        if !messages.is_empty() {
            return serialize_response_with_format(
                wire_format,
                session_id,
                request_id,
                messages,
                Some(core.latest_patch_seq()),
            );
        }
        core = shared.changed.wait(core).expect("core mutex poisoned");
    }
}

fn wait_for_event_completed(
    shared: &SharedCore,
    session_id: &str,
    request_id: Option<&str>,
    event_id: &str,
    last_patch_seq: Option<u64>,
    wire_format: WireFormat,
) -> Vec<u8> {
    let mut core = shared.core.lock().expect("core mutex poisoned");
    loop {
        if !core.active_hosts.contains(session_id) {
            return serialize_response_with_format(
                wire_format,
                session_id,
                request_id,
                vec![CoreToHostMessage::Disconnected {
                    host_id: session_id.to_string(),
                }],
                Some(core.latest_patch_seq()),
            );
        }
        match core.store.events.get(event_id) {
            Some(event) if event.event_status == EventStatus::Completed => {
                let mut messages = core.patch_messages_since_optional(last_patch_seq);
                messages.push(CoreToHostMessage::EventCompleted {
                    event_id: event_id.to_string(),
                });
                if let Ok(Some(snapshot)) = core.event_snapshot(event_id) {
                    messages.push(CoreToHostMessage::EventSnapshot { event: snapshot });
                }
                return serialize_response_with_format(
                    wire_format,
                    session_id,
                    request_id,
                    messages,
                    Some(core.latest_patch_seq()),
                );
            }
            Some(_) => {
                if core.maintenance_tick() {
                    shared.changed.notify_all();
                    continue;
                }
                if let Some(wait) = next_core_deadline_wait(&core) {
                    let (next_core, _) = shared
                        .changed
                        .wait_timeout(core, wait)
                        .expect("core mutex poisoned");
                    core = next_core;
                } else {
                    core = shared.changed.wait(core).expect("core mutex poisoned");
                }
            }
            None => {
                return serialize_response_with_format(
                    wire_format,
                    session_id,
                    request_id,
                    vec![CoreToHostMessage::Error {
                        message: format!("missing event: {event_id}"),
                    }],
                    Some(core.latest_patch_seq()),
                );
            }
        }
    }
}

fn serialize_response(
    session_id: &str,
    request_id: Option<&str>,
    messages: Vec<CoreToHostMessage>,
    last_patch_seq: Option<u64>,
) -> Vec<u8> {
    serialize_response_with_format(
        WireFormat::MessagePack,
        session_id,
        request_id,
        messages,
        last_patch_seq,
    )
}

fn serialize_response_with_format(
    wire_format: WireFormat,
    session_id: &str,
    request_id: Option<&str>,
    messages: Vec<CoreToHostMessage>,
    last_patch_seq: Option<u64>,
) -> Vec<u8> {
    #[derive(Serialize)]
    struct ResponseEnvelope {
        protocol_version: u16,
        session_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        last_patch_seq: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<CoreToHostMessage>,
    }

    let mut envelopes: Vec<ResponseEnvelope> = messages
        .into_iter()
        .map(|message| ResponseEnvelope {
            protocol_version: CORE_PROTOCOL_VERSION,
            session_id: session_id.to_string(),
            request_id: request_id.map(str::to_string),
            last_patch_seq,
            message: Some(message),
        })
        .collect();
    if envelopes.is_empty() && last_patch_seq.is_some() {
        envelopes.push(ResponseEnvelope {
            protocol_version: CORE_PROTOCOL_VERSION,
            session_id: session_id.to_string(),
            request_id: request_id.map(str::to_string),
            last_patch_seq,
            message: None,
        });
    }
    match wire_format {
        WireFormat::MessagePack => encode_msgpack(&envelopes),
        WireFormat::Json => {
            serde_json::to_vec(&envelopes).expect("failed to serialize core JSON response")
        }
    }
}

fn encode_msgpack<T: Serialize>(value: &T) -> Vec<u8> {
    let mut output = Vec::new();
    value
        .serialize(&mut rmp_encode::Serializer::new(&mut output).with_struct_map())
        .expect("failed to serialize core MessagePack response");
    output
}

#[allow(dead_code)]
fn _request_type_id() -> u32 {
    REQUEST_TYPE_ID
}
