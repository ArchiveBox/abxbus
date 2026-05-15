"""Tachyon RPC client for the Rust abxbus core."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false

from __future__ import annotations

import json
import os
import signal
import subprocess
import tempfile
import threading
import time
from hashlib import sha256
from pathlib import Path
from typing import Any, NotRequired, TypedDict, cast
from uuid import uuid4

import msgpack

CORE_PROTOCOL_VERSION = 1
REQUEST_TYPE_ID = 1
FAST_HANDLER_COMPLETED_TYPE_ID = 3
FAST_ACK_RESPONSE_TYPE_ID = 4
FAST_ERROR_RESPONSE_TYPE_ID = 5
FAST_MESSAGES_RESPONSE_TYPE_ID = 6
FAST_QUEUE_JUMP_TYPE_ID = 7
FAST_REGISTER_HANDLER_TYPE_ID = 8
FAST_UNREGISTER_HANDLER_TYPE_ID = 9
DEFAULT_CAPACITY = 1 << 20
SPIN_THRESHOLD = 50_000_000
POLLING_MODE = 1
RPC_WAIT_TIMEOUT_SECONDS = 120.0
HANDLER_OUTCOME_BATCH_LIMIT = 4096
CORE_STARTUP_TIMEOUT_SECONDS = 120.0


class ProtocolEnvelope(TypedDict):
    protocol_version: int
    session_id: str
    message: dict[str, Any]
    request_id: NotRequired[str]
    last_patch_seq: NotRequired[int]


class RustCoreClient:
    """Synchronous Tachyon RPC client for the Rust core.

    Tachyon owns the shared-memory transport; the Rust core still owns all
    scheduling, lock, timeout, and result state. The host sends one versioned
    MessagePack message and receives MessagePack core messages in a single RPC reply.
    """

    _shared_lock = threading.RLock()
    _rpc_request_lock = threading.RLock()
    _shared_named_clients: dict[str, tuple[RustCoreClient, int]] = {}

    def __init__(
        self,
        command: list[str] | None = None,
        *,
        session_id: str | None = None,
        socket_path: str | None = None,
        bus_name: str | None = None,
        capacity: int = DEFAULT_CAPACITY,
    ) -> None:
        self.session_id = session_id or f'py-{uuid4()}'
        self.socket_path = socket_path or (
            stable_core_socket_path(bus_name) if bus_name else os.path.join(tempfile.gettempdir(), f'abxbus-core-{uuid4()}.sock')
        )
        self.bus_name = bus_name
        self.command = command or default_core_command(self.socket_path, daemon=bus_name is not None)
        self.capacity = capacity
        self._request_lock = threading.RLock()
        self._last_patch_seq = 0
        self._process: subprocess.Popen[str] | None = None
        self._rpc: Any | None = None
        self._release_transport_timer: threading.Timer | None = None
        self._release_transport_generation = 0
        self._active_request_count = 0
        self._kill_process_on_close = bus_name is None
        if bus_name is not None:
            self._rpc = self._connect_named_bus(bus_name)
            return
        if socket_path is not None:
            existing = self._try_connect()
            if existing is not None:
                self._rpc = existing
                return
        self._process = subprocess.Popen(
            self.command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            env=_core_process_env(owner_pid=True, capacity=self.capacity),
        )
        try:
            self._rpc = self._connect(self.socket_path, require_process=True)
        except BaseException:
            self._cleanup_failed_spawn()
            raise

    @classmethod
    def acquire_named(cls, bus_name: str) -> RustCoreClient:
        key = stable_core_socket_path(bus_name)
        with cls._shared_lock:
            existing = cls._shared_named_clients.get(key)
            if existing is not None:
                client, refs = existing
                client._cancel_transport_release()
                cls._shared_named_clients[key] = (client, refs + 1)
                return client
            client = cls(bus_name=bus_name)
            cls._shared_named_clients[key] = (client, 1)
            return client

    @classmethod
    def release_named(cls, client: RustCoreClient, *, stop_core: bool = False) -> None:
        key = client.socket_path
        with cls._shared_lock:
            existing = cls._shared_named_clients.get(key)
            if existing is None or existing[0] is not client:
                if stop_core:
                    client.stop()
                elif os.environ.get('ABXBUS_CORE_NAMESPACE'):
                    client.disconnect_host()
                    client.disconnect()
                else:
                    client.stop()
                return
            _client, refs = existing
            if refs > 1 and not stop_core:
                cls._shared_named_clients[key] = (client, refs - 1)
                return
            if not stop_core:
                cls._shared_named_clients[key] = (client, 0)
                client.release_transport_soon(delay=1.0)
                return
            cls._shared_named_clients.pop(key, None)
        if stop_core:
            client.stop()
        else:
            client.disconnect()

    @classmethod
    def release_named_from_finalizer(cls, client: RustCoreClient) -> None:
        key = client.socket_path
        with cls._shared_lock:
            existing = cls._shared_named_clients.get(key)
            if existing is None or existing[0] is not client:
                should_abandon = True
            else:
                _client, refs = existing
                if refs > 1:
                    cls._shared_named_clients[key] = (client, refs - 1)
                    return
                cls._shared_named_clients.pop(key, None)
                should_abandon = True
        if should_abandon:
            client.abandon(terminate_process='ABXBUS_CORE_NAMESPACE' not in os.environ)

    @classmethod
    def evict_named_socket(cls, socket_path: str, *, stopped_client: RustCoreClient | None = None) -> None:
        with cls._shared_lock:
            existing = cls._shared_named_clients.pop(socket_path, None)
        if existing is None:
            return
        client, _refs = existing
        if client is stopped_client:
            client._cancel_transport_release()
            return
        client.abandon(terminate_process=False)

    @classmethod
    def named_ref_count(cls, client: RustCoreClient) -> int:
        key = client.socket_path
        with cls._shared_lock:
            existing = cls._shared_named_clients.get(key)
            if existing is None or existing[0] is not client:
                return 0
            return existing[1]

    def request(
        self,
        message: dict[str, Any],
        *,
        include_patches: bool = True,
        advance_patch_seq: bool = False,
        advance_patch_seq_when_no_patches: bool = False,
    ) -> list[dict[str, Any]]:
        return self._request_once(
            message,
            include_patches=include_patches,
            advance_patch_seq=advance_patch_seq,
            advance_patch_seq_when_no_patches=advance_patch_seq_when_no_patches,
        )

    def _request_once(
        self,
        message: dict[str, Any],
        *,
        include_patches: bool = True,
        advance_patch_seq: bool = False,
        advance_patch_seq_when_no_patches: bool = False,
    ) -> list[dict[str, Any]]:
        request_id = str(uuid4())
        envelope: ProtocolEnvelope = {
            'protocol_version': CORE_PROTOCOL_VERSION,
            'session_id': self.session_id,
            'request_id': request_id,
            'message': message,
        }
        if include_patches:
            envelope['last_patch_seq'] = self._last_patch_seq
        payload = msgpack.packb(envelope, use_bin_type=True, default=_msgpack_default)
        with type(self)._rpc_request_lock:
            with self._request_lock:
                self._active_request_count += 1
                rpc = self._ensure_rpc()
                try:
                    cid = rpc.call(payload, msg_type=REQUEST_TYPE_ID, spin_threshold=SPIN_THRESHOLD)
                    with self._wait_for_response(rpc, cid, message) as response:
                        try:
                            with memoryview(response) as view:
                                decoded = msgpack.unpackb(view.tobytes(), raw=False)
                        finally:
                            commit = getattr(response, 'commit', None)
                            if callable(commit):
                                commit()
                finally:
                    self._active_request_count -= 1
        responses = [cast(dict[str, Any], item) for item in decoded if isinstance(item, dict)]
        has_patch = any(
            isinstance(response.get('message'), dict) and cast(dict[str, Any], response['message']).get('type') == 'patch'
            for response in responses
        )
        if advance_patch_seq:
            self.ack_patch_messages(response.get('message') for response in responses)
        for response in responses:
            response_patch_seq = response.get('last_patch_seq')
            if isinstance(response_patch_seq, int) and (
                (advance_patch_seq and not include_patches) or (advance_patch_seq_when_no_patches and not has_patch)
            ):
                self.set_patch_seq(response_patch_seq)
        return responses

    def _wait_for_response(
        self,
        rpc: Any,
        cid: int,
        message: dict[str, Any],
        *,
        wait_timeout: float | None = None,
    ) -> Any:
        if wait_timeout is None:
            wait_timeout = self._rpc_wait_timeout_for_message(message)
        if wait_timeout is None or threading.current_thread() is not threading.main_thread():
            return rpc.wait(cid, spin_threshold=SPIN_THRESHOLD)

        sigalrm = getattr(signal, 'SIGALRM', None)
        itimer_real = getattr(signal, 'ITIMER_REAL', None)
        getitimer = getattr(signal, 'getitimer', None)
        setitimer = getattr(signal, 'setitimer', None)
        if sigalrm is None or itimer_real is None or not callable(getitimer) or not callable(setitimer):
            return rpc.wait(cid, spin_threshold=SPIN_THRESHOLD)

        def timeout_handler(_signum: int, _frame: Any) -> None:
            raise TimeoutError(f'timed out waiting for Rust core response to {message.get("type")!r}')

        previous_handler = signal.getsignal(sigalrm)
        previous_timer = getitimer(itimer_real)
        effective_timeout = wait_timeout
        if previous_timer[0] > 0:
            effective_timeout = min(effective_timeout, previous_timer[0])
        signal.signal(sigalrm, timeout_handler)
        setitimer(itimer_real, effective_timeout)
        try:
            return rpc.wait(cid, spin_threshold=SPIN_THRESHOLD)
        finally:
            signal.signal(sigalrm, previous_handler)
            setitimer(itimer_real, *previous_timer)

    def _rpc_wait_timeout_for_message(self, message: dict[str, Any]) -> float | None:
        message_type = message.get('type')
        if message_type in {'wait_invocations', 'wait_event_completed'}:
            return None
        if message_type == 'wait_bus_idle':
            timeout = message.get('timeout')
            if isinstance(timeout, int | float):
                return max(RPC_WAIT_TIMEOUT_SECONDS, float(timeout) + RPC_WAIT_TIMEOUT_SECONDS)
        return RPC_WAIT_TIMEOUT_SECONDS

    def request_messages(
        self,
        message: dict[str, Any],
        *,
        include_patches: bool = True,
        advance_patch_seq: bool = False,
        advance_patch_seq_when_no_patches: bool = False,
    ) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = []
        for response in self.request(
            message,
            include_patches=include_patches,
            advance_patch_seq=advance_patch_seq,
            advance_patch_seq_when_no_patches=advance_patch_seq_when_no_patches,
        ):
            message_obj = response.get('message')
            if isinstance(message_obj, dict):
                messages.append(cast(dict[str, Any], message_obj))
        for message_obj in messages:
            if message_obj.get('type') == 'error':
                raise RuntimeError(str(message_obj.get('message', 'Rust core request failed')))
        return messages

    def get_patch_seq(self) -> int:
        return self._last_patch_seq

    def set_patch_seq(self, last_patch_seq: int) -> None:
        if last_patch_seq > self._last_patch_seq:
            self._last_patch_seq = int(last_patch_seq)

    def filter_unseen_patch_messages(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            message
            for message in messages
            if message.get('type') != 'patch'
            or not isinstance(message.get('patch_seq'), int)
            or cast(int, message['patch_seq']) > self._last_patch_seq
        ]

    def ack_patch_messages(self, messages: Any) -> None:
        max_patch_seq = self._last_patch_seq
        for message in messages:
            if not isinstance(message, dict) or message.get('type') != 'patch':
                continue
            patch_seq = message.get('patch_seq')
            if isinstance(patch_seq, int) and patch_seq > max_patch_seq:
                max_patch_seq = patch_seq
        self.set_patch_seq(max_patch_seq)

    def register_bus(self, bus: dict[str, Any]) -> list[dict[str, Any]]:
        return self.request_messages({'type': 'register_bus', 'bus': bus}, include_patches=False, advance_patch_seq=True)

    def unregister_bus(self, bus_id: str) -> list[dict[str, Any]]:
        return self.request_messages({'type': 'unregister_bus', 'bus_id': bus_id}, include_patches=False, advance_patch_seq=True)

    def register_handler(self, handler: dict[str, Any]) -> list[dict[str, Any]]:
        self._request_fast_register_handler(handler)
        return []

    def import_bus_snapshot(
        self,
        *,
        bus: dict[str, Any],
        handlers: list[dict[str, Any]],
        events: list[dict[str, Any]],
        pending_event_ids: list[str],
    ) -> list[dict[str, Any]]:
        return self.request_messages(
            {
                'type': 'import_bus_snapshot',
                'bus': bus,
                'handlers': handlers,
                'events': events,
                'pending_event_ids': pending_event_ids,
            },
            include_patches=False,
            advance_patch_seq=True,
        )

    def unregister_handler(self, handler_id: str) -> list[dict[str, Any]]:
        self._request_fast_unregister_handler(handler_id)
        return []

    def disconnect_host(self, host_id: str | None = None) -> list[dict[str, Any]]:
        return self.request_messages(
            {'type': 'disconnect_host', 'host_id': host_id or self.session_id}, include_patches=False, advance_patch_seq=True
        )

    def close_session(self) -> None:
        self._request_once({'type': 'close_session'})

    def stop_core(self) -> list[dict[str, Any]]:
        if self.bus_name is not None:
            try:
                self._request_named_stop()
            except Exception:
                pass
            if self._process is not None:
                try:
                    self._process.terminate()
                    self._process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    self._process.kill()
                    self._process.wait(timeout=2)
                except Exception:
                    pass
            for path in (self.socket_path, named_core_lock_path(self.socket_path)):
                try:
                    os.unlink(path)
                except OSError:
                    pass
            return [{'type': 'core_stopped'}]
        return self.request_messages({'type': 'stop_core'}, include_patches=False, advance_patch_seq=True)

    def emit_event(
        self,
        event: dict[str, Any],
        bus_id: str,
        *,
        defer_start: bool = True,
        compact_response: bool = False,
        parent_invocation_id: str | None = None,
        block_parent_completion: bool = False,
        pause_parent_route: bool = False,
    ) -> list[dict[str, Any]]:
        request = {
            'type': 'emit_event',
            'event': event,
            'bus_id': bus_id,
            'defer_start': defer_start,
            'compact_response': compact_response,
        }
        if parent_invocation_id is not None:
            request['parent_invocation_id'] = parent_invocation_id
            request['block_parent_completion'] = block_parent_completion
            request['pause_parent_route'] = pause_parent_route
        return self.request_messages(request, advance_patch_seq_when_no_patches=compact_response)

    def emit_events(
        self,
        events: list[dict[str, Any]],
        bus_id: str,
        *,
        defer_start: bool = True,
        compact_response: bool = False,
    ) -> list[dict[str, Any]]:
        if not events:
            return []
        return self.request_messages(
            {
                'type': 'emit_events',
                'events': events,
                'bus_id': bus_id,
                'defer_start': defer_start,
                'compact_response': compact_response,
            },
            advance_patch_seq_when_no_patches=compact_response,
        )

    def forward_event(
        self,
        event_id: str,
        bus_id: str,
        *,
        defer_start: bool = True,
        compact_response: bool = False,
        parent_invocation_id: str | None = None,
        block_parent_completion: bool = False,
        pause_parent_route: bool = False,
        event_options: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        request = {
            'type': 'forward_event',
            'event_id': event_id,
            'bus_id': bus_id,
            'defer_start': defer_start,
            'compact_response': compact_response,
        }
        if parent_invocation_id is not None:
            request['parent_invocation_id'] = parent_invocation_id
            request['block_parent_completion'] = block_parent_completion
            request['pause_parent_route'] = pause_parent_route
        if event_options:
            for key, value in event_options.items():
                if value is not None:
                    request[key] = value
        return self.request_messages(request, advance_patch_seq_when_no_patches=compact_response)

    def process_next_route(
        self,
        bus_id: str,
        limit: int | None = None,
        *,
        compact_response: bool = False,
    ) -> list[dict[str, Any]]:
        return self.request_messages(
            {'type': 'process_next_route', 'bus_id': bus_id, 'limit': limit, 'compact_response': compact_response},
            advance_patch_seq_when_no_patches=compact_response,
        )

    def wait_invocations(self, bus_id: str | None = None, limit: int | None = None) -> list[dict[str, Any]]:
        return self.request_messages({'type': 'wait_invocations', 'bus_id': bus_id, 'limit': limit})

    def wait_event_completed(self, event_id: str) -> list[dict[str, Any]]:
        return self.request_messages({'type': 'wait_event_completed', 'event_id': event_id})

    def wait_bus_idle(self, bus_id: str, timeout: float | None = None) -> bool:
        return any(
            message.get('type') == 'bus_idle'
            for message in self.request_messages(
                {'type': 'wait_bus_idle', 'bus_id': bus_id, 'timeout': timeout}, include_patches=False
            )
        )

    def process_route(
        self,
        route_id: str,
        limit: int | None = None,
        *,
        compact_response: bool = False,
    ) -> list[dict[str, Any]]:
        return self.request_messages(
            {'type': 'process_route', 'route_id': route_id, 'limit': limit, 'compact_response': compact_response},
            advance_patch_seq_when_no_patches=True,
        )

    def queue_jump_event(
        self,
        event_id: str,
        parent_invocation_id: str,
        *,
        block_parent_completion: bool = True,
        pause_parent_route: bool = True,
    ) -> list[dict[str, Any]]:
        return self._request_fast_queue_jump_event(
            event_id,
            parent_invocation_id,
            block_parent_completion=block_parent_completion,
            pause_parent_route=pause_parent_route,
            compact_response=True,
            include_patches=True,
        )

    def get_event(self, event_id: str) -> dict[str, Any] | None:
        for message in self.request_messages({'type': 'get_event', 'event_id': event_id}, include_patches=False):
            if message.get('type') == 'event_snapshot':
                event = message.get('event')
                return cast(dict[str, Any], event) if isinstance(event, dict) else None
        return None

    def list_events(self, event_pattern: str = '*', limit: int | None = None, bus_id: str | None = None) -> list[dict[str, Any]]:
        for message in self.request_messages(
            {'type': 'list_events', 'event_pattern': event_pattern, 'limit': limit, 'bus_id': bus_id}, include_patches=False
        ):
            if message.get('type') == 'event_list':
                events = message.get('events')
                if isinstance(events, list):
                    event_items = cast(list[Any], events)
                    return [cast(dict[str, Any], event) for event in event_items if isinstance(event, dict)]
        return []

    def complete_handler(
        self,
        invocation: dict[str, Any],
        value: Any,
        *,
        result_is_event_reference: bool = False,
        process_route_after: bool = False,
        process_available_after: bool = False,
        compact_response: bool = False,
    ) -> list[dict[str, Any]]:
        return self._complete_handler_fast(
            invocation,
            value,
            result_is_event_reference=result_is_event_reference,
            process_route_after=process_route_after,
            process_available_after=process_available_after,
            compact_response=compact_response,
            include_patches=True,
        )

    def complete_handler_no_patches(
        self,
        invocation: dict[str, Any],
        value: Any,
        *,
        result_is_event_reference: bool = False,
        process_route_after: bool = False,
        process_available_after: bool = False,
        compact_response: bool = False,
    ) -> list[dict[str, Any]]:
        return self._complete_handler_fast(
            invocation,
            value,
            result_is_event_reference=result_is_event_reference,
            process_route_after=process_route_after,
            process_available_after=process_available_after,
            compact_response=compact_response,
            include_patches=False,
        )

    def _complete_handler_fast(
        self,
        invocation: dict[str, Any],
        value: Any,
        *,
        result_is_event_reference: bool,
        process_route_after: bool,
        process_available_after: bool,
        compact_response: bool,
        include_patches: bool,
    ) -> list[dict[str, Any]]:
        result_id = _required_str(invocation.get('result_id'), 'result_id')
        invocation_id = _required_str(invocation.get('invocation_id'), 'invocation_id')
        fence = _required_uint64(invocation.get('fence'), 'fence')
        flags = 0
        if result_is_event_reference:
            flags |= 0b0000_0001
        if value is None:
            value_bytes = b''
        else:
            value_bytes = msgpack.packb(value, use_bin_type=True, default=_msgpack_default)
        if process_route_after:
            flags |= 0b0000_0100
        if process_available_after:
            flags |= 0b0000_1000
        if compact_response:
            flags |= 0b0001_0000
        if include_patches:
            flags |= 0b0010_0000
        session_bytes = self.session_id.encode()
        result_bytes = result_id.encode()
        invocation_bytes = invocation_id.encode()
        for label, field in (('session_id', session_bytes), ('result_id', result_bytes), ('invocation_id', invocation_bytes)):
            if len(field) > 0xFFFF:
                raise ValueError(f'fast handler completion {label} exceeds 65535 bytes')
        if len(value_bytes) > 0xFFFFFFFF:
            raise ValueError('fast handler completion value exceeds 4294967295 bytes')
        payload = bytearray()
        payload.extend(CORE_PROTOCOL_VERSION.to_bytes(2, 'little'))
        payload.extend(flags.to_bytes(2, 'little'))
        payload.extend(fence.to_bytes(8, 'little'))
        payload.extend(self._last_patch_seq.to_bytes(8, 'little'))
        payload.extend(len(session_bytes).to_bytes(2, 'little'))
        payload.extend(len(result_bytes).to_bytes(2, 'little'))
        payload.extend(len(invocation_bytes).to_bytes(2, 'little'))
        payload.extend(len(value_bytes).to_bytes(4, 'little'))
        payload.extend(session_bytes)
        payload.extend(result_bytes)
        payload.extend(invocation_bytes)
        payload.extend(value_bytes)
        with type(self)._rpc_request_lock:
            with self._request_lock:
                self._active_request_count += 1
                rpc = self._ensure_rpc()
                try:
                    cid = rpc.call(bytes(payload), msg_type=FAST_HANDLER_COMPLETED_TYPE_ID, spin_threshold=SPIN_THRESHOLD)
                    response = rpc.wait(cid, spin_threshold=SPIN_THRESHOLD)
                    try:
                        msg_type = int(getattr(response, 'type_id', 0))
                        with memoryview(response) as view:
                            data = view.tobytes()
                        if msg_type == FAST_ACK_RESPONSE_TYPE_ID:
                            self.set_patch_seq(_read_fast_patch_seq(data))
                            return []
                        if msg_type == FAST_ERROR_RESPONSE_TYPE_ID:
                            raise RuntimeError(data.decode() or 'Rust core fast handler completion failed')
                        if msg_type == FAST_MESSAGES_RESPONSE_TYPE_ID:
                            patch_seq = _read_fast_patch_seq(data[:8])
                            decoded = msgpack.unpackb(data[8:], raw=False)
                            messages = [cast(dict[str, Any], item) for item in decoded if isinstance(item, dict)]
                            for message in messages:
                                if message.get('type') == 'error':
                                    raise RuntimeError(str(message.get('message', 'Rust core fast handler completion failed')))
                            if not any(message.get('type') == 'patch' for message in messages):
                                self.set_patch_seq(patch_seq)
                            return messages
                        raise RuntimeError(f'unexpected Rust core fast response type: {msg_type}')
                    finally:
                        commit = getattr(response, 'commit', None)
                        if callable(commit):
                            commit()
                finally:
                    self._active_request_count -= 1

    def _request_fast_queue_jump_event(
        self,
        event_id: str,
        parent_invocation_id: str,
        *,
        block_parent_completion: bool = True,
        pause_parent_route: bool = True,
        compact_response: bool = False,
        include_patches: bool = False,
    ) -> list[dict[str, Any]]:
        if not event_id:
            raise ValueError('fast queue jump requires string event_id')
        if not parent_invocation_id:
            raise ValueError('fast queue jump requires string parent_invocation_id')
        flags = 0
        if block_parent_completion:
            flags |= 0b0000_0001
        if pause_parent_route:
            flags |= 0b0000_0010
        if compact_response:
            flags |= 0b0000_0100
        if include_patches:
            flags |= 0b0000_1000
        session_bytes = self.session_id.encode()
        parent_invocation_bytes = parent_invocation_id.encode()
        event_bytes = event_id.encode()
        for label, field in (
            ('session_id', session_bytes),
            ('parent_invocation_id', parent_invocation_bytes),
            ('event_id', event_bytes),
        ):
            if len(field) > 0xFFFF:
                raise ValueError(f'fast queue jump {label} exceeds 65535 bytes')
        payload = bytearray()
        payload.extend(CORE_PROTOCOL_VERSION.to_bytes(2, 'little'))
        payload.extend(flags.to_bytes(2, 'little'))
        payload.extend(self._last_patch_seq.to_bytes(8, 'little'))
        payload.extend(len(session_bytes).to_bytes(2, 'little'))
        payload.extend(len(parent_invocation_bytes).to_bytes(2, 'little'))
        payload.extend(len(event_bytes).to_bytes(2, 'little'))
        payload.extend(session_bytes)
        payload.extend(parent_invocation_bytes)
        payload.extend(event_bytes)
        return self._wait_fast_core_messages(bytes(payload), FAST_QUEUE_JUMP_TYPE_ID, 'fast queue jump')

    def _wait_fast_core_messages(self, payload: bytes, type_id: int, label: str) -> list[dict[str, Any]]:
        with type(self)._rpc_request_lock:
            with self._request_lock:
                self._active_request_count += 1
                rpc = self._ensure_rpc()
                try:
                    cid = rpc.call(payload, msg_type=type_id, spin_threshold=SPIN_THRESHOLD)
                    response = rpc.wait(cid, spin_threshold=SPIN_THRESHOLD)
                    try:
                        msg_type = int(getattr(response, 'type_id', 0))
                        with memoryview(response) as view:
                            data = view.tobytes()
                        if msg_type == FAST_ACK_RESPONSE_TYPE_ID:
                            self.set_patch_seq(_read_fast_patch_seq(data))
                            return []
                        if msg_type == FAST_ERROR_RESPONSE_TYPE_ID:
                            raise RuntimeError(data.decode() or f'Rust core {label} failed')
                        if msg_type == FAST_MESSAGES_RESPONSE_TYPE_ID:
                            patch_seq = _read_fast_patch_seq(data[:8])
                            decoded = msgpack.unpackb(data[8:], raw=False)
                            messages = [cast(dict[str, Any], item) for item in decoded if isinstance(item, dict)]
                            for message in messages:
                                if message.get('type') == 'error':
                                    raise RuntimeError(str(message.get('message', f'Rust core {label} failed')))
                            if not any(message.get('type') == 'patch' for message in messages):
                                self.set_patch_seq(patch_seq)
                            return messages
                        raise RuntimeError(f'unexpected Rust core fast response type: {msg_type}')
                    finally:
                        commit = getattr(response, 'commit', None)
                        if callable(commit):
                            commit()
                finally:
                    self._active_request_count -= 1

    def _request_fast_register_handler(self, handler: dict[str, Any]) -> None:
        session_bytes = self.session_id.encode()
        handler_bytes = msgpack.packb(handler, use_bin_type=True, default=_msgpack_default)
        if len(session_bytes) > 0xFFFF:
            raise ValueError('fast register handler session_id exceeds 65535 bytes')
        if len(handler_bytes) > 0xFFFFFFFF:
            raise ValueError('fast register handler record exceeds 4294967295 bytes')
        payload = bytearray()
        payload.extend(CORE_PROTOCOL_VERSION.to_bytes(2, 'little'))
        payload.extend(self._last_patch_seq.to_bytes(8, 'little'))
        payload.extend(len(session_bytes).to_bytes(2, 'little'))
        payload.extend(len(handler_bytes).to_bytes(4, 'little'))
        payload.extend(session_bytes)
        payload.extend(handler_bytes)
        self._wait_fast_core_messages(bytes(payload), FAST_REGISTER_HANDLER_TYPE_ID, 'fast register handler')

    def _request_fast_unregister_handler(self, handler_id: str) -> None:
        if not handler_id:
            raise ValueError('fast unregister handler requires string handler_id')
        session_bytes = self.session_id.encode()
        handler_id_bytes = handler_id.encode()
        for label, field in (('session_id', session_bytes), ('handler_id', handler_id_bytes)):
            if len(field) > 0xFFFF:
                raise ValueError(f'fast unregister handler {label} exceeds 65535 bytes')
        payload = bytearray()
        payload.extend(CORE_PROTOCOL_VERSION.to_bytes(2, 'little'))
        payload.extend(self._last_patch_seq.to_bytes(8, 'little'))
        payload.extend(len(session_bytes).to_bytes(2, 'little'))
        payload.extend(len(handler_id_bytes).to_bytes(2, 'little'))
        payload.extend(session_bytes)
        payload.extend(handler_id_bytes)
        self._wait_fast_core_messages(bytes(payload), FAST_UNREGISTER_HANDLER_TYPE_ID, 'fast unregister handler')

    def complete_handler_outcomes(
        self, outcomes: list[dict[str, Any]], *, compact_response: bool = False
    ) -> list[dict[str, Any]]:
        include_patches = not (compact_response and len(outcomes) > 1)
        messages: list[dict[str, Any]] = []
        for index in range(0, len(outcomes), HANDLER_OUTCOME_BATCH_LIMIT):
            chunk = outcomes[index : index + HANDLER_OUTCOME_BATCH_LIMIT]
            messages.extend(
                self.request_messages(
                    {'type': 'handler_outcomes', 'outcomes': chunk, 'compact_response': compact_response},
                    include_patches=include_patches,
                    advance_patch_seq=not include_patches,
                    advance_patch_seq_when_no_patches=compact_response,
                )
            )
        return messages

    def complete_handler_outcomes_no_patches(
        self, outcomes: list[dict[str, Any]], *, compact_response: bool = False
    ) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = []
        for index in range(0, len(outcomes), HANDLER_OUTCOME_BATCH_LIMIT):
            chunk = outcomes[index : index + HANDLER_OUTCOME_BATCH_LIMIT]
            messages.extend(
                self.request_messages(
                    {'type': 'handler_outcomes', 'outcomes': chunk, 'compact_response': compact_response},
                    include_patches=False,
                    advance_patch_seq=True,
                    advance_patch_seq_when_no_patches=compact_response,
                )
            )
        return messages

    def error_handler(
        self,
        invocation: dict[str, Any],
        error: BaseException | str,
        *,
        process_route_after: bool = False,
        process_available_after: bool = False,
        compact_response: bool = False,
    ) -> list[dict[str, Any]]:
        error_name = type(error).__name__
        if isinstance(error, TimeoutError):
            error_kind = 'handler_timeout'
        elif error_name == 'EventHandlerResultSchemaError':
            error_kind = 'schema_error'
        elif error_name == 'EventHandlerAbortedError':
            error_kind = 'handler_aborted'
        elif error_name == 'EventHandlerCancelledError':
            error_kind = 'handler_cancelled'
        else:
            error_kind = 'host_error'
        return self.request_messages(
            {
                'type': 'handler_outcome',
                'result_id': invocation['result_id'],
                'invocation_id': invocation['invocation_id'],
                'fence': invocation['fence'],
                'process_route_after': process_route_after,
                'process_available_after': process_available_after,
                'compact_response': compact_response,
                'outcome': {
                    'status': 'errored',
                    'error': {
                        'kind': error_kind,
                        'message': str(error),
                    },
                },
            }
        )

    def close(self) -> None:
        self.disconnect()

    def disconnect(self) -> None:
        self._cancel_transport_release()
        try:
            if self._rpc is not None:
                self.close_session()
        except Exception:
            pass
        if self._process is not None and self._kill_process_on_close:
            try:
                self._process.terminate()
                self._process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=2)
        try:
            close = getattr(self._rpc, 'close', None)
            if callable(close):
                close()
        except Exception:
            pass
        self._rpc = None
        if self._process is not None and self._kill_process_on_close:
            try:
                os.unlink(self.socket_path)
            except OSError:
                pass

    def stop(self) -> None:
        self._cancel_transport_release()
        if self.bus_name is not None:
            type(self).evict_named_socket(self.socket_path, stopped_client=self)
        try:
            self.stop_core()
        finally:
            try:
                close = getattr(self._rpc, 'close', None)
                if callable(close):
                    close()
            except Exception:
                pass
            self._rpc = None
            if self._process is not None and self._kill_process_on_close:
                try:
                    self._process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    self._process.kill()
                    self._process.wait(timeout=2)
            if self._process is not None and self._kill_process_on_close:
                try:
                    os.unlink(self.socket_path)
                except OSError:
                    pass

    def _connect(self, socket_path: str, *, require_process: bool = False, timeout: float = CORE_STARTUP_TIMEOUT_SECONDS) -> Any:
        try:
            import tachyon
        except ImportError as exc:
            raise RuntimeError('Rust core Tachyon transport requires `tachyon-ipc`') from exc
        deadline = time.monotonic() + timeout
        last_error: BaseException | None = None
        while time.monotonic() < deadline:
            if require_process and self._process is not None and self._process.poll() is not None:
                stderr = self._process.stderr.read() if self._process.stderr else ''
                raise RuntimeError(f'Rust Tachyon core exited before connect: {stderr}')
            try:
                rpc = tachyon.RpcBus.rpc_connect(socket_path)
                rpc.set_polling_mode(POLLING_MODE)
                return rpc
            except Exception as exc:
                last_error = exc
                time.sleep(0.01)
        raise RuntimeError(f'failed to connect to Rust Tachyon core at {socket_path}') from last_error

    def _try_connect(self) -> Any | None:
        try:
            import tachyon

            rpc = tachyon.RpcBus.rpc_connect(self.socket_path)
            rpc.set_polling_mode(POLLING_MODE)
            return rpc
        except Exception:
            return None

    def _ensure_rpc(self) -> Any:
        self._cancel_transport_release()
        if self._rpc is not None:
            return self._rpc
        if self.bus_name is not None:
            self._rpc = self._connect_named_bus(self.bus_name)
            return self._rpc
        self._rpc = self._connect(self.socket_path, require_process=self._process is not None)
        return self._rpc

    def _cancel_transport_release(self) -> None:
        timer: threading.Timer | None
        with self._request_lock:
            self._release_transport_generation += 1
            timer = self._release_transport_timer
            self._release_transport_timer = None
        if timer is not None:
            timer.cancel()

    def release_transport_soon(self, *, delay: float = 0.01) -> None:
        with self._request_lock:
            if self.bus_name is None or self._rpc is None or self._release_transport_timer is not None:
                return
            self._release_transport_generation += 1
            generation = self._release_transport_generation
            timer = threading.Timer(delay, self._release_transport_timer_fired, args=(generation,))
            timer.daemon = True
            self._release_transport_timer = timer
        timer.start()

    def _release_transport_timer_fired(self, generation: int) -> None:
        rpc: Any | None = None
        reschedule = False
        with self._request_lock:
            if generation != self._release_transport_generation:
                return
            self._release_transport_generation += 1
            self._release_transport_timer = None
            if self._active_request_count > 0:
                reschedule = self.bus_name is not None and self._rpc is not None
            else:
                rpc = self._rpc
                self._rpc = None
        self._close_rpc_handle(rpc, close_session=True)
        if reschedule:
            self.release_transport_soon()

    def _close_rpc_handle(self, rpc: Any | None, *, close_session: bool = False) -> None:
        if rpc is None:
            return
        if close_session:
            try:
                self._send_close_session(rpc)
            except Exception:
                pass
        try:
            close = getattr(rpc, 'close', None)
            if callable(close):
                close()
        except Exception:
            pass

    def close_transport_only(self) -> None:
        rpc: Any | None = None
        reschedule = False
        with self._request_lock:
            self._release_transport_generation += 1
            timer = self._release_transport_timer
            self._release_transport_timer = None
            if self._active_request_count > 0:
                reschedule = self.bus_name is not None and self._rpc is not None
            else:
                rpc = self._rpc
                self._rpc = None
        if timer is not None:
            timer.cancel()
        self._close_rpc_handle(rpc, close_session=True)
        if reschedule:
            self.release_transport_soon()

    def abandon(self, *, terminate_process: bool = False) -> None:
        """Drop local transport/process references without sending RPC cleanup.

        This is used from weakref finalizers, which can run during arbitrary GC
        points inside an event loop. Finalizers must not wait on Tachyon replies.
        """
        rpc: Any | None
        with self._request_lock:
            self._release_transport_generation += 1
            timer = self._release_transport_timer
            self._release_transport_timer = None
            rpc = self._rpc
            self._rpc = None
        if timer is not None:
            timer.cancel()
        try:
            close = getattr(rpc, 'close', None)
            if callable(close):
                close()
        except Exception:
            pass
        if terminate_process and self._process is not None:
            try:
                self._process.terminate()
            except Exception:
                pass

    def _send_close_session(self, rpc: Any) -> None:
        envelope: ProtocolEnvelope = {
            'protocol_version': CORE_PROTOCOL_VERSION,
            'session_id': self.session_id,
            'message': {'type': 'close_session'},
        }
        payload = msgpack.packb(envelope, use_bin_type=True, default=_msgpack_default)
        with type(self)._rpc_request_lock:
            cid = rpc.call(payload, msg_type=REQUEST_TYPE_ID, spin_threshold=SPIN_THRESHOLD)
            with self._wait_for_response(rpc, cid, {'type': 'close_session'}) as response:
                commit = getattr(response, 'commit', None)
                if callable(commit):
                    commit()

    def _connect_named_bus(self, bus_name: str) -> Any:
        if os.path.exists(self.socket_path):
            try:
                return self._request_named_session(bus_name, timeout=0.1)
            except Exception:
                self._ensure_named_daemon(force_restart=True)
        else:
            self._ensure_named_daemon(force_restart=False)
        try:
            return self._request_named_session(bus_name, timeout=CORE_STARTUP_TIMEOUT_SECONDS)
        except Exception:
            self._ensure_named_daemon(force_restart=True)
            return self._request_named_session(bus_name, timeout=CORE_STARTUP_TIMEOUT_SECONDS)

    def _request_named_session(self, bus_name: str, *, timeout: float) -> Any:
        session_socket = stable_core_session_socket_path(bus_name)
        control = self._connect(self.socket_path, require_process=self._process is not None, timeout=timeout)
        try:
            payload = msgpack.packb({'socket_path': session_socket}, use_bin_type=True)
            with type(self)._rpc_request_lock:
                cid = control.call(payload, msg_type=REQUEST_TYPE_ID, spin_threshold=SPIN_THRESHOLD)
                with self._wait_for_response(control, cid, {'type': 'named_session'}, wait_timeout=timeout) as response:
                    try:
                        with memoryview(response) as view:
                            decoded_ack: Any = msgpack.unpackb(view.tobytes(), raw=False)
                    finally:
                        commit = getattr(response, 'commit', None)
                        if callable(commit):
                            commit()
            ack = cast(dict[str, Any], decoded_ack) if isinstance(decoded_ack, dict) else None
            if ack is None or ack.get('ok') is not True:
                error_message: object = cast(object, ack.get('error')) if ack is not None else cast(object, decoded_ack)
                raise RuntimeError(str(error_message))
        finally:
            close = getattr(control, 'close', None)
            if callable(close):
                close()
        try:
            return self._connect(session_socket, require_process=self._process is not None, timeout=timeout)
        except Exception:
            try:
                os.unlink(session_socket)
            except OSError:
                pass
            raise

    def _request_named_stop(self) -> None:
        control = self._connect(self.socket_path, timeout=0.5)
        try:
            payload = msgpack.packb({'stop': True}, use_bin_type=True)
            with type(self)._rpc_request_lock:
                cid = control.call(payload, msg_type=REQUEST_TYPE_ID, spin_threshold=SPIN_THRESHOLD)
                with self._wait_for_response(control, cid, {'type': 'named_stop'}, wait_timeout=0.5) as response:
                    try:
                        with memoryview(response) as view:
                            decoded_ack: Any = msgpack.unpackb(view.tobytes(), raw=False)
                    finally:
                        commit = getattr(response, 'commit', None)
                        if callable(commit):
                            commit()
            ack = cast(dict[str, Any], decoded_ack) if isinstance(decoded_ack, dict) else None
            if ack is None or ack.get('ok') is not True:
                error_message: object = cast(object, ack.get('error')) if ack is not None else cast(object, decoded_ack)
                raise RuntimeError(str(error_message))
        finally:
            close = getattr(control, 'close', None)
            if callable(close):
                close()

    def _ensure_named_daemon(self, *, force_restart: bool) -> None:
        if force_restart:
            process = self._process
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=1.0)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=1.0)
            self._process = None
            for path in (self.socket_path, f'{self.socket_path}.lock'):
                try:
                    os.unlink(path)
                except OSError:
                    pass
        self._process = subprocess.Popen(
            self.command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
            start_new_session=True,
            env=_core_process_env(owner_pid='ABXBUS_CORE_NAMESPACE' not in os.environ, capacity=self.capacity),
        )

    def _cleanup_failed_spawn(self) -> None:
        process = self._process
        self._process = None
        if process is not None:
            try:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=2)
                else:
                    process.wait(timeout=0)
            except Exception:
                pass
        try:
            os.unlink(self.socket_path)
        except OSError:
            pass

    def __enter__(self) -> RustCoreClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def _msgpack_default(value: Any) -> Any:
    model_dump = getattr(value, 'model_dump', None)
    if callable(model_dump):
        return model_dump(mode='json')
    to_json = getattr(value, 'to_json', None)
    if callable(to_json):
        return json.loads(cast(str, to_json()))
    raise TypeError(f'Object of type {type(value).__name__} is not MessagePack serializable')


def _core_process_env(*, owner_pid: bool, capacity: int | None = None) -> dict[str, str]:
    env = os.environ.copy()
    if owner_pid:
        env['ABXBUS_CORE_OWNER_PID'] = str(os.getpid())
    else:
        env.pop('ABXBUS_CORE_OWNER_PID', None)
    if capacity is not None:
        env['ABXBUS_CORE_TACHYON_CAPACITY'] = str(capacity)
    return env


def _required_str(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f'fast handler completion requires string {label}')
    return value


def _required_uint64(value: Any, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0 or value > 0xFFFF_FFFF_FFFF_FFFF:
        raise ValueError(f'fast handler completion requires uint64 {label}')
    return value


def _read_fast_patch_seq(data: bytes) -> int:
    if len(data) < 8:
        raise RuntimeError('fast handler completion response missing patch sequence')
    return int.from_bytes(data[:8], 'little')


def default_core_command(socket_path: str, *, daemon: bool = False) -> list[str]:
    socket_args = ['--daemon', socket_path] if daemon else [socket_path]
    configured = os.environ.get('ABXBUS_CORE_TACHYON_BIN')
    if configured:
        return [configured, *socket_args]
    repo_root = Path(__file__).resolve().parent.parent
    manifest = repo_root / 'abxbus-core' / 'Cargo.toml'
    release_bin = repo_root / 'abxbus-core' / 'target' / 'release' / 'abxbus-core-tachyon'
    if release_bin.exists():
        return [str(release_bin), *socket_args]
    debug_bin = repo_root / 'abxbus-core' / 'target' / 'debug' / 'abxbus-core-tachyon'
    if debug_bin.exists():
        return [str(debug_bin), *socket_args]
    return [
        'cargo',
        'run',
        '--quiet',
        '--manifest-path',
        str(manifest),
        '--bin',
        'abxbus-core-tachyon',
        '--',
        *socket_args,
    ]


def stable_core_socket_path(bus_name: str) -> str:
    namespace = os.environ.get('ABXBUS_CORE_NAMESPACE') or f'process-{os.getpid()}'
    digest = sha256(namespace.encode()).hexdigest()[:24]
    return os.path.join(tempfile.gettempdir(), f'abxbus-core-{digest}.sock')


def stable_core_session_socket_path(bus_name: str) -> str:
    digest = sha256(bus_name.encode()).hexdigest()[:16]
    return os.path.join(tempfile.gettempdir(), f'abxbus-core-{digest}-{str(uuid4())[:8]}.sock')


def named_core_lock_path(socket_path: str) -> str:
    return f'{socket_path}.lock' if not socket_path.endswith('.sock') else f'{socket_path}.lock'
