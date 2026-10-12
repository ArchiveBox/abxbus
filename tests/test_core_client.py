from typing import Any, cast
from uuid import uuid4

from abxbus import BaseEvent, RustCoreClient


class PyClientEvent(BaseEvent[int]):
    value: int


class PyClientErrorEvent(BaseEvent[object]):
    value: int


class PyClientOptionsEvent(BaseEvent[object]):
    value: int


def _event_record(event: BaseEvent[Any]) -> dict[str, object]:
    record = event.model_dump(mode='json')
    record.pop('event_pending_bus_count', None)
    return record


def _bus_record() -> dict[str, object]:
    return {
        'bus_id': 'py-bus',
        'name': 'PyBus',
        'label': 'PyBus#0001',
        'defaults': {
            'event_concurrency': 'bus-serial',
            'event_handler_concurrency': 'serial',
            'event_handler_completion': 'all',
            'event_timeout': 60,
            'event_slow_timeout': 300,
            'event_handler_timeout': None,
            'event_handler_slow_timeout': 30,
        },
        'host_id': 'py-host',
    }


def _handler_record() -> dict[str, object]:
    return {
        'handler_id': 'py-handler',
        'bus_id': 'py-bus',
        'host_id': 'py-host',
        'event_pattern': PyClientEvent.__name__,
        'handler_name': 'py_handler',
        'handler_file_path': None,
        'handler_registered_at': '2026-05-08T00:00:00Z',
        'handler_timeout': None,
        'handler_slow_timeout': None,
        'handler_concurrency': None,
        'handler_completion': None,
    }


def test_rust_core_client_roundtrips_versioned_tachyon_protocol() -> None:
    with RustCoreClient(session_id='py-test') as core:
        responses = core.request({'type': 'register_bus', 'bus': _bus_record()})

    assert responses
    first = responses[0]
    assert first['protocol_version'] == 1
    assert first['session_id'] == 'py-test'
    assert first['message']['type'] == 'patch'
    assert first['message']['patch']['type'] == 'bus_registered'
    assert first['message']['patch']['bus']['bus_id'] == 'py-bus'


def test_rust_core_client_drives_native_handler_turn() -> None:
    with RustCoreClient(session_id='py-turn') as core:
        core.register_bus(_bus_record())
        core.register_handler(_handler_record())
        core.emit_event(_event_record(PyClientEvent(event_id='018f8e40-1234-7000-8000-00000000abcd', value=41)), 'py-bus')

        messages = core.process_next_route('py-bus')
        invocation = next(message for message in messages if message['type'] == 'invoke_handler')
        assert invocation['handler_id'] == 'py-handler'
        event_snapshot = invocation['event_snapshot']

        messages = core.complete_handler(invocation, event_snapshot['value'] + 1)
        assert any(message['type'] == 'patch' and message['patch']['type'] == 'result_completed' for message in messages)
        result_patch = next(
            message['patch']
            for message in messages
            if message['type'] == 'patch' and message['patch']['type'] == 'result_completed'
        )
        assert result_patch['result']['result'] == 42
        route_id = next(
            message['patch']['result']['route_id']
            for message in messages
            if message['type'] == 'patch' and message['patch']['type'] == 'result_completed'
        )

        messages = core.process_route(route_id)
        assert any(message['type'] == 'patch' and message['patch']['type'] == 'event_completed' for message in messages)


def test_rust_core_client_commits_host_error_outcome() -> None:
    with RustCoreClient(session_id='py-error') as core:
        bus = _bus_record()
        bus['bus_id'] = 'py-error-bus'
        handler = _handler_record()
        handler['bus_id'] = 'py-error-bus'
        handler['handler_id'] = 'py-error-handler'
        handler['event_pattern'] = PyClientErrorEvent.__name__
        event = _event_record(PyClientErrorEvent(event_id='018f8e40-1234-7000-8000-00000000abce', value=41))
        core.register_bus(bus)
        core.register_handler(handler)
        core.emit_event(event, 'py-error-bus')
        invocation = next(message for message in core.process_next_route('py-error-bus') if message['type'] == 'invoke_handler')
        messages = core.error_handler(invocation, RuntimeError('py boom'))

    result_patch = next(
        message['patch'] for message in messages if message['type'] == 'patch' and message['patch']['type'] == 'result_completed'
    )
    assert result_patch['result']['status'] == 'error'
    assert result_patch['result']['error']['kind'] == 'host_error'
    assert result_patch['result']['error']['message'] == 'py boom'


def test_rust_core_client_honors_bus_parallel_handlers_and_null_timeout() -> None:
    with RustCoreClient(session_id='py-options') as core:
        bus = _bus_record()
        bus['bus_id'] = 'py-options-bus'
        defaults = cast(dict[str, object], bus['defaults']).copy()
        defaults['event_handler_concurrency'] = 'parallel'
        defaults['event_timeout'] = None
        defaults['event_handler_timeout'] = None
        bus['defaults'] = defaults
        first = _handler_record()
        first['bus_id'] = 'py-options-bus'
        first['handler_id'] = 'py-options-handler-a'
        first['event_pattern'] = PyClientOptionsEvent.__name__
        second = _handler_record()
        second['bus_id'] = 'py-options-bus'
        second['handler_id'] = 'py-options-handler-b'
        second['event_pattern'] = PyClientOptionsEvent.__name__
        event = _event_record(PyClientOptionsEvent(event_id='018f8e40-1234-7000-8000-00000000abcf', value=41))

        core.register_bus(bus)
        core.register_handler(first)
        core.register_handler(second)
        core.emit_event(event, 'py-options-bus')
        messages = core.process_next_route('py-options-bus')

    invocations = [message for message in messages if message['type'] == 'invoke_handler']
    assert [invocation['handler_id'] for invocation in invocations] == [
        'py-options-handler-a',
        'py-options-handler-b',
    ]
    assert all(invocation['deadline_at'] is None for invocation in invocations)


def test_rust_core_client_queries_core_owned_event_snapshots() -> None:
    with RustCoreClient(session_id='py-query') as core:
        bus = _bus_record()
        bus['bus_id'] = 'py-query-bus'
        core.register_bus(bus)
        first = PyClientEvent(event_id='018f8e40-1234-7000-8000-00000000abd0', value=1)
        second = PyClientEvent(event_id='018f8e40-1234-7000-8000-00000000abd1', value=2)
        core.emit_event(_event_record(first), 'py-query-bus')
        core.emit_event(_event_record(second), 'py-query-bus')

        snapshot = core.get_event(first.event_id)
        events = core.list_events(PyClientEvent.__name__, limit=1)

    assert snapshot is not None
    assert snapshot['event_id'] == first.event_id
    assert snapshot['value'] == 1
    assert [event['event_id'] for event in events] == [second.event_id]


def test_rust_core_client_reconnects_by_bus_name_to_shared_core() -> None:
    bus_name = f'PyNamedCore-{uuid4()}'
    first = RustCoreClient(session_id='py-shared-a', bus_name=bus_name)
    second = RustCoreClient(session_id='py-shared-b', bus_name=bus_name)
    try:
        bus = _bus_record()
        bus['bus_id'] = 'py-shared-bus'
        first.register_bus(bus)
        event = PyClientEvent(event_id='018f8e40-1234-7000-8000-00000000abd2', value=7)
        first.emit_event(_event_record(event), 'py-shared-bus')

        snapshot = second.get_event(event.event_id)
    finally:
        second.disconnect()
        first.stop()

    assert snapshot is not None
    assert snapshot['event_id'] == event.event_id
    assert snapshot['value'] == 7
