# pyright: reportUnnecessaryComparison=false

import asyncio
import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from types import NoneType
from typing import Any, TypeAlias, cast

import pytest
from pydantic import BaseModel, TypeAdapter, ValidationError
from typing_extensions import TypedDict

from abxbus import BaseEvent, EventBus
from abxbus.helpers import CleanShutdownQueue

SUBPROCESS_TIMEOUT_SECONDS = 30
RUST_SUBPROCESS_TIMEOUT_SECONDS = 120
GO_SUBPROCESS_TIMEOUT_SECONDS = 120
EVENT_WAIT_TIMEOUT_SECONDS = 15
JsonShape: TypeAlias = str | list['JsonShape'] | dict[str, 'JsonShape']


class ScreenshotRegion(BaseModel):
    id: str
    label: str
    score: float
    visible: bool


class ScreenshotResult(BaseModel):
    image_url: str
    width: int
    height: int
    tags: list[str]
    is_animated: bool
    confidence_scores: list[float]
    metadata: dict[str, float]
    regions: list[ScreenshotRegion]


class PyTsTypedDictResult(TypedDict):
    name: str
    active: bool
    count: int


@dataclass(slots=True)
class PyTsDataclassResult:
    name: str
    score: float
    tags: list[str]


@dataclass(slots=True)
class RoundtripCase:
    event: BaseEvent[Any]
    valid_results: list[Any]
    invalid_results: list[Any]


@dataclass(slots=True)
class BusResumeRoundtripCase:
    source_bus: EventBus
    source_dump: dict[str, Any]
    handler_one_id: str
    handler_two_id: str
    event_one_id: str
    event_two_id: str


class PyTsIntResultEvent(BaseEvent[int]):
    value: int
    label: str


class PyTsFloatResultEvent(BaseEvent[float]):
    marker: str


class PyTsStringResultEvent(BaseEvent[str]):
    marker: str


class PyTsBoolResultEvent(BaseEvent[bool]):
    marker: str


class PyTsNullResultEvent(BaseEvent[NoneType]):
    marker: str


class PyTsStringListResultEvent(BaseEvent[list[str]]):
    marker: str


class PyTsDictResultEvent(BaseEvent[dict[str, int]]):
    marker: str


class PyTsNestedMapResultEvent(BaseEvent[dict[str, list[int]]]):
    marker: str


class PyTsTypedDictResultEvent(BaseEvent[PyTsTypedDictResult]):
    marker: str


class PyTsDataclassResultEvent(BaseEvent[PyTsDataclassResult]):
    marker: str


class PyTsScreenshotEvent(BaseEvent[ScreenshotResult]):
    target_id: str
    quality: str


def _value_repr(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True)
    except TypeError:
        return repr(value)


def _json_shape(value: Any) -> JsonShape:
    if isinstance(value, list):
        return [_json_shape(item) for item in cast(list[Any], value)]
    if isinstance(value, dict):
        value_dict = cast(dict[Any, Any], value)
        if isinstance(value_dict.get('$schema'), str) and isinstance(value_dict.get('type'), str):
            return {'$schema': 'str', 'type': 'str'}
        return {str(key): _json_shape(item) for key, item in value_dict.items()}
    if value is None:
        return 'null'
    if isinstance(value, bool):
        return 'bool'
    if isinstance(value, int | float):
        return 'number'
    return type(value).__name__


def _json_shape_contains(actual: JsonShape, expected: JsonShape) -> bool:
    if isinstance(actual, dict) and isinstance(expected, dict):
        return all(key in actual and _json_shape_contains(actual[key], value) for key, value in expected.items())
    if isinstance(actual, list) and isinstance(expected, list):
        return len(actual) == len(expected) and all(
            _json_shape_contains(actual_item, expected_item) for actual_item, expected_item in zip(actual, expected)
        )
    return actual == expected


def _assert_json_shape_equal(actual: Any, expected: Any, context: str) -> None:
    assert _json_shape_contains(_json_shape(actual), _json_shape(expected)), f'{context}: JSON shape changed'


def _accepts_result_type(result_type: Any, value: Any) -> bool:
    try:
        TypeAdapter(result_type).validate_python(value)
    except ValidationError:
        return False
    return True


def _assert_result_type_semantics_equal(
    original_result_type: Any,
    candidate_schema_json: dict[str, Any],
    valid_results: list[Any],
    invalid_results: list[Any],
    context: str,
) -> None:
    hydrated = BaseEvent[Any].model_validate({'event_type': 'SchemaSemanticsEvent', 'event_result_type': candidate_schema_json})
    candidate_result_type = hydrated.event_result_type
    assert candidate_result_type is not None, f'{context}: missing candidate result type after hydration'

    for value in valid_results:
        original_ok = _accepts_result_type(original_result_type, value)
        candidate_ok = _accepts_result_type(candidate_result_type, value)
        assert original_ok, f'{context}: original schema should accept {_value_repr(value)}'
        assert candidate_ok, f'{context}: candidate schema should accept {_value_repr(value)}'

    for value in invalid_results:
        original_ok = _accepts_result_type(original_result_type, value)
        candidate_ok = _accepts_result_type(candidate_result_type, value)
        assert not original_ok, f'{context}: original schema should reject {_value_repr(value)}'
        assert not candidate_ok, f'{context}: candidate schema should reject {_value_repr(value)}'

    for value in [*valid_results, *invalid_results]:
        original_ok = _accepts_result_type(original_result_type, value)
        candidate_ok = _accepts_result_type(candidate_result_type, value)
        assert candidate_ok == original_ok, (
            f'{context}: schema decision mismatch for {_value_repr(value)} (expected {original_ok}, got {candidate_ok})'
        )


def _build_python_roundtrip_cases() -> list[RoundtripCase]:
    parent = PyTsIntResultEvent(
        value=7,
        label='parent',
        event_path=['PyBus#aaaa'],
        event_timeout=12.5,
    )

    screenshot_event = PyTsScreenshotEvent(
        target_id='0c1ccf21-65c0-7390-8b64-9182e985740e',
        quality='high',
        event_parent_id=parent.event_id,
        event_path=['PyBus#aaaa', 'TsBridge#bbbb'],
        event_timeout=33.0,
    )

    float_event = PyTsFloatResultEvent(
        marker='float',
        event_parent_id=parent.event_id,
        event_path=['PyBus#aaaa'],
    )
    string_event = PyTsStringResultEvent(
        marker='string',
        event_parent_id=parent.event_id,
        event_path=['PyBus#aaaa'],
    )
    bool_event = PyTsBoolResultEvent(
        marker='bool',
        event_path=['PyBus#aaaa'],
    )
    null_event = PyTsNullResultEvent(
        marker='null',
        event_path=['PyBus#aaaa'],
    )
    list_event = PyTsStringListResultEvent(
        marker='list[str]',
        event_parent_id=parent.event_id,
        event_path=['PyBus#aaaa'],
    )
    dict_event = PyTsDictResultEvent(
        marker='dict[str,int]',
        event_path=['PyBus#aaaa'],
    )
    nested_map_event = PyTsNestedMapResultEvent(
        marker='dict[str,list[int]]',
        event_path=['PyBus#aaaa'],
    )
    typed_dict_event = PyTsTypedDictResultEvent(
        marker='typeddict',
        event_path=['PyBus#aaaa'],
    )
    dataclass_event = PyTsDataclassResultEvent(
        marker='dataclass',
        event_path=['PyBus#aaaa'],
    )

    return [
        RoundtripCase(
            event=parent,
            valid_results=[0, -5, 42],
            invalid_results=[{}, [], 'not-int'],
        ),
        RoundtripCase(
            event=float_event,
            valid_results=[0.5, 12.25, 3],
            invalid_results=[{}, [], 'not-number'],
        ),
        RoundtripCase(
            event=string_event,
            valid_results=['ok', ''],
            invalid_results=[{}, [], 123],
        ),
        RoundtripCase(
            event=bool_event,
            valid_results=[True, False],
            invalid_results=[{}, [], 'not-bool'],
        ),
        RoundtripCase(
            event=null_event,
            valid_results=[None],
            invalid_results=[{}, [], 0, False, 'not-null'],
        ),
        RoundtripCase(
            event=list_event,
            valid_results=[['a', 'b'], []],
            invalid_results=[{}, 'not-list', 123],
        ),
        RoundtripCase(
            event=dict_event,
            valid_results=[{'ok': 1, 'failed': 2}, {}],
            invalid_results=[['not', 'dict'], 'bad', 123],
        ),
        RoundtripCase(
            event=nested_map_event,
            valid_results=[{'a': [1, 2], 'b': []}, {}],
            invalid_results=[{'a': 'not-list'}, ['bad'], 123],
        ),
        RoundtripCase(
            event=typed_dict_event,
            valid_results=[{'name': 'alpha', 'active': True, 'count': 2}],
            invalid_results=[{'name': 'alpha'}, {'name': 123, 'active': True, 'count': 2}],
        ),
        RoundtripCase(
            event=dataclass_event,
            valid_results=[{'name': 'model', 'score': 0.85, 'tags': ['a', 'b']}],
            invalid_results=[{'name': 'model', 'score': 'not-number', 'tags': ['a']}, {'name': 'model', 'score': 1.0}],
        ),
        RoundtripCase(
            event=screenshot_event,
            valid_results=[
                {
                    'image_url': 'https://img.local/1.png',
                    'width': 1920,
                    'height': 1080,
                    'tags': ['hero', 'dashboard'],
                    'is_animated': False,
                    'confidence_scores': [0.95, 0.89],
                    'metadata': {'score': 0.99, 'variance': 0.01},
                    'regions': [
                        {'id': '98f51f1d-b10a-7cd9-8ee6-cb706153f717', 'label': 'face', 'score': 0.9, 'visible': True},
                        {'id': '5f234e9d-29e9-7921-8cf2-2a65f6ba3bdd', 'label': 'button', 'score': 0.7, 'visible': False},
                    ],
                }
            ],
            invalid_results=[
                {
                    'image_url': 123,
                    'width': 1920,
                    'height': 1080,
                    'tags': ['hero'],
                    'is_animated': False,
                    'confidence_scores': [0.95],
                    'metadata': {'score': 0.99},
                    'regions': [{'id': '98f51f1d-b10a-7cd9-8ee6-cb706153f717', 'label': 'face', 'score': 0.9, 'visible': True}],
                },
                {
                    'image_url': 'https://img.local/1.png',
                    'width': 1920,
                    'height': 1080,
                    'tags': ['hero'],
                    'is_animated': False,
                    'confidence_scores': [0.95],
                    'metadata': {'score': 0.99},
                    'regions': [{'id': 123, 'label': 'face', 'score': 0.9, 'visible': True}],
                },
            ],
        ),
    ]


def _ts_roundtrip_events(payload: list[dict[str, Any]], tmp_path: Path) -> list[dict[str, Any]]:
    node_bin = shutil.which('node')
    assert node_bin is not None, 'node is required for python<->ts roundtrip tests'

    repo_root = Path(__file__).resolve().parents[1]
    ts_root = repo_root / 'abxbus-ts'
    assert (ts_root / 'dist' / 'esm' / 'index.js').exists(), (
        'abxbus-ts dist/esm build not found. Run `pnpm --dir abxbus-ts run build` before cross-runtime tests.'
    )

    in_path = tmp_path / 'python_events.json'
    out_path = tmp_path / 'ts_events.json'
    in_path.write_text(json.dumps(payload, indent=2), encoding='utf-8')

    ts_script = """
import { readFileSync, writeFileSync } from 'node:fs'
import { BaseEvent } from './dist/esm/index.js'

const inputPath = process.env.ABXBUS_PY_TS_INPUT_PATH
const outputPath = process.env.ABXBUS_PY_TS_OUTPUT_PATH
if (!inputPath || !outputPath) {
  throw new Error('missing ABXBUS_PY_TS_INPUT_PATH or ABXBUS_PY_TS_OUTPUT_PATH')
}

const raw = JSON.parse(readFileSync(inputPath, 'utf8'))
if (!Array.isArray(raw)) {
  throw new Error('expected array payload')
}

const roundtripped = raw.map((item) => BaseEvent.fromJSON(item).toJSON())
writeFileSync(outputPath, JSON.stringify(roundtripped, null, 2), 'utf8')
"""

    env = os.environ.copy()
    env['ABXBUS_PY_TS_INPUT_PATH'] = str(in_path)
    env['ABXBUS_PY_TS_OUTPUT_PATH'] = str(out_path)
    try:
        proc = subprocess.run(
            [node_bin, '--input-type=module', '-e', ts_script],
            cwd=ts_root,
            env=env,
            capture_output=True,
            text=True,
            timeout=SUBPROCESS_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        pytest.fail(f'node/esm event roundtrip timed out after {SUBPROCESS_TIMEOUT_SECONDS}s: {exc}')

    assert proc.returncode == 0, f'node/esm roundtrip failed:\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}'
    return json.loads(out_path.read_text(encoding='utf-8'))


def _ts_roundtrip_bus(payload: dict[str, Any], tmp_path: Path) -> dict[str, Any]:
    node_bin = shutil.which('node')
    assert node_bin is not None, 'node is required for python<->ts roundtrip tests'

    repo_root = Path(__file__).resolve().parents[1]
    ts_root = repo_root / 'abxbus-ts'
    assert (ts_root / 'dist' / 'esm' / 'index.js').exists(), (
        'abxbus-ts dist/esm build not found. Run `pnpm --dir abxbus-ts run build` before cross-runtime tests.'
    )

    in_path = tmp_path / 'python_bus.json'
    out_path = tmp_path / 'ts_bus.json'
    in_path.write_text(json.dumps(payload, indent=2), encoding='utf-8')

    ts_script = """
import { readFileSync, writeFileSync } from 'node:fs'
import { EventBus } from './dist/esm/index.js'

const inputPath = process.env.ABXBUS_PY_TS_BUS_INPUT_PATH
const outputPath = process.env.ABXBUS_PY_TS_BUS_OUTPUT_PATH
if (!inputPath || !outputPath) {
  throw new Error('missing ABXBUS_PY_TS_BUS_INPUT_PATH or ABXBUS_PY_TS_BUS_OUTPUT_PATH')
}

const raw = JSON.parse(readFileSync(inputPath, 'utf8'))
if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
  throw new Error('expected object payload')
}

const roundtripped = EventBus.fromJSON(raw).toJSON()
writeFileSync(outputPath, JSON.stringify(roundtripped, null, 2), 'utf8')
"""

    env = os.environ.copy()
    env['ABXBUS_PY_TS_BUS_INPUT_PATH'] = str(in_path)
    env['ABXBUS_PY_TS_BUS_OUTPUT_PATH'] = str(out_path)
    try:
        proc = subprocess.run(
            [node_bin, '--input-type=module', '-e', ts_script],
            cwd=ts_root,
            env=env,
            capture_output=True,
            text=True,
            timeout=SUBPROCESS_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        pytest.fail(f'node/esm bus roundtrip timed out after {SUBPROCESS_TIMEOUT_SECONDS}s: {exc}')

    assert proc.returncode == 0, f'node/esm bus roundtrip failed:\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}'
    return json.loads(out_path.read_text(encoding='utf-8'))


def _rust_roundtrip(mode: str, payload: list[dict[str, Any]] | dict[str, Any], tmp_path: Path) -> Any:
    cargo_bin = shutil.which('cargo')
    assert cargo_bin is not None, 'cargo is required for python<->rust roundtrip tests'

    repo_root = Path(__file__).resolve().parents[1]
    rust_root = repo_root / 'abxbus-rust'
    input_path = tmp_path / f'python_{mode}_for_rust.json'
    output_path = tmp_path / f'rust_{mode}_roundtripped.json'
    input_path.write_text(json.dumps(payload, indent=2), encoding='utf-8')

    try:
        proc = subprocess.run(
            [
                cargo_bin,
                'run',
                '--quiet',
                '--manifest-path',
                str(rust_root / 'Cargo.toml'),
                '--bin',
                'abxbus-rust-roundtrip',
                '--',
                mode,
                str(input_path),
                str(output_path),
            ],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=RUST_SUBPROCESS_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        pytest.fail(f'rust {mode} roundtrip timed out after {RUST_SUBPROCESS_TIMEOUT_SECONDS}s: {exc}')

    assert proc.returncode == 0, f'rust {mode} roundtrip failed:\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}'
    return json.loads(output_path.read_text(encoding='utf-8'))


def _rust_roundtrip_events(payload: list[dict[str, Any]], tmp_path: Path) -> list[dict[str, Any]]:
    result = _rust_roundtrip('events', payload, tmp_path)
    assert isinstance(result, list)
    return cast(list[dict[str, Any]], result)


def _rust_roundtrip_bus(payload: dict[str, Any], tmp_path: Path) -> dict[str, Any]:
    result = _rust_roundtrip('bus', payload, tmp_path)
    assert isinstance(result, dict)
    return cast(dict[str, Any], result)


def _go_roundtrip(mode: str, payload: list[dict[str, Any]] | dict[str, Any], tmp_path: Path) -> Any:
    go_bin = shutil.which('go')
    assert go_bin is not None, 'go is required for python<->go roundtrip tests'

    repo_root = Path(__file__).resolve().parents[1]
    go_root = repo_root / 'abxbus-go'
    input_path = tmp_path / f'python_{mode}_for_go.json'
    output_path = tmp_path / f'go_{mode}_roundtripped.json'
    input_path.write_text(json.dumps(payload, indent=2), encoding='utf-8')

    try:
        proc = subprocess.run(
            [
                go_bin,
                'run',
                './tests/roundtrip_cli',
                mode,
                str(input_path),
                str(output_path),
            ],
            cwd=go_root,
            capture_output=True,
            text=True,
            timeout=GO_SUBPROCESS_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        pytest.fail(f'go {mode} roundtrip timed out after {GO_SUBPROCESS_TIMEOUT_SECONDS}s: {exc}')

    assert proc.returncode == 0, f'go {mode} roundtrip failed:\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}'
    return json.loads(output_path.read_text(encoding='utf-8'))


def _go_roundtrip_events(payload: list[dict[str, Any]], tmp_path: Path) -> list[dict[str, Any]]:
    result = _go_roundtrip('events', payload, tmp_path)
    assert isinstance(result, list)
    return cast(list[dict[str, Any]], result)


def _go_roundtrip_bus(payload: dict[str, Any], tmp_path: Path) -> dict[str, Any]:
    result = _go_roundtrip('bus', payload, tmp_path)
    assert isinstance(result, dict)
    return cast(dict[str, Any], result)


def _assert_events_roundtrip_matches_original(
    original_dumped: list[dict[str, Any]],
    roundtripped: list[dict[str, Any]],
    cases_by_type: dict[str, RoundtripCase],
    context: str,
) -> None:
    assert len(roundtripped) == len(original_dumped)

    for i, original in enumerate(original_dumped):
        runtime_event = roundtripped[i]
        assert isinstance(runtime_event, dict)

        event_type = str(original.get('event_type'))
        semantics_case = cases_by_type.get(event_type)
        assert semantics_case is not None, f'missing semantics case for event_type={event_type}'

        _assert_json_shape_equal(runtime_event, original, f'{context} {event_type}')

        for key, value in original.items():
            assert key in runtime_event, f'missing key after {context}: {key}'
            if key == 'event_result_type':
                assert isinstance(runtime_event[key], dict), 'event_result_type should serialize as JSON schema dict'
                assert runtime_event[key] == value, f'event_result_type schema changed after {context}'
                _assert_result_type_semantics_equal(
                    semantics_case.event.event_result_type,
                    runtime_event[key],
                    semantics_case.valid_results,
                    semantics_case.invalid_results,
                    f'{context} {event_type}',
                )
            else:
                assert runtime_event[key] == value, f'field changed after {context}: {key}'

        restored = BaseEvent[Any].model_validate(runtime_event)
        restored_dump = restored.model_dump(mode='json')
        _assert_json_shape_equal(restored_dump, original, f'python reload after {context} {event_type}')
        for key, value in original.items():
            assert key in restored_dump, f'missing key after python reload from {context}: {key}'
            if key == 'event_result_type':
                assert isinstance(restored_dump[key], dict), 'event_result_type should remain JSON schema after reload'
                assert restored_dump[key] == value, f'event_result_type schema changed after python reload from {context}'
                _assert_result_type_semantics_equal(
                    semantics_case.event.event_result_type,
                    restored_dump[key],
                    semantics_case.valid_results,
                    semantics_case.invalid_results,
                    f'python reload after {context} {event_type}',
                )
            else:
                assert restored_dump[key] == value, f'field changed after python reload from {context}: {key}'


def test_python_to_ts_roundtrip_preserves_event_fields_and_result_type_semantics(tmp_path: Path) -> None:
    cases = _build_python_roundtrip_cases()
    events = [entry.event for entry in cases]
    cases_by_type = {entry.event.event_type: entry for entry in cases}
    python_dumped = [event.model_dump(mode='json') for event in events]

    # Ensure Python emits JSONSchema for return value types before sending to TS.
    for event_dump in python_dumped:
        assert 'event_result_type' in event_dump
        assert isinstance(event_dump['event_result_type'], dict)

    ts_roundtripped = _ts_roundtrip_events(python_dumped, tmp_path)
    assert len(ts_roundtripped) == len(python_dumped)

    for i, original in enumerate(python_dumped):
        ts_event = ts_roundtripped[i]
        assert isinstance(ts_event, dict)

        event_type = str(original.get('event_type'))
        semantics_case = cases_by_type.get(event_type)
        assert semantics_case is not None, f'missing semantics case for event_type={event_type}'

        _assert_json_shape_equal(ts_event, original, f'ts roundtrip {event_type}')

        # Every field Python emitted should survive through TS serialization.
        for key, value in original.items():
            assert key in ts_event, f'missing key after ts roundtrip: {key}'
            if key == 'event_result_type':
                assert isinstance(ts_event[key], dict), 'event_result_type should serialize as JSON schema dict'
                _assert_result_type_semantics_equal(
                    semantics_case.event.event_result_type,
                    ts_event[key],
                    semantics_case.valid_results,
                    semantics_case.invalid_results,
                    f'ts roundtrip {event_type}',
                )
            else:
                assert ts_event[key] == value, f'field changed after ts roundtrip: {key}'

        # Verify we can load back into Python BaseEvent and keep the same payload/semantics.
        restored = BaseEvent[Any].model_validate(ts_event)
        restored_dump = restored.model_dump(mode='json')
        _assert_json_shape_equal(restored_dump, original, f'python reload {event_type}')
        for key, value in original.items():
            assert key in restored_dump, f'missing key after python reload: {key}'
            if key == 'event_result_type':
                assert isinstance(restored_dump[key], dict), 'event_result_type should remain JSON schema after reload'
                _assert_result_type_semantics_equal(
                    semantics_case.event.event_result_type,
                    restored_dump[key],
                    semantics_case.valid_results,
                    semantics_case.invalid_results,
                    f'python reload {event_type}',
                )
            else:
                assert restored_dump[key] == value, f'field changed after python reload: {key}'


def test_python_to_rust_roundtrip_preserves_event_fields_and_result_type_semantics(tmp_path: Path) -> None:
    cases = _build_python_roundtrip_cases()
    events = [entry.event for entry in cases]
    cases_by_type = {entry.event.event_type: entry for entry in cases}
    python_dumped = [event.model_dump(mode='json') for event in events]

    # Ensure Python emits JSONSchema for return value types before sending to Rust.
    for event_dump in python_dumped:
        assert 'event_result_type' in event_dump
        assert isinstance(event_dump['event_result_type'], dict)

    rust_roundtripped = _rust_roundtrip_events(python_dumped, tmp_path)
    assert len(rust_roundtripped) == len(python_dumped)

    for i, original in enumerate(python_dumped):
        rust_event = rust_roundtripped[i]
        assert isinstance(rust_event, dict)

        event_type = str(original.get('event_type'))
        semantics_case = cases_by_type.get(event_type)
        assert semantics_case is not None, f'missing semantics case for event_type={event_type}'

        _assert_json_shape_equal(rust_event, original, f'rust roundtrip {event_type}')

        # Every field Python emitted should survive through Rust serialization.
        for key, value in original.items():
            assert key in rust_event, f'missing key after rust roundtrip: {key}'
            if key == 'event_result_type':
                assert isinstance(rust_event[key], dict), 'event_result_type should serialize as JSON schema dict'
                _assert_result_type_semantics_equal(
                    semantics_case.event.event_result_type,
                    rust_event[key],
                    semantics_case.valid_results,
                    semantics_case.invalid_results,
                    f'rust roundtrip {event_type}',
                )
            else:
                assert rust_event[key] == value, f'field changed after rust roundtrip: {key}'

        # Verify we can load back into Python BaseEvent and keep the same payload/semantics.
        restored = BaseEvent[Any].model_validate(rust_event)
        restored_dump = restored.model_dump(mode='json')
        _assert_json_shape_equal(restored_dump, original, f'python reload {event_type}')
        for key, value in original.items():
            assert key in restored_dump, f'missing key after python reload: {key}'
            if key == 'event_result_type':
                assert isinstance(restored_dump[key], dict), 'event_result_type should remain JSON schema after reload'
                _assert_result_type_semantics_equal(
                    semantics_case.event.event_result_type,
                    restored_dump[key],
                    semantics_case.valid_results,
                    semantics_case.invalid_results,
                    f'python reload {event_type}',
                )
            else:
                assert restored_dump[key] == value, f'field changed after python reload: {key}'


def test_python_to_go_roundtrip_preserves_event_fields_and_result_type_semantics(tmp_path: Path) -> None:
    cases = _build_python_roundtrip_cases()
    events = [entry.event for entry in cases]
    cases_by_type = {entry.event.event_type: entry for entry in cases}
    python_dumped = [event.model_dump(mode='json') for event in events]

    for event_dump in python_dumped:
        assert 'event_result_type' in event_dump
        assert isinstance(event_dump['event_result_type'], dict)

    go_roundtripped = _go_roundtrip_events(python_dumped, tmp_path)
    assert len(go_roundtripped) == len(python_dumped)

    for i, original in enumerate(python_dumped):
        go_event = go_roundtripped[i]
        assert isinstance(go_event, dict)

        event_type = str(original.get('event_type'))
        semantics_case = cases_by_type.get(event_type)
        assert semantics_case is not None, f'missing semantics case for event_type={event_type}'

        _assert_json_shape_equal(go_event, original, f'go roundtrip {event_type}')

        for key, value in original.items():
            assert key in go_event, f'missing key after go roundtrip: {key}'
            if key == 'event_result_type':
                assert isinstance(go_event[key], dict), 'event_result_type should serialize as JSON schema dict'
                assert go_event[key] == value, f'event_result_type schema changed after go roundtrip: {event_type}'
                _assert_result_type_semantics_equal(
                    semantics_case.event.event_result_type,
                    go_event[key],
                    semantics_case.valid_results,
                    semantics_case.invalid_results,
                    f'go roundtrip {event_type}',
                )
            else:
                assert go_event[key] == value, f'field changed after go roundtrip: {key}'

        restored = BaseEvent[Any].model_validate(go_event)
        restored_dump = restored.model_dump(mode='json')
        _assert_json_shape_equal(restored_dump, original, f'python reload {event_type}')
        for key, value in original.items():
            assert key in restored_dump, f'missing key after python reload: {key}'
            if key == 'event_result_type':
                assert isinstance(restored_dump[key], dict), 'event_result_type should remain JSON schema after reload'
                assert restored_dump[key] == value, f'event_result_type schema changed after python reload from go: {event_type}'
                _assert_result_type_semantics_equal(
                    semantics_case.event.event_result_type,
                    restored_dump[key],
                    semantics_case.valid_results,
                    semantics_case.invalid_results,
                    f'python reload {event_type}',
                )
            else:
                assert restored_dump[key] == value, f'field changed after python reload: {key}'


def test_python_to_rust_to_go_to_python_roundtrip_preserves_event_fields_and_result_type_semantics(
    tmp_path: Path,
) -> None:
    cases = _build_python_roundtrip_cases()
    events = [entry.event for entry in cases]
    cases_by_type = {entry.event.event_type: entry for entry in cases}
    python_dumped = [event.model_dump(mode='json') for event in events]

    rust_roundtripped = _rust_roundtrip_events(python_dumped, tmp_path)
    go_roundtripped = _go_roundtrip_events(rust_roundtripped, tmp_path)
    _assert_events_roundtrip_matches_original(
        python_dumped,
        go_roundtripped,
        cases_by_type,
        'python -> rust -> go roundtrip',
    )


def test_python_to_go_to_rust_to_python_roundtrip_preserves_event_fields_and_result_type_semantics(
    tmp_path: Path,
) -> None:
    cases = _build_python_roundtrip_cases()
    events = [entry.event for entry in cases]
    cases_by_type = {entry.event.event_type: entry for entry in cases}
    python_dumped = [event.model_dump(mode='json') for event in events]

    go_roundtripped = _go_roundtrip_events(python_dumped, tmp_path)
    rust_roundtripped = _rust_roundtrip_events(go_roundtripped, tmp_path)
    _assert_events_roundtrip_matches_original(
        python_dumped,
        rust_roundtripped,
        cases_by_type,
        'python -> go -> rust roundtrip',
    )


async def test_python_to_ts_roundtrip_schema_enforcement_after_reload(tmp_path: Path) -> None:
    events = [entry.event for entry in _build_python_roundtrip_cases()]
    python_dumped = [event.model_dump(mode='json') for event in events]
    ts_roundtripped = _ts_roundtrip_events(python_dumped, tmp_path)
    await _assert_python_schema_enforcement_after_runtime_reload(
        ts_roundtripped,
        wrong_bus_name='py_ts_py_wrong_shape',
        right_bus_name='py_ts_py_right_shape',
    )


async def test_python_to_rust_roundtrip_schema_enforcement_after_reload(tmp_path: Path) -> None:
    events = [entry.event for entry in _build_python_roundtrip_cases()]
    python_dumped = [event.model_dump(mode='json') for event in events]
    rust_roundtripped = _rust_roundtrip_events(python_dumped, tmp_path)
    await _assert_python_schema_enforcement_after_runtime_reload(
        rust_roundtripped,
        wrong_bus_name='py_rust_py_wrong_shape',
        right_bus_name='py_rust_py_right_shape',
    )


async def test_python_to_go_roundtrip_schema_enforcement_after_reload(tmp_path: Path) -> None:
    events = [entry.event for entry in _build_python_roundtrip_cases()]
    python_dumped = [event.model_dump(mode='json') for event in events]
    go_roundtripped = _go_roundtrip_events(python_dumped, tmp_path)
    await _assert_python_schema_enforcement_after_runtime_reload(
        go_roundtripped,
        wrong_bus_name='py_go_py_wrong_shape',
        right_bus_name='py_go_py_right_shape',
    )


async def _assert_python_schema_enforcement_after_runtime_reload(
    runtime_roundtripped: list[dict[str, Any]],
    *,
    wrong_bus_name: str,
    right_bus_name: str,
) -> None:
    screenshot_payload = next(event for event in runtime_roundtripped if event.get('event_type') == 'PyTsScreenshotEvent')

    wrong_bus = EventBus(name=wrong_bus_name)

    async def wrong_shape_handler(event: BaseEvent[Any]) -> dict[str, Any]:
        return {
            'image_url': 123,  # wrong: should be string
            'width': '1920',  # wrong: should be int
            'height': 1080,
            'tags': ['a', 'b'],
            'is_animated': 'false',  # wrong: should be bool
            'confidence_scores': [0.9, 0.8],
            'metadata': {'score': 0.99},
            'regions': [{'id': '98f51f1d-b10a-7cd9-8ee6-cb706153f717', 'label': 'face', 'score': 0.9, 'visible': True}],
        }

    wrong_bus.on('PyTsScreenshotEvent', wrong_shape_handler)
    wrong_event = BaseEvent[Any].model_validate(screenshot_payload)
    assert isinstance(wrong_event.event_result_type, type)
    assert issubclass(wrong_event.event_result_type, BaseModel)
    await asyncio.wait_for(wrong_bus.emit(wrong_event), timeout=EVENT_WAIT_TIMEOUT_SECONDS)
    wrong_result = next(iter(wrong_event.event_results.values()))
    assert wrong_result.status == 'error'
    assert wrong_result.error is not None
    await wrong_bus.destroy()

    right_bus = EventBus(name=right_bus_name)

    async def right_shape_handler(event: BaseEvent[Any]) -> dict[str, Any]:
        return {
            'image_url': 'https://img.local/1.png',
            'width': 1920,
            'height': 1080,
            'tags': ['hero', 'dashboard'],
            'is_animated': False,
            'confidence_scores': [0.95, 0.89],
            'metadata': {'score': 0.99, 'variance': 0.01},
            'regions': [
                {'id': '98f51f1d-b10a-7cd9-8ee6-cb706153f717', 'label': 'face', 'score': 0.9, 'visible': True},
                {'id': '5f234e9d-29e9-7921-8cf2-2a65f6ba3bdd', 'label': 'button', 'score': 0.7, 'visible': False},
            ],
        }

    right_bus.on('PyTsScreenshotEvent', right_shape_handler)
    right_event = BaseEvent[Any].model_validate(screenshot_payload)
    assert isinstance(right_event.event_result_type, type)
    assert issubclass(right_event.event_result_type, BaseModel)
    await asyncio.wait_for(right_bus.emit(right_event), timeout=EVENT_WAIT_TIMEOUT_SECONDS)
    right_result = next(iter(right_event.event_results.values()))
    assert right_result.status == 'completed'
    assert right_result.error is None
    assert right_result.result is not None
    await right_bus.destroy()


class PyTsBusResumeEvent(BaseEvent[str]):
    label: str


def _build_bus_resume_roundtrip_case(name: str, bus_id: str) -> BusResumeRoundtripCase:
    source_bus = EventBus(
        name=name,
        id=bus_id,
        event_handler_detect_file_paths=False,
        event_handler_concurrency='serial',
        event_handler_completion='all',
    )

    async def handler_one(event: PyTsBusResumeEvent) -> str:
        return f'h1:{event.label}'

    async def handler_two(event: PyTsBusResumeEvent) -> str:
        return f'h2:{event.label}'

    handler_one_entry = source_bus.on(PyTsBusResumeEvent, handler_one)
    handler_two_entry = source_bus.on(PyTsBusResumeEvent, handler_two)
    assert handler_one_entry.id is not None
    assert handler_two_entry.id is not None

    event_one = PyTsBusResumeEvent(label='e1')
    event_two = PyTsBusResumeEvent(label='e2')
    seeded = event_one.event_result_update(handler=handler_one_entry, eventbus=source_bus, status='pending')
    event_one.event_result_update(handler=handler_two_entry, eventbus=source_bus, status='pending')
    seeded.update(status='completed', result='seeded')

    source_bus.event_history[event_one.event_id] = event_one
    source_bus.event_history[event_two.event_id] = event_two
    source_bus.pending_event_queue = CleanShutdownQueue[BaseEvent[Any]](maxsize=0)
    source_bus.pending_event_queue.put_nowait(event_one)
    source_bus.pending_event_queue.put_nowait(event_two)

    return BusResumeRoundtripCase(
        source_bus=source_bus,
        source_dump=source_bus.model_dump(),
        handler_one_id=handler_one_entry.id,
        handler_two_id=handler_two_entry.id,
        event_one_id=event_one.event_id,
        event_two_id=event_two.event_id,
    )


async def _assert_bus_roundtrip_rehydrates_and_resumes(
    case: BusResumeRoundtripCase,
    roundtripped: dict[str, Any],
    context: str,
) -> None:
    _assert_json_shape_equal(roundtripped, case.source_dump, context)
    restored = EventBus.validate(roundtripped)
    restored_dump = restored.model_dump()
    _assert_json_shape_equal(restored_dump, case.source_dump, f'{context} python reload')

    assert restored_dump['handlers'] == case.source_dump['handlers']
    assert restored_dump['handlers_by_key'] == case.source_dump['handlers_by_key']
    assert restored_dump['pending_event_queue'] == case.source_dump['pending_event_queue']
    assert set(restored_dump['event_history']) == set(case.source_dump['event_history'])

    restored_event_one = restored.event_history[case.event_one_id]
    preseeded = restored_event_one.event_results[case.handler_one_id]
    assert preseeded.status == 'completed'
    assert preseeded.result == 'seeded'
    assert preseeded.handler is restored.handlers[case.handler_one_id]

    trigger = restored.emit(PyTsBusResumeEvent(label='e3'))
    await asyncio.wait_for(trigger.wait(), timeout=EVENT_WAIT_TIMEOUT_SECONDS)

    done_one = restored.event_history[case.event_one_id]
    done_two = restored.event_history[case.event_two_id]
    done_three = restored.event_history[trigger.event_id]
    assert done_three.event_status == 'completed'
    if restored.pending_event_queue is not None:
        assert restored.pending_event_queue.qsize() == 0
    assert all(result.status == 'completed' for result in done_one.event_results.values())
    assert all(result.status == 'completed' for result in done_two.event_results.values())
    assert done_one.event_results[case.handler_one_id].result == 'seeded'
    assert done_one.event_results[case.handler_two_id].result is None

    await restored.destroy(clear=True)


@pytest.mark.asyncio
async def test_python_to_ts_to_python_bus_roundtrip_rehydrates_and_resumes(tmp_path: Path) -> None:
    source_bus = EventBus(
        name='PyTsBusSource',
        id='018f8e40-1234-7000-8000-00000000bb22',
        event_handler_detect_file_paths=False,
        event_handler_concurrency='serial',
        event_handler_completion='all',
    )

    async def handler_one(event: PyTsBusResumeEvent) -> str:
        return f'h1:{event.label}'

    async def handler_two(event: PyTsBusResumeEvent) -> str:
        return f'h2:{event.label}'

    handler_one_entry = source_bus.on(PyTsBusResumeEvent, handler_one)
    handler_two_entry = source_bus.on(PyTsBusResumeEvent, handler_two)
    assert handler_one_entry.id is not None
    assert handler_two_entry.id is not None
    handler_one_id = handler_one_entry.id
    handler_two_id = handler_two_entry.id

    event_one = PyTsBusResumeEvent(label='e1')
    event_two = PyTsBusResumeEvent(label='e2')
    seeded = event_one.event_result_update(handler=handler_one_entry, eventbus=source_bus, status='pending')
    event_one.event_result_update(handler=handler_two_entry, eventbus=source_bus, status='pending')
    seeded.update(status='completed', result='seeded')

    source_bus.event_history[event_one.event_id] = event_one
    source_bus.event_history[event_two.event_id] = event_two
    source_bus.pending_event_queue = CleanShutdownQueue[BaseEvent[Any]](maxsize=0)
    source_bus.pending_event_queue.put_nowait(event_one)
    source_bus.pending_event_queue.put_nowait(event_two)

    source_dump = source_bus.model_dump()
    ts_roundtripped = _ts_roundtrip_bus(source_dump, tmp_path)
    _assert_json_shape_equal(ts_roundtripped, source_dump, 'python -> ts bus roundtrip')
    restored = EventBus.validate(ts_roundtripped)
    restored_dump = restored.model_dump()
    _assert_json_shape_equal(restored_dump, source_dump, 'python -> ts -> python bus reload')

    assert restored_dump['handlers'] == source_dump['handlers']
    assert restored_dump['handlers_by_key'] == source_dump['handlers_by_key']
    assert restored_dump['pending_event_queue'] == source_dump['pending_event_queue']
    assert set(restored_dump['event_history']) == set(source_dump['event_history'])

    restored_event_one = restored.event_history[event_one.event_id]
    preseeded = restored_event_one.event_results[handler_one_id]
    assert preseeded.status == 'completed'
    assert preseeded.result == 'seeded'
    assert preseeded.handler is restored.handlers[handler_one_id]

    trigger = restored.emit(PyTsBusResumeEvent(label='e3'))
    await asyncio.wait_for(trigger.wait(), timeout=EVENT_WAIT_TIMEOUT_SECONDS)

    done_one = restored.event_history[event_one.event_id]
    done_two = restored.event_history[event_two.event_id]
    done_three = restored.event_history[trigger.event_id]
    assert done_three.event_status == 'completed'
    if restored.pending_event_queue is not None:
        assert restored.pending_event_queue.qsize() == 0
    assert all(result.status == 'completed' for result in done_one.event_results.values())
    assert all(result.status == 'completed' for result in done_two.event_results.values())
    assert done_one.event_results[handler_one_id].result == 'seeded'
    assert done_one.event_results[handler_two_id].result is None

    await source_bus.destroy(clear=True)
    await restored.destroy(clear=True)


@pytest.mark.asyncio
async def test_python_to_rust_to_go_to_python_bus_roundtrip_rehydrates_and_resumes(tmp_path: Path) -> None:
    case = _build_bus_resume_roundtrip_case(
        name='PyRustGoBusSource',
        bus_id='018f8e40-1234-7000-8000-00000000bb25',
    )
    rust_roundtripped = _rust_roundtrip_bus(case.source_dump, tmp_path)
    go_roundtripped = _go_roundtrip_bus(rust_roundtripped, tmp_path)
    await _assert_bus_roundtrip_rehydrates_and_resumes(
        case,
        go_roundtripped,
        'python -> rust -> go bus roundtrip',
    )
    await case.source_bus.destroy(clear=True)


@pytest.mark.asyncio
async def test_python_to_go_to_rust_to_python_bus_roundtrip_rehydrates_and_resumes(tmp_path: Path) -> None:
    case = _build_bus_resume_roundtrip_case(
        name='PyGoRustBusSource',
        bus_id='018f8e40-1234-7000-8000-00000000bb26',
    )
    go_roundtripped = _go_roundtrip_bus(case.source_dump, tmp_path)
    rust_roundtripped = _rust_roundtrip_bus(go_roundtripped, tmp_path)
    await _assert_bus_roundtrip_rehydrates_and_resumes(
        case,
        rust_roundtripped,
        'python -> go -> rust bus roundtrip',
    )
    await case.source_bus.destroy(clear=True)


@pytest.mark.asyncio
async def test_python_to_rust_to_python_bus_roundtrip_rehydrates_and_resumes(tmp_path: Path) -> None:
    source_bus = EventBus(
        name='PyRustBusSource',
        id='018f8e40-1234-7000-8000-00000000bb23',
        event_handler_detect_file_paths=False,
        event_handler_concurrency='serial',
        event_handler_completion='all',
    )

    async def handler_one(event: PyTsBusResumeEvent) -> str:
        return f'h1:{event.label}'

    async def handler_two(event: PyTsBusResumeEvent) -> str:
        return f'h2:{event.label}'

    handler_one_entry = source_bus.on(PyTsBusResumeEvent, handler_one)
    handler_two_entry = source_bus.on(PyTsBusResumeEvent, handler_two)
    assert handler_one_entry.id is not None
    assert handler_two_entry.id is not None
    handler_one_id = handler_one_entry.id
    handler_two_id = handler_two_entry.id

    event_one = PyTsBusResumeEvent(label='e1')
    event_two = PyTsBusResumeEvent(label='e2')
    seeded = event_one.event_result_update(handler=handler_one_entry, eventbus=source_bus, status='pending')
    event_one.event_result_update(handler=handler_two_entry, eventbus=source_bus, status='pending')
    seeded.update(status='completed', result='seeded')

    source_bus.event_history[event_one.event_id] = event_one
    source_bus.event_history[event_two.event_id] = event_two
    source_bus.pending_event_queue = CleanShutdownQueue[BaseEvent[Any]](maxsize=0)
    source_bus.pending_event_queue.put_nowait(event_one)
    source_bus.pending_event_queue.put_nowait(event_two)

    source_dump = source_bus.model_dump()
    rust_roundtripped = _rust_roundtrip_bus(source_dump, tmp_path)
    _assert_json_shape_equal(rust_roundtripped, source_dump, 'python -> rust bus roundtrip')
    restored = EventBus.validate(rust_roundtripped)
    restored_dump = restored.model_dump()
    _assert_json_shape_equal(restored_dump, source_dump, 'python -> rust -> python bus reload')

    assert restored_dump['handlers'] == source_dump['handlers']
    assert restored_dump['handlers_by_key'] == source_dump['handlers_by_key']
    assert restored_dump['pending_event_queue'] == source_dump['pending_event_queue']
    assert set(restored_dump['event_history']) == set(source_dump['event_history'])

    restored_event_one = restored.event_history[event_one.event_id]
    preseeded = restored_event_one.event_results[handler_one_id]
    assert preseeded.status == 'completed'
    assert preseeded.result == 'seeded'
    assert preseeded.handler is restored.handlers[handler_one_id]

    trigger = restored.emit(PyTsBusResumeEvent(label='e3'))
    await asyncio.wait_for(trigger.wait(), timeout=EVENT_WAIT_TIMEOUT_SECONDS)

    done_one = restored.event_history[event_one.event_id]
    done_two = restored.event_history[event_two.event_id]
    done_three = restored.event_history[trigger.event_id]
    assert done_three.event_status == 'completed'
    if restored.pending_event_queue is not None:
        assert restored.pending_event_queue.qsize() == 0
    assert all(result.status == 'completed' for result in done_one.event_results.values())
    assert all(result.status == 'completed' for result in done_two.event_results.values())
    assert done_one.event_results[handler_one_id].result == 'seeded'
    assert done_one.event_results[handler_two_id].result is None

    await source_bus.destroy(clear=True)
    await restored.destroy(clear=True)


@pytest.mark.asyncio
async def test_python_to_go_to_python_bus_roundtrip_rehydrates_and_resumes(tmp_path: Path) -> None:
    source_bus = EventBus(
        name='PyGoBusSource',
        id='018f8e40-1234-7000-8000-00000000bb24',
        event_handler_detect_file_paths=False,
        event_handler_concurrency='serial',
        event_handler_completion='all',
    )

    async def handler_one(event: PyTsBusResumeEvent) -> str:
        return f'h1:{event.label}'

    async def handler_two(event: PyTsBusResumeEvent) -> str:
        return f'h2:{event.label}'

    handler_one_entry = source_bus.on(PyTsBusResumeEvent, handler_one)
    handler_two_entry = source_bus.on(PyTsBusResumeEvent, handler_two)
    assert handler_one_entry.id is not None
    assert handler_two_entry.id is not None
    handler_one_id = handler_one_entry.id
    handler_two_id = handler_two_entry.id

    event_one = PyTsBusResumeEvent(label='e1')
    event_two = PyTsBusResumeEvent(label='e2')
    seeded = event_one.event_result_update(handler=handler_one_entry, eventbus=source_bus, status='pending')
    event_one.event_result_update(handler=handler_two_entry, eventbus=source_bus, status='pending')
    seeded.update(status='completed', result='seeded')

    source_bus.event_history[event_one.event_id] = event_one
    source_bus.event_history[event_two.event_id] = event_two
    source_bus.pending_event_queue = CleanShutdownQueue[BaseEvent[Any]](maxsize=0)
    source_bus.pending_event_queue.put_nowait(event_one)
    source_bus.pending_event_queue.put_nowait(event_two)

    source_dump = source_bus.model_dump()
    go_roundtripped = _go_roundtrip_bus(source_dump, tmp_path)
    _assert_json_shape_equal(go_roundtripped, source_dump, 'python -> go bus roundtrip')
    restored = EventBus.validate(go_roundtripped)
    restored_dump = restored.model_dump()
    _assert_json_shape_equal(restored_dump, source_dump, 'python -> go -> python bus reload')

    assert restored_dump['handlers'] == source_dump['handlers']
    assert restored_dump['handlers_by_key'] == source_dump['handlers_by_key']
    assert restored_dump['pending_event_queue'] == source_dump['pending_event_queue']
    assert set(restored_dump['event_history']) == set(source_dump['event_history'])

    restored_event_one = restored.event_history[event_one.event_id]
    preseeded = restored_event_one.event_results[handler_one_id]
    assert preseeded.status == 'completed'
    assert preseeded.result == 'seeded'
    assert preseeded.handler is restored.handlers[handler_one_id]

    trigger = restored.emit(PyTsBusResumeEvent(label='e3'))
    await asyncio.wait_for(trigger.wait(), timeout=EVENT_WAIT_TIMEOUT_SECONDS)

    done_one = restored.event_history[event_one.event_id]
    done_two = restored.event_history[event_two.event_id]
    done_three = restored.event_history[trigger.event_id]
    assert done_three.event_status == 'completed'
    if restored.pending_event_queue is not None:
        assert restored.pending_event_queue.qsize() == 0
    assert all(result.status == 'completed' for result in done_one.event_results.values())
    assert all(result.status == 'completed' for result in done_two.event_results.values())
    assert done_one.event_results[handler_one_id].result == 'seeded'
    assert done_one.event_results[handler_two_id].result is None

    await source_bus.destroy(clear=True)
    await restored.destroy(clear=True)


# Folded from test_bridges.py to keep test layout class-based.
"""Process-isolated roundtrip tests for bridge transports."""

import socket
import sqlite3
import sys
import tempfile
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from shutil import rmtree
from typing import Any

from uuid_extensions import uuid7str

from abxbus import BaseEvent, HTTPEventBridge, SocketEventBridge
from abxbus.bridge_jsonl import JSONLEventBridge
from abxbus.bridge_nats import NATSEventBridge
from abxbus.bridge_postgres import PostgresEventBridge
from abxbus.bridge_redis import RedisEventBridge
from abxbus.bridge_sqlite import SQLiteEventBridge
from abxbus.bridge_tachyon import TachyonEventBridge


class IPCPingEvent(BaseEvent):
    label: str


_TEST_RUN_ID = f'{int(time.time() * 1000)}-{uuid7str()[-8:]}'


def _make_temp_dir(prefix: str) -> Path:
    return Path(tempfile.mkdtemp(prefix=f'{prefix}-{_TEST_RUN_ID}-'))


def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('127.0.0.1', 0))
        return int(sock.getsockname()[1])


def _canonical(payload: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in payload.items():
        if key.endswith('_at') and isinstance(value, str):
            try:
                normalized[key] = datetime.fromisoformat(value).timestamp()
                continue
            except ValueError:
                pass
        normalized[key] = value
    return normalized


def _normalize_roundtrip_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _canonical(payload)
    normalized.pop('event_id', None)
    normalized.pop('event_path', None)
    # The listener snapshots the event from inside its local handler, where the
    # receiving bus has already attached handler bookkeeping that was not part
    # of the bridge payload.
    normalized.pop('event_results', None)
    # Dispatch now materializes event_concurrency defaults on the receiving bus.
    if normalized.get('event_concurrency') is None:
        normalized['event_concurrency'] = 'bus-serial'
    # Dispatch also materializes handler-level defaults on the receiving bus.
    if normalized.get('event_handler_concurrency') is None:
        normalized['event_handler_concurrency'] = 'serial'
    if normalized.get('event_handler_completion') is None:
        normalized['event_handler_completion'] = 'all'
    # event_status/event_started_at are now serialized, but the receiving bus
    # can advance them while handling the event. Normalize in-flight statuses.
    if normalized.get('event_status') in ('pending', 'started'):
        normalized['event_status'] = 'pending'
        normalized['event_started_at'] = None
        normalized['event_completed_at'] = None
    return normalized


@asynccontextmanager
async def _running_process(command: list[str], *, cwd: Path | None = None) -> AsyncGenerator[subprocess.Popen[str]]:
    process = subprocess.Popen(
        command,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        yield process
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


async def _wait_for_port(port: int, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            _, writer = await asyncio.open_connection('127.0.0.1', port)
            writer.close()
            await writer.wait_closed()
            return
        except OSError:
            await asyncio.sleep(0.05)
    raise TimeoutError(f'port did not open in time: {port}')


async def _wait_for_path(path: Path, *, process: subprocess.Popen[str], timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise AssertionError(f'worker exited early ({process.returncode})\nstdout:\n{stdout}\nstderr:\n{stderr}')
        await asyncio.sleep(0.05)
    raise TimeoutError(f'path did not appear in time: {path}')


def _make_sender_bridge(kind: str, config: dict[str, Any], *, low_latency: bool = False) -> Any:
    if kind == 'http':
        return HTTPEventBridge(send_to=str(config['endpoint']))
    if kind == 'socket':
        return SocketEventBridge(path=str(config['path']))
    if kind == 'jsonl':
        return JSONLEventBridge(str(config['path']), poll_interval=0.001 if low_latency else 0.05)
    if kind == 'sqlite':
        return SQLiteEventBridge(
            str(config['path']),
            str(config['table']),
            poll_interval=0.001 if low_latency else 0.05,
        )
    if kind == 'redis':
        return RedisEventBridge(str(config['url']))
    if kind == 'nats':
        return NATSEventBridge(str(config['server']), str(config['subject']))
    if kind == 'postgres':
        return PostgresEventBridge(str(config['url']))
    if kind == 'tachyon':
        return TachyonEventBridge(str(config['path']))
    raise ValueError(f'Unsupported bridge kind: {kind}')


def _make_listener_bridge(kind: str, config: dict[str, Any], *, low_latency: bool = False) -> Any:
    if kind == 'http':
        return HTTPEventBridge(listen_on=str(config['endpoint']))
    if kind == 'socket':
        return SocketEventBridge(path=str(config['path']))
    if kind == 'jsonl':
        return JSONLEventBridge(str(config['path']), poll_interval=0.001 if low_latency else 0.05)
    if kind == 'sqlite':
        return SQLiteEventBridge(
            str(config['path']),
            str(config['table']),
            poll_interval=0.001 if low_latency else 0.05,
        )
    if kind == 'redis':
        return RedisEventBridge(str(config['url']))
    if kind == 'nats':
        return NATSEventBridge(str(config['server']), str(config['subject']))
    if kind == 'postgres':
        return PostgresEventBridge(str(config['url']))
    if kind == 'tachyon':
        return TachyonEventBridge(str(config['path']))
    raise ValueError(f'Unsupported bridge kind: {kind}')


async def _measure_warm_latency_ms(kind: str, config: dict[str, Any]) -> float:
    attempts = 3
    last_error: BaseException | None = None

    for _attempt in range(attempts):
        sender = _make_sender_bridge(kind, config, low_latency=True)
        receiver = _make_listener_bridge(kind, config, low_latency=True)

        run_suffix = uuid7str()[-8:]
        warmup_prefix = f'warmup_{run_suffix}_'
        measured_prefix = f'measured_{run_suffix}_'
        warmup_count_target = 5
        measured_count_target = 1000

        warmup_seen_count = 0
        measured_seen_count = 0
        warmup_seen = asyncio.Event()
        measured_seen = asyncio.Event()

        async def _on_event(event: BaseEvent[Any]) -> None:
            nonlocal warmup_seen_count, measured_seen_count
            label = getattr(event, 'label', '')
            if not isinstance(label, str):
                return
            if label.startswith(warmup_prefix):
                warmup_seen_count += 1
                if warmup_seen_count >= warmup_count_target:
                    warmup_seen.set()
                return
            if label.startswith(measured_prefix):
                measured_seen_count += 1
                if measured_seen_count >= measured_count_target:
                    measured_seen.set()

        try:
            await sender.start()
            await receiver.start()
            receiver.on('IPCPingEvent', _on_event)
            await asyncio.sleep(0.1)

            for index in range(warmup_count_target):
                await sender.emit(
                    IPCPingEvent(
                        label=f'{warmup_prefix}{index}',
                    )
                )
            await asyncio.wait_for(warmup_seen.wait(), timeout=60.0)

            start_ns = time.perf_counter_ns()
            for index in range(measured_count_target):
                await sender.emit(
                    IPCPingEvent(
                        label=f'{measured_prefix}{index}',
                    )
                )
            await asyncio.wait_for(measured_seen.wait(), timeout=600.0)
            elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
            return elapsed_ms / measured_count_target
        except TimeoutError as exc:
            last_error = exc
        finally:
            await sender.close()
            await receiver.close()

        await asyncio.sleep(0.2)

    raise RuntimeError(f'bridge latency measurement timed out after {attempts} attempts: {kind}') from last_error


async def _assert_roundtrip(kind: str, config: dict[str, Any]) -> None:
    temp_path = _make_temp_dir(f'abxbus-bridge-{kind}')
    try:
        worker_config_path = temp_path / 'worker_config.json'
        worker_ready_path = temp_path / 'worker_ready'
        received_event_path = temp_path / 'received_event.json'
        worker_config = {
            **config,
            'kind': kind,
            'ready_path': str(worker_ready_path),
            'output_path': str(received_event_path),
        }
        worker_config_path.write_text(json.dumps(worker_config), encoding='utf-8')

        sender = _make_sender_bridge(kind, config)

        worker = subprocess.Popen(
            [sys.executable, str(Path(__file__).with_name('bridge_listener_worker.py')), str(worker_config_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            await _wait_for_path(worker_ready_path, process=worker)
            if kind == 'postgres':
                await sender.start()
            outbound = IPCPingEvent(
                label=f'{kind}_ok',
                event_result_type={
                    '$schema': 'https://json-schema.org/draft/2020-12/schema',
                    'type': 'object',
                    'properties': {
                        'ok': {'type': 'boolean'},
                        'score': {'type': 'number'},
                        'tags': {'type': 'array', 'items': {'type': 'string'}},
                    },
                    'required': ['ok', 'score', 'tags'],
                    'additionalProperties': False,
                },
            )
            await sender.emit(outbound)
            await _wait_for_path(received_event_path, process=worker)
            received_payload = json.loads(received_event_path.read_text(encoding='utf-8'))
            assert 'event_status' in received_payload
            assert 'event_started_at' in received_payload
            assert _normalize_roundtrip_payload(received_payload) == _normalize_roundtrip_payload(
                outbound.model_dump(mode='json')
            )
        finally:
            await sender.close()
            if worker.poll() is None:
                worker.terminate()
                try:
                    worker.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    worker.kill()
                    worker.wait(timeout=5)
    finally:
        rmtree(temp_path, ignore_errors=True)


@pytest.mark.asyncio
async def test_http_event_bridge_roundtrip_between_processes() -> None:
    endpoint = f'http://127.0.0.1:{_free_tcp_port()}/events'
    await _assert_roundtrip('http', {'endpoint': endpoint})
    latency_ms = await _measure_warm_latency_ms('http', {'endpoint': endpoint})
    print(f'LATENCY python http {latency_ms:.3f}ms')


@pytest.mark.asyncio
async def test_socket_event_bridge_roundtrip_between_processes() -> None:
    socket_path = Path('/tmp') / f'bb-{_TEST_RUN_ID}-{uuid7str()[-8:]}.sock'
    await _assert_roundtrip('socket', {'path': str(socket_path)})
    latency_ms = await _measure_warm_latency_ms('socket', {'path': str(socket_path)})
    print(f'LATENCY python socket {latency_ms:.3f}ms')


def test_socket_event_bridge_rejects_long_socket_paths() -> None:
    long_path = '/tmp/' + ('a' * 100) + '.sock'
    with pytest.raises(ValueError, match='too long'):
        SocketEventBridge(path=long_path)


@pytest.mark.asyncio
async def test_jsonl_event_bridge_roundtrip_between_processes() -> None:
    temp_dir = _make_temp_dir('abxbus-jsonl')
    try:
        jsonl_path = temp_dir / 'events.jsonl'
        await _assert_roundtrip('jsonl', {'path': str(jsonl_path)})
        latency_ms = await _measure_warm_latency_ms('jsonl', {'path': str(jsonl_path)})
        print(f'LATENCY python jsonl {latency_ms:.3f}ms')
    finally:
        rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_sqlite_event_bridge_roundtrip_between_processes() -> None:
    temp_dir = _make_temp_dir('abxbus-sqlite')
    try:
        sqlite_path = temp_dir / 'events.sqlite3'
        await _assert_roundtrip('sqlite', {'path': str(sqlite_path), 'table': 'abxbus_events'})

        with sqlite3.connect(sqlite_path) as conn:
            columns = {str(row[1]) for row in conn.execute('PRAGMA table_info("abxbus_events")').fetchall()}
            assert 'event_payload' in columns
            assert 'label' not in columns
            assert all(column == 'event_payload' or column.startswith('event_') for column in columns)

            row = conn.execute(
                'SELECT event_payload FROM "abxbus_events" ORDER BY COALESCE("event_created_at", \'\') DESC LIMIT 1'
            ).fetchone()
            assert row is not None
            payload = json.loads(str(row[0]))
            assert payload.get('label') == 'sqlite_ok'

        measure_sqlite_path = temp_dir / 'events.measure.sqlite3'
        latency_ms = await _measure_warm_latency_ms('sqlite', {'path': str(measure_sqlite_path), 'table': 'abxbus_events'})
        print(f'LATENCY python sqlite {latency_ms:.3f}ms')
    finally:
        rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_redis_event_bridge_roundtrip_between_processes() -> None:
    temp_dir = _make_temp_dir('abxbus-redis')
    try:
        port = _free_tcp_port()
        command = [
            'redis-server',
            '--save',
            '',
            '--appendonly',
            'no',
            '--bind',
            '127.0.0.1',
            '--port',
            str(port),
            '--dir',
            str(temp_dir),
        ]
        async with _running_process(command) as redis_process:
            await _wait_for_port(port)
            await _assert_roundtrip('redis', {'url': f'redis://127.0.0.1:{port}/1/abxbus_events'})
            latency_ms = await _measure_warm_latency_ms('redis', {'url': f'redis://127.0.0.1:{port}/1/abxbus_events'})
            print(f'LATENCY python redis {latency_ms:.3f}ms')
            assert redis_process.poll() is None
    finally:
        rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_nats_event_bridge_roundtrip_between_processes() -> None:
    port = _free_tcp_port()
    command = ['nats-server', '-a', '127.0.0.1', '-p', str(port)]
    async with _running_process(command) as nats_process:
        await _wait_for_port(port)
        await _assert_roundtrip('nats', {'server': f'nats://127.0.0.1:{port}', 'subject': 'abxbus_events'})
        latency_ms = await _measure_warm_latency_ms('nats', {'server': f'nats://127.0.0.1:{port}', 'subject': 'abxbus_events'})
        print(f'LATENCY python nats {latency_ms:.3f}ms')
        assert nats_process.poll() is None


@pytest.mark.asyncio
async def test_tachyon_event_bridge_roundtrip_between_processes() -> None:
    socket_path = Path('/tmp') / f'bb-tachyon-{_TEST_RUN_ID}-{uuid7str()[-8:]}.sock'
    try:
        await _assert_roundtrip('tachyon', {'path': str(socket_path)})
        latency_ms = await _measure_warm_latency_ms('tachyon', {'path': str(socket_path)})
        print(f'LATENCY python tachyon {latency_ms:.3f}ms')
    finally:
        if socket_path.exists():
            try:
                socket_path.unlink()
            except OSError:
                pass


@pytest.mark.asyncio
async def test_postgres_event_bridge_roundtrip_between_processes() -> None:
    temp_dir = _make_temp_dir('abxbus-postgres')
    try:
        data_dir = temp_dir / 'pgdata'
        initdb = subprocess.run(
            ['initdb', '-D', str(data_dir), '-A', 'trust', '-U', 'postgres'],
            capture_output=True,
            text=True,
            check=False,
        )
        assert initdb.returncode == 0, f'initdb failed\nstdout:\n{initdb.stdout}\nstderr:\n{initdb.stderr}'

        port = _free_tcp_port()
        command = ['postgres', '-D', str(data_dir), '-h', '127.0.0.1', '-p', str(port), '-k', '/tmp']
        async with _running_process(command) as postgres_process:
            await _wait_for_port(port)
            await _assert_roundtrip('postgres', {'url': f'postgresql://postgres@127.0.0.1:{port}/postgres/abxbus_events'})

            asyncpg = __import__('asyncpg')
            conn = await asyncpg.connect(f'postgresql://postgres@127.0.0.1:{port}/postgres')
            try:
                rows = await conn.fetch(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = $1
                    """,
                    'abxbus_events',
                )
                columns = {str(row['column_name']) for row in rows}
                assert 'event_payload' in columns
                assert 'label' not in columns
                assert all(column == 'event_payload' or column.startswith('event_') for column in columns)

                row = await conn.fetchrow(
                    'SELECT event_payload FROM "abxbus_events" ORDER BY COALESCE("event_created_at", \'\') DESC LIMIT 1'
                )
                assert row is not None
                payload = json.loads(str(row['event_payload']))
                assert payload.get('label') == 'postgres_ok'
            finally:
                await conn.close()

            latency_ms = await _measure_warm_latency_ms(
                'postgres', {'url': f'postgresql://postgres@127.0.0.1:{port}/postgres/abxbus_events'}
            )
            print(f'LATENCY python postgres {latency_ms:.3f}ms')
            assert postgres_process.poll() is None
    finally:
        rmtree(temp_dir, ignore_errors=True)
