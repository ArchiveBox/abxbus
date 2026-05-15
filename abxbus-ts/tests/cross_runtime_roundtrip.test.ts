import assert from 'node:assert/strict'
import { spawn, spawnSync, type ChildProcess } from 'node:child_process'
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { createConnection, createServer as createNetServer } from 'node:net'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { test } from 'node:test'
import { z } from 'zod'

import { NATSEventBridge, PostgresEventBridge, RedisEventBridge, TachyonEventBridge } from '../src/bridges.js'
import { BaseEvent, EventBridge, EventBus, HTTPEventBridge, JSONLEventBridge, SQLiteEventBridge, SocketEventBridge } from '../src/index.js'
import { fromJsonSchema } from '../src/types.js'

const tests_dir = dirname(fileURLToPath(import.meta.url))
const ts_root = resolve(tests_dir, '..')
const repo_root = resolve(ts_root, '..')
const PROCESS_TIMEOUT_MS = 30_000
const PYTHON_BUS_PROCESS_TIMEOUT_MS = 120_000
const RUST_PROCESS_TIMEOUT_MS = 120_000
const GO_PROCESS_TIMEOUT_MS = 120_000
const EVENT_WAIT_TIMEOUT_MS = 15_000

const jsonSafe = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T

const jsonShape = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(jsonShape)
  if (value !== null && typeof value === 'object') {
    const record = value as Record<string, unknown>
    if (typeof record.$schema === 'string' && typeof record.type === 'string') {
      return { $schema: 'string', type: 'string' }
    }
    return Object.fromEntries(Object.entries(value).map(([key, entry]) => [key, jsonShape(entry)]))
  }
  if (typeof value === 'number') return 'number'
  return value === null ? 'null' : typeof value
}

const assertJsonShapeEqual = (actual: unknown, expected: unknown, context: string): void => {
  const containsShape = (actual_shape: unknown, expected_shape: unknown): boolean => {
    if (
      actual_shape !== null &&
      expected_shape !== null &&
      typeof actual_shape === 'object' &&
      typeof expected_shape === 'object' &&
      !Array.isArray(actual_shape) &&
      !Array.isArray(expected_shape)
    ) {
      const actual_record = actual_shape as Record<string, unknown>
      return Object.entries(expected_shape as Record<string, unknown>).every(
        ([key, value]) => key in actual_record && containsShape(actual_record[key], value)
      )
    }
    if (Array.isArray(actual_shape) && Array.isArray(expected_shape)) {
      return (
        actual_shape.length === expected_shape.length && actual_shape.every((value, index) => containsShape(value, expected_shape[index]))
      )
    }
    return actual_shape === expected_shape
  }
  assert.equal(containsShape(jsonShape(actual), jsonShape(expected)), true, `${context}: JSON shape changed`)
}

type ResultSemanticsCase = {
  event: BaseEvent
  valid_results: unknown[]
  invalid_results: unknown[]
}

const assertFieldEqual = (key: string, actual: unknown, expected: unknown, context: string): void => {
  if (key.endsWith('_at') && typeof actual === 'string' && typeof expected === 'string') {
    assert.equal(actual, expected, `${context}: ${key}`)
    return
  }
  assert.deepEqual(actual, expected, `${context}: ${key}`)
}

const stableValue = (value: unknown): string => {
  if (value === undefined) {
    return 'undefined'
  }
  try {
    return JSON.stringify(value)
  } catch {
    return String(value)
  }
}

const assertSchemaSemanticsEqual = (
  original_schema_json: unknown,
  candidate_schema_json: unknown,
  valid_results: unknown[],
  invalid_results: unknown[],
  context: string
): void => {
  const original_schema = fromJsonSchema(original_schema_json)
  const candidate_schema = fromJsonSchema(candidate_schema_json)

  for (const result of valid_results) {
    const original_ok = original_schema.safeParse(result).success
    const candidate_ok = candidate_schema.safeParse(result).success
    assert.equal(original_ok, true, `${context}: original schema should accept ${stableValue(result)}`)
    assert.equal(candidate_ok, true, `${context}: candidate schema should accept ${stableValue(result)}`)
  }

  for (const result of invalid_results) {
    const original_ok = original_schema.safeParse(result).success
    const candidate_ok = candidate_schema.safeParse(result).success
    assert.equal(original_ok, false, `${context}: original schema should reject ${stableValue(result)}`)
    assert.equal(candidate_ok, false, `${context}: candidate schema should reject ${stableValue(result)}`)
  }

  for (const result of [...valid_results, ...invalid_results]) {
    const original_ok = original_schema.safeParse(result).success
    const candidate_ok = candidate_schema.safeParse(result).success
    assert.equal(
      candidate_ok,
      original_ok,
      `${context}: schema decision mismatch for ${stableValue(result)} (expected ${original_ok}, got ${candidate_ok})`
    )
  }
}

const buildRoundtripCases = (): ResultSemanticsCase[] => {
  const NumberResultEvent = BaseEvent.extend('TsPy_NumberResultEvent', {
    value: z.number(),
    label: z.string(),
    event_result_type: z.number(),
  })
  const StringResultEvent = BaseEvent.extend('TsPy_StringResultEvent', {
    id: z.string(),
    event_result_type: z.string(),
  })
  const BooleanResultEvent = BaseEvent.extend('TsPy_BooleanResultEvent', {
    id: z.string(),
    event_result_type: z.boolean(),
  })
  const NullResultEvent = BaseEvent.extend('TsPy_NullResultEvent', {
    id: z.string(),
    event_result_type: z.null(),
  })
  const StringCtorResultEvent = BaseEvent.extend('TsPy_StringCtorResultEvent', {
    id: z.string(),
    event_result_type: String,
  })
  const NumberCtorResultEvent = BaseEvent.extend('TsPy_NumberCtorResultEvent', {
    id: z.string(),
    event_result_type: Number,
  })
  const BooleanCtorResultEvent = BaseEvent.extend('TsPy_BooleanCtorResultEvent', {
    id: z.string(),
    event_result_type: Boolean,
  })
  const ArrayResultEvent = BaseEvent.extend('TsPy_ArrayResultEvent', {
    id: z.string(),
    event_result_type: z.array(z.string()),
  })
  const ArrayCtorResultEvent = BaseEvent.extend('TsPy_ArrayCtorResultEvent', {
    id: z.string(),
    event_result_type: Array,
  })
  const RecordResultEvent = BaseEvent.extend('TsPy_RecordResultEvent', {
    id: z.string(),
    event_result_type: z.record(z.string(), z.array(z.number())),
  })
  const ObjectCtorResultEvent = BaseEvent.extend('TsPy_ObjectCtorResultEvent', {
    id: z.string(),
    event_result_type: Object,
  })
  const ScreenshotResultEvent = BaseEvent.extend('TsPy_ScreenshotResultEvent', {
    target_id: z.string(),
    quality: z.string(),
    event_result_type: z.object({
      image_url: z.string(),
      width: z.number(),
      height: z.number(),
      tags: z.array(z.string()),
      is_animated: z.boolean(),
      confidence_scores: z.array(z.number()),
      metadata: z.record(z.string(), z.number()),
      regions: z.array(
        z.object({
          id: z.string(),
          label: z.string(),
          score: z.number(),
          visible: z.boolean(),
        })
      ),
    }),
  })

  const number_event = NumberResultEvent({
    value: 7,
    label: 'parent',
    event_path: ['TsBus#aaaa'],
    event_timeout: 12.5,
  })

  const screenshot_event = ScreenshotResultEvent({
    target_id: '0c1ccf21-65c0-7390-8b64-9182e985740e',
    quality: 'high',
    event_parent_id: number_event.event_id,
    event_path: ['TsBus#aaaa', 'PyBridge#bbbb'],
    event_timeout: 33.0,
  })

  const string_event = StringResultEvent({
    id: 'ecea6334-c939-7540-89b9-29b439c9a1f4',
    event_parent_id: number_event.event_id,
    event_path: ['TsBus#aaaa'],
  })
  const bool_event = BooleanResultEvent({
    id: '87dc4d01-be2d-7057-834e-5faf35705400',
    event_path: ['TsBus#aaaa'],
  })
  const null_event = NullResultEvent({
    id: '5fc19a35-064c-7ec1-8d1a-4fb33f119abc',
    event_path: ['TsBus#aaaa'],
  })
  const string_ctor_event = StringCtorResultEvent({
    id: 'df54dc78-e988-75bc-8457-87d5bd2d7c4c',
    event_path: ['TsBus#aaaa'],
  })
  const number_ctor_event = NumberCtorResultEvent({
    id: 'bfe9459c-c1a4-7906-8a13-c9855aac0001',
    event_path: ['TsBus#aaaa'],
  })
  const boolean_ctor_event = BooleanCtorResultEvent({
    id: 'f472d2e0-5815-7dad-8fb1-a9ce4315cd6e',
    event_path: ['TsBus#aaaa'],
  })
  const array_event = ArrayResultEvent({
    id: 'e35d91b5-1ca9-7833-8b3d-1516e2896f1e',
    event_path: ['TsBus#aaaa'],
  })
  const array_ctor_event = ArrayCtorResultEvent({
    id: 'f21399dd-6162-7ac2-832d-a3870373278a',
    event_path: ['TsBus#aaaa'],
  })
  const record_event = RecordResultEvent({
    id: 'ba1a8735-0955-737f-8b4d-7337d2169a3c',
    event_path: ['TsBus#aaaa'],
  })
  const object_ctor_event = ObjectCtorResultEvent({
    id: '2aa37066-45e8-7f65-8ada-7c30ac8982d5',
    event_path: ['TsBus#aaaa'],
  })

  return [
    {
      event: number_event,
      valid_results: [0, -1, 1.5],
      invalid_results: ['1', true, { value: 1 }],
    },
    {
      event: string_event,
      valid_results: ['ok', ''],
      invalid_results: [123, false, ['x']],
    },
    {
      event: bool_event,
      valid_results: [true, false],
      invalid_results: ['false', 0, {}],
    },
    {
      event: null_event,
      valid_results: [null],
      invalid_results: [0, false, 'not-null', {}, []],
    },
    {
      event: string_ctor_event,
      valid_results: ['ok', ''],
      invalid_results: [123, false, ['x']],
    },
    {
      event: number_ctor_event,
      valid_results: [3.14, 42],
      invalid_results: ['42', false, {}],
    },
    {
      event: boolean_ctor_event,
      valid_results: [true, false],
      invalid_results: ['true', 1, []],
    },
    {
      event: array_event,
      valid_results: [['a', 'b'], []],
      invalid_results: [['a', 1], {}, 'not-array'],
    },
    {
      event: array_ctor_event,
      valid_results: [[1, 'two', false], []],
      invalid_results: ['not-array', { 0: 'x' }, true],
    },
    {
      event: record_event,
      valid_results: [{ a: [1, 2], b: [] }, {}],
      invalid_results: [{ a: ['1'] }, ['not-object'], 12],
    },
    {
      event: object_ctor_event,
      valid_results: [{ any: 'shape', count: 2 }, {}],
      invalid_results: ['not-object', [1, 2], true],
    },
    {
      event: screenshot_event,
      valid_results: [
        {
          image_url: 'https://img.local/1.png',
          width: 1920,
          height: 1080,
          tags: ['hero', 'dashboard'],
          is_animated: false,
          confidence_scores: [0.95, 0.89],
          metadata: { score: 0.99, variance: 0.01 },
          regions: [
            { id: '98f51f1d-b10a-7cd9-8ee6-cb706153f717', label: 'face', score: 0.9, visible: true },
            { id: '5f234e9d-29e9-7921-8cf2-2a65f6ba3bdd', label: 'button', score: 0.7, visible: false },
          ],
        },
      ],
      invalid_results: [
        {
          image_url: 123,
          width: '1920',
          height: 1080,
          tags: ['hero'],
          is_animated: false,
          confidence_scores: [0.95],
          metadata: { score: 0.99 },
          regions: [{ id: '98f51f1d-b10a-7cd9-8ee6-cb706153f717', label: 'face', score: 0.9, visible: true }],
        },
        {
          image_url: 'https://img.local/1.png',
          width: 1920,
          height: 1080,
          tags: ['hero'],
          is_animated: false,
          confidence_scores: [0.95],
          metadata: { score: 0.99 },
          regions: [{ id: 123, label: 'face', score: 0.9, visible: true }],
        },
      ],
    },
  ]
}

const runCommand = (cmd: string, args: string[], cwd = repo_root, timeout_ms = PROCESS_TIMEOUT_MS): ReturnType<typeof spawnSync> =>
  spawnSync(cmd, args, {
    cwd,
    env: process.env,
    encoding: 'utf8',
    timeout: timeout_ms,
    maxBuffer: 10 * 1024 * 1024,
  })

const assertProcessSucceeded = (proc: ReturnType<typeof spawnSync>, label: string): void => {
  if (proc.error) {
    throw new Error(`${label} failed: ${proc.error.message}\nstdout:\n${proc.stdout ?? ''}\nstderr:\n${proc.stderr ?? ''}`)
  }
  if (proc.signal) {
    throw new Error(`${label} terminated by signal ${proc.signal}\nstdout:\n${proc.stdout ?? ''}\nstderr:\n${proc.stderr ?? ''}`)
  }
  assert.equal(proc.status, 0, `${label} failed:\nstdout:\n${proc.stdout ?? ''}\nstderr:\n${proc.stderr ?? ''}`)
}

const runWithTimeout = async <T>(promise: Promise<T>, timeout_ms: number, label: string): Promise<T> =>
  new Promise<T>((resolve, reject) => {
    const timeout_id = setTimeout(() => {
      reject(new Error(`${label} timed out after ${timeout_ms}ms`))
    }, timeout_ms)
    promise.then(
      (value) => {
        clearTimeout(timeout_id)
        resolve(value)
      },
      (error) => {
        clearTimeout(timeout_id)
        reject(error)
      }
    )
  })

type PythonRunner = {
  command: string
  args_prefix: string[]
  label: string
}

const resolvePython = (): PythonRunner | null => {
  if (process.env.ABXBUS_PYTHON_BIN) {
    const probe = runCommand(process.env.ABXBUS_PYTHON_BIN, ['--version'])
    if (probe.status === 0) {
      return { command: process.env.ABXBUS_PYTHON_BIN, args_prefix: [], label: process.env.ABXBUS_PYTHON_BIN }
    }
  }

  const uv_probe = runCommand('uv', ['--version'])
  if (uv_probe.status === 0) {
    const uv_python_probe = runCommand('uv', ['run', 'python', '--version'])
    if (uv_python_probe.status === 0) {
      return { command: 'uv', args_prefix: ['run', 'python'], label: 'uv run python' }
    }
  }

  const candidates = [
    resolve(repo_root, '.venv', 'bin', 'python'),
    resolve(repo_root, '.venv', 'Scripts', 'python.exe'),
    'python3',
    'python',
  ].filter((candidate): candidate is string => typeof candidate === 'string' && candidate.length > 0)

  for (const candidate of candidates) {
    if ((candidate.includes('/') || candidate.includes('\\')) && !existsSync(candidate)) {
      continue
    }
    const probe = runCommand(candidate, ['--version'])
    if (probe.status === 0) {
      return { command: candidate, args_prefix: [], label: candidate }
    }
  }

  return null
}

const runPythonCommand = (
  python_runner: PythonRunner,
  args: string[],
  extra_env: Record<string, string> = {},
  timeout_ms = PROCESS_TIMEOUT_MS
): ReturnType<typeof spawnSync> =>
  spawnSync(python_runner.command, [...python_runner.args_prefix, ...args], {
    cwd: repo_root,
    env: {
      ...process.env,
      ...extra_env,
    },
    encoding: 'utf8',
    timeout: timeout_ms,
    maxBuffer: 10 * 1024 * 1024,
  })

const assertPythonCanImportAbxBus = (python_runner: PythonRunner): void => {
  const probe = runPythonCommand(python_runner, ['-c', 'import pydantic; import abxbus'])
  if (probe.status !== 0) {
    throw new Error(
      `python environment (${python_runner.label}) cannot import abxbus/pydantic:\nstdout:\n${probe.stdout ?? ''}\nstderr:\n${probe.stderr ?? ''}`
    )
  }
}

const runPythonRoundtrip = (python_runner: PythonRunner, payload: Array<Record<string, unknown>>): Array<Record<string, unknown>> => {
  const temp_dir = mkdtempSync(join(tmpdir(), 'abxbus-ts-to-python-'))
  const input_path = join(temp_dir, 'ts_events.json')
  const output_path = join(temp_dir, 'python_events.json')

  const python_script = `
import json
import os
from typing import Any
from abxbus import BaseEvent

input_path = os.environ.get('ABXBUS_TS_PY_INPUT_PATH')
output_path = os.environ.get('ABXBUS_TS_PY_OUTPUT_PATH')
if not input_path or not output_path:
    raise RuntimeError('missing ABXBUS_TS_PY_INPUT_PATH or ABXBUS_TS_PY_OUTPUT_PATH')

with open(input_path, 'r', encoding='utf-8') as f:
    raw = json.load(f)

if not isinstance(raw, list):
    raise TypeError('expected array payload')

roundtripped: list[dict[str, Any]] = []
for item in raw:
    event = BaseEvent[Any].model_validate(item)
    event_dump = event.model_dump(mode='json')
    if event.event_results:
        event_dump['event_results'] = {
            handler_id: event_result.model_dump(mode='json')
            for handler_id, event_result in event.event_results.items()
        }
    roundtripped.append(event_dump)

with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(roundtripped, f, indent=2)
`

  try {
    writeFileSync(input_path, JSON.stringify(payload, null, 2), 'utf8')
    const proc = runPythonCommand(python_runner, ['-c', python_script], {
      ABXBUS_TS_PY_INPUT_PATH: input_path,
      ABXBUS_TS_PY_OUTPUT_PATH: output_path,
    })

    assertProcessSucceeded(proc, 'python roundtrip')
    assert.ok(existsSync(output_path), 'python roundtrip did not produce output payload')

    return JSON.parse(readFileSync(output_path, 'utf8')) as Array<Record<string, unknown>>
  } finally {
    rmSync(temp_dir, { recursive: true, force: true })
  }
}

const runPythonBusRoundtrip = (python_runner: PythonRunner, payload: Record<string, unknown>): Record<string, unknown> => {
  const temp_dir = mkdtempSync(join(tmpdir(), 'abxbus-ts-bus-to-python-'))
  const input_path = join(temp_dir, 'ts_bus.json')
  const output_path = join(temp_dir, 'python_bus.json')

  const python_script = `
import json
import os
from abxbus import EventBus

input_path = os.environ.get('ABXBUS_TS_PY_BUS_INPUT_PATH')
output_path = os.environ.get('ABXBUS_TS_PY_BUS_OUTPUT_PATH')
if not input_path or not output_path:
    raise RuntimeError('missing ABXBUS_TS_PY_BUS_INPUT_PATH or ABXBUS_TS_PY_BUS_OUTPUT_PATH')

with open(input_path, 'r', encoding='utf-8') as f:
    raw = json.load(f)

if not isinstance(raw, dict):
    raise TypeError('expected object payload')

bus = EventBus.validate(raw)
roundtripped = bus.model_dump()

with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(roundtripped, f, indent=2)
`

  try {
    writeFileSync(input_path, JSON.stringify(payload, null, 2), 'utf8')
    const proc = runPythonCommand(
      python_runner,
      ['-c', python_script],
      {
        ABXBUS_TS_PY_BUS_INPUT_PATH: input_path,
        ABXBUS_TS_PY_BUS_OUTPUT_PATH: output_path,
      },
      PYTHON_BUS_PROCESS_TIMEOUT_MS
    )

    assertProcessSucceeded(proc, 'python bus roundtrip')
    assert.ok(existsSync(output_path), 'python bus roundtrip did not produce output payload')
    return JSON.parse(readFileSync(output_path, 'utf8')) as Record<string, unknown>
  } finally {
    rmSync(temp_dir, { recursive: true, force: true })
  }
}

const runRustRoundtrip = <T extends Array<Record<string, unknown>> | Record<string, unknown>>(mode: 'events' | 'bus', payload: T): T => {
  const cargo_probe = runCommand('cargo', ['--version'])
  assertProcessSucceeded(cargo_probe, 'cargo probe')

  const temp_dir = mkdtempSync(join(tmpdir(), `abxbus-ts-${mode}-to-rust-`))
  const input_path = join(temp_dir, `ts_${mode}.json`)
  const output_path = join(temp_dir, `rust_${mode}.json`)
  const rust_manifest = join(repo_root, 'abxbus-rust', 'Cargo.toml')

  try {
    writeFileSync(input_path, JSON.stringify(payload, null, 2), 'utf8')
    const proc = runCommand(
      'cargo',
      ['run', '--quiet', '--manifest-path', rust_manifest, '--bin', 'abxbus-rust-roundtrip', '--', mode, input_path, output_path],
      repo_root,
      RUST_PROCESS_TIMEOUT_MS
    )

    assertProcessSucceeded(proc, `rust ${mode} roundtrip`)
    assert.ok(existsSync(output_path), `rust ${mode} roundtrip did not produce output payload`)

    return JSON.parse(readFileSync(output_path, 'utf8')) as T
  } finally {
    rmSync(temp_dir, { recursive: true, force: true })
  }
}

const runGoRoundtrip = <T extends Array<Record<string, unknown>> | Record<string, unknown>>(mode: 'events' | 'bus', payload: T): T => {
  const go_probe = runCommand('go', ['version'])
  assertProcessSucceeded(go_probe, 'go probe')

  const temp_dir = mkdtempSync(join(tmpdir(), `abxbus-ts-${mode}-to-go-`))
  const input_path = join(temp_dir, `ts_${mode}.json`)
  const output_path = join(temp_dir, `go_${mode}.json`)
  const go_root = join(repo_root, 'abxbus-go')

  try {
    writeFileSync(input_path, JSON.stringify(payload, null, 2), 'utf8')
    const proc = runCommand('go', ['run', './tests/roundtrip_cli', mode, input_path, output_path], go_root, GO_PROCESS_TIMEOUT_MS)

    assertProcessSucceeded(proc, `go ${mode} roundtrip`)
    assert.ok(existsSync(output_path), `go ${mode} roundtrip did not produce output payload`)

    return JSON.parse(readFileSync(output_path, 'utf8')) as T
  } finally {
    rmSync(temp_dir, { recursive: true, force: true })
  }
}

const assertTsSchemaEnforcementAfterRuntimeReload = async (
  runtime_roundtripped: Array<Record<string, unknown>>,
  wrong_bus_name: string,
  right_bus_name: string
): Promise<void> => {
  const screenshot_payload = runtime_roundtripped.find((event) => event.event_type === 'TsPy_ScreenshotResultEvent')
  assert.ok(screenshot_payload, 'missing TsPy_ScreenshotResultEvent in roundtrip payload')
  assert.equal(typeof screenshot_payload.event_result_type, 'object')

  const wrong_bus = new EventBus(wrong_bus_name)
  wrong_bus.on('TsPy_ScreenshotResultEvent', () => ({
    image_url: 123,
    width: '1920',
    height: 1080,
    tags: ['hero', 'dashboard'],
    is_animated: 'false',
    confidence_scores: [0.95, 0.89],
    metadata: { score: 0.99 },
    regions: [{ id: '98f51f1d-b10a-7cd9-8ee6-cb706153f717', label: 'face', score: 0.9, visible: true }],
  }))
  const wrong_event = BaseEvent.fromJSON(screenshot_payload)
  assert.equal(typeof (wrong_event.event_result_type as { safeParse?: unknown } | undefined)?.safeParse, 'function')
  const wrong_dispatched = wrong_bus.emit(wrong_event)
  await runWithTimeout(wrong_dispatched.now(), EVENT_WAIT_TIMEOUT_MS, 'wrong-shape event completion')
  const wrong_result = Array.from(wrong_dispatched.event_results.values())[0]
  assert.equal(wrong_result.status, 'error')
  assert.equal((wrong_result.error as { name?: string } | undefined)?.name, 'EventHandlerResultSchemaError')
  wrong_bus.destroy()

  const right_bus = new EventBus(right_bus_name)
  right_bus.on('TsPy_ScreenshotResultEvent', () => ({
    image_url: 'https://img.local/1.png',
    width: 1920,
    height: 1080,
    tags: ['hero', 'dashboard'],
    is_animated: false,
    confidence_scores: [0.95, 0.89],
    metadata: { score: 0.99, variance: 0.01 },
    regions: [
      { id: '98f51f1d-b10a-7cd9-8ee6-cb706153f717', label: 'face', score: 0.9, visible: true },
      { id: '5f234e9d-29e9-7921-8cf2-2a65f6ba3bdd', label: 'button', score: 0.7, visible: false },
    ],
  }))
  const right_event = BaseEvent.fromJSON(screenshot_payload)
  assert.equal(typeof (right_event.event_result_type as { safeParse?: unknown } | undefined)?.safeParse, 'function')
  const right_dispatched = right_bus.emit(right_event)
  await runWithTimeout(right_dispatched.now(), EVENT_WAIT_TIMEOUT_MS, 'right-shape event completion')
  const right_result = Array.from(right_dispatched.event_results.values())[0]
  assert.equal(right_result.status, 'completed')
  assert.deepEqual(right_result.result, {
    image_url: 'https://img.local/1.png',
    width: 1920,
    height: 1080,
    tags: ['hero', 'dashboard'],
    is_animated: false,
    confidence_scores: [0.95, 0.89],
    metadata: { score: 0.99, variance: 0.01 },
    regions: [
      { id: '98f51f1d-b10a-7cd9-8ee6-cb706153f717', label: 'face', score: 0.9, visible: true },
      { id: '5f234e9d-29e9-7921-8cf2-2a65f6ba3bdd', label: 'button', score: 0.7, visible: false },
    ],
  })
  right_bus.destroy()
}

test('ts_to_python_roundtrip preserves event fields and result type semantics', async () => {
  const python_runner = resolvePython()
  assert.ok(python_runner, 'python is required for ts<->python roundtrip tests')
  assertPythonCanImportAbxBus(python_runner)

  const roundtrip_cases = buildRoundtripCases()
  const events = roundtrip_cases.map((entry) => entry.event)
  const roundtrip_cases_by_type = new Map(roundtrip_cases.map((entry) => [entry.event.event_type, entry]))
  const ts_dumped = events.map((event) => jsonSafe(event.toJSON()))

  for (const event_dump of ts_dumped) {
    assert.ok('event_result_type' in event_dump)
    assert.equal(typeof event_dump.event_result_type, 'object')
  }

  const python_roundtripped = runPythonRoundtrip(python_runner, ts_dumped)
  assert.equal(python_roundtripped.length, ts_dumped.length)

  for (let i = 0; i < ts_dumped.length; i += 1) {
    const original = ts_dumped[i]
    const python_event = python_roundtripped[i]

    const event_type = String(original.event_type)
    const semantics_case = roundtrip_cases_by_type.get(event_type)
    assert.ok(semantics_case, `missing semantics case for event_type=${event_type}`)

    assertJsonShapeEqual(python_event, original, `python roundtrip ${event_type}`)

    for (const [key, value] of Object.entries(original)) {
      assert.ok(key in python_event, `missing key after python roundtrip: ${key}`)
      if (key === 'event_result_type') {
        assert.equal(typeof python_event[key], 'object')
        assertSchemaSemanticsEqual(
          value,
          python_event[key],
          semantics_case.valid_results,
          semantics_case.invalid_results,
          `python roundtrip ${event_type}`
        )
        continue
      }
      assertFieldEqual(key, python_event[key], value, 'field changed after python roundtrip')
    }

    const restored = BaseEvent.fromJSON(python_event)
    const restored_dump = jsonSafe(restored.toJSON())

    assertJsonShapeEqual(restored_dump, original, `ts reload ${event_type}`)

    for (const [key, value] of Object.entries(original)) {
      assert.ok(key in restored_dump, `missing key after ts reload: ${key}`)
      if (key === 'event_result_type') {
        assert.equal(typeof restored_dump[key], 'object')
        assertSchemaSemanticsEqual(
          value,
          restored_dump[key],
          semantics_case.valid_results,
          semantics_case.invalid_results,
          `ts reload ${event_type}`
        )
        continue
      }
      assertFieldEqual(key, restored_dump[key], value, 'field changed after ts reload')
    }
  }

  await assertTsSchemaEnforcementAfterRuntimeReload(python_roundtripped, 'TsPyTsWrongShape', 'TsPyTsRightShape')
})

test('ts_to_rust_roundtrip preserves event fields and result type semantics', async () => {
  const roundtrip_cases = buildRoundtripCases()
  const events = roundtrip_cases.map((entry) => entry.event)
  const roundtrip_cases_by_type = new Map(roundtrip_cases.map((entry) => [entry.event.event_type, entry]))
  const ts_dumped = events.map((event) => jsonSafe(event.toJSON()))

  for (const event_dump of ts_dumped) {
    assert.ok('event_result_type' in event_dump)
    assert.equal(typeof event_dump.event_result_type, 'object')
  }

  const rust_roundtripped = runRustRoundtrip('events', ts_dumped)
  assert.equal(rust_roundtripped.length, ts_dumped.length)

  for (let i = 0; i < ts_dumped.length; i += 1) {
    const original = ts_dumped[i]
    const rust_event = rust_roundtripped[i]

    const event_type = String(original.event_type)
    const semantics_case = roundtrip_cases_by_type.get(event_type)
    assert.ok(semantics_case, `missing semantics case for event_type=${event_type}`)

    assertJsonShapeEqual(rust_event, original, `rust roundtrip ${event_type}`)

    for (const [key, value] of Object.entries(original)) {
      assert.ok(key in rust_event, `missing key after rust roundtrip: ${key}`)
      if (key === 'event_result_type') {
        assert.equal(typeof rust_event[key], 'object')
        assertSchemaSemanticsEqual(
          value,
          rust_event[key],
          semantics_case.valid_results,
          semantics_case.invalid_results,
          `rust roundtrip ${event_type}`
        )
        continue
      }
      assertFieldEqual(key, rust_event[key], value, 'field changed after rust roundtrip')
    }

    const restored = BaseEvent.fromJSON(rust_event)
    const restored_dump = jsonSafe(restored.toJSON())

    assertJsonShapeEqual(restored_dump, original, `ts reload ${event_type}`)

    for (const [key, value] of Object.entries(original)) {
      assert.ok(key in restored_dump, `missing key after ts reload: ${key}`)
      if (key === 'event_result_type') {
        assert.equal(typeof restored_dump[key], 'object')
        assertSchemaSemanticsEqual(
          value,
          restored_dump[key],
          semantics_case.valid_results,
          semantics_case.invalid_results,
          `ts reload ${event_type}`
        )
        continue
      }
      assertFieldEqual(key, restored_dump[key], value, 'field changed after ts reload')
    }
  }

  await assertTsSchemaEnforcementAfterRuntimeReload(rust_roundtripped, 'TsRustTsWrongShape', 'TsRustTsRightShape')
})

test('ts_to_go_roundtrip preserves event fields and result type semantics', async () => {
  const roundtrip_cases = buildRoundtripCases()
  const events = roundtrip_cases.map((entry) => entry.event)
  const roundtrip_cases_by_type = new Map(roundtrip_cases.map((entry) => [entry.event.event_type, entry]))
  const ts_dumped = events.map((event) => jsonSafe(event.toJSON()))

  for (const event_dump of ts_dumped) {
    assert.ok('event_result_type' in event_dump)
    assert.equal(typeof event_dump.event_result_type, 'object')
  }

  const go_roundtripped = runGoRoundtrip('events', ts_dumped)
  assert.equal(go_roundtripped.length, ts_dumped.length)

  for (let i = 0; i < ts_dumped.length; i += 1) {
    const original = ts_dumped[i]
    const go_event = go_roundtripped[i]

    const event_type = String(original.event_type)
    const semantics_case = roundtrip_cases_by_type.get(event_type)
    assert.ok(semantics_case, `missing semantics case for event_type=${event_type}`)

    assertJsonShapeEqual(go_event, original, `go roundtrip ${event_type}`)

    for (const [key, value] of Object.entries(original)) {
      assert.ok(key in go_event, `missing key after go roundtrip: ${key}`)
      if (key === 'event_result_type') {
        assert.equal(typeof go_event[key], 'object')
        assert.deepEqual(go_event[key], value, `event_result_type schema changed after go roundtrip: ${event_type}`)
        assertSchemaSemanticsEqual(
          value,
          go_event[key],
          semantics_case.valid_results,
          semantics_case.invalid_results,
          `go roundtrip ${event_type}`
        )
        continue
      }
      assertFieldEqual(key, go_event[key], value, 'field changed after go roundtrip')
    }

    const restored = BaseEvent.fromJSON(go_event)
    const restored_dump = jsonSafe(restored.toJSON())

    assertJsonShapeEqual(restored_dump, original, `ts reload ${event_type}`)

    for (const [key, value] of Object.entries(original)) {
      assert.ok(key in restored_dump, `missing key after ts reload: ${key}`)
      if (key === 'event_result_type') {
        assert.equal(typeof restored_dump[key], 'object')
        assertSchemaSemanticsEqual(
          value,
          restored_dump[key],
          semantics_case.valid_results,
          semantics_case.invalid_results,
          `ts reload ${event_type}`
        )
        continue
      }
      assertFieldEqual(key, restored_dump[key], value, 'field changed after ts reload')
    }
  }

  await assertTsSchemaEnforcementAfterRuntimeReload(go_roundtripped, 'TsGoTsWrongShape', 'TsGoTsRightShape')
})

test('ts -> python -> ts bus roundtrip rehydrates and resumes pending queue', async (t) => {
  const python_runner = resolvePython()
  assert.ok(python_runner, 'python is required for ts<->python roundtrip tests')
  assertPythonCanImportAbxBus(python_runner)

  const ResumeEvent = BaseEvent.extend('TsPyBusResumeEvent', {
    label: z.string(),
    event_result_type: z.string(),
  })

  const source_bus = new EventBus('TsPyBusSource', {
    id: '018f8e40-1234-7000-8000-00000000aa11',
    event_handler_detect_file_paths: false,
    event_handler_concurrency: 'serial',
    event_handler_completion: 'all',
  })
  let restored: EventBus | null = null
  t.after(async () => {
    await restored?.destroy()
    await source_bus.destroy()
  })

  const handler_one = source_bus.on(ResumeEvent, (event) => `h1:${(event as unknown as { label: string }).label}`)
  const handler_two = source_bus.on(ResumeEvent, (event) => `h2:${(event as unknown as { label: string }).label}`)

  const event_one = ResumeEvent({ label: 'e1' })
  const event_two = ResumeEvent({ label: 'e2' })

  const seeded = event_one.eventResultUpdate(handler_one, { eventbus: source_bus, status: 'pending' })
  event_one.eventResultUpdate(handler_two, { eventbus: source_bus, status: 'pending' })
  seeded.update({ status: 'completed', result: 'seeded' })

  source_bus.event_history.set(event_one.event_id, event_one)
  source_bus.event_history.set(event_two.event_id, event_two)
  source_bus.pending_event_queue = [event_one, event_two]

  const source_dump = source_bus.toJSON()
  const py_roundtripped = runPythonBusRoundtrip(python_runner, source_dump)
  restored = EventBus.fromJSON(py_roundtripped)
  const restored_dump = restored.toJSON()

  assert.deepEqual(Object.keys(restored_dump.handlers), Object.keys(source_dump.handlers))
  for (const [handler_id, handler_payload] of Object.entries(source_dump.handlers as Record<string, Record<string, unknown>>)) {
    const restored_handler = (restored_dump.handlers as Record<string, Record<string, unknown>>)[handler_id]
    assert.ok(restored_handler, `missing handler ${handler_id}`)
    assert.equal(restored_handler.eventbus_id, handler_payload.eventbus_id)
    assert.equal(restored_handler.eventbus_name, handler_payload.eventbus_name)
    assert.equal(restored_handler.event_pattern, handler_payload.event_pattern)
  }
  assert.deepEqual(restored_dump.handlers_by_key, source_dump.handlers_by_key)
  assert.deepEqual(restored_dump.pending_event_queue, source_dump.pending_event_queue)
  assert.deepEqual(Object.keys(restored_dump.event_history), Object.keys(source_dump.event_history))

  const restored_event_one = restored.event_history.get(event_one.event_id)
  assert.ok(restored_event_one)
  const preseeded = Array.from(restored_event_one!.event_results.values()).find((result) => result.result === 'seeded')
  assert.ok(preseeded)
  assert.equal(preseeded!.status, 'completed')
  assert.equal(preseeded!.result, 'seeded')
  assert.equal(preseeded!.handler, restored.handlers.get(preseeded!.handler_id))

  const run_order: string[] = []
  const restored_handler_one = restored.handlers.get(handler_one.id)
  const restored_handler_two = restored.handlers.get(handler_two.id)
  assert.ok(restored_handler_one)
  assert.ok(restored_handler_two)
  const restored_handler_one_fn = (event: BaseEvent): string => {
    const label = Reflect.get(event, 'label')
    run_order.push(`h1:${String(label)}`)
    return `h1:${String(label)}`
  }
  const restored_handler_two_fn = (event: BaseEvent): string => {
    const label = Reflect.get(event, 'label')
    run_order.push(`h2:${String(label)}`)
    return `h2:${String(label)}`
  }
  restored_handler_one.handler = restored_handler_one_fn
  restored_handler_two.handler = restored_handler_two_fn

  const trigger = restored.emit(ResumeEvent({ label: 'e3' }))
  await runWithTimeout(trigger.wait(), EVENT_WAIT_TIMEOUT_MS, 'bus resume completion')

  const done_one = restored.event_history.get(event_one.event_id)
  const done_two = restored.event_history.get(event_two.event_id)
  const done_three = restored.event_history.get(trigger.event_id)
  assert.equal(done_three?.event_status, 'completed')
  assert.equal(restored.pending_event_queue.length, 0)
  assert.ok(Array.from(done_one?.event_results.values() ?? []).every((result) => result.status === 'completed'))
  assert.ok(Array.from(done_two?.event_results.values() ?? []).every((result) => result.status === 'completed'))
  assert.equal(done_one?.event_results.get(handler_one.id)?.result, 'seeded')
  assert.equal(done_one?.event_results.get(handler_two.id)?.result, 'h2:e1')
  assert.equal(done_two?.event_results.get(handler_one.id)?.result, 'h1:e2')
  assert.equal(done_two?.event_results.get(handler_two.id)?.result, 'h2:e2')
  assert.equal(done_three?.event_results.get(handler_one.id)?.result, 'h1:e3')
  assert.equal(done_three?.event_results.get(handler_two.id)?.result, 'h2:e3')
  assert.deepEqual(run_order, ['h2:e1', 'h1:e2', 'h2:e2', 'h1:e3', 'h2:e3'])
})

test('ts -> rust -> ts bus roundtrip rehydrates and resumes pending queue', async (t) => {
  const ResumeEvent = BaseEvent.extend('TsRustBusResumeEvent', {
    label: z.string(),
    event_result_type: z.string(),
  })

  const source_bus = new EventBus('TsRustBusSource', {
    id: '018f8e40-1234-7000-8000-00000000aa12',
    event_handler_detect_file_paths: false,
    event_handler_concurrency: 'serial',
    event_handler_completion: 'all',
  })
  let restored: EventBus | null = null
  t.after(async () => {
    await restored?.destroy()
    await source_bus.destroy()
  })

  const handler_one = source_bus.on(ResumeEvent, (event) => `h1:${(event as unknown as { label: string }).label}`)
  const handler_two = source_bus.on(ResumeEvent, (event) => `h2:${(event as unknown as { label: string }).label}`)

  const event_one = ResumeEvent({ label: 'e1' })
  const event_two = ResumeEvent({ label: 'e2' })

  const seeded = event_one.eventResultUpdate(handler_one, { eventbus: source_bus, status: 'pending' })
  event_one.eventResultUpdate(handler_two, { eventbus: source_bus, status: 'pending' })
  seeded.update({ status: 'completed', result: 'seeded' })

  source_bus.event_history.set(event_one.event_id, event_one)
  source_bus.event_history.set(event_two.event_id, event_two)
  source_bus.pending_event_queue = [event_one, event_two]

  const source_dump = source_bus.toJSON()
  const rust_roundtripped = runRustRoundtrip('bus', source_dump)
  restored = EventBus.fromJSON(rust_roundtripped)
  const restored_dump = restored.toJSON()

  assert.deepEqual(Object.keys(restored_dump.handlers), Object.keys(source_dump.handlers))
  for (const [handler_id, handler_payload] of Object.entries(source_dump.handlers as Record<string, Record<string, unknown>>)) {
    const restored_handler = (restored_dump.handlers as Record<string, Record<string, unknown>>)[handler_id]
    assert.ok(restored_handler, `missing handler ${handler_id}`)
    assert.equal(restored_handler.eventbus_id, handler_payload.eventbus_id)
    assert.equal(restored_handler.eventbus_name, handler_payload.eventbus_name)
    assert.equal(restored_handler.event_pattern, handler_payload.event_pattern)
  }
  assert.deepEqual(restored_dump.handlers_by_key, source_dump.handlers_by_key)
  assert.deepEqual(restored_dump.pending_event_queue, source_dump.pending_event_queue)
  assert.deepEqual(Object.keys(restored_dump.event_history), Object.keys(source_dump.event_history))

  const restored_event_one = restored.event_history.get(event_one.event_id)
  assert.ok(restored_event_one)
  const preseeded = Array.from(restored_event_one!.event_results.values()).find((result) => result.result === 'seeded')
  assert.ok(preseeded)
  assert.equal(preseeded!.status, 'completed')
  assert.equal(preseeded!.result, 'seeded')
  assert.equal(preseeded!.handler, restored.handlers.get(preseeded!.handler_id))

  const run_order: string[] = []
  const restored_handler_one = restored.handlers.get(handler_one.id)
  const restored_handler_two = restored.handlers.get(handler_two.id)
  assert.ok(restored_handler_one)
  assert.ok(restored_handler_two)
  const restored_handler_one_fn = (event: BaseEvent): string => {
    const label = Reflect.get(event, 'label')
    run_order.push(`h1:${String(label)}`)
    return `h1:${String(label)}`
  }
  const restored_handler_two_fn = (event: BaseEvent): string => {
    const label = Reflect.get(event, 'label')
    run_order.push(`h2:${String(label)}`)
    return `h2:${String(label)}`
  }
  restored_handler_one.handler = restored_handler_one_fn
  restored_handler_two.handler = restored_handler_two_fn

  const trigger = restored.emit(ResumeEvent({ label: 'e3' }))
  await runWithTimeout(trigger.wait(), EVENT_WAIT_TIMEOUT_MS, 'bus resume completion')

  const done_one = restored.event_history.get(event_one.event_id)
  const done_two = restored.event_history.get(event_two.event_id)
  const done_three = restored.event_history.get(trigger.event_id)
  assert.equal(done_three?.event_status, 'completed')
  assert.equal(restored.pending_event_queue.length, 0)
  assert.ok(Array.from(done_one?.event_results.values() ?? []).every((result) => result.status === 'completed'))
  assert.ok(Array.from(done_two?.event_results.values() ?? []).every((result) => result.status === 'completed'))
  assert.equal(done_one?.event_results.get(handler_one.id)?.result, 'seeded')
  assert.equal(done_one?.event_results.get(handler_two.id)?.result, 'h2:e1')
  assert.equal(done_two?.event_results.get(handler_one.id)?.result, 'h1:e2')
  assert.equal(done_two?.event_results.get(handler_two.id)?.result, 'h2:e2')
  assert.equal(done_three?.event_results.get(handler_one.id)?.result, 'h1:e3')
  assert.equal(done_three?.event_results.get(handler_two.id)?.result, 'h2:e3')
  assert.deepEqual(run_order, ['h2:e1', 'h1:e2', 'h2:e2', 'h1:e3', 'h2:e3'])
})

test('ts -> go -> ts bus roundtrip rehydrates and resumes pending queue', async (t) => {
  const ResumeEvent = BaseEvent.extend('TsGoBusResumeEvent', {
    label: z.string(),
    event_result_type: z.string(),
  })

  const source_bus = new EventBus('TsGoBusSource', {
    id: '018f8e40-1234-7000-8000-00000000aa13',
    event_handler_detect_file_paths: false,
    event_handler_concurrency: 'serial',
    event_handler_completion: 'all',
  })
  let restored: EventBus | null = null
  t.after(async () => {
    await restored?.destroy()
    await source_bus.destroy()
  })

  const handler_one = source_bus.on(ResumeEvent, (event) => `h1:${(event as unknown as { label: string }).label}`)
  const handler_two = source_bus.on(ResumeEvent, (event) => `h2:${(event as unknown as { label: string }).label}`)

  const event_one = ResumeEvent({ label: 'e1' })
  const event_two = ResumeEvent({ label: 'e2' })

  const seeded = event_one.eventResultUpdate(handler_one, { eventbus: source_bus, status: 'pending' })
  event_one.eventResultUpdate(handler_two, { eventbus: source_bus, status: 'pending' })
  seeded.update({ status: 'completed', result: 'seeded' })

  source_bus.event_history.set(event_one.event_id, event_one)
  source_bus.event_history.set(event_two.event_id, event_two)
  source_bus.pending_event_queue = [event_one, event_two]

  const source_dump = source_bus.toJSON()
  const go_roundtripped = runGoRoundtrip('bus', source_dump)
  restored = EventBus.fromJSON(go_roundtripped)
  const restored_dump = restored.toJSON()

  assert.deepEqual(Object.keys(restored_dump.handlers), Object.keys(source_dump.handlers))
  for (const [handler_id, handler_payload] of Object.entries(source_dump.handlers as Record<string, Record<string, unknown>>)) {
    const restored_handler = (restored_dump.handlers as Record<string, Record<string, unknown>>)[handler_id]
    assert.ok(restored_handler, `missing handler ${handler_id}`)
    assert.equal(restored_handler.eventbus_id, handler_payload.eventbus_id)
    assert.equal(restored_handler.eventbus_name, handler_payload.eventbus_name)
    assert.equal(restored_handler.event_pattern, handler_payload.event_pattern)
  }
  assert.deepEqual(restored_dump.handlers_by_key, source_dump.handlers_by_key)
  assert.deepEqual(restored_dump.pending_event_queue, source_dump.pending_event_queue)
  assert.deepEqual(Object.keys(restored_dump.event_history), Object.keys(source_dump.event_history))

  const restored_event_one = restored.event_history.get(event_one.event_id)
  assert.ok(restored_event_one)
  const preseeded = Array.from(restored_event_one!.event_results.values()).find((result) => result.result === 'seeded')
  assert.ok(preseeded)
  assert.equal(preseeded!.status, 'completed')
  assert.equal(preseeded!.result, 'seeded')
  assert.equal(preseeded!.handler, restored.handlers.get(preseeded!.handler_id))

  const run_order: string[] = []
  const restored_handler_one = restored.handlers.get(handler_one.id)
  const restored_handler_two = restored.handlers.get(handler_two.id)
  assert.ok(restored_handler_one)
  assert.ok(restored_handler_two)
  const restored_handler_one_fn = (event: BaseEvent): string => {
    const label = Reflect.get(event, 'label')
    run_order.push(`h1:${String(label)}`)
    return `h1:${String(label)}`
  }
  const restored_handler_two_fn = (event: BaseEvent): string => {
    const label = Reflect.get(event, 'label')
    run_order.push(`h2:${String(label)}`)
    return `h2:${String(label)}`
  }
  restored_handler_one.handler = restored_handler_one_fn
  restored_handler_two.handler = restored_handler_two_fn

  const trigger = restored.emit(ResumeEvent({ label: 'e3' }))
  await runWithTimeout(trigger.wait(), EVENT_WAIT_TIMEOUT_MS, 'bus resume completion')

  const done_one = restored.event_history.get(event_one.event_id)
  const done_two = restored.event_history.get(event_two.event_id)
  const done_three = restored.event_history.get(trigger.event_id)
  assert.equal(done_three?.event_status, 'completed')
  assert.equal(restored.pending_event_queue.length, 0)
  assert.ok(Array.from(done_one?.event_results.values() ?? []).every((result) => result.status === 'completed'))
  assert.ok(Array.from(done_two?.event_results.values() ?? []).every((result) => result.status === 'completed'))
  assert.equal(done_one?.event_results.get(handler_one.id)?.result, 'seeded')
  assert.equal(done_one?.event_results.get(handler_two.id)?.result, 'h2:e1')
  assert.equal(done_two?.event_results.get(handler_one.id)?.result, 'h1:e2')
  assert.equal(done_two?.event_results.get(handler_two.id)?.result, 'h2:e2')
  assert.equal(done_three?.event_results.get(handler_one.id)?.result, 'h1:e3')
  assert.equal(done_three?.event_results.get(handler_two.id)?.result, 'h2:e3')
  assert.deepEqual(run_order, ['h2:e1', 'h1:e2', 'h2:e2', 'h1:e3', 'h2:e3'])
})

// Folded from bridges.test.ts to keep test layout class-based.
const TEST_RUN_ID = `${process.pid}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`

const makeTempDir = (prefix: string): string => mkdtempSync(join(tmpdir(), `${prefix}-${TEST_RUN_ID}-`))

const IPCPingEvent = BaseEvent.extend('IPCPingEvent', {
  label: z.string(),
})

const getFreePort = async (): Promise<number> =>
  await new Promise<number>((resolve, reject) => {
    const server = createNetServer()
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => {
      const address = server.address()
      if (!address || typeof address === 'string') {
        server.close(() => reject(new Error('failed to allocate test port')))
        return
      }
      server.close(() => resolve(address.port))
    })
  })

const sleep = async (ms: number): Promise<void> => await new Promise((resolve) => setTimeout(resolve, ms))

const importDynamicModule = async (module_name: string): Promise<any> => {
  const dynamic_import = Function('module_name', 'return import(module_name)') as (module_name: string) => Promise<unknown>
  return dynamic_import(module_name) as Promise<any>
}

const canonical = (payload: Record<string, unknown>): Record<string, unknown> => {
  const normalized: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(payload)) {
    if (key.endsWith('_at') && typeof value === 'string') {
      const ts = Date.parse(value)
      if (!Number.isNaN(ts)) {
        normalized[key] = ts
        continue
      }
    }
    normalized[key] = value
  }
  return normalized
}

const normalizeRoundtripPayload = (payload: Record<string, unknown>): Record<string, unknown> => {
  const normalized = canonical(payload)
  const dynamic_keys = [
    'event_id',
    'event_path',
    'event_result_type',
    'event_results',
    'event_pending_bus_count',
    'event_status',
    'event_started_at',
    'event_completed_at',
    'event_timeout',
    'event_handler_completion',
    'event_handler_concurrency',
    'event_handler_slow_timeout',
    'event_handler_timeout',
    'event_parent_id',
    'event_emitted_by_handler_id',
    'event_concurrency',
  ]
  for (const key of dynamic_keys) {
    delete normalized[key]
  }
  for (const [key, value] of Object.entries(normalized)) {
    if (value === undefined) {
      delete normalized[key]
    }
  }
  return normalized
}

const waitForPort = async (port: number, timeout_ms = 30000): Promise<void> => {
  const started = Date.now()
  while (Date.now() - started < timeout_ms) {
    const ok = await new Promise<boolean>((resolve) => {
      const socket = createConnection({ host: '127.0.0.1', port }, () => {
        socket.end()
        resolve(true)
      })
      socket.once('error', () => resolve(false))
    })
    if (ok) return
    await sleep(50)
  }
  throw new Error(`port did not open in time: ${port}`)
}

const waitForPath = async (
  path: string,
  worker: ChildProcess,
  stdout_log: { value: string },
  stderr_log: { value: string },
  timeout_ms = 30000
): Promise<void> => {
  const started = Date.now()
  while (Date.now() - started < timeout_ms) {
    if (existsSync(path)) return
    if (worker.exitCode !== null) {
      throw new Error(`worker exited early (${worker.exitCode})\nstdout:\n${stdout_log.value}\nstderr:\n${stderr_log.value}`)
    }
    await sleep(50)
  }
  throw new Error(`path did not appear in time: ${path}`)
}

const stopProcess = async (proc: ChildProcess): Promise<void> => {
  if (proc.exitCode !== null) return
  proc.kill('SIGTERM')
  await sleep(250)
  if (proc.exitCode === null) {
    proc.kill('SIGKILL')
    await sleep(250)
  }
}

const runChecked = (cmd: string, args: string[], cwd?: string): void => {
  const result = spawnSync(cmd, args, { cwd, encoding: 'utf8' })
  assert.equal(result.status, 0, `${cmd} failed\nstdout:\n${result.stdout ?? ''}\nstderr:\n${result.stderr ?? ''}`)
}

const makeSenderBridge = (kind: string, config: Record<string, string>, low_latency: boolean = false): any => {
  if (kind === 'http') return new HTTPEventBridge({ send_to: config.endpoint })
  if (kind === 'socket') return new SocketEventBridge(config.path)
  if (kind === 'jsonl') return new JSONLEventBridge(config.path, low_latency ? 0.001 : 0.05)
  if (kind === 'sqlite') return new SQLiteEventBridge(config.path, config.table, low_latency ? 0.001 : 0.05)
  if (kind === 'redis') return new RedisEventBridge(config.url)
  if (kind === 'nats') return new NATSEventBridge(config.server, config.subject)
  if (kind === 'postgres') return new PostgresEventBridge(config.url)
  if (kind === 'tachyon') return new TachyonEventBridge(config.path)
  throw new Error(`unsupported bridge kind: ${kind}`)
}

const makeListenerBridge = (kind: string, config: Record<string, string>, low_latency: boolean = false): any => {
  if (kind === 'http') return new HTTPEventBridge({ listen_on: config.endpoint })
  if (kind === 'socket') return new SocketEventBridge(config.path)
  if (kind === 'jsonl') return new JSONLEventBridge(config.path, low_latency ? 0.001 : 0.05)
  if (kind === 'sqlite') return new SQLiteEventBridge(config.path, config.table, low_latency ? 0.001 : 0.05)
  if (kind === 'redis') return new RedisEventBridge(config.url)
  if (kind === 'nats') return new NATSEventBridge(config.server, config.subject)
  if (kind === 'postgres') return new PostgresEventBridge(config.url)
  if (kind === 'tachyon') return new TachyonEventBridge(config.path)
  throw new Error(`unsupported bridge kind: ${kind}`)
}

const waitForEvent = async (event: Promise<void>, timeout_ms: number): Promise<void> => {
  let timer: ReturnType<typeof setTimeout> | null = null
  try {
    await Promise.race([
      event,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`timed out waiting for bridge event after ${timeout_ms}ms`)), timeout_ms)
      }),
    ])
  } finally {
    if (timer) clearTimeout(timer)
  }
}

const measureWarmLatencyMs = async (kind: string, config: Record<string, string>): Promise<number> => {
  const attempts = 3
  let last_error: unknown

  for (let attempt = 0; attempt < attempts; attempt += 1) {
    const sender = makeSenderBridge(kind, config, true)
    const receiver = makeListenerBridge(kind, config, true)

    const run_suffix = Math.random().toString(36).slice(2, 10)
    const warmup_prefix = `warmup_${run_suffix}_`
    const measured_prefix = `measured_${run_suffix}_`
    const warmup_count_target = 5
    const measured_count_target = 1000

    let warmup_seen_count = 0
    let measured_seen_count = 0
    let warmup_resolve: (() => void) | null = null
    let measured_resolve: (() => void) | null = null
    const warmup_seen = new Promise<void>((resolve) => {
      warmup_resolve = resolve
    })
    const measured_seen = new Promise<void>((resolve) => {
      measured_resolve = resolve
    })

    const onEvent = (event: { label?: unknown }): void => {
      const label = typeof event.label === 'string' ? event.label : ''
      if (label.startsWith(warmup_prefix)) {
        warmup_seen_count += 1
        if (warmup_seen_count >= warmup_count_target) {
          warmup_resolve?.()
          warmup_resolve = null
        }
        return
      }
      if (label.startsWith(measured_prefix)) {
        measured_seen_count += 1
        if (measured_seen_count >= measured_count_target) {
          measured_resolve?.()
          measured_resolve = null
        }
      }
    }

    const emitBatch = async (prefix: string, count: number): Promise<void> => {
      for (let i = 0; i < count; i += 1) {
        await sender.emit(IPCPingEvent({ label: `${prefix}${i}` }))
      }
    }

    try {
      await sender.start()
      await receiver.start()
      receiver.on('IPCPingEvent', onEvent)
      await sleep(100)

      await emitBatch(warmup_prefix, warmup_count_target)
      await waitForEvent(warmup_seen, 60000)

      const start_ms = performance.now()
      await emitBatch(measured_prefix, measured_count_target)
      await waitForEvent(measured_seen, 120000)
      return (performance.now() - start_ms) / measured_count_target
    } catch (error: unknown) {
      last_error = error
    } finally {
      await sender.close()
      await receiver.close()
    }

    await sleep(200)
  }

  throw new Error(`bridge latency measurement timed out after ${attempts} attempts: ${kind} (${String(last_error)})`)
}

const assertRoundtrip = async (kind: string, config: Record<string, string>): Promise<void> => {
  const temp_dir = makeTempDir(`abxbus-bridge-${kind}`)
  const ready_path = join(temp_dir, 'worker.ready')
  const output_path = join(temp_dir, 'received.json')
  const config_path = join(temp_dir, 'worker_config.json')
  const worker_payload = {
    ...config,
    kind,
    ready_path,
    output_path,
  }
  writeFileSync(config_path, JSON.stringify(worker_payload), 'utf8')

  const sender = makeSenderBridge(kind, config)

  const worker = spawn(process.execPath, ['--import', 'tsx', join(tests_dir, 'EventBridge_listener_worker.ts'), config_path], {
    cwd: tests_dir,
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  const worker_stdout = { value: '' }
  const worker_stderr = { value: '' }
  worker.stdout?.on('data', (chunk) => {
    worker_stdout.value += String(chunk)
  })
  worker.stderr?.on('data', (chunk) => {
    worker_stderr.value += String(chunk)
  })

  try {
    await waitForPath(ready_path, worker, worker_stdout, worker_stderr)
    if (kind === 'postgres') {
      await sender.start()
    }
    const outbound = IPCPingEvent({ label: `${kind}_ok` })
    await sender.emit(outbound)
    await waitForPath(output_path, worker, worker_stdout, worker_stderr)
    const received_payload = JSON.parse(readFileSync(output_path, 'utf8')) as Record<string, unknown>
    assert.deepEqual(normalizeRoundtripPayload(received_payload), normalizeRoundtripPayload(outbound.toJSON() as Record<string, unknown>))
  } finally {
    await sender.close()
    await stopProcess(worker)
    rmSync(temp_dir, { recursive: true, force: true })
  }
}

test('HTTPEventBridge roundtrip between processes', async () => {
  assert.ok(HTTPEventBridge.prototype instanceof EventBridge)
  const endpoint = `http://127.0.0.1:${await getFreePort()}/events`
  await assertRoundtrip('http', { endpoint })
  const latency_ms = await measureWarmLatencyMs('http', { endpoint })
  console.log(`LATENCY ts http ${latency_ms.toFixed(3)}ms`)
})

test('SocketEventBridge roundtrip between processes', async () => {
  const socket_path = `/tmp/bb-${TEST_RUN_ID}-${Math.random().toString(16).slice(2)}.sock`
  await assertRoundtrip('socket', { path: socket_path })
  const latency_ms = await measureWarmLatencyMs('socket', { path: socket_path })
  console.log(`LATENCY ts socket ${latency_ms.toFixed(3)}ms`)
})

test('SocketEventBridge rejects long socket paths', async () => {
  const long_path = `/tmp/${'a'.repeat(100)}.sock`
  assert.throws(() => {
    new SocketEventBridge(long_path)
  })
})

test('JSONLEventBridge roundtrip between processes', async () => {
  const temp_dir = makeTempDir('abxbus-jsonl')
  try {
    const config = { path: join(temp_dir, 'events.jsonl') }
    await assertRoundtrip('jsonl', config)
    const latency_ms = await measureWarmLatencyMs('jsonl', config)
    console.log(`LATENCY ts jsonl ${latency_ms.toFixed(3)}ms`)
  } finally {
    rmSync(temp_dir, { recursive: true, force: true })
  }
})

test('SQLiteEventBridge roundtrip between processes', async () => {
  const temp_dir = makeTempDir('abxbus-sqlite')
  try {
    const sqlite_path = join(temp_dir, 'events.sqlite3')
    const config = { path: sqlite_path, table: 'abxbus_events' }
    await assertRoundtrip('sqlite', config)

    const sqlite_mod = await importDynamicModule('node:sqlite')
    const Database = sqlite_mod.DatabaseSync ?? sqlite_mod.default?.DatabaseSync
    assert.equal(typeof Database, 'function', 'expected DatabaseSync from node:sqlite')
    const db = new Database(sqlite_path)
    try {
      const columns = new Set<string>(
        (db.prepare('PRAGMA table_info("abxbus_events")').all() as Array<{ name: string }>).map((row) => String(row.name))
      )
      assert.ok(columns.has('event_payload'))
      assert.ok(!columns.has('label'))
      for (const column of columns) {
        assert.ok(column === 'event_payload' || column.startsWith('event_'))
      }

      const row = db.prepare('SELECT event_payload FROM "abxbus_events" ORDER BY COALESCE("event_created_at", \'\') DESC LIMIT 1').get() as
        | { event_payload?: string }
        | undefined
      assert.ok(row?.event_payload, 'expected event_payload row')
      const payload = JSON.parse(String(row.event_payload)) as Record<string, unknown>
      assert.equal(payload.label, 'sqlite_ok')
    } finally {
      db.close()
    }

    const latency_ms = await measureWarmLatencyMs('sqlite', config)
    console.log(`LATENCY ts sqlite ${latency_ms.toFixed(3)}ms`)
  } finally {
    rmSync(temp_dir, { recursive: true, force: true })
  }
})

test('RedisEventBridge roundtrip between processes', async () => {
  const temp_dir = makeTempDir('abxbus-redis')
  const port = await getFreePort()
  const redis = spawn(
    'redis-server',
    ['--save', '', '--appendonly', 'no', '--bind', '127.0.0.1', '--port', String(port), '--dir', temp_dir],
    { stdio: ['ignore', 'pipe', 'pipe'] }
  )
  try {
    await waitForPort(port)
    const config = { url: `redis://127.0.0.1:${port}/1/abxbus_events` }
    await assertRoundtrip('redis', config)
    const latency_ms = await measureWarmLatencyMs('redis', config)
    console.log(`LATENCY ts redis ${latency_ms.toFixed(3)}ms`)
  } finally {
    await stopProcess(redis)
    rmSync(temp_dir, { recursive: true, force: true })
  }
})

test('NATSEventBridge roundtrip between processes', async () => {
  const port = await getFreePort()
  const nats = spawn('nats-server', ['-a', '127.0.0.1', '-p', String(port)], { stdio: ['ignore', 'pipe', 'pipe'] })
  try {
    await waitForPort(port)
    const config = { server: `nats://127.0.0.1:${port}`, subject: 'abxbus_events' }
    await assertRoundtrip('nats', config)
    const latency_ms = await measureWarmLatencyMs('nats', config)
    console.log(`LATENCY ts nats ${latency_ms.toFixed(3)}ms`)
  } finally {
    await stopProcess(nats)
  }
})

test('TachyonEventBridge roundtrip between processes', async () => {
  const socket_path = `/tmp/bb-tachyon-${TEST_RUN_ID}-${Math.random().toString(16).slice(2)}.sock`
  try {
    await assertRoundtrip('tachyon', { path: socket_path })
    const latency_ms = await measureWarmLatencyMs('tachyon', { path: socket_path })
    console.log(`LATENCY ts tachyon ${latency_ms.toFixed(3)}ms`)
  } finally {
    if (existsSync(socket_path)) {
      try {
        rmSync(socket_path, { force: true })
      } catch {
        // ignore
      }
    }
  }
})

test('PostgresEventBridge roundtrip between processes', async () => {
  const temp_dir = makeTempDir('abxbus-postgres')
  const data_dir = join(temp_dir, 'pgdata')
  runChecked('initdb', ['-D', data_dir, '-A', 'trust', '-U', 'postgres'])
  const port = await getFreePort()
  const postgres = spawn('postgres', ['-D', data_dir, '-h', '127.0.0.1', '-p', String(port), '-k', '/tmp'], {
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  try {
    await waitForPort(port)
    const config = { url: `postgresql://postgres@127.0.0.1:${port}/postgres/abxbus_events` }
    await assertRoundtrip('postgres', config)

    const pg_mod = await importDynamicModule('pg')
    const Client = pg_mod.Client ?? pg_mod.default?.Client
    assert.equal(typeof Client, 'function', 'expected pg Client')
    const client = new Client({ connectionString: `postgresql://postgres@127.0.0.1:${port}/postgres` })
    await client.connect()
    try {
      const columns_result = await client.query(
        `SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1`,
        ['abxbus_events']
      )
      const columns = new Set<string>((columns_result.rows as Array<{ column_name: string }>).map((row) => String(row.column_name)))
      assert.ok(columns.has('event_payload'))
      assert.ok(!columns.has('label'))
      for (const column of columns) {
        assert.ok(column === 'event_payload' || column.startsWith('event_'))
      }

      const payload_result = await client.query(
        `SELECT event_payload FROM "abxbus_events" ORDER BY COALESCE("event_created_at", '') DESC LIMIT 1`
      )
      const payload_raw = payload_result.rows?.[0]?.event_payload
      assert.equal(typeof payload_raw, 'string')
      const payload = JSON.parse(payload_raw) as Record<string, unknown>
      assert.equal(payload.label, 'postgres_ok')
    } finally {
      await client.end()
    }

    const latency_ms = await measureWarmLatencyMs('postgres', config)
    console.log(`LATENCY ts postgres ${latency_ms.toFixed(3)}ms`)
  } finally {
    await stopProcess(postgres)
    rmSync(temp_dir, { recursive: true, force: true })
  }
})
