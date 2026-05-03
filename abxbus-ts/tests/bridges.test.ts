import assert from 'node:assert/strict'
import { spawn, spawnSync, type ChildProcess } from 'node:child_process'
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { createConnection, createServer as createNetServer } from 'node:net'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { test } from 'node:test'

import { z } from 'zod'

import { NATSEventBridge, PostgresEventBridge, RedisEventBridge, TachyonEventBridge } from '../src/bridges.js'
import { BaseEvent, EventBridge, HTTPEventBridge, JSONLEventBridge, SQLiteEventBridge, SocketEventBridge } from '../src/index.js'

const tests_dir = dirname(fileURLToPath(import.meta.url))
const TEST_RUN_ID = `${process.pid}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`

const tachyonAvailable = async (): Promise<boolean> => {
  const dynamic_import = Function('module_name', 'return import(module_name)') as (module_name: string) => Promise<unknown>
  try {
    await dynamic_import('@tachyon-ipc/core')
    return true
  } catch (error: unknown) {
    const code = typeof error === 'object' && error !== null && 'code' in error ? (error as { code?: string }).code : undefined
    const message = error instanceof Error ? error.message : ''
    // Treat both "package not installed" and "native binary missing" (the cmake-js
    // build step was skipped) as legitimate "not available" signals; rethrow anything
    // else so a real integration breakage still fails the test file.
    if (code === 'ERR_MODULE_NOT_FOUND' || message.includes('tachyon_node.node not found')) {
      return false
    }
    throw error
  }
}
const TACHYON_AVAILABLE = await tachyonAvailable()

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

test('TachyonEventBridge roundtrip between processes', { skip: !TACHYON_AVAILABLE && '@tachyon-ipc/core not installed' }, async () => {
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
