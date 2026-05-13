import assert from 'node:assert/strict'
import { spawn, spawnSync } from 'node:child_process'
import { existsSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { test } from 'node:test'
import { fileURLToPath } from 'node:url'

import { retry } from '../src/index.js'

const tests_dir = dirname(fileURLToPath(import.meta.url))
const worker_path = resolve(tests_dir, 'subtests', 'retry_multiprocess_worker.ts')
const repo_root = resolve(tests_dir, '..', '..')

const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

const runWorker = async (
  worker_id: number,
  start_ms: number,
  hold_ms: number,
  semaphore_name: string,
  semaphore_limit: number
): Promise<{ code: number | null; events: Array<Record<string, unknown>>; stderr: string }> => {
  const proc = spawn(
    process.execPath,
    ['--import', 'tsx', worker_path, String(worker_id), String(start_ms), String(hold_ms), semaphore_name, String(semaphore_limit)],
    {
      stdio: ['ignore', 'pipe', 'pipe'],
      env: process.env,
    }
  )

  return await new Promise((resolvePromise, reject) => {
    const events: Array<Record<string, unknown>> = []
    let stdout = ''
    let stderr = ''

    proc.stdout?.on('data', (chunk: Buffer | string) => {
      stdout += String(chunk)
      const lines = stdout.split(/\r?\n/)
      stdout = lines.pop() ?? ''
      for (const line of lines) {
        const trimmed = line.trim()
        if (!trimmed) continue
        events.push(JSON.parse(trimmed) as Record<string, unknown>)
      }
    })
    proc.stderr?.on('data', (chunk: Buffer | string) => {
      stderr += String(chunk)
    })
    proc.once('error', reject)
    proc.once('close', (code) => {
      if (stdout.trim()) {
        events.push(JSON.parse(stdout.trim()) as Record<string, unknown>)
      }
      resolvePromise({ code, events, stderr })
    })
  })
}

test('retry: semaphore_scope=multiprocess serializes across JS processes', async () => {
  const semaphore_name = `retry-multiprocess-${Date.now()}-${Math.random().toString(16).slice(2)}`
  const start_ms = Date.now()

  const first_batch = [runWorker(0, start_ms, 700, semaphore_name, 2), runWorker(1, start_ms, 700, semaphore_name, 2)]
  await delay(150)
  const second_batch = [runWorker(2, start_ms, 200, semaphore_name, 2), runWorker(3, start_ms, 200, semaphore_name, 2)]
  const results = await Promise.all([...first_batch, ...second_batch])

  for (const result of results) {
    assert.equal(result.code, 0, result.stderr || JSON.stringify(result.events))
  }

  const acquired = results
    .flatMap((result) => result.events)
    .filter((event) => event.type === 'acquired')
    .sort((a, b) => Number(a.at_ms) - Number(b.at_ms))
  const timeline = results
    .flatMap((result) => result.events)
    .filter((event) => event.type === 'acquired' || event.type === 'released')
    .sort((a, b) => {
      const delta = Number(a.at_ms) - Number(b.at_ms)
      if (delta !== 0) return delta
      return a.type === 'released' ? -1 : 1
    })

  const completed = results.flatMap((result) => result.events).filter((event) => event.type === 'completed')
  assert.equal(acquired.length, 4)
  assert.equal(completed.length, 4)

  let in_flight = 0
  for (const event of timeline) {
    in_flight += event.type === 'acquired' ? 1 : -1
    assert.ok(in_flight >= 0, `negative in-flight count after ${JSON.stringify(event)}`)
    assert.ok(in_flight <= 2, `semaphore limit exceeded by ${JSON.stringify(event)}`)
  }
})

test('retry: semaphore_scope=multiprocess contends with Python retry() using the same semaphore name', async () => {
  const local_venv_python = resolve(
    repo_root,
    '.venv',
    process.platform === 'win32' ? 'Scripts' : 'bin',
    process.platform === 'win32' ? 'python.exe' : 'python'
  )
  const candidates = [
    ...(existsSync(local_venv_python) ? [{ executable: local_venv_python, args: [] as string[] }] : []),
    ...(spawnSync('uv', ['--version'], { stdio: 'ignore' }).status === 0 ? [{ executable: 'uv', args: ['run', 'python'] }] : []),
    ...(spawnSync('python3', ['-c', 'print("ok")'], { stdio: 'ignore' }).status === 0
      ? [{ executable: 'python3', args: [] as string[] }]
      : []),
    { executable: 'python', args: [] as string[] },
  ]
  const python = candidates.find((candidate) => {
    const probe = spawnSync(candidate.executable, [...candidate.args, '-c', 'import abxbus.retry'], { cwd: repo_root, stdio: 'ignore' })
    return probe.status === 0
  })
  if (!python) {
    throw new Error('python abxbus runtime is unavailable for cross-language multiprocess test')
  }

  const semaphore_name = `retry-crosslang-${Date.now()}-${Math.random().toString(16).slice(2)}`
  const python_lock = spawn(
    python.executable,
    [
      ...python.args,
      '-u',
      '-c',
      `
import asyncio
import sys
from abxbus.retry import retry

@retry(max_attempts=1, timeout=5, semaphore_limit=1, semaphore_name=sys.argv[1], semaphore_scope='multiprocess', semaphore_timeout=5, semaphore_lax=False)
async def hold_lock():
    print("LOCKED", flush=True)
    await asyncio.sleep(float(sys.argv[2]))

asyncio.run(hold_lock())
      `,
      semaphore_name,
      '0.7',
    ],
    {
      cwd: repo_root,
      stdio: ['ignore', 'pipe', 'pipe'],
    }
  )

  let stdout = ''
  let stderr = ''
  const python_lock_exit = new Promise<number | null>((resolvePromise, reject) => {
    python_lock.once('error', reject)
    python_lock.stderr?.on('data', (chunk: Buffer | string) => {
      stderr += String(chunk)
    })
    python_lock.once('close', (code) => {
      resolvePromise(code)
    })
  })

  await new Promise<void>((resolvePromise, reject) => {
    python_lock.stdout?.on('data', (chunk: Buffer | string) => {
      stdout += String(chunk)
      if (stdout.includes('LOCKED')) {
        resolvePromise()
      }
    })
    python_lock.once('error', reject)
    python_lock.once('close', (code) => {
      if (!stdout.includes('LOCKED')) {
        reject(new Error(`python locker exited before acquiring (code=${code ?? 'null'}): ${stderr.trim()}`))
      }
    })
  })

  const guarded = retry({
    max_attempts: 1,
    timeout: 5,
    semaphore_limit: 1,
    semaphore_name,
    semaphore_scope: 'multiprocess',
    semaphore_timeout: 5,
    semaphore_lax: false,
  })(async () => Date.now())

  const started = Date.now()
  await guarded()
  const elapsed_ms = Date.now() - started

  const python_lock_code = await python_lock_exit
  assert.equal(python_lock_code, 0, stderr.trim())

  assert.ok(elapsed_ms >= 500, `expected JS acquisition to wait behind Python lock, got ${elapsed_ms}ms`)
})
