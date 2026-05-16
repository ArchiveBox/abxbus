/**
 * Tachyon SPSC IPC bridge for forwarding events between runtimes.
 *
 * Tachyon is a same-machine shared-memory ring buffer with single-producer/
 * single-consumer semantics. Each bridge instance plays exactly one role per
 * session (sender XOR listener) — the role is committed on the first call to
 * `emit()` (sender) or `on()` (listener).
 *
 * Usage:
 *   const bridge = new TachyonEventBridge('/tmp/abxbus.sock')
 *
 *   // listener side (creates the SHM arena, must exist before sender connects)
 *   bridge.on('SomeEvent', handler)
 *
 *   // sender side (separate process or instance)
 *   const sender = new TachyonEventBridge('/tmp/abxbus.sock')
 *   await sender.emit(event)
 */
import { existsSync, symlinkSync, unlinkSync } from 'node:fs'
import { spawn, type ChildProcess } from 'node:child_process'
import { createRequire } from 'node:module'
import { dirname, join } from 'node:path'

import { BaseEvent } from './BaseEvent.js'
import { EventBus } from './EventBus.js'
import { assertOptionalDependencyAvailable, isNodeRuntime } from './optional_deps.js'
import type { EventClass, EventHandlerCallable, EventPattern, UntypedEventHandlerFunction } from './types.js'

const randomSuffix = (): string => Math.random().toString(36).slice(2, 10)
const DEFAULT_TACHYON_CAPACITY = 16 << 20
const TACHYON_CONNECT_TIMEOUT_MS = 5000
const TACHYON_LISTEN_TIMEOUT_MS = 5000
// Tachyon recv() blocks on a futex that worker.terminate() cannot preempt; the
// producer signals graceful shutdown by emitting one final message with this
// reserved type id, which lets the consumer break out of its recv loop.
const TACHYON_SHUTDOWN_TYPE_ID = 0xdead
const TACHYON_DATA_TYPE_ID = 1
const requireForTachyon = createRequire(import.meta.url)

const ensureTachyonNativeLayout = (): void => {
  try {
    const package_json = requireForTachyon.resolve('@tachyon-ipc/core/package.json')
    const core_dir = dirname(package_json)
    const expected_build_dir = join(dirname(core_dir), 'build')
    const actual_build_dir = join(core_dir, 'build')
    if (!existsSync(expected_build_dir) && existsSync(actual_build_dir)) {
      symlinkSync(actual_build_dir, expected_build_dir, 'dir')
    }
  } catch {
    // Optional dependency availability and import errors are reported by the caller.
  }
}

const TACHYON_NATIVE_LAYOUT_FIX = `
const ensureTachyonNativeLayout = () => {
  try {
    const fs = require('node:fs')
    const path = require('node:path')
    const packageJson = require.resolve('@tachyon-ipc/core/package.json')
    const coreDir = path.dirname(packageJson)
    const expectedBuildDir = path.join(path.dirname(coreDir), 'build')
    const actualBuildDir = path.join(coreDir, 'build')
    if (!fs.existsSync(expectedBuildDir) && fs.existsSync(actualBuildDir)) {
      fs.symlinkSync(actualBuildDir, expectedBuildDir, 'dir')
    }
  } catch {}
}
ensureTachyonNativeLayout()
`

const TACHYON_LISTENER_CHILD_CODE = `
${TACHYON_NATIVE_LAYOUT_FIX}

const SHUTDOWN_TYPE_ID = ${TACHYON_SHUTDOWN_TYPE_ID}
const send = (msg) => { if (process.send) process.send(msg) }

const probeListenerAlive = (path) => new Promise((resolve) => {
  const net = require('node:net')
  const sock = net.createConnection(path)
  const settle = (alive) => {
    try { sock.destroy() } catch {}
    resolve(alive)
  }
  sock.setTimeout(50, () => settle(false))
  sock.once('connect', () => settle(true))
  sock.once('error', () => settle(false))
})

const main = async () => {
  const { Bus } = await import('@tachyon-ipc/core')
  const fs = require('node:fs')
  const { path, capacity } = JSON.parse(process.argv[1])
  let bus
  try {
    bus = Bus.listen(path, capacity)
  } catch (firstErr) {
    // The bind failed because the path is in use. If a live listener owns it, propagate
    // the error so the user can resolve the conflict; if it's a stale socket from a
    // previous crash, unlink it and retry exactly once. Refuse to unlink anything that
    // isn't a unix socket — we have no business deleting an unrelated regular file.
    const alive = await probeListenerAlive(path)
    if (alive || !fs.existsSync(path)) throw firstErr
    let st
    try { st = fs.statSync(path) } catch { throw firstErr }
    if (!st.isSocket()) throw firstErr
    try { fs.unlinkSync(path) } catch {}
    bus = Bus.listen(path, capacity)
  }
  send({ type: 'ready' })
  while (true) {
    let msg
    try {
      msg = bus.recv()
    } catch (err) {
      send({ type: 'error', message: err && err.message ? err.message : String(err) })
      break
    }
    if (msg.typeId === SHUTDOWN_TYPE_ID) break
    send({ type: 'message', data: Buffer.from(msg.data).toString('base64'), typeId: msg.typeId })
  }
}

main().catch((err) => {
  send({ type: 'error', message: err && err.message ? err.message : String(err) })
}).finally(() => process.exit(0))
`

const TACHYON_SENDER_CHILD_CODE = `
${TACHYON_NATIVE_LAYOUT_FIX}

const SHUTDOWN_TYPE_ID = ${TACHYON_SHUTDOWN_TYPE_ID}
const DATA_TYPE_ID = ${TACHYON_DATA_TYPE_ID}
const send = (msg) => { if (process.send) process.send(msg) }

const main = async () => {
  const { Bus } = await import('@tachyon-ipc/core')
  const { path, connect_timeout_ms } = JSON.parse(process.argv[1])
  let bus = null
  let last_err = null
  const deadline = Date.now() + connect_timeout_ms
  while (Date.now() < deadline) {
    try {
      bus = Bus.connect(path)
      break
    } catch (err) {
      last_err = err
      await new Promise((resolve) => setTimeout(resolve, 10))
    }
  }
  if (!bus) {
    send({ type: 'error', message: 'TachyonEventBridge sender failed to connect: ' + (last_err && last_err.message ? last_err.message : String(last_err)) })
    return
  }
  send({ type: 'ready' })
  process.on('message', (msg) => {
    if (!msg) return
    if (msg.type === 'send') {
      try {
        bus.send(Buffer.from(msg.payload, 'base64'), DATA_TYPE_ID)
        send({ type: 'sent', id: msg.id })
      } catch (err) {
        send({ type: 'send_error', id: msg.id, message: err && err.message ? err.message : String(err) })
      }
      return
    }
    if (msg.type === 'close') {
      try { bus.send(Buffer.alloc(0), SHUTDOWN_TYPE_ID) } catch {}
      process.exit(0)
    }
  })
}

main().catch((err) => {
  send({ type: 'error', message: err && err.message ? err.message : String(err) })
})
`

type SendResolver = { resolve: () => void; reject: (err: Error) => void }

export class TachyonEventBridge {
  readonly path: string
  readonly capacity: number
  readonly name: string

  private inbound_bus: EventBus | null
  private listener_worker: ChildProcess | null
  // Sticky: `listener_worker` may be cleared mid-session (graceful exit, retry path),
  // but the socket on disk is still ours to unlink in close().
  private acted_as_listener: boolean
  private listener_startup_error: Error | null
  private sender_worker: ChildProcess | null
  private sender_ready_promise: Promise<void> | null
  private send_seq: number
  private pending_sends: Map<number, SendResolver>
  private closed: boolean

  constructor(path: string, capacity: number = DEFAULT_TACHYON_CAPACITY, name?: string) {
    if (!path) throw new Error('TachyonEventBridge path must not be empty')
    if (capacity <= 0 || (capacity & (capacity - 1)) !== 0) {
      throw new Error(`TachyonEventBridge capacity must be a positive power of two, got: ${capacity}`)
    }
    assertOptionalDependencyAvailable('TachyonEventBridge', '@tachyon-ipc/core')
    ensureTachyonNativeLayout()

    this.path = path
    this.capacity = capacity
    this.name = name ?? `TachyonEventBridge_${randomSuffix()}`
    this.inbound_bus = null
    this.listener_worker = null
    this.acted_as_listener = false
    this.listener_startup_error = null
    this.sender_worker = null
    this.sender_ready_promise = null
    this.send_seq = 0
    this.pending_sends = new Map()
    this.closed = false

    this.dispatch = this.dispatch.bind(this)
    this.emit = this.emit.bind(this)
    this.on = this.on.bind(this)
  }

  on<T extends BaseEvent>(event_pattern: EventClass<T>, handler: EventHandlerCallable<T>): void
  on<T extends BaseEvent>(event_pattern: string | '*', handler: UntypedEventHandlerFunction<T>): void
  on(event_pattern: EventPattern | '*', handler: EventHandlerCallable | UntypedEventHandlerFunction): void {
    this.ensureListenerStarted()
    const inbound_bus = this.getInboundBus()
    if (typeof event_pattern === 'string') {
      inbound_bus.on(event_pattern, handler as UntypedEventHandlerFunction<BaseEvent>)
      return
    }
    inbound_bus.on(event_pattern as EventClass<BaseEvent>, handler as EventHandlerCallable<BaseEvent>)
  }

  async emit<T extends BaseEvent>(event: T): Promise<void> {
    if (this.closed) throw new Error('TachyonEventBridge is closed')
    await this.ensureSenderConnected()
    if (this.closed || !this.sender_worker) {
      throw new Error('TachyonEventBridge is closed')
    }
    const payload = Buffer.from(JSON.stringify(event.toJSON()))
    const id = ++this.send_seq
    await new Promise<void>((resolve, reject) => {
      this.pending_sends.set(id, { resolve, reject })
      this.sender_worker!.send({ type: 'send', id, payload: payload.toString('base64') })
    })
  }

  async dispatch<T extends BaseEvent>(event: T): Promise<void> {
    return this.emit(event)
  }

  async start(): Promise<void> {
    // Role is committed lazily on first on() / emit(). For listener-side bridges,
    // await the underlying socket bind so callers that need fail-fast readiness
    // (peers about to connect, tests writing a ready_path file) can rely on it.
    const worker = this.listener_worker
    if (!worker) return
    const deadline = Date.now() + TACHYON_LISTEN_TIMEOUT_MS
    let path_first_seen_at: number | null = null
    while (Date.now() < deadline) {
      if (this.listener_startup_error) throw this.listener_startup_error
      // acted_as_listener is set when the worker posts 'ready' — the only signal that
      // *we* (not some other process) actually own the socket file at `path`.
      if (this.acted_as_listener) return
      if (existsSync(this.path)) {
        if (path_first_seen_at === null) {
          path_first_seen_at = Date.now()
        } else if (Date.now() - path_first_seen_at >= 200) {
          // The path exists, but it might belong to another process. After a brief
          // grace window with no startup error and no ready signal, assume our worker
          // bound it (it would otherwise have raised EADDRINUSE almost immediately).
          return
        }
      }
      await new Promise((resolve) => setTimeout(resolve, 5))
    }
    if (this.listener_startup_error) throw this.listener_startup_error
    worker.kill('SIGKILL')
    if (this.listener_worker === worker) this.listener_worker = null
    throw new Error(`TachyonEventBridge listener did not bind socket ${this.path} within ${TACHYON_LISTEN_TIMEOUT_MS}ms`)
  }

  async close(): Promise<void> {
    this.closed = true
    if (this.sender_worker) {
      const sender = this.sender_worker
      this.sender_worker = null
      const sender_exited = new Promise<void>((resolve) => {
        sender.once('exit', () => resolve())
      })
      try {
        sender.send({ type: 'close' })
      } catch {
        // listener may already be gone
      }
      await Promise.race([sender_exited, new Promise((resolve) => setTimeout(resolve, 1000))])
      if (sender.exitCode === null && sender.signalCode === null) {
        try {
          sender.kill('SIGKILL')
        } catch {
          // ignore
        }
      }
    }
    if (this.listener_worker) {
      const listener = this.listener_worker
      this.listener_worker = null
      const listener_exited = new Promise<void>((resolve) => {
        listener.once('exit', () => resolve())
      })
      const exited = await Promise.race([
        listener_exited.then(() => true),
        new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 1000)),
      ])
      if (!exited) {
        try {
          listener.kill('SIGKILL')
        } catch {
          // ignore
        }
      }
    }
    for (const pending of this.pending_sends.values()) {
      pending.reject(new Error('TachyonEventBridge closed'))
    }
    this.pending_sends.clear()
    // Only the side that bound the socket (the listener) owns the path on disk.
    // Sender-only instances must leave it alone so other senders/listeners can keep using it.
    if (this.acted_as_listener && existsSync(this.path)) {
      try {
        unlinkSync(this.path)
      } catch {
        // ignore
      }
    }
    this.inbound_bus?.destroy()
    this.inbound_bus = null
  }

  private ensureListenerStarted(): void {
    if (this.closed) throw new Error('TachyonEventBridge is closed')
    if (this.listener_worker) return
    if (!isNodeRuntime()) {
      throw new Error('TachyonEventBridge is only supported in Node.js runtimes')
    }
    // The worker probes the path before unlinking — only stale sockets (no live
    // listener) get cleared. This avoids clobbering a listener owned by another
    // process when two bridges race on the same path.
    const worker = spawn(
      process.execPath,
      ['-e', TACHYON_LISTENER_CHILD_CODE, JSON.stringify({ path: this.path, capacity: this.capacity })],
      { stdio: ['ignore', 'ignore', 'ignore', 'ipc'] }
    )
    this.listener_startup_error = null
    worker.on('message', (msg: { type: string; data?: string; typeId?: number; message?: string }) => {
      if (msg.type === 'ready') {
        // Bus.listen returning means *this* worker completed the bind+handshake; only
        // now can we safely claim socket ownership for close()'s unlink path.
        this.acted_as_listener = true
      } else if (msg.type === 'message' && msg.data) {
        if (!this.inbound_bus) {
          return
        }
        try {
          const text = Buffer.from(msg.data, 'base64').toString('utf-8')
          const payload = JSON.parse(text)
          const event = BaseEvent.fromJSON(payload).eventReset()
          this.inbound_bus.emit(event)
        } catch {
          // ignore malformed payloads
        }
      } else if (msg.type === 'error') {
        // Surface the failure so a concurrent start() can fail fast instead of
        // hanging until the bind-wait deadline.
        this.listener_startup_error = new Error(msg.message ?? 'TachyonEventBridge listener error')
        console.error('[abxbus] TachyonEventBridge listener error:', msg.message)
      }
    })
    worker.on('error', (err: unknown) => {
      this.listener_startup_error = err instanceof Error ? err : new Error(String(err))
      console.error('[abxbus] TachyonEventBridge listener worker crashed:', err)
    })
    // Drop the cached reference if the worker dies (crash, init failure, or natural exit
    // after consuming a shutdown sentinel) so a subsequent on() call can spin up a fresh one.
    worker.on('exit', () => {
      if (this.listener_worker === worker) this.listener_worker = null
      // Only flag a startup error if we never confirmed a bind AND the bridge isn't
      // shutting down; a normal shutdown exit (close(), or recv loop after consuming a
      // sentinel) shouldn't poison future start() calls.
      if (!this.acted_as_listener && !this.listener_startup_error && !this.closed) {
        this.listener_startup_error = new Error('TachyonEventBridge listener worker exited before binding')
      }
    })
    this.listener_worker = worker
    // on() returns immediately so the Node event loop isn't frozen on startup; peers
    // that try Bus.connect before this worker finishes binding are covered by the
    // sender-side connect retry loop (see TACHYON_CONNECT_TIMEOUT_MS) and by the
    // worker's 'ready' message which flips acted_as_listener once bind completes.
  }

  private getInboundBus(): EventBus {
    if (!this.inbound_bus) {
      this.inbound_bus = new EventBus(this.name, { max_history_size: 100, max_history_drop: true })
    }
    return this.inbound_bus
  }

  private async ensureSenderConnected(): Promise<void> {
    if (this.sender_worker) return
    if (this.sender_ready_promise) {
      await this.sender_ready_promise
      return
    }
    if (!isNodeRuntime()) {
      throw new Error('TachyonEventBridge is only supported in Node.js runtimes')
    }
    const promise = new Promise<void>((resolve, reject) => {
      const worker = spawn(
        process.execPath,
        ['-e', TACHYON_SENDER_CHILD_CODE, JSON.stringify({ path: this.path, connect_timeout_ms: TACHYON_CONNECT_TIMEOUT_MS })],
        { stdio: ['ignore', 'ignore', 'ignore', 'ipc'] }
      )
      let resolved = false
      worker.on('message', (msg: { type: string; id?: number; message?: string }) => {
        if (msg.type === 'ready') {
          resolved = true
          resolve()
          return
        }
        if (msg.type === 'sent' && typeof msg.id === 'number') {
          const pending = this.pending_sends.get(msg.id)
          if (pending) {
            this.pending_sends.delete(msg.id)
            pending.resolve()
          }
          return
        }
        if (msg.type === 'send_error' && typeof msg.id === 'number') {
          const pending = this.pending_sends.get(msg.id)
          if (pending) {
            this.pending_sends.delete(msg.id)
            pending.reject(new Error(msg.message ?? 'TachyonEventBridge send failed'))
          }
          return
        }
        if (msg.type === 'error') {
          const err = new Error(msg.message ?? 'TachyonEventBridge sender error')
          if (!resolved) {
            reject(err)
            return
          }
          console.error('[abxbus] TachyonEventBridge sender error:', err)
        }
      })
      worker.on('error', (err) => {
        if (!resolved) reject(err instanceof Error ? err : new Error(String(err)))
        else console.error('[abxbus] TachyonEventBridge sender process crashed:', err)
      })
      worker.on('exit', (code, signal) => {
        if (code === 0 && this.closed) return
        const err = new Error(`TachyonEventBridge sender process exited with code ${code} signal ${signal}`)
        for (const pending of this.pending_sends.values()) pending.reject(err)
        this.pending_sends.clear()
        if (this.sender_worker === worker) {
          this.sender_worker = null
          this.sender_ready_promise = null
        }
        if (!resolved) reject(err)
      })
      this.sender_worker = worker
    })
    this.sender_ready_promise = promise
    try {
      await promise
    } catch (err) {
      // Allow a later emit() to retry once the listener becomes available.
      this.sender_ready_promise = null
      if (this.sender_worker) {
        try {
          const worker = this.sender_worker as ChildProcess
          worker.kill('SIGKILL')
        } catch {
          /* ignore */
        }
        this.sender_worker = null
      }
      throw err
    } finally {
      if (this.sender_ready_promise === promise) {
        this.sender_ready_promise = null
      }
    }
  }
}
