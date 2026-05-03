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
import { existsSync, unlinkSync } from 'node:fs'
import { Worker } from 'node:worker_threads'

import { BaseEvent } from './BaseEvent.js'
import { EventBus } from './EventBus.js'
import { assertOptionalDependencyAvailable, isNodeRuntime } from './optional_deps.js'
import type { EventClass, EventHandlerCallable, EventPattern, UntypedEventHandlerFunction } from './types.js'

const randomSuffix = (): string => Math.random().toString(36).slice(2, 10)
const DEFAULT_TACHYON_CAPACITY = 1 << 20
const TACHYON_CONNECT_TIMEOUT_MS = 5000
const TACHYON_LISTEN_TIMEOUT_MS = 5000
// Tachyon recv() blocks on a futex that worker.terminate() cannot preempt; the
// producer signals graceful shutdown by emitting one final message with this
// reserved type id, which lets the consumer break out of its recv loop.
const TACHYON_SHUTDOWN_TYPE_ID = 0xdead
const TACHYON_DATA_TYPE_ID = 1

const TACHYON_LISTENER_WORKER_CODE = `
const { parentPort, workerData } = require('node:worker_threads')

const SHUTDOWN_TYPE_ID = ${TACHYON_SHUTDOWN_TYPE_ID}

const main = async () => {
  const { Bus } = await import('@tachyon-ipc/core')
  const { path, capacity } = workerData
  const bus = Bus.listen(path, capacity)
  parentPort.postMessage({ type: 'ready' })
  while (true) {
    let msg
    try {
      msg = bus.recv()
    } catch (err) {
      parentPort.postMessage({ type: 'error', message: err && err.message ? err.message : String(err) })
      break
    }
    if (msg.typeId === SHUTDOWN_TYPE_ID) break
    parentPort.postMessage({ type: 'message', data: msg.data, typeId: msg.typeId })
  }
  try { bus.close && bus.close() } catch {}
}

main().catch((err) => {
  if (parentPort) {
    parentPort.postMessage({ type: 'error', message: err && err.message ? err.message : String(err) })
  }
}).finally(() => process.exit(0))
`

const TACHYON_SENDER_WORKER_CODE = `
const { parentPort, workerData } = require('node:worker_threads')

const SHUTDOWN_TYPE_ID = ${TACHYON_SHUTDOWN_TYPE_ID}
const DATA_TYPE_ID = ${TACHYON_DATA_TYPE_ID}

const main = async () => {
  const { Bus } = await import('@tachyon-ipc/core')
  const { path, connect_timeout_ms } = workerData
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
    parentPort.postMessage({ type: 'error', message: 'TachyonEventBridge sender failed to connect: ' + (last_err && last_err.message ? last_err.message : String(last_err)) })
    return
  }
  parentPort.postMessage({ type: 'ready' })
  parentPort.on('message', (msg) => {
    if (!msg) return
    if (msg.type === 'send') {
      try {
        bus.send(Buffer.from(msg.payload), DATA_TYPE_ID)
        parentPort.postMessage({ type: 'sent', id: msg.id })
      } catch (err) {
        parentPort.postMessage({ type: 'send_error', id: msg.id, message: err && err.message ? err.message : String(err) })
      }
      return
    }
    if (msg.type === 'close') {
      try { bus.send(Buffer.alloc(0), SHUTDOWN_TYPE_ID) } catch {}
      try { bus.close && bus.close() } catch {}
      process.exit(0)
    }
  })
}

main().catch((err) => {
  if (parentPort) {
    parentPort.postMessage({ type: 'error', message: err && err.message ? err.message : String(err) })
  }
})
`

type SendResolver = { resolve: () => void; reject: (err: Error) => void }

export class TachyonEventBridge {
  readonly path: string
  readonly capacity: number
  readonly name: string

  private readonly inbound_bus: EventBus
  private listener_worker: Worker | null
  private sender_worker: Worker | null
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

    this.path = path
    this.capacity = capacity
    this.name = name ?? `TachyonEventBridge_${randomSuffix()}`
    this.inbound_bus = new EventBus(this.name, { max_history_size: 0 })
    this.listener_worker = null
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
    if (typeof event_pattern === 'string') {
      this.inbound_bus.on(event_pattern, handler as UntypedEventHandlerFunction<BaseEvent>)
      return
    }
    this.inbound_bus.on(event_pattern as EventClass<BaseEvent>, handler as EventHandlerCallable<BaseEvent>)
  }

  async emit<T extends BaseEvent>(event: T): Promise<void> {
    await this.ensureSenderConnected()
    if (!this.sender_worker || this.closed) {
      throw new Error('TachyonEventBridge is closed')
    }
    const payload = Buffer.from(JSON.stringify(event.toJSON()))
    const id = ++this.send_seq
    await new Promise<void>((resolve, reject) => {
      this.pending_sends.set(id, { resolve, reject })
      this.sender_worker!.postMessage({ type: 'send', id, payload })
    })
  }

  async dispatch<T extends BaseEvent>(event: T): Promise<void> {
    return this.emit(event)
  }

  async start(): Promise<void> {
    return
  }

  async close(): Promise<void> {
    this.closed = true
    if (this.sender_worker) {
      const sender_exited = new Promise<void>((resolve) => {
        this.sender_worker!.once('exit', () => resolve())
      })
      try {
        this.sender_worker.postMessage({ type: 'close' })
      } catch {
        // worker may already be gone
      }
      // The sender flushes a SHUTDOWN_TYPE_ID sentinel and self-exits in response
      // to `close`; await that natural exit before terminating to avoid orphaning.
      await Promise.race([sender_exited, new Promise((resolve) => setTimeout(resolve, 1000))])
      try {
        await this.sender_worker.terminate()
      } catch {
        // ignore
      }
      this.sender_worker = null
    }
    const was_listener = this.listener_worker !== null
    if (this.listener_worker) {
      // The listener exits naturally once it consumes the shutdown sentinel.
      const listener_exited = new Promise<void>((resolve) => {
        this.listener_worker!.once('exit', () => resolve())
      })
      await Promise.race([listener_exited, new Promise((resolve) => setTimeout(resolve, 1000))])
      try {
        await this.listener_worker.terminate()
      } catch {
        // ignore
      }
      this.listener_worker = null
    }
    for (const pending of this.pending_sends.values()) {
      pending.reject(new Error('TachyonEventBridge closed'))
    }
    this.pending_sends.clear()
    // Only the side that bound the socket (the listener) owns the path on disk.
    // Sender-only instances must leave it alone so other senders/listeners can keep using it.
    if (was_listener && existsSync(this.path)) {
      try {
        unlinkSync(this.path)
      } catch {
        // ignore
      }
    }
    this.inbound_bus.destroy()
  }

  private ensureListenerStarted(): void {
    if (this.closed) throw new Error('TachyonEventBridge is closed')
    if (this.listener_worker) return
    if (!isNodeRuntime()) {
      throw new Error('TachyonEventBridge is only supported in Node.js runtimes')
    }
    if (existsSync(this.path)) {
      try {
        unlinkSync(this.path)
      } catch {
        // ignore
      }
    }
    const worker = new Worker(TACHYON_LISTENER_WORKER_CODE, {
      eval: true,
      workerData: { path: this.path, capacity: this.capacity },
    })
    worker.on('message', (msg: { type: string; data?: Uint8Array; typeId?: number; message?: string }) => {
      if (msg.type === 'message' && msg.data) {
        try {
          const text = Buffer.from(msg.data).toString('utf-8')
          const payload = JSON.parse(text)
          const event = BaseEvent.fromJSON(payload).eventReset()
          this.inbound_bus.emit(event)
        } catch {
          // ignore malformed payloads
        }
      } else if (msg.type === 'error') {
        console.error('[abxbus] TachyonEventBridge listener error:', msg.message)
      }
    })
    worker.on('error', (err: unknown) => {
      console.error('[abxbus] TachyonEventBridge listener worker crashed:', err)
    })
    // Drop the cached reference if the worker dies (crash, init failure, or natural exit
    // after consuming a shutdown sentinel) so a subsequent on() call can spin up a fresh one.
    worker.on('exit', () => {
      if (this.listener_worker === worker) this.listener_worker = null
    })
    this.listener_worker = worker

    // Block until the worker has bound the unix socket so peers calling Bus.connect(path)
    // immediately after on() do not race the worker's async startup.
    const wait_buffer = new Int32Array(new SharedArrayBuffer(4))
    const deadline = Date.now() + TACHYON_LISTEN_TIMEOUT_MS
    while (Date.now() < deadline) {
      if (existsSync(this.path)) return
      Atomics.wait(wait_buffer, 0, 0, 5)
    }
    // Listener never bound the socket; tear down the dead worker so a retry on()
    // doesn't short-circuit on a permanently unhealthy reference.
    try {
      void worker.terminate()
    } catch {
      // ignore
    }
    if (this.listener_worker === worker) this.listener_worker = null
    throw new Error(`TachyonEventBridge listener did not bind socket ${this.path} within ${TACHYON_LISTEN_TIMEOUT_MS}ms`)
  }

  private async ensureSenderConnected(): Promise<void> {
    if (this.sender_ready_promise) {
      await this.sender_ready_promise
      return
    }
    if (!isNodeRuntime()) {
      throw new Error('TachyonEventBridge is only supported in Node.js runtimes')
    }
    const promise = new Promise<void>((resolve, reject) => {
      const worker = new Worker(TACHYON_SENDER_WORKER_CODE, {
        eval: true,
        workerData: { path: this.path, connect_timeout_ms: TACHYON_CONNECT_TIMEOUT_MS },
      })
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
        else console.error('[abxbus] TachyonEventBridge sender worker crashed:', err)
      })
      worker.on('exit', (code) => {
        // The sender worker exits with code 0 only when responding to our `close`
        // message; any other exit means the worker died unexpectedly and pending
        // sends would otherwise hang forever.
        if (code === 0 && this.closed) return
        const err = new Error(`TachyonEventBridge sender worker exited with code ${code}`)
        for (const pending of this.pending_sends.values()) pending.reject(err)
        this.pending_sends.clear()
        if (this.sender_worker === worker) {
          this.sender_worker = null
          this.sender_ready_promise = null
        }
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
          await this.sender_worker.terminate()
        } catch {
          /* ignore */
        }
        this.sender_worker = null
      }
      throw err
    }
  }
}
