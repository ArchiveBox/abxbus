import { BaseEvent } from './BaseEvent.js'
import { EventBus } from './EventBus.js'
import type { EventClass, EventHandlerCallable, EventPattern, UntypedEventHandlerFunction } from './types.js'

const isNodeRuntime = (): boolean => {
  const maybe_process = (globalThis as { process?: { versions?: { node?: string } } }).process
  return typeof maybe_process?.versions?.node === 'string'
}

const importNodeModule = async (specifier: string): Promise<any> => {
  const dynamic_import = Function('module_name', 'return import(module_name)') as (module_name: string) => Promise<unknown>
  return dynamic_import(specifier) as Promise<any>
}

const randomSuffix = (): string => Math.random().toString(36).slice(2, 10)

export class JSONLEventBridge {
  readonly path: string
  readonly poll_interval: number
  readonly name: string

  private readonly inbound_bus: EventBus
  private running: boolean
  private byte_offset: number
  private pending_line: string
  private listener_task: Promise<void> | null

  constructor(path: string, poll_interval: number = 0.25, name?: string) {
    this.path = path
    this.poll_interval = poll_interval
    this.name = name ?? `JSONLEventBridge_${randomSuffix()}`
    this.inbound_bus = new EventBus(this.name, { max_history_size: 0 })
    this.running = false
    this.byte_offset = 0
    this.pending_line = ''
    this.listener_task = null

    this.dispatch = this.dispatch.bind(this)
    this.emit = this.emit.bind(this)
    this.on = this.on.bind(this)
  }

  on<T extends BaseEvent>(event_pattern: EventClass<T>, handler: EventHandlerCallable<T>): void
  on<T extends BaseEvent>(event_pattern: string | '*', handler: UntypedEventHandlerFunction<T>): void
  on(event_pattern: EventPattern | '*', handler: EventHandlerCallable | UntypedEventHandlerFunction): void {
    this.ensureStarted()
    if (typeof event_pattern === 'string') {
      this.inbound_bus.on(event_pattern, handler as UntypedEventHandlerFunction<BaseEvent>)
      return
    }
    this.inbound_bus.on(event_pattern as EventClass<BaseEvent>, handler as EventHandlerCallable<BaseEvent>)
  }

  async emit<T extends BaseEvent>(event: T): Promise<void> {
    this.ensureStarted()
    const fs = await this.loadFs()
    await fs.promises.mkdir(this.dirname(this.path), { recursive: true })
    const payload = JSON.stringify(event.toJSON()) + '\n'
    await fs.promises.appendFile(this.path, payload, 'utf8')
  }

  async dispatch<T extends BaseEvent>(event: T): Promise<void> {
    return this.emit(event)
  }

  async start(): Promise<void> {
    if (this.running) return
    const fs = await this.loadFs()
    await fs.promises.mkdir(this.dirname(this.path), { recursive: true })
    await fs.promises.appendFile(this.path, '', 'utf8')
    const stats = await fs.promises.stat(this.path)
    this.byte_offset = Number(stats.size ?? 0)
    this.pending_line = ''
    this.running = true
    this.listener_task = this.listenLoop()
  }

  async close(): Promise<void> {
    this.running = false
    await Promise.allSettled(this.listener_task ? [this.listener_task] : [])
    this.listener_task = null
    this.inbound_bus.destroy()
  }

  private ensureStarted(): void {
    if (this.running || this.listener_task) return
    void this.start().catch((error: unknown) => {
      console.error('[abxbus] JSONLEventBridge failed to start', error)
    })
  }

  private async listenLoop(): Promise<void> {
    while (this.running) {
      try {
        await this.pollNewLines()
      } catch {
        // Keep polling on transient errors.
      }
      await new Promise((resolve) => setTimeout(resolve, Math.max(1, this.poll_interval * 1000)))
    }
  }

  private async pollNewLines(): Promise<void> {
    const previous_offset = this.byte_offset
    const { chunk, next_offset } = await this.readAppended(previous_offset)
    this.byte_offset = next_offset
    if (next_offset < previous_offset) {
      this.pending_line = ''
    }
    if (!chunk) return

    const new_lines = (this.pending_line + chunk).split('\n')
    this.pending_line = new_lines.pop() ?? ''

    for (const line of new_lines) {
      const trimmed = line.trim()
      if (!trimmed) continue
      try {
        const payload = JSON.parse(trimmed)
        await this.dispatchInboundPayload(payload)
      } catch {
        // Ignore malformed line.
      }
    }
  }

  private async dispatchInboundPayload(payload: unknown): Promise<void> {
    const event = BaseEvent.fromJSON(payload).eventReset()
    this.inbound_bus.emit(event)
  }

  private async readAppended(offset: number): Promise<{ chunk: string; next_offset: number }> {
    const fs = await this.loadFs()
    let size = 0
    try {
      const stats = await fs.promises.stat(this.path)
      size = Number(stats.size ?? 0)
    } catch (error: unknown) {
      const code = (error as { code?: string }).code
      if (code === 'ENOENT') {
        return { chunk: '', next_offset: 0 }
      }
      throw error
    }

    const start_offset = size < offset ? 0 : offset
    if (size === start_offset) {
      return { chunk: '', next_offset: size }
    }

    const handle = await fs.promises.open(this.path, 'r')
    try {
      const byte_count = size - start_offset
      const bytes = new Uint8Array(byte_count)
      const { bytesRead } = await handle.read(bytes, 0, byte_count, start_offset)
      const chunk = new TextDecoder().decode(bytes.subarray(0, Number(bytesRead ?? 0)))
      return { chunk, next_offset: start_offset + Number(bytesRead ?? 0) }
    } finally {
      await handle.close()
    }
  }

  private dirname(path: string): string {
    const idx = path.lastIndexOf('/')
    return idx >= 0 ? path.slice(0, idx) || '.' : '.'
  }

  private async loadFs(): Promise<any> {
    if (!isNodeRuntime()) {
      throw new Error('JSONLEventBridge is only supported in Node.js runtimes')
    }
    return importNodeModule('node:fs')
  }
}
