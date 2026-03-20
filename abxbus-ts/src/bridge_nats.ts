import { BaseEvent } from './base_event.js'
import { EventBus } from './event_bus.js'
import { assertOptionalDependencyAvailable, importOptionalDependency, isNodeRuntime } from './optional_deps.js'
import type { EventClass, EventHandlerCallable, EventPattern, UntypedEventHandlerFunction } from './types.js'

const randomSuffix = (): string => Math.random().toString(36).slice(2, 10)

export class NATSEventBridge {
  readonly server: string
  readonly subject: string
  readonly name: string

  private readonly inbound_bus: EventBus
  private running: boolean
  private nc: any | null
  private sub_task: Promise<void> | null

  constructor(server: string, subject: string, name?: string) {
    assertOptionalDependencyAvailable('NATSEventBridge', 'nats')

    this.server = server
    this.subject = subject
    this.name = name ?? `NATSEventBridge_${randomSuffix()}`
    this.inbound_bus = new EventBus(this.name, { max_history_size: 0 })
    this.running = false
    this.nc = null
    this.sub_task = null

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
    if (!this.nc) await this.start()

    const payload = JSON.stringify(event.toJSON())
    this.nc.publish(this.subject, new TextEncoder().encode(payload))
  }

  async dispatch<T extends BaseEvent>(event: T): Promise<void> {
    return this.emit(event)
  }

  async start(): Promise<void> {
    if (this.running) return
    if (!isNodeRuntime()) {
      throw new Error('NATSEventBridge is only supported in Node.js runtimes')
    }

    const mod = await importOptionalDependency('NATSEventBridge', 'nats')
    const connect = mod.connect
    this.nc = await connect({ servers: this.server })
    const sub = this.nc.subscribe(this.subject)

    this.running = true
    this.sub_task = (async () => {
      for await (const msg of sub) {
        try {
          const payload = JSON.parse(new TextDecoder().decode(msg.data))
          await this.dispatchInboundPayload(payload)
        } catch {
          // Ignore malformed payloads.
        }
      }
    })()
  }

  async close(): Promise<void> {
    this.running = false
    if (this.nc) {
      await this.nc.drain()
      await this.nc.close()
      this.nc = null
    }
    await Promise.allSettled(this.sub_task ? [this.sub_task] : [])
    this.sub_task = null
    this.inbound_bus.destroy()
  }

  private ensureStarted(): void {
    if (this.running) return
    void this.start().catch((error: unknown) => {
      console.error('[abxbus] NATSEventBridge failed to start', error)
    })
  }

  private async dispatchInboundPayload(payload: unknown): Promise<void> {
    const event = BaseEvent.fromJSON(payload).eventReset()
    this.inbound_bus.emit(event)
  }
}
