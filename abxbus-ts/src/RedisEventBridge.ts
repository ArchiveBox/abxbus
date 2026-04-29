/**
 * Redis pub/sub bridge for forwarding events between runtimes.
 *
 * Usage:
 *   // channel from URL path
 *   const bridge = new RedisEventBridge('redis://user:pass@localhost:6379/1/my_channel')
 *
 *   // explicit channel override
 *   const bridge2 = new RedisEventBridge('redis://user:pass@localhost:6379/1', 'my_channel')
 *
 * URL format:
 *   redis://user:pass@host:6379/<db>/<optional_channel>
 */
import { BaseEvent } from './BaseEvent.js'
import { EventBus } from './EventBus.js'
import { assertOptionalDependencyAvailable, importOptionalDependency, isNodeRuntime } from './optional_deps.js'
import type { EventClass, EventHandlerCallable, EventPattern, UntypedEventHandlerFunction } from './types.js'

const randomSuffix = (): string => Math.random().toString(36).slice(2, 10)
const DEFAULT_REDIS_CHANNEL = 'abxbus_events'
const DB_INIT_KEY = '__abxbus:bridge_init__'

const parseRedisUrl = (redis_url: string, channel?: string): { url: string; channel: string } => {
  let parsed: URL
  try {
    parsed = new URL(redis_url)
  } catch {
    throw new Error(`RedisEventBridge URL must be a valid redis:// or rediss:// URL, got: ${redis_url}`)
  }

  const protocol = parsed.protocol.replace(/:$/, '').toLowerCase()
  if (protocol !== 'redis' && protocol !== 'rediss') {
    throw new Error(`RedisEventBridge URL must use redis:// or rediss://, got: ${redis_url}`)
  }

  const segments = parsed.pathname.split('/').filter(Boolean)
  if (segments.length > 2) {
    throw new Error(`RedisEventBridge URL path must be /<db> or /<db>/<channel>, got: ${parsed.pathname || '/'}`)
  }

  let db_index = '0'
  let channel_from_url: string | undefined

  if (segments.length > 0) {
    db_index = segments[0]
    if (!/^\d+$/.test(db_index)) {
      throw new Error(`RedisEventBridge URL db path segment must be numeric, got: ${JSON.stringify(db_index)} in ${redis_url}`)
    }
    if (segments.length === 2) {
      channel_from_url = segments[1]
    }
  }

  const resolved_channel = channel ?? channel_from_url ?? DEFAULT_REDIS_CHANNEL
  if (!resolved_channel) {
    throw new Error('RedisEventBridge channel must not be empty')
  }

  const normalized = new URL(parsed.toString())
  normalized.pathname = `/${db_index}`
  return { url: normalized.toString(), channel: resolved_channel }
}

export class RedisEventBridge {
  readonly url: string
  readonly channel: string
  readonly name: string

  private readonly inbound_bus: EventBus
  private running: boolean
  private start_promise: Promise<void> | null
  private redis_pub: any | null
  private redis_sub: any | null

  constructor(redis_url: string, channel?: string, name?: string) {
    assertOptionalDependencyAvailable('RedisEventBridge', 'ioredis')

    const parsed = parseRedisUrl(redis_url, channel)
    this.url = parsed.url
    this.channel = parsed.channel
    this.name = name ?? `RedisEventBridge_${randomSuffix()}`
    this.inbound_bus = new EventBus(this.name, { max_history_size: 0 })
    this.running = false
    this.start_promise = null
    this.redis_pub = null
    this.redis_sub = null

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
    if (!this.redis_pub) await this.start()
    const payload = JSON.stringify(event.toJSON())
    await this.redis_pub.publish(this.channel, payload)
  }

  async dispatch<T extends BaseEvent>(event: T): Promise<void> {
    return this.emit(event)
  }

  async start(): Promise<void> {
    if (this.running) return
    if (this.start_promise) {
      await this.start_promise
      return
    }

    // `on(...)` auto-start and explicit `await start()` can happen back-to-back; use one in-flight
    // startup promise so we do not leak extra Redis clients.
    this.start_promise = (async () => {
      if (!isNodeRuntime()) {
        throw new Error('RedisEventBridge is only supported in Node.js runtimes')
      }

      const mod = await importOptionalDependency('RedisEventBridge', 'ioredis')
      const Redis = mod.default ?? mod.Redis ?? mod
      const redis_pub = new Redis(this.url)
      const redis_sub = new Redis(this.url)

      redis_pub.on('error', () => {})
      redis_sub.on('error', () => {})

      // Redis logical DBs are created lazily; writing a short-lived key initializes/validates the selected DB.
      await redis_pub.set(DB_INIT_KEY, '1', 'EX', 60, 'NX')
      redis_sub.on('message', (channel_name: string, message: string) => {
        if (channel_name !== this.channel) return
        try {
          const payload = JSON.parse(message)
          void this.dispatchInboundPayload(payload)
        } catch {
          // Ignore malformed payloads.
        }
      })
      await redis_sub.subscribe(this.channel)
      this.redis_pub = redis_pub
      this.redis_sub = redis_sub
      this.running = true
    })()

    try {
      await this.start_promise
    } finally {
      this.start_promise = null
    }
  }

  async close(): Promise<void> {
    if (this.start_promise) {
      await this.start_promise.catch(() => {})
    }
    this.running = false
    if (this.redis_sub) {
      try {
        await this.redis_sub.unsubscribe(this.channel)
      } catch {
        // ignore
      }
      await this.redis_sub.quit()
      this.redis_sub = null
    }
    if (this.redis_pub) {
      await this.redis_pub.quit()
      this.redis_pub = null
    }
    this.inbound_bus.destroy()
  }

  private ensureStarted(): void {
    if (this.running) return
    if (this.start_promise) return
    void this.start().catch((error: unknown) => {
      console.error('[abxbus] RedisEventBridge failed to start', error)
    })
  }

  private async dispatchInboundPayload(payload: unknown): Promise<void> {
    const event = BaseEvent.fromJSON(payload).eventReset()
    this.inbound_bus.emit(event)
  }
}
