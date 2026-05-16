/**
 * PostgreSQL LISTEN/NOTIFY + flat-table bridge for forwarding events.
 */
import { BaseEvent } from './BaseEvent.js'
import { EventBus } from './EventBus.js'
import { assertOptionalDependencyAvailable, importOptionalDependency, isNodeRuntime } from './optional_deps.js'
import type { EventClass, EventHandlerCallable, EventPattern, UntypedEventHandlerFunction } from './types.js'

const randomSuffix = (): string => Math.random().toString(36).slice(2, 10)
const IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]*$/
const DEFAULT_POSTGRES_TABLE = 'abxbus_events'
const DEFAULT_POSTGRES_CHANNEL = 'abxbus_events'
const EVENT_PAYLOAD_COLUMN = 'event_payload'

const validateIdentifier = (value: string, label: string): string => {
  if (!IDENTIFIER_RE.test(value)) {
    throw new Error(`Invalid ${label}: ${JSON.stringify(value)}. Use only [A-Za-z0-9_] and start with a letter/_`)
  }
  return value
}

const indexName = (table: string, suffix: string): string => validateIdentifier(`${table}_${suffix}`.slice(0, 63), 'index name')

const parseTableUrl = (table_url: string): { dsn: string; table: string } => {
  let parsed: URL
  try {
    parsed = new URL(table_url)
  } catch {
    throw new Error(
      'PostgresEventBridge URL must include at least database in path, e.g. postgresql://user:pass@host:5432/dbname[/tablename]'
    )
  }

  const segments = parsed.pathname.split('/').filter(Boolean)
  if (segments.length < 1) {
    throw new Error(
      'PostgresEventBridge URL must include at least database in path, e.g. postgresql://user:pass@host:5432/dbname[/tablename]'
    )
  }

  const db_name = segments[0]
  const table = segments.length >= 2 ? validateIdentifier(segments[1], 'table name') : DEFAULT_POSTGRES_TABLE
  const dsn_url = new URL(parsed.toString())
  dsn_url.pathname = `/${db_name}`
  return { dsn: dsn_url.toString(), table }
}

const splitBridgePayload = (
  payload: Record<string, unknown>
): { event_fields: Record<string, unknown>; event_payload: Record<string, unknown> } => {
  const event_fields: Record<string, unknown> = {}
  const event_payload: Record<string, unknown> = { ...payload }
  for (const [key, value] of Object.entries(payload)) {
    if (key.startsWith('event_')) {
      event_fields[key] = value
    }
  }
  return { event_fields, event_payload }
}

export class PostgresEventBridge {
  readonly table_url: string
  readonly dsn: string
  readonly table: string
  readonly channel: string
  readonly name: string

  private readonly inbound_bus: EventBus
  private running: boolean
  private client: any | null
  private table_columns: Set<string>
  private notification_handler: ((msg: { channel: string; payload?: string }) => void) | null

  constructor(table_url: string, channel?: string, name?: string) {
    assertOptionalDependencyAvailable('PostgresEventBridge', 'pg')

    const parsed = parseTableUrl(table_url)
    this.table_url = table_url
    this.dsn = parsed.dsn
    this.table = parsed.table

    const derived_channel = channel ?? DEFAULT_POSTGRES_CHANNEL
    this.channel = validateIdentifier(derived_channel.slice(0, 63), 'channel name')
    this.name = name ?? `PostgresEventBridge_${randomSuffix()}`

    this.inbound_bus = new EventBus(this.name, { max_history_size: 100, max_history_drop: true })
    this.running = false
    this.client = null
    this.table_columns = new Set(['event_id', 'event_created_at', 'event_type', EVENT_PAYLOAD_COLUMN])
    this.notification_handler = null

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
    if (!this.client) await this.start()

    const payload = event.toJSON() as Record<string, unknown>
    const { event_fields, event_payload } = splitBridgePayload(payload)
    const write_payload: Record<string, unknown> = { ...event_fields, [EVENT_PAYLOAD_COLUMN]: event_payload }
    const keys = Object.keys(write_payload).sort()
    await this.ensureColumns(keys)

    const columns_sql = keys.map((key) => `"${key}"`).join(', ')
    const placeholders_sql = keys.map((_, index) => `$${index + 1}`).join(', ')
    const values = keys.map((key) =>
      write_payload[key] === null || write_payload[key] === undefined ? null : JSON.stringify(write_payload[key])
    )

    const update_fields = keys.filter((key) => key !== 'event_id')
    let upsert_sql = `INSERT INTO "${this.table}" (${columns_sql}) VALUES (${placeholders_sql})`
    if (update_fields.length > 0) {
      const updates_sql = update_fields.map((key) => `"${key}" = EXCLUDED."${key}"`).join(', ')
      upsert_sql += ` ON CONFLICT ("event_id") DO UPDATE SET ${updates_sql}`
    } else {
      upsert_sql += ' ON CONFLICT ("event_id") DO NOTHING'
    }

    await this.client.query(upsert_sql, values)
    await this.client.query('SELECT pg_notify($1, $2)', [this.channel, JSON.stringify(String(event.event_id))])
  }

  async dispatch<T extends BaseEvent>(event: T): Promise<void> {
    return this.emit(event)
  }

  async start(): Promise<void> {
    if (this.running) return
    if (!isNodeRuntime()) {
      throw new Error('PostgresEventBridge is only supported in Node.js runtimes')
    }

    const mod = await importOptionalDependency('PostgresEventBridge', 'pg')
    const Client = mod.Client ?? mod.default?.Client
    this.client = new Client({ connectionString: this.dsn })
    this.client.on('error', () => {})
    await this.client.connect()

    await this.ensureTableExists()
    await this.refreshColumnCache()
    await this.ensureColumns(['event_id', 'event_created_at', 'event_type', EVENT_PAYLOAD_COLUMN])
    await this.ensureBaseIndexes()

    this.notification_handler = (msg: { channel: string; payload?: string }) => {
      if (msg.channel !== this.channel || !msg.payload) return
      void this.dispatchByEventId(msg.payload).catch(() => {
        // Ignore transient shutdown races while closing connections.
      })
    }

    this.client.on('notification', this.notification_handler)
    await this.client.query(`LISTEN ${this.channel}`)
    this.running = true
  }

  async close(): Promise<void> {
    this.running = false
    if (this.client) {
      try {
        await this.client.query(`UNLISTEN ${this.channel}`)
      } catch {
        // ignore
      }
      if (this.notification_handler) {
        this.client.off('notification', this.notification_handler)
        this.notification_handler = null
      }
      await this.client.end()
      this.client = null
    }
    this.inbound_bus.destroy()
  }

  private ensureStarted(): void {
    if (this.running) return
    void this.start().catch((error: unknown) => {
      console.error('[abxbus] PostgresEventBridge failed to start', error)
    })
  }

  private async dispatchByEventId(event_id: string): Promise<void> {
    if (!this.running || !this.client) return
    const result = await this.client.query(`SELECT * FROM "${this.table}" WHERE "event_id" = $1`, [event_id])
    const row = result.rows?.[0] as Record<string, unknown> | undefined
    if (!row) return

    const payload: Record<string, unknown> = {}
    const raw_event_payload = row[EVENT_PAYLOAD_COLUMN]
    if (typeof raw_event_payload === 'string') {
      try {
        const decoded_event_payload = JSON.parse(raw_event_payload)
        if (decoded_event_payload && typeof decoded_event_payload === 'object' && !Array.isArray(decoded_event_payload)) {
          Object.assign(payload, decoded_event_payload as Record<string, unknown>)
        }
      } catch {
        // ignore malformed payload column
      }
    }

    for (const [key, raw_value] of Object.entries(row)) {
      if (key === EVENT_PAYLOAD_COLUMN || !key.startsWith('event_')) continue
      if (raw_value === null || raw_value === undefined) continue
      if (typeof raw_value !== 'string') {
        payload[key] = raw_value
        continue
      }
      try {
        payload[key] = JSON.parse(raw_value)
      } catch {
        payload[key] = raw_value
      }
    }

    await this.dispatchInboundPayload(payload)
  }

  private async dispatchInboundPayload(payload: unknown): Promise<void> {
    const event = BaseEvent.fromJSON(payload).eventReset()
    this.inbound_bus.emit(event)
  }

  private async ensureTableExists(): Promise<void> {
    if (!this.client) return
    await this.client.query(
      `CREATE TABLE IF NOT EXISTS "${this.table}" ("event_id" TEXT PRIMARY KEY, "event_created_at" TEXT, "event_type" TEXT, "event_payload" TEXT)`
    )
  }

  private async ensureBaseIndexes(): Promise<void> {
    if (!this.client) return

    const event_created_at_idx = indexName(this.table, 'event_created_at_idx')
    const event_type_idx = indexName(this.table, 'event_type_idx')

    await this.client.query(`CREATE INDEX IF NOT EXISTS "${event_created_at_idx}" ON "${this.table}" ("event_created_at")`)
    await this.client.query(`CREATE INDEX IF NOT EXISTS "${event_type_idx}" ON "${this.table}" ("event_type")`)
  }

  private async refreshColumnCache(): Promise<void> {
    if (!this.client) return
    const result = await this.client.query(
      `SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1`,
      [this.table]
    )
    this.table_columns = new Set((result.rows as Array<{ column_name: string }>).map((row) => row.column_name))
  }

  private async ensureColumns(keys: string[]): Promise<void> {
    if (!this.client) return
    for (const key of keys) {
      validateIdentifier(key, 'event field name')
      if (key !== EVENT_PAYLOAD_COLUMN && !key.startsWith('event_')) {
        throw new Error(`Invalid event field name for bridge column: ${JSON.stringify(key)}. Only event_* fields become columns`)
      }
    }

    const missing = keys.filter((key) => !this.table_columns.has(key))
    for (const key of missing) {
      await this.client.query(`ALTER TABLE "${this.table}" ADD COLUMN IF NOT EXISTS "${key}" TEXT`)
      this.table_columns.add(key)
    }
  }
}
