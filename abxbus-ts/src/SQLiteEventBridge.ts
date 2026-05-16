import { BaseEvent } from './BaseEvent.js'
import { EventBus } from './EventBus.js'
import { isNodeRuntime } from './optional_deps.js'
import type { EventClass, EventHandlerCallable, EventPattern, UntypedEventHandlerFunction } from './types.js'

const randomSuffix = (): string => Math.random().toString(36).slice(2, 10)
const IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]*$/
const EVENT_PAYLOAD_COLUMN = 'event_payload'

const validateIdentifier = (value: string, label: string): string => {
  if (!IDENTIFIER_RE.test(value)) {
    throw new Error(`Invalid ${label}: ${JSON.stringify(value)}. Use only [A-Za-z0-9_] and start with a letter/_`)
  }
  return value
}

const loadNodeSqlite = async (): Promise<any> => {
  const dynamic_import = Function('module_name', 'return import(module_name)') as (module_name: string) => Promise<unknown>
  try {
    return (await dynamic_import('node:sqlite')) as any
  } catch {
    throw new Error('SQLiteEventBridge requires Node.js with built-in "node:sqlite" support (Node 22+).')
  }
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

export class SQLiteEventBridge {
  readonly path: string
  readonly table: string
  readonly poll_interval: number
  readonly name: string

  private inbound_bus: EventBus | null
  private running: boolean
  private last_seen_event_created_at: string
  private last_seen_event_id: string
  private listener_task: Promise<void> | null
  private start_task: Promise<void> | null
  private db: any | null
  private table_columns: Set<string>

  constructor(path: string, table: string = 'abxbus_events', poll_interval: number = 0.25, name?: string) {
    this.path = path
    this.table = validateIdentifier(table, 'table name')
    this.poll_interval = poll_interval
    this.name = name ?? `SQLiteEventBridge_${randomSuffix()}`
    this.inbound_bus = null
    this.running = false
    this.last_seen_event_created_at = ''
    this.last_seen_event_id = ''
    this.listener_task = null
    this.start_task = null
    this.db = null
    this.table_columns = new Set(['event_id', 'event_created_at', 'event_type', EVENT_PAYLOAD_COLUMN])

    this.dispatch = this.dispatch.bind(this)
    this.emit = this.emit.bind(this)
    this.on = this.on.bind(this)
  }

  on<T extends BaseEvent>(event_pattern: EventClass<T>, handler: EventHandlerCallable<T>): void
  on<T extends BaseEvent>(event_pattern: string | '*', handler: UntypedEventHandlerFunction<T>): void
  on(event_pattern: EventPattern | '*', handler: EventHandlerCallable | UntypedEventHandlerFunction): void {
    this.ensureStarted()
    const inbound_bus = this.getInboundBus()
    if (typeof event_pattern === 'string') {
      inbound_bus.on(event_pattern, handler as UntypedEventHandlerFunction<BaseEvent>)
      return
    }
    inbound_bus.on(event_pattern as EventClass<BaseEvent>, handler as EventHandlerCallable<BaseEvent>)
  }

  async emit<T extends BaseEvent>(event: T): Promise<void> {
    await this.initializeDatabase()

    const payload = event.toJSON() as Record<string, unknown>
    const { event_fields, event_payload } = splitBridgePayload(payload)
    const write_payload: Record<string, unknown> = { ...event_fields, [EVENT_PAYLOAD_COLUMN]: event_payload }
    const payload_keys = Object.keys(write_payload).sort()
    this.ensureColumns(payload_keys)

    const columns_sql = payload_keys.map((key) => `"${key}"`).join(', ')
    const placeholders_sql = payload_keys.map((key) => (key === EVENT_PAYLOAD_COLUMN ? 'json(?)' : '?')).join(', ')
    const values = payload_keys.map((key) =>
      write_payload[key] === null || write_payload[key] === undefined ? null : JSON.stringify(write_payload[key])
    )

    const update_fields = payload_keys.filter((key) => key !== 'event_id')
    let upsert_sql = `INSERT INTO "${this.table}" (${columns_sql}) VALUES (${placeholders_sql})`
    if (update_fields.length > 0) {
      const updates_sql = update_fields.map((key) => `"${key}" = excluded."${key}"`).join(', ')
      upsert_sql += ` ON CONFLICT("event_id") DO UPDATE SET ${updates_sql}`
    } else {
      upsert_sql += ' ON CONFLICT("event_id") DO NOTHING'
    }

    this.db.prepare(upsert_sql).run(...values)
  }

  async dispatch<T extends BaseEvent>(event: T): Promise<void> {
    return this.emit(event)
  }

  async start(): Promise<void> {
    if (this.running) return
    if (this.start_task) {
      await this.start_task
      return
    }

    this.start_task = (async (): Promise<void> => {
      await this.initializeDatabase()
      this.setCursorToLatestRow()

      this.running = true
      this.listener_task = this.listenLoop()
    })()

    try {
      await this.start_task
    } finally {
      this.start_task = null
    }
  }

  async close(): Promise<void> {
    await Promise.allSettled(this.start_task ? [this.start_task] : [])
    this.running = false
    await Promise.allSettled(this.listener_task ? [this.listener_task] : [])
    this.listener_task = null

    if (this.db) {
      this.db.close()
      this.db = null
    }

    this.inbound_bus?.destroy()
    this.inbound_bus = null
  }

  private ensureStarted(): void {
    if (this.running || this.listener_task || this.start_task) return
    void this.start().catch((error: unknown) => {
      console.error('[abxbus] SQLiteEventBridge failed to start', error)
    })
  }

  private async listenLoop(): Promise<void> {
    while (this.running) {
      try {
        if (this.db) {
          const rows = this.db
            .prepare(
              `SELECT * FROM "${this.table}" WHERE COALESCE("event_created_at", '') > ? OR (COALESCE("event_created_at", '') = ? AND COALESCE("event_id", '') > ?) ORDER BY COALESCE("event_created_at", '') ASC, COALESCE("event_id", '') ASC`
            )
            .all(this.last_seen_event_created_at, this.last_seen_event_created_at, this.last_seen_event_id) as Array<
            Record<string, unknown>
          >

          for (const row of rows) {
            this.last_seen_event_created_at = String(row.event_created_at ?? '')
            this.last_seen_event_id = String(row.event_id ?? '')

            const raw_payload_blob = row[EVENT_PAYLOAD_COLUMN]
            const payload: Record<string, unknown> = {}
            if (typeof raw_payload_blob === 'string') {
              try {
                const decoded_event_payload = JSON.parse(raw_payload_blob)
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
        }
      } catch {
        // Keep polling on transient errors.
      }
      await new Promise((resolve) => setTimeout(resolve, Math.max(1, this.poll_interval * 1000)))
    }
  }

  private async dispatchInboundPayload(payload: unknown): Promise<void> {
    if (!this.inbound_bus) {
      return
    }
    const event = BaseEvent.fromJSON(payload).eventReset()
    this.inbound_bus.emit(event)
  }

  private getInboundBus(): EventBus {
    if (!this.inbound_bus) {
      this.inbound_bus = new EventBus(this.name, { max_history_size: 100, max_history_drop: true })
    }
    return this.inbound_bus
  }

  private async initializeDatabase(): Promise<void> {
    if (this.db) return
    if (!isNodeRuntime()) {
      throw new Error('SQLiteEventBridge is only supported in Node.js runtimes')
    }

    const mod = await loadNodeSqlite()
    const Database = mod.DatabaseSync ?? mod.default?.DatabaseSync
    if (typeof Database !== 'function') {
      throw new Error('SQLiteEventBridge could not load DatabaseSync from node:sqlite. Please use Node.js 22+.')
    }
    this.db = new Database(this.path)
    this.db.exec('PRAGMA journal_mode = WAL')
    this.db
      .prepare(
        `CREATE TABLE IF NOT EXISTS "${this.table}" ("event_id" TEXT PRIMARY KEY, "event_created_at" TEXT, "event_type" TEXT, "event_payload" JSON)`
      )
      .run()

    this.refreshColumnCache()
    this.ensureColumns(['event_id', 'event_created_at', 'event_type', EVENT_PAYLOAD_COLUMN])
    this.ensureBaseIndexes()
  }

  private refreshColumnCache(): void {
    if (!this.db) return
    const rows = this.db.prepare(`PRAGMA table_info("${this.table}")`).all() as Array<{ name: string }>
    this.table_columns = new Set(rows.map((row) => String(row.name)))
  }

  private ensureColumns(keys: string[]): void {
    if (!this.db) return

    for (const key of keys) {
      validateIdentifier(key, 'event field name')
      if (key !== EVENT_PAYLOAD_COLUMN && !key.startsWith('event_')) {
        throw new Error(`Invalid event field name for bridge column: ${JSON.stringify(key)}. Only event_* fields become columns`)
      }
    }

    const missing_columns = keys.filter((key) => !this.table_columns.has(key))
    for (const key of missing_columns) {
      const column_type = key === EVENT_PAYLOAD_COLUMN ? 'JSON' : 'TEXT'
      this.db.prepare(`ALTER TABLE "${this.table}" ADD COLUMN "${key}" ${column_type}`).run()
      this.table_columns.add(key)
    }
  }

  private ensureBaseIndexes(): void {
    if (!this.db) return

    const event_created_at_index = `${this.table}_event_created_at_idx`
    const event_type_index = `${this.table}_event_type_idx`

    this.db.prepare(`CREATE INDEX IF NOT EXISTS "${event_created_at_index}" ON "${this.table}" ("event_created_at")`).run()
    this.db.prepare(`CREATE INDEX IF NOT EXISTS "${event_type_index}" ON "${this.table}" ("event_type")`).run()
  }

  private setCursorToLatestRow(): void {
    if (!this.db) return

    const row = this.db
      .prepare(
        `SELECT COALESCE("event_created_at", '') AS event_created_at, COALESCE("event_id", '') AS event_id FROM "${this.table}" ORDER BY COALESCE("event_created_at", '') DESC, COALESCE("event_id", '') DESC LIMIT 1`
      )
      .get() as { event_created_at?: string; event_id?: string } | undefined

    this.last_seen_event_created_at = String(row?.event_created_at ?? '')
    this.last_seen_event_id = String(row?.event_id ?? '')
  }
}
