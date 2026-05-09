import { spawn, type ChildProcess } from 'node:child_process'
import { createHash, randomUUID } from 'node:crypto'
import { existsSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import { decode, encode } from '@msgpack/msgpack'
import { RpcBus } from '@tachyon-ipc/core'

export const CORE_PROTOCOL_VERSION = 1
const REQUEST_TYPE_ID = 1
const FAST_HANDLER_COMPLETED_TYPE_ID = 3
const FAST_ACK_RESPONSE_TYPE_ID = 4
const FAST_ERROR_RESPONSE_TYPE_ID = 5
const FAST_MESSAGES_RESPONSE_TYPE_ID = 6
const FAST_QUEUE_JUMP_TYPE_ID = 7
const FAST_REGISTER_HANDLER_TYPE_ID = 8
const FAST_UNREGISTER_HANDLER_TYPE_ID = 9
const SPIN_THRESHOLD = 50_000_000
const POLLING_MODE = 1
const HANDLER_OUTCOME_BATCH_LIMIT = 4096
const CORE_STARTUP_TIMEOUT_MS = 120_000
const DEFAULT_TS_HOST_ID = `ts-${randomUUID()}`
const DEFAULT_CORE_NAMESPACE = `process-${process.pid}`
const DEBUG_CORE_REQUESTS = typeof process !== 'undefined' && process.env.ABXBUS_DEBUG_CORE_REQUESTS === '1'
const EMPTY_BUFFER = Buffer.alloc(0)
const U32_SIZE = 0x1_0000_0000
let DEBUG_CORE_REQUEST_SEQ = 0

const writeUint64LEFromSafeInteger = (payload: Buffer, value: number, offset: number): void => {
  payload.writeUInt32LE(value >>> 0, offset)
  payload.writeUInt32LE(Math.floor(value / U32_SIZE), offset + 4)
}

export type ProtocolEnvelope<TMessage = Record<string, unknown>> = {
  protocol_version: number
  session_id: string
  request_id?: string
  last_patch_seq?: number
  message: TMessage
}

export type CoreMessage = Record<string, unknown> & { type?: string }

type FastCompletedHandlerOptions = {
  result_is_event_reference?: boolean
  process_route_after?: boolean
  process_available_after?: boolean
  compact_response?: boolean
  include_patches?: boolean
}

type ForwardEventOptions = {
  parent_invocation_id?: string | null
  block_parent_completion?: boolean
  pause_parent_route?: boolean
  event_timeout?: number | null
  event_slow_timeout?: number | null
  event_concurrency?: string | null
  event_handler_timeout?: number | null
  event_handler_slow_timeout?: number | null
  event_handler_concurrency?: string | null
  event_handler_completion?: string | null
  event_blocks_parent_completion?: boolean | null
}

export class RustCoreClient {
  private static shared_named_clients = new Map<
    string,
    { client: RustCoreClient; refs: number; stop_timer: ReturnType<typeof setTimeout> | null }
  >()
  private static cleanup_registered = false

  static acquireNamed(bus_name: string): RustCoreClient {
    RustCoreClient.registerProcessCleanup()
    const key = stableCoreSocketPath(bus_name)
    const existing = RustCoreClient.shared_named_clients.get(key)
    if (existing) {
      if (existing.stop_timer) {
        clearTimeout(existing.stop_timer)
        existing.stop_timer = null
      }
      existing.refs += 1
      return existing.client
    }
    const client = new RustCoreClient({ bus_name })
    RustCoreClient.shared_named_clients.set(key, { client, refs: 1, stop_timer: null })
    return client
  }

  static releaseNamed(client: RustCoreClient, options: { stopCore?: boolean } = {}): void {
    const existing = RustCoreClient.shared_named_clients.get(client.socket_path)
    if (!existing || existing.client !== client) {
      if (options.stopCore === true) {
        client.stop()
      } else {
        client.disconnectHost()
        client.disconnect()
      }
      return
    }
    if (options.stopCore === true) {
      RustCoreClient.shared_named_clients.delete(client.socket_path)
      client.stop()
      return
    }
    if (existing.refs > 0) {
      existing.refs -= 1
    }
    if (existing.refs === 0) {
      if (!existing.stop_timer) {
        existing.stop_timer = setTimeout(() => {
          existing.stop_timer = null
          if (existing.refs !== 0) {
            return
          }
          RustCoreClient.shared_named_clients.delete(client.socket_path)
          client.stop()
        }, 250)
        existing.stop_timer.unref?.()
      }
    }
  }

  private static registerProcessCleanup(): void {
    if (RustCoreClient.cleanup_registered) {
      return
    }
    RustCoreClient.cleanup_registered = true
    process.once('exit', () => {
      for (const { client } of RustCoreClient.shared_named_clients.values()) {
        try {
          if (process.env.ABXBUS_CORE_NAMESPACE) {
            client.disconnectHost()
            client.disconnect()
          } else {
            client.stop()
          }
        } catch {
          // Process exit cleanup cannot surface late transport errors.
        }
      }
      RustCoreClient.shared_named_clients.clear()
    })
  }

  static stopIdleNamedClients(): void {
    for (const [key, entry] of RustCoreClient.shared_named_clients) {
      if (entry.refs > 0) {
        continue
      }
      if (entry.stop_timer) {
        clearTimeout(entry.stop_timer)
        entry.stop_timer = null
      }
      RustCoreClient.shared_named_clients.delete(key)
      entry.client.stop()
    }
  }

  readonly session_id: string
  readonly command: string
  readonly args: string[]
  readonly socket_path: string

  private process: ChildProcess | null
  private rpc: RpcBus | null
  private last_patch_seq: number
  private request_seq: number
  private release_transport_timer: ReturnType<typeof setTimeout> | null
  private readonly kill_process_on_close: boolean
  private readonly bus_name: string | null
  private readonly session_id_bytes: Buffer

  constructor(options: { command?: string; args?: string[]; session_id?: string; socket_path?: string; bus_name?: string } = {}) {
    this.session_id = options.session_id ?? DEFAULT_TS_HOST_ID
    this.session_id_bytes = Buffer.from(this.session_id)
    this.socket_path =
      options.socket_path ??
      (options.bus_name ? stableCoreSocketPath(options.bus_name) : resolve(tmpdir(), `abxbus-core-${randomUUID()}.sock`))
    this.bus_name = options.bus_name ?? null
    this.last_patch_seq = 0
    this.request_seq = 0
    this.release_transport_timer = null
    const default_command = defaultCoreCommand(this.socket_path, { daemon: options.bus_name !== undefined })
    this.command = options.command ?? default_command.command
    this.args = options.args ?? default_command.args
    this.kill_process_on_close = options.bus_name === undefined
    if (options.bus_name) {
      this.process = null
      this.rpc = this.connectNamedBus(options.bus_name)
      return
    }
    const existing = options.socket_path ? this.tryConnect(this.socket_path) : null
    if (existing) {
      this.process = null
      this.rpc = existing
    } else {
      this.process = spawn(this.command, this.args, {
        stdio: ['ignore', 'ignore', 'pipe'],
        env: this.coreProcessEnv({ ownerPid: true }),
      })
      this.rpc = this.connect(this.socket_path)
    }
  }

  request(
    message: CoreMessage,
    options: { includePatches?: boolean; advancePatchSeq?: boolean; advancePatchSeqWhenNoPatches?: boolean } = {}
  ): ProtocolEnvelope<CoreMessage>[] {
    return this.requestOnce(message, options)
  }

  private requestOnce(
    message: CoreMessage,
    options: { includePatches?: boolean; advancePatchSeq?: boolean; advancePatchSeqWhenNoPatches?: boolean } = {}
  ): ProtocolEnvelope<CoreMessage>[] {
    const include_patches = options.includePatches ?? true
    const debug_request_id = DEBUG_CORE_REQUESTS ? ++DEBUG_CORE_REQUEST_SEQ : 0
    if (DEBUG_CORE_REQUESTS) {
      console.error(
        `[abxbus-core-request:start] #${debug_request_id} ${String(message.type)} bus=${this.bus_name ?? '-'} session=${this.session_id.slice(-8)} patch=${this.last_patch_seq}`
      )
    }
    const envelope: ProtocolEnvelope<CoreMessage> = {
      protocol_version: CORE_PROTOCOL_VERSION,
      session_id: this.session_id,
      message,
    }
    if (include_patches) {
      envelope.last_patch_seq = this.last_patch_seq
    }
    const rpc = this.ensureRpc()
    const cid = rpc.call(Buffer.from(encode(envelope)), REQUEST_TYPE_ID)
    let response = rpc.wait(cid, SPIN_THRESHOLD)
    while (response === null) {
      response = rpc.wait(cid, SPIN_THRESHOLD)
    }
    try {
      const envelopes = decode(response.data()) as ProtocolEnvelope<CoreMessage>[]
      if (!include_patches && options.advancePatchSeq === true) {
        for (const envelope of envelopes) {
          if (typeof envelope.last_patch_seq === 'number') {
            this.setPatchSeq(envelope.last_patch_seq)
          }
        }
      }
      if (include_patches && options.advancePatchSeqWhenNoPatches === true) {
        const has_patch = envelopes.some((envelope) => envelope.message?.type === 'patch')
        if (!has_patch) {
          for (const envelope of envelopes) {
            if (typeof envelope.last_patch_seq === 'number') {
              this.setPatchSeq(envelope.last_patch_seq)
            }
          }
        }
      }
      if (DEBUG_CORE_REQUESTS) {
        console.error(`[abxbus-core-request:done] #${debug_request_id} ${String(message.type)} responses=${envelopes.length}`)
      }
      return envelopes
    } finally {
      try {
        response.commit()
      } finally {
        this.releaseTransportSoon()
      }
    }
  }

  requestMessages(
    message: CoreMessage,
    options: { includePatches?: boolean; advancePatchSeq?: boolean; advancePatchSeqWhenNoPatches?: boolean } = {}
  ): CoreMessage[] {
    const responses = this.request(message, options)
    const messages: CoreMessage[] = []
    for (const response of responses) {
      const response_message = response.message
      if (response_message === undefined || response_message === null) {
        continue
      }
      if (response_message.type === 'error') {
        throw new Error(typeof response_message.message === 'string' ? response_message.message : 'Rust core request failed')
      }
      messages.push(response_message)
    }
    return messages
  }

  getPatchSeq(): number {
    return this.last_patch_seq
  }

  setPatchSeq(last_patch_seq: number): void {
    if (Number.isFinite(last_patch_seq) && last_patch_seq > this.last_patch_seq) {
      this.last_patch_seq = Math.floor(last_patch_seq)
    }
  }

  filterUnseenPatchMessages(messages: CoreMessage[]): CoreMessage[] {
    return messages.filter((message) => {
      const patch_seq = message.type === 'patch' && typeof message.patch_seq === 'number' ? message.patch_seq : null
      return patch_seq === null || patch_seq > this.last_patch_seq
    })
  }

  ackPatchMessages(messages: CoreMessage | CoreMessage[]): void {
    const list = Array.isArray(messages) ? messages : [messages]
    let max_patch_seq = this.last_patch_seq
    for (const message of list) {
      const patch_seq = message.type === 'patch' && typeof message.patch_seq === 'number' ? message.patch_seq : null
      if (patch_seq !== null && patch_seq > max_patch_seq) {
        max_patch_seq = patch_seq
      }
    }
    this.setPatchSeq(max_patch_seq)
  }

  registerBus(bus: Record<string, unknown>): CoreMessage[] {
    return this.requestMessages({ type: 'register_bus', bus }, { includePatches: false, advancePatchSeq: true })
  }

  unregisterBus(bus_id: string): CoreMessage[] {
    return this.requestMessages({ type: 'unregister_bus', bus_id }, { includePatches: false, advancePatchSeq: true })
  }

  registerHandler(handler: Record<string, unknown>): CoreMessage[] {
    return this.requestFastRegisterHandler(handler)
  }

  importBusSnapshot(snapshot: {
    bus: Record<string, unknown>
    handlers: Record<string, unknown>[]
    events: Record<string, unknown>[]
    pending_event_ids: string[]
  }): CoreMessage[] {
    return this.requestMessages({ type: 'import_bus_snapshot', ...snapshot }, { includePatches: false, advancePatchSeq: true })
  }

  unregisterHandler(handler_id: string): CoreMessage[] {
    return this.requestFastUnregisterHandler(handler_id)
  }

  disconnectHost(host_id: string | null = null): CoreMessage[] {
    return this.requestMessages(
      { type: 'disconnect_host', host_id: host_id ?? this.session_id },
      { includePatches: false, advancePatchSeq: true }
    )
  }

  closeSession(): void {
    this.requestOnce({ type: 'close_session' })
  }

  stopCore(): CoreMessage[] {
    if (this.bus_name) {
      this.requestNamedStop()
      return [{ type: 'core_stopped' }]
    }
    return this.requestMessages({ type: 'stop_core' }, { includePatches: false, advancePatchSeq: true })
  }

  emitEvent(
    event: Record<string, unknown>,
    bus_id: string,
    defer_start = true,
    compact_response = false,
    options: { parent_invocation_id?: string | null; block_parent_completion?: boolean; pause_parent_route?: boolean } = {}
  ): CoreMessage[] {
    const request: CoreMessage = { type: 'emit_event', event, bus_id, defer_start, compact_response }
    if (options.parent_invocation_id) {
      request.parent_invocation_id = options.parent_invocation_id
      request.block_parent_completion = options.block_parent_completion ?? false
      request.pause_parent_route = options.pause_parent_route ?? false
    }
    return this.requestMessages(request, { advancePatchSeqWhenNoPatches: compact_response })
  }

  forwardEvent(
    event_id: string,
    bus_id: string,
    defer_start = true,
    compact_response = false,
    options: ForwardEventOptions = {}
  ): CoreMessage[] {
    const request: CoreMessage = { type: 'forward_event', event_id, bus_id, defer_start, compact_response }
    if (options.parent_invocation_id) {
      request.parent_invocation_id = options.parent_invocation_id
      request.block_parent_completion = options.block_parent_completion ?? false
      request.pause_parent_route = options.pause_parent_route ?? false
    }
    for (const key of [
      'event_timeout',
      'event_slow_timeout',
      'event_concurrency',
      'event_handler_timeout',
      'event_handler_slow_timeout',
      'event_handler_concurrency',
      'event_handler_completion',
    ] as const) {
      const value = options[key]
      if (value !== undefined && value !== null) request[key] = value
    }
    if (options.event_blocks_parent_completion === true) {
      request.event_blocks_parent_completion = true
    }
    return this.requestMessages(request, { advancePatchSeqWhenNoPatches: compact_response })
  }

  updateEventOptions(
    event_id: string,
    options: { event_handler_completion?: string | null; event_blocks_parent_completion?: boolean | null }
  ): CoreMessage[] {
    return this.requestMessages({ type: 'update_event_options', event_id, ...options })
  }

  processNextRoute(bus_id: string, limit: number | null = null, compact_response = false): CoreMessage[] {
    return this.requestMessages(
      { type: 'process_next_route', bus_id, limit, compact_response },
      { advancePatchSeqWhenNoPatches: compact_response }
    )
  }

  waitInvocations(bus_id: string | null = null, limit: number | null = null): CoreMessage[] {
    return this.requestMessages({ type: 'wait_invocations', bus_id, limit })
  }

  waitEventCompleted(event_id: string): CoreMessage[] {
    return this.requestMessages({ type: 'wait_event_completed', event_id })
  }

  waitEventEmitted(
    bus_id: string,
    event_pattern = '*',
    seen_event_ids: string[] = [],
    after_event_id: string | null = null,
    after_created_at: string | null = null
  ): CoreMessage[] {
    return this.requestMessages({ type: 'wait_event_emitted', bus_id, event_pattern, seen_event_ids, after_event_id, after_created_at })
  }

  waitBusIdle(bus_id: string, timeout: number | null = null): boolean {
    const messages = this.requestMessages({ type: 'wait_bus_idle', bus_id, timeout }, { includePatches: false })
    return messages.some((message) => message.type === 'bus_idle')
  }

  processRoute(route_id: string, limit: number | null = null, compact_response = false): CoreMessage[] {
    return this.requestMessages(
      { type: 'process_route', route_id, limit, compact_response },
      { advancePatchSeqWhenNoPatches: compact_response }
    )
  }

  awaitEvent(event_id: string, parent_invocation_id: string | null = null): CoreMessage[] {
    return this.requestMessages({ type: 'await_event', event_id, parent_invocation_id })
  }

  queueJumpEvent(event_id: string, parent_invocation_id: string, block_parent_completion = true, pause_parent_route = true): CoreMessage[] {
    return this.requestFastQueueJumpEvent(event_id, parent_invocation_id, {
      block_parent_completion,
      pause_parent_route,
      compact_response: true,
      include_patches: true,
    })
  }

  getEvent(event_id: string): Record<string, unknown> | null {
    for (const message of this.requestMessages({ type: 'get_event', event_id }, { includePatches: false })) {
      if (message.type === 'event_snapshot' && typeof message.event === 'object' && message.event !== null) {
        return message.event as Record<string, unknown>
      }
    }
    return null
  }

  listEvents(event_pattern = '*', limit: number | null = null, bus_id: string | null = null): Record<string, unknown>[] {
    for (const message of this.requestMessages({ type: 'list_events', event_pattern, limit, bus_id }, { includePatches: false })) {
      if (message.type === 'event_list' && Array.isArray(message.events)) {
        return message.events.filter(
          (event): event is Record<string, unknown> => typeof event === 'object' && event !== null && !Array.isArray(event)
        )
      }
    }
    return []
  }

  listEventIds(event_pattern = '*', limit: number | null = null, bus_id: string | null = null, statuses: string[] | null = null): string[] {
    for (const message of this.requestMessages(
      { type: 'list_event_ids', event_pattern, limit, bus_id, statuses },
      { includePatches: false }
    )) {
      if (message.type === 'event_id_list' && Array.isArray(message.event_ids)) {
        return message.event_ids.filter((event_id): event_id is string => typeof event_id === 'string')
      }
    }
    return []
  }

  listPendingEventIds(bus_id: string): string[] {
    for (const message of this.requestMessages({ type: 'list_pending_event_ids', bus_id }, { includePatches: false })) {
      if (message.type === 'event_id_list' && Array.isArray(message.event_ids)) {
        return message.event_ids.filter((event_id): event_id is string => typeof event_id === 'string')
      }
    }
    return []
  }

  completeHandler(invocation: Record<string, unknown>, value: unknown, options: FastCompletedHandlerOptions = {}): CoreMessage[] {
    return this.requestFastCompletedHandler(invocation, value, {
      ...options,
      include_patches: true,
    })
  }

  completeHandlerNoPatches(invocation: Record<string, unknown>, value: unknown, options: FastCompletedHandlerOptions = {}): CoreMessage[] {
    return this.requestFastCompletedHandler(invocation, value, options)
  }

  private requestFastCompletedHandler(
    invocation: Record<string, unknown>,
    value: unknown,
    options: FastCompletedHandlerOptions = {}
  ): CoreMessage[] {
    const result_id = this.requiredString(invocation.result_id, 'result_id')
    const invocation_id = this.requiredString(invocation.invocation_id, 'invocation_id')
    const fence = this.requiredUint64(invocation.fence, 'fence')
    let flags = 0
    if (options.result_is_event_reference ?? false) flags |= 0b0000_0001
    if (value === undefined) flags |= 0b0000_0010
    if (options.process_route_after ?? false) flags |= 0b0000_0100
    if (options.process_available_after ?? false) flags |= 0b0000_1000
    if (options.compact_response ?? false) flags |= 0b0001_0000
    if (options.include_patches ?? false) flags |= 0b0010_0000
    const session_id = this.session_id_bytes
    const result_id_length = Buffer.byteLength(result_id)
    const invocation_id_length = Buffer.byteLength(invocation_id)
    const value_bytes = value === undefined || value === null ? EMPTY_BUFFER : Buffer.from(encode(value))
    const payload_size = 30 + session_id.length + result_id_length + invocation_id_length + value_bytes.length
    if (session_id.length > 0xffff || result_id_length > 0xffff || invocation_id_length > 0xffff) {
      throw new Error('fast handler completion id fields exceed 65535 bytes')
    }
    if (value_bytes.length > 0xffffffff) {
      throw new Error('fast handler completion value exceeds 4294967295 bytes')
    }
    const payload = Buffer.allocUnsafe(payload_size)
    let offset = 0
    payload.writeUInt16LE(CORE_PROTOCOL_VERSION, offset)
    offset += 2
    payload.writeUInt16LE(flags, offset)
    offset += 2
    writeUint64LEFromSafeInteger(payload, fence, offset)
    offset += 8
    writeUint64LEFromSafeInteger(payload, this.last_patch_seq, offset)
    offset += 8
    payload.writeUInt16LE(session_id.length, offset)
    offset += 2
    payload.writeUInt16LE(result_id_length, offset)
    offset += 2
    payload.writeUInt16LE(invocation_id_length, offset)
    offset += 2
    payload.writeUInt32LE(value_bytes.length, offset)
    offset += 4
    session_id.copy(payload, offset)
    offset += session_id.length
    payload.write(result_id, offset, result_id_length, 'utf8')
    offset += result_id_length
    payload.write(invocation_id, offset, invocation_id_length, 'utf8')
    offset += invocation_id_length
    value_bytes.copy(payload, offset)

    const rpc = this.ensureRpc()
    const cid = rpc.call(payload, FAST_HANDLER_COMPLETED_TYPE_ID)
    let response = rpc.wait(cid, SPIN_THRESHOLD)
    while (response === null) {
      response = rpc.wait(cid, SPIN_THRESHOLD)
    }
    try {
      const data = response.data()
      if (response.msgType === FAST_ACK_RESPONSE_TYPE_ID) {
        this.setPatchSeq(this.readFastPatchSeq(data))
        return []
      }
      if (response.msgType === FAST_ERROR_RESPONSE_TYPE_ID) {
        throw new Error(data.toString('utf8') || 'Rust core fast handler completion failed')
      }
      if (response.msgType === FAST_MESSAGES_RESPONSE_TYPE_ID) {
        const patch_seq = this.readFastPatchSeq(data.subarray(0, 8))
        const messages = decode(data.subarray(8)) as CoreMessage[]
        let has_patch = false
        for (const message of messages) {
          if (message.type === 'error') {
            throw new Error(typeof message.message === 'string' ? message.message : 'Rust core fast handler completion failed')
          }
          if (message.type === 'patch') {
            has_patch = true
          }
        }
        if (!has_patch) {
          this.setPatchSeq(patch_seq)
        }
        return messages
      }
      throw new Error(`unexpected Rust core fast response type: ${response.msgType}`)
    } finally {
      try {
        response.commit()
      } finally {
        this.releaseTransportSoon()
      }
    }
  }

  private requestFastRegisterHandler(handler: Record<string, unknown>): CoreMessage[] {
    const session_id = this.session_id_bytes
    const handler_bytes = Buffer.from(encode(handler))
    const payload_size = 16 + session_id.length + handler_bytes.length
    if (session_id.length > 0xffff) {
      throw new Error('fast register handler session id exceeds 65535 bytes')
    }
    if (handler_bytes.length > 0xffffffff) {
      throw new Error('fast register handler record exceeds 4294967295 bytes')
    }
    const payload = Buffer.allocUnsafe(payload_size)
    let offset = 0
    payload.writeUInt16LE(CORE_PROTOCOL_VERSION, offset)
    offset += 2
    writeUint64LEFromSafeInteger(payload, this.last_patch_seq, offset)
    offset += 8
    payload.writeUInt16LE(session_id.length, offset)
    offset += 2
    payload.writeUInt32LE(handler_bytes.length, offset)
    offset += 4
    session_id.copy(payload, offset)
    offset += session_id.length
    handler_bytes.copy(payload, offset)
    return this.waitFastCoreAck(payload, FAST_REGISTER_HANDLER_TYPE_ID, 'fast register handler')
  }

  private requestFastUnregisterHandler(handler_id: string): CoreMessage[] {
    const session_id = this.session_id_bytes
    const handler_id_length = Buffer.byteLength(handler_id)
    const payload_size = 14 + session_id.length + handler_id_length
    if (session_id.length > 0xffff || handler_id_length > 0xffff) {
      throw new Error('fast unregister handler id fields exceed 65535 bytes')
    }
    const payload = Buffer.allocUnsafe(payload_size)
    let offset = 0
    payload.writeUInt16LE(CORE_PROTOCOL_VERSION, offset)
    offset += 2
    writeUint64LEFromSafeInteger(payload, this.last_patch_seq, offset)
    offset += 8
    payload.writeUInt16LE(session_id.length, offset)
    offset += 2
    payload.writeUInt16LE(handler_id_length, offset)
    offset += 2
    session_id.copy(payload, offset)
    offset += session_id.length
    payload.write(handler_id, offset, handler_id_length, 'utf8')
    return this.waitFastCoreAck(payload, FAST_UNREGISTER_HANDLER_TYPE_ID, 'fast unregister handler')
  }

  private requestFastQueueJumpEvent(
    event_id: string,
    parent_invocation_id: string,
    options: {
      block_parent_completion?: boolean
      pause_parent_route?: boolean
      compact_response?: boolean
      include_patches?: boolean
    } = {}
  ): CoreMessage[] {
    if (typeof event_id !== 'string' || event_id.length === 0) {
      throw new Error('fast queue jump requires string event_id')
    }
    if (typeof parent_invocation_id !== 'string' || parent_invocation_id.length === 0) {
      throw new Error('fast queue jump requires string parent_invocation_id')
    }
    let flags = 0
    if (options.block_parent_completion ?? true) flags |= 0b0000_0001
    if (options.pause_parent_route ?? true) flags |= 0b0000_0010
    if (options.compact_response ?? false) flags |= 0b0000_0100
    if (options.include_patches ?? false) flags |= 0b0000_1000
    const session_id = this.session_id_bytes
    const parent_invocation_length = Buffer.byteLength(parent_invocation_id)
    const event_id_length = Buffer.byteLength(event_id)
    const payload_size = 18 + session_id.length + parent_invocation_length + event_id_length
    if (session_id.length > 0xffff || parent_invocation_length > 0xffff || event_id_length > 0xffff) {
      throw new Error('fast queue jump id fields exceed 65535 bytes')
    }
    const payload = Buffer.allocUnsafe(payload_size)
    let offset = 0
    payload.writeUInt16LE(CORE_PROTOCOL_VERSION, offset)
    offset += 2
    payload.writeUInt16LE(flags, offset)
    offset += 2
    writeUint64LEFromSafeInteger(payload, this.last_patch_seq, offset)
    offset += 8
    payload.writeUInt16LE(session_id.length, offset)
    offset += 2
    payload.writeUInt16LE(parent_invocation_length, offset)
    offset += 2
    payload.writeUInt16LE(event_id_length, offset)
    offset += 2
    session_id.copy(payload, offset)
    offset += session_id.length
    payload.write(parent_invocation_id, offset, parent_invocation_length, 'utf8')
    offset += parent_invocation_length
    payload.write(event_id, offset, event_id_length, 'utf8')

    return this.waitFastCoreMessages(payload, FAST_QUEUE_JUMP_TYPE_ID, 'fast queue jump')
  }

  private waitFastCoreAck(payload: Buffer, type_id: number, label: string): CoreMessage[] {
    const rpc = this.ensureRpc()
    const cid = rpc.call(payload, type_id)
    let response = rpc.wait(cid, SPIN_THRESHOLD)
    while (response === null) {
      response = rpc.wait(cid, SPIN_THRESHOLD)
    }
    try {
      const data = response.data()
      if (response.msgType === FAST_ACK_RESPONSE_TYPE_ID) {
        this.setPatchSeq(this.readFastPatchSeq(data))
        return []
      }
      if (response.msgType === FAST_ERROR_RESPONSE_TYPE_ID) {
        throw new Error(data.toString('utf8') || `Rust core ${label} failed`)
      }
      throw new Error(`unexpected Rust core ${label} response type: ${response.msgType}`)
    } finally {
      try {
        response.commit()
      } finally {
        this.releaseTransportSoon()
      }
    }
  }

  private waitFastCoreMessages(payload: Buffer, type_id: number, label: string): CoreMessage[] {
    const rpc = this.ensureRpc()
    const cid = rpc.call(payload, type_id)
    let response = rpc.wait(cid, SPIN_THRESHOLD)
    while (response === null) {
      response = rpc.wait(cid, SPIN_THRESHOLD)
    }
    try {
      const data = response.data()
      if (response.msgType === FAST_ACK_RESPONSE_TYPE_ID) {
        this.setPatchSeq(this.readFastPatchSeq(data))
        return []
      }
      if (response.msgType === FAST_ERROR_RESPONSE_TYPE_ID) {
        throw new Error(data.toString('utf8') || `Rust core ${label} failed`)
      }
      if (response.msgType === FAST_MESSAGES_RESPONSE_TYPE_ID) {
        const patch_seq = this.readFastPatchSeq(data.subarray(0, 8))
        const messages = decode(data.subarray(8)) as CoreMessage[]
        let has_patch = false
        for (const message of messages) {
          if (message.type === 'error') {
            throw new Error(typeof message.message === 'string' ? message.message : `Rust core ${label} failed`)
          }
          if (message.type === 'patch') {
            has_patch = true
          }
        }
        if (!has_patch) {
          this.setPatchSeq(patch_seq)
        }
        return messages
      }
      throw new Error(`unexpected Rust core ${label} response type: ${response.msgType}`)
    } finally {
      try {
        response.commit()
      } finally {
        this.releaseTransportSoon()
      }
    }
  }

  private readFastPatchSeq(data: Buffer): number {
    if (data.length < 8) {
      throw new Error('fast handler completion response missing patch sequence')
    }
    const value = data.readBigUInt64LE(0)
    if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new Error('fast handler completion patch sequence exceeds safe integer range')
    }
    return Number(value)
  }

  private requiredString(value: unknown, label: string): string {
    if (typeof value !== 'string' || value.length === 0) {
      throw new Error(`fast handler completion requires string ${label}`)
    }
    return value
  }

  private requiredUint64(value: unknown, label: string): number {
    if (typeof value !== 'number' || !Number.isInteger(value) || value < 0 || value > Number.MAX_SAFE_INTEGER) {
      throw new Error(`fast handler completion requires uint64-safe ${label}`)
    }
    return value
  }

  completeHandlerOutcomes(outcomes: Record<string, unknown>[], options: { compact_response?: boolean } = {}): CoreMessage[] {
    const compact_response = options.compact_response ?? false
    const include_patches = !(compact_response && outcomes.length > 1)
    const messages: CoreMessage[] = []
    for (let index = 0; index < outcomes.length; index += HANDLER_OUTCOME_BATCH_LIMIT) {
      const chunk = outcomes.slice(index, index + HANDLER_OUTCOME_BATCH_LIMIT)
      messages.push(
        ...this.requestMessages(
          { type: 'handler_outcomes', outcomes: chunk, compact_response },
          {
            includePatches: include_patches,
            advancePatchSeq: !include_patches,
            advancePatchSeqWhenNoPatches: compact_response,
          }
        )
      )
    }
    return messages
  }

  completedHandlerOutcome(value: unknown, result_is_event_reference = false): CoreMessage {
    return {
      status: 'completed',
      value: value === undefined ? null : value,
      result_is_event_reference,
      result_is_undefined: value === undefined,
    }
  }

  handlerOutcomeRecord(
    invocation: Record<string, unknown>,
    outcome: CoreMessage,
    options: { process_available_after?: boolean } = {}
  ): CoreMessage {
    return {
      result_id: invocation.result_id,
      invocation_id: invocation.invocation_id,
      fence: invocation.fence,
      process_available_after: options.process_available_after ?? false,
      outcome,
    }
  }

  errorHandler(
    invocation: Record<string, unknown>,
    error: unknown,
    options: { process_route_after?: boolean; process_available_after?: boolean; compact_response?: boolean } = {}
  ): CoreMessage[] {
    return this.requestMessages({
      type: 'handler_outcome',
      result_id: invocation.result_id,
      invocation_id: invocation.invocation_id,
      fence: invocation.fence,
      process_route_after: options.process_route_after ?? false,
      process_available_after: options.process_available_after ?? false,
      compact_response: options.compact_response ?? false,
      outcome: this.erroredHandlerOutcome(error),
    })
  }

  erroredHandlerOutcome(error: unknown): CoreMessage {
    const message = error instanceof Error ? error.message : String(error)
    const error_name = error instanceof Error ? error.name : undefined
    const raw_value = error && typeof error === 'object' && 'raw_value' in error ? (error as { raw_value?: unknown }).raw_value : undefined
    const timeout_seconds =
      error && typeof error === 'object' && 'timeout_seconds' in error
        ? (error as { timeout_seconds?: unknown }).timeout_seconds
        : undefined
    const attempt = error && typeof error === 'object' && 'attempt' in error ? (error as { attempt?: unknown }).attempt : undefined
    const cause = error && typeof error === 'object' && 'cause' in error ? (error as { cause?: unknown }).cause : undefined
    const cause_name = cause instanceof Error ? cause.name : undefined
    const cause_message = cause instanceof Error ? cause.message : undefined
    const cause_timeout_seconds =
      cause && typeof cause === 'object' && 'timeout_seconds' in cause
        ? (cause as { timeout_seconds?: unknown }).timeout_seconds
        : undefined
    const cause_attempt = cause && typeof cause === 'object' && 'attempt' in cause ? (cause as { attempt?: unknown }).attempt : undefined
    const kind =
      error_name === 'EventHandlerResultSchemaError'
        ? 'schema_error'
        : error_name === 'EventHandlerTimeoutError' || error_name === 'RetryTimeoutError'
          ? 'handler_timeout'
          : error_name === 'EventHandlerAbortedError'
            ? 'handler_aborted'
            : error_name === 'EventHandlerCancelledError'
              ? 'handler_cancelled'
              : 'host_error'
    return {
      status: 'errored',
      error: {
        kind,
        message,
        details: {
          name: error_name,
          raw_value,
          timeout_seconds,
          attempt,
          cause_name,
          cause_message,
          cause_timeout_seconds,
          cause_attempt,
        },
      },
    }
  }

  close(): void {
    this.disconnect()
  }

  closeTransportOnly(options: { closeSession?: boolean } = {}): void {
    this.closeTransport(options)
  }

  disconnect(): void {
    try {
      this.closeSession()
    } catch {
      // The transport may already be closed or the core may have been stopped.
    }
    this.closeTransport()
    if (this.kill_process_on_close) {
      this.process?.kill()
    }
  }

  stop(): void {
    try {
      this.stopCore()
    } finally {
      this.closeTransport()
      if (this.kill_process_on_close) {
        this.process?.kill()
      }
    }
  }

  private connect(socket_path: string, timeout_ms = CORE_STARTUP_TIMEOUT_MS): RpcBus {
    let last_error: unknown
    const effective_deadline = Date.now() + timeout_ms
    while (Date.now() < effective_deadline) {
      if (this.process?.exitCode !== null && this.process?.exitCode !== undefined) {
        throw new Error(`Rust Tachyon core exited before connect: ${this.process.exitCode}`)
      }
      try {
        const rpc = RpcBus.connect(socket_path)
        this.configureRpc(rpc)
        return rpc
      } catch (error) {
        last_error = error
        sleepMs(10)
      }
    }
    throw new Error(`failed to connect to Rust Tachyon core at ${socket_path}: ${String(last_error)}`)
  }

  private tryConnect(socket_path: string): RpcBus | null {
    try {
      const rpc = RpcBus.connect(socket_path)
      this.configureRpc(rpc)
      return rpc
    } catch {
      return null
    }
  }

  private connectNamedBus(bus_name: string): RpcBus {
    if (existsSync(this.socket_path)) {
      try {
        return this.requestNamedSession(bus_name, 100)
      } catch {
        this.ensureNamedDaemon(true)
      }
    } else {
      this.ensureNamedDaemon(false)
    }
    try {
      return this.requestNamedSession(bus_name, CORE_STARTUP_TIMEOUT_MS)
    } catch {
      this.ensureNamedDaemon(true)
      return this.requestNamedSession(bus_name, CORE_STARTUP_TIMEOUT_MS)
    }
  }

  private requestNamedSession(bus_name: string, timeout_ms: number): RpcBus {
    const session_socket = stableCoreSessionSocketPath(bus_name)
    const control = this.connect(this.socket_path, timeout_ms)
    try {
      const cid = control.call(Buffer.from(encode({ socket_path: session_socket })), REQUEST_TYPE_ID)
      let response = control.wait(cid, SPIN_THRESHOLD)
      while (response === null) {
        response = control.wait(cid, SPIN_THRESHOLD)
      }
      try {
        const ack = decode(response.data()) as { ok?: boolean; error?: string }
        if (!ack.ok) {
          throw new Error(ack.error ?? 'named core rejected session request')
        }
      } finally {
        response.commit()
      }
    } finally {
      control.close()
    }
    try {
      return this.connect(session_socket, timeout_ms)
    } catch (error) {
      rmSync(session_socket, { force: true })
      throw error
    }
  }

  private requestNamedStop(): void {
    const control = this.connect(this.socket_path, 500)
    try {
      const cid = control.call(Buffer.from(encode({ stop: true })), REQUEST_TYPE_ID)
      let response = control.wait(cid, SPIN_THRESHOLD)
      while (response === null) {
        response = control.wait(cid, SPIN_THRESHOLD)
      }
      try {
        const ack = decode(response.data()) as { ok?: boolean; error?: string }
        if (!ack.ok) {
          throw new Error(ack.error ?? 'named core rejected stop request')
        }
      } finally {
        response.commit()
      }
    } finally {
      control.close()
    }
    this.waitForNamedDaemonStopped(1000)
  }

  private waitForNamedDaemonStopped(timeout_ms: number): void {
    const lock_path = namedCoreLockPath(this.socket_path)
    const deadline = Date.now() + timeout_ms
    while (Date.now() < deadline) {
      if (!existsSync(this.socket_path) && !existsSync(lock_path)) {
        return
      }
      sleepMs(5)
    }
    if (existsSync(lock_path) && !lockOwnerIsRunning(lock_path)) {
      rmSync(lock_path, { force: true })
    }
    if (!existsSync(lock_path)) {
      rmSync(this.socket_path, { force: true })
    }
  }

  private ensureNamedDaemon(force_restart: boolean): void {
    if (force_restart) {
      if (this.process && this.process.exitCode === null) {
        try {
          this.process.kill()
        } catch {
          // Best-effort restart cleanup.
        }
      }
      this.process = null
      rmSync(this.socket_path, { force: true })
      rmSync(namedCoreLockPath(this.socket_path), { force: true })
    } else {
      const lock_path = namedCoreLockPath(this.socket_path)
      if (existsSync(lock_path) && !lockOwnerIsRunning(lock_path)) {
        rmSync(lock_path, { force: true })
        rmSync(this.socket_path, { force: true })
      }
    }
    const child = spawn(this.command, this.args, {
      stdio: 'ignore',
      detached: true,
      env: this.coreProcessEnv({ ownerPid: !process.env.ABXBUS_CORE_NAMESPACE }),
    })
    child.unref()
    this.process = this.kill_process_on_close ? child : null
  }

  private coreProcessEnv(options: { ownerPid: boolean }): NodeJS.ProcessEnv {
    const env: NodeJS.ProcessEnv = { ...process.env }
    if (options.ownerPid) {
      env.ABXBUS_CORE_OWNER_PID = String(process.pid)
    } else {
      delete env.ABXBUS_CORE_OWNER_PID
    }
    return env
  }

  private ensureRpc(): RpcBus {
    if (this.release_transport_timer) {
      clearTimeout(this.release_transport_timer)
      this.release_transport_timer = null
    }
    if (this.rpc) {
      return this.rpc
    }
    if (this.bus_name) {
      this.rpc = this.connectNamedBus(this.bus_name)
      return this.rpc
    }
    this.rpc = this.connect(this.socket_path)
    return this.rpc
  }

  private configureRpc(rpc: RpcBus): void {
    rpc.setPollingMode(POLLING_MODE)
  }

  private closeTransport(options: { closeSession?: boolean } = {}): void {
    if (this.release_transport_timer) {
      clearTimeout(this.release_transport_timer)
      this.release_transport_timer = null
    }
    const rpc = this.rpc
    try {
      if (options.closeSession === true && rpc) {
        this.sendCloseSession(rpc)
      }
    } catch {
      // ignore close races
    }
    try {
      rpc?.close()
    } catch {
      // ignore close races
    } finally {
      this.rpc = null
    }
  }

  releaseTransportSoon(): void {
    if (this.bus_name === null || this.release_transport_timer || !this.rpc) {
      return
    }
    this.release_transport_timer = setTimeout(() => {
      this.release_transport_timer = null
      this.closeTransport({ closeSession: true })
    }, 10)
    this.release_transport_timer.unref?.()
  }

  private sendCloseSession(rpc: RpcBus): void {
    const envelope: ProtocolEnvelope<CoreMessage> = {
      protocol_version: CORE_PROTOCOL_VERSION,
      session_id: this.session_id,
      message: { type: 'close_session' },
    }
    const cid = rpc.call(Buffer.from(encode(envelope)), REQUEST_TYPE_ID)
    let response = rpc.wait(cid, SPIN_THRESHOLD)
    while (response === null) {
      response = rpc.wait(cid, SPIN_THRESHOLD)
    }
    try {
      response.commit()
    } catch {
      // The transport is already being closed.
    }
  }
}

const sleepMs = (ms: number): void => {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms)
}

const lockOwnerIsRunning = (lock_path: string): boolean => {
  try {
    const pid = Number.parseInt(readFileSync(lock_path, 'utf8').trim(), 10)
    if (!Number.isFinite(pid) || pid <= 0) {
      return false
    }
    process.kill(pid, 0)
    return true
  } catch (error) {
    return Boolean(error && typeof error === 'object' && 'code' in error && (error as { code?: unknown }).code === 'EPERM')
  }
}

export const defaultCoreCommand = (socket_path: string, options: { daemon?: boolean } = {}): { command: string; args: string[] } => {
  const socket_args = options.daemon ? ['--daemon', socket_path] : [socket_path]
  if (process.env.ABXBUS_CORE_TACHYON_BIN) {
    return { command: process.env.ABXBUS_CORE_TACHYON_BIN, args: socket_args }
  }
  const src_dir = dirname(fileURLToPath(import.meta.url))
  const package_dir = existsSync(resolve(src_dir, '..', '..', 'package.json')) ? resolve(src_dir, '..', '..') : resolve(src_dir, '..')
  const repo_root = resolve(package_dir, '..')
  const manifest = resolve(repo_root, 'abxbus-core', 'Cargo.toml')
  const release_bin = resolve(repo_root, 'abxbus-core', 'target', 'release', 'abxbus-core-tachyon')
  if (existsSync(release_bin)) {
    return { command: release_bin, args: socket_args }
  }
  const debug_bin = resolve(repo_root, 'abxbus-core', 'target', 'debug', 'abxbus-core-tachyon')
  if (existsSync(debug_bin)) {
    return { command: debug_bin, args: socket_args }
  }
  return {
    command: 'cargo',
    args: ['run', '--quiet', '--manifest-path', manifest, '--bin', 'abxbus-core-tachyon', '--', ...socket_args],
  }
}

export const stableCoreSocketPath = (_bus_name: string): string => {
  const namespace = coreNamespace()
  const digest = createHash('sha256').update(namespace).digest('hex').slice(0, 24)
  return resolve(tmpdir(), `abxbus-core-${digest}.sock`)
}

export const coreNamespace = (): string => process.env.ABXBUS_CORE_NAMESPACE || DEFAULT_CORE_NAMESPACE

export const stableCoreSessionSocketPath = (bus_name: string): string => {
  const digest = createHash('sha256').update(bus_name).digest('hex').slice(0, 16)
  return resolve(tmpdir(), `abxbus-core-${digest}-${randomUUID().slice(0, 8)}.sock`)
}

export const namedCoreLockPath = (socket_path: string): string => socket_path.replace(/\.sock$/, '.sock.lock')
