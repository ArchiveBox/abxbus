import { BaseEvent } from './BaseEvent.js'
import { EventBus } from './EventBus.js'
import type { EventClass, EventHandlerCallable, EventPattern, UntypedEventHandlerFunction } from './types.js'

export type EndpointScheme = 'unix' | 'http' | 'https'

export type ParsedEndpoint = {
  raw: string
  scheme: EndpointScheme
  host?: string
  port?: number
  path?: string
}

const isNodeRuntime = (): boolean => {
  const maybe_process = (globalThis as { process?: { versions?: { node?: string } } }).process
  return typeof maybe_process?.versions?.node === 'string'
}

const isBrowserRuntime = (): boolean => !isNodeRuntime() && typeof globalThis.window !== 'undefined'

export const randomSuffix = (): string => Math.random().toString(36).slice(2, 10)
const UNIX_SOCKET_MAX_PATH_CHARS = 90

export const parseEndpoint = (raw_endpoint: string): ParsedEndpoint => {
  let parsed: URL
  try {
    parsed = new URL(raw_endpoint)
  } catch {
    throw new Error(`Invalid endpoint URL: ${raw_endpoint}`)
  }

  const protocol = parsed.protocol.replace(/:$/, '').toLowerCase()
  if (protocol !== 'unix' && protocol !== 'http' && protocol !== 'https') {
    throw new Error(`Unsupported endpoint scheme: ${raw_endpoint}`)
  }

  if (protocol === 'unix') {
    const socket_path = decodeURIComponent(parsed.pathname || '')
    if (!socket_path) {
      throw new Error(`Invalid unix endpoint (missing socket path): ${raw_endpoint}`)
    }
    const socket_path_len = new TextEncoder().encode(socket_path).length
    if (socket_path_len > UNIX_SOCKET_MAX_PATH_CHARS) {
      throw new Error(`Unix socket path is too long (${socket_path_len} chars), max is ${UNIX_SOCKET_MAX_PATH_CHARS}: ${socket_path}`)
    }
    return { raw: raw_endpoint, scheme: 'unix', path: socket_path }
  }

  if (!parsed.hostname) {
    throw new Error(`Invalid HTTP endpoint (missing hostname): ${raw_endpoint}`)
  }

  const default_port = protocol === 'https' ? 443 : 80
  return {
    raw: raw_endpoint,
    scheme: protocol,
    host: parsed.hostname,
    port: parsed.port ? Number(parsed.port) : default_port,
    path: `${parsed.pathname || '/'}${parsed.search || ''}`,
  }
}

const importNodeModule = async (specifier: string): Promise<any> => {
  const dynamic_import = Function('module_name', 'return import(module_name)') as (module_name: string) => Promise<unknown>
  return dynamic_import(specifier) as Promise<any>
}

export class EventBridge {
  readonly send_to: ParsedEndpoint | null
  readonly listen_on: ParsedEndpoint | null
  readonly name: string

  protected readonly inbound_bus: EventBus
  private start_promise: Promise<void> | null
  private node_server: any | null

  constructor(send_to?: string | null, listen_on?: string | null, name?: string) {
    this.send_to = send_to ? parseEndpoint(send_to) : null
    this.listen_on = listen_on ? parseEndpoint(listen_on) : null
    this.name = name ?? `EventBridge_${randomSuffix()}`
    this.inbound_bus = new EventBus(this.name, { max_history_size: 0 })
    this.start_promise = null
    this.node_server = null

    if (this.listen_on && isBrowserRuntime()) {
      throw new Error(`${this.constructor.name} listen_on is not supported in browser runtimes`)
    }

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
    if (!this.send_to) {
      throw new Error(`${this.constructor.name}.emit() requires send_to`)
    }

    const payload = event.toJSON()

    if (this.send_to.scheme === 'unix') {
      await this.sendUnix(this.send_to, payload)
      return
    }

    await this.sendHttp(this.send_to, payload)
  }

  async dispatch<T extends BaseEvent>(event: T): Promise<void> {
    return this.emit(event)
  }

  async start(): Promise<void> {
    if (!this.listen_on) return
    if (this.node_server) return
    if (this.start_promise) {
      await this.start_promise
      return
    }

    if (!isNodeRuntime()) {
      throw new Error(`${this.constructor.name} listen_on is only supported in Node.js runtimes`)
    }

    const launch = (async () => {
      const endpoint = this.listen_on
      if (!endpoint) return

      if (endpoint.scheme === 'unix') {
        await this.startUnixListener(endpoint)
        return
      }

      if (endpoint.scheme !== 'http') {
        throw new Error(`listen_on only supports unix:// or http:// endpoints, got: ${endpoint.raw}`)
      }

      await this.startHttpListener(endpoint)
    })()
    this.start_promise = launch

    try {
      await launch
    } finally {
      if (this.start_promise === launch) {
        this.start_promise = null
      }
    }
  }

  async close(): Promise<void> {
    if (this.start_promise) {
      await Promise.allSettled([this.start_promise])
      this.start_promise = null
    }

    if (this.node_server) {
      const server = this.node_server
      await new Promise<void>((resolve) => {
        server.close(() => resolve())
      })
      this.node_server = null
    }

    this.inbound_bus.destroy()
  }

  private ensureListenerStarted(): void {
    if (!this.listen_on || this.node_server || this.start_promise) {
      return
    }
    void this.start().catch((error: unknown) => {
      console.error('[abxbus] EventBridge failed to start listener', error)
    })
  }

  private async handleIncomingPayload(payload: unknown): Promise<void> {
    const event = BaseEvent.fromJSON(payload).eventReset()
    this.inbound_bus.emit(event)
  }

  private async sendHttp(endpoint: ParsedEndpoint, payload: unknown): Promise<void> {
    const response = await fetch(endpoint.raw, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload),
    })
    if (!response.ok) {
      throw new Error(`IPC HTTP send failed with status ${response.status}: ${endpoint.raw}`)
    }
  }

  private async sendUnix(endpoint: ParsedEndpoint, payload: unknown): Promise<void> {
    if (!isNodeRuntime()) {
      throw new Error('unix:// send_to is only supported in Node.js runtimes')
    }

    const socket_path = endpoint.path
    if (!socket_path) {
      throw new Error(`Invalid unix endpoint: ${endpoint.raw}`)
    }

    const node_net = await importNodeModule('node:net')
    await new Promise<void>((resolve, reject) => {
      const socket = node_net.createConnection(socket_path, () => {
        socket.end(`${JSON.stringify(payload)}\n`)
      })
      socket.on('error', (error: unknown) => reject(error))
      socket.on('close', () => resolve())
    })
  }

  private async startHttpListener(endpoint: ParsedEndpoint): Promise<void> {
    const node_http = await importNodeModule('node:http')
    const expected_path = endpoint.path || '/'

    this.node_server = node_http.createServer((req: any, res: any) => {
      const method = (req.method || '').toUpperCase()
      const request_url = String(req.url || '/')

      if (method !== 'POST') {
        res.statusCode = 405
        res.end('method not allowed')
        return
      }
      if (request_url !== expected_path) {
        res.statusCode = 404
        res.end('not found')
        return
      }

      let body = ''
      req.setEncoding('utf8')
      req.on('data', (chunk: string) => {
        body += chunk
      })
      req.on('end', () => {
        let parsed_payload: unknown
        try {
          parsed_payload = JSON.parse(body)
        } catch {
          res.statusCode = 400
          res.end('invalid json')
          return
        }

        void this.handleIncomingPayload(parsed_payload)
          .then(() => {
            res.statusCode = 202
            res.end('accepted')
          })
          .catch((error: unknown) => {
            res.statusCode = 500
            res.end('failed to process event')
            console.error('[abxbus] EventBridge HTTP listener error', error)
          })
      })
    })

    await new Promise<void>((resolve, reject) => {
      this.node_server.once('error', (error: unknown) => reject(error))
      this.node_server.listen(endpoint.port, endpoint.host, () => resolve())
    })
  }

  private async startUnixListener(endpoint: ParsedEndpoint): Promise<void> {
    const socket_path = endpoint.path
    if (!socket_path) {
      throw new Error(`Invalid unix endpoint: ${endpoint.raw}`)
    }

    const node_net = await importNodeModule('node:net')
    const node_fs = await importNodeModule('node:fs')

    try {
      await node_fs.promises.unlink(socket_path)
    } catch (error: unknown) {
      const code = (error as { code?: string }).code
      if (code !== 'ENOENT') {
        throw error
      }
    }

    this.node_server = node_net.createServer((socket: any) => {
      let buffer = ''
      socket.setEncoding('utf8')
      socket.on('data', (chunk: string) => {
        buffer += chunk
        while (true) {
          const newline_index = buffer.indexOf('\n')
          if (newline_index < 0) break
          const line = buffer.slice(0, newline_index).trim()
          buffer = buffer.slice(newline_index + 1)
          if (!line) continue
          try {
            const parsed_payload = JSON.parse(line)
            void this.handleIncomingPayload(parsed_payload)
          } catch {
            // Ignore malformed lines and continue reading next frames.
          }
        }
      })
      socket.on('end', () => {
        const remainder = buffer.trim()
        if (!remainder) return
        try {
          const parsed_payload = JSON.parse(remainder)
          void this.handleIncomingPayload(parsed_payload)
        } catch {
          // Ignore malformed trailing frame.
        }
      })
    })

    await new Promise<void>((resolve, reject) => {
      this.node_server.once('error', (error: unknown) => reject(error))
      this.node_server.listen(socket_path, () => resolve())
    })
  }
}
