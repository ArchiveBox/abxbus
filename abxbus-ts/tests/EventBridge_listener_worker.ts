import { readFileSync, writeFileSync } from 'node:fs'

import { NATSEventBridge, PostgresEventBridge, RedisEventBridge } from '../src/bridges.js'
import { HTTPEventBridge, JSONLEventBridge, SQLiteEventBridge, SocketEventBridge } from '../src/index.js'

type WorkerConfig = {
  kind: string
  ready_path: string
  output_path: string
  endpoint?: string
  path?: string
  table?: string
  url?: string
  server?: string
  subject?: string
}

const makeListenerBridge = (config: WorkerConfig): any => {
  if (config.kind === 'http') return new HTTPEventBridge({ listen_on: config.endpoint })
  if (config.kind === 'socket') return new SocketEventBridge(config.path)
  if (config.kind === 'jsonl') return new JSONLEventBridge(config.path ?? '', 0.05)
  if (config.kind === 'sqlite') return new SQLiteEventBridge(config.path ?? '', config.table ?? 'abxbus_events', 0.05)
  if (config.kind === 'redis') return new RedisEventBridge(config.url ?? '')
  if (config.kind === 'nats') return new NATSEventBridge(config.server ?? '', config.subject ?? '')
  if (config.kind === 'postgres') return new PostgresEventBridge(config.url ?? '')
  throw new Error(`Unsupported bridge kind: ${config.kind}`)
}

const main = async (): Promise<void> => {
  const config_path = process.argv[2]
  const config = JSON.parse(readFileSync(config_path, 'utf8')) as WorkerConfig
  const bridge = makeListenerBridge(config)

  let resolve_done: (() => void) | null = null
  const done = new Promise<void>((resolve) => {
    resolve_done = resolve
  })

  await bridge.start()
  bridge.on('*', (event: { toJSON: () => unknown }) => {
    writeFileSync(config.output_path, JSON.stringify(event.toJSON()), 'utf8')
    resolve_done?.()
  })
  writeFileSync(config.ready_path, 'ready', 'utf8')
  await Promise.race([done, new Promise((_, reject) => setTimeout(() => reject(new Error('worker timeout')), 30000))])
  await bridge.close()
}

await main()
