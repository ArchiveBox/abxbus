import { EventBridge, randomSuffix } from './EventBridge.js'

export class SocketEventBridge extends EventBridge {
  constructor(path?: string | null, name?: string) {
    const normalized = path ? (path.startsWith('unix://') ? path.slice(7) : path) : null
    if (normalized === '') {
      throw new Error('SocketEventBridge path must not be empty')
    }

    const endpoint = normalized ? `unix://${normalized}` : null
    super(endpoint, endpoint, name ?? `SocketEventBridge_${randomSuffix()}`)
  }
}
