import { EventBridge, parseEndpoint, randomSuffix } from './EventBridge.js'

export type HTTPEventBridgeOptions = {
  send_to?: string | null
  listen_on?: string | null
  name?: string
}

export class HTTPEventBridge extends EventBridge {
  constructor(send_to?: string | null, listen_on?: string | null, name?: string)
  constructor(options?: HTTPEventBridgeOptions)
  constructor(send_to_or_options?: string | null | HTTPEventBridgeOptions, listen_on?: string | null, name?: string) {
    const options: HTTPEventBridgeOptions =
      typeof send_to_or_options === 'object'
        ? (send_to_or_options ?? {})
        : { send_to: send_to_or_options ?? undefined, listen_on: listen_on ?? undefined, name }

    if (options.send_to && parseEndpoint(options.send_to).scheme === 'unix') {
      throw new Error('HTTPEventBridge send_to must be http:// or https://')
    }
    if (options.listen_on && parseEndpoint(options.listen_on).scheme !== 'http') {
      throw new Error('HTTPEventBridge listen_on must be http://')
    }

    super(options.send_to, options.listen_on, options.name ?? `HTTPEventBridge_${randomSuffix()}`)
  }
}
