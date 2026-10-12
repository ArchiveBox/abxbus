import { retry } from '../src/index.js'

const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))
const blockFor = (ms: number): void => {
  const deadline = Date.now() + ms
  while (Date.now() < deadline) {}
}

const worker_id = Number(process.argv[2] ?? '0')
const start_ms = Number(process.argv[3] ?? String(Date.now()))
const hold_ms = Number(process.argv[4] ?? '200')
const semaphore_name = process.argv[5] ?? 'retry_multiprocess_worker'
const semaphore_limit = Number(process.argv[6] ?? '1')
const mode = process.argv[7] ?? 'async'

const log = (payload: Record<string, unknown>): void => {
  process.stdout.write(`${JSON.stringify(payload)}\n`)
}

const retry_options = {
  max_attempts: 1,
  timeout: 10,
  semaphore_limit,
  semaphore_name,
  semaphore_scope: 'multiprocess',
  semaphore_timeout: 5,
  semaphore_lax: false,
} as const

const run =
  mode === 'sync'
    ? retry(retry_options)(() => {
        log({ type: 'acquired', worker_id, at_ms: Date.now() - start_ms })
        blockFor(hold_ms)
        log({ type: 'released', worker_id, at_ms: Date.now() - start_ms })
        return worker_id
      })
    : retry(retry_options)(async () => {
        log({ type: 'acquired', worker_id, at_ms: Date.now() - start_ms })
        await delay(hold_ms)
        log({ type: 'released', worker_id, at_ms: Date.now() - start_ms })
        return worker_id
      })

void Promise.resolve(run())
  .then(() => {
    log({ type: 'completed', worker_id, at_ms: Date.now() - start_ms })
  })
  .catch((error: unknown) => {
    const message = error instanceof Error ? `${error.name}: ${error.message}` : String(error)
    log({ type: 'error', worker_id, at_ms: Date.now() - start_ms, error: message })
    process.exitCode = 1
  })
