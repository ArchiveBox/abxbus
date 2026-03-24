import { createAsyncLocalStorage, type AsyncLocalStorageLike } from './async_context.js'
import { isNodeRuntime } from './optional_deps.js'

type SemaphoreScope = 'multiprocess' | 'global' | 'class' | 'instance'

type MultiprocessLockHandle = {
  release: () => Promise<void>
}

const MULTIPROCESS_SEMAPHORE_DIRNAME = 'browser_use_semaphores'
const MULTIPROCESS_STALE_LOCK_MS = 5 * 60 * 1000

let multiprocess_fallback_reason_logged: string | null = null

// ─── Types ───────────────────────────────────────────────────────────────────

export interface RetryOptions {
  /** Total number of attempts including the initial call (1 = no retry, 3 = up to 2 retries). Default: 1 */
  max_attempts?: number

  /** Seconds to wait between retries. Default: 0 */
  retry_after?: number

  /** Multiplier applied to retry_after after each attempt for exponential backoff. Default: 1.0 (constant delay) */
  retry_backoff_factor?: number

  /** Only retry when the thrown error matches one of these matchers. Accepts error class constructors,
   *  string error names (matched against error.name), or RegExp patterns (tested against String(error)).
   *  Default: undefined (retry on any error) */
  retry_on_errors?: Array<(new (...args: any[]) => Error) | string | RegExp>

  /** Per-attempt timeout in seconds. Default: undefined (no per-attempt timeout) */
  timeout?: number | null

  /** Maximum concurrent executions sharing this semaphore. Default: undefined (no concurrency limit) */
  semaphore_limit?: number | null

  /** Semaphore identifier. Functions with the same name share the same concurrency slot pool. Default: function name.
   *  If a function is provided, it receives the same arguments as the wrapped function. */
  semaphore_name?: string | ((...args: any[]) => string) | null

  /** If true, proceed without concurrency limit when semaphore acquisition times out. Default: true */
  semaphore_lax?: boolean

  /** Semaphore scoping strategy. Default: 'global'
   *  - 'multiprocess': all processes on the machine share one semaphore (Node.js only)
   *  - 'global': all calls share one semaphore (keyed by semaphore_name)
   *  - 'class': all instances of the same class share one semaphore (keyed by className.semaphore_name)
   *  - 'instance': each object instance gets its own semaphore (keyed by instanceId.semaphore_name)
   *  'class' and 'instance' require `this` to be an object; they fall back to 'global' for standalone calls. */
  semaphore_scope?: SemaphoreScope

  /** Maximum seconds to wait for semaphore acquisition. Default: undefined → timeout * max(1, limit - 1) */
  semaphore_timeout?: number | null
}

// ─── Errors ──────────────────────────────────────────────────────────────────

/** Thrown when a single attempt exceeds the per-attempt timeout. */
export class RetryTimeoutError extends Error {
  timeout_seconds: number
  attempt: number

  constructor(message: string, params: { timeout_seconds: number; attempt: number }) {
    super(message)
    this.name = 'RetryTimeoutError'
    this.timeout_seconds = params.timeout_seconds
    this.attempt = params.attempt
  }
}

/** Thrown (when semaphore_lax=false) if the semaphore cannot be acquired within the timeout. */
export class SemaphoreTimeoutError extends Error {
  semaphore_name: string
  semaphore_limit: number
  timeout_seconds: number

  constructor(message: string, params: { semaphore_name: string; semaphore_limit: number; timeout_seconds: number }) {
    super(message)
    this.name = 'SemaphoreTimeoutError'
    this.semaphore_name = params.semaphore_name
    this.semaphore_limit = params.semaphore_limit
    this.timeout_seconds = params.timeout_seconds
  }
}

// ─── Re-entrancy tracking via AsyncLocalStorage ──────────────────────────────
//
// Prevents deadlocks when a retry()-wrapped function calls another retry()-wrapped
// function that shares the same semaphore (or calls itself recursively).
//
// Each async call stack tracks which semaphore names it currently holds. When a
// nested call encounters a semaphore it already holds, it skips acquisition and
// runs directly within the parent's slot.
//
// Uses the same AsyncLocalStorage polyfill as the rest of abxbus (see async_context.ts)
// so it works in Node.js and gracefully degrades to a no-op in browsers.

type ReentrantStore = Set<string>

// Separate AsyncLocalStorage instance for retry re-entrancy tracking.
// Created via the shared factory in async_context.ts (returns null in browsers).
const retry_context_storage: AsyncLocalStorageLike | null = createAsyncLocalStorage()

function getHeldSemaphores(): ReentrantStore {
  return (retry_context_storage?.getStore() as ReentrantStore | undefined) ?? new Set()
}

function runWithHeldSemaphores<T>(held: ReentrantStore, fn: () => T): T {
  if (!retry_context_storage) return fn()
  return retry_context_storage.run(held, fn)
}

// ─── Semaphore scope helpers ─────────────────────────────────────────────────

let _next_instance_id = 1
const _instance_ids = new WeakMap<object, number>()

function scopedSemaphoreKey(base_name: string, scope: SemaphoreScope, context: unknown): string {
  if (scope === 'class' && context && typeof context === 'object') {
    return `${(context as object).constructor?.name ?? 'Object'}.${base_name}`
  }
  if (scope === 'instance' && context && typeof context === 'object') {
    let id = _instance_ids.get(context as object)
    if (id === undefined) {
      id = _next_instance_id++
      _instance_ids.set(context as object, id)
    }
    return `${id}.${base_name}`
  }
  return base_name
}

// ─── Global semaphore registry ───────────────────────────────────────────────

class RetrySemaphore {
  readonly size: number
  private inUse: number
  private waiters: Array<() => void>

  constructor(size: number) {
    this.size = size
    this.inUse = 0
    this.waiters = []
  }

  async acquire(): Promise<void> {
    if (this.size === Infinity) {
      return
    }
    if (this.inUse < this.size) {
      this.inUse += 1
      return
    }
    await new Promise<void>((resolve) => {
      this.waiters.push(resolve)
    })
  }

  release(): void {
    if (this.size === Infinity) {
      return
    }
    const next = this.waiters.shift()
    if (next) {
      // Handoff: keep the permit accounted for and transfer it directly to the waiter.
      next()
      return
    }
    this.inUse = Math.max(0, this.inUse - 1)
  }
}

const SEMAPHORE_REGISTRY = new Map<string, RetrySemaphore>()

function getOrCreateSemaphore(name: string, limit: number): RetrySemaphore {
  const existing = SEMAPHORE_REGISTRY.get(name)
  if (existing && existing.size === limit) return existing
  const sem = new RetrySemaphore(limit)
  SEMAPHORE_REGISTRY.set(name, sem)
  return sem
}

/** Reset the global semaphore registry. Useful in tests. */
export function clearSemaphoreRegistry(): void {
  SEMAPHORE_REGISTRY.clear()
  multiprocess_fallback_reason_logged = null
}

// ─── retry() decorator / higher-order wrapper ────────────────────────────────
//
// Usage as a higher-order function (works on any async function):
//
//   const fetchWithRetry = retry({ max_attempts: 3, retry_after: 1 })(async (url: string) => {
//     return await fetch(url)
//   })
//
// Usage as a TC39 Stage 3 decorator on class methods (TS 5.0+):
//
//   class ApiClient {
//     @retry({ max_attempts: 3, retry_after: 1 })
//     async fetchData(): Promise<Data> { ... }
//   }
//
// Usage on event bus handlers:
//
//   bus.on(MyEvent, retry({ max_attempts: 3 })(async (event) => {
//     await riskyOperation(event.data)
//   }))

export function retry(options: RetryOptions = {}) {
  const {
    max_attempts = 1,
    retry_after = 0,
    retry_backoff_factor = 1.0,
    retry_on_errors,
    timeout,
    semaphore_limit,
    semaphore_name: semaphore_name_option,
    semaphore_lax = true,
    semaphore_scope = 'global',
    semaphore_timeout,
  } = options

  return function decorator<T extends (...args: any[]) => any>(target: T, _context?: ClassMethodDecoratorContext): T {
    const fn_name = target.name || (_context?.name as string) || 'anonymous'
    const effective_max_attempts = Math.max(1, max_attempts)
    const effective_retry_after = Math.max(0, retry_after)

    async function retryWrapper(this: any, ...args: any[]): Promise<any> {
      const base_name = typeof semaphore_name_option === 'function' ? semaphore_name_option(...args) : (semaphore_name_option ?? fn_name)
      const sem_name = typeof base_name === 'string' ? base_name : String(base_name)
      // ── Resolve scoped semaphore key at call time (uses `this` for class/instance scopes) ──
      const scoped_key = scopedSemaphoreKey(sem_name, semaphore_scope, this)

      // ── Check re-entrancy: skip semaphore if we already hold it in this async context ──
      const held = getHeldSemaphores()
      const needs_semaphore = semaphore_limit != null && semaphore_limit > 0
      const is_reentrant = needs_semaphore && held.has(scoped_key)

      // ── Semaphore acquisition (held across all retry attempts, skipped if re-entrant) ──
      let semaphore: RetrySemaphore | null = null
      let multiprocess_lock: MultiprocessLockHandle | null = null
      let semaphore_acquired = false

      if (needs_semaphore && !is_reentrant) {
        const effective_sem_timeout =
          semaphore_timeout != null ? semaphore_timeout : timeout != null ? timeout * Math.max(1, semaphore_limit! - 1) : null

        if (semaphore_scope === 'multiprocess') {
          if (isNodeRuntime()) {
            multiprocess_lock = await acquireMultiprocessSemaphore(scoped_key, semaphore_limit!, effective_sem_timeout, semaphore_lax)
            semaphore_acquired = multiprocess_lock !== null
          } else {
            logMultiprocessFallbackOnce('multiprocess semaphores require a Node.js runtime; falling back to in-process global scope')
            semaphore = getOrCreateSemaphore(scoped_key, semaphore_limit!)
            if (effective_sem_timeout != null && effective_sem_timeout > 0) {
              semaphore_acquired = await acquireWithTimeout(semaphore, effective_sem_timeout * 1000)
              if (!semaphore_acquired) {
                if (!semaphore_lax) {
                  throw new SemaphoreTimeoutError(
                    `Failed to acquire semaphore "${scoped_key}" within ${effective_sem_timeout}s (limit=${semaphore_limit})`,
                    { semaphore_name: scoped_key, semaphore_limit: semaphore_limit!, timeout_seconds: effective_sem_timeout }
                  )
                }
              }
            } else {
              await semaphore.acquire()
              semaphore_acquired = true
            }
          }
        } else {
          semaphore = getOrCreateSemaphore(scoped_key, semaphore_limit!)

          if (effective_sem_timeout != null && effective_sem_timeout > 0) {
            semaphore_acquired = await acquireWithTimeout(semaphore, effective_sem_timeout * 1000)
            if (!semaphore_acquired) {
              if (!semaphore_lax) {
                throw new SemaphoreTimeoutError(
                  `Failed to acquire semaphore "${scoped_key}" within ${effective_sem_timeout}s (limit=${semaphore_limit})`,
                  { semaphore_name: scoped_key, semaphore_limit: semaphore_limit!, timeout_seconds: effective_sem_timeout }
                )
              }
            }
          } else {
            await semaphore.acquire()
            semaphore_acquired = true
          }
        }
      }

      // ── Build the set of held semaphores for nested calls ──
      const new_held = new Set(held)
      if (semaphore_acquired) {
        new_held.add(scoped_key)
      }

      // ── Retry loop (runs inside the semaphore and re-entrancy context) ──
      const runRetryLoop = async (): Promise<any> => {
        for (let attempt = 1; attempt <= effective_max_attempts; attempt++) {
          try {
            if (timeout != null && timeout > 0) {
              return await _runWithTimeout(() => Promise.resolve(target.apply(this, args)), timeout * 1000, attempt)
            } else {
              return await Promise.resolve(target.apply(this, args))
            }
          } catch (error) {
            // Check if this error type should trigger a retry
            if (retry_on_errors && retry_on_errors.length > 0) {
              const is_retryable = retry_on_errors.some((matcher) =>
                typeof matcher === 'string'
                  ? (error as Error)?.name === matcher
                  : matcher instanceof RegExp
                    ? matcher.test(String(error))
                    : error instanceof matcher
              )
              if (!is_retryable) throw error
            }

            // Last attempt: rethrow
            if (attempt >= effective_max_attempts) throw error

            // Wait before next attempt with exponential backoff
            const delay_seconds = effective_retry_after * Math.pow(retry_backoff_factor, attempt - 1)
            if (delay_seconds > 0) {
              await sleep(delay_seconds * 1000)
            }
          }
        }

        // Unreachable, but satisfies the type checker
        throw new Error(`retry(${fn_name}): unexpected end of retry loop`)
      }

      try {
        return await runWithHeldSemaphores(new_held, runRetryLoop)
      } finally {
        if (semaphore_acquired && multiprocess_lock) {
          await multiprocess_lock.release()
        } else if (semaphore_acquired && semaphore) {
          semaphore.release()
        }
      }
    }

    Object.defineProperty(retryWrapper, 'name', { value: fn_name, configurable: true })
    return retryWrapper as unknown as T
  }
}

// ─── Internal helpers ────────────────────────────────────────────────────────

/**
 * Try to acquire a semaphore within a timeout. Returns true if acquired, false if timed out.
 * If the semaphore is acquired after the timeout (due to the waiter remaining queued),
 * it is immediately released to avoid leaking slots.
 */
async function acquireWithTimeout(semaphore: RetrySemaphore, timeout_ms: number): Promise<boolean> {
  return new Promise<boolean>((resolve) => {
    let settled = false

    const timer = setTimeout(() => {
      if (!settled) {
        settled = true
        resolve(false)
      }
    }, timeout_ms)

    semaphore.acquire().then(() => {
      if (!settled) {
        settled = true
        clearTimeout(timer)
        resolve(true)
      } else {
        // Acquired after timeout fired — release immediately to avoid slot leak
        semaphore.release()
      }
    })
  })
}

function logMultiprocessFallbackOnce(reason: string): void {
  if (multiprocess_fallback_reason_logged === reason) return
  multiprocess_fallback_reason_logged = reason
  console.warn(`[abxbus.retry] ${reason}`)
}

async function importNodeModule(specifier: string): Promise<any> {
  const dynamic_import = Function('module_name', 'return import(module_name)') as (module_name: string) => Promise<unknown>
  return dynamic_import(specifier) as Promise<any>
}

async function acquireMultiprocessSemaphore(
  scoped_key: string,
  semaphore_limit: number,
  semaphore_timeout_seconds: number | null,
  semaphore_lax: boolean
): Promise<MultiprocessLockHandle | null> {
  const [crypto, fs, os, path] = await Promise.all([
    importNodeModule('node:crypto'),
    importNodeModule('node:fs'),
    importNodeModule('node:os'),
    importNodeModule('node:path'),
  ])
  const semaphore_directory = path.join(os.tmpdir(), MULTIPROCESS_SEMAPHORE_DIRNAME)
  const lock_prefix = crypto.createHash('sha256').update(scoped_key).digest('hex').slice(0, 40)
  fs.mkdirSync(semaphore_directory, { recursive: true })

  const start = Date.now()
  let retry_delay_ms = 100

  while (true) {
    const elapsed_ms = Date.now() - start
    const remaining_ms =
      semaphore_timeout_seconds != null && semaphore_timeout_seconds > 0 ? semaphore_timeout_seconds * 1000 - elapsed_ms : null

    if (remaining_ms != null && remaining_ms <= 0) {
      break
    }

    for (let slot = 0; slot < semaphore_limit; slot++) {
      const slot_file = path.join(semaphore_directory, `${lock_prefix}.${String(slot).padStart(2, '0')}.lock`)
      const token = `${process.pid}:${Date.now()}:${Math.random().toString(16).slice(2)}`

      try {
        const fd = fs.openSync(slot_file, 'wx', 0o600)
        try {
          fs.writeFileSync(
            fd,
            JSON.stringify({
              token,
              pid: process.pid,
              semaphore_name: scoped_key,
              created_at_ms: Date.now(),
            }),
            'utf8'
          )
        } finally {
          fs.closeSync(fd)
        }
        return {
          release: async () => {
            try {
              const raw = String(fs.readFileSync(slot_file, 'utf8') || '').trim()
              const current_owner = raw ? (JSON.parse(raw) as { token?: unknown }) : null
              if (current_owner?.token === token) {
                fs.unlinkSync(slot_file)
              }
            } catch {}
          },
        }
      } catch (error) {
        if (!(error instanceof Error) || (error as NodeJS.ErrnoException).code !== 'EEXIST') {
          throw error
        }

        try {
          const raw = String(fs.readFileSync(slot_file, 'utf8') || '').trim()
          const current_owner = raw ? (JSON.parse(raw) as { pid?: unknown }) : null
          const current_pid = typeof current_owner?.pid === 'number' ? current_owner.pid : null
          if (current_pid != null) {
            try {
              process.kill(current_pid, 0)
              continue
            } catch {}
          }

          const slot_age_ms = Date.now() - fs.statSync(slot_file).mtimeMs
          if (current_pid != null || slot_age_ms >= MULTIPROCESS_STALE_LOCK_MS) {
            fs.unlinkSync(slot_file)
          }
        } catch {}
      }
    }

    const sleep_ms = Math.min(retry_delay_ms, remaining_ms ?? retry_delay_ms)
    if (sleep_ms > 0) {
      await sleep(sleep_ms)
    }
    retry_delay_ms = Math.min(retry_delay_ms * 2, 1000)
  }

  if (!semaphore_lax) {
    throw new SemaphoreTimeoutError(
      `Failed to acquire semaphore "${scoped_key}" within ${semaphore_timeout_seconds}s (limit=${semaphore_limit})`,
      { semaphore_name: scoped_key, semaphore_limit, timeout_seconds: semaphore_timeout_seconds ?? 0 }
    )
  }

  return null
}

/** Run fn() with a timeout. Rejects with RetryTimeoutError if the timeout fires first. */
async function _runWithTimeout<T>(fn: () => Promise<T>, timeout_ms: number, attempt: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let settled = false

    const timer = setTimeout(() => {
      if (!settled) {
        settled = true
        reject(
          new RetryTimeoutError(`Timed out after ${timeout_ms / 1000}s (attempt ${attempt})`, {
            timeout_seconds: timeout_ms / 1000,
            attempt,
          })
        )
      }
    }, timeout_ms)

    fn().then(
      (value) => {
        if (!settled) {
          settled = true
          clearTimeout(timer)
          resolve(value)
        }
      },
      (error) => {
        if (!settled) {
          settled = true
          clearTimeout(timer)
          reject(error)
        }
      }
    )
  })
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
