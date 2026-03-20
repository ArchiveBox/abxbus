declare const process: { versions?: { node?: string } } | undefined

type AsyncLocalStorageLike = {
  getStore(): unknown
  run<T>(store: unknown, callback: () => T): T
  enterWith?(store: unknown): void
}

export type { AsyncLocalStorageLike }

// Cache the AsyncLocalStorage constructor so multiple modules can create separate instances.
let _AsyncLocalStorageClass: (new () => AsyncLocalStorageLike) | null = null

const is_node = typeof process !== 'undefined' && typeof process.versions !== 'undefined' && typeof process.versions.node === 'string'

const loadAsyncLocalStorageClass = (): (new () => AsyncLocalStorageLike) | null => {
  if (!is_node) return null

  // Prefer process.getBuiltinModule when available (works in ESM and CJS without top-level await).
  const maybe_process = (globalThis as { process?: { getBuiltinModule?: (name: string) => unknown } }).process
  const get_builtin_module = maybe_process?.getBuiltinModule
  if (typeof get_builtin_module === 'function') {
    const mod = get_builtin_module('node:async_hooks') as { AsyncLocalStorage?: new () => AsyncLocalStorageLike } | undefined
    if (mod?.AsyncLocalStorage) {
      return mod.AsyncLocalStorage
    }
  }

  // Fallback for older Node runtimes where require is available.
  try {
    const maybe_require = Function('return typeof require === "function" ? require : undefined')() as
      | ((specifier: string) => { AsyncLocalStorage?: new () => AsyncLocalStorageLike })
      | undefined
    const mod = maybe_require?.('node:async_hooks')
    if (mod?.AsyncLocalStorage) {
      return mod.AsyncLocalStorage
    }
  } catch {
    return null
  }

  return null
}

_AsyncLocalStorageClass = loadAsyncLocalStorageClass()

/** Create a new AsyncLocalStorage instance, or null if unavailable (e.g. in browsers). */
export const createAsyncLocalStorage = (): AsyncLocalStorageLike | null => {
  if (!_AsyncLocalStorageClass) return null
  return new _AsyncLocalStorageClass()
}

// The primary AsyncLocalStorage instance used for event dispatch context propagation.
export let async_local_storage: AsyncLocalStorageLike | null = _AsyncLocalStorageClass ? new _AsyncLocalStorageClass() : null

export const captureAsyncContext = (): unknown | null => {
  if (!async_local_storage) {
    return null
  }
  return async_local_storage.getStore() ?? null
}

export const _runWithAsyncContext = <T>(context: unknown | null, fn: () => T): T => {
  if (!async_local_storage) {
    return fn()
  }
  return async_local_storage.run(context ?? undefined, fn)
}

export const hasAsyncLocalStorage = (): boolean => async_local_storage !== null
