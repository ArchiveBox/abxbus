export async function _runWithTimeout<T>(timeout_seconds: number | null, on_timeout: () => Error, fn: () => Promise<T>): Promise<T> {
  const task = Promise.resolve().then(fn)
  if (timeout_seconds === null) {
    return await task
  }
  const timeout_ms = timeout_seconds * 1000
  return await new Promise<T>((resolve, reject) => {
    let settled = false
    const finishResolve = (value: T) => {
      if (settled) {
        return
      }
      settled = true
      clearTimeout(timer)
      resolve(value)
    }
    const finishReject = (error: unknown) => {
      if (settled) {
        return
      }
      settled = true
      clearTimeout(timer)
      reject(error)
    }
    const timer = setTimeout(() => {
      if (settled) {
        return
      }
      settled = true
      reject(on_timeout())
      void task.catch(() => undefined)
    }, timeout_ms)
    task.then(finishResolve).catch(finishReject)
  })
}

export async function _runWithSlowMonitor<T>(slow_timer: ReturnType<typeof setTimeout> | null, fn: () => Promise<T>): Promise<T> {
  try {
    return await fn()
  } finally {
    if (slow_timer) {
      clearTimeout(slow_timer)
    }
  }
}

export async function _runWithAbortMonitor<T>(fn: () => T | Promise<T>, abort_signal: Promise<never>): Promise<T> {
  const task = Promise.resolve().then(fn)
  const raced = Promise.race([task, abort_signal])
  void task.catch(() => undefined)
  return await raced
}
