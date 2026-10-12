export async function _runWithTimeout<T>(timeout_seconds: number | null, on_timeout: () => Error, fn: () => Promise<T>): Promise<T> {
  const task = Promise.resolve().then(fn)
  if (timeout_seconds === null || timeout_seconds <= 0) {
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

export type WarningTimerHandle = ReturnType<typeof setTimeout> | { cancel: () => void }

type ScheduledWarning = {
  deadline_ms: number
  callback: () => void
}

const DEFERRED_TIMER_ARM_THRESHOLD_MS = 1000
const scheduled_warnings = new Map<number, ScheduledWarning>()
let scheduled_warning_seq = 0
let scheduled_warning_timer: ReturnType<typeof setTimeout> | null = null
let scheduled_warning_timer_deadline_ms = Infinity

const scheduleWarningDriver = (deadline_ms: number): void => {
  if (scheduled_warning_timer && deadline_ms >= scheduled_warning_timer_deadline_ms) {
    return
  }
  if (scheduled_warning_timer) {
    clearTimeout(scheduled_warning_timer)
  }
  scheduled_warning_timer_deadline_ms = deadline_ms
  const delay_ms = Math.max(0, deadline_ms - Date.now())
  scheduled_warning_timer = setTimeout(runDueWarnings, delay_ms)
  scheduled_warning_timer.unref?.()
}

const runDueWarnings = (): void => {
  scheduled_warning_timer = null
  scheduled_warning_timer_deadline_ms = Infinity
  const now_ms = Date.now()
  let next_deadline_ms = Infinity

  for (const [id, entry] of scheduled_warnings.entries()) {
    if (entry.deadline_ms <= now_ms) {
      scheduled_warnings.delete(id)
      entry.callback()
    } else if (entry.deadline_ms < next_deadline_ms) {
      next_deadline_ms = entry.deadline_ms
    }
  }

  if (Number.isFinite(next_deadline_ms)) {
    scheduleWarningDriver(next_deadline_ms)
  }
}

export const scheduleWarningTimer = (delay_ms: number, callback: () => void): WarningTimerHandle => {
  const id = ++scheduled_warning_seq
  const normalized_delay_ms = Math.max(0, delay_ms)
  const deadline_ms = Date.now() + normalized_delay_ms
  if (normalized_delay_ms >= DEFERRED_TIMER_ARM_THRESHOLD_MS) {
    scheduled_warnings.set(id, { deadline_ms, callback })
    scheduleWarningDriver(deadline_ms)
    return {
      cancel: () => {
        scheduled_warnings.delete(id)
      },
    }
  }
  scheduled_warnings.set(id, { deadline_ms, callback })
  scheduleWarningDriver(deadline_ms)
  return {
    cancel: () => {
      scheduled_warnings.delete(id)
    },
  }
}

export const cancelWarningTimer = (handle: WarningTimerHandle | null): void => {
  if (!handle) {
    return
  }
  if (typeof (handle as { cancel?: unknown }).cancel === 'function') {
    ;(handle as { cancel: () => void }).cancel()
    return
  }
  clearTimeout(handle as ReturnType<typeof setTimeout>)
}

export async function _runWithSlowMonitor<T>(slow_timer: WarningTimerHandle | null, fn: () => Promise<T>): Promise<T> {
  try {
    return await fn()
  } finally {
    if (slow_timer) {
      cancelWarningTimer(slow_timer)
    }
  }
}

export async function _runWithAbortMonitor<T>(fn: () => T | Promise<T>, abort_signal: Promise<never>): Promise<T> {
  const task = Promise.resolve().then(fn)
  const raced = Promise.race([task, abort_signal])
  void task.catch(() => undefined)
  return await raced
}
