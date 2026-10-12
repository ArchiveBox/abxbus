const MONOTONIC_DATETIME_REGEX = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$/
const MONOTONIC_DATETIME_LENGTH = 30 // YYYY-MM-DDTHH:MM:SS.fffffffffZ
const NS_PER_MS_NUMBER = 1_000_000

const has_performance_now = typeof performance !== 'undefined' && typeof performance.now === 'function'
const monotonic_clock_anchor_ms = has_performance_now ? performance.now() : 0
const monotonic_epoch_anchor_ms = Date.now()
let last_monotonic_epoch_ms = monotonic_epoch_anchor_ms
let last_monotonic_extra_ns = 0

function assertYearRange(date: Date, context: string): void {
  const year = date.getUTCFullYear()
  if (year <= 1990 || year >= 2500) {
    throw new Error(`${context} year must be >1990 and <2500, got ${year}`)
  }
}

function formatEpochMsNs(epoch_ms: number, extra_ns: number): string {
  const date = new Date(epoch_ms)
  if (Number.isNaN(date.getTime())) {
    throw new Error(`Failed to format datetime from epoch ms: ${epoch_ms}`)
  }
  assertYearRange(date, 'monotonicDatetime()')
  const base = date.toISOString().slice(0, 19)
  const ms_within_second = ((epoch_ms % 1000) + 1000) % 1000
  const fraction_ns = ms_within_second * NS_PER_MS_NUMBER + extra_ns
  const fraction = fraction_ns.toString().padStart(9, '0')
  const normalized = `${base}.${fraction}Z`
  if (normalized.length !== MONOTONIC_DATETIME_LENGTH) {
    throw new Error(`Expected canonical datetime length ${MONOTONIC_DATETIME_LENGTH}, got ${normalized.length}: ${normalized}`)
  }
  return normalized
}

export function monotonicDatetime(isostring?: string): string {
  if (isostring !== undefined) {
    if (typeof isostring !== 'string') {
      throw new Error(`monotonicDatetime(isostring?) requires string | undefined, got ${typeof isostring}`)
    }
    const match = MONOTONIC_DATETIME_REGEX.exec(isostring)
    if (!match) {
      throw new Error(`Invalid ISO datetime: ${isostring}`)
    }
    const parsed = new Date(isostring)
    if (Number.isNaN(parsed.getTime())) {
      throw new Error(`Invalid ISO datetime: ${isostring}`)
    }
    assertYearRange(parsed, 'monotonicDatetime(isostring)')
    const base = parsed.toISOString().slice(0, 19)
    const fraction = (match[7] ?? '').padEnd(9, '0')
    const normalized = `${base}.${fraction}Z`
    if (normalized.length !== MONOTONIC_DATETIME_LENGTH) {
      throw new Error(`Expected canonical datetime length ${MONOTONIC_DATETIME_LENGTH}, got ${normalized.length}: ${normalized}`)
    }
    return normalized
  }

  const elapsed_ms = has_performance_now ? Math.max(0, performance.now() - monotonic_clock_anchor_ms) : 0
  const elapsed_ms_whole = Math.floor(elapsed_ms)
  let epoch_ms = monotonic_epoch_anchor_ms + elapsed_ms_whole
  let extra_ns = Math.floor((elapsed_ms - elapsed_ms_whole) * NS_PER_MS_NUMBER)
  if (epoch_ms < last_monotonic_epoch_ms || (epoch_ms === last_monotonic_epoch_ms && extra_ns <= last_monotonic_extra_ns)) {
    epoch_ms = last_monotonic_epoch_ms
    extra_ns = last_monotonic_extra_ns + 1
    if (extra_ns >= NS_PER_MS_NUMBER) {
      epoch_ms += 1
      extra_ns = 0
    }
  }
  last_monotonic_epoch_ms = epoch_ms
  last_monotonic_extra_ns = extra_ns
  return formatEpochMsNs(epoch_ms, extra_ns)
}
