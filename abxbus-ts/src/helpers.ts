const MONOTONIC_DATETIME_REGEX = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$/
const MONOTONIC_DATETIME_LENGTH = 30 // YYYY-MM-DDTHH:MM:SS.fffffffffZ
const NS_PER_MS = 1_000_000n
const NS_PER_SECOND = 1_000_000_000n

const has_performance_now = typeof performance !== 'undefined' && typeof performance.now === 'function'
const monotonic_clock_anchor_ms = has_performance_now ? performance.now() : 0
const monotonic_epoch_anchor_ns = BigInt(Date.now()) * NS_PER_MS
let last_monotonic_datetime_ns = monotonic_epoch_anchor_ns

function assertYearRange(date: Date, context: string): void {
  const year = date.getUTCFullYear()
  if (year <= 1990 || year >= 2500) {
    throw new Error(`${context} year must be >1990 and <2500, got ${year}`)
  }
}

function formatEpochNs(epoch_ns: bigint): string {
  const epoch_ms = Number(epoch_ns / NS_PER_MS)
  const date = new Date(epoch_ms)
  if (Number.isNaN(date.getTime())) {
    throw new Error(`Failed to format datetime from epoch ns: ${epoch_ns.toString()}`)
  }
  assertYearRange(date, 'monotonicDatetime()')
  const base = date.toISOString().slice(0, 19)
  const fraction = (epoch_ns % NS_PER_SECOND).toString().padStart(9, '0')
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

  const elapsed_ms = has_performance_now ? performance.now() - monotonic_clock_anchor_ms : 0
  const elapsed_ns = BigInt(Math.max(0, Math.floor(elapsed_ms * 1_000_000)))
  let epoch_ns = monotonic_epoch_anchor_ns + elapsed_ns
  if (epoch_ns <= last_monotonic_datetime_ns) {
    epoch_ns = last_monotonic_datetime_ns + 1n
  }
  last_monotonic_datetime_ns = epoch_ns
  return formatEpochNs(epoch_ns)
}
