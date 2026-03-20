export type PerfScenarioResult = Record<string, unknown>

export type PerfScenarioInput = {
  runtimeName: string
  api: {
    BaseEvent: unknown
    EventBus: unknown
    EventHandlerTimeoutError: unknown
    EventHandlerCancelledError: unknown
  }
  now: () => number
  sleep: (ms: number) => Promise<void>
  log: (message: string) => void
  getMemoryUsage: () => { rss: number; heapUsed: number }
  forceGc?: () => void
  limits: {
    singleRunMs: number
    worstCaseMs: number
    maxHeapDeltaAfterGcMb?: number
  }
}

export const PERF_SCENARIO_IDS: readonly string[]
export function runPerfScenarioById(input: PerfScenarioInput, scenarioId: string): Promise<PerfScenarioResult>
export function runAllPerfScenarios(input: PerfScenarioInput): Promise<PerfScenarioResult[]>
export function runPerf50kEvents(input: PerfScenarioInput): Promise<PerfScenarioResult>
export function runPerfEphemeralBuses(input: PerfScenarioInput): Promise<PerfScenarioResult>
export function runPerfSingleEventManyFixedHandlers(input: PerfScenarioInput): Promise<PerfScenarioResult>
export function runPerfOnOffChurn(input: PerfScenarioInput): Promise<PerfScenarioResult>
export function runPerfWorstCase(input: PerfScenarioInput): Promise<PerfScenarioResult>
export function runCleanupEquivalence(input: PerfScenarioInput): Promise<PerfScenarioResult>
