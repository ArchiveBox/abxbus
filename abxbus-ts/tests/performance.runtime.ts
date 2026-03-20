import { BaseEvent, EventBus, EventHandlerCancelledError, EventHandlerTimeoutError } from '../src/index.js'
import { PERF_SCENARIO_IDS, runAllPerfScenarios, runPerfScenarioById } from './performance.scenarios.js'

declare const Bun: { gc?: (full?: boolean) => void } | undefined
declare const Deno:
  | {
      args?: string[]
      memoryUsage?: () => { rss: number; heapUsed: number }
      [key: symbol]: unknown
    }
  | undefined

const runtime = typeof Bun !== 'undefined' && Bun ? 'bun' : typeof Deno !== 'undefined' && Deno ? 'deno' : 'node'

const getCliArgs = () => {
  const processArgs = typeof process !== 'undefined' && Array.isArray(process.argv) ? process.argv.slice(2) : []
  if (processArgs.length > 0) return processArgs
  return typeof Deno !== 'undefined' && Deno && Array.isArray(Deno.args) ? (Deno.args ?? []) : []
}

const getScenarioArg = () => {
  const args = getCliArgs()
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i]
    if (!arg) continue
    if (arg.startsWith('--scenario=')) {
      const value = arg.slice('--scenario='.length).trim()
      return value.length > 0 ? value : null
    }
    if (arg === '--scenario') {
      const value = args[i + 1]?.trim()
      return value && value.length > 0 ? value : null
    }
  }
  return null
}

const getMemoryUsage = () => {
  if (typeof process !== 'undefined' && typeof process.memoryUsage === 'function') {
    return process.memoryUsage()
  }
  if (typeof Deno !== 'undefined' && Deno && typeof Deno.memoryUsage === 'function') {
    return Deno.memoryUsage()
  }
  return { heapUsed: 0, rss: 0 }
}

const forceGc = () => {
  if (runtime === 'bun' && typeof Bun !== 'undefined' && Bun && typeof Bun.gc === 'function') {
    Bun.gc(true)
    Bun.gc(true)
    return
  }

  const maybeGlobalGc = (globalThis as { gc?: () => void }).gc
  if (typeof maybeGlobalGc === 'function') {
    for (let i = 0; i < 4; i += 1) maybeGlobalGc()
  }
}

const main = async () => {
  const scenario = getScenarioArg()
  console.log(`[${runtime}] runtime perf harness starting`)

  const input = {
    runtimeName: runtime,
    api: { BaseEvent, EventBus, EventHandlerTimeoutError, EventHandlerCancelledError },
    now: () => performance.now(),
    sleep: (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)),
    log: (message: string) => console.log(message),
    getMemoryUsage,
    forceGc,
    limits: {
      singleRunMs: 90_000,
      worstCaseMs: 180_000,
      maxHeapDeltaAfterGcMb: 0,
    },
  }

  if (scenario) {
    if (!PERF_SCENARIO_IDS.includes(scenario)) {
      throw new Error(`Unknown --scenario value "${scenario}". Expected one of: ${PERF_SCENARIO_IDS.join(', ')}`)
    }
    await runPerfScenarioById(input, scenario)
  } else {
    await runAllPerfScenarios(input)
  }

  console.log(`[${runtime}] runtime perf harness complete`)
}

await main()
