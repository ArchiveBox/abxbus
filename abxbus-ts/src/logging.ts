import { BaseEvent } from './base_event.js'
import { EventResult } from './event_result.js'
import { EventHandlerCancelledError, EventHandlerTimeoutError } from './event_handler.js'

type LogTreeBus = {
  name: string
  event_history: {
    values(): IterableIterator<BaseEvent>
    has(event_id: string): boolean
  }
  toString?: () => string
}

export const logTree = (bus: LogTreeBus): string => {
  const parent_to_children = new Map<string, BaseEvent[]>()

  const addChild = (parent_id: string, child: BaseEvent): void => {
    const existing = parent_to_children.get(parent_id) ?? []
    existing.push(child)
    parent_to_children.set(parent_id, existing)
  }

  const root_events: BaseEvent[] = []
  const seen = new Set<string>()

  for (const event of bus.event_history.values()) {
    const parent_id = event.event_parent_id
    if (!parent_id || parent_id === event.event_id || !bus.event_history.has(parent_id)) {
      if (!seen.has(event.event_id)) {
        root_events.push(event)
        seen.add(event.event_id)
      }
    }
  }

  if (root_events.length === 0) {
    return '(No events in history)'
  }

  const nodes_by_id = new Map<string, BaseEvent>()
  for (const root of root_events) {
    nodes_by_id.set(root.event_id, root)
    for (const descendant of root.event_descendants) {
      nodes_by_id.set(descendant.event_id, descendant)
    }
  }

  for (const node of nodes_by_id.values()) {
    const parent_id = node.event_parent_id
    if (!parent_id || parent_id === node.event_id) {
      continue
    }
    if (!nodes_by_id.has(parent_id)) {
      continue
    }
    addChild(parent_id, node)
  }

  for (const children of parent_to_children.values()) {
    children.sort((a, b) => (a.event_created_at < b.event_created_at ? -1 : a.event_created_at > b.event_created_at ? 1 : 0))
  }

  const lines: string[] = []
  const bus_label = typeof bus.toString === 'function' ? bus.toString() : bus.name
  lines.push(`📊 Event History Tree for ${bus_label}`)
  lines.push('='.repeat(80))

  root_events.sort((a, b) => (a.event_created_at < b.event_created_at ? -1 : a.event_created_at > b.event_created_at ? 1 : 0))
  const visited = new Set<string>()
  root_events.forEach((event, index) => {
    lines.push(buildTreeLine(event, '', index === root_events.length - 1, parent_to_children, visited))
  })

  lines.push('='.repeat(80))

  return lines.join('\n')
}

export const buildTreeLine = (
  event: BaseEvent,
  indent: string,
  is_last: boolean,
  parent_to_children: Map<string, BaseEvent[]>,
  visited: Set<string>
): string => {
  const connector = is_last ? '└── ' : '├── '
  const status_icon = event.event_status === 'completed' ? '✅' : event.event_status === 'started' ? '🏃' : '⏳'

  const created_at = formatTimestamp(event.event_created_at)
  let timing = `[${created_at}`
  if (event.event_completed_at) {
    const created_ms = Date.parse(event.event_created_at)
    const completed_ms = Date.parse(event.event_completed_at)
    if (!Number.isNaN(created_ms) && !Number.isNaN(completed_ms)) {
      const duration = (completed_ms - created_ms) / 1000
      timing += ` (${duration.toFixed(3)}s)`
    }
  }
  timing += ']'

  const line = `${indent}${connector}${status_icon} ${event.event_type}#${event.event_id.slice(-4)} ${timing}`

  if (visited.has(event.event_id)) {
    return line
  }
  visited.add(event.event_id)

  const extension = is_last ? '    ' : '│   '
  const new_indent = indent + extension

  const result_items: Array<{ type: 'result'; result: EventResult } | { type: 'child'; child: BaseEvent }> = []
  for (const result of event.event_results.values()) {
    result_items.push({ type: 'result', result })
  }
  const children = parent_to_children.get(event.event_id) ?? []
  const printed_child_ids = new Set<string>(event.event_results.size > 0 ? event.event_results.keys() : [])
  for (const child of children) {
    if (!printed_child_ids.has(child.event_id) && !child.event_emitted_by_handler_id) {
      result_items.push({ type: 'child', child })
      printed_child_ids.add(child.event_id)
    }
  }

  if (result_items.length === 0) {
    return line
  }

  const child_lines: string[] = []
  result_items.forEach((item, index) => {
    const is_last_item = index === result_items.length - 1
    if (item.type === 'result') {
      child_lines.push(buildResultLine(item.result, new_indent, is_last_item, parent_to_children, visited))
    } else {
      child_lines.push(buildTreeLine(item.child, new_indent, is_last_item, parent_to_children, visited))
    }
  })

  return [line, ...child_lines].join('\n')
}

export const buildResultLine = (
  result: EventResult,
  indent: string,
  is_last: boolean,
  parent_to_children: Map<string, BaseEvent[]>,
  visited: Set<string>
): string => {
  const connector = is_last ? '└── ' : '├── '
  const status_icon = result.status === 'completed' ? '✅' : result.status === 'error' ? '❌' : result.status === 'started' ? '🏃' : '⏳'

  const handler_label =
    result.handler_name && result.handler_name !== 'anonymous'
      ? result.handler_name
      : result.handler_file_path
        ? result.handler_file_path
        : 'anonymous'
  const handler_display = `${result.eventbus_label}.${handler_label}#${result.handler_id.slice(-4)}`
  let line = `${indent}${connector}${status_icon} ${handler_display}`

  if (result.started_at) {
    line += ` [${formatTimestamp(result.started_at)}`
    if (result.completed_at) {
      const started_ms = Date.parse(result.started_at)
      const completed_ms = Date.parse(result.completed_at)
      if (!Number.isNaN(started_ms) && !Number.isNaN(completed_ms)) {
        const duration = (completed_ms - started_ms) / 1000
        line += ` (${duration.toFixed(3)}s)`
      }
    }
    line += ']'
  }

  if (result.status === 'error' && result.error) {
    if (result.error instanceof EventHandlerTimeoutError) {
      line += ` ⏱️ Timeout: ${result.error.message}`
    } else if (result.error instanceof EventHandlerCancelledError) {
      line += ` 🚫 Cancelled: ${result.error.message}`
    } else {
      const error_name = result.error instanceof Error ? result.error.name : 'Error'
      const error_message = result.error instanceof Error ? result.error.message : String(result.error)
      line += ` ☠️ ${error_name}: ${error_message}`
    }
  } else if (result.status === 'completed') {
    line += ` → ${formatResultValue(result.result)}`
  }

  const extension = is_last ? '    ' : '│   '
  const new_indent = indent + extension

  const direct_children = result.event_children
  if (direct_children.length === 0) {
    return line
  }

  const child_lines: string[] = []
  const parent_children = parent_to_children.get(result.event_id) ?? []
  const emitted_children = parent_children.filter((child) => child.event_emitted_by_handler_id === result.handler_id)
  const children_by_id = new Map<string, BaseEvent>()
  direct_children.forEach((child) => {
    children_by_id.set(child.event_id, child)
  })
  emitted_children.forEach((child) => {
    if (!children_by_id.has(child.event_id)) {
      children_by_id.set(child.event_id, child)
    }
  })
  const children_to_print = Array.from(children_by_id.values()).filter((child) => !visited.has(child.event_id))

  children_to_print.forEach((child, index) => {
    child_lines.push(buildTreeLine(child, new_indent, index === children_to_print.length - 1, parent_to_children, visited))
  })

  return [line, ...child_lines].join('\n')
}

export const formatTimestamp = (value?: string): string => {
  if (!value) {
    return 'N/A'
  }
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return 'N/A'
  }
  return date.toISOString().slice(11, 23)
}

export const formatResultValue = (value: unknown): string => {
  if (value === null || value === undefined) {
    return 'None'
  }
  if (value instanceof BaseEvent) {
    return `Event(${value.event_type}#${value.event_id.slice(-4)})`
  }
  if (typeof value === 'string') {
    return JSON.stringify(value)
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value)
  }
  if (Array.isArray(value)) {
    return `list(${value.length} items)`
  }
  if (typeof value === 'object') {
    return `dict(${Object.keys(value as Record<string, unknown>).length} items)`
  }
  return `${typeof value}(...)`
}
