from __future__ import annotations

import asyncio
import json
from datetime import datetime
from typing import Annotated, Any

try:
    from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
    from fastapi.responses import HTMLResponse, JSONResponse
except ModuleNotFoundError as exc:  # pragma: no cover - optional UI dependency
    raise ModuleNotFoundError("Install 'fastapi' to run the abxbus UI module.") from exc

from . import db
from .config import resolve_db_path

app = FastAPI(title='abxbus event monitor', version='0.1.0')


def _format_timestamp(value: str | None) -> str | None:
    if not value:
        return None
    # SQLite timestamp string -> ISO 8601
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00')).isoformat()
    except ValueError:
        return value


async def _fetch_events(limit: int) -> list[dict[str, Any]]:
    rows = await db.fetch_events(limit)
    for row in rows:
        row['inserted_at'] = _format_timestamp(row.get('inserted_at'))
    return rows


async def _fetch_results(limit: int) -> list[dict[str, Any]]:
    rows = await db.fetch_results(limit)
    for row in rows:
        row['inserted_at'] = _format_timestamp(row.get('inserted_at'))
    return rows


def _parse_nonnegative_int(value: str | None, default: int = 0) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(0, parsed)


@app.get('/', response_class=HTMLResponse)
async def index() -> str:
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="utf-8" />
        <title>abxbus Event Monitor</title>
        <style>
            :root {
                color-scheme: dark;
                font-family: 'Inter', system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                font-size: 15px;
                line-height: 1.45;
            }
            body { margin: 0; background: #0f172a; color: #e2e8f0; min-height: 100vh; display: flex; flex-direction: column; }
            header { padding: 1.35rem 1.8rem 1rem; border-bottom: 1px solid rgba(148, 163, 184, 0.22); }
            h1 { margin: 0 0 0.55rem; font-size: 1.7rem; font-weight: 600; }
            .meta { font-size: 0.92rem; display: flex; gap: 0.85rem; flex-wrap: wrap; align-items: center; opacity: 0.92; }
            .status-indicator { padding: 0.2rem 0.55rem; border-radius: 999px; font-size: 0.82rem; background: rgba(148, 163, 184, 0.24); }
            main { flex: 1; overflow-y: auto; padding: 1.1rem 1.8rem 1.5rem; }
            .toolbar { display: flex; flex-wrap: wrap; gap: 0.55rem; margin-bottom: 0.75rem; align-items: center; font-size: 0.86rem; }
            .toolbar input,
            .toolbar select { background: rgba(15,23,42,0.72); border: 1px solid rgba(148,163,184,0.35); color: inherit; border-radius: 0.5rem; padding: 0.4rem 0.65rem; font-size: 0.86rem; min-width: 9.5rem; }
            .toolbar label { display: flex; align-items: center; gap: 0.35rem; }
            #events-tree { display: grid; gap: 0.6rem; grid-template-columns: repeat(auto-fit, minmax(420px, 1fr)); align-items: start; }
            
            .tree-node { background: rgba(15,23,42,0.36); border: 1px solid rgba(148,163,184,0.22); border-radius: 0.6rem; padding: 0.55rem 0.75rem 0.65rem; box-shadow: 0 12px 24px rgba(8,11,25,0.35); }
            .tree-node > details > summary { list-style: none; cursor: pointer; padding: 0; margin: 0; outline: none; }
            .tree-node summary::-webkit-details-marker { display: none; }
            .event-summary { display: flex; flex-wrap: wrap; gap: 0.4rem; align-items: center; }
            .pill { display: inline-flex; align-items: center; gap: 0.35rem; border-radius: 999px; padding: 0.22rem 0.6rem; border: 1px solid rgba(148,163,184,0.32); background: rgba(13,23,42,0.78); font-size: 0.85rem; font-weight: 500; color: rgba(226,232,240,0.92); }
            .pill-type { text-transform: uppercase; letter-spacing: 0.055em; background: rgba(94,234,212,0.14); border-color: rgba(94,234,212,0.42); color: #5eead4; }
            .pill-muted { color: rgba(226,232,240,0.88); }
            .pill-status { font-weight: 600; letter-spacing: 0.05em; text-transform: uppercase; }
            .pill-status.pill-completed { background: rgba(16,185,129,0.2); border-color: rgba(16,185,129,0.5); color: #34d399; }
            .pill-status.pill-started { background: rgba(250,204,21,0.2); border-color: rgba(250,204,21,0.45); color: #facc15; }
            .pill-status.pill-pending { background: rgba(59,130,246,0.24); border-color: rgba(59,130,246,0.42); color: #60a5fa; }
            .pill-status.pill-error { background: rgba(239,68,68,0.22); border-color: rgba(239,68,68,0.48); color: #f87171; }
            .event-meta { margin-top: 0.5rem; padding: 0.45rem 0.55rem 0.3rem; background: rgba(15,23,42,0.46); border-radius: 0.55rem; border: 1px solid rgba(148,163,184,0.2); }
            .meta-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 0.35rem 0.55rem; }
            .meta-item { display: flex; flex-direction: column; gap: 0.18rem; padding: 0.22rem 0.5rem 0.28rem; background: rgba(13,22,40,0.62); border-radius: 0.45rem; border: 1px solid rgba(148,163,184,0.2); }
            .meta-label { font-size: 0.68rem; text-transform: uppercase; letter-spacing: 0.08em; color: rgba(203,213,225,0.75); display: flex; align-items: center; gap: 0.25rem; }
            .meta-label-icon { font-size: 0.78rem; }
            .meta-value { font-size: 0.88rem; font-weight: 600; color: rgba(226,232,240,0.96); }
            .meta-value code { font-size: 0.8rem; background: rgba(2,6,23,0.7); padding: 0.1rem 0.4rem; border-radius: 0.35rem; border: 1px solid rgba(148,163,184,0.26); }
            .results-section { margin-top: 0.45rem; }
            .results-title { font-size: 0.76rem; text-transform: uppercase; letter-spacing: 0.12em; color: rgba(148,163,184,0.85); margin-bottom: 0.3rem; }
            .results-table { width: 100%; border-collapse: collapse; font-size: 0.82rem; background: rgba(12,22,40,0.6); border-radius: 0.45rem; overflow: hidden; }
            .results-table th { text-align: left; padding: 0.3rem 0.45rem; font-size: 0.74rem; text-transform: uppercase; letter-spacing: 0.05em; color: rgba(148,163,184,0.9); background: rgba(30,41,59,0.6); }
            .results-table td { padding: 0.3rem 0.45rem; border-top: 1px solid rgba(148,163,184,0.16); color: rgba(226,232,240,0.92); }
            .results-table td pre { margin: 0; font-size: 0.78rem; white-space: pre-wrap; background: none; }
            .results-table tbody tr:hover { background: rgba(59,130,246,0.12); }
            .results-table details summary { cursor: pointer; color: rgba(125,211,252,0.92); }
            .results-table details[open] summary { color: rgba(56,189,248,0.95); }
            .results-table details ul { list-style: none; margin: 0.25rem 0 0; padding: 0; }
            .results-table details li { margin: 0.18rem 0; display: flex; flex-direction: column; gap: 0.12rem; font-size: 0.74rem; color: rgba(203,213,225,0.88); }
            .results-table details li::before { content: ""; }
            .results-table details li pre { font-size: 0.74rem; }
            .results-table details ul { list-style: none; margin: 0.25rem 0 0; padding: 0; }
            .results-table details li { margin: 0.18rem 0; display: flex; flex-direction: column; gap: 0.12rem; font-size: 0.74rem; color: rgba(203,213,225,0.88); }
            .results-table details li pre { font-size: 0.74rem; }
            .children { list-style: none; margin: 0.45rem 0 0.2rem 0; padding: 0; display: grid; gap: 0.4rem; }
            .children > li { border-left: 1px solid rgba(148,163,184,0.18); padding-left: 0.75rem; }
            .event-json { margin-top: 0.45rem; }
            .event-json summary { cursor: pointer; color: rgba(125,211,252,0.9); font-size: 0.8rem; }
            .event-json pre { margin-top: 0.35rem; max-height: 220px; overflow: auto; padding: 0.55rem; background: rgba(13,23,42,0.78); border-radius: 0.45rem; border: 1px solid rgba(148,163,184,0.2); font-size: 0.8rem; }
            .empty { text-align: center; padding: 2rem 0; color: rgba(148,163,184,0.72); font-size: 0.9rem; }
            @media (max-width: 900px) {
                header, main { padding: 1rem 1.1rem; }
                .toolbar input, .toolbar select { min-width: 0; flex: 1 1 140px; }
                #events-tree { grid-template-columns: 1fr; }
            }
        </style>
    </head>
    <body>
        <header>
            <h1>abxbus Event Monitor</h1>
            <div class="meta">
                <span>Database: <code id="db-path"></code></span>
                <span class="status-indicator" id="ws-status">connecting…</span>
                <span id="counts"></span>
            </div>
        </header>
        <main>
            <div id="db-warning" class="empty" style="display:none;"></div>
            <div class="toolbar">
                <label>
                    🔍
                    <input id="search-input" type="search" placeholder="Search events and results…" />
                </label>
                <label>
                    Status
                    <select id="filter-status">
                        <option value="all">All statuses</option>
                        <option value="completed">Completed</option>
                        <option value="started">Started</option>
                        <option value="pending">Pending</option>
                        <option value="error">Error</option>
                    </select>
                </label>
                <label>
                    EventBus
                    <select id="filter-bus">
                        <option value="all">All buses</option>
                    </select>
                </label>
            </div>
            <div id="events-tree"></div>
        </main>
        <script>
            const state = {
                events: new Map(),
                results: new Map(),
                busNames: new Set(),
                filters: { search: '', status: 'all', bus: 'all' },
                lastEventRowId: 0,
                lastResultRowId: 0,
            };

            const dbPathEl = document.getElementById('db-path');
            const wsStatus = document.getElementById('ws-status');
            const countsEl = document.getElementById('counts');
            const warningEl = document.getElementById('db-warning');
            const treeContainer = document.getElementById('events-tree');
            const searchInput = document.getElementById('search-input');
            const statusSelect = document.getElementById('filter-status');
            const busSelect = document.getElementById('filter-bus');

            const SAFE_DEFAULT = Object.freeze({});

            function escapeHtml(value) {
                if (value === null || value === undefined) return '';
                return value.toString().replace(/[&<>"']/g, (char) => ({
                    '&': '&amp;',
                    '<': '&lt;',
                    '>': '&gt;',
                    '"': '&quot;',
                    "'": '&#39;',
                })[char]);
            }

            function safeJsonParse(raw) {
                if (!raw || typeof raw !== 'string') return SAFE_DEFAULT;
                try { return JSON.parse(raw); } catch { return SAFE_DEFAULT; }
            }

            function renderMetaItem(label, value, options = {}) {
                const { code = false, icon = '' } = options;
                const safeValue = value !== undefined && value !== null && value !== '' ? String(value) : '—';
                const formatted = code ? `<code>${escapeHtml(safeValue)}</code>` : escapeHtml(safeValue);
                const iconHtml = icon ? `<span class="meta-label-icon">${escapeHtml(icon)}</span>` : '';
                return `<div class="meta-item"><div class="meta-label">${iconHtml}${escapeHtml(label)}</div><div class="meta-value">${formatted}</div></div>`;
            }

            function ingestEvents(rows) {
                rows.forEach((row) => {
                    if (typeof row.id === 'number' && row.id > state.lastEventRowId) {
                        state.lastEventRowId = row.id;
                    }
                    const current = state.events.get(row.event_id);
                    const currentRowId = current && typeof current._row_id === 'number' ? current._row_id : -Infinity;
                    if (row.id <= currentRowId) {
                        return;
                    }

                    const data = safeJsonParse(row.event_json);
                    const record = { ...row, data, _row_id: row.id };
                    state.events.set(row.event_id, record);
                    if (record.eventbus_name) state.busNames.add(record.eventbus_name);
                });
            }

            function ingestResults(rows) {
                rows.forEach((row) => {
                    if (typeof row.id === 'number' && row.id > state.lastResultRowId) {
                        state.lastResultRowId = row.id;
                    }
                    const list = state.results.get(row.event_id) || [];
                    const idx = list.findIndex((item) => item.id === row.id);
                    if (idx >= 0) {
                        list[idx] = { ...list[idx], ...row };
                    } else {
                        list.push({ ...row });
                    }
                    list.sort((a, b) => (b.id || 0) - (a.id || 0));
                    state.results.set(row.event_id, list);
                });
            }

            function updateBusOptions() {
                const previous = busSelect.value;
                const options = ['all', ...Array.from(state.busNames).sort()];
                busSelect.innerHTML = options.map((name) => {
                    const label = name === 'all' ? 'All buses' : name;
                    return `<option value="${escapeHtml(name)}">${escapeHtml(label)}</option>`;
                }).join('');
                if (options.includes(previous)) {
                    busSelect.value = previous;
                } else {
                    state.filters.bus = 'all';
                }
            }

            function buildTree() {
                const nodes = new Map();
                state.events.forEach((event) => {
                    const results = aggregateResults(state.results.get(event.event_id) || []);
                    nodes.set(event.event_id, { ...event, children: [], results });
                });
                const roots = [];
                nodes.forEach((node) => {
                    const parentId = node.data?.event_parent_id;
                    if (parentId && nodes.has(parentId)) {
                        nodes.get(parentId).children.push(node);
                    } else {
                        roots.push(node);
                    }
                });
                function sortNodes(list) {
                    list.sort((a, b) => {
                        const rowA = Number(a._row_id) || 0;
                        const rowB = Number(b._row_id) || 0;
                        return rowB - rowA;
                    });
                    list.forEach((child) => sortNodes(child.children));
                }
                sortNodes(roots);
                return roots;
            }

            function matchesFilters(node) {
                const statusMatch = state.filters.status === 'all' || (node.event_status || '').toLowerCase() === state.filters.status;
                const busMatch = state.filters.bus === 'all' || node.eventbus_name === state.filters.bus;
                if (!statusMatch || !busMatch) return false;
                if (!state.filters.search) return true;
                const haystack = [
                    node.event_type,
                    node.event_id,
                    node.eventbus_name,
                    node.phase,
                    JSON.stringify(node.data || SAFE_DEFAULT),
                ].join(' ').toLowerCase();
                return haystack.includes(state.filters.search);
            }

            function filterResults(results) {
                if (!state.filters.search) return results;
                return results.filter((result) =>
                    JSON.stringify(result || SAFE_DEFAULT).toLowerCase().includes(state.filters.search)
                );
            }

            function filterTree(nodes) {
                const filtered = [];
                for (const node of nodes) {
                    const children = filterTree(node.children);
                    const results = filterResults(node.results);
                    const matches = matchesFilters(node);
                    if (matches || children.length || results.length) {
                        filtered.push({ ...node, children, results });
                    }
                }
                return filtered;
            }

            function aggregateResults(rawResults) {
                const groups = new Map();
                rawResults.forEach((row) => {
                    const key = row.event_result_id || `${row.handler_name}-${row.event_id}`;
                    const group = groups.get(key) || {
                        handler_name: row.handler_name,
                        event_result_id: row.event_result_id,
                        attempts: [],
                        final_status: row.status || row.phase,
                        error_repr: row.error_repr,
                        result_repr: row.result_repr,
                        started_at: null,
                        completed_at: null,
                    };

                    if (row.status === 'started' || row.phase === 'started') {
                        group.started_at = row.inserted_at;
                    }
                    if (row.status === 'completed' || row.status === 'error') {
                        group.completed_at = row.inserted_at;
                        group.final_status = row.status;
                        group.error_repr = row.error_repr;
                        group.result_repr = row.result_repr;
                    }
                    group.attempts.push({
                        status: row.status || row.phase,
                        inserted_at: row.inserted_at,
                        result_repr: row.result_repr,
                        error_repr: row.error_repr,
                    });
                    groups.set(key, group);
                });

                return Array.from(groups.values()).sort((a, b) => {
                    const timeA = Date.parse(a.completed_at || a.started_at || '') || 0;
                    const timeB = Date.parse(b.completed_at || b.started_at || '') || 0;
                    return timeB - timeA;
                });
            }

            function renderResults(results) {
                return `
                <div class="results-section">
                    <table class="results-table">
                        <thead>
                            <tr>
                                <th>Handler</th>
                                <th>Status</th>
                                <th>Started</th>
                                <th>Completed</th>
                                <th>Result</th>
                                <th>Error</th>
                                <th>Log entries</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${results.map((result) => `
                                <tr>
                                    <td>${escapeHtml(result.handler_name || '—')}</td>
                                    <td>${escapeHtml(result.final_status || '—')}</td>
                                    <td>${escapeHtml(result.started_at || '—')}</td>
                                    <td>${escapeHtml(result.completed_at || '—')}</td>
                                    <td><pre>${escapeHtml(result.result_repr || '')}</pre></td>
                                    <td><pre>${escapeHtml(result.error_repr || '')}</pre></td>
                                    <td>
                                        <details>
                                            <summary>${result.attempts.length} entr${result.attempts.length === 1 ? 'y' : 'ies'}</summary>
                                            <ul>
                                                ${result.attempts.map((attempt) => `
                                                    <li>
                                                        <strong>${escapeHtml(attempt.status || '—')}</strong>
                                                        @ ${escapeHtml(attempt.inserted_at || '—')}
                                                        ${attempt.result_repr ? `<pre>${escapeHtml(attempt.result_repr)}</pre>` : ''}
                                                        ${attempt.error_repr ? `<pre>${escapeHtml(attempt.error_repr)}</pre>` : ''}
                                                    </li>
                                                `).join('')}
                                            </ul>
                                        </details>
                                    </td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>`;
            }

            function renderNode(node) {
                const data = node.data || SAFE_DEFAULT;
                const shortId = (node.event_id || '').slice(-8) || '—';
                const rawStatus = (node.event_status || node.phase || 'unknown');
                const normalizedStatus = rawStatus.toLowerCase().replace(/[^a-z0-9]+/g, '-');
                const timeoutDisplay = data.event_timeout != null ? `${data.event_timeout}s` : '—';
                const insertedAt = node.inserted_at || '—';
                const path = Array.isArray(data.event_path) ? data.event_path.join(' → ') : '';
                const parentId = data.event_parent_id || '—';
                const version = data.event_version || '—';
                const createdAt = data.event_created_at || '—';
                const processedAt = data.event_completed_at || '—';

                                const summaryPrimary = [
                    `<span class="pill pill-type">${escapeHtml(node.event_type || 'UnknownEvent')}</span>`,
                    `<span class="pill pill-status pill-${escapeHtml(normalizedStatus)}">${escapeHtml(rawStatus)}</span>`,
                    `<span class="pill pill-muted">🚌 ${escapeHtml(node.eventbus_name || '—')}</span>`,
                ];
                if (path) summaryPrimary.push(`<span class="pill pill-muted">🧭 ${escapeHtml(path)}</span>`);

                const summarySecondary = [
                    `<span class="pill pill-muted">ID ${escapeHtml(shortId)}</span>`,
                    `<span class="pill pill-muted">⏱ ${escapeHtml(timeoutDisplay)}</span>`,
                    `<span class="pill pill-muted">🕒 ${escapeHtml(createdAt)}</span>`,
                    `<span class="pill pill-muted">✅ ${escapeHtml(processedAt)}</span>`,
                ];

                const summaryBadges = summaryPrimary.concat(summarySecondary).join('');

                const metaItems = [
                    renderMetaItem('Event ID', node.event_id || '—', { code: true, icon: '🆔' }),
                    renderMetaItem('Parent ID', parentId, { code: true, icon: '👪' }),
                    renderMetaItem('Path', path || '—', { icon: '🧭' }),
                    renderMetaItem('Version', version, { code: true, icon: '📦' }),
                ].join('');

                const resultsSection = node.results.length ? renderResults(node.results) : '';
                const childrenSection = node.children.length ? `<ul class="children">${node.children.map(renderNode).join('')}</ul>` : '';
                const eventJson = data && Object.keys(data).length
                    ? `<details class="event-json"><summary>Payload</summary><pre>${escapeHtml(JSON.stringify(data, null, 2))}</pre></details>`
                    : '';

                return `
                <li class="tree-node">
                    <details open>
                        <summary>
                            <div class="event-summary">${summaryBadges}</div>
                        </summary>
                        <div class="event-meta"><div class="meta-grid">${metaItems}</div></div>
                        ${resultsSection}
                        ${eventJson}
                        ${childrenSection}
                    </details>
                </li>`;
            }


            function render() {
                updateBusOptions();
                const tree = filterTree(buildTree());
                const totalResults = Array.from(state.results.values()).reduce((acc, list) => acc + list.length, 0);
                countsEl.textContent = `Events: ${state.events.size} · Results: ${totalResults}`;
                if (!tree.length) {
                    treeContainer.innerHTML = '<div class="empty">No matching events yet.</div>';
                    return;
                }
                treeContainer.innerHTML = `<ul class="tree-root">${tree.map(renderNode).join('')}</ul>`;
            }

            function setupFilters() {
                searchInput.addEventListener('input', (event) => {
                    state.filters.search = event.target.value.trim().toLowerCase();
                    render();
                });
                statusSelect.addEventListener('change', (event) => {
                    state.filters.status = event.target.value;
                    render();
                });
                busSelect.addEventListener('change', (event) => {
                    state.filters.bus = event.target.value;
                    render();
                });
            }

            async function loadInitial() {
                const [events, results, meta] = await Promise.all([
                    fetch('/events?limit=120').then((r) => r.json()),
                    fetch('/results?limit=200').then((r) => r.json()),
                    fetch('/meta').then((r) => r.json()),
                ]);
                dbPathEl.textContent = meta.db_path;
                if (!meta.db_exists) {
                    warningEl.textContent = 'No SQLite history file found yet. Dispatch events to see activity here.';
                    warningEl.style.display = 'block';
                } else if (!meta.events_table_exists || !meta.results_table_exists) {
                    warningEl.textContent =
                        'Database found, but mirror tables are missing. Enable SQLiteHistoryMirrorMiddleware on your EventBus to populate events_log and event_results_log.';
                    warningEl.style.display = 'block';
                } else {
                    warningEl.style.display = 'none';
                }
                ingestEvents(events);
                ingestResults(results);
                render();
            }

            function connectWebSocket() {
                const protocol = location.protocol === 'https:' ? 'wss' : 'ws';
                const params = new URLSearchParams({
                    since_event_id: String(state.lastEventRowId),
                    since_result_id: String(state.lastResultRowId),
                });
                const ws = new WebSocket(`${protocol}://${location.host}/ws/events?${params.toString()}`);
                wsStatus.textContent = 'connecting…';

                ws.onopen = () => {
                    wsStatus.textContent = 'connected';
                };

                ws.onmessage = (event) => {
                    let payload;
                    try {
                        payload = JSON.parse(event.data);
                    } catch {
                        return;
                    }
                    if (payload.events && payload.events.length) {
                        ingestEvents(payload.events);
                    }
                    if (payload.results && payload.results.length) {
                        ingestResults(payload.results);
                    }
                    render();
                };
                ws.onclose = () => {
                    wsStatus.textContent = 'disconnected – retrying…';
                    setTimeout(connectWebSocket, 2000);
                };
                ws.onerror = () => {
                    ws.close();
                };
            }

            setupFilters();
            loadInitial().then(connectWebSocket);
        </script>
    </body>
    </html>
    """


@app.get('/events')
async def list_events(limit: Annotated[int, Query(ge=1, le=200)] = 20) -> JSONResponse:
    rows = await _fetch_events(limit)
    return JSONResponse(rows)


@app.get('/results')
async def list_results(limit: Annotated[int, Query(ge=1, le=200)] = 20) -> JSONResponse:
    rows = await _fetch_results(limit)
    return JSONResponse(rows)


@app.get('/meta')
async def meta() -> dict[str, Any]:
    db_path = resolve_db_path()
    exists = db_path.exists()
    schema = await db.fetch_schema_status()
    return {
        'db_path': str(db_path),
        'db_exists': exists,
        'events_table_exists': schema.events_table_exists,
        'results_table_exists': schema.results_table_exists,
    }


@app.websocket('/ws/events')
async def websocket_events(socket: WebSocket) -> None:
    await socket.accept()
    state = db.HistoryStreamState(
        last_event_id=_parse_nonnegative_int(socket.query_params.get('since_event_id')),
        last_result_id=_parse_nonnegative_int(socket.query_params.get('since_result_id')),
    )
    try:
        while True:
            updates = await db.stream_new_rows(state)
            if updates['events'] or updates['results']:
                for key in ('events', 'results'):
                    for row in updates[key]:
                        row['inserted_at'] = _format_timestamp(row.get('inserted_at'))
                await socket.send_text(json.dumps(updates))
            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        return
    except Exception as exc:  # pragma: no cover - surface to client
        await socket.send_text(json.dumps({'error': str(exc)}))
        await asyncio.sleep(0.5)
