"""SQLite flat-table bridge for forwarding events between runtimes.

Uses Python stdlib sqlite3 and polling for new rows.
Schema mirrors Postgres bridge shape:
- event_id (PRIMARY KEY)
- event_created_at (indexed)
- event_type (indexed)
- event_payload (full event JSON payload)
- one TEXT column per event_* field storing JSON-serialized values
"""

from __future__ import annotations

import asyncio
import json
import re
import sqlite3
import time
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from typing import Any, TypeGuard

from uuid_extensions import uuid7str

from abxbus.base_event import BaseEvent
from abxbus.event_bus import EventBus, EventPatternType, in_handler_context

_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_EVENT_PAYLOAD_COLUMN = 'event_payload'


def _validate_identifier(identifier: str, *, label: str) -> str:
    if not _IDENTIFIER_RE.match(identifier):
        raise ValueError(f'Invalid {label}: {identifier!r}. Use only [A-Za-z0-9_] and start with a letter/_')
    return identifier


def _is_str_keyed_dict(value: Any) -> TypeGuard[dict[str, Any]]:
    return isinstance(value, dict)


def _split_bridge_payload(payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    event_fields: dict[str, Any] = {}
    event_payload: dict[str, Any] = dict(payload)
    for key, value in payload.items():
        if key.startswith('event_'):
            event_fields[key] = value
    return event_fields, event_payload


class SQLiteEventBridge:
    def __init__(
        self,
        path: str,
        table: str = 'abxbus_events',
        *,
        poll_interval: float = 0.25,
        name: str | None = None,
    ):
        self.path = Path(path)
        self.table = _validate_identifier(table, label='table name')
        self.poll_interval = poll_interval
        self._inbound_bus = EventBus(name=name or f'SQLiteEventBridge_{uuid7str()[-8:]}', max_history_size=0)

        self._running = False
        self._start_task: asyncio.Task[None] | None = None
        self._start_lock = asyncio.Lock()
        self._listener_task: asyncio.Task[None] | None = None
        self._last_seen_event_created_at = ''
        self._last_seen_event_id = ''
        self._table_columns: set[str] = {'event_id', 'event_created_at', 'event_type', _EVENT_PAYLOAD_COLUMN}

    def on(self, event_pattern: EventPatternType, handler: Callable[[BaseEvent[Any]], Any]) -> None:
        self._ensure_started()
        self._inbound_bus.on(event_pattern, handler)

    async def emit(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        self._ensure_started()
        if not self._running:
            await self.start()

        payload = event.model_dump(mode='json')
        event_fields, event_payload = _split_bridge_payload(payload)
        write_payload = {**event_fields, _EVENT_PAYLOAD_COLUMN: event_payload}
        payload_keys = sorted(write_payload.keys())

        await asyncio.to_thread(self._ensure_columns, payload_keys)
        await asyncio.to_thread(self._upsert_payload, write_payload, payload_keys)

        if in_handler_context():
            return None
        return event

    async def dispatch(self, event: BaseEvent[Any]) -> BaseEvent[Any] | None:
        return await self.emit(event)

    async def start(self) -> None:
        current_task = asyncio.current_task()
        if self._start_task is not None and self._start_task is not current_task and not self._start_task.done():
            await self._start_task
            return

        if self._running:
            return

        try:
            async with self._start_lock:
                if self._running:
                    return
                self.path.parent.mkdir(parents=True, exist_ok=True)
                await asyncio.to_thread(self._init_db)
                await asyncio.to_thread(self._refresh_column_cache)
                await asyncio.to_thread(
                    self._ensure_columns, ['event_id', 'event_created_at', 'event_type', _EVENT_PAYLOAD_COLUMN]
                )
                await asyncio.to_thread(self._ensure_base_indexes)
                await asyncio.to_thread(self._set_cursor_to_latest_row)
                self._running = True
                if self._listener_task is None or self._listener_task.done():
                    self._listener_task = asyncio.create_task(self._listen_loop())
        finally:
            if self._start_task is current_task:
                self._start_task = None

    async def close(self, *, clear: bool = True) -> None:
        self._running = False
        if self._start_task is not None:
            self._start_task.cancel()
            await asyncio.gather(self._start_task, return_exceptions=True)
            self._start_task = None
        if self._listener_task is not None:
            self._listener_task.cancel()
            await asyncio.gather(self._listener_task, return_exceptions=True)
            self._listener_task = None
        await self._inbound_bus.stop(clear=clear)

    def _ensure_started(self) -> None:
        if self._running:
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        if self._start_task is None or self._start_task.done():
            self._start_task = asyncio.create_task(self.start())

    async def _listen_loop(self) -> None:
        while self._running:
            try:
                rows = await asyncio.to_thread(
                    self._fetch_new_rows,
                    self._last_seen_event_created_at,
                    self._last_seen_event_id,
                )
                for row in rows:
                    event_created_at = str(row.get('event_created_at') or '')
                    event_id = str(row.get('event_id') or '')
                    if event_created_at or event_id:
                        self._last_seen_event_created_at = event_created_at
                        self._last_seen_event_id = event_id

                    payload: dict[str, Any] = {}
                    raw_event_payload = row.get(_EVENT_PAYLOAD_COLUMN)
                    if isinstance(raw_event_payload, str):
                        try:
                            decoded_event_payload: Any = json.loads(raw_event_payload)
                            if _is_str_keyed_dict(decoded_event_payload):
                                payload.update(decoded_event_payload)
                        except Exception:
                            pass

                    for key, raw_value in row.items():
                        if key == _EVENT_PAYLOAD_COLUMN or raw_value is None or not key.startswith('event_'):
                            continue
                        if isinstance(raw_value, str):
                            try:
                                payload[key] = json.loads(raw_value)
                            except Exception:
                                payload[key] = raw_value
                        else:
                            payload[key] = raw_value

                    await self._dispatch_inbound_payload(payload)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            await asyncio.sleep(self.poll_interval)

    async def _dispatch_inbound_payload(self, payload: Any) -> None:
        event = BaseEvent[Any].model_validate(payload).event_reset()
        self._inbound_bus.emit(event)

    def _connect(self) -> sqlite3.Connection:
        # Under concurrent bridge startup/teardown across processes, sqlite can
        # intermittently fail with "unable to open database file" while the
        # parent path is being materialized. Recover by ensuring parent exists
        # and retrying a bounded number of times.
        connect_attempts = 20
        conn: sqlite3.Connection | None = None
        last_error: sqlite3.OperationalError | None = None
        for _ in range(connect_attempts):
            try:
                conn = sqlite3.connect(str(self.path), timeout=30.0)
                break
            except sqlite3.OperationalError as exc:
                message = str(exc).lower()
                if 'unable to open database file' not in message:
                    raise
                last_error = exc
                self.path.parent.mkdir(parents=True, exist_ok=True)
                time.sleep(0.05)
        if conn is None:
            assert last_error is not None
            raise last_error

        conn.execute('PRAGMA busy_timeout=30000')
        for _ in range(20):
            try:
                conn.execute('PRAGMA journal_mode=WAL')
                break
            except sqlite3.OperationalError as exc:
                if 'locked' not in str(exc).lower():
                    raise
                time.sleep(0.05)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                f'''
                CREATE TABLE IF NOT EXISTS "{self.table}" (
                    "event_id" TEXT PRIMARY KEY,
                    "event_created_at" TEXT,
                    "event_type" TEXT,
                    "event_payload" JSON
                )
                '''
            )
            conn.commit()

    def _refresh_column_cache(self) -> None:
        with closing(self._connect()) as conn:
            rows = conn.execute(f'PRAGMA table_info("{self.table}")').fetchall()
            self._table_columns = {str(row['name']) for row in rows}

    def _ensure_columns(self, keys: list[str]) -> None:
        for key in keys:
            _validate_identifier(key, label='event field name')
            if key != _EVENT_PAYLOAD_COLUMN and not key.startswith('event_'):
                raise ValueError(f'Invalid event field name for bridge column: {key!r}. Only event_* fields become columns')

        missing_columns = [key for key in keys if key not in self._table_columns]
        if not missing_columns:
            return

        with closing(self._connect()) as conn:
            for key in missing_columns:
                column_type = 'JSON' if key == _EVENT_PAYLOAD_COLUMN else 'TEXT'
                conn.execute(f'ALTER TABLE "{self.table}" ADD COLUMN "{key}" {column_type}')
                self._table_columns.add(key)
            conn.commit()

    def _ensure_base_indexes(self) -> None:
        event_created_at_index = f'{self.table}_event_created_at_idx'
        event_type_index = f'{self.table}_event_type_idx'

        with closing(self._connect()) as conn:
            conn.execute(f'CREATE INDEX IF NOT EXISTS "{event_created_at_index}" ON "{self.table}" ("event_created_at")')
            conn.execute(f'CREATE INDEX IF NOT EXISTS "{event_type_index}" ON "{self.table}" ("event_type")')
            conn.commit()

    def _upsert_payload(self, payload: dict[str, Any], payload_keys: list[str]) -> None:
        columns_sql = ', '.join(f'"{key}"' for key in payload_keys)
        placeholders_sql = ', '.join('json(?)' if key == _EVENT_PAYLOAD_COLUMN else '?' for key in payload_keys)
        values = [json.dumps(payload[key], separators=(',', ':')) if payload[key] is not None else None for key in payload_keys]

        update_fields = [key for key in payload_keys if key != 'event_id']
        if update_fields:
            updates_sql = ', '.join(f'"{key}" = excluded."{key}"' for key in update_fields)
            upsert_sql = (
                f'INSERT INTO "{self.table}" ({columns_sql}) VALUES ({placeholders_sql}) '
                f'ON CONFLICT("event_id") DO UPDATE SET {updates_sql}'
            )
        else:
            upsert_sql = (
                f'INSERT INTO "{self.table}" ({columns_sql}) VALUES ({placeholders_sql}) ON CONFLICT("event_id") DO NOTHING'
            )

        with closing(self._connect()) as conn:
            conn.execute(upsert_sql, values)
            conn.commit()

    def _set_cursor_to_latest_row(self) -> None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                f'''
                SELECT
                    COALESCE("event_created_at", '') AS event_created_at,
                    COALESCE("event_id", '') AS event_id
                FROM "{self.table}"
                ORDER BY COALESCE("event_created_at", '') DESC, COALESCE("event_id", '') DESC
                LIMIT 1
                '''
            ).fetchone()
            if row is None:
                self._last_seen_event_created_at = ''
                self._last_seen_event_id = ''
                return
            self._last_seen_event_created_at = str(row['event_created_at'] or '')
            self._last_seen_event_id = str(row['event_id'] or '')

    def _fetch_new_rows(self, last_event_created_at: str, last_event_id: str) -> list[dict[str, Any]]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                f'''
                SELECT *
                FROM "{self.table}"
                WHERE
                    COALESCE("event_created_at", '') > ?
                    OR (
                        COALESCE("event_created_at", '') = ?
                        AND COALESCE("event_id", '') > ?
                    )
                ORDER BY COALESCE("event_created_at", '') ASC, COALESCE("event_id", '') ASC
                ''',
                (last_event_created_at, last_event_created_at, last_event_id),
            ).fetchall()
            return [dict(row) for row in rows]
