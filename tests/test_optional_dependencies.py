from __future__ import annotations

import ast
import os
import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]


def _ast_import_roots(path: Path) -> set[str]:
    parsed = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
    roots: set[str] = set()
    for node in ast.walk(parsed):
        if isinstance(node, ast.Import):
            for alias in node.names:
                roots.add(alias.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            roots.add(node.module.split('.')[0])
    return roots


def test_bridge_modules_do_not_eager_import_optional_packages() -> None:
    bridge_modules = {
        _ROOT / 'abxbus' / 'bridge_postgres.py': {'asyncpg'},
        _ROOT / 'abxbus' / 'bridge_nats.py': {'nats'},
        _ROOT / 'abxbus' / 'bridge_redis.py': {'redis'},
        _ROOT / 'abxbus' / 'bridge_tachyon.py': {'tachyon'},
    }

    for path, forbidden_roots in bridge_modules.items():
        imported_roots = _ast_import_roots(path)
        assert forbidden_roots.isdisjoint(imported_roots), f'{path} eagerly imports {forbidden_roots & imported_roots}'


def test_root_import_excludes_optional_integrations_while_namespaced_imports_resolve() -> None:
    code = """
import sys

import abxbus

assert hasattr(abxbus, 'EventBus')
assert hasattr(abxbus, 'EventBusMiddleware')
assert hasattr(abxbus, 'EventBridge')
assert hasattr(abxbus, 'HTTPEventBridge')
assert hasattr(abxbus, 'JSONLEventBridge')
assert hasattr(abxbus, 'SQLiteEventBridge')

assert not hasattr(abxbus, 'PostgresEventBridge')
assert not hasattr(abxbus, 'RedisEventBridge')
assert not hasattr(abxbus, 'NATSEventBridge')
assert not hasattr(abxbus, 'TachyonEventBridge')
assert not hasattr(abxbus, 'OtelTracingMiddleware')

assert 'asyncpg' not in sys.modules
assert 'redis' not in sys.modules
assert 'nats' not in sys.modules
assert 'tachyon' not in sys.modules
assert not any(name == 'opentelemetry' or name.startswith('opentelemetry.') for name in sys.modules)

from abxbus.bridges import PostgresEventBridge, TachyonEventBridge
from abxbus.middlewares import OtelTracingMiddleware

assert PostgresEventBridge.__name__ == 'PostgresEventBridge'
assert TachyonEventBridge.__name__ == 'TachyonEventBridge'
assert OtelTracingMiddleware.__name__ == 'OtelTracingMiddleware'
"""

    result = subprocess.run(
        [sys.executable, '-c', code],
        cwd=_ROOT,
        env={**os.environ, 'PYDANTIC_DISABLE_PLUGINS': '__all__'},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr or result.stdout
