from __future__ import annotations

import ast
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
    }

    for path, forbidden_roots in bridge_modules.items():
        imported_roots = _ast_import_roots(path)
        assert forbidden_roots.isdisjoint(imported_roots), f'{path} eagerly imports {forbidden_roots & imported_roots}'
