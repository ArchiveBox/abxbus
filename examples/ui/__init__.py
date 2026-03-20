"""Minimal FastAPI app for monitoring abxbus SQLite event history."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .main import app as app

__all__ = ['app']


def __getattr__(name: str) -> Any:
    if name != 'app':
        raise AttributeError(name)
    from .main import app

    return app
