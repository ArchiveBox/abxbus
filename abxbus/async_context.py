from __future__ import annotations

import contextvars


class ActiveAbortContext:
    def __init__(self) -> None:
        self.error: BaseException | None = None

    @property
    def aborted(self) -> bool:
        return self.error is not None

    def abort(self, error: BaseException) -> None:
        if self.error is None:
            self.error = error

    def raise_if_aborted(self) -> None:
        if self.error is not None:
            raise self.error


_active_abort_context: contextvars.ContextVar[ActiveAbortContext | None] = contextvars.ContextVar(
    'abxbus_active_abort_context',
    default=None,
)


def get_active_abort_context() -> ActiveAbortContext | None:
    return _active_abort_context.get()


def set_active_abort_context(context: ActiveAbortContext | None) -> contextvars.Token[ActiveAbortContext | None]:
    return _active_abort_context.set(context)


def reset_active_abort_context(token: contextvars.Token[ActiveAbortContext | None]) -> None:
    _active_abort_context.reset(token)
