import asyncio
import contextvars
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

from abxbus.base_event import BaseEvent, EventConcurrencyMode, EventHandlerConcurrencyMode, EventResult

if TYPE_CHECKING:
    from abxbus.event_bus import EventBus

T_EventResultType = TypeVar('T_EventResultType')


class LockManagerProtocol(Protocol):
    """Minimal lock API required by EventBus runtime execution paths."""

    def get_lock_for_event(self, bus: 'EventBus', event: BaseEvent[T_EventResultType]) -> 'ReentrantLock | None':
        """Return the concrete event-level lock object or ``None`` for parallel mode."""
        ...

    def get_lock_for_event_handler(
        self,
        bus: 'EventBus',
        event: BaseEvent[T_EventResultType],
        event_result: EventResult[T_EventResultType],
    ) -> 'ReentrantLock | None':
        """Return the concrete handler-level lock object or ``None`` for parallel mode."""
        ...

    def _run_with_event_lock(self, bus: 'EventBus', event: BaseEvent[T_EventResultType]) -> Any:
        """Context manager for event-level lock scope."""
        ...

    def _run_with_handler_lock(
        self, bus: 'EventBus', event: BaseEvent[T_EventResultType], event_result: EventResult[T_EventResultType]
    ) -> Any:
        """Context manager for per-handler lock scope."""
        ...

    def _run_with_handler_dispatch_context(self, bus: 'EventBus', event: BaseEvent[T_EventResultType]) -> Any:
        """Context manager that mirrors held event-lock state into dispatch context."""
        ...


class ReentrantLock:
    """Context-aware re-entrant lock over an asyncio semaphore.

    Lifecycle:
    1. `__aenter__` acquires the semaphore when this context does not already hold
       the lock id.
    2. Nested entries in the same context only bump the local depth counter.
    3. `__aexit__` decrements depth and releases semaphore at depth zero.
    """

    # Context variable storing lock-id -> re-entrant depth for the current async context.
    _held_lock_depths: ContextVar[dict[int, int]] = ContextVar('held_lock_depths', default={})

    def __init__(self):
        self._semaphore: asyncio.Semaphore | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._lock_id = id(self)

    def _get_semaphore(self) -> asyncio.Semaphore:
        """Get or create the semaphore for the current event loop."""
        current_loop = asyncio.get_running_loop()
        if self._semaphore is None or self._loop != current_loop:
            # Create new semaphore for this event loop
            self._semaphore = asyncio.Semaphore(1)
            self._loop = current_loop
        return self._semaphore

    def _depth(self) -> int:
        return ReentrantLock._held_lock_depths.get().get(self._lock_id, 0)

    def _set_depth(self, depth: int) -> None:
        current = ReentrantLock._held_lock_depths.get()
        updated = dict(current)
        if depth <= 0:
            updated.pop(self._lock_id, None)
        else:
            updated[self._lock_id] = depth
        ReentrantLock._held_lock_depths.set(updated)

    def mark_held_in_current_context(self) -> contextvars.Token[dict[int, int]]:
        """Temporarily mark this lock as already held in the current context.

        Used when a handler runs in a copied dispatch context and needs re-entrant
        lock behavior to match the parent processing context.
        """
        current = ReentrantLock._held_lock_depths.get()
        updated = dict(current)
        updated[self._lock_id] = updated.get(self._lock_id, 0) + 1
        return ReentrantLock._held_lock_depths.set(updated)

    @staticmethod
    def reset_context_mark(token: contextvars.Token[dict[int, int]]) -> None:
        """Undo a prior `mark_held_in_current_context` update."""
        ReentrantLock._held_lock_depths.reset(token)

    async def __aenter__(self):
        depth = self._depth()
        if depth > 0:
            self._set_depth(depth + 1)
            return self

        # Acquire the lock
        await self._get_semaphore().acquire()
        self._set_depth(1)
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> None:
        depth = self._depth()
        if depth <= 0:
            return

        next_depth = depth - 1
        self._set_depth(next_depth)
        if next_depth == 0:
            self._get_semaphore().release()

    def locked(self) -> bool:
        """Check if the lock is currently held."""
        # If semaphore doesn't exist yet or is from a different loop, it's not locked
        try:
            current_loop = asyncio.get_running_loop()
            if self._semaphore is None or self._loop != current_loop:
                return False
            return self._semaphore.locked()
        except RuntimeError:
            # No running loop, can't check
            return False


class LockManager:
    """Centralized lock/semaphore policy for event and handler execution.

    This manager owns lock resolution and all lock mutations. `EventBus` and
    handlers should use only these APIs instead of touching lock objects directly.
    """

    def get_lock_for_event(self, bus: 'EventBus', event: BaseEvent[T_EventResultType]) -> ReentrantLock | None:
        """Resolve the event-level lock for one event execution.

        Lifecycle:
        - Called before processing an event (runloop, step, queue-jump).
        - Returns `None` for `'parallel'`, so no lock is acquired.
        - Returns the shared class lock for `'global-serial'`.
        - Returns `bus.event_bus_serial_lock` for `'bus-serial'`.
        """
        resolved = event.event_concurrency or bus.event_concurrency
        if resolved == EventConcurrencyMode.PARALLEL:
            return None
        if resolved == EventConcurrencyMode.GLOBAL_SERIAL:
            return bus.event_global_serial_lock
        return bus.event_bus_serial_lock

    def get_lock_for_event_handler(
        self,
        bus: 'EventBus',
        event: BaseEvent[T_EventResultType],
        event_result: EventResult[T_EventResultType],
    ) -> ReentrantLock | None:
        """Resolve the per-event handler lock for one handler execution.

        Lifecycle:
        - Called inside `EventBus._run_handler` before running a handler.
        - Returns `None` for `'parallel'` handler mode.
        - Returns and lazily initializes the event handler lock for `'serial'`.
        """
        del event_result  # reserved for future mode-specific rules
        resolved = event.event_handler_concurrency or bus.event_handler_concurrency
        if resolved == EventHandlerConcurrencyMode.PARALLEL:
            return None
        current_lock = event._get_handler_lock()  # pyright: ignore[reportPrivateUsage]
        if current_lock is None:
            current_lock = ReentrantLock()
            event._set_handler_lock(current_lock)  # pyright: ignore[reportPrivateUsage]
        return current_lock

    @asynccontextmanager
    async def _run_with_event_lock(self, bus: 'EventBus', event: BaseEvent[T_EventResultType]):
        """Acquire/release the resolved event lock around event processing.

        Lifecycle:
        - Wraps event processing in runloop and manual `step()`.
        - No-op for `'parallel'` events.
        """
        lock = self.get_lock_for_event(bus, event)
        if lock is None:
            yield
            return
        async with lock:
            yield

    @asynccontextmanager
    async def _run_with_handler_lock(
        self, bus: 'EventBus', event: BaseEvent[T_EventResultType], event_result: EventResult[T_EventResultType]
    ):
        """Acquire/release the resolved per-event handler lock around one handler run.

        Lifecycle:
        - Used directly inside `EventResult.run_handler(...)`.
        - No-op for `'parallel'` handler mode.
        """
        lock = self.get_lock_for_event_handler(bus, event, event_result)
        if lock is None:
            yield
            return
        async with lock:
            yield

    @contextmanager
    def _run_with_handler_dispatch_context(self, bus: 'EventBus', event: BaseEvent[T_EventResultType]):
        """Mirror parent event-lock ownership into the current copied context.

        Lifecycle:
        - Used only by `EventResult.run_handler` when running handlers inside a copied
          dispatch context (`context=dispatch_context`).
        - Marks the resolved event lock as held in this context without acquiring
          the semaphore, enabling safe re-entry for awaited child events.
        """
        lock = self.get_lock_for_event(bus, event)
        if lock is None:
            yield
            return
        token = lock.mark_held_in_current_context()
        try:
            yield
        finally:
            ReentrantLock.reset_context_mark(token)
