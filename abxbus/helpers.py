import asyncio
import logging
import re
import threading
import time
import traceback
from collections import deque
from collections.abc import Awaitable, Callable, Coroutine
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from functools import wraps
from typing import Any, ParamSpec, TypeVar, cast

# Define generic type variables for return type and parameters
R = TypeVar('R')
P = ParamSpec('P')
QueueEntryType = TypeVar('QueueEntryType')

_MONOTONIC_DATETIME_REGEX = re.compile(
    r'^(\d{4})-(\d{2})-(\d{2})'
    r'T(\d{2}):(\d{2}):(\d{2})'
    r'(?:\.(\d{1,9}))?'
    r'(Z|[+-]\d{2}:\d{2})$'
)
_MONOTONIC_DATETIME_LENGTH = 30  # YYYY-MM-DDTHH:MM:SS.fffffffffZ
_MONOTONIC_EPOCH_ANCHOR_NS = time.time_ns()
_MONOTONIC_CLOCK_ANCHOR_NS = time.monotonic_ns()
_last_monotonic_datetime_ns = _MONOTONIC_EPOCH_ANCHOR_NS
_last_monotonic_datetime_lock = threading.Lock()


def _format_epoch_ns_to_iso(epoch_ns: int) -> str:
    seconds, fractional_ns = divmod(epoch_ns, 1_000_000_000)
    base_datetime = datetime.fromtimestamp(seconds, tz=UTC)
    if base_datetime.year <= 1990 or base_datetime.year >= 2500:
        raise ValueError('Datetime year must be >1990 and <2500')
    normalized = f'{base_datetime.strftime("%Y-%m-%dT%H:%M:%S")}.{fractional_ns:09d}Z'
    assert len(normalized) == _MONOTONIC_DATETIME_LENGTH, (
        f'Expected canonical datetime length {_MONOTONIC_DATETIME_LENGTH}, got {len(normalized)}: {normalized!r}'
    )
    return normalized


def _monotonic_datetime_py(isostring: str | None = None) -> str:
    """Pure-Python fallback: canonical UTC ISO datetime with exactly 9 fractional digits."""
    if isostring is not None:
        match = _MONOTONIC_DATETIME_REGEX.fullmatch(isostring)
        if match is None:
            raise ValueError(f'Invalid ISO datetime: {isostring!r}')

        parsed = datetime.fromisoformat(isostring.replace('Z', '+00:00'))
        parsed_utc = parsed.astimezone(UTC)
        if parsed_utc.year <= 1990 or parsed_utc.year >= 2500:
            raise ValueError(f'Datetime year must be >1990 and <2500: {isostring!r}')

        fractional = (match.group(7) or '').ljust(9, '0')
        normalized = f'{parsed_utc.strftime("%Y-%m-%dT%H:%M:%S")}.{fractional}Z'
        assert len(normalized) == _MONOTONIC_DATETIME_LENGTH, (
            f'Expected canonical datetime length {_MONOTONIC_DATETIME_LENGTH}, got {len(normalized)}: {normalized!r}'
        )
        return normalized

    global _last_monotonic_datetime_ns
    elapsed_ns = time.monotonic_ns() - _MONOTONIC_CLOCK_ANCHOR_NS
    epoch_ns = _MONOTONIC_EPOCH_ANCHOR_NS + elapsed_ns
    with _last_monotonic_datetime_lock:
        if epoch_ns <= _last_monotonic_datetime_ns:
            epoch_ns = _last_monotonic_datetime_ns + 1
        _last_monotonic_datetime_ns = epoch_ns
    return _format_epoch_ns_to_iso(epoch_ns)


# Use Rust-accelerated monotonic_datetime when available (~4x faster)
try:
    from _abxbus_rust import monotonic_datetime as _monotonic_datetime_rust

    def monotonic_datetime(isostring: str | None = None) -> str:
        """Return canonical UTC ISO datetime with exactly 9 fractional digits. (Rust-accelerated)"""
        return _monotonic_datetime_rust(isostring)

except ImportError:
    monotonic_datetime = _monotonic_datetime_py


async def run_with_timeout(awaitable: Awaitable[R], timeout: float | None = None) -> R:
    """Await `awaitable` with optional timeout."""
    if timeout is None:
        return await awaitable
    return await asyncio.wait_for(awaitable, timeout=timeout)


async def cancel_and_await(task: asyncio.Task[Any] | None, timeout: float | None = None) -> None:
    """Best-effort task cancellation helper that suppresses cancellation-time noise."""
    if task is None:
        return
    if not task.done():
        task.cancel()
    try:
        await run_with_timeout(task, timeout=timeout)
    except (asyncio.CancelledError, TimeoutError):
        pass
    except Exception:
        pass


@asynccontextmanager
async def _run_with_slow_monitor(
    monitor_factory: Callable[[], Coroutine[Any, Any, Any]] | None,
    *,
    task_name: str | None = None,
):
    """Run an optional slow-monitor task scoped to the surrounding execution.

    The monitor is started on enter and always cancelled on exit.
    """
    task: asyncio.Task[Any] | None = None
    if monitor_factory is not None:
        if task_name is None:
            task = asyncio.create_task(monitor_factory())
        else:
            task = asyncio.create_task(monitor_factory(), name=task_name)
    try:
        yield task
    finally:
        await cancel_and_await(task)


class QueueShutDown(Exception):
    """Raised when putting on to or getting from a shut-down Queue."""

    pass


class CleanShutdownQueue(asyncio.Queue[QueueEntryType]):
    """asyncio.Queue subclass that handles shutdown cleanly without warnings."""

    _is_shutdown: bool = False
    _queue: deque[QueueEntryType]
    _getters: deque[asyncio.Future[QueueEntryType]]
    _putters: deque[asyncio.Future[QueueEntryType]]

    def iter_items(self) -> tuple[QueueEntryType, ...]:
        """Return a snapshot of queued items in FIFO order."""
        return tuple(self._queue)

    def remove_item(self, item: QueueEntryType) -> bool:
        """Remove one matching queued item if present."""
        if item not in self._queue:
            return False
        self._queue.remove(item)
        return True

    def shutdown(self, immediate: bool = True):
        """Shutdown the queue and clean up all pending futures."""
        del immediate
        self._is_shutdown = True

        # Cancel all waiting getters without triggering warnings
        while self._getters:
            getter = self._getters.popleft()
            if not getter.done():
                # Set exception instead of cancelling to avoid "Event loop is closed" errors
                getter.set_exception(QueueShutDown())

        # Cancel all waiting putters
        while self._putters:
            putter = self._putters.popleft()
            if not putter.done():
                putter.set_exception(QueueShutDown())

    async def get(self) -> QueueEntryType:
        """Remove and return an item from the queue, with shutdown support."""
        while self.empty():
            if self._is_shutdown:
                raise QueueShutDown

            getter: asyncio.Future[QueueEntryType] = asyncio.get_running_loop().create_future()
            assert isinstance(getter, asyncio.Future)
            self._getters.append(getter)
            try:
                await getter
            except:
                # Clean up the getter if we're cancelled
                getter.cancel()  # Just in case getter is not done yet.
                try:
                    self._getters.remove(getter)
                except ValueError:
                    pass
                # Re-raise the exception
                raise

        return self.get_nowait()

    async def put(self, item: QueueEntryType) -> None:
        """Put an item into the queue, with shutdown support."""
        while self.full():
            if self._is_shutdown:
                raise QueueShutDown

            putter: asyncio.Future[QueueEntryType] = asyncio.get_running_loop().create_future()
            assert isinstance(putter, asyncio.Future)
            self._putters.append(putter)
            try:
                await putter
            except:
                putter.cancel()  # Just in case putter is not done yet.
                try:
                    self._putters.remove(putter)
                except ValueError:
                    pass
                raise

        return self.put_nowait(item)

    def put_nowait(self, item: QueueEntryType) -> None:
        """Put an item into the queue without blocking, with shutdown support."""
        if self._is_shutdown:
            raise QueueShutDown
        return super().put_nowait(item)

    def get_nowait(self) -> QueueEntryType:
        """Remove and return an item if one is immediately available, with shutdown support."""
        if self._is_shutdown and self.empty():
            raise QueueShutDown
        return super().get_nowait()


def extract_basemodel_generic_arg(cls: type) -> Any:
    """
    Extract T_EventResultType Generic arg from BaseEvent[T_EventResultType] subclasses using pydantic generic metadata.
    Needed because pydantic messes with the mro and obscures the Generic from the bases list.
    https://github.com/pydantic/pydantic/issues/8410
    """

    def _extract_arg_from_metadata(metadata_value: Any) -> Any:
        metadata = cast(dict[str, Any], metadata_value)
        origin: Any = metadata.get('origin')
        args: tuple[Any, ...] = cast(tuple[Any, ...], metadata.get('args') or ())
        if not args:
            return None
        # Avoid importing BaseEvent here to keep helpers.py decoupled from models.py.
        if getattr(origin, '__name__', None) == 'BaseEvent' and getattr(origin, '__module__', None) == 'abxbus.base_event':
            return args[0]
        return None

    # Direct check first for speed - most subclasses will have it directly
    if hasattr(cls, '__pydantic_generic_metadata__'):
        generic_arg = _extract_arg_from_metadata(getattr(cls, '__pydantic_generic_metadata__'))
        if generic_arg is not None:
            return generic_arg

    # Only check MRO if direct check failed
    for parent in cls.__mro__[1:]:
        if hasattr(parent, '__pydantic_generic_metadata__'):
            generic_arg = _extract_arg_from_metadata(getattr(parent, '__pydantic_generic_metadata__'))
            if generic_arg is not None:
                return generic_arg

    return None


def time_execution(
    additional_text: str = '',
) -> Callable[[Callable[P, Coroutine[Any, Any, R]]], Callable[P, Coroutine[Any, Any, R]]]:
    """Decorator that logs how much time execution of a function takes"""

    def decorator(func: Callable[P, Coroutine[Any, Any, R]]) -> Callable[P, Coroutine[Any, Any, R]]:
        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            start_time = time.time()
            result = await func(*args, **kwargs)
            execution_time = time.time() - start_time
            # Only log if execution takes more than 0.25 seconds to avoid spamming the logs
            # you can lower this threshold locally when you're doing dev work to performance optimize stuff
            if execution_time > 0.25:
                self_has_logger = args and getattr(args[0], 'logger', None)
                if self_has_logger:
                    logger = getattr(args[0], 'logger')
                elif 'agent' in kwargs:
                    logger = getattr(kwargs['agent'], 'logger')
                elif 'browser_session' in kwargs:
                    logger = getattr(kwargs['browser_session'], 'logger')
                else:
                    logger = logging.getLogger(__name__)
                logger.debug(f'⏳ {additional_text.strip("-")}() took {execution_time:.2f}s')
            return result

        return wrapper

    return decorator


def log_filtered_traceback(exc: BaseException) -> str:
    """Format traceback while filtering noisy asyncio/stdlib frames."""
    trace_exc = traceback.TracebackException.from_exception(exc, capture_locals=False)

    def _filter(_: traceback.TracebackException):
        trace_exc.stack = traceback.StackSummary.from_list(
            [f for f in trace_exc.stack if 'asyncio/tasks.py' not in f.filename and 'lib/python' not in f.filename]
        )
        if trace_exc.__cause__:
            _filter(trace_exc.__cause__)
        if trace_exc.__context__:
            _filter(trace_exc.__context__)

    _filter(trace_exc)
    return ''.join(trace_exc.format())


__all__ = [
    'run_with_timeout',
    'cancel_and_await',
    '_run_with_slow_monitor',
    'log_filtered_traceback',
    'CleanShutdownQueue',
    'QueueShutDown',
    'extract_basemodel_generic_arg',
    'time_execution',
]
