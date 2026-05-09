import asyncio
import hashlib
import json
import logging
import os
import re
import tempfile
import threading
import time
from collections.abc import Callable, Coroutine
from functools import wraps
from pathlib import Path
from types import ModuleType
from typing import Any, Literal, ParamSpec, TypeVar, cast

psutil: ModuleType | None
try:
    import psutil as _psutil
except ImportError:
    psutil = None
else:
    psutil = _psutil

PSUTIL_AVAILABLE: bool = psutil is not None


logger = logging.getLogger(__name__)


T = TypeVar('T')
P = ParamSpec('P')
RetryErrorMatcher = type[Exception] | re.Pattern[str]
RetryOnErrors = list[RetryErrorMatcher] | tuple[RetryErrorMatcher, ...]

# Global semaphore registry for retry decorator
GLOBAL_RETRY_SEMAPHORES: dict[str, asyncio.Semaphore] = {}
GLOBAL_RETRY_SEMAPHORE_LOCK = threading.Lock()

# Multiprocess semaphore support
MULTIPROCESS_SEMAPHORE_DIR = Path(tempfile.gettempdir()) / 'browser_use_semaphores'
MULTIPROCESS_SEMAPHORE_DIR.mkdir(exist_ok=True)
MULTIPROCESS_STALE_LOCK_SECONDS = 300.0

# Global overload detection state
_last_overload_check = 0.0
_overload_check_interval = 5.0  # Check every 5 seconds
_active_retry_operations = 0
_active_operations_lock = threading.Lock()


def _check_system_overload() -> tuple[bool, str]:
    """Check if system is overloaded and return (is_overloaded, reason)"""
    if not PSUTIL_AVAILABLE:
        return False, ''

    assert psutil is not None
    try:
        # Get system stats
        cpu_percent = psutil.cpu_percent(interval=None)
        memory = psutil.virtual_memory()

        # Check thresholds
        reasons: list[str] = []
        is_overloaded = False

        if cpu_percent > 85:
            is_overloaded = True
            reasons.append(f'CPU: {cpu_percent:.1f}%')

        if memory.percent > 85:
            is_overloaded = True
            reasons.append(f'Memory: {memory.percent:.1f}%')

        # Check number of concurrent operations
        with _active_operations_lock:
            if _active_retry_operations > 30:
                is_overloaded = True
                reasons.append(f'Active operations: {_active_retry_operations}')

        return is_overloaded, ', '.join(reasons)
    except Exception:
        return False, ''


def _get_semaphore_key(
    base_name: str,
    semaphore_scope: Literal['multiprocess', 'global', 'class', 'instance'],
    args: tuple[Any, ...],
) -> str:
    """Determine the semaphore key based on scope."""
    if semaphore_scope == 'multiprocess':
        return base_name
    elif semaphore_scope == 'global':
        return base_name
    elif semaphore_scope == 'class' and args and hasattr(args[0], '__class__'):
        class_name = args[0].__class__.__name__
        return f'{class_name}.{base_name}'
    elif semaphore_scope == 'instance' and args:
        instance_id = id(args[0])
        return f'{instance_id}.{base_name}'
    else:
        # Fallback to global if we can't determine scope
        return base_name


def _get_or_create_semaphore(
    sem_key: str,
    semaphore_limit: int,
    semaphore_scope: Literal['multiprocess', 'global', 'class', 'instance'],
) -> Any:
    """Get or create a semaphore based on scope."""
    if semaphore_scope == 'multiprocess':
        return sem_key
    else:
        with GLOBAL_RETRY_SEMAPHORE_LOCK:
            if sem_key not in GLOBAL_RETRY_SEMAPHORES:
                GLOBAL_RETRY_SEMAPHORES[sem_key] = asyncio.Semaphore(semaphore_limit)
            return GLOBAL_RETRY_SEMAPHORES[sem_key]


def _calculate_semaphore_timeout(
    semaphore_timeout: float | None,
    timeout: float | None,
    semaphore_limit: int,
) -> float | None:
    """Calculate the timeout for semaphore acquisition."""
    if semaphore_timeout is not None:
        return semaphore_timeout
    if timeout is None:
        return None
    # Default aligns with TS: timeout * max(1, semaphore_limit - 1)
    return timeout * max(1, semaphore_limit - 1)


def _callable_name(func: Callable[..., Any]) -> str:
    """Return a stable name for logs even for callable instances."""
    return getattr(func, '__name__', func.__class__.__name__)


def _resolve_semaphore_name(
    func_name: str,
    semaphore_name: str | Callable[..., str] | None,
    args: tuple[Any, ...],
) -> str:
    """Resolve semaphore name from a static name or call-time getter."""
    if semaphore_name is None:
        return func_name
    if isinstance(semaphore_name, str):
        return semaphore_name
    return semaphore_name(*args)


def _matches_retry_on_error(error: Exception, retry_on_errors: RetryOnErrors | None) -> bool:
    """Return True when an error matches any configured retry matcher."""
    if not retry_on_errors:
        return True

    error_text = f'{error.__class__.__name__}: {error}'
    for matcher in retry_on_errors:
        if isinstance(matcher, re.Pattern):
            if matcher.search(error_text):
                return True
            continue
        if isinstance(error, matcher):
            return True

    return False


async def _acquire_multiprocess_semaphore(
    semaphore: Any,
    sem_timeout: float | None,
    sem_key: str,
    semaphore_lax: bool,
    semaphore_limit: int,
    timeout: float | None,
) -> tuple[bool, Any]:
    """Acquire a cross-process semaphore using shared slot files.

    Each slot is a file created with O_EXCL. The file content stores the owning
    pid and a per-acquisition token so release can verify ownership. This format
    is mirrored in abxbus-ts so the same semaphore name can contend across
    Python and JS processes.
    """
    del semaphore

    start_time = time.time()
    retry_delay = 0.1  # Start with 100ms
    backoff_factor = 2.0
    has_timeout = sem_timeout is not None and sem_timeout > 0
    lock_prefix = hashlib.sha256(sem_key.encode()).hexdigest()[:40]
    MULTIPROCESS_SEMAPHORE_DIR.mkdir(exist_ok=True, parents=True)

    while True:
        elapsed = time.time() - start_time
        remaining_time: float | None = (sem_timeout - elapsed) if has_timeout and sem_timeout is not None else None
        if remaining_time is not None and remaining_time <= 0:
            break

        for slot in range(semaphore_limit):
            slot_file = MULTIPROCESS_SEMAPHORE_DIR / f'{lock_prefix}.{slot:02d}.lock'
            token = f'{os.getpid()}:{time.time_ns()}:{threading.get_ident()}'
            owner = json.dumps(
                {
                    'token': token,
                    'pid': os.getpid(),
                    'semaphore_name': sem_key,
                    'created_at_ms': int(time.time() * 1000),
                }
            )

            try:
                fd = os.open(slot_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                with os.fdopen(fd, 'w', encoding='utf-8') as handle:
                    handle.write(owner)
                    handle.flush()
                    os.fsync(handle.fileno())
                return True, (slot_file, token)
            except FileExistsError:
                try:
                    raw = slot_file.read_text(encoding='utf-8').strip()
                    current_owner: dict[str, Any] | None = json.loads(raw) if raw else None
                    if not isinstance(current_owner, dict):
                        current_owner = None
                    current_pid = current_owner.get('pid') if current_owner else None
                    if isinstance(current_pid, int):
                        try:
                            os.kill(current_pid, 0)
                            continue
                        except OSError:
                            pass

                    slot_age = time.time() - slot_file.stat().st_mtime
                    if isinstance(current_pid, int) or slot_age >= MULTIPROCESS_STALE_LOCK_SECONDS:
                        slot_file.unlink(missing_ok=True)
                except Exception:
                    pass
            except FileNotFoundError:
                MULTIPROCESS_SEMAPHORE_DIR.mkdir(exist_ok=True, parents=True)
                continue

        sleep_for = retry_delay if remaining_time is None else min(retry_delay, remaining_time)
        if sleep_for > 0:
            await asyncio.sleep(sleep_for)
        retry_delay = min(retry_delay * backoff_factor, 1.0)

    # Timeout reached
    if not semaphore_lax:
        timeout_str = f', timeout={timeout}s per operation' if timeout is not None else ''
        raise TimeoutError(
            f'Failed to acquire multiprocess semaphore "{sem_key}" within {sem_timeout}s (limit={semaphore_limit}{timeout_str})'
        )
    logger.warning(
        f'Failed to acquire multiprocess semaphore "{sem_key}" after {sem_timeout:.1f}s, proceeding without concurrency limit'
    )
    return False, None


async def _acquire_asyncio_semaphore(
    semaphore: asyncio.Semaphore,
    sem_timeout: float | None,
    sem_key: str,
    semaphore_lax: bool,
    semaphore_limit: int,
    timeout: float | None,
    sem_start: float,
) -> bool:
    """Acquire an asyncio semaphore."""
    if sem_timeout is None or sem_timeout <= 0:
        await semaphore.acquire()
        return True

    try:
        async with asyncio.timeout(sem_timeout):
            await semaphore.acquire()
            return True
    except TimeoutError:
        sem_wait_time = time.time() - sem_start
        if not semaphore_lax:
            timeout_str = f', timeout={timeout}s per operation' if timeout is not None else ''
            raise TimeoutError(
                f'Failed to acquire semaphore "{sem_key}" within {sem_timeout}s (limit={semaphore_limit}{timeout_str})'
            )
        logger.warning(
            f'Failed to acquire semaphore "{sem_key}" after {sem_wait_time:.1f}s, proceeding without concurrency limit'
        )
        return False


async def _execute_with_retries(
    func: Callable[P, Coroutine[Any, Any, T]],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    max_attempts: int,
    timeout: float | None,
    retry_after: float,
    retry_backoff_factor: float,
    retry_on_errors: RetryOnErrors | None,
    start_time: float,
    sem_start: float,
    semaphore_limit: int | None,
) -> T:
    """Execute the function with retry logic."""
    func_name = _callable_name(func)
    func_runner = cast(Callable[..., Coroutine[Any, Any, T]], func)
    for attempt in range(1, max_attempts + 1):
        try:
            # Execute with per-attempt timeout
            if timeout is not None and timeout > 0:
                async with asyncio.timeout(timeout):
                    return await func_runner(*args, **kwargs)
            return await func_runner(*args, **kwargs)

        except Exception as e:
            # Check if we should retry this exception
            if not _matches_retry_on_error(e, retry_on_errors):
                raise

            if attempt < max_attempts:
                # Calculate wait time with backoff
                current_wait = retry_after * (retry_backoff_factor ** (attempt - 1))

                # Only log warning on the final retry attempt (second-to-last overall attempt)
                if attempt == max_attempts - 1:
                    logger.warning(
                        f'{func_name} failed (attempt {attempt}/{max_attempts}): '
                        f'{type(e).__name__}: {e}. Waiting {current_wait:.1f}s before retry...'
                    )
                if current_wait > 0:
                    await asyncio.sleep(current_wait)
            else:
                # Final failure
                total_time = time.time() - start_time
                sem_wait = time.time() - sem_start - total_time if semaphore_limit else 0
                sem_str = f'Semaphore wait: {sem_wait:.1f}s. ' if sem_wait > 0 else ''
                logger.error(
                    f'{func_name} failed after {max_attempts} attempts over {total_time:.1f}s. '
                    f'{sem_str}Final error: {type(e).__name__}: {e}'
                )
                raise

    # This should never be reached, but satisfies type checker
    raise RuntimeError('Unexpected state in retry logic')


def _track_active_operations(increment: bool = True) -> None:
    """Track active retry operations."""
    global _active_retry_operations
    with _active_operations_lock:
        if increment:
            _active_retry_operations += 1
        else:
            _active_retry_operations = max(0, _active_retry_operations - 1)


def _check_system_overload_if_needed() -> None:
    """Check for system overload if enough time has passed since last check."""
    global _last_overload_check
    current_time = time.time()
    if current_time - _last_overload_check > _overload_check_interval:
        _last_overload_check = current_time
        is_overloaded, reason = _check_system_overload()
        if is_overloaded:
            logger.warning(f'⚠️  System overload detected: {reason}. Consider reducing concurrent operations to prevent hanging.')


def retry(
    retry_after: float = 0,
    max_attempts: int = 1,
    timeout: float | None = None,
    retry_on_errors: RetryOnErrors | None = None,
    retry_backoff_factor: float = 1.0,
    semaphore_limit: int | None = None,
    semaphore_name: str | Callable[..., str] | None = None,
    semaphore_lax: bool = True,
    semaphore_scope: Literal['multiprocess', 'global', 'class', 'instance'] = 'global',
    semaphore_timeout: float | None = None,
):
    """
        Retry decorator with semaphore support for async functions.

        Args:
                retry_after: Seconds to wait between retries
                max_attempts: Total attempts including the initial call (1 = no retries)
                timeout: Per-attempt timeout in seconds (`None` = no per-attempt timeout)
                retry_on_errors: Error matchers to retry on (Exception subclasses or compiled regexes)
                retry_backoff_factor: Multiplier for retry delay after each attempt (1.0 = no backoff)
                semaphore_limit: Max concurrent executions (creates semaphore if needed)
                semaphore_name: Name for semaphore (defaults to function name), or callable receiving function args
                semaphore_lax: If True, continue without semaphore on acquisition failure
                semaphore_scope: Scope for semaphore sharing:
                        - 'global': All calls share one semaphore (default)
                        - 'class': All instances of a class share one semaphore
                        - 'instance': Each instance gets its own semaphore
                        - 'multiprocess': All processes on the machine share one semaphore
                semaphore_timeout: Max time to wait for semaphore acquisition
                                   (`None` => `timeout * max(1, limit - 1)` when timeout is set, else unbounded)

        Example:
                @retry(retry_after=3, max_attempts=3, timeout=5, semaphore_limit=3, semaphore_scope='instance')
                async def some_function(self, ...):
                        # Limited to 5s per attempt, up to 3 total attempts
                        # Max 3 concurrent executions per instance

    Notes:
                - semaphore acquisition happens once at start time, it is not retried
                - semaphore_timeout is only used if semaphore_limit is set.
                - if semaphore_timeout is set to 0, it waits forever for a semaphore slot.
                - if semaphore_timeout is None and timeout is None, semaphore acquisition wait is unbounded.
    """

    def decorator(func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, Coroutine[Any, Any, T]]:
        func_name = _callable_name(func)
        effective_max_attempts = max(1, max_attempts)
        effective_retry_after = max(0, retry_after)
        effective_semaphore_limit = semaphore_limit if semaphore_limit is not None and semaphore_limit > 0 else None

        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            # Initialize semaphore-related variables
            semaphore: Any = None
            semaphore_acquired = False
            multiprocess_lock: Any = None
            sem_start = time.time()

            # Handle semaphore if specified
            if effective_semaphore_limit is not None:
                # Get semaphore key and create/retrieve semaphore
                base_name = _resolve_semaphore_name(func_name, semaphore_name, tuple(args))
                sem_key = _get_semaphore_key(base_name, semaphore_scope, tuple(args))
                semaphore = _get_or_create_semaphore(sem_key, effective_semaphore_limit, semaphore_scope)

                # Calculate timeout for semaphore acquisition
                sem_timeout = _calculate_semaphore_timeout(semaphore_timeout, timeout, effective_semaphore_limit)

                # Acquire semaphore based on type
                if semaphore_scope == 'multiprocess':
                    semaphore_acquired, multiprocess_lock = await _acquire_multiprocess_semaphore(
                        semaphore, sem_timeout, sem_key, semaphore_lax, effective_semaphore_limit, timeout
                    )
                else:
                    semaphore_acquired = await _acquire_asyncio_semaphore(
                        semaphore, sem_timeout, sem_key, semaphore_lax, effective_semaphore_limit, timeout, sem_start
                    )

            # Track active operations and check system overload
            _track_active_operations(increment=True)
            _check_system_overload_if_needed()

            # Execute function with retries
            start_time = time.time()
            try:
                return await _execute_with_retries(
                    func,
                    tuple(args),
                    dict(kwargs),
                    effective_max_attempts,
                    timeout,
                    effective_retry_after,
                    retry_backoff_factor,
                    retry_on_errors,
                    start_time,
                    sem_start,
                    effective_semaphore_limit,
                )
            finally:
                # Clean up: decrement active operations and release semaphore
                _track_active_operations(increment=False)

                if semaphore_acquired and semaphore:
                    try:
                        if semaphore_scope == 'multiprocess' and multiprocess_lock:
                            slot_file, token = cast(tuple[Path, str], multiprocess_lock)
                            raw = slot_file.read_text(encoding='utf-8').strip() if slot_file.exists() else ''
                            current_owner: dict[str, Any] | None = json.loads(raw) if raw else None
                            if not isinstance(current_owner, dict):
                                current_owner = None
                            if current_owner and current_owner.get('token') == token:
                                slot_file.unlink(missing_ok=True)
                        elif semaphore:
                            semaphore.release()
                    except (FileNotFoundError, OSError) as e:
                        # Handle case where lock file was removed during operation
                        if isinstance(e, FileNotFoundError) or 'No such file or directory' in str(e):
                            logger.warning(f'Semaphore lock file disappeared during release, ignoring: {e}')
                        else:
                            # Log other OS errors but don't raise - we already completed the operation
                            logger.error(f'Error releasing semaphore: {e}')

        return wrapper

    return decorator


__all__ = [
    'MULTIPROCESS_SEMAPHORE_DIR',
    'retry',
]
