import asyncio
import logging
import re
import tempfile
import threading
import time
from collections.abc import Callable, Coroutine
from functools import wraps
from pathlib import Path
from types import ModuleType
from typing import Any, Literal, ParamSpec, TypeVar, cast

import portalocker

# Silence portalocker debug messages
portalocker_logger = logging.getLogger('portalocker.utils')
portalocker_logger.setLevel(logging.WARNING)

# Silence root level portalocker logs too
portalocker_root_logger = logging.getLogger('portalocker')
portalocker_root_logger.setLevel(logging.WARNING)

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

# Global multiprocess semaphore registry
# Multiprocess semaphores are not cached due to internal state issues causing "Already locked" errors
MULTIPROCESS_SEMAPHORE_LOCK = threading.Lock()

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
        cpu_percent = psutil.cpu_percent(interval=0.1)
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
        # Don't cache multiprocess semaphores - they have internal state issues
        # Create a new instance each time to avoid "Already locked" errors
        with MULTIPROCESS_SEMAPHORE_LOCK:
            # Ensure the directory exists (it might have been cleaned up in cloud environments)
            MULTIPROCESS_SEMAPHORE_DIR.mkdir(exist_ok=True, parents=True)

            # Clean up any stale lock files before creating semaphore
            lock_pattern = f'{sem_key}.*.lock'
            for lock_file in MULTIPROCESS_SEMAPHORE_DIR.glob(lock_pattern):
                try:
                    # Try to remove lock files older than 5 minutes
                    if lock_file.stat().st_mtime < time.time() - 300:
                        lock_file.unlink(missing_ok=True)
                except Exception:
                    pass  # Ignore errors when cleaning up

            # Use a more aggressive timeout for lock acquisition
            try:
                semaphore = portalocker.utils.NamedBoundedSemaphore(
                    maximum=semaphore_limit,
                    name=sem_key,
                    directory=str(MULTIPROCESS_SEMAPHORE_DIR),
                    timeout=0.1,  # Very short timeout for internal lock acquisition
                )
                return semaphore
            except FileNotFoundError as e:
                # In some cloud environments, the lock file creation might fail
                # Try once more after ensuring directory exists
                logger.warning(f'Lock file creation failed: {e}. Retrying after ensuring directory exists.')
                MULTIPROCESS_SEMAPHORE_DIR.mkdir(exist_ok=True, parents=True)

                # Create a fallback asyncio semaphore instead of multiprocess
                logger.warning(f'Falling back to asyncio semaphore for {sem_key} due to filesystem issues')
                with GLOBAL_RETRY_SEMAPHORE_LOCK:
                    fallback_key = f'multiprocess_fallback_{sem_key}'
                    if fallback_key not in GLOBAL_RETRY_SEMAPHORES:
                        GLOBAL_RETRY_SEMAPHORES[fallback_key] = asyncio.Semaphore(semaphore_limit)
                    return GLOBAL_RETRY_SEMAPHORES[fallback_key]
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
    base_name: str | Any
    if callable(semaphore_name):
        base_name = semaphore_name(*args)
    else:
        base_name = semaphore_name if semaphore_name is not None else func_name
    return str(base_name)


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
    """Acquire a multiprocess semaphore with retries and exponential backoff."""
    start_time = time.time()
    retry_delay = 0.1  # Start with 100ms
    backoff_factor = 2.0
    max_single_attempt = 1.0  # Max time for a single acquire attempt
    recreate_attempts = 0
    max_recreate_attempts = 3
    has_timeout = sem_timeout is not None and sem_timeout > 0

    while True:
        try:
            # Calculate remaining time (when configured)
            elapsed = time.time() - start_time
            remaining_time: float | None = (sem_timeout - elapsed) if has_timeout and sem_timeout is not None else None
            if remaining_time is not None and remaining_time <= 0:
                break

            # Use bounded one-second acquire loops so we can recover from transient lock file errors.
            attempt_timeout = min(remaining_time, max_single_attempt) if remaining_time is not None else max_single_attempt

            # Use a temporary thread to run the blocking operation
            multiprocess_lock = await asyncio.to_thread(
                lambda: semaphore.acquire(timeout=attempt_timeout, check_interval=0.1, fail_when_locked=False)
            )
            if multiprocess_lock:
                return True, multiprocess_lock

            # If we didn't get the lock, wait before retrying
            if remaining_time is None or remaining_time > retry_delay:
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * backoff_factor, 1.0)  # Cap at 1 second

        except (FileNotFoundError, OSError) as e:
            # Handle case where lock file disappears
            if isinstance(e, FileNotFoundError) or 'No such file or directory' in str(e):
                recreate_attempts += 1
                if recreate_attempts <= max_recreate_attempts:
                    logger.warning(
                        f'Semaphore lock file disappeared for "{sem_key}". Attempting to recreate (attempt {recreate_attempts}/{max_recreate_attempts})...'
                    )

                    # Ensure directory exists
                    with MULTIPROCESS_SEMAPHORE_LOCK:
                        MULTIPROCESS_SEMAPHORE_DIR.mkdir(exist_ok=True, parents=True)

                    # Try to recreate the semaphore
                    try:
                        semaphore = await asyncio.to_thread(
                            lambda: portalocker.utils.NamedBoundedSemaphore(
                                maximum=semaphore_limit,
                                name=sem_key,
                                directory=str(MULTIPROCESS_SEMAPHORE_DIR),
                                timeout=0.1,
                            )
                        )
                        # Continue with the new semaphore
                        continue
                    except Exception as recreate_error:
                        logger.error(f'Failed to recreate semaphore: {recreate_error}')
                        # If recreation fails and we're in lax mode, return without lock
                        if semaphore_lax:
                            logger.warning(f'Failed to recreate semaphore "{sem_key}", proceeding without concurrency limit')
                            return False, None
                        raise
                else:
                    # Max recreate attempts exceeded
                    if semaphore_lax:
                        logger.warning(
                            f'Max semaphore recreation attempts exceeded for "{sem_key}", proceeding without concurrency limit'
                        )
                        return False, None
                    raise
            else:
                # Other OS errors
                raise

        except (AssertionError, Exception) as e:
            # Handle "Already locked" error by skipping this attempt
            if 'Already locked' in str(e) or isinstance(e, AssertionError):
                # Lock file might be stale from a previous process crash
                # Wait before retrying
                elapsed = time.time() - start_time
                remaining_time = (sem_timeout - elapsed) if has_timeout and sem_timeout is not None else None
                if remaining_time is None or remaining_time > retry_delay:
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * backoff_factor, 1.0)
                continue
            elif 'Could not acquire' not in str(e) and not isinstance(e, TimeoutError):
                raise

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
                            await asyncio.to_thread(lambda: multiprocess_lock.release())
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
