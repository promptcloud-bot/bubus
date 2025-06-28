import asyncio
import logging
import tempfile
import threading
import time
from collections.abc import Callable, Coroutine
from functools import wraps
from pathlib import Path
from typing import Any, Literal, ParamSpec, TypeVar

import portalocker

PSUTIL_AVAILABLE = False
try:
    import psutil  # type: ignore[import]

    PSUTIL_AVAILABLE = True  # type: ignore[assignment]
except ImportError:
    psutil = None
    pass


logger = logging.getLogger(__name__)


# Define generic type variables for return type and parameters
R = TypeVar('R')
T = TypeVar('T')
P = ParamSpec('P')


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


# Global semaphore registry for retry decorator
GLOBAL_RETRY_SEMAPHORES: dict[str, asyncio.Semaphore] = {}
GLOBAL_RETRY_SEMAPHORE_LOCK = threading.Lock()

# Multiprocess semaphore support
MULTIPROCESS_SEMAPHORE_DIR = Path(tempfile.gettempdir()) / 'browser_use_semaphores'
MULTIPROCESS_SEMAPHORE_DIR.mkdir(exist_ok=True)

# Global multiprocess semaphore registry
MULTIPROCESS_SEMAPHORES: dict[str, portalocker.utils.NamedBoundedSemaphore] = {}
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
    func_name: str,
    semaphore_name: str | None,
    semaphore_scope: Literal['multiprocess', 'global', 'class', 'self'],
    args: tuple[Any, ...],
) -> str:
    """Determine the semaphore key based on scope."""
    base_name = semaphore_name or func_name

    if semaphore_scope == 'multiprocess':
        return base_name
    elif semaphore_scope == 'global':
        return base_name
    elif semaphore_scope == 'class' and args and hasattr(args[0], '__class__'):
        class_name = args[0].__class__.__name__
        return f'{class_name}.{base_name}'
    elif semaphore_scope == 'self' and args:
        instance_id = id(args[0])
        return f'{instance_id}.{base_name}'
    else:
        # Fallback to global if we can't determine scope
        return base_name


def _get_or_create_semaphore(
    sem_key: str,
    semaphore_limit: int,
    semaphore_scope: Literal['multiprocess', 'global', 'class', 'self'],
) -> Any:
    """Get or create a semaphore based on scope."""
    if semaphore_scope == 'multiprocess':
        with MULTIPROCESS_SEMAPHORE_LOCK:
            if sem_key not in MULTIPROCESS_SEMAPHORES:
                MULTIPROCESS_SEMAPHORES[sem_key] = portalocker.utils.NamedBoundedSemaphore(
                    maximum=semaphore_limit,
                    name=sem_key,
                    directory=str(MULTIPROCESS_SEMAPHORE_DIR),
                )
            return MULTIPROCESS_SEMAPHORES[sem_key]
    else:
        with GLOBAL_RETRY_SEMAPHORE_LOCK:
            if sem_key not in GLOBAL_RETRY_SEMAPHORES:
                GLOBAL_RETRY_SEMAPHORES[sem_key] = asyncio.Semaphore(semaphore_limit)
            return GLOBAL_RETRY_SEMAPHORES[sem_key]


def _calculate_semaphore_timeout(
    semaphore_timeout: float | None,
    timeout: float,
    semaphore_limit: int,
) -> float:
    """Calculate the timeout for semaphore acquisition."""
    if semaphore_timeout is None:
        # Default: wait time is if all other slots are occupied with max timeout operations
        # Ensure minimum of timeout value when limit=1
        return max(timeout, timeout * (semaphore_limit - 1))
    else:
        # Use provided timeout, but ensure minimum of 0.01 if 0 was passed
        return max(0.01, semaphore_timeout) if semaphore_timeout == 0 else semaphore_timeout


async def _acquire_multiprocess_semaphore(
    semaphore: Any,
    sem_timeout: float,
    sem_key: str,
    semaphore_lax: bool,
    semaphore_limit: int,
    timeout: float,
) -> tuple[bool, Any]:
    """Acquire a multiprocess semaphore."""
    try:
        # Use a temporary thread to run the blocking operation
        multiprocess_lock = await asyncio.to_thread(
            lambda: semaphore.acquire(timeout=sem_timeout, check_interval=0.05, fail_when_locked=False)
        )
        if multiprocess_lock:
            return True, multiprocess_lock
        else:
            raise TimeoutError(f'Could not acquire semaphore within {sem_timeout}s')
    except Exception as e:
        if 'Could not acquire' in str(e) or isinstance(e, TimeoutError):
            if not semaphore_lax:
                raise TimeoutError(
                    f'Failed to acquire multiprocess semaphore "{sem_key}" within {sem_timeout}s '
                    f'(limit={semaphore_limit}, timeout={timeout}s per operation)'
                )
            logger.warning(
                f'Failed to acquire multiprocess semaphore "{sem_key}" after {sem_timeout:.1f}s, '
                f'proceeding without concurrency limit'
            )
            return False, None
        else:
            raise


async def _acquire_asyncio_semaphore(
    semaphore: asyncio.Semaphore,
    sem_timeout: float,
    sem_key: str,
    semaphore_lax: bool,
    semaphore_limit: int,
    timeout: float,
    sem_start: float,
) -> bool:
    """Acquire an asyncio semaphore."""
    try:
        async with asyncio.timeout(sem_timeout):
            await semaphore.acquire()
            return True
    except TimeoutError:
        sem_wait_time = time.time() - sem_start
        if not semaphore_lax:
            raise TimeoutError(
                f'Failed to acquire semaphore "{sem_key}" within {sem_timeout}s '
                f'(limit={semaphore_limit}, timeout={timeout}s per operation)'
            )
        logger.warning(
            f'Failed to acquire semaphore "{sem_key}" after {sem_wait_time:.1f}s, proceeding without concurrency limit'
        )
        return False


async def _execute_with_retries(
    func: Callable[P, Coroutine[Any, Any, T]],
    args: P.args,  # type: ignore
    kwargs: P.kwargs,  # type: ignore
    retries: int,
    timeout: float,
    wait: float,
    backoff_factor: float,
    retry_on: tuple[type[Exception], ...] | None,
    start_time: float,
    sem_start: float,
    semaphore_limit: int | None,
) -> T:
    """Execute the function with retry logic."""
    for attempt in range(retries + 1):
        try:
            # Execute with per-attempt timeout
            async with asyncio.timeout(timeout):
                return await func(*args, **kwargs)  # type: ignore[reportCallIssue]

        except Exception as e:
            # Check if we should retry this exception
            if retry_on is not None and not isinstance(e, retry_on):
                raise

            if attempt < retries:
                # Calculate wait time with backoff
                current_wait = wait * (backoff_factor**attempt)

                logger.warning(
                    f'{func.__name__} failed (attempt {attempt + 1}/{retries + 1}): '
                    f'{type(e).__name__}: {e}. Waiting {current_wait:.1f}s before retry...'
                )
                await asyncio.sleep(current_wait)
            else:
                # Final failure
                total_time = time.time() - start_time
                sem_wait = time.time() - sem_start - total_time if semaphore_limit else 0

                logger.error(
                    f'{func.__name__} failed after {retries + 1} attempts over {total_time:.1f}s. '
                    f'Semaphore wait: {sem_wait:.1f}s. Final error: {type(e).__name__}: {e}'
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
    wait: float = 3,
    retries: int = 3,
    timeout: float = 5,
    retry_on: tuple[type[Exception], ...] | None = None,
    backoff_factor: float = 1.0,
    semaphore_limit: int | None = None,
    semaphore_name: str | None = None,
    semaphore_lax: bool = True,
    semaphore_scope: Literal['multiprocess', 'global', 'class', 'self'] = 'global',
    semaphore_timeout: float | None = None,
):
    """
        Retry decorator with semaphore support for async functions.

        Args:
                wait: Seconds to wait between retries
                retries: Number of retry attempts after initial failure
                timeout: Per-attempt timeout in seconds
                retry_on: Tuple of exception types to retry on (None = retry all exceptions)
                backoff_factor: Multiplier for wait time after each retry (1.0 = no backoff)
                semaphore_limit: Max concurrent executions (creates semaphore if needed)
                semaphore_name: Name for semaphore (defaults to function name)
                semaphore_lax: If True, continue without semaphore on acquisition failure
                semaphore_scope: Scope for semaphore sharing:
                        - 'global': All calls share one semaphore (default)
                        - 'class': All instances of a class share one semaphore
                        - 'self': Each instance gets its own semaphore
                        - 'multiprocess': All processes on the machine share one semaphore
                semaphore_timeout: Max time to wait for semaphore acquisition (None = timeout * (limit - 1)) or 0.01s

        Example:
                @retry(wait=3, retries=3, timeout=5, semaphore_limit=3, semaphore_scope='self')
                async def some_function(self, ...):
                        # Limited to 5s per attempt, retries up to 3 times on failure
                        # Max 3 concurrent executions per instance

    Notes:
                - semaphore aquision happens once at start time, it's not retried
                - semaphore_timeout is only used if semaphore_limit is set.
                - if semaphore_timeout is set to 0, it will wait forever for a semaphore slot to become available.
                - if semaphore_timeout is set to None, it will wait for the default (timeout * (semaphore_limit - 1)) +0.01s
                - retries are 0-indexed, so retries=1 means the function will be called 2 times total (1 initial + 1 retry)
    """

    def decorator(func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, Coroutine[Any, Any, T]]:
        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:  # type: ignore[return]
            # Initialize semaphore-related variables
            semaphore: Any = None
            semaphore_acquired = False
            multiprocess_lock: Any = None
            sem_start = time.time()

            # Handle semaphore if specified
            if semaphore_limit is not None:
                # Get semaphore key and create/retrieve semaphore
                sem_key = _get_semaphore_key(func.__name__, semaphore_name, semaphore_scope, args)
                semaphore = _get_or_create_semaphore(sem_key, semaphore_limit, semaphore_scope)

                # Calculate timeout for semaphore acquisition
                sem_timeout = _calculate_semaphore_timeout(semaphore_timeout, timeout, semaphore_limit)

                # Acquire semaphore based on type
                if semaphore_scope == 'multiprocess':
                    semaphore_acquired, multiprocess_lock = await _acquire_multiprocess_semaphore(
                        semaphore, sem_timeout, sem_key, semaphore_lax, semaphore_limit, timeout
                    )
                else:
                    semaphore_acquired = await _acquire_asyncio_semaphore(
                        semaphore, sem_timeout, sem_key, semaphore_lax, semaphore_limit, timeout, sem_start
                    )

            # Track active operations and check system overload
            _track_active_operations(increment=True)
            _check_system_overload_if_needed()

            # Execute function with retries
            start_time = time.time()
            try:
                return await _execute_with_retries(
                    func, args, kwargs, retries, timeout, wait, backoff_factor, retry_on, start_time, sem_start, semaphore_limit
                )
            finally:
                # Clean up: decrement active operations and release semaphore
                _track_active_operations(increment=False)

                if semaphore_acquired and semaphore:
                    if semaphore_scope == 'multiprocess' and multiprocess_lock:
                        await asyncio.to_thread(lambda: multiprocess_lock.release())
                    elif semaphore:
                        semaphore.release()

        return wrapper

    return decorator
