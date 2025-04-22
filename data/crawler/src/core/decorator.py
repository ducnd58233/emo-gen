import time
from functools import wraps
from threading import Lock
from typing import Any, Callable, Optional, TypeVar, cast, Dict, List
from core.logger import get_logger
import inspect

T = TypeVar('T')
F = TypeVar('F', bound=Callable[..., Any])

logger = get_logger("core.decorator")


def singleton(cls):
    """
    Decorator to implement the Singleton pattern.
    Ensures only one instance of a class is created.
    """
    orig_new = cls.__new__
    lock = Lock()
    instance = None

    @wraps(orig_new)
    def __new__(cls, *args, **kwargs):
        nonlocal instance
        with lock:
            if instance is None:
                instance = orig_new(cls, *args, **kwargs)
            return instance

    cls.__new__ = __new__
    return cls


def timer(func: Optional[F] = None, *, log_level: str = "info", name: Optional[str] = None) -> Any:
    """
    Decorator to measure and log execution time of a function

    Args:
        func: Function to decorate
        log_level: Logging level (debug, info, warning, error)
        name: Custom name for the timed operation

    Usage:
        @timer
        def my_function():
            # Simple timing with default settings
            pass

        @timer(log_level="debug", name="Database Query")
        def query_db():
            # Custom log level and operation name
            pass
    """
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            operation_name = name or func.__name__
            start_time = time.time()

            try:
                return func(*args, **kwargs)
            finally:
                end_time = time.time()
                duration = end_time - start_time
                log_method = getattr(logger, log_level.lower(), logger.info)
                log_method(
                    f"{operation_name} completed in {duration:.4f} seconds")

        return cast(F, wrapper)

    if func:
        return decorator(func)
    return decorator


def retry(max_attempts=3, delay=1, backoff=2, exceptions=(Exception,)):
    """
    Retry decorator with exponential backoff

    Args:
        max_attempts: Maximum number of attempts
        delay: Initial delay between retries (seconds)
        backoff: Backoff multiplier
        exceptions: Tuple of exceptions to catch
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            current_delay = delay

            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    if attempt == max_attempts:
                        raise

                    logger.warning(
                        f"Attempt {attempt} failed: {e}. Retrying in {current_delay}s..."
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff

        return wrapper
    return decorator


def memoize(ttl_seconds: Optional[int] = None):
    """
    Cache function results with optional TTL

    Args:
        ttl_seconds: Time to live in seconds, None for indefinite caching
    """
    cache: Dict[str, Any] = {}
    timestamps: Dict[str, float] = {}

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Create a unique key from function arguments
            key = f"{func.__name__}:{str(args)}:{str(sorted(kwargs.items()))}"

            current_time = time.time()

            # Check if cached and not expired
            if key in cache:
                if ttl_seconds is None or current_time - timestamps[key] < ttl_seconds:
                    return cache[key]

            # Call function and cache result
            result = func(*args, **kwargs)
            cache[key] = result
            timestamps[key] = current_time

            return result

        # Add method to clear cache
        wrapper.clear_cache = lambda: cache.clear()

        return wrapper

    return decorator


def validate_input(**validators):
    """
    Validates function inputs based on provided validator functions

    Example:
    @validate_input(
        user_id=lambda x: isinstance(x, int) and x > 0,
        name=lambda x: isinstance(x, str) and len(x) > 0
    )
    def create_user(user_id, name):
        pass
    """
    def decorator(func):
        sig = inspect.signature(func)

        @wraps(func)
        def wrapper(*args, **kwargs):
            # Merge args and kwargs based on function signature
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()

            # Validate each parameter with its validator
            for param_name, validator in validators.items():
                if param_name in bound_args.arguments:
                    value = bound_args.arguments[param_name]
                    if not validator(value):
                        raise ValueError(
                            f"Invalid value for parameter '{param_name}': {value}")

            return func(*args, **kwargs)

        return wrapper

    return decorator


def lazy_property(func):
    """
    Decorator for lazy-loaded properties that are computed once and cached
    """
    attr_name = '_lazy_' + func.__name__

    @property
    @wraps(func)
    def lazy_property_wrapper(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, func(self))
        return getattr(self, attr_name)

    return lazy_property_wrapper
