from __future__ import annotations

import functools
import inspect
import logging
import timeit
import types
import warnings
from typing import (
    Callable,
    Coroutine,
    ParamSpec,
    Self,
    TypeVar,
    cast,
)

P = ParamSpec("P")
R = TypeVar("R")

logger = logging.getLogger(__name__)


class Timer:
    """A simple timer context manager for measuring execution time.

    Supports both synchronous and asynchronous context management.

    Examples
    --------
    >>> # Synchronous context manager
    >>> with Timer(name="my_sync_func") as t:
    ...     time.sleep(1)
    >>> print(f"Execution time: {t.elapsed_seconds:.4f} seconds")

    >>> # Asynchronous context manager
    >>> import asyncio
    >>> async def async_function() -> None:
    ...     async with Timer(name="async_function") as t:
    ...         await asyncio.sleep(1)
    ...     print(f"Execution time: {t.elapsed_seconds:.4f} seconds")

    >>> # Silent mode
    >>> with Timer(silent=True) as t:
    ...     expensive_operation()
    >>> result = t.elapsed_seconds
    """

    __slots__ = ("_name", "_silent", "_start", "_end")

    def __init__(self, name: str | None = None, *, silent: bool = False) -> None:
        """Initialize the timer.

        Parameters
        ----------
        name : str | None, optional
            A name for the timed block. Defaults to None.
        silent : bool, optional
            If True, suppress logging output. Defaults to False.
        """
        self._name = name
        self._silent = silent
        self._start: float | None = None
        self._end: float | None = None

    @property
    def elapsed_seconds(self) -> float:
        if self._start is None:
            return 0.0
        end = self._end if self._end is not None else timeit.default_timer()
        return end - self._start

    def __enter__(self) -> Self:
        self._start = timeit.default_timer()
        self._end = None
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        self._end = timeit.default_timer()
        if not self._silent:
            name = self._name or "code"
            logger.info(f"{name} took {self.elapsed_seconds:.4f} seconds to execute.")

    async def __aenter__(self) -> Self:
        return self.__enter__()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        self.__exit__(exc_type, exc_val, exc_tb)


def timer(name: str | None = None, *, silent: bool = False) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator to time function execution (supports both sync and async functions).

    Parameters
    ----------
    name : str | None, optional
        Custom name for the timed function. Defaults to function's __name__.
    silent : bool, optional
        If True, suppress logging output. Defaults to False.

    Returns
    -------
    Callable
        Decorated function that logs execution time.

    Examples
    --------
    >>> @timer()
    ... def sync_function():
    ...     time.sleep(1)

    >>> @timer(name="async_operation")
    ... async def async_function():
    ...     await asyncio.sleep(1)

    >>> @timer(silent=True)
    ... def quiet_function():
    ...     pass  # No logging output
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        if inspect.isgeneratorfunction(func) or inspect.isasyncgenfunction(func):
            warnings.warn(
                f"@timer decorator does not properly time generator function '{func.__name__}'. "
                f"Timer will only measure generator object creation, not iteration time.",
                UserWarning,
                stacklevel=2,
            )

        func = cast(Callable[P, R], func)

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                async with Timer(name or func.__name__, silent=silent):
                    # NOTE: Cast needed because type system doesn't know func is async here
                    coro_func = cast(Callable[P, Coroutine[object, object, R]], func)
                    return await coro_func(*args, **kwargs)

            return cast(Callable[P, R], async_wrapper)
        else:

            @functools.wraps(func)
            def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                with Timer(name or func.__name__, silent=silent):
                    return func(*args, **kwargs)

            return sync_wrapper

    return decorator
