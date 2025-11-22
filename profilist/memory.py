"""
Memory management utilities for long-running processes.

Provides Linux-focused memory trimming using ctypes with optional threading support.
Combines Python garbage collection with OS-level memory management via malloc_trim().
"""

from __future__ import annotations

import ctypes
import gc
import logging
import sys
import threading
import time
from collections.abc import Callable, Generator
from contextlib import contextmanager
from types import TracebackType
from typing import Any, ParamSpec, Self, TypeVar

import psutil

logger = logging.getLogger(__name__)

P = ParamSpec("P")
T = TypeVar("T")


class MemoryTrimmer:
    """Linux-focused memory trimming with optional threading support."""

    def __init__(self, threaded: bool = False, interval: float | None = None) -> None:
        self.threaded = threaded
        self.interval = interval
        self._is_linux: bool = sys.platform.startswith("linux")
        self._trim_thread: threading.Thread | None = None
        self._stop_event: threading.Event | None = None
        self._libc: ctypes.CDLL | None = None
        self._malloc_trim_func: Any = None

        if self._is_linux:
            self._setup_malloc_trim()
        else:
            logger.warning(f"Platform {sys.platform} not supported - memory trimming disabled")

    def _setup_malloc_trim(self) -> None:
        """Setup ctypes for malloc_trim on Linux systems."""
        try:
            self._libc = ctypes.CDLL(None)
            self._malloc_trim_func = self._libc.malloc_trim
            if self._malloc_trim_func:
                self._malloc_trim_func.argtypes = [ctypes.c_size_t]
                self._malloc_trim_func.restype = ctypes.c_int
        except (OSError, AttributeError) as e:
            logger.warning(f"Failed to setup malloc_trim: {e}")
            self._malloc_trim_func = None

    def _get_memory_usage(self) -> int:
        """Get current memory usage in bytes using psutil."""
        try:
            return int(psutil.Process().memory_info().rss)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return 0

    def _trim_python_memory(self) -> int:
        """Force Python garbage collection."""
        gc.collect()
        return 0

    def _trim_os_memory(self, pad: int = 0) -> int:
        """Trim OS memory using malloc_trim on Linux."""
        if not self._malloc_trim_func or not self._is_linux:
            return 0

        try:
            result = self._malloc_trim_func(pad)
            if result == 1:
                return 0
            return -1
        except Exception as e:
            logger.warning(f"malloc_trim failed: {e}")
            return -1

    def trim_memory(self, pad: int = 0) -> int:
        """Trim both Python and OS memory. Returns bytes reclaimed estimate."""
        if not self._is_linux:
            logger.info("Memory trimming not supported on this platform")
            return 0

        before = self._get_memory_usage()

        python_reclaimed = self._trim_python_memory()
        os_result = self._trim_os_memory(pad)

        after = self._get_memory_usage()
        total_reclaimed = max(0, before - after)

        logger.info(
            f"Memory trimmed: {total_reclaimed:,} bytes "
            f"(Python: {python_reclaimed}, OS: {'success' if os_result == 0 else 'failed'})"
        )

        return total_reclaimed

    def start_background_trimming(self, interval: float = 30.0, pad: int = 0) -> None:
        """Start background thread for periodic memory trimming."""
        if not self._is_linux:
            logger.warning("Background trimming not supported on this platform")
            return

        if self._trim_thread and self._trim_thread.is_alive():
            logger.warning("Background trimming already running")
            return

        self.interval = interval
        self._stop_event = threading.Event()
        self._trim_thread = threading.Thread(
            target=self._background_trimmer, args=(pad,), daemon=True, name="MemoryTrimmer"
        )
        self._trim_thread.start()
        logger.info(f"Started background memory trimming every {interval}s")

    def stop_background_trimming(self) -> None:
        """Stop background memory trimming thread."""
        if not self._trim_thread or not self._trim_thread.is_alive():
            return

        if self._stop_event:
            self._stop_event.set()

        self._trim_thread.join(timeout=5.0)
        logger.info("Stopped background memory trimming")

    def _background_trimmer(self, pad: int) -> None:
        """Background thread function for periodic trimming."""
        while not (self._stop_event and self._stop_event.is_set()):
            self.trim_memory(pad)

            if self._stop_event:
                self._stop_event.wait(self.interval or 30.0)
            else:
                time.sleep(self.interval or 30.0)

    @property
    def memory_reclaimed_mb(self) -> float:
        """Calculate memory reclaimed in MB (placeholder)."""
        return 0.0

    @property
    def is_linux(self) -> bool:
        """Check if running on Linux platform."""
        return self._is_linux

    @property
    def is_trimming_supported(self) -> bool:
        """Check if memory trimming is supported."""
        return self._is_linux and self._malloc_trim_func is not None

    def __enter__(self) -> Self:
        """Context manager entry."""
        if self.threaded and self.interval:
            self.start_background_trimming(self.interval)
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Context manager exit."""
        if self.threaded:
            self.stop_background_trimming()
        self.trim_memory()


def trim_memory(pad: int = 0) -> int:
    """Simple function to trim memory. Returns bytes reclaimed."""
    trimmer = MemoryTrimmer()
    return trimmer.trim_memory(pad)


@contextmanager
def memory_trimmer(threaded: bool = False, interval: float | None = None) -> Generator[MemoryTrimmer, None, None]:
    """Context manager for memory trimming."""
    with MemoryTrimmer(threaded=threaded, interval=interval) as trimmer:
        yield trimmer


def trim_memory_decorator(
    threaded: bool = False,
    interval: float | None = None,
    trim_before: bool = True,
    trim_after: bool = True,
    pad: int = 0,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator for automatic memory trimming around functions."""

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            trimmer = MemoryTrimmer(threaded=threaded, interval=interval)

            try:
                if trim_before:
                    trimmer.trim_memory(pad)

                if threaded and interval:
                    trimmer.start_background_trimming(interval, pad)

                result = func(*args, **kwargs)

                if trim_after:
                    trimmer.trim_memory(pad)

                return result

            finally:
                if threaded and interval:
                    trimmer.stop_background_trimming()

        return wrapper

    return decorator


def get_memory_info() -> dict[str, int | float]:
    """Get current memory usage information."""
    try:
        process = psutil.Process()
        memory_info = process.memory_info()

        return {
            "rss_bytes": memory_info.rss,
            "rss_mb": memory_info.rss / 1024 / 1024,
            "vms_bytes": memory_info.vms,
            "vms_mb": memory_info.vms / 1024 / 1024,
            "percent": process.memory_percent(),
        }
    except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
        logger.warning(f"Failed to get memory info: {e}")
        return {
            "rss_bytes": 0,
            "rss_mb": 0.0,
            "vms_bytes": 0,
            "vms_mb": 0.0,
            "percent": 0.0,
        }


def is_memory_trim_supported() -> bool:
    """Check if memory trimming is supported on current platform."""
    trimmer = MemoryTrimmer()
    return trimmer.is_trimming_supported
