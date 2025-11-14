from __future__ import annotations

from .memory import (
    MemoryTrimmer,
    get_memory_info,
    is_memory_trim_supported,
    memory_trimmer,
    trim_memory,
    trim_memory_decorator,
)
from .timer import Timer, timer

__all__ = [
    "Timer",
    "timer",
    "MemoryTrimmer",
    "trim_memory",
    "memory_trimmer",
    "trim_memory_decorator",
    "get_memory_info",
    "is_memory_trim_supported",
]

__version__ = "2.0.0"
