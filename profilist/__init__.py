from __future__ import annotations

from . import backends
from .models import ProfileResult
from .profiler import Profiler, ProfilerFactory, profile
from .timer import Timer, timer

__all__ = [
    "Timer",
    "timer",
    "Profiler",
    "ProfilerFactory",
    "ProfileResult",
    "profile",
    "backends",
]

__version__ = "2.0.0"
