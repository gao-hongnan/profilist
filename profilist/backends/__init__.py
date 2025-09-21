from __future__ import annotations

from ..profiler import ProfilerFactory
from .cprofile import CProfileProfiler

ProfilerFactory.register("cprofile", CProfileProfiler)

try:
    from .scalene import ScaleneProfiler
except ImportError:
    ScaleneProfiler = None  # type: ignore[assignment, misc]

__all__ = ["CProfileProfiler", "ScaleneProfiler"]
