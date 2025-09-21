from __future__ import annotations

from typing import TYPE_CHECKING

from ..profiler import ProfilerFactory
from .cprofile import CProfileProfiler

ProfilerFactory.register("cprofile", CProfileProfiler)

if TYPE_CHECKING:
    from .scalene import ScaleneProfiler
else:
    try:
        from .scalene import ScaleneProfiler
    except ImportError:
        ScaleneProfiler = None

__all__ = ["CProfileProfiler", "ScaleneProfiler"]
