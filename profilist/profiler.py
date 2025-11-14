from __future__ import annotations

import asyncio
import gc
import os
import time
import tracemalloc
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, TypeVar

try:
    import psutil
except ImportError:
    psutil = None


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class ComprehensiveSnapshot:
    timestamp: float
    python_heap_current_bytes: int
    python_heap_peak_bytes: int
    allocation_count: int
    deallocation_count: int
    rss_bytes: int
    vms_bytes: int
    percent_memory: float
    available_memory_bytes: int
    gc_gen0_count: int
    gc_gen1_count: int
    gc_gen2_count: int
    gc_collected: int
    gc_uncollectable: int
    total_objects: int
    object_growth: int
    tracemalloc_snapshot: tracemalloc.Snapshot | None = None
    context_info: dict[str, Any] = field(default_factory=dict)

    @property
    def python_heap_mb(self) -> float:
        return self.python_heap_current_bytes / (1024 * 1024)

    @property
    def python_heap_peak_mb(self) -> float:
        return self.python_heap_peak_bytes / (1024 * 1024)

    @property
    def rss_mb(self) -> float:
        return self.rss_bytes / (1024 * 1024)

    @property
    def vms_mb(self) -> float:
        return self.vms_bytes / (1024 * 1024)

    @property
    def available_memory_mb(self) -> float:
        return self.available_memory_bytes / (1024 * 1024)

    @property
    def python_heap_kb(self) -> float:
        return self.python_heap_current_bytes / 1024

    @property
    def rss_kb(self) -> float:
        return self.rss_bytes / 1024

    @property
    def vms_kb(self) -> float:
        return self.vms_bytes / 1024

    @property
    def native_memory_bytes(self) -> int:
        return max(0, self.rss_bytes - self.python_heap_current_bytes)

    @property
    def native_memory_mb(self) -> float:
        return self.native_memory_bytes / (1024 * 1024)

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "python_heap": {
                "current_bytes": self.python_heap_current_bytes,
                "current_mb": self.python_heap_mb,
                "peak_bytes": self.python_heap_peak_bytes,
                "peak_mb": self.python_heap_peak_mb,
                "allocations": self.allocation_count,
                "deallocations": self.deallocation_count,
            },
            "process": {
                "rss_bytes": self.rss_bytes,
                "rss_mb": self.rss_mb,
                "vms_bytes": self.vms_bytes,
                "vms_mb": self.vms_mb,
                "percent_memory": self.percent_memory,
                "available_mb": self.available_memory_mb,
            },
            "native_memory": {
                "bytes": self.native_memory_bytes,
                "mb": self.native_memory_mb,
            },
            "gc": {
                "gen0_count": self.gc_gen0_count,
                "gen1_count": self.gc_gen1_count,
                "gen2_count": self.gc_gen2_count,
                "collected": self.gc_collected,
                "uncollectable": self.gc_uncollectable,
            },
            "objects": {
                "total": self.total_objects,
                "growth": self.object_growth,
            },
            "context": self.context_info,
        }


@dataclass(frozen=True, slots=True)
class AllocationDifference:
    filename: str
    lineno: int
    size_diff_bytes: int
    size_diff_mb: float
    count_diff: int
    size_before_bytes: int
    size_after_bytes: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "filename": self.filename,
            "lineno": self.lineno,
            "size_diff_bytes": self.size_diff_bytes,
            "size_diff_mb": self.size_diff_mb,
            "count_diff": self.count_diff,
            "size_before_bytes": self.size_before_bytes,
            "size_after_bytes": self.size_after_bytes,
        }


@dataclass
class LeakReport:
    has_leaks: bool
    circular_references: list[tuple[type, ...]]
    growing_objects: dict[str, int]
    native_leak_detected: bool
    uncollectable_objects: int
    recommendations: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "has_leaks": self.has_leaks,
            "circular_references": [[t.__name__ for t in chain] for chain in self.circular_references],
            "growing_objects": self.growing_objects,
            "native_leak_detected": self.native_leak_detected,
            "uncollectable_objects": self.uncollectable_objects,
            "recommendations": self.recommendations,
        }


class MemoryProfiler:
    def __init__(
        self,
        track_objects: bool = True,
        enable_gc_before_snapshot: bool = False,
        baseline_snapshot: bool = True,
    ) -> None:
        self.track_objects = track_objects
        self.enable_gc_before_snapshot = enable_gc_before_snapshot

        if psutil is None:
            raise ImportError("psutil is required for MemoryProfiler. Install with: pip install psutil")

        self._is_running = False
        self._started_tracemalloc = False
        self._baseline_snapshot: ComprehensiveSnapshot | None = None
        self._current_snapshot: ComprehensiveSnapshot | None = None
        self._snapshots: list[ComprehensiveSnapshot] = []
        self._baseline_object_count: int = 0
        self._baseline_gc_stats: tuple[int, int, int] = (0, 0, 0)
        self._task_name: str | None = None

        if baseline_snapshot:
            self._start_profiling()
            self._baseline_snapshot = self._take_snapshot()
            self._baseline_object_count = self._baseline_snapshot.total_objects
            self._baseline_gc_stats = (
                self._baseline_snapshot.gc_gen0_count,
                self._baseline_snapshot.gc_gen1_count,
                self._baseline_snapshot.gc_gen2_count,
            )
            self._stop_profiling()

    def _start_profiling(self) -> None:
        if not tracemalloc.is_tracing():
            tracemalloc.start()
            self._started_tracemalloc = True
        self._is_running = True

    def _stop_profiling(self) -> None:
        if self._started_tracemalloc and tracemalloc.is_tracing():
            tracemalloc.stop()
            self._started_tracemalloc = False
        self._is_running = False

    def _get_process_info(self) -> tuple[int, int, float, int]:
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        mem_percent = process.memory_percent()
        available = psutil.virtual_memory().available

        return (mem_info.rss, mem_info.vms, mem_percent, available)

    def _get_gc_info(self) -> tuple[int, int, int, int, int]:
        gc_counts = gc.get_count()
        uncollectable = len(gc.garbage)

        collected = 0
        if self.enable_gc_before_snapshot:
            collected = gc.collect()

        return (gc_counts[0], gc_counts[1], gc_counts[2], collected, uncollectable)

    def _get_object_count(self) -> int:
        if not self.track_objects:
            return 0
        return len(gc.get_objects())

    def _take_snapshot(self, label: str | None = None) -> ComprehensiveSnapshot:
        context_info: dict[str, Any] = {}

        if label:
            context_info["label"] = label

        try:
            task = asyncio.current_task()
            if task is not None:
                context_info["async_task"] = task.get_name()
                self._task_name = task.get_name()
        except RuntimeError:
            pass

        current_heap, peak_heap = tracemalloc.get_traced_memory()

        tracemalloc_snap = tracemalloc.take_snapshot()
        allocation_count = len(tracemalloc_snap.statistics("lineno"))

        deallocation_count = 0

        rss, vms, percent, available = self._get_process_info()

        gc0, gc1, gc2, collected, uncollectable = self._get_gc_info()

        total_objects = self._get_object_count()
        object_growth = total_objects - self._baseline_object_count

        return ComprehensiveSnapshot(
            timestamp=time.time(),
            python_heap_current_bytes=current_heap,
            python_heap_peak_bytes=peak_heap,
            allocation_count=allocation_count,
            deallocation_count=deallocation_count,
            tracemalloc_snapshot=tracemalloc_snap,
            rss_bytes=rss,
            vms_bytes=vms,
            percent_memory=percent,
            available_memory_bytes=available,
            gc_gen0_count=gc0,
            gc_gen1_count=gc1,
            gc_gen2_count=gc2,
            gc_collected=collected,
            gc_uncollectable=uncollectable,
            total_objects=total_objects,
            object_growth=object_growth,
            context_info=context_info,
        )

    def snapshot(self, label: str | None = None) -> ComprehensiveSnapshot:
        if not self._is_running:
            raise RuntimeError("Profiler is not running. Use as context manager or call start().")

        snap = self._take_snapshot(label=label)
        self._snapshots.append(snap)
        self._current_snapshot = snap
        return snap

    def detect_leaks(self, threshold_mb: float = 1.0) -> LeakReport:
        has_leaks = False
        recommendations: list[str] = []
        circular_refs: list[tuple[type, ...]] = []
        growing_objects: dict[str, int] = {}
        native_leak = False

        gc.collect()
        if len(gc.garbage) > 0:
            has_leaks = True
            recommendations.append(f"Found {len(gc.garbage)} uncollectable objects. Check for circular references.")
            for obj in gc.garbage[:10]:
                refs = gc.get_referrers(obj)
                chain = tuple(type(r) for r in refs[:5])
                if chain:
                    circular_refs.append(chain)

        if len(self._snapshots) >= 2:
            first_snap = self._snapshots[0]
            last_snap = self._snapshots[-1]

            object_growth = last_snap.total_objects - first_snap.total_objects
            if object_growth > 1000:
                has_leaks = True
                recommendations.append(
                    f"Object count grew by {object_growth:,} objects. Check for accumulating collections or caches."
                )

            heap_growth_mb = (last_snap.python_heap_current_bytes - first_snap.python_heap_current_bytes) / (
                1024 * 1024
            )
            if heap_growth_mb > threshold_mb:
                recommendations.append(f"Python heap grew by {heap_growth_mb:.1f} MB. Review large allocations.")

            rss_growth_mb = (last_snap.rss_bytes - first_snap.rss_bytes) / (1024 * 1024)
            if rss_growth_mb > heap_growth_mb + threshold_mb:
                has_leaks = True
                native_leak = True
                recommendations.append(
                    f"RSS grew {rss_growth_mb:.1f} MB but heap only grew {heap_growth_mb:.1f} MB. "
                    "Possible native/C extension leak."
                )

        if self.track_objects and len(self._snapshots) >= 2:
            obj_types: dict[str, int] = {}
            for obj in gc.get_objects()[:10000]:
                obj_type = type(obj).__name__
                obj_types[obj_type] = obj_types.get(obj_type, 0) + 1

            growing_objects = dict(sorted(obj_types.items(), key=lambda x: x[1], reverse=True)[:10])

        uncollectable = gc.get_stats()[-1].get("uncollectable", 0)
        if uncollectable > 0:
            has_leaks = True
            recommendations.append(
                f"Found {uncollectable} uncollectable objects. Review __del__ methods and circular refs."
            )

        return LeakReport(
            has_leaks=has_leaks or len(recommendations) > 0,
            circular_references=circular_refs,
            growing_objects=growing_objects,
            native_leak_detected=native_leak,
            uncollectable_objects=len(gc.garbage),
            recommendations=recommendations if recommendations else ["No obvious leaks detected."],
        )

    def get_top_allocations(self, limit: int = 10, key_type: str = "lineno") -> list[dict[str, Any]]:
        if not tracemalloc.is_tracing():
            return []

        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics(key_type)

        results = []
        for stat in top_stats[:limit]:
            alloc_info: dict[str, Any] = {
                "size_mb": stat.size / (1024 * 1024),
                "size_kb": stat.size / 1024,
                "count": stat.count,
            }

            if stat.traceback:
                frame = stat.traceback[0]
                alloc_info["filename"] = frame.filename
                alloc_info["lineno"] = frame.lineno

                if key_type == "traceback":
                    alloc_info["trace"] = "\n".join([f"  File {f.filename}:{f.lineno}" for f in stat.traceback])

            results.append(alloc_info)

        return results

    def compare_allocations(
        self,
        before_label: str,
        after_label: str,
        limit: int = 10,
        key_type: str = "lineno",
    ) -> list[AllocationDifference]:
        before_snap: ComprehensiveSnapshot | None = None
        after_snap: ComprehensiveSnapshot | None = None

        for snap in self._snapshots:
            label = snap.context_info.get("label")
            if label == before_label:
                before_snap = snap
            elif label == after_label:
                after_snap = snap

        if before_snap is None:
            raise ValueError(
                f"Snapshot with label '{before_label}' not found. "
                f"Available labels: {[s.context_info.get('label') for s in self._snapshots if 'label' in s.context_info]}"
            )
        if after_snap is None:
            raise ValueError(
                f"Snapshot with label '{after_label}' not found. "
                f"Available labels: {[s.context_info.get('label') for s in self._snapshots if 'label' in s.context_info]}"
            )

        if before_snap.tracemalloc_snapshot is None:
            raise ValueError(
                f"Snapshot '{before_label}' was taken when tracemalloc was not running. "
                "Ensure profiler is active when taking snapshots."
            )
        if after_snap.tracemalloc_snapshot is None:
            raise ValueError(
                f"Snapshot '{after_label}' was taken when tracemalloc was not running. "
                "Ensure profiler is active when taking snapshots."
            )

        after_stats = after_snap.tracemalloc_snapshot.compare_to(before_snap.tracemalloc_snapshot, key_type)

        results: list[AllocationDifference] = []

        for stat_diff in after_stats[:limit]:
            if stat_diff.traceback:
                frame = stat_diff.traceback[0]
                filename = frame.filename
                lineno = frame.lineno
            else:
                filename = "<unknown>"
                lineno = 0

            diff = AllocationDifference(
                filename=filename,
                lineno=lineno,
                size_diff_bytes=stat_diff.size_diff,
                size_diff_mb=stat_diff.size_diff / (1024 * 1024),
                count_diff=stat_diff.count_diff,
                size_before_bytes=stat_diff.size - stat_diff.size_diff if stat_diff.size_diff > 0 else stat_diff.size,
                size_after_bytes=stat_diff.size,
            )
            results.append(diff)

        return results

    def __enter__(self) -> ComprehensiveSnapshot:
        self._start_profiling()
        return self.snapshot()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.snapshot()
        self._stop_profiling()

    async def __aenter__(self) -> ComprehensiveSnapshot:
        self._start_profiling()
        return self.snapshot()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.snapshot()
        self._stop_profiling()


@contextmanager
def track_memory(
    track_objects: bool = True,
    enable_gc: bool = False,
) -> Generator[MemoryProfiler, None, None]:
    profiler = MemoryProfiler(
        track_objects=track_objects,
        enable_gc_before_snapshot=enable_gc,
        baseline_snapshot=False,
    )

    with profiler:
        yield profiler


@asynccontextmanager
async def track_memory_async(
    track_objects: bool = True,
    enable_gc: bool = False,
) -> AsyncGenerator[MemoryProfiler, None]:
    profiler = MemoryProfiler(
        track_objects=track_objects,
        enable_gc_before_snapshot=enable_gc,
        baseline_snapshot=False,
    )

    async with profiler:
        yield profiler
