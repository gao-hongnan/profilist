from __future__ import annotations

import asyncio
import gc
import os
import time
import tracemalloc
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from types import TracebackType
from typing import Any, TypeVar

import psutil
from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator

T = TypeVar("T")


class ComprehensiveSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

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
    context_info: dict[str, Any] = Field(default_factory=dict)

    @computed_field
    def python_heap_mb(self) -> float:
        return self.python_heap_current_bytes / (1024 * 1024)

    @computed_field
    def python_heap_peak_mb(self) -> float:
        return self.python_heap_peak_bytes / (1024 * 1024)

    @computed_field
    def rss_mb(self) -> float:
        return self.rss_bytes / (1024 * 1024)

    @computed_field
    def vms_mb(self) -> float:
        return self.vms_bytes / (1024 * 1024)

    @computed_field
    def available_memory_mb(self) -> float:
        return self.available_memory_bytes / (1024 * 1024)

    @computed_field
    def python_heap_kb(self) -> float:
        return self.python_heap_current_bytes / 1024

    @computed_field
    def rss_kb(self) -> float:
        return self.rss_bytes / 1024

    @computed_field
    def vms_kb(self) -> float:
        return self.vms_bytes / 1024

    @computed_field
    def native_memory_bytes(self) -> int:
        return max(0, self.rss_bytes - self.python_heap_current_bytes)

    @computed_field
    def native_memory_mb(self) -> float:
        return max(0, self.rss_bytes - self.python_heap_current_bytes) / (1024 * 1024)


class AllocationDifference(BaseModel):
    model_config = ConfigDict(frozen=True)

    filename: str = Field(default="<unknown>")
    lineno: int = Field(ge=0)
    size_diff_bytes: int
    size_diff_mb: float
    count_diff: int
    size_before_bytes: int = Field(ge=0)
    size_after_bytes: int = Field(ge=0)

    @field_validator("filename", mode="before")
    @classmethod
    def validate_filename(cls, v: str | None) -> str:
        if not v:
            return "<unknown>"
        return str(v)


class LeakReport(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    has_leaks: bool
    circular_references: list[tuple[type, ...]]
    growing_objects: dict[str, int]
    native_leak_detected: bool
    uncollectable_objects: int = Field(ge=0)
    recommendations: list[str]

    @field_validator("recommendations")
    @classmethod
    def validate_recommendations(cls, v: list[str]) -> list[str]:
        return v or ["No leaks detected."]


class MemoryProfiler:
    def __init__(
        self,
        track_objects: bool = True,
        enable_gc_before_snapshot: bool = False,
        baseline_snapshot: bool = True,
    ) -> None:
        self.track_objects = track_objects
        self.enable_gc_before_snapshot = enable_gc_before_snapshot

        self._is_running = False
        self._started_tracemalloc = False
        self._baseline_snapshot: ComprehensiveSnapshot | None = None
        self._current_snapshot: ComprehensiveSnapshot | None = None
        self._snapshots: list[ComprehensiveSnapshot] = []
        self._task_name: str | None = None

        if baseline_snapshot:
            try:
                self._start_profiling()
                self._baseline_snapshot = self._take_snapshot()
            finally:
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
        try:
            process = psutil.Process(os.getpid())
            mem_info = process.memory_info()
            mem_percent = process.memory_percent()
            available = psutil.virtual_memory().available
            return (mem_info.rss, mem_info.vms, mem_percent, available)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
            raise RuntimeError(f"Failed to get process info: {e}") from e

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
        try:
            return len(gc.get_objects())
        except Exception:
            return 0

    def _take_snapshot(self, label: str | None = None) -> ComprehensiveSnapshot:
        context_info = {"label": label} if label else {}

        try:
            if task := asyncio.current_task():
                context_info["async_task"] = task.get_name()
                self._task_name = task.get_name()
        except RuntimeError:
            pass

        current_heap, peak_heap = tracemalloc.get_traced_memory()
        tracemalloc_snap = tracemalloc.take_snapshot()

        rss, vms, percent, available = self._get_process_info()
        gc0, gc1, gc2, collected, uncollectable = self._get_gc_info()
        total_objects = self._get_object_count()
        object_growth = total_objects - (self._baseline_snapshot.total_objects if self._baseline_snapshot else 0)

        return ComprehensiveSnapshot(
            timestamp=time.time(),
            python_heap_current_bytes=current_heap,
            python_heap_peak_bytes=peak_heap,
            allocation_count=len(tracemalloc_snap.statistics("lineno")),
            deallocation_count=0,
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
        gc.collect()
        recommendations: list[str] = []
        circular_refs: list[tuple[type, ...]] = []
        native_leak = False

        if gc.garbage:
            recommendations.append(f"Found {len(gc.garbage)} uncollectable objects. Check for circular references.")
            circular_refs = [
                tuple(type(r) for r in gc.get_referrers(obj)[:5]) for obj in gc.garbage[:10] if gc.get_referrers(obj)
            ]

        if len(self._snapshots) >= 2:
            first, last = self._snapshots[0], self._snapshots[-1]
            object_growth = last.total_objects - first.total_objects

            if object_growth > 1000:
                recommendations.append(
                    f"Object count grew by {object_growth:,} objects. Check for accumulating collections or caches."
                )

            heap_growth_mb = (last.python_heap_current_bytes - first.python_heap_current_bytes) / (1024 * 1024)
            rss_growth_mb = (last.rss_bytes - first.rss_bytes) / (1024 * 1024)

            if heap_growth_mb > threshold_mb:
                recommendations.append(f"Python heap grew by {heap_growth_mb:.1f} MB. Review large allocations.")

            if rss_growth_mb > heap_growth_mb + threshold_mb:
                native_leak = True
                recommendations.append(
                    f"RSS grew {rss_growth_mb:.1f} MB but heap only grew {heap_growth_mb:.1f} MB. "
                    "Possible native/C extension leak."
                )

        if self.track_objects:
            obj_types: dict[str, int] = {}
            for obj in gc.get_objects()[:10000]:
                obj_types[type(obj).__name__] = obj_types.get(type(obj).__name__, 0) + 1
            growing_objects = dict(sorted(obj_types.items(), key=lambda x: x[1], reverse=True)[:10])
        else:
            growing_objects = {}

        uncollectable = len(gc.garbage)
        if uncollectable > 0:
            recommendations.append(
                f"Found {uncollectable} uncollectable objects. Review __del__ methods and circular refs."
            )

        return LeakReport(
            has_leaks=bool(gc.garbage or recommendations),
            circular_references=circular_refs,
            growing_objects=growing_objects,
            native_leak_detected=native_leak,
            uncollectable_objects=len(gc.garbage),
            recommendations=recommendations or ["No obvious leaks detected."],
        )

    def get_top_allocations(self, limit: int = 10, key_type: str = "lineno") -> list[dict[str, Any]]:
        if not tracemalloc.is_tracing():
            import warnings

            warnings.warn(
                "tracemalloc is not active. Use the profiler as a context manager or call start().",
                RuntimeWarning,
                stacklevel=2,
            )
            return []

        results = []
        for stat in tracemalloc.take_snapshot().statistics(key_type)[:limit]:
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
        snapshots = {snap.context_info.get("label"): snap for snap in self._snapshots if "label" in snap.context_info}

        if before_snap := snapshots.get(before_label):
            if after_snap := snapshots.get(after_label):
                if before_snap.tracemalloc_snapshot and after_snap.tracemalloc_snapshot:
                    results = []
                    for stat_diff in after_snap.tracemalloc_snapshot.compare_to(
                        before_snap.tracemalloc_snapshot, key_type
                    )[:limit]:
                        frame = stat_diff.traceback[0] if stat_diff.traceback else None

                        results.append(
                            AllocationDifference(
                                filename=frame.filename if frame else "<unknown>",
                                lineno=frame.lineno if frame else 0,
                                size_diff_bytes=stat_diff.size_diff,
                                size_diff_mb=stat_diff.size_diff / (1024 * 1024),
                                count_diff=stat_diff.count_diff,
                                size_before_bytes=stat_diff.size - stat_diff.size_diff
                                if stat_diff.size_diff > 0
                                else stat_diff.size,
                                size_after_bytes=stat_diff.size,
                            )
                        )
                    return results
                else:
                    missing = before_label if not before_snap.tracemalloc_snapshot else after_label
                    raise ValueError(f"Snapshot '{missing}' was taken when tracemalloc was not running.")
            else:
                raise ValueError(
                    f"Snapshot with label '{after_label}' not found. Available labels: {list(snapshots.keys())}"
                )
        else:
            raise ValueError(
                f"Snapshot with label '{before_label}' not found. Available labels: {list(snapshots.keys())}"
            )

    def __enter__(self) -> ComprehensiveSnapshot:
        self._start_profiling()
        return self.snapshot()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        try:
            self.snapshot()
        finally:
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
        try:
            self.snapshot()
        finally:
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
