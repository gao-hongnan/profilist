from __future__ import annotations

import asyncio
import gc
import os
import tracemalloc
from collections import deque
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime
from itertools import islice
from types import TracebackType
from typing import Literal, Protocol, Self

import psutil
from pydantic import BaseModel, ConfigDict, Field, field_validator

BYTES_PER_KB: int = 1024
BYTES_PER_MB: int = 1024 * 1024
DEFAULT_OBJECT_GROWTH_THRESHOLD: int = 1000
DEFAULT_SNAPSHOT_LIMIT: int = 100

type ContextInfo = dict[str, str | int | float | bool]
type KeyType = Literal["lineno", "filename", "traceback"]
type Unit = Literal["bytes", "kb", "mb"]


class Snapshot(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    created_at: datetime
    timestamp_iso8601: str
    current_python_heap_memory: float
    peak_python_heap_memory: float
    resident_set_size_memory: float
    virtual_memory_size: float
    system_available_memory: float
    native_memory_allocated: float
    memory_usage_percent: float
    total_allocated_objects: int
    object_growth_since_baseline: int
    gc_generation0_collections: int
    gc_generation1_collections: int
    gc_generation2_collections: int
    gc_objects_collected: int
    gc_uncollectable_objects: int
    allocation_sites_tracked: int
    memory_unit: Unit
    decimal_precision: int
    tracemalloc_snapshot: tracemalloc.Snapshot | None = None
    context_metadata: ContextInfo = Field(default_factory=dict)


class AllocationInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

    size_mb: float
    size_kb: float
    count: int
    filename: str | None = None
    lineno: int | None = None
    trace: str | None = None


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


class ProcessInfoProvider(Protocol):
    def get_process_info(self) -> tuple[int, int, float, int]: ...


class GCInfoProvider(Protocol):
    def get_gc_info(self, enable_gc: bool) -> tuple[int, int, int, int, int]: ...


class ProcessInfoCollector:
    def get_process_info(self) -> tuple[int, int, float, int]:
        try:
            process = psutil.Process(os.getpid())
            mem_info = process.memory_info()
            mem_percent = process.memory_percent()
            available = psutil.virtual_memory().available
            return (mem_info.rss, mem_info.vms, mem_percent, available)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
            raise RuntimeError(f"Failed to get process info: {e}") from e


class GCStatsCollector:
    def get_gc_info(self, enable_gc: bool) -> tuple[int, int, int, int, int]:
        gc_counts = gc.get_count()
        uncollectable = len(gc.garbage)
        collected = gc.collect() if enable_gc else 0
        return (gc_counts[0], gc_counts[1], gc_counts[2], collected, uncollectable)


class TracemallocManager:
    def __init__(self) -> None:
        self._started_tracemalloc = False

    def start(self) -> None:
        if not tracemalloc.is_tracing():
            tracemalloc.start()
            self._started_tracemalloc = True

    def stop(self) -> None:
        if self._started_tracemalloc and tracemalloc.is_tracing():
            tracemalloc.stop()
            self._started_tracemalloc = False

    def is_tracing(self) -> bool:
        return tracemalloc.is_tracing()

    def get_traced_memory(self) -> tuple[int, int]:
        return tracemalloc.get_traced_memory()

    def take_snapshot(self) -> tracemalloc.Snapshot:
        return tracemalloc.take_snapshot()


class MemoryProfiler:
    def __init__(
        self,
        *,
        memory_unit: Unit = "mb",
        decimal_places: int = 2,
        track_objects: bool = True,
        enable_gc_before_snapshot: bool = False,
        baseline_snapshot: bool = True,
        max_snapshots: int | None = DEFAULT_SNAPSHOT_LIMIT,
        process_info_provider: ProcessInfoProvider | None = None,
        gc_info_provider: GCInfoProvider | None = None,
    ) -> None:
        self._memory_unit: Unit = memory_unit
        self._decimal_places: int = decimal_places
        self._track_objects = track_objects
        self._enable_gc_before_snapshot = enable_gc_before_snapshot
        self._max_snapshots = max_snapshots

        self._tracemalloc_manager = TracemallocManager()
        self._process_info = process_info_provider or ProcessInfoCollector()
        self._gc_info = gc_info_provider or GCStatsCollector()

        self._is_running = False
        self._baseline_snapshot: Snapshot | None = None
        self._current_snapshot: Snapshot | None = None
        self._snapshots: deque[Snapshot] = deque(maxlen=max_snapshots)

        if baseline_snapshot:
            try:
                self._start_profiling()
                self._baseline_snapshot = self._take_snapshot()
            finally:
                self._stop_profiling()

    def _format_memory(self, bytes_val: int) -> float:
        divisor = BYTES_PER_MB if self._memory_unit == "mb" else BYTES_PER_KB if self._memory_unit == "kb" else 1
        return round(bytes_val / divisor, self._decimal_places)

    def _start_profiling(self) -> None:
        self._tracemalloc_manager.start()
        self._is_running = True

    def _stop_profiling(self) -> None:
        self._tracemalloc_manager.stop()
        self._is_running = False

    def _get_object_count(self) -> int:
        if not self._track_objects:
            return 0
        try:
            return len(gc.get_objects())
        except Exception:
            return 0

    def _take_snapshot(self, label: str | None = None) -> Snapshot:
        context_metadata: ContextInfo = {"label": label} if label else {}

        try:
            if task := asyncio.current_task():
                context_metadata["async_task"] = task.get_name()
        except RuntimeError:
            pass

        now = datetime.now(UTC)
        iso_timestamp = now.isoformat()

        current_heap_bytes, peak_heap_bytes = self._tracemalloc_manager.get_traced_memory()
        tracemalloc_snap = self._tracemalloc_manager.take_snapshot()

        rss_bytes, vms_bytes, percent, available_bytes = self._process_info.get_process_info()
        gc0, gc1, gc2, collected, uncollectable = self._gc_info.get_gc_info(self._enable_gc_before_snapshot)
        total_objects = self._get_object_count()
        object_growth = total_objects - (
            self._baseline_snapshot.total_allocated_objects if self._baseline_snapshot else 0
        )

        native_bytes = max(0, rss_bytes - current_heap_bytes)

        return Snapshot(
            created_at=now,
            timestamp_iso8601=iso_timestamp,
            current_python_heap_memory=self._format_memory(current_heap_bytes),
            peak_python_heap_memory=self._format_memory(peak_heap_bytes),
            resident_set_size_memory=self._format_memory(rss_bytes),
            virtual_memory_size=self._format_memory(vms_bytes),
            system_available_memory=self._format_memory(available_bytes),
            native_memory_allocated=self._format_memory(native_bytes),
            memory_usage_percent=percent,
            total_allocated_objects=total_objects,
            object_growth_since_baseline=object_growth,
            gc_generation0_collections=gc0,
            gc_generation1_collections=gc1,
            gc_generation2_collections=gc2,
            gc_objects_collected=collected,
            gc_uncollectable_objects=uncollectable,
            allocation_sites_tracked=len(tracemalloc_snap.statistics("lineno")),
            memory_unit=self._memory_unit,
            decimal_precision=self._decimal_places,
            tracemalloc_snapshot=tracemalloc_snap,
            context_metadata=context_metadata,
        )

    def snapshot(self, label: str | None = None) -> Snapshot:
        if not self._is_running:
            raise RuntimeError("Profiler is not running. Use as context manager or call start().")

        snap = self._take_snapshot(label=label)
        self._snapshots.append(snap)
        self._current_snapshot = snap
        return snap

    def detect_leaks(self, threshold: float = 1.0, *, force_gc: bool = True) -> LeakReport:
        if force_gc:
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
            unit = first.memory_unit
            object_growth = last.total_allocated_objects - first.total_allocated_objects

            if object_growth > DEFAULT_OBJECT_GROWTH_THRESHOLD:
                recommendations.append(
                    f"Object count grew by {object_growth:,} objects. Check for accumulating collections or caches."
                )

            heap_growth = last.current_python_heap_memory - first.current_python_heap_memory
            rss_growth = last.resident_set_size_memory - first.resident_set_size_memory

            if heap_growth > threshold:
                recommendations.append(f"Python heap grew by {heap_growth:.1f} {unit}. Review large allocations.")

            if rss_growth > heap_growth + threshold:
                native_leak = True
                recommendations.append(
                    f"RSS grew {rss_growth:.1f} {unit} but heap only grew {heap_growth:.1f} {unit}. "
                    "Possible native/C extension leak."
                )

        if self._track_objects:
            obj_types: dict[str, int] = {}
            for obj in islice(gc.get_objects(), 10000):
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

    def get_top_allocations(
        self,
        limit: int = 10,
        key_type: KeyType = "lineno",
        snapshot: tracemalloc.Snapshot | None = None,
    ) -> list[AllocationInfo]:
        if not self._tracemalloc_manager.is_tracing() and snapshot is None:
            import warnings

            warnings.warn(
                "tracemalloc is not active. Use the profiler as a context manager or call start().",
                RuntimeWarning,
                stacklevel=2,
            )
            return []

        snap = snapshot or self._tracemalloc_manager.take_snapshot()
        results: list[AllocationInfo] = []

        for stat in snap.statistics(key_type)[:limit]:
            filename: str | None = None
            lineno: int | None = None
            trace: str | None = None

            if stat.traceback:
                frame = stat.traceback[0]
                filename = frame.filename
                lineno = frame.lineno
                if key_type == "traceback":
                    trace = "\n".join([f"  File {f.filename}:{f.lineno}" for f in stat.traceback])

            results.append(
                AllocationInfo(
                    size_mb=stat.size / BYTES_PER_MB,
                    size_kb=stat.size / BYTES_PER_KB,
                    count=stat.count,
                    filename=filename,
                    lineno=lineno,
                    trace=trace,
                )
            )

        return results

    def compare_allocations(
        self,
        before_label: str,
        after_label: str,
        limit: int = 10,
        key_type: KeyType = "lineno",
    ) -> list[AllocationDifference]:
        snapshots = {
            snap.context_metadata.get("label"): snap for snap in self._snapshots if "label" in snap.context_metadata
        }

        if before_snap := snapshots.get(before_label):
            if after_snap := snapshots.get(after_label):
                if before_snap.tracemalloc_snapshot and after_snap.tracemalloc_snapshot:
                    results: list[AllocationDifference] = []
                    for stat_diff in after_snap.tracemalloc_snapshot.compare_to(
                        before_snap.tracemalloc_snapshot, key_type
                    )[:limit]:
                        frame = stat_diff.traceback[0] if stat_diff.traceback else None

                        results.append(
                            AllocationDifference(
                                filename=frame.filename if frame else "<unknown>",
                                lineno=frame.lineno if frame else 0,
                                size_diff_bytes=stat_diff.size_diff,
                                size_diff_mb=stat_diff.size_diff / BYTES_PER_MB,
                                count_diff=stat_diff.count_diff,
                                size_before_bytes=stat_diff.size - stat_diff.size_diff,
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

    @property
    def baseline(self) -> Snapshot | None:
        return self._baseline_snapshot

    @property
    def latest(self) -> Snapshot | None:
        return self._current_snapshot

    @property
    def all_snapshots(self) -> list[Snapshot]:
        return list(self._snapshots)

    def __enter__(self) -> Self:
        self._start_profiling()
        return self

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

    async def __aenter__(self) -> Self:
        self._start_profiling()
        return self

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
    *,
    memory_unit: Unit = "mb",
    decimal_places: int = 2,
    track_objects: bool = True,
    enable_gc: bool = False,
    baseline_snapshot: bool = False,
    max_snapshots: int | None = DEFAULT_SNAPSHOT_LIMIT,
) -> Generator[MemoryProfiler, None, None]:
    profiler = MemoryProfiler(
        memory_unit=memory_unit,
        decimal_places=decimal_places,
        track_objects=track_objects,
        enable_gc_before_snapshot=enable_gc,
        baseline_snapshot=baseline_snapshot,
        max_snapshots=max_snapshots,
    )

    with profiler:
        yield profiler


@asynccontextmanager
async def track_memory_async(
    *,
    memory_unit: Unit = "mb",
    decimal_places: int = 2,
    track_objects: bool = True,
    enable_gc: bool = False,
    baseline_snapshot: bool = False,
    max_snapshots: int | None = DEFAULT_SNAPSHOT_LIMIT,
) -> AsyncGenerator[MemoryProfiler, None]:
    profiler = MemoryProfiler(
        memory_unit=memory_unit,
        decimal_places=decimal_places,
        track_objects=track_objects,
        enable_gc_before_snapshot=enable_gc,
        baseline_snapshot=baseline_snapshot,
        max_snapshots=max_snapshots,
    )

    async with profiler:
        yield profiler
