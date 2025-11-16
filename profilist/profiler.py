from __future__ import annotations

import asyncio
import gc
import os
import threading
import tracemalloc
from collections import deque
from collections.abc import Callable, Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from types import TracebackType
from typing import Literal, Self, TypeAlias

import psutil
from pydantic import BaseModel, ConfigDict, Field

BYTES_PER_KB: int = 1024
BYTES_PER_MB: int = 1024 * 1024

KeyType: TypeAlias = Literal["lineno", "filename", "traceback"]
Unit: TypeAlias = Literal["bytes", "kb", "mb"]
MemoryValue: TypeAlias = int | float
ObjectCount: TypeAlias = int


class ProfilerConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    memory_unit: Unit = "mb"
    decimal_places: int = Field(default=2, ge=0, le=10)
    track_objects: bool = True
    enable_gc_before_snapshot: bool = False
    baseline_snapshot: bool = True
    max_snapshots: int = Field(default=100, gt=0)
    object_growth_threshold: int = Field(default=1000, gt=0)
    leak_detection_sample_size: int = Field(default=10000, gt=0)
    circular_ref_sample_limit: int = Field(default=10, gt=0)
    circular_ref_referrer_limit: int = Field(default=5, gt=0)


class ProcessMemoryInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

    resident_set_size: MemoryValue = Field(ge=0)
    virtual_memory_size: MemoryValue = Field(ge=0)
    memory_usage_percent: float = Field(ge=0.0, le=100.0)
    system_available: MemoryValue = Field(ge=0)
    unit: Unit


class GCStatistics(BaseModel):
    model_config = ConfigDict(frozen=True)

    generation0_collections: int = Field(ge=0)
    generation1_collections: int = Field(ge=0)
    generation2_collections: int = Field(ge=0)
    objects_collected: ObjectCount = Field(ge=0)
    uncollectable_objects: ObjectCount = Field(ge=0)


class HeapMemoryInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

    current: MemoryValue = Field(ge=0)
    peak: MemoryValue = Field(ge=0)
    allocation_sites_tracked: int = Field(ge=0)
    unit: Unit


class ObjectTracking(BaseModel):
    model_config = ConfigDict(frozen=True)

    total_allocated_objects: ObjectCount = Field(ge=0)
    object_growth_since_baseline: ObjectCount


class SnapshotMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)

    created_at: datetime
    timestamp_iso8601: str
    label: str | None = None
    async_task_name: str | None = None


class Snapshot(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    metadata: SnapshotMetadata
    heap_memory: HeapMemoryInfo
    process_memory: ProcessMemoryInfo
    gc_statistics: GCStatistics
    object_tracking: ObjectTracking
    tracemalloc_snapshot: tracemalloc.Snapshot | None = None


class AllocationInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

    size: MemoryValue = Field(ge=0)
    count: int = Field(ge=0)
    filename: str = "<unknown>"
    lineno: int = Field(default=0, ge=0)
    trace: str | None = None
    unit: Unit


class AllocationDifference(BaseModel):
    model_config = ConfigDict(frozen=True)

    filename: str = "<unknown>"
    lineno: int = Field(default=0, ge=0)
    size_diff: MemoryValue
    count_diff: int
    size_before: MemoryValue = Field(ge=0)
    size_after: MemoryValue = Field(ge=0)
    unit: Unit


class CircularReference(BaseModel):
    model_config = ConfigDict(frozen=True)

    object_type: str
    referrer_types: list[str]


class MemoryGrowth(BaseModel):
    model_config = ConfigDict(frozen=True)

    heap_growth: MemoryValue
    rss_growth: MemoryValue
    object_count_growth: ObjectCount
    unit: Unit


class LeakReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    has_leaks: bool
    circular_references: list[CircularReference] = Field(default_factory=list)
    top_growing_types: dict[str, int] = Field(default_factory=dict)
    memory_growth: MemoryGrowth | None = None
    uncollectable_count: int = Field(ge=0)
    native_leak_suspected: bool = False


class ProcessInfoCollector:
    def get_process_info(self, convert_fn: Callable[[int], MemoryValue], unit: Unit) -> ProcessMemoryInfo:
        try:
            process = psutil.Process(os.getpid())
            mem_info = process.memory_info()
            mem_percent = process.memory_percent()
            available = psutil.virtual_memory().available
            return ProcessMemoryInfo(
                resident_set_size=convert_fn(mem_info.rss),
                virtual_memory_size=convert_fn(mem_info.vms),
                memory_usage_percent=mem_percent,
                system_available=convert_fn(available),
                unit=unit,
            )
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
            raise RuntimeError(f"Failed to get process info: {e}") from e


class GCStatsCollector:
    def __init__(self) -> None:
        self._original_gc_debug = gc.get_debug()
        self._gc_debug_enabled = False

    def enable_debug_mode(self) -> None:
        """Enable DEBUG_SAVEALL to populate gc.garbage with uncollectable objects."""
        if not self._gc_debug_enabled:
            gc.set_debug(gc.DEBUG_SAVEALL)
            self._gc_debug_enabled = True

    def restore_debug_mode(self) -> None:
        """Restore original GC debug flags."""
        if self._gc_debug_enabled:
            gc.set_debug(self._original_gc_debug)
            self._gc_debug_enabled = False

    def get_gc_info(self, enable_gc: bool) -> GCStatistics:
        gc_counts = gc.get_count()
        uncollectable = len(gc.garbage)
        collected = gc.collect() if enable_gc else 0
        return GCStatistics(
            generation0_collections=gc_counts[0],
            generation1_collections=gc_counts[1],
            generation2_collections=gc_counts[2],
            objects_collected=collected,
            uncollectable_objects=uncollectable,
        )


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
        config: ProfilerConfig | None = None,
        memory_unit: Unit = "mb",
        decimal_places: int = 2,
        track_objects: bool = True,
        enable_gc_before_snapshot: bool = False,
        baseline_snapshot: bool = True,
        max_snapshots: int = 100,
    ) -> None:
        if config is None:
            config = ProfilerConfig(
                memory_unit=memory_unit,
                decimal_places=decimal_places,
                track_objects=track_objects,
                enable_gc_before_snapshot=enable_gc_before_snapshot,
                baseline_snapshot=baseline_snapshot,
                max_snapshots=max_snapshots,
            )

        self._config = config
        self._tracemalloc_manager = TracemallocManager()
        self._process_info = ProcessInfoCollector()
        self._gc_info = GCStatsCollector()
        self._lock = threading.RLock()

        self._is_running = False
        self._baseline_snapshot: Snapshot | None = None
        self._current_snapshot: Snapshot | None = None
        self._first_runtime_snapshot: Snapshot | None = None  # First snapshot after profiling starts
        self._snapshots: deque[Snapshot] = deque(maxlen=config.max_snapshots)
        self._baseline_object_types: dict[str, int] = {}  # Track object types at baseline

        # Note: baseline_snapshot will be created when profiling starts, not here
        # This avoids the start/stop gap issue

    def _start_profiling(self) -> None:
        with self._lock:
            if self._is_running:
                return

            self._tracemalloc_manager.start()
            self._gc_info.enable_debug_mode()
            self._is_running = True

            # Create baseline snapshot if configured
            if self._config.baseline_snapshot and self._baseline_snapshot is None:
                self._baseline_snapshot = self._take_snapshot(label="_baseline")
                # Track object types at baseline for growth comparison
                if self._config.track_objects:
                    self._baseline_object_types = self._get_object_type_counts()

    def _stop_profiling(self) -> None:
        with self._lock:
            if not self._is_running:
                return

            self._tracemalloc_manager.stop()
            self._gc_info.restore_debug_mode()
            self._is_running = False

    def _get_object_count(self) -> int:
        if not self._config.track_objects:
            return 0
        return len(gc.get_objects())

    def _get_object_type_counts(self, sample_size: int | None = None) -> dict[str, int]:
        """Get counts of object types. Returns all types if sample_size is None."""
        if not self._config.track_objects:
            return {}

        obj_types: dict[str, int] = {}
        all_objects = gc.get_objects()

        # If sample_size specified, use it; otherwise count all objects
        limit = min(len(all_objects), sample_size) if sample_size else len(all_objects)

        for obj in all_objects[:limit]:
            type_name = type(obj).__name__
            obj_types[type_name] = obj_types.get(type_name, 0) + 1

        return obj_types

    def _convert_bytes(self, value_bytes: int) -> MemoryValue:
        match self._config.memory_unit:
            case "bytes":
                return value_bytes
            case "kb":
                return round(value_bytes / BYTES_PER_KB, self._config.decimal_places)
            case "mb":
                return round(value_bytes / BYTES_PER_MB, self._config.decimal_places)

    def _take_snapshot(self, label: str | None = None) -> Snapshot:
        async_task_name: str | None = None
        try:
            if task := asyncio.current_task():
                async_task_name = task.get_name()
        except RuntimeError:
            pass

        now = datetime.now(UTC)
        iso_timestamp = now.isoformat()

        current_heap_bytes, peak_heap_bytes = self._tracemalloc_manager.get_traced_memory()
        tracemalloc_snap = self._tracemalloc_manager.take_snapshot()

        process_memory = self._process_info.get_process_info(self._convert_bytes, self._config.memory_unit)
        gc_statistics = self._gc_info.get_gc_info(self._config.enable_gc_before_snapshot)
        total_objects = self._get_object_count()
        object_growth = total_objects - (
            self._baseline_snapshot.object_tracking.total_allocated_objects if self._baseline_snapshot else 0
        )

        metadata = SnapshotMetadata(
            created_at=now,
            timestamp_iso8601=iso_timestamp,
            label=label,
            async_task_name=async_task_name,
        )

        heap_memory = HeapMemoryInfo(
            current=self._convert_bytes(current_heap_bytes),
            peak=self._convert_bytes(peak_heap_bytes),
            allocation_sites_tracked=len(tracemalloc_snap.statistics("lineno")),
            unit=self._config.memory_unit,
        )

        object_tracking = ObjectTracking(
            total_allocated_objects=total_objects,
            object_growth_since_baseline=object_growth,
        )

        return Snapshot(
            metadata=metadata,
            heap_memory=heap_memory,
            process_memory=process_memory,
            gc_statistics=gc_statistics,
            object_tracking=object_tracking,
            tracemalloc_snapshot=tracemalloc_snap,
        )

    def snapshot(self, label: str | None = None) -> Snapshot:
        with self._lock:
            if not self._is_running:
                raise RuntimeError("Profiler is not running. Use as context manager or call start().")

            # Check for duplicate labels
            if label is not None:
                existing_labels = {snap.metadata.label for snap in self._snapshots if snap.metadata.label is not None}
                if label in existing_labels:
                    import warnings

                    warnings.warn(
                        f"Snapshot with label '{label}' already exists and will be overwritten in comparisons.",
                        UserWarning,
                        stacklevel=2,
                    )

            snap = self._take_snapshot(label=label)
            self._snapshots.append(snap)
            self._current_snapshot = snap

            # Track first runtime snapshot for growth calculations
            if self._first_runtime_snapshot is None and label != "_baseline":
                self._first_runtime_snapshot = snap

            return snap

    def detect_leaks(self, threshold_bytes: int = BYTES_PER_MB, *, force_gc: bool = True) -> LeakReport:
        with self._lock:
            if force_gc:
                gc.collect()

            threshold = self._convert_bytes(threshold_bytes)
            circular_refs: list[CircularReference] = []
            native_leak_suspected = False
            memory_growth: MemoryGrowth | None = None

            # Check gc.garbage for circular references (now works with DEBUG_SAVEALL enabled)
            if gc.garbage:
                for obj in gc.garbage[: self._config.circular_ref_sample_limit]:
                    referrers = gc.get_referrers(obj)
                    if referrers:
                        circular_refs.append(
                            CircularReference(
                                object_type=type(obj).__name__,
                                referrer_types=[
                                    type(r).__name__ for r in referrers[: self._config.circular_ref_referrer_limit]
                                ],
                            )
                        )

            # Calculate memory growth using first runtime snapshot (not deque[0] which may have rotated)
            first_snap = self._first_runtime_snapshot or self._baseline_snapshot
            last_snap = self._current_snapshot

            if first_snap and last_snap and first_snap != last_snap:
                heap_growth = last_snap.heap_memory.current - first_snap.heap_memory.current
                rss_growth = last_snap.process_memory.resident_set_size - first_snap.process_memory.resident_set_size
                object_count_growth = (
                    last_snap.object_tracking.total_allocated_objects
                    - first_snap.object_tracking.total_allocated_objects
                )

                memory_growth = MemoryGrowth(
                    heap_growth=heap_growth,
                    rss_growth=rss_growth,
                    object_count_growth=object_count_growth,
                    unit=self._config.memory_unit,
                )

                # Detect native leaks: RSS growing much faster than heap
                if rss_growth > heap_growth + threshold:
                    native_leak_suspected = True

            # Calculate ACTUAL growth in object types by comparing against baseline
            top_growing_types: dict[str, int] = {}
            if self._config.track_objects and self._baseline_object_types:
                current_types = self._get_object_type_counts()

                # Calculate growth = current - baseline
                type_growth: dict[str, int] = {}
                for type_name, current_count in current_types.items():
                    baseline_count = self._baseline_object_types.get(type_name, 0)
                    growth = current_count - baseline_count
                    if growth > 0:  # Only include types that grew
                        type_growth[type_name] = growth

                # Get top 10 growing types
                top_growing_types = dict(sorted(type_growth.items(), key=lambda x: x[1], reverse=True)[:10])

            # Determine if leaks detected
            has_leaks = bool(
                gc.garbage
                or circular_refs
                or native_leak_suspected
                or (
                    memory_growth
                    and (
                        memory_growth.heap_growth > threshold
                        or memory_growth.object_count_growth > self._config.object_growth_threshold
                    )
                )
            )

            return LeakReport(
                has_leaks=has_leaks,
                circular_references=circular_refs,
                top_growing_types=top_growing_types,
                memory_growth=memory_growth,
                uncollectable_count=len(gc.garbage),
                native_leak_suspected=native_leak_suspected,
            )

    def get_top_allocations(
        self,
        limit: int = 10,
        key_type: KeyType = "lineno",
        snapshot: tracemalloc.Snapshot | None = None,
    ) -> list[AllocationInfo]:
        with self._lock:
            if not self._tracemalloc_manager.is_tracing() and snapshot is None:
                raise RuntimeError(
                    "tracemalloc is not active. Use the profiler as a context manager or start profiling first."
                )

            snap = snapshot or self._tracemalloc_manager.take_snapshot()
            results: list[AllocationInfo] = []

            for stat in snap.statistics(key_type)[:limit]:
                filename = "<unknown>"
                lineno = 0
                trace: str | None = None

                if stat.traceback:
                    frame = stat.traceback[0]
                    filename = frame.filename
                    lineno = frame.lineno
                    if key_type == "traceback":
                        trace = "\n".join([f"  File {f.filename}:{f.lineno}" for f in stat.traceback])

                results.append(
                    AllocationInfo(
                        size=self._convert_bytes(stat.size),
                        count=stat.count,
                        filename=filename,
                        lineno=lineno,
                        trace=trace,
                        unit=self._config.memory_unit,
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
        with self._lock:
            # Build snapshot lookup including baseline
            snapshots: dict[str, Snapshot] = {
                snap.metadata.label: snap for snap in self._snapshots if snap.metadata.label is not None
            }

            # Add baseline snapshot if it exists
            if self._baseline_snapshot and self._baseline_snapshot.metadata.label:
                snapshots[self._baseline_snapshot.metadata.label] = self._baseline_snapshot

            before_snap = snapshots.get(before_label)
            if before_snap is None:
                available = list(snapshots.keys())
                raise ValueError(
                    f"Snapshot with label '{before_label}' not found. Available labels: {available}"
                )

            after_snap = snapshots.get(after_label)
            if after_snap is None:
                available = list(snapshots.keys())
                raise ValueError(
                    f"Snapshot with label '{after_label}' not found. Available labels: {available}"
                )

            if before_snap.tracemalloc_snapshot is None:
                raise ValueError(f"Snapshot '{before_label}' was taken when tracemalloc was not running.")

            if after_snap.tracemalloc_snapshot is None:
                raise ValueError(f"Snapshot '{after_label}' was taken when tracemalloc was not running.")

            results: list[AllocationDifference] = []
            for stat_diff in after_snap.tracemalloc_snapshot.compare_to(before_snap.tracemalloc_snapshot, key_type)[
                :limit
            ]:
                filename = "<unknown>"
                lineno = 0

                if stat_diff.traceback:
                    frame = stat_diff.traceback[0]
                    filename = frame.filename
                    lineno = frame.lineno

                results.append(
                    AllocationDifference(
                        filename=filename,
                        lineno=lineno,
                        size_diff=self._convert_bytes(stat_diff.size_diff),
                        count_diff=stat_diff.count_diff,
                        size_before=self._convert_bytes(stat_diff.size - stat_diff.size_diff),
                        size_after=self._convert_bytes(stat_diff.size),
                        unit=self._config.memory_unit,
                    )
                )
            return results

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


@contextmanager
def track_memory(
    *,
    memory_unit: Unit = "mb",
    decimal_places: int = 2,
    track_objects: bool = True,
    enable_gc: bool = False,
    baseline_snapshot: bool = False,
    max_snapshots: int = 100,
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
