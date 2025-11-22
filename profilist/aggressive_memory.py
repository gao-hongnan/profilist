"""
Ultra-aggressive memory management for long-running processes.

Combines ALL memory trimming techniques:
- Generational garbage collection
- Python cache clearing (type cache, import cache)
- OS-level trimming (Linux malloc_trim, macOS zones, Windows working set)
- Adaptive interval threading
- Thread-safe statistics tracking
- Memory threshold triggering
"""

from __future__ import annotations

import ctypes
import gc
import importlib
import logging
import sys
import threading
import time
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any, Literal

import psutil

logger = logging.getLogger(__name__)


class AggressiveMemoryTrimmer:
    """
    Ultra-aggressive cross-platform memory trimming.

    Features:
    - Generational GC (gen0, gen1, gen2)
    - Cache clearing (sys._clear_type_cache, importlib.invalidate_caches)
    - OS-level trimming (malloc_trim/zones/working set)
    - Adaptive intervals based on memory pressure
    - Thread-safe statistics tracking
    - Memory threshold triggering
    """

    def __init__(
        self,
        aggressive: bool = True,
        memory_threshold_mb: float = 100.0,
        adaptive_interval: bool = False,
    ) -> None:
        self.aggressive = aggressive
        self.memory_threshold_mb = memory_threshold_mb
        self.adaptive_interval = adaptive_interval

        # Platform detection
        self.platform = sys.platform
        self._is_linux = self.platform.startswith("linux")
        self._is_macos = self.platform == "darwin"
        self._is_windows = self.platform == "win32"

        # Threading
        self._trim_thread: threading.Thread | None = None
        self._stop_event: threading.Event | None = None
        self._stats_lock = threading.Lock()

        # Statistics
        self._stats = {
            'trim_count': 0,
            'total_reclaimed_bytes': 0,
            'total_gc_collected': 0,
            'os_trim_successes': 0,
            'os_trim_failures': 0,
            'last_trim_time': None,
        }

        # Platform-specific setup
        self._setup_platform_specific()

    def _setup_platform_specific(self) -> None:
        """Setup platform-specific memory trimming functions."""
        if self._is_linux:
            self._setup_linux()
        elif self._is_macos:
            self._setup_macos()
        elif self._is_windows:
            self._setup_windows()
        else:
            logger.warning(f"Platform {self.platform} not fully supported")

    def _setup_linux(self) -> None:
        """Setup malloc_trim for Linux."""
        try:
            self._libc = ctypes.CDLL(None)
            self._malloc_trim = self._libc.malloc_trim
            self._malloc_trim.argtypes = [ctypes.c_size_t]
            self._malloc_trim.restype = ctypes.c_int
            logger.info("✓ Linux malloc_trim initialized")
        except Exception as e:
            logger.error(f"✗ Failed to setup malloc_trim: {e}")
            self._malloc_trim = None

    def _setup_macos(self) -> None:
        """Setup malloc_zone_pressure_relief for macOS."""
        try:
            self._libc = ctypes.CDLL("/usr/lib/system/libsystem_malloc.dylib")

            # malloc_default_zone
            self._malloc_default_zone = self._libc.malloc_default_zone
            self._malloc_default_zone.argtypes = []
            self._malloc_default_zone.restype = ctypes.c_void_p

            # malloc_zone_pressure_relief
            self._malloc_zone_pressure_relief = self._libc.malloc_zone_pressure_relief
            self._malloc_zone_pressure_relief.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
            self._malloc_zone_pressure_relief.restype = ctypes.c_size_t

            logger.info("✓ macOS malloc zones initialized")
        except Exception as e:
            logger.error(f"✗ Failed to setup macOS malloc zones: {e}")
            self._malloc_zone_pressure_relief = None

    def _setup_windows(self) -> None:
        """Setup SetProcessWorkingSetSize for Windows."""
        try:
            from ctypes import wintypes

            self._kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)
            self._kernel32.GetCurrentProcess.restype = wintypes.HANDLE
            self._kernel32.SetProcessWorkingSetSize.argtypes = (
                wintypes.HANDLE,
                ctypes.c_size_t,
                ctypes.c_size_t
            )

            logger.info("✓ Windows working set trimming initialized")
        except Exception as e:
            logger.error(f"✗ Failed to setup Windows trimming: {e}")
            self._kernel32 = None

    def _get_memory_usage(self) -> int:
        """Get current RSS memory usage in bytes."""
        try:
            return int(psutil.Process().memory_info().rss)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return 0

    def _aggressive_python_cleanup(self) -> dict[str, int]:
        """
        Ultra-aggressive Python-level cleanup.

        Returns dict with:
        - gen0_collected: Objects collected from generation 0
        - gen1_collected: Objects collected from generation 1
        - gen2_collected: Objects collected from generation 2
        - total_collected: Total objects collected
        - type_cache_cleared: Whether type cache was cleared
        - import_cache_cleared: Whether import cache was cleared
        """
        stats = {}

        # 1. Clear Python caches
        sys._clear_type_cache()
        stats['type_cache_cleared'] = True

        importlib.invalidate_caches()
        stats['import_cache_cleared'] = True

        # 2. Generational garbage collection (MORE effective than single gc.collect())
        stats['gen0_collected'] = gc.collect(0)  # Young objects
        stats['gen1_collected'] = gc.collect(1)  # Mid-age objects
        stats['gen2_collected'] = gc.collect(2)  # Old objects
        stats['total_collected'] = (
            stats['gen0_collected'] + stats['gen1_collected'] + stats['gen2_collected']
        )

        # 3. Check for memory leaks
        stats['uncollectable_objects'] = len(gc.garbage)
        if gc.garbage:
            logger.warning(f"⚠️  Uncollectable objects detected: {len(gc.garbage)}")

        return stats

    def _standard_python_cleanup(self) -> dict[str, int]:
        """Standard Python cleanup (less aggressive)."""
        collected = gc.collect()
        return {
            'total_collected': collected,
            'gen0_collected': 0,
            'gen1_collected': 0,
            'gen2_collected': 0,
            'type_cache_cleared': False,
            'import_cache_cleared': False,
            'uncollectable_objects': 0,
        }

    def _trim_os_memory(self, pad: int = 0) -> dict[str, Any]:
        """Platform-specific OS-level memory trimming."""
        if self._is_linux:
            return self._trim_linux(pad)
        elif self._is_macos:
            return self._trim_macos()
        elif self._is_windows:
            return self._trim_windows()
        else:
            return {'success': False, 'method': 'unsupported'}

    def _trim_linux(self, pad: int) -> dict[str, Any]:
        """Linux malloc_trim."""
        if not hasattr(self, '_malloc_trim') or self._malloc_trim is None:
            return {'success': False, 'method': 'malloc_trim_unavailable'}

        try:
            result = self._malloc_trim(pad)
            return {
                'success': result == 1,
                'method': 'malloc_trim',
                'platform': 'linux',
            }
        except Exception as e:
            logger.error(f"malloc_trim failed: {e}")
            return {'success': False, 'method': 'malloc_trim', 'error': str(e)}

    def _trim_macos(self) -> dict[str, Any]:
        """macOS malloc_zone_pressure_relief."""
        if not hasattr(self, '_malloc_zone_pressure_relief'):
            return {'success': False, 'method': 'zones_unavailable'}

        try:
            zone = self._malloc_default_zone()
            if zone:
                bytes_released = self._malloc_zone_pressure_relief(zone, 0)
                return {
                    'success': True,
                    'method': 'malloc_zone_pressure_relief',
                    'platform': 'macos',
                    'bytes_released': bytes_released,
                }
        except Exception as e:
            logger.error(f"malloc_zone_pressure_relief failed: {e}")

        return {'success': False, 'method': 'malloc_zone_pressure_relief'}

    def _trim_windows(self) -> dict[str, Any]:
        """Windows working set trimming (VERY aggressive)."""
        if not hasattr(self, '_kernel32') or self._kernel32 is None:
            return {'success': False, 'method': 'workingset_unavailable'}

        try:
            hProcess = self._kernel32.GetCurrentProcess()
            # -1, -1 = trim to minimum (swaps process out of RAM!)
            self._kernel32.SetProcessWorkingSetSize(hProcess, -1, -1)
            return {
                'success': True,
                'method': 'SetProcessWorkingSetSize',
                'platform': 'windows',
            }
        except Exception as e:
            logger.error(f"SetProcessWorkingSetSize failed: {e}")
            return {'success': False, 'method': 'SetProcessWorkingSetSize', 'error': str(e)}

    def trim_memory(self, pad: int = 0) -> dict[str, Any]:
        """
        Perform aggressive memory trimming.

        Args:
            pad: Memory padding for malloc_trim (Linux only)

        Returns:
            Dictionary with detailed trimming results
        """
        start_time = time.time()
        before_bytes = self._get_memory_usage()

        # Python-level cleanup
        if self.aggressive:
            python_stats = self._aggressive_python_cleanup()
        else:
            python_stats = self._standard_python_cleanup()

        # OS-level cleanup
        os_stats = self._trim_os_memory(pad)

        # Measure results
        after_bytes = self._get_memory_usage()
        reclaimed_bytes = max(0, before_bytes - after_bytes)
        duration_ms = (time.time() - start_time) * 1000

        # Update statistics (thread-safe)
        with self._stats_lock:
            self._stats['trim_count'] += 1
            self._stats['total_reclaimed_bytes'] += reclaimed_bytes
            self._stats['total_gc_collected'] += python_stats['total_collected']
            self._stats['last_trim_time'] = time.time()

            if os_stats.get('success'):
                self._stats['os_trim_successes'] += 1
            else:
                self._stats['os_trim_failures'] += 1

        result = {
            'before_mb': before_bytes / (1024 * 1024),
            'after_mb': after_bytes / (1024 * 1024),
            'reclaimed_mb': reclaimed_bytes / (1024 * 1024),
            'reclaimed_bytes': reclaimed_bytes,
            'duration_ms': duration_ms,
            'python': python_stats,
            'os': os_stats,
            'aggressive': self.aggressive,
        }

        logger.info(
            f"🗑️  Memory trim: {reclaimed_bytes / (1024 * 1024):.2f} MB reclaimed "
            f"({before_bytes / (1024 * 1024):.1f} → {after_bytes / (1024 * 1024):.1f} MB) "
            f"in {duration_ms:.1f}ms | "
            f"GC: {python_stats['total_collected']} objects | "
            f"OS: {os_stats.get('method', 'none')} {'✓' if os_stats.get('success') else '✗'}"
        )

        return result

    def start_background_trimming(
        self,
        interval: float = 30.0,
        pad: int = 0,
    ) -> None:
        """
        Start background thread for periodic memory trimming.

        Args:
            interval: Base interval in seconds (adaptive if enabled)
            pad: Memory padding for malloc_trim
        """
        if self._trim_thread and self._trim_thread.is_alive():
            logger.warning("Background trimming already running")
            return

        self._stop_event = threading.Event()
        self._trim_thread = threading.Thread(
            target=self._background_trimmer,
            args=(interval, pad),
            daemon=True,
            name="AggressiveMemoryTrimmer"
        )
        self._trim_thread.start()

        mode = "adaptive" if self.adaptive_interval else f"fixed {interval}s"
        logger.info(f"🚀 Started aggressive background memory trimming ({mode})")

    def stop_background_trimming(self) -> None:
        """Stop background memory trimming thread."""
        if not self._trim_thread or not self._trim_thread.is_alive():
            return

        if self._stop_event:
            self._stop_event.set()

        self._trim_thread.join(timeout=5.0)
        logger.info("🛑 Stopped background memory trimming")

    def _calculate_adaptive_interval(self, base_interval: float) -> float:
        """Calculate adaptive interval based on memory pressure."""
        if not self.adaptive_interval:
            return base_interval

        current_mb = self._get_memory_usage() / (1024 * 1024)
        threshold = self.memory_threshold_mb

        # Adaptive intervals based on memory pressure
        if current_mb > threshold * 2:
            return max(2.0, base_interval / 4)  # VERY aggressive
        elif current_mb > threshold * 1.5:
            return max(5.0, base_interval / 2)  # Aggressive
        elif current_mb > threshold:
            return base_interval  # Normal
        else:
            return min(60.0, base_interval * 2)  # Conservative

    def _background_trimmer(self, base_interval: float, pad: int) -> None:
        """Background thread function with adaptive intervals."""
        while not (self._stop_event and self._stop_event.is_set()):
            current_mb = self._get_memory_usage() / (1024 * 1024)

            # Trim if: (1) aggressive mode OR (2) above threshold
            should_trim = self.aggressive or current_mb > self.memory_threshold_mb

            if should_trim:
                self.trim_memory(pad)

            # Calculate next interval
            interval = self._calculate_adaptive_interval(base_interval)

            # Interruptible wait (GIL-friendly)
            if self._stop_event:
                self._stop_event.wait(interval)

    def get_statistics(self) -> dict[str, Any]:
        """Get thread-safe trimming statistics."""
        with self._stats_lock:
            stats = self._stats.copy()

        # Add computed stats
        if stats['trim_count'] > 0:
            stats['avg_reclaimed_mb'] = (
                stats['total_reclaimed_bytes'] / (1024 * 1024) / stats['trim_count']
            )
            stats['avg_gc_collected'] = stats['total_gc_collected'] / stats['trim_count']
        else:
            stats['avg_reclaimed_mb'] = 0.0
            stats['avg_gc_collected'] = 0

        stats['os_success_rate'] = (
            stats['os_trim_successes'] / stats['trim_count']
            if stats['trim_count'] > 0
            else 0.0
        )

        return stats

    def __enter__(self) -> AggressiveMemoryTrimmer:
        """Context manager entry - trim on start."""
        self.trim_memory()
        return self

    def __exit__(self, *exc_info: Any) -> None:
        """Context manager exit - stop thread and final trim."""
        self.stop_background_trimming()
        self.trim_memory()


# ============================================================================
# Advanced Patterns
# ============================================================================

@contextmanager
def performance_critical_section():
    """
    Context manager for GC-free critical sections.

    Disables GC during execution, then forces collection after.
    Use for tight loops or real-time processing.
    """
    gc_was_enabled = gc.isenabled()
    try:
        gc.disable()
        logger.debug("⏸️  GC disabled for performance-critical section")
        yield
    finally:
        if gc_was_enabled:
            gc.enable()
            collected = gc.collect()
            logger.debug(f"▶️  GC re-enabled, collected {collected} objects")


@contextmanager
def fork_optimized_section():
    """
    Context manager for fork-optimized setup.

    Freezes objects before multiprocessing.fork() to reduce copy-on-write.
    Automatically unfreezes after.
    """
    gc.collect()
    gc.freeze()
    frozen_count = gc.get_freeze_count()
    logger.info(f"❄️  Froze {frozen_count} objects for fork optimization")

    try:
        yield frozen_count
    finally:
        gc.unfreeze()
        logger.info("☀️  Unfroze objects after fork")


def detect_memory_leaks() -> dict[str, Any]:
    """
    Enable leak detection and analyze uncollectable objects.

    Returns:
        Dictionary with leak information
    """
    gc.set_debug(gc.DEBUG_LEAK)
    gc.collect()

    leak_info = {
        'uncollectable_count': len(gc.garbage),
        'uncollectable_objects': [],
        'circular_references': [],
    }

    # Analyze uncollectable objects
    for obj in gc.garbage[:10]:  # Limit to first 10
        referrers = gc.get_referrers(obj)
        leak_info['circular_references'].append({
            'type': type(obj).__name__,
            'id': id(obj),
            'referrer_count': len(referrers),
            'referrer_types': [type(r).__name__ for r in referrers[:5]],
        })

    gc.set_debug(0)  # Disable debug

    if leak_info['uncollectable_count'] > 0:
        logger.warning(
            f"🔍 Memory leak detection: {leak_info['uncollectable_count']} uncollectable objects"
        )

    return leak_info


# ============================================================================
# Convenience Functions
# ============================================================================

def aggressive_trim(pad: int = 0) -> dict[str, Any]:
    """
    One-shot aggressive memory trim.

    Combines all techniques: generational GC, cache clearing, OS trimming.
    """
    trimmer = AggressiveMemoryTrimmer(aggressive=True)
    return trimmer.trim_memory(pad)


def standard_trim(pad: int = 0) -> dict[str, Any]:
    """One-shot standard memory trim (less aggressive)."""
    trimmer = AggressiveMemoryTrimmer(aggressive=False)
    return trimmer.trim_memory(pad)


# ============================================================================
# Example Usage
# ============================================================================

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )

    print("=" * 80)
    print("ULTRA-AGGRESSIVE MEMORY TRIMMING DEMO")
    print("=" * 80)

    # Example 1: One-shot aggressive trim
    print("\n1. One-shot aggressive trim:")
    result = aggressive_trim()
    print(f"   Reclaimed: {result['reclaimed_mb']:.2f} MB")
    print(f"   GC collected: {result['python']['total_collected']} objects")

    # Example 2: Background trimming with adaptive intervals
    print("\n2. Background adaptive trimming (5 seconds):")
    trimmer = AggressiveMemoryTrimmer(
        aggressive=True,
        adaptive_interval=True,
        memory_threshold_mb=100.0,
    )

    trimmer.start_background_trimming(interval=2.0)
    time.sleep(5)
    trimmer.stop_background_trimming()

    stats = trimmer.get_statistics()
    print(f"   Total trims: {stats['trim_count']}")
    print(f"   Total reclaimed: {stats['total_reclaimed_bytes'] / (1024 * 1024):.2f} MB")
    print(f"   Avg reclaimed: {stats['avg_reclaimed_mb']:.2f} MB per trim")

    # Example 3: Performance-critical section
    print("\n3. Performance-critical section (GC disabled):")
    with performance_critical_section():
        # Simulate critical code
        data = [i * i for i in range(1000000)]
    print("   ✓ Completed without GC interruptions")

    # Example 4: Leak detection
    print("\n4. Memory leak detection:")
    leak_info = detect_memory_leaks()
    print(f"   Uncollectable objects: {leak_info['uncollectable_count']}")

    print("\n" + "=" * 80)
    print("DEMO COMPLETED")
    print("=" * 80)
