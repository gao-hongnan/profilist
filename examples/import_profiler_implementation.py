"""
Import Profiler Implementation for Profilist
============================================

This demonstrates how to extend your profilist library to profile imports.
Shows memory and timing for lazy vs eager imports.
"""

from __future__ import annotations

import importlib
import sys
import time
import tracemalloc
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ImportProfile:
    """Profile information for a single import."""

    module_name: str
    import_time_seconds: float
    memory_mb: float
    already_loaded: bool
    submodules_count: int
    file_path: str | None = None


@dataclass
class ImportReport:
    """Complete report of import profiling session."""

    profiles: list[ImportProfile] = field(default_factory=list)
    total_time_seconds: float = 0.0
    total_memory_mb: float = 0.0

    def add_profile(self, profile: ImportProfile) -> None:
        """Add a profile to the report."""
        self.profiles.append(profile)
        if not profile.already_loaded:
            self.total_time_seconds += profile.import_time_seconds
            self.total_memory_mb += profile.memory_mb

    def summary(self) -> str:
        """Generate a summary report."""
        lines = [
            "=" * 80,
            "Import Profile Report",
            "=" * 80,
            f"\nTotal imports: {len(self.profiles)}",
            f"Total import time: {self.total_time_seconds:.3f} seconds",
            f"Total memory consumed: {self.total_memory_mb:.2f} MB",
            "\nDetailed Breakdown:",
            "-" * 80,
        ]

        # Sort by import time (slowest first)
        sorted_profiles = sorted(
            self.profiles,
            key=lambda p: p.import_time_seconds,
            reverse=True,
        )

        for profile in sorted_profiles:
            status = "CACHED" if profile.already_loaded else "LOADED"
            lines.append(
                f"{profile.module_name:30s} | "
                f"{profile.import_time_seconds:8.4f}s | "
                f"{profile.memory_mb:8.2f} MB | "
                f"{status:8s} | "
                f"{profile.submodules_count:3d} submodules"
            )

        lines.extend([
            "-" * 80,
            "\n💡 Optimization Tips:",
        ])

        # Find candidates for lazy loading
        slow_imports = [p for p in sorted_profiles if p.import_time_seconds > 0.1 and not p.already_loaded]
        if slow_imports:
            lines.append("\n🐌 Slow imports (>100ms) - consider lazy loading:")
            for profile in slow_imports[:5]:  # Top 5
                lines.append(f"   • {profile.module_name}: {profile.import_time_seconds:.3f}s")

        # Find memory hogs
        heavy_imports = [p for p in sorted_profiles if p.memory_mb > 10 and not p.already_loaded]
        if heavy_imports:
            lines.append("\n🐘 Memory-heavy imports (>10MB) - consider lazy loading:")
            for profile in heavy_imports[:5]:  # Top 5
                lines.append(f"   • {profile.module_name}: {profile.memory_mb:.2f} MB")

        lines.append("\n" + "=" * 80)
        return "\n".join(lines)


class ImportProfiler:
    """Profile import time and memory consumption."""

    def __init__(self) -> None:
        self.report = ImportReport()
        self._tracemalloc_started = False

    def _start_tracemalloc(self) -> None:
        """Start tracemalloc if not already started."""
        if not tracemalloc.is_tracing():
            tracemalloc.start()
            self._tracemalloc_started = True

    def _stop_tracemalloc(self) -> None:
        """Stop tracemalloc if we started it."""
        if self._tracemalloc_started and tracemalloc.is_tracing():
            tracemalloc.stop()
            self._tracemalloc_started = False

    def profile_import(self, module_name: str) -> ImportProfile:
        """
        Profile a single import.

        Args:
            module_name: Name of the module to import (e.g., 'pandas', 'numpy')

        Returns:
            ImportProfile with timing and memory information
        """
        # Check if already loaded
        already_loaded = module_name in sys.modules

        # Start memory tracking
        self._start_tracemalloc()
        mem_before, _ = tracemalloc.get_traced_memory()

        # Count modules before import
        modules_before = len(sys.modules)

        # Time the import
        start_time = time.perf_counter()

        try:
            module = importlib.import_module(module_name)
            import_time = time.perf_counter() - start_time
        except ImportError as e:
            import_time = time.perf_counter() - start_time
            raise ImportError(f"Failed to import {module_name}: {e}") from e

        # Memory after import
        mem_after, _ = tracemalloc.get_traced_memory()
        memory_consumed = (mem_after - mem_before) / 1024 / 1024  # Convert to MB

        # Count new modules loaded
        modules_after = len(sys.modules)
        submodules_count = modules_after - modules_before

        # Get file path
        file_path = getattr(module, "__file__", None)

        profile = ImportProfile(
            module_name=module_name,
            import_time_seconds=import_time,
            memory_mb=memory_consumed if not already_loaded else 0.0,
            already_loaded=already_loaded,
            submodules_count=submodules_count,
            file_path=file_path,
        )

        self.report.add_profile(profile)
        return profile

    @contextmanager
    def track_imports(self):
        """
        Context manager to track all imports in a code block.

        Usage:
            with profiler.track_imports():
                import pandas as pd
                import numpy as np
        """
        self._start_tracemalloc()
        modules_before = set(sys.modules.keys())

        yield

        modules_after = set(sys.modules.keys())
        new_modules = modules_after - modules_before

        print(f"\n📦 Detected {len(new_modules)} new modules loaded")
        for module_name in sorted(new_modules)[:10]:  # Show first 10
            print(f"   • {module_name}")

        self._stop_tracemalloc()

    def print_report(self) -> None:
        """Print the import profile report."""
        print(self.report.summary())


# ============================================================================
# Demonstration Examples
# ============================================================================


def demo_basic_import_profiling():
    """Demo: Profile individual imports."""
    print("\n" + "=" * 80)
    print("DEMO 1: Basic Import Profiling")
    print("=" * 80 + "\n")

    profiler = ImportProfiler()

    # Profile standard library (fast)
    print("Profiling standard library imports...")
    profiler.profile_import("json")
    profiler.profile_import("os")
    profiler.profile_import("sys")

    # Profile heavier libraries (uncomment if installed)
    # print("\nProfiling third-party libraries...")
    # profiler.profile_import("requests")
    # profiler.profile_import("pydantic")

    # Profile heavy scientific libraries (uncomment if installed)
    # print("\nProfiling scientific libraries...")
    # profiler.profile_import("pandas")
    # profiler.profile_import("numpy")

    profiler.print_report()


def demo_lazy_vs_eager_imports():
    """Demo: Compare lazy loading vs eager loading."""
    print("\n" + "=" * 80)
    print("DEMO 2: Lazy Loading vs Eager Loading Comparison")
    print("=" * 80 + "\n")

    # Scenario 1: Eager loading (traditional)
    print("📌 Scenario 1: Eager Loading (import at module level)")
    print("-" * 80)

    profiler_eager = ImportProfiler()

    tracemalloc.start()
    mem_before_eager, _ = tracemalloc.get_traced_memory()
    start_eager = time.perf_counter()

    # Simulate module-level imports
    profiler_eager.profile_import("json")
    profiler_eager.profile_import("csv")
    profiler_eager.profile_import("pathlib")
    # Uncomment to test with heavy libs:
    # profiler_eager.profile_import("pandas")

    eager_time = time.perf_counter() - start_eager
    mem_after_eager, _ = tracemalloc.get_traced_memory()
    eager_memory = (mem_after_eager - mem_before_eager) / 1024 / 1024

    print(f"\n✅ Eager loading completed:")
    print(f"   Startup time: {eager_time:.4f} seconds")
    print(f"   Memory consumed: {eager_memory:.2f} MB")
    print("   App is ready - all dependencies loaded")

    tracemalloc.stop()

    # Scenario 2: Lazy loading (import in function)
    print("\n📌 Scenario 2: Lazy Loading (import inside functions)")
    print("-" * 80)

    profiler_lazy = ImportProfiler()

    tracemalloc.start()
    mem_before_lazy, _ = tracemalloc.get_traced_memory()
    start_lazy = time.perf_counter()

    # Startup - no imports!
    def process_json(data):
        """Import json only when called."""
        import json  # Lazy import
        return json.dumps(data)

    def process_csv(data):
        """Import csv only when called."""
        import csv  # Lazy import
        # ... use csv ...
        return data

    # Simulate startup with lazy imports
    lazy_startup_time = time.perf_counter() - start_lazy
    mem_after_lazy, _ = tracemalloc.get_traced_memory()
    lazy_startup_memory = (mem_after_lazy - mem_before_lazy) / 1024 / 1024

    print(f"\n✅ Lazy loading completed:")
    print(f"   Startup time: {lazy_startup_time:.6f} seconds (nearly instant!)")
    print(f"   Memory consumed: {lazy_startup_memory:.2f} MB (minimal)")
    print("   App is ready - dependencies will load on-demand")

    # Now actually call the functions
    print("\n📞 Calling process_json() for the first time...")
    call_start = time.perf_counter()
    process_json({"test": "data"})
    first_call_time = time.perf_counter() - call_start
    print(f"   First call time: {first_call_time:.4f}s (includes import)")

    print("\n📞 Calling process_json() again...")
    call_start = time.perf_counter()
    process_json({"test": "data2"})
    second_call_time = time.perf_counter() - call_start
    print(f"   Second call time: {second_call_time:.6f}s (cached)")

    tracemalloc.stop()

    # Comparison
    print("\n" + "=" * 80)
    print("COMPARISON: Eager vs Lazy Loading")
    print("=" * 80)
    print(f"{'Metric':<30s} | {'Eager':<15s} | {'Lazy':<15s} | {'Winner':<10s}")
    print("-" * 80)

    startup_winner = "Lazy" if lazy_startup_time < eager_time else "Eager"
    print(
        f"{'Startup time':<30s} | "
        f"{eager_time:>14.4f}s | "
        f"{lazy_startup_time:>14.6f}s | "
        f"{startup_winner:<10s}"
    )

    memory_winner = "Lazy" if lazy_startup_memory < eager_memory else "Eager"
    print(
        f"{'Startup memory':<30s} | "
        f"{eager_memory:>14.2f} MB | "
        f"{lazy_startup_memory:>14.2f} MB | "
        f"{memory_winner:<10s}"
    )

    print("-" * 80)


def demo_with_profilist_integration():
    """Demo: Integrate with existing profilist Timer and MemoryProfiler."""
    print("\n" + "=" * 80)
    print("DEMO 3: Integration with Profilist Library")
    print("=" * 80 + "\n")

    try:
        from profilist.timer import Timer
        from profilist.profiler import MemoryProfiler
    except ImportError:
        print("⚠️  Profilist not in path - run from project root")
        return

    print("Using profilist.Timer to profile imports:\n")

    # Method 1: Using Timer
    with Timer("Import json"):
        import json

    with Timer("Import pathlib"):
        import pathlib

    # Method 2: Using MemoryProfiler
    print("\nUsing profilist.MemoryProfiler to track import memory:\n")

    with MemoryProfiler(memory_unit="mb") as profiler:
        profiler.snapshot("baseline")

        import collections
        profiler.snapshot("after_collections")

        import dataclasses
        profiler.snapshot("after_dataclasses")

        # Compare memory growth
        if len(profiler.all_snapshots) >= 3:
            baseline = profiler.all_snapshots[0].process_memory.rss_mb
            after_collections = profiler.all_snapshots[1].process_memory.rss_mb
            after_dataclasses = profiler.all_snapshots[2].process_memory.rss_mb

            print(f"Memory growth:")
            print(f"  Baseline:          {baseline:.2f} MB")
            print(f"  After collections: {after_collections:.2f} MB (+{after_collections - baseline:.2f} MB)")
            print(f"  After dataclasses: {after_dataclasses:.2f} MB (+{after_dataclasses - after_collections:.2f} MB)")


def demo_import_hook_advanced():
    """Demo: Advanced import hook to auto-profile all imports (educational)."""
    print("\n" + "=" * 80)
    print("DEMO 4: Advanced - Custom Import Hook (Educational)")
    print("=" * 80 + "\n")

    import builtins

    original_import = builtins.__import__
    import_stats: dict[str, float] = {}

    def profiling_import(name, *args, **kwargs):
        """Wrapper around __import__ to profile all imports."""
        start = time.perf_counter()
        module = original_import(name, *args, **kwargs)
        elapsed = time.perf_counter() - start

        if name not in import_stats:  # Only log first import
            import_stats[name] = elapsed
            if elapsed > 0.001:  # Only show imports >1ms
                print(f"   📦 Imported {name:30s} in {elapsed:.4f}s")

        return module

    # Monkey-patch __import__
    print("Installing import hook...\n")
    builtins.__import__ = profiling_import

    # Now any import will be profiled
    print("Importing modules (with auto-profiling):")
    import urllib
    import email
    import html

    # Restore original
    builtins.__import__ = original_import

    print(f"\n✅ Profiled {len(import_stats)} unique imports")
    print("\nTop 5 slowest imports:")
    for module, elapsed in sorted(import_stats.items(), key=lambda x: x[1], reverse=True)[:5]:
        print(f"   {module:30s}: {elapsed:.4f}s")

    print("\n⚠️  Note: This is educational. For production, use:")
    print("   python -X importtime -c 'import mymodule'")


# ============================================================================
# Run Demonstrations
# ============================================================================

if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    Import Profiling Demonstrations                          ║
║                                                                              ║
║  This script demonstrates how to profile Python imports using:              ║
║  1. Custom ImportProfiler class                                             ║
║  2. Lazy loading strategies                                                 ║
║  3. Integration with profilist.Timer and MemoryProfiler                     ║
║  4. Advanced import hooks                                                   ║
╚══════════════════════════════════════════════════════════════════════════════╝
    """)

    # Run demos
    demo_basic_import_profiling()
    demo_lazy_vs_eager_imports()
    demo_with_profilist_integration()
    demo_import_hook_advanced()

    print("\n" + "=" * 80)
    print("✅ All demonstrations completed!")
    print("=" * 80)
    print("""
Key Takeaways:
1. Use ImportProfiler to profile individual imports
2. Lazy loading reduces startup time and memory
3. profilist.Timer and MemoryProfiler work great for import profiling
4. Custom import hooks can auto-profile all imports (educational)

Next steps:
• Run: python examples/import_profiler_implementation.py
• Read: examples/lru_cache_explained.py
• Read: examples/import_profiling_guide.py
    """)
