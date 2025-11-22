#!/usr/bin/env python3
"""
Demo: Ultra-Aggressive Memory Trimming in Action

Shows all the techniques combined:
1. Generational GC (gen0, gen1, gen2)
2. Cache clearing (type cache, import cache)
3. OS-level trimming (malloc_trim on Linux)
4. Adaptive interval background thread
5. Memory threshold triggering
"""

import logging
import time

from profilist.aggressive_memory import (
    AggressiveMemoryTrimmer,
    aggressive_trim,
    detect_memory_leaks,
    fork_optimized_section,
    performance_critical_section,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
)

logger = logging.getLogger(__name__)


def simulate_memory_intensive_work():
    """Simulate memory-intensive work."""
    logger.info("Starting memory-intensive work...")

    # Create large data structures
    data = []
    for i in range(10):
        chunk = [x * x for x in range(100_000)]
        data.append(chunk)
        time.sleep(0.1)

    logger.info(f"Created {len(data)} chunks of data")

    # Delete some
    del data[:5]

    return len(data)


def demo_one_shot_trim():
    """Demo: One-shot aggressive memory trim."""
    print("\n" + "=" * 80)
    print("DEMO 1: One-Shot Aggressive Trim")
    print("=" * 80)

    logger.info("Creating garbage...")
    for _ in range(5):
        _ = [x * x for x in range(100_000)]

    logger.info("Performing aggressive trim...")
    result = aggressive_trim()

    print(f"\n📊 Results:")
    print(f"   Before:      {result['before_mb']:.2f} MB")
    print(f"   After:       {result['after_mb']:.2f} MB")
    print(f"   Reclaimed:   {result['reclaimed_mb']:.2f} MB")
    print(f"   Duration:    {result['duration_ms']:.1f} ms")
    print(f"   GC Gen0:     {result['python']['gen0_collected']} objects")
    print(f"   GC Gen1:     {result['python']['gen1_collected']} objects")
    print(f"   GC Gen2:     {result['python']['gen2_collected']} objects")
    print(f"   Type cache:  {'✓' if result['python']['type_cache_cleared'] else '✗'}")
    print(f"   Import cache: {'✓' if result['python']['import_cache_cleared'] else '✗'}")
    print(f"   OS method:   {result['os'].get('method', 'none')}")
    print(f"   OS success:  {'✓' if result['os'].get('success') else '✗'}")


def demo_background_adaptive():
    """Demo: Background trimming with adaptive intervals."""
    print("\n" + "=" * 80)
    print("DEMO 2: Background Adaptive Trimming")
    print("=" * 80)

    trimmer = AggressiveMemoryTrimmer(
        aggressive=True,
        adaptive_interval=True,
        memory_threshold_mb=50.0,  # Low threshold for demo
    )

    logger.info("Starting background trimming (adaptive intervals)...")
    trimmer.start_background_trimming(interval=2.0)

    try:
        # Simulate workload
        for i in range(3):
            logger.info(f"Work iteration {i + 1}/3")
            simulate_memory_intensive_work()
            time.sleep(1)

    finally:
        trimmer.stop_background_trimming()

    stats = trimmer.get_statistics()
    print(f"\n📊 Background Trimming Statistics:")
    print(f"   Total trims:      {stats['trim_count']}")
    print(f"   Total reclaimed:  {stats['total_reclaimed_bytes'] / (1024 * 1024):.2f} MB")
    print(f"   Avg reclaimed:    {stats['avg_reclaimed_mb']:.2f} MB per trim")
    print(f"   Total GC objects: {stats['total_gc_collected']}")
    print(f"   Avg GC objects:   {stats['avg_gc_collected']:.0f} per trim")
    print(f"   OS success rate:  {stats['os_success_rate'] * 100:.1f}%")


def demo_context_manager():
    """Demo: Context manager with auto-trim on entry/exit."""
    print("\n" + "=" * 80)
    print("DEMO 3: Context Manager (auto-trim on entry/exit)")
    print("=" * 80)

    logger.info("Using context manager...")
    with AggressiveMemoryTrimmer(aggressive=True) as trimmer:
        logger.info("Inside context - work starting")
        simulate_memory_intensive_work()
        logger.info("Inside context - work completed")

    logger.info("Context exited - automatic cleanup performed")


def demo_performance_critical():
    """Demo: GC-free performance-critical section."""
    print("\n" + "=" * 80)
    print("DEMO 4: Performance-Critical Section (GC Disabled)")
    print("=" * 80)

    logger.info("Entering performance-critical section...")

    with performance_critical_section():
        logger.info("GC is disabled - no interruptions")

        # Simulate critical computation
        start = time.time()
        result = sum(x * x for x in range(5_000_000))
        duration = time.time() - start

        logger.info(f"Computation completed in {duration * 1000:.1f}ms (result: {result})")

    logger.info("Exited critical section - GC re-enabled and collected")


def demo_fork_optimization():
    """Demo: Fork optimization with gc.freeze()."""
    print("\n" + "=" * 80)
    print("DEMO 5: Fork Optimization (gc.freeze for multiprocessing)")
    print("=" * 80)

    logger.info("Preparing for multiprocessing.fork()...")

    with fork_optimized_section() as frozen_count:
        logger.info(f"{frozen_count} objects frozen to reduce copy-on-write")

        # In real code, you would fork here:
        # with multiprocessing.Pool() as pool:
        #     results = pool.map(worker, tasks)

        logger.info("Fork would happen here with minimal COW overhead")

    logger.info("Objects unfrozen after fork")


def demo_leak_detection():
    """Demo: Memory leak detection."""
    print("\n" + "=" * 80)
    print("DEMO 6: Memory Leak Detection")
    print("=" * 80)

    logger.info("Running leak detection...")
    leak_info = detect_memory_leaks()

    print(f"\n📊 Leak Detection Results:")
    print(f"   Uncollectable objects: {leak_info['uncollectable_count']}")

    if leak_info['circular_references']:
        print(f"\n   Circular references found:")
        for ref in leak_info['circular_references']:
            print(f"      • {ref['type']} (id: {ref['id']})")
            print(f"        Referrers: {ref['referrer_count']}")
            print(f"        Types: {', '.join(ref['referrer_types'])}")
    else:
        print(f"   ✓ No circular references detected")


def main():
    """Run all demos."""
    print("=" * 80)
    print("ULTRA-AGGRESSIVE MEMORY TRIMMING - COMPREHENSIVE DEMO")
    print("=" * 80)

    demos = [
        demo_one_shot_trim,
        demo_background_adaptive,
        demo_context_manager,
        demo_performance_critical,
        demo_fork_optimization,
        demo_leak_detection,
    ]

    for demo in demos:
        try:
            demo()
        except Exception as e:
            logger.error(f"Demo failed: {e}", exc_info=True)

    print("\n" + "=" * 80)
    print("ALL DEMOS COMPLETED!")
    print("=" * 80)

    print("\n💡 Key Takeaways:")
    print("   1. Generational GC (gen0/1/2) > single gc.collect()")
    print("   2. Cache clearing (type/import) reduces memory footprint")
    print("   3. OS-level trimming (malloc_trim) returns memory to OS")
    print("   4. Adaptive intervals balance overhead vs. memory pressure")
    print("   5. Background threading keeps trimming non-intrusive")
    print("   6. GC disable/enable optimizes performance-critical sections")
    print("   7. gc.freeze() reduces multiprocessing copy-on-write")


if __name__ == "__main__":
    main()
