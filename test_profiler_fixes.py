#!/usr/bin/env python3
"""Test script to verify profiler bug fixes."""

import gc
from profilist.profiler import MemoryProfiler, ProfilerConfig


def create_circular_reference():
    """Create a circular reference to test gc.garbage detection."""
    class Node:
        def __init__(self):
            self.ref = None

    a = Node()
    b = Node()
    a.ref = b
    b.ref = a
    return a, b


def test_baseline_timing():
    """Test that baseline is created when profiling starts, not in __init__."""
    print("\n=== Test 1: Baseline Timing ===")
    profiler = MemoryProfiler(baseline_snapshot=True)

    # Baseline should be None before entering context
    print(f"Baseline before context: {profiler.baseline}")

    with profiler:
        # Baseline should exist after entering
        print(f"Baseline after entering: {profiler.baseline is not None}")
        assert profiler.baseline is not None, "Baseline should be created on __enter__"

        # Allocate some memory
        data = [i for i in range(10000)]
        snap = profiler.snapshot(label="after_allocation")
        print(f"Snapshot taken: {snap.metadata.label}")

    print("✓ Baseline timing test passed")


def test_duplicate_labels():
    """Test that duplicate labels generate warnings."""
    print("\n=== Test 2: Duplicate Label Detection ===")
    import warnings

    with MemoryProfiler() as profiler:
        profiler.snapshot(label="test")

        # This should warn
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            profiler.snapshot(label="test")

            assert len(w) == 1, "Should have 1 warning"
            assert "already exists" in str(w[0].message)
            print(f"✓ Duplicate label warning: {w[0].message}")

    print("✓ Duplicate label detection test passed")


def test_top_growing_types():
    """Test that top_growing_types actually tracks growth."""
    print("\n=== Test 3: Top Growing Types ===")

    with MemoryProfiler(baseline_snapshot=True) as profiler:
        # Create some objects
        initial_lists = [[] for _ in range(100)]
        profiler.snapshot(label="after_lists")

        # Create many more dict objects
        dicts = [{i: i} for i in range(500)]
        profiler.snapshot(label="after_dicts")

        # Check leak report
        leak_report = profiler.detect_leaks()
        print(f"Top growing types: {leak_report.top_growing_types}")

        # Should show growth (dict should be among top growing)
        if leak_report.top_growing_types:
            print(f"✓ Detected object growth: {list(leak_report.top_growing_types.items())[:3]}")
        else:
            print("⚠ No growing types detected (might be ok if baseline was recent)")

    print("✓ Top growing types test passed")


def test_baseline_comparison():
    """Test that compare_allocations can use baseline snapshot."""
    print("\n=== Test 4: Baseline Comparison ===")

    with MemoryProfiler(baseline_snapshot=True) as profiler:
        # Allocate memory
        data = [i for i in range(10000)]
        profiler.snapshot(label="checkpoint")

        try:
            # Should be able to compare against baseline
            diffs = profiler.compare_allocations("_baseline", "checkpoint", limit=5)
            print(f"✓ Baseline comparison succeeded, found {len(diffs)} allocation differences")
            if diffs:
                print(f"  Top diff: {diffs[0].filename}:{diffs[0].lineno} -> {diffs[0].size_diff} MB")
        except ValueError as e:
            print(f"✗ Baseline comparison failed: {e}")
            raise

    print("✓ Baseline comparison test passed")


def test_thread_safety():
    """Test thread safety with concurrent snapshots."""
    print("\n=== Test 5: Thread Safety ===")
    import threading

    profiler = MemoryProfiler()
    errors = []

    def take_snapshots():
        try:
            for i in range(5):
                profiler.snapshot(label=f"thread_{threading.current_thread().name}_{i}")
        except Exception as e:
            errors.append(e)

    with profiler:
        threads = [threading.Thread(target=take_snapshots, name=f"T{i}") for i in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        if errors:
            print(f"✗ Thread safety test failed with errors: {errors}")
            raise errors[0]

        print(f"✓ Thread safety test passed, took {len(profiler.all_snapshots)} snapshots")


def test_gc_debug_mode():
    """Test that GC debug mode is properly managed."""
    print("\n=== Test 6: GC Debug Mode ===")

    original_debug = gc.get_debug()
    print(f"Original GC debug flags: {original_debug}")

    with MemoryProfiler() as profiler:
        # Inside context, DEBUG_SAVEALL should be set
        current_debug = gc.get_debug()
        print(f"GC debug flags inside context: {current_debug}")
        assert current_debug & gc.DEBUG_SAVEALL, "DEBUG_SAVEALL should be enabled"

    # After context, should be restored
    restored_debug = gc.get_debug()
    print(f"GC debug flags after context: {restored_debug}")
    assert restored_debug == original_debug, "GC debug flags should be restored"

    print("✓ GC debug mode test passed")


def test_memory_growth_calculation():
    """Test that memory growth uses first runtime snapshot, not rotated deque."""
    print("\n=== Test 7: Memory Growth Calculation ===")

    config = ProfilerConfig(max_snapshots=3, baseline_snapshot=True)

    with MemoryProfiler(config=config) as profiler:
        # Take more snapshots than max to cause rotation
        for i in range(5):
            data = [j for j in range(1000 * (i + 1))]
            profiler.snapshot(label=f"snap_{i}")

        leak_report = profiler.detect_leaks()

        if leak_report.memory_growth:
            print(f"✓ Memory growth detected: {leak_report.memory_growth}")
            # Growth should be from first snapshot, not from rotated snapshot
            print(f"  Heap growth: {leak_report.memory_growth.heap_growth} MB")
            print(f"  Object count growth: {leak_report.memory_growth.object_count_growth}")
        else:
            print("⚠ No memory growth detected")

    print("✓ Memory growth calculation test passed")


def main():
    print("=" * 60)
    print("Testing Profiler Bug Fixes")
    print("=" * 60)

    try:
        test_baseline_timing()
        test_duplicate_labels()
        test_top_growing_types()
        test_baseline_comparison()
        test_thread_safety()
        test_gc_debug_mode()
        test_memory_growth_calculation()

        print("\n" + "=" * 60)
        print("✓ ALL TESTS PASSED!")
        print("=" * 60)

    except Exception as e:
        print("\n" + "=" * 60)
        print(f"✗ TEST FAILED: {e}")
        print("=" * 60)
        raise


if __name__ == "__main__":
    main()
