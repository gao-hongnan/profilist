"""
Python Import Profiling & Lazy Loading Guide
=============================================

This guide shows:
1. How to profile import time and memory
2. How imports consume memory for the app's lifetime
3. Lazy loading strategies (NOT lru_cache!)
4. When to use each approach
"""

import sys
import time
import tracemalloc
from typing import TYPE_CHECKING

# ============================================================================
# Part 1: Understanding Import Memory Behavior
# ============================================================================

print("=" * 70)
print("PART 1: How Imports Consume Memory (Lifetime Demonstration)")
print("=" * 70)


def demonstrate_import_lifetime():
    """Show that imports persist in sys.modules forever."""

    print("\n1️⃣  Before import:")
    print(f"   'pandas' in sys.modules: {'pandas' in sys.modules}")

    print("\n2️⃣  Importing pandas...")
    start_time = time.perf_counter()
    tracemalloc.start()

    import pandas as pd  # First import is SLOW

    snapshot_after_import = tracemalloc.take_snapshot()
    import_time = time.perf_counter() - start_time
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    print(f"   Import time: {import_time:.3f} seconds")
    print(f"   Memory allocated: {current / 1024 / 1024:.2f} MB")
    print(f"   'pandas' in sys.modules: {'pandas' in sys.modules}")

    print("\n3️⃣  Trying to 'delete' pandas:")
    del pd
    print(f"   del pd executed")
    print(f"   'pandas' in sys.modules: {'pandas' in sys.modules}")  # Still True!

    print("\n4️⃣  Importing pandas again:")
    start_time = time.perf_counter()
    import pandas as pd  # Second import is INSTANT (from sys.modules)
    import_time = time.perf_counter() - start_time
    print(f"   Import time: {import_time:.6f} seconds (nearly instant!)")
    print(f"   'pandas' in sys.modules: {'pandas' in sys.modules}")

    print("\n💡 KEY INSIGHT:")
    print("   • First import: SLOW (loads from disk, parses, executes)")
    print("   • Subsequent imports: INSTANT (returns from sys.modules)")
    print("   • Memory stays allocated for ENTIRE app lifetime")
    print("   • Even 'del pandas' doesn't remove it from sys.modules")
    print("   • Only way to free memory: EXIT the Python process")


# Uncomment to run (requires pandas installed):
# demonstrate_import_lifetime()


# ============================================================================
# Part 2: Profiling Import Time and Memory
# ============================================================================

print("\n" + "=" * 70)
print("PART 2: How to Profile Imports with Your Profilist Library")
print("=" * 70)


def profile_imports_with_timer():
    """Use profilist.Timer to measure import time."""
    from profilist.timer import Timer

    print("\n📊 Method 1: Using Timer context manager")

    # Profile a heavy import
    with Timer("Import pandas", silent=False) as t:
        import pandas as pd  # type: ignore

    print(f"   Import completed in {t.elapsed_seconds:.4f} seconds")

    # Profile multiple imports
    print("\n📊 Method 2: Profile multiple imports")
    imports_to_test = [
        ("numpy", "import numpy as np"),
        ("requests", "import requests"),
        ("json", "import json"),  # Standard lib - fast
    ]

    for name, import_stmt in imports_to_test:
        with Timer(f"Import {name}", silent=True) as t:
            exec(import_stmt)
        print(f"   {name:12s}: {t.elapsed_seconds:.6f} seconds")


def profile_imports_with_memory():
    """Use profilist.MemoryProfiler to measure import memory."""
    from profilist.profiler import MemoryProfiler

    print("\n📊 Method 3: Using MemoryProfiler for import memory tracking")

    profiler = MemoryProfiler(memory_unit="mb", decimal_places=2)

    # Take baseline before import
    profiler.start()
    profiler.snapshot("baseline")

    # Import heavy library
    import pandas as pd  # type: ignore
    profiler.snapshot("after_pandas")

    # Import another library
    import numpy as np  # type: ignore
    profiler.snapshot("after_numpy")

    profiler.stop()

    # Compare memory growth
    print("\n   Memory growth analysis:")
    if profiler.baseline and profiler.latest:
        baseline_mem = profiler.baseline.process_memory.rss_mb
        pandas_mem = profiler.all_snapshots[1].process_memory.rss_mb
        numpy_mem = profiler.latest.process_memory.rss_mb

        print(f"   Baseline:      {baseline_mem:.2f} MB")
        print(f"   After pandas:  {pandas_mem:.2f} MB (+{pandas_mem - baseline_mem:.2f} MB)")
        print(f"   After numpy:   {numpy_mem:.2f} MB (+{numpy_mem - pandas_mem:.2f} MB)")


# Uncomment to run:
# profile_imports_with_timer()
# profile_imports_with_memory()


# ============================================================================
# Part 3: Lazy Loading Strategies (NOT lru_cache!)
# ============================================================================

print("\n" + "=" * 70)
print("PART 3: Lazy Loading Strategies for Imports")
print("=" * 70)

print("""
⚠️  CRITICAL: LRU Cache is NOT for lazy imports!

@lru_cache is for CACHING FUNCTION RESULTS, not for deferring imports.
Once a module is imported (even inside a cached function), it stays in
sys.modules FOREVER. Re-calling the function doesn't re-import.

Use these strategies instead:
""")


# ----------------------------------------------------------------------------
# Strategy 1: Function-Level Lazy Import (Most Common)
# ----------------------------------------------------------------------------

print("\n" + "-" * 70)
print("Strategy 1: Function-Level Lazy Import")
print("-" * 70)


def process_dataframe_lazy(data: list) -> None:
    """Import pandas ONLY when this function is called."""
    import pandas as pd  # Imported on first call only

    df = pd.DataFrame(data)
    print(f"   Processed DataFrame with {len(df)} rows")


def analyze_with_numpy_lazy(data: list) -> float:
    """Import numpy ONLY when this function is called."""
    import numpy as np  # Imported on first call only

    return np.mean(data)


print("""
✅ Benefits:
   • Import happens ONLY if function is called
   • Reduces startup time if function is rarely used
   • Good for optional features or CLI subcommands

❌ Drawbacks:
   • Small overhead on FIRST function call
   • Type checkers may complain (can fix with TYPE_CHECKING)
   • Module still stays in memory after first call

Example:
""")

print("   # pandas NOT imported yet")
print("   process_dataframe_lazy([1, 2, 3])  # <-- pandas imported HERE")
print("   process_dataframe_lazy([4, 5, 6])  # pandas already in sys.modules")


# ----------------------------------------------------------------------------
# Strategy 2: Conditional Import (Feature Flags)
# ----------------------------------------------------------------------------

print("\n" + "-" * 70)
print("Strategy 2: Conditional Import (Feature Flags)")
print("-" * 70)

USE_GPU = False  # Feature flag

if USE_GPU:
    import tensorflow as tf  # Only imported if GPU enabled
    print("   GPU mode enabled")
else:
    print("   CPU mode - tensorflow NOT imported")

print("""
✅ Benefits:
   • Import only when configuration requires it
   • Great for optional dependencies
   • Clear dependency management

❌ Drawbacks:
   • Requires runtime configuration
   • Cannot change after module load

Example use cases:
   • Development vs production dependencies
   • GPU vs CPU modes
   • Debug vs release modes
""")


# ----------------------------------------------------------------------------
# Strategy 3: TYPE_CHECKING (Type Hints Only, No Runtime Import)
# ----------------------------------------------------------------------------

print("\n" + "-" * 70)
print("Strategy 3: TYPE_CHECKING (Type Hints Only)")
print("-" * 70)

if TYPE_CHECKING:
    import pandas as pd  # NOT imported at runtime!
    import numpy as np


def process_data(data: list) -> "pd.DataFrame":  # String annotation
    """Type checker sees pd.DataFrame, but pandas NOT imported at runtime."""
    import pandas as pd  # Actual import happens here
    return pd.DataFrame(data)


print("""
✅ Benefits:
   • Type hints work in IDE/mypy
   • ZERO runtime cost (not imported at runtime)
   • Clean type annotations

❌ Drawbacks:
   • Must use string annotations: "pd.DataFrame"
   • Still need to import inside function for runtime use

Example:
""")

print("   from typing import TYPE_CHECKING")
print("   if TYPE_CHECKING:")
print("       import pandas as pd  # Editor sees this")
print("")
print("   def foo() -> 'pd.DataFrame':  # Type hint (no runtime cost)")
print("       import pandas as pd  # Runtime import")
print("       return pd.DataFrame()")


# ----------------------------------------------------------------------------
# Strategy 4: Module-Level Lazy Import with __getattr__ (Advanced)
# ----------------------------------------------------------------------------

print("\n" + "-" * 70)
print("Strategy 4: Module __getattr__ (Advanced - Python 3.7+)")
print("-" * 70)

print("""
This is the MOST advanced lazy import strategy used by libraries like
NumPy, Pandas, and SciPy for internal modules.

Example module structure:

   # mymodule/__init__.py
   def __getattr__(name):
       if name == "heavy_submodule":
           from . import heavy_submodule  # Imported on first access
           return heavy_submodule
       raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

   # Usage:
   import mymodule
   mymodule.heavy_submodule  # <-- Import happens HERE, not at 'import mymodule'

✅ Benefits:
   • Transparent to users (looks like normal import)
   • Import happens on first attribute access
   • Used by major scientific libraries

❌ Drawbacks:
   • Complex to implement correctly
   • Can confuse static analysis tools
   • Only works for submodules, not for external packages
""")


# ----------------------------------------------------------------------------
# Strategy 5: Singleton Pattern with Lazy Initialization
# ----------------------------------------------------------------------------

print("\n" + "-" * 70)
print("Strategy 5: Singleton Pattern with Lazy Init")
print("-" * 70)


class DataProcessor:
    """Singleton that lazily imports pandas on first use."""

    _instance = None
    _pandas = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @property
    def pd(self):
        """Lazy import pandas on first access."""
        if self._pandas is None:
            print("   🔄 Importing pandas (first access)...")
            import pandas
            self._pandas = pandas
        return self._pandas

    def process(self, data: list):
        """Process data with pandas (imports on first call)."""
        df = self.pd.DataFrame(data)
        return df


# Demo
print("\nDemo:")
print("   processor = DataProcessor()  # pandas NOT imported yet")
processor = DataProcessor()

print("   processor.process([1, 2, 3])  # pandas imported HERE")
# Uncomment to run:
# processor.process([1, 2, 3])

print("""
✅ Benefits:
   • Clean API
   • Import happens on first method call
   • Good for libraries with optional heavy dependencies

❌ Drawbacks:
   • More complex code
   • Singleton pattern may not be needed
""")


# ============================================================================
# Part 4: Comparison Table - When to Use What?
# ============================================================================

print("\n" + "=" * 70)
print("PART 4: Decision Matrix - Lazy Import vs LRU Cache")
print("=" * 70)

comparison = """
┌─────────────────────────┬─────────────────────────┬────────────────────────┐
│       Use Case          │       Solution          │       Reason           │
├─────────────────────────┼─────────────────────────┼────────────────────────┤
│ Defer heavy import      │ Function-level import   │ Import on first call   │
│ until needed            │ OR __getattr__          │ only                   │
├─────────────────────────┼─────────────────────────┼────────────────────────┤
│ Optional dependency     │ Conditional import      │ Based on config/flag   │
│ based on config         │ (if USE_FEATURE:)       │                        │
├─────────────────────────┼─────────────────────────┼────────────────────────┤
│ Type hints without      │ TYPE_CHECKING import    │ Zero runtime cost      │
│ runtime cost            │                         │                        │
├─────────────────────────┼─────────────────────────┼────────────────────────┤
│ Avoid recomputing       │ @lru_cache             │ Cache RESULTS, not     │
│ expensive function      │                         │ imports                │
├─────────────────────────┼─────────────────────────┼────────────────────────┤
│ Database query results  │ @lru_cache(maxsize=128)│ Cache query results    │
├─────────────────────────┼─────────────────────────┼────────────────────────┤
│ API response caching    │ @lru_cache(maxsize=256)│ Cache API responses    │
├─────────────────────────┼─────────────────────────┼────────────────────────┤
│ Pure math function      │ @lru_cache(maxsize=None)│ Memoization            │
│ (fibonacci, factorial)  │                         │                        │
└─────────────────────────┴─────────────────────────┴────────────────────────┘

🎯 KEY PRINCIPLE:
   • Lazy imports = DEFER LOADING
   • LRU cache = AVOID RECOMPUTATION
   • They solve DIFFERENT problems!

❌ NEVER do this:

   @lru_cache(maxsize=1)
   def get_pandas():
       import pandas as pd  # Still loads pandas fully into sys.modules!
       return pd

   # This doesn't help! Pandas is loaded on first call and stays forever.

✅ DO this instead:

   def process_data(data):
       import pandas as pd  # Lazy import
       return pd.DataFrame(data)

   @lru_cache(maxsize=128)
   def expensive_calculation(x):  # Cache the RESULT
       import pandas as pd  # (could also be at module level)
       # ... expensive computation ...
       return result
"""

print(comparison)


# ============================================================================
# Part 5: Real-World Example - FastAPI App
# ============================================================================

print("\n" + "=" * 70)
print("PART 5: Real-World Example - FastAPI with Lazy Imports")
print("=" * 70)

print("""
Scenario: FastAPI app with multiple endpoints, some rarely used

# main.py (BAD - imports everything at startup)
import pandas as pd  # ❌ Loaded even if /analytics never called
import tensorflow as tf  # ❌ Loaded even if /predict never called
import requests  # ❌ Loaded even if /webhook never called
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Hello"}

@app.get("/analytics")  # Rarely used
def analytics():
    df = pd.DataFrame(...)
    return df.to_dict()

@app.get("/predict")  # Only for premium users
def predict():
    model = tf.keras.models.load_model(...)
    return {"prediction": ...}


# main.py (GOOD - lazy imports)
from fastapi import FastAPI  # Only essential imports at top
from profilist.timer import Timer

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Hello"}  # Fast startup!

@app.get("/analytics")
def analytics():
    # Import pandas ONLY when this endpoint is hit
    with Timer("Import pandas"):
        import pandas as pd

    df = pd.DataFrame(...)
    return df.to_dict()

@app.get("/predict")
def predict():
    # Import tensorflow ONLY when this endpoint is hit
    with Timer("Import tensorflow"):
        import tensorflow as tf

    model = tf.keras.models.load_model(...)
    return {"prediction": ...}


Results:
   • App startup: 0.5s → 0.05s (10x faster!)
   • Memory at startup: 500MB → 50MB (10x less!)
   • First /analytics call: +50ms import time (acceptable)
   • Subsequent calls: instant (tf already in sys.modules)
""")


# ============================================================================
# Part 6: Profiling Import Impact
# ============================================================================

print("\n" + "=" * 70)
print("PART 6: How to Profile Your App's Import Time")
print("=" * 70)

print("""
Method 1: Python's built-in import profiler

   $ python -X importtime -c "import pandas"

   Output:
   import time:      1234 |   1234 | pandas
   import time:       456 |    456 |   pandas._libs
   import time:       789 |    789 |   numpy

Method 2: Using your Timer in __init__.py

   # myapp/__init__.py
   from profilist.timer import Timer

   with Timer("Import heavy dependencies"):
       import pandas as pd
       import tensorflow as tf
       import numpy as np

Method 3: tuna (visual import profiler)

   $ pip install tuna
   $ python -X importtime -c "import myapp" 2> import.log
   $ tuna import.log

   Opens interactive browser visualization!

Method 4: Manual timing with MemoryProfiler

   from profilist.profiler import MemoryProfiler
   from profilist.timer import Timer

   profiler = MemoryProfiler()
   profiler.start()
   profiler.snapshot("baseline")

   with Timer("Import pandas"):
       import pandas as pd
   profiler.snapshot("after_pandas")

   with Timer("Import tensorflow"):
       import tensorflow as tf
   profiler.snapshot("after_tensorflow")

   profiler.stop()

   # Analyze snapshots
   for snap in profiler.all_snapshots:
       print(f"{snap.metadata.label}: {snap.process_memory.rss_mb:.2f} MB")
""")


# ============================================================================
# Summary
# ============================================================================

print("\n" + "=" * 70)
print("FINAL SUMMARY")
print("=" * 70)

print("""
📌 Key Takeaways:

1. Import Behavior:
   ✅ Imports happen ONCE per process lifetime
   ✅ Modules stored in sys.modules forever
   ✅ Even 'del module' doesn't free memory
   ✅ Only way to free: restart process

2. LRU Cache vs Lazy Imports:
   ✅ LRU cache = memoize FUNCTION RESULTS
   ✅ Lazy imports = DEFER MODULE LOADING
   ✅ They solve DIFFERENT problems
   ❌ DO NOT use lru_cache for imports

3. When to Use Lazy Imports:
   ✅ Heavy dependencies (pandas, tensorflow, etc.)
   ✅ Optional features
   ✅ CLI subcommands
   ✅ Rarely-used endpoints
   ❌ NOT for frequently-used core dependencies

4. LRU Cache maxsize Guide:
   • maxsize=1: Last call only (tight loops with same args)
   • maxsize=2-10: Toggle between few configs
   • maxsize=128: Good default for most apps
   • maxsize=None: Pure math functions ONLY (dangerous!)

5. Best Practices:
   ✅ Profile your imports (python -X importtime)
   ✅ Use lazy imports for optional/heavy deps
   ✅ Use lru_cache for expensive computations
   ✅ Monitor cache hit rate with cache_info()
   ✅ Use TYPE_CHECKING for type-only imports

6. Your Profilist Library:
   ✅ Use Timer to measure import time
   ✅ Use MemoryProfiler to track import memory
   ✅ Take snapshots before/after imports
   ✅ Analyze memory growth with compare_allocations()
""")

print("\n✅ Run this script to learn about imports and caching!")
print("   Uncomment the demo functions to see them in action.\n")
