# Import Profiling & LRU Cache Guide

This directory contains comprehensive examples and explanations for profiling Python imports, understanding LRU cache, and implementing lazy loading strategies.

## 📚 Files Overview

### 1. `lru_cache_explained.py`
**Comprehensive LRU Cache Tutorial**

Covers:
- ✅ How `@lru_cache` works with intuitive examples
- ✅ What `maxsize=1`, `maxsize=2`, `maxsize=128`, `maxsize=None` mean
- ✅ Real-world use cases (API caching, database queries)
- ✅ Cache management (`cache_info()`, `cache_clear()`)
- ✅ **Why LRU cache is NOT for imports** (critical!)

**Run it:**
```bash
python examples/lru_cache_explained.py
```

**Key sections:**
- Example 1: maxsize=1 (parking lot with 1 space)
- Example 2: maxsize=2 (parking lot with 2 spaces)
- Example 3: maxsize=None (unlimited - dangerous!)
- Example 4: Real-world API caching
- Example 5: Cache management
- **Summary table** for choosing maxsize

### 2. `import_profiling_guide.py`
**Complete Guide to Import Profiling & Lazy Loading**

Covers:
- ✅ How imports consume memory for app lifetime
- ✅ Why `del pandas` doesn't free memory
- ✅ How to profile imports with `profilist.Timer` and `MemoryProfiler`
- ✅ 5 lazy loading strategies with examples
- ✅ **LRU Cache vs Lazy Imports decision matrix**
- ✅ Real-world FastAPI example

**Run it:**
```bash
python examples/import_profiling_guide.py
```

**Key strategies covered:**
1. Function-level lazy import
2. Conditional import (feature flags)
3. `TYPE_CHECKING` (type hints only)
4. Module `__getattr__` (advanced)
5. Singleton pattern with lazy init

### 3. `import_profiler_implementation.py`
**Practical Implementation & Demonstrations**

Provides:
- ✅ `ImportProfiler` class (ready to use!)
- ✅ `ImportProfile` and `ImportReport` dataclasses
- ✅ Integration with your existing `profilist.Timer` and `MemoryProfiler`
- ✅ Demo: Lazy vs Eager loading comparison
- ✅ Demo: Custom import hooks (educational)

**Run it:**
```bash
python examples/import_profiler_implementation.py
```

**What you get:**
- Detailed import timing and memory reports
- Automatic detection of slow imports (>100ms)
- Automatic detection of memory-heavy imports (>10MB)
- Recommendations for lazy loading candidates

---

## 🎯 Quick Reference

### When to Use LRU Cache vs Lazy Imports

| Scenario | Solution | Reason |
|----------|----------|--------|
| Avoid recomputing expensive function | `@lru_cache` | Cache **results**, not imports |
| Defer heavy import until needed | Function-level import | Import on first call only |
| Optional dependency based on config | Conditional import | Based on feature flag |
| Type hints without runtime cost | `TYPE_CHECKING` import | Zero runtime cost |
| Database query results | `@lru_cache(maxsize=128)` | Cache query results |
| API response caching | `@lru_cache(maxsize=256)` | Cache API responses |
| Pure math function | `@lru_cache(maxsize=None)` | Memoization (be careful!) |

### LRU Cache maxsize Guidelines

```python
@lru_cache(maxsize=1)    # Only last result cached (tight loops)
@lru_cache(maxsize=2)    # Toggle between 2 configs
@lru_cache(maxsize=128)  # Good default for most apps ⭐
@lru_cache(maxsize=256)  # High-traffic web apps
@lru_cache(maxsize=None) # Unlimited (ONLY for bounded math functions)
```

**Rule of thumb:**
- Start with **128** (good balance)
- Monitor hit rate with `cache_info()`
- Increase if hit rate < 70%
- Decrease if memory usage is high

---

## 🔍 How Your Codebase Currently Works

### Current Import Patterns

**File: `/home/user/profilist/profilist/profiler.py`**

✅ **One lazy import found:**
```python
def get_top_allocations(self, ...):
    if not self._tracemalloc_manager.is_tracing() and snapshot is None:
        import warnings  # Lazy import - only on error path
        warnings.warn(...)
```

**Why this is good:**
- `warnings` module only loaded when error occurs
- Reduces startup time for normal profiling workflows
- Minimal overhead

### Current Caching Patterns

**File: `/home/user/profilist/profilist/profiler.py`**

✅ **Bounded deque for snapshot storage:**
```python
from collections import deque

self._snapshots: deque[Snapshot] = deque(maxlen=config.max_snapshots)
```

**Configuration:**
- Default: `max_snapshots = 100`
- Similar to LRU cache with fixed size
- Automatically evicts oldest snapshots

❌ **No `@lru_cache` usage found**
- None in `timer.py`
- None in `profiler.py`
- `functools` only used for `@functools.wraps` (preserves function metadata)

---

## 📊 Import Memory Behavior

### The Pandas Problem

```python
# At module level - loaded IMMEDIATELY
import pandas as pd  # ~50-100 MB loaded NOW

def my_function():
    # pandas is ALREADY in memory, even if this never runs!
    df = pd.DataFrame([1, 2, 3])
```

**Key facts:**
1. ✅ Once imported → stays in `sys.modules` **forever**
2. ✅ Memory is **NOT freed** until process exits
3. ✅ Even `del pandas` doesn't help
4. ✅ Subsequent imports are **instant** (from cache)
5. ✅ All submodules are loaded (pandas loads numpy, pytz, dateutil, etc.)

**Verify yourself:**
```python
import sys
import pandas as pd

print(sys.modules['pandas'])  # Will stay here forever
del pd
print(sys.modules['pandas'])  # Still there!
```

---

## 🚀 Lazy Loading Strategies

### Strategy 1: Function-Level Import (Most Common)

```python
def process_data(data: list):
    import pandas as pd  # Imported ONLY when function is called
    return pd.DataFrame(data)
```

**Benefits:**
- ✅ Import happens only if function is called
- ✅ Reduces startup time
- ✅ Good for optional features, CLI subcommands, rarely-used endpoints

**Drawbacks:**
- ❌ Small overhead on first call (~50-100ms for pandas)
- ❌ Module still stays in memory after first call

### Strategy 2: Conditional Import

```python
USE_GPU = False  # Feature flag

if USE_GPU:
    import tensorflow as tf
else:
    # tensorflow NOT imported
    pass
```

**Benefits:**
- ✅ Import only when configuration requires it
- ✅ Great for optional dependencies
- ✅ Clear dependency management

**Use cases:**
- Development vs production dependencies
- GPU vs CPU modes
- Debug vs release modes

### Strategy 3: TYPE_CHECKING (Type Hints Only)

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd  # NOT imported at runtime!

def process_data(data: list) -> "pd.DataFrame":  # String annotation
    import pandas as pd  # Actual import happens here
    return pd.DataFrame(data)
```

**Benefits:**
- ✅ Type hints work in IDE/mypy
- ✅ **ZERO runtime cost** (not imported at runtime)
- ✅ Clean type annotations

**Drawbacks:**
- ❌ Must use string annotations: `"pd.DataFrame"`
- ❌ Still need to import inside function for runtime

### Strategy 4: Module __getattr__ (Advanced)

```python
# mymodule/__init__.py
def __getattr__(name):
    if name == "heavy_submodule":
        from . import heavy_submodule  # Imported on first access
        return heavy_submodule
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

# Usage:
import mymodule
mymodule.heavy_submodule  # Import happens HERE
```

**Benefits:**
- ✅ Transparent to users
- ✅ Import happens on first attribute access
- ✅ Used by NumPy, Pandas, SciPy internally

**Drawbacks:**
- ❌ Complex to implement correctly
- ❌ Can confuse static analysis tools
- ❌ Only works for submodules, not external packages

---

## 🛠️ How to Profile Imports

### Method 1: Python Built-in

```bash
python -X importtime -c "import pandas"
```

Output shows import time for each module.

### Method 2: With Your Profilist Library

```python
from profilist.timer import Timer
from profilist.profiler import MemoryProfiler

# Profile import time
with Timer("Import pandas"):
    import pandas as pd

# Profile import memory
with MemoryProfiler() as profiler:
    profiler.snapshot("baseline")

    import pandas as pd
    profiler.snapshot("after_pandas")

    import numpy as np
    profiler.snapshot("after_numpy")

# Analyze
for snap in profiler.all_snapshots:
    print(f"{snap.metadata.label}: {snap.process_memory.rss_mb:.2f} MB")
```

### Method 3: Using ImportProfiler (from examples)

```python
from examples.import_profiler_implementation import ImportProfiler

profiler = ImportProfiler()
profiler.profile_import("pandas")
profiler.profile_import("numpy")
profiler.print_report()
```

**Automatically shows:**
- Import time per module
- Memory consumed
- Submodules loaded
- Recommendations for lazy loading

### Method 4: Visual Profiler (tuna)

```bash
pip install tuna
python -X importtime -c "import myapp" 2> import.log
tuna import.log
```

Opens interactive browser visualization!

---

## ⚠️ Common Mistakes

### ❌ WRONG: Using LRU Cache for Imports

```python
@lru_cache(maxsize=1)
def get_pandas():
    import pandas as pd  # Still loads pandas fully into sys.modules!
    return pd
```

**Why this doesn't work:**
- pandas is loaded on **first call** and stays in `sys.modules` **forever**
- Second call returns cached reference, but pandas is already loaded
- **No memory savings**, only slight reference lookup speedup (negligible)

### ✅ CORRECT: Lazy Import

```python
def process_data(data):
    import pandas as pd  # Lazy import
    return pd.DataFrame(data)
```

### ✅ CORRECT: LRU Cache for Results

```python
@lru_cache(maxsize=128)
def expensive_calculation(x: int) -> int:
    # Cache the RESULT, not the import
    import pandas as pd  # (could also be at module level)
    # ... expensive computation ...
    return result
```

---

## 🎓 Real-World Example: FastAPI

### ❌ Bad: Eager Imports

```python
# main.py
import pandas as pd  # ❌ Loaded even if /analytics never called
import tensorflow as tf  # ❌ Loaded even if /predict never called
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Hello"}

@app.get("/analytics")  # Rarely used
def analytics():
    df = pd.DataFrame(...)
    return df.to_dict()
```

**Results:**
- App startup: **500ms**
- Memory at startup: **500MB**
- First request: instant

### ✅ Good: Lazy Imports

```python
# main.py
from fastapi import FastAPI
from profilist.timer import Timer

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Hello"}

@app.get("/analytics")
def analytics():
    with Timer("Import pandas"):
        import pandas as pd

    df = pd.DataFrame(...)
    return df.to_dict()
```

**Results:**
- App startup: **50ms** (10x faster!)
- Memory at startup: **50MB** (10x less!)
- First `/analytics` call: **+50ms** for import (acceptable)
- Subsequent calls: instant (pandas already in sys.modules)

---

## 📖 Summary

### Key Principles

1. **Import Behavior**
   - Imports happen **once** per process lifetime
   - Modules stored in `sys.modules` **forever**
   - Only way to free memory: **restart process**

2. **LRU Cache vs Lazy Imports**
   - LRU cache = **memoize function results**
   - Lazy imports = **defer module loading**
   - They solve **DIFFERENT problems**

3. **When to Use Lazy Imports**
   - ✅ Heavy dependencies (pandas, tensorflow)
   - ✅ Optional features
   - ✅ CLI subcommands
   - ✅ Rarely-used endpoints
   - ❌ NOT for frequently-used core dependencies

4. **When to Use LRU Cache**
   - ✅ Expensive computations
   - ✅ Database queries
   - ✅ API calls
   - ✅ File parsing
   - ❌ NOT for imports

5. **Best Practices**
   - ✅ Profile your imports first (`python -X importtime`)
   - ✅ Use lazy imports for optional/heavy deps
   - ✅ Use `@lru_cache` for expensive computations
   - ✅ Monitor cache hit rate with `cache_info()`
   - ✅ Use `TYPE_CHECKING` for type-only imports
   - ✅ Start with `maxsize=128` for lru_cache

---

## 🎯 Next Steps

1. **Run the examples:**
   ```bash
   python examples/lru_cache_explained.py
   python examples/import_profiling_guide.py
   python examples/import_profiler_implementation.py
   ```

2. **Profile your app:**
   ```bash
   python -X importtime -c "import your_app" 2> import.log
   ```

3. **Identify slow imports** (>100ms) and consider lazy loading

4. **Add import profiling to your profilist library** (see `import_profiler_implementation.py` for implementation)

5. **Use your existing Timer and MemoryProfiler** to track import overhead

---

## 📚 Additional Resources

- [Python importlib documentation](https://docs.python.org/3/library/importlib.html)
- [functools.lru_cache documentation](https://docs.python.org/3/library/functools.html#functools.lru_cache)
- [PEP 562 - Module __getattr__](https://peps.python.org/pep-0562/)
- [Python -X importtime](https://docs.python.org/3/using/cmdline.html#cmdoption-X)

---

**Created by:** 3 parallel exploration agents analyzing your profilist codebase
**Date:** 2025-11-17
**Branch:** `claude/profile-python-imports-014DQ5qh5xSinuCxHQ95Yojv`
