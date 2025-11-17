"""
The LRU Cache + Import Pattern: Myth vs Reality
================================================

You've probably seen code like this:

    @lru_cache(maxsize=1)
    def get_pandas():
        import pandas as pd
        return pd

This is a COMMON PATTERN, but it's widely MISUNDERSTOOD.
Let's understand what it ACTUALLY does vs what people THINK it does.
"""

from functools import lru_cache
import sys
import time
import tracemalloc


print("=" * 80)
print("MYTH vs REALITY: @lru_cache with imports")
print("=" * 80)


# ============================================================================
# Example 1: What People THINK It Does (WRONG)
# ============================================================================

print("\n" + "-" * 80)
print("What People THINK This Does:")
print("-" * 80)

print("""
@lru_cache(maxsize=1)
def get_pandas():
    import pandas as pd
    return pd

# They think:
# 1st call: "Imports pandas (slow)"
# 2nd call: "Returns cached pandas, AVOIDS re-importing (fast)"

❌ WRONG! This is NOT what happens!
""")


# ============================================================================
# Example 2: What It ACTUALLY Does (Reality)
# ============================================================================

print("\n" + "-" * 80)
print("What It ACTUALLY Does:")
print("-" * 80)

print("""
The truth is more nuanced. Let's break it down with a lightweight example
using the 'json' module (since pandas might not be installed).
""")


# First, let's see what happens WITHOUT lru_cache
print("\n📊 Test 1: WITHOUT @lru_cache (normal import)")
print("-" * 80)


def get_json_normal():
    """Normal function that imports json."""
    import json
    return json


# Remove json from sys.modules if it's there (for clean test)
if 'json' in sys.modules:
    del sys.modules['json']

print(f"Before call: 'json' in sys.modules = {'json' in sys.modules}")

# First call
start = time.perf_counter()
json1 = get_json_normal()
time1 = time.perf_counter() - start
print(f"\n1st call: {time1:.6f} seconds")
print(f"After 1st call: 'json' in sys.modules = {'json' in sys.modules}")
print(f"Module ID: {id(json1)}")

# Second call
start = time.perf_counter()
json2 = get_json_normal()
time2 = time.perf_counter() - start
print(f"\n2nd call: {time2:.6f} seconds")
print(f"After 2nd call: 'json' in sys.modules = {'json' in sys.modules}")
print(f"Module ID: {id(json2)}")

print(f"\nSame module object? {json1 is json2}")

print("""
💡 KEY INSIGHT:
   Even WITHOUT @lru_cache, the 2nd call is INSTANT because:
   1. First call loads json into sys.modules
   2. Second call finds json in sys.modules (instant!)
   3. Both calls return the SAME module object (same ID)

   The 'import json' statement is CACHED BY PYTHON ITSELF in sys.modules!
""")


# Now let's test WITH lru_cache
print("\n" + "=" * 80)
print("📊 Test 2: WITH @lru_cache")
print("-" * 80)

# Remove json from sys.modules again for clean test
if 'json' in sys.modules:
    del sys.modules['json']


@lru_cache(maxsize=1)
def get_json_cached():
    """Function with @lru_cache that imports json."""
    print("   🔄 Inside get_json_cached() - executing import statement")
    import json
    return json


print(f"Before call: 'json' in sys.modules = {'json' in sys.modules}")

# First call
print("\n1st call to get_json_cached():")
start = time.perf_counter()
json1 = get_json_cached()
time1 = time.perf_counter() - start
print(f"   Time: {time1:.6f} seconds")
print(f"   'json' in sys.modules = {'json' in sys.modules}")
print(f"   Module ID: {id(json1)}")
print(f"   Cache info: {get_json_cached.cache_info()}")

# Second call
print("\n2nd call to get_json_cached():")
start = time.perf_counter()
json2 = get_json_cached()
time2 = time.perf_counter() - start
print(f"   Time: {time2:.6f} seconds")
print(f"   'json' in sys.modules = {'json' in sys.modules}")
print(f"   Module ID: {id(json2)}")
print(f"   Cache info: {get_json_cached.cache_info()}")

print(f"\nSame module object? {json1 is json2}")

print("""
💡 WHAT HAPPENED:
   1st call:
   - Executed the function body (you saw the print)
   - 'import json' loaded json into sys.modules
   - Returned the module object
   - @lru_cache stored the RETURN VALUE (the module object)

   2nd call:
   - @lru_cache returned the CACHED module object
   - Function body NEVER executed (no print!)
   - 'import json' was SKIPPED

   BUT WAIT! Even without @lru_cache, 'import json' on 2nd call would be
   instant because json is already in sys.modules!
""")


# ============================================================================
# Example 3: The REAL Difference (Benchmarking)
# ============================================================================

print("\n" + "=" * 80)
print("📊 Test 3: Benchmarking - What's the ACTUAL speedup?")
print("=" * 80)

# Setup: json is already loaded from previous tests


def get_json_no_cache():
    import json  # Already in sys.modules - will be instant
    return json


@lru_cache(maxsize=1)
def get_json_with_cache():
    import json  # Already in sys.modules - will be instant
    return json


# Warm up
get_json_no_cache()
get_json_with_cache()

# Benchmark WITHOUT cache (but module already loaded)
print("\n🔄 Calling 10,000 times WITHOUT @lru_cache:")
start = time.perf_counter()
for _ in range(10000):
    get_json_no_cache()
time_no_cache = time.perf_counter() - start
print(f"   Total time: {time_no_cache:.6f} seconds")
print(f"   Per call:   {time_no_cache / 10000 * 1_000_000:.3f} microseconds")

# Benchmark WITH cache
print("\n⚡ Calling 10,000 times WITH @lru_cache:")
start = time.perf_counter()
for _ in range(10000):
    get_json_with_cache()
time_with_cache = time.perf_counter() - start
print(f"   Total time: {time_with_cache:.6f} seconds")
print(f"   Per call:   {time_with_cache / 10000 * 1_000_000:.3f} microseconds")

speedup = time_no_cache / time_with_cache
print(f"\n⚡ Speedup: {speedup:.2f}x faster")

print(f"""
💡 ANALYSIS:
   @lru_cache is {speedup:.2f}x faster, but this speedup is NOT from avoiding import!

   The speedup comes from:
   ✅ Skipping the function call overhead
   ✅ Skipping the 'import json' statement execution (even though it's cached)
   ✅ Directly returning the cached reference

   WITHOUT @lru_cache:
   - Function is called
   - 'import json' statement is executed
   - Python checks sys.modules (instant, but still overhead)
   - Returns module

   WITH @lru_cache:
   - Function is NOT called at all
   - Returns cached result immediately

   The speedup is TINY (microseconds) because sys.modules lookup is already
   very fast. You're only saving the function call + sys.modules lookup overhead.
""")


# ============================================================================
# Example 4: When This Pattern IS Used (And Why)
# ============================================================================

print("\n" + "=" * 80)
print("When People Actually Use This Pattern (And The Real Reasons)")
print("=" * 80)

print("""
Despite the minimal speedup, you see this pattern in real codebases.
Here's WHY people actually use it:

1️⃣  LAZY LOADING (Main reason):

   The function isn't called until needed, so the import is deferred.

   @lru_cache(maxsize=1)
   def get_pandas():
       import pandas as pd  # Only imported when function is called
       return pd

   # App starts - pandas NOT imported yet

   if user_wants_advanced_features:
       pd = get_pandas()  # Import happens HERE
       df = pd.DataFrame(...)

   Benefits:
   ✅ Faster app startup (pandas not loaded immediately)
   ✅ Lower initial memory usage
   ❌ But you could just use lazy import directly! (see below)


2️⃣  SINGLETON PATTERN:

   Ensures the same module reference is returned (though Python already does this).
   Some people use it to be "explicit" about returning a singleton.

   @lru_cache(maxsize=1)
   def get_database_module():
       import custom_db_module
       return custom_db_module

   Benefits:
   ✅ Makes singleton behavior explicit
   ❌ Redundant since Python already caches imports


3️⃣  AVOIDING REPEATED sys.modules LOOKUPS (Micro-optimization):

   In tight loops (millions of iterations), saving even microseconds matters.

   @lru_cache(maxsize=1)
   def get_math():
       import math
       return math

   math = get_math()
   for _ in range(1_000_000):
       result = math.sqrt(42)  # Slightly faster than importing each iteration

   Benefits:
   ✅ Tiny speedup in tight loops
   ❌ But just import at module level or assign once!


4️⃣  DEPENDENCY INJECTION / TESTING:

   Makes it easier to mock/patch the module in tests.

   @lru_cache(maxsize=1)
   def get_requests():
       import requests
       return requests

   def fetch_data():
       requests = get_requests()
       return requests.get("https://api.example.com")

   # In tests:
   @patch('mymodule.get_requests')
   def test_fetch_data(mock_get_requests):
       mock_requests = Mock()
       mock_get_requests.return_value = mock_requests
       # ...

   Benefits:
   ✅ Easier to mock in tests
   ✅ Clear dependency injection point
   ❌ But you could use other DI patterns


5️⃣  DOCUMENTATION / INTENT:

   Some teams use it to signal "this is a lazy-loaded, singleton dependency".

   Benefits:
   ✅ Self-documenting code
   ❌ Could use comments instead
""")


# ============================================================================
# Example 5: Better Alternatives
# ============================================================================

print("\n" + "=" * 80)
print("BETTER ALTERNATIVES")
print("=" * 80)

print("""
Instead of @lru_cache with imports, consider these alternatives:


✅ ALTERNATIVE 1: Simple lazy import (no cache needed)

   def process_data(data):
       import pandas as pd  # Lazy import
       return pd.DataFrame(data)

   Why it's better:
   - Simpler (no decorator)
   - Same lazy loading benefit
   - Python already caches in sys.modules


✅ ALTERNATIVE 2: Module-level import (if used frequently)

   import pandas as pd  # Just import it at the top!

   def process_data(data):
       return pd.DataFrame(data)

   Why it's better:
   - Clearer dependencies (at top of file)
   - Faster function calls (no import overhead at all)
   - Standard Python convention


✅ ALTERNATIVE 3: Import once, assign to module variable

   _pandas = None

   def get_pandas():
       global _pandas
       if _pandas is None:
           import pandas as pd
           _pandas = pd
       return _pandas

   Why it's better:
   - Explicit lazy loading
   - No functools dependency
   - More control over initialization
   - But @lru_cache does the same thing more concisely


✅ ALTERNATIVE 4: Conditional import based on config

   USE_PANDAS = os.getenv("USE_PANDAS", "false").lower() == "true"

   if USE_PANDAS:
       import pandas as pd
   else:
       pd = None

   def process_data(data):
       if pd is None:
           raise RuntimeError("Pandas not enabled")
       return pd.DataFrame(data)

   Why it's better:
   - Clear feature flag
   - Import decision at startup
   - No runtime overhead
""")


# ============================================================================
# Example 6: Real-World Measurement
# ============================================================================

print("\n" + "=" * 80)
print("📊 REAL-WORLD MEASUREMENT: Does @lru_cache actually help?")
print("=" * 80)

print("\nLet's test with a realistic scenario: 1000 function calls")

# Scenario 1: Lazy import without cache


def process_no_cache(x):
    import json
    return json.dumps({"value": x})


# Scenario 2: Lazy import with cache
@lru_cache(maxsize=1)
def get_json_module():
    import json
    return json


def process_with_cache_import(x):
    json = get_json_module()
    return json.dumps({"value": x})


# Scenario 3: Module-level import (normal)
import json as json_module


def process_normal(x):
    return json_module.dumps({"value": x})


# Benchmark
iterations = 1000

print(f"\nProcessing {iterations} items...\n")

# Test 1
start = time.perf_counter()
for i in range(iterations):
    process_no_cache(i)
time1 = time.perf_counter() - start
print(f"1. Lazy import (no cache):     {time1:.6f}s ({time1/iterations*1_000_000:.2f} μs/call)")

# Test 2
start = time.perf_counter()
for i in range(iterations):
    process_with_cache_import(i)
time2 = time.perf_counter() - start
print(f"2. Lazy import + @lru_cache:   {time2:.6f}s ({time2/iterations*1_000_000:.2f} μs/call)")

# Test 3
start = time.perf_counter()
for i in range(iterations):
    process_normal(i)
time3 = time.perf_counter() - start
print(f"3. Module-level import:        {time3:.6f}s ({time3/iterations*1_000_000:.2f} μs/call)")

print(f"""
📊 RESULTS:
   Lazy import + @lru_cache is {time1/time2:.2f}x faster than lazy import without cache
   Module-level import is {time1/time3:.2f}x faster than lazy import without cache
   Module-level import is {time2/time3:.2f}x faster than lazy import + @lru_cache

💡 CONCLUSION:
   1. @lru_cache does provide a SMALL speedup over repeated lazy imports
   2. But module-level import is STILL faster (no function call overhead)
   3. The difference is TINY (microseconds per call)

   USE @lru_cache with imports ONLY IF:
   ✅ You need lazy loading (defer import until function is called)
   ✅ The import is expensive (like pandas, tensorflow)
   ✅ The function might not be called at all

   DO NOT use it if:
   ❌ The module is always used (just import at module level)
   ❌ You're trying to "speed up imports" (it doesn't work that way)
   ❌ You think it caches the import itself (it caches the return value)
""")


# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 80)
print("🎯 FINAL SUMMARY: @lru_cache with imports")
print("=" * 80)

print("""
WHAT IT DOES:
✅ Caches the MODULE REFERENCE (return value)
✅ Skips function execution on subsequent calls
✅ Provides LAZY LOADING (import happens on first call)

WHAT IT DOES NOT DO:
❌ Does NOT speed up the import itself
❌ Does NOT prevent the module from staying in sys.modules
❌ Does NOT free memory when function isn't called

WHEN YOU SEE IT IN THE WILD:
1. Lazy loading of heavy dependencies (pandas, tensorflow)
2. Optional features that might not be used
3. Micro-optimization in tight loops (questionable benefit)
4. Dependency injection / testing (easier mocking)
5. Cargo cult programming (copying without understanding)

BETTER ALTERNATIVES:
1. Simple lazy import (no cache) if you need lazy loading
2. Module-level import if you always use it (fastest)
3. Conditional import based on config/feature flags
4. Explicit singleton pattern with global variable

THE BOTTOM LINE:
- It's not WRONG, but it's usually UNNECESSARY
- The main benefit is LAZY LOADING, not caching
- You can achieve the same with simpler patterns
- Use it only if you have a specific reason (lazy loading + want lru_cache benefits)

IF YOU SEE IT IN PRODUCTION CODE:
- It's probably for lazy loading, not performance
- The developers might not fully understand what it does
- It's not harmful, just not usually the best approach
- Consider refactoring to simpler lazy import if you touch the code
""")

print("\n" + "=" * 80)
print("✅ Run this script to see all benchmarks and measurements!")
print("   python examples/lru_cache_import_myth.py")
print("=" * 80)
