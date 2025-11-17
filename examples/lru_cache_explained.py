"""
Comprehensive LRU Cache Examples - Understanding maxsize Parameter
==================================================================

LRU = Least Recently Used
Cache stores function results to avoid recomputation.
When cache is full, the LEAST RECENTLY USED result is evicted.
"""

from functools import lru_cache
import time


# ============================================================================
# Example 1: maxsize=1 (Only ONE result cached at a time)
# ============================================================================
print("=" * 70)
print("EXAMPLE 1: maxsize=1 - Parking lot with 1 space")
print("=" * 70)


@lru_cache(maxsize=1)
def expensive_computation_maxsize_1(n: int) -> int:
    """Simulate expensive computation with 1-slot cache."""
    print(f"  🔄 Computing {n}^2 (cache miss)...")
    time.sleep(0.5)  # Simulate expensive work
    return n * n


# Call sequence with maxsize=1
print("\n1️⃣  Call with n=5")
result = expensive_computation_maxsize_1(5)  # Cache miss - computes
print(f"   Result: {result}")
print(f"   Cache info: {expensive_computation_maxsize_1.cache_info()}")

print("\n2️⃣  Call with n=5 again")
result = expensive_computation_maxsize_1(5)  # Cache hit! ✅
print(f"   Result: {result} (instant!)")
print(f"   Cache info: {expensive_computation_maxsize_1.cache_info()}")

print("\n3️⃣  Call with n=10")
result = expensive_computation_maxsize_1(10)  # Cache miss - evicts n=5
print(f"   Result: {result}")
print(f"   Cache info: {expensive_computation_maxsize_1.cache_info()}")

print("\n4️⃣  Call with n=5 again")
result = expensive_computation_maxsize_1(5)  # Cache miss! n=5 was evicted
print(f"   Result: {result}")
print(f"   Cache info: {expensive_computation_maxsize_1.cache_info()}")

print("\n💡 INSIGHT:")
print("   maxsize=1 means only the LAST call is cached.")
print("   Any new call with different args evicts the previous cache.")
print("   Use case: When you repeatedly call with the SAME args in a loop")


# ============================================================================
# Example 2: maxsize=2 (TWO results cached)
# ============================================================================
print("\n" + "=" * 70)
print("EXAMPLE 2: maxsize=2 - Parking lot with 2 spaces")
print("=" * 70)


@lru_cache(maxsize=2)
def expensive_computation_maxsize_2(n: int) -> int:
    """Simulate expensive computation with 2-slot cache."""
    print(f"  🔄 Computing {n}^2 (cache miss)...")
    time.sleep(0.5)
    return n * n


print("\n1️⃣  Call with n=5")
result = expensive_computation_maxsize_2(5)  # Cache miss
print(f"   Result: {result}")
print(f"   Cache: {expensive_computation_maxsize_2.cache_info()}")

print("\n2️⃣  Call with n=10")
result = expensive_computation_maxsize_2(10)  # Cache miss
print(f"   Result: {result}")
print(f"   Cache: {expensive_computation_maxsize_2.cache_info()}")
print("   📦 Cache now has: [5, 10]")

print("\n3️⃣  Call with n=5 again")
result = expensive_computation_maxsize_2(5)  # Cache HIT! ✅
print(f"   Result: {result} (instant!)")
print(f"   Cache: {expensive_computation_maxsize_2.cache_info()}")
print("   📦 Cache still has: [5, 10] (5 was accessed, moved to 'most recent')")

print("\n4️⃣  Call with n=15")
result = expensive_computation_maxsize_2(15)  # Cache miss - evicts 10 (least recently used)
print(f"   Result: {result}")
print(f"   Cache: {expensive_computation_maxsize_2.cache_info()}")
print("   📦 Cache now has: [5, 15] (10 was evicted)")

print("\n5️⃣  Call with n=10 again")
result = expensive_computation_maxsize_2(10)  # Cache MISS! 10 was evicted
print(f"   Result: {result}")
print(f"   Cache: {expensive_computation_maxsize_2.cache_info()}")

print("\n💡 INSIGHT:")
print("   maxsize=2 caches the 2 MOST RECENTLY USED results.")
print("   When you add a 3rd unique arg, the OLDEST one is evicted.")
print("   Use case: Toggle between 2 configurations repeatedly")


# ============================================================================
# Example 3: maxsize=None (Unlimited cache - DANGEROUS!)
# ============================================================================
print("\n" + "=" * 70)
print("EXAMPLE 3: maxsize=None - Infinite parking lot 🚨")
print("=" * 70)


@lru_cache(maxsize=None)
def fibonacci(n: int) -> int:
    """Classic fibonacci with unlimited caching."""
    if n < 2:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)


print("\n1️⃣  Calculate fib(10)")
result = fibonacci(10)
print(f"   Result: {result}")
print(f"   Cache: {fibonacci.cache_info()}")

print("\n2️⃣  Calculate fib(20)")
result = fibonacci(20)
print(f"   Result: {result}")
print(f"   Cache: {fibonacci.cache_info()}")

print("\n💡 INSIGHT:")
print("   maxsize=None caches EVERY unique input FOREVER.")
print("   ⚠️  WARNING: Can lead to MEMORY LEAKS if unbounded inputs!")
print("   Use case: Pure functions with small, finite input space")


# ============================================================================
# Example 4: Real-world use case - API response caching
# ============================================================================
print("\n" + "=" * 70)
print("EXAMPLE 4: Real-world - API response caching")
print("=" * 70)


@lru_cache(maxsize=128)
def fetch_user_data(user_id: int) -> dict:
    """Simulate expensive API call."""
    print(f"  🌐 Making API call for user {user_id}...")
    time.sleep(0.3)
    return {"id": user_id, "name": f"User{user_id}", "email": f"user{user_id}@example.com"}


print("\nSimulating web server handling requests:")
print("\n📨 Request 1: GET /user/42")
user = fetch_user_data(42)
print(f"   Response: {user}")

print("\n📨 Request 2: GET /user/42 (same user)")
user = fetch_user_data(42)  # Instant! ✅
print(f"   Response: {user} (from cache!)")

print("\n📨 Request 3: GET /user/99")
user = fetch_user_data(99)
print(f"   Response: {user}")

print(f"\n   Final cache stats: {fetch_user_data.cache_info()}")
print("   💡 Cached 128 most recent users - typical web traffic pattern!")


# ============================================================================
# Example 5: Cache management
# ============================================================================
print("\n" + "=" * 70)
print("EXAMPLE 5: Cache management - clear and info")
print("=" * 70)


@lru_cache(maxsize=3)
def process_data(x: int) -> int:
    print(f"  Processing {x}...")
    return x * 2


# Build cache
for i in [1, 2, 3]:
    process_data(i)

print(f"\nCache before clear: {process_data.cache_info()}")

# Clear cache (e.g., after config change or memory pressure)
process_data.cache_clear()
print(f"Cache after clear:  {process_data.cache_info()}")

# Next call will be cache miss even though we called it before
process_data(1)  # Cache miss!
print(f"Cache after call:   {process_data.cache_info()}")


# ============================================================================
# SUMMARY TABLE
# ============================================================================
print("\n" + "=" * 70)
print("SUMMARY: When to use which maxsize?")
print("=" * 70)

summary = """
┌──────────────┬────────────────────────────┬─────────────────────────────┐
│   maxsize    │         Behavior           │         Use Case            │
├──────────────┼────────────────────────────┼─────────────────────────────┤
│      1       │ Only last result cached    │ Repeated calls with same    │
│              │                            │ args in tight loop          │
├──────────────┼────────────────────────────┼─────────────────────────────┤
│      2       │ Last 2 results cached      │ Toggle between 2 configs/   │
│              │                            │ states repeatedly           │
├──────────────┼────────────────────────────┼─────────────────────────────┤
│    32-128    │ Most common production     │ API responses, DB queries,  │
│              │ size (balance memory/hits) │ file parsing                │
├──────────────┼────────────────────────────┼─────────────────────────────┤
│   256-1024   │ Large cache for hot data   │ Web app with many users,    │
│              │                            │ high-traffic endpoints      │
├──────────────┼────────────────────────────┼─────────────────────────────┤
│     None     │ UNLIMITED (dangerous!)     │ Pure math functions with    │
│              │                            │ small finite input space    │
│              │                            │ (e.g., fib, factorial)      │
└──────────────┴────────────────────────────┴─────────────────────────────┘

🎯 RULE OF THUMB:
   • Start with 128 (good default for most apps)
   • Monitor cache_info() hit rate
   • Increase if hit rate < 70%
   • Decrease if memory usage is high
   • Use None ONLY for bounded mathematical functions
"""

print(summary)


# ============================================================================
# Advanced: LRU Cache is NOT for imports!
# ============================================================================
print("\n" + "=" * 70)
print("⚠️  IMPORTANT: LRU Cache ≠ Import Caching!")
print("=" * 70)

print("""
LRU Cache is for FUNCTION RESULTS, not for IMPORTS!

❌ WRONG - This doesn't help with import time:

   @lru_cache(maxsize=1)
   def get_pandas():
       import pandas as pd  # Still loads pandas fully!
       return pd

   df = get_pandas().DataFrame([1, 2, 3])  # Slow first time
   df = get_pandas().DataFrame([4, 5, 6])  # Still slow! (pandas already loaded)

✅ CORRECT - For imports, use LAZY LOADING:

   # Option 1: Import inside function
   def process_data():
       import pandas as pd  # Only loads when function is called
       return pd.DataFrame([1, 2, 3])

   # Option 2: Conditional import
   if use_advanced_features:
       import tensorflow as tf  # Only loads if needed

   # Option 3: Module-level lazy import (advanced)
   from typing import TYPE_CHECKING
   if TYPE_CHECKING:
       import pandas as pd  # Type hints only, not runtime
""")

print("\n✅ Script completed! Run this to see LRU cache in action.")
