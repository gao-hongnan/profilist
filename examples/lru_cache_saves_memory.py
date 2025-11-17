"""
When @lru_cache SAVES Memory: Real-World Examples
==================================================

Counter-intuitive: @lru_cache can SAVE memory by preventing duplicate objects!

This demonstrates scenarios where caching REDUCES memory usage by avoiding
the creation of duplicate large objects.
"""

import sys
import tracemalloc
from functools import lru_cache
import random
import time


# ============================================================================
# Example 1: Loading Large Configuration Objects (CLASSIC CASE)
# ============================================================================

print("=" * 80)
print("Example 1: Loading Large Configuration Objects")
print("=" * 80)

print("""
Scenario: A web app loads user settings from a database. Many users have
identical settings (e.g., default theme, default language). Without caching,
each request creates a duplicate settings object.
""")


class UserSettings:
    """Large settings object with many fields."""

    def __init__(self, user_id: int):
        self.user_id = user_id
        # Simulate large configuration data
        self.theme_colors = {f"color_{i}": f"#{random.randint(0, 0xFFFFFF):06x}" for i in range(100)}
        self.language_pack = {f"key_{i}": f"translation_{i}" * 10 for i in range(500)}
        self.preferences = {f"pref_{i}": random.choice([True, False]) for i in range(200)}
        self.cached_data = [random.random() for _ in range(1000)]  # Large numeric data

    def __sizeof__(self):
        """Approximate size in bytes."""
        return sum([
            sys.getsizeof(self.theme_colors),
            sys.getsizeof(self.language_pack),
            sys.getsizeof(self.preferences),
            sys.getsizeof(self.cached_data),
        ])


# WITHOUT caching - creates duplicates
def load_settings_no_cache(user_id: int) -> UserSettings:
    """Simulate loading settings (creates new object each time)."""
    return UserSettings(user_id)


# WITH caching - reuses same object
@lru_cache(maxsize=128)
def load_settings_cached(user_id: int) -> UserSettings:
    """Cached version - returns same object for same user_id."""
    return UserSettings(user_id)


# Simulate web app handling 1000 requests
# In reality, many users have the same ID (returning users)
user_ids = [random.randint(1, 50) for _ in range(1000)]  # Only 50 unique users

print(f"\nSimulating 1000 requests from {len(set(user_ids))} unique users...\n")

# Test WITHOUT cache
print("📊 Test 1: WITHOUT @lru_cache")
tracemalloc.start()
settings_no_cache = []

for user_id in user_ids:
    settings = load_settings_no_cache(user_id)
    settings_no_cache.append(settings)

current_no_cache, peak_no_cache = tracemalloc.get_traced_memory()
tracemalloc.stop()

print(f"   Objects created: {len(settings_no_cache)}")
print(f"   Memory used: {current_no_cache / 1024 / 1024:.2f} MB")
print(f"   Peak memory: {peak_no_cache / 1024 / 1024:.2f} MB")

# Check for duplicates
unique_objects_no_cache = len(set(id(s) for s in settings_no_cache))
print(f"   Unique objects: {unique_objects_no_cache}")
print(f"   Duplicate objects: {len(settings_no_cache) - unique_objects_no_cache}")

# Test WITH cache
print("\n📊 Test 2: WITH @lru_cache(maxsize=128)")
tracemalloc.start()
settings_cached = []

for user_id in user_ids:
    settings = load_settings_cached(user_id)
    settings_cached.append(settings)

current_cached, peak_cached = tracemalloc.get_traced_memory()
tracemalloc.stop()

print(f"   Objects created: {len(settings_cached)}")
print(f"   Memory used: {current_cached / 1024 / 1024:.2f} MB")
print(f"   Peak memory: {peak_cached / 1024 / 1024:.2f} MB")

# Check for duplicates
unique_objects_cached = len(set(id(s) for s in settings_cached))
print(f"   Unique objects: {unique_objects_cached}")
print(f"   Shared references: {len(settings_cached) - unique_objects_cached}")

print(f"\n🎯 MEMORY SAVINGS:")
memory_saved = (current_no_cache - current_cached) / 1024 / 1024
savings_percent = (1 - current_cached / current_no_cache) * 100
print(f"   Memory saved: {memory_saved:.2f} MB ({savings_percent:.1f}% reduction)")
print(f"   Cache info: {load_settings_cached.cache_info()}")

print(f"""
💡 WHY THIS SAVES MEMORY:
   Without cache:
   - Created {len(settings_no_cache)} separate UserSettings objects
   - Each object ~{current_no_cache / len(settings_no_cache) / 1024:.2f} KB
   - Many duplicates for the same user_id

   With cache:
   - Created only {unique_objects_cached} unique UserSettings objects
   - Other {len(settings_cached) - unique_objects_cached} are REFERENCES to cached objects
   - Same user_id = same object in memory (no duplication)

   Saved: {memory_saved:.2f} MB by avoiding {len(settings_no_cache) - unique_objects_cached} duplicate objects!
""")


# ============================================================================
# Example 2: ML Feature Engineering (REAL-WORLD SCENARIO)
# ============================================================================

print("\n" + "=" * 80)
print("Example 2: Machine Learning Feature Vectors")
print("=" * 80)

print("""
Scenario: ML model needs feature vectors for products. Same products appear
repeatedly in a batch. Without caching, we compute the same features multiple
times, creating duplicate arrays.
""")


def compute_features_no_cache(product_id: int) -> list[float]:
    """Expensive feature computation (no cache)."""
    # Simulate expensive feature extraction
    features = []
    for i in range(1000):  # 1000-dimensional feature vector
        # Simulate complex computation
        features.append(sum([product_id * j * 0.001 for j in range(100)]))
    return features


@lru_cache(maxsize=256)
def compute_features_cached(product_id: int) -> tuple[float, ...]:
    """Cached version (returns tuple for hashability)."""
    features = []
    for i in range(1000):
        features.append(sum([product_id * j * 0.001 for j in range(100)]))
    return tuple(features)  # Tuple is hashable (required for lru_cache)


# Simulate processing a batch with repeated product IDs
batch_size = 5000
product_ids = [random.randint(1, 100) for _ in range(batch_size)]  # 100 unique products

print(f"\nProcessing batch of {batch_size} items ({len(set(product_ids))} unique products)...\n")

# Test WITHOUT cache
print("📊 Test 1: WITHOUT @lru_cache")
tracemalloc.start()
start = time.perf_counter()

features_no_cache = []
for pid in product_ids:
    features = compute_features_no_cache(pid)
    features_no_cache.append(features)

time_no_cache = time.perf_counter() - start
current_no_cache, peak_no_cache = tracemalloc.get_traced_memory()
tracemalloc.stop()

print(f"   Time: {time_no_cache:.3f} seconds")
print(f"   Memory: {current_no_cache / 1024 / 1024:.2f} MB")
print(f"   Unique feature vectors: {len(set(id(f) for f in features_no_cache))}")

# Test WITH cache
print("\n📊 Test 2: WITH @lru_cache(maxsize=256)")
tracemalloc.start()
start = time.perf_counter()

features_cached = []
for pid in product_ids:
    features = compute_features_cached(pid)
    features_cached.append(features)

time_cached = time.perf_counter() - start
current_cached, peak_cached = tracemalloc.get_traced_memory()
tracemalloc.stop()

print(f"   Time: {time_cached:.3f} seconds")
print(f"   Memory: {current_cached / 1024 / 1024:.2f} MB")
print(f"   Unique feature vectors: {len(set(id(f) for f in features_cached))}")
print(f"   Cache info: {compute_features_cached.cache_info()}")

print(f"\n🎯 RESULTS:")
memory_saved = (current_no_cache - current_cached) / 1024 / 1024
time_saved = time_no_cache - time_cached
print(f"   Memory saved: {memory_saved:.2f} MB ({(1 - current_cached/current_no_cache)*100:.1f}% reduction)")
print(f"   Time saved: {time_saved:.3f} seconds ({time_no_cache/time_cached:.1f}x faster)")

print(f"""
💡 WHY THIS SAVES MEMORY:
   Without cache:
   - Computed features {batch_size} times
   - Created {batch_size} separate arrays in memory
   - Even for duplicate product_ids

   With cache:
   - Computed features {compute_features_cached.cache_info().misses} times (only unique products)
   - Other {compute_features_cached.cache_info().hits} are references to cached tuples
   - Same product_id = same feature vector in memory

   This is CRUCIAL for:
   ✅ Batch processing with duplicates
   ✅ Real-time serving with hot products
   ✅ Training data with class imbalance
""")


# ============================================================================
# Example 3: String/Object Interning Pattern
# ============================================================================

print("\n" + "=" * 80)
print("Example 3: String/Object Interning (Advanced Pattern)")
print("=" * 80)

print("""
Scenario: Parsing large CSV with repeated categorical values. Each row creates
string objects, but many are duplicates (e.g., "USA", "Male", "Premium").

Python interns small strings automatically, but not large ones or custom objects.
@lru_cache can implement custom interning.
""")


class CategoryData:
    """Represents categorical data with metadata."""

    def __init__(self, value: str):
        self.value = value
        self.metadata = {f"attr_{i}": f"value_{i}" for i in range(50)}  # Large metadata
        self.encoding = [ord(c) for c in value]  # Character encodings

    def __eq__(self, other):
        return isinstance(other, CategoryData) and self.value == other.value

    def __hash__(self):
        return hash(self.value)


def create_category_no_cache(value: str) -> CategoryData:
    """Create category object (no cache)."""
    return CategoryData(value)


@lru_cache(maxsize=1000)
def create_category_cached(value: str) -> CategoryData:
    """Create category object (cached - interning pattern)."""
    return CategoryData(value)


# Simulate parsing CSV with 10,000 rows
# Only 20 unique categories, but repeated many times
categories = ["Premium", "Basic", "Free", "Enterprise", "Trial"] * 2000  # 10,000 total
countries = random.choices(["USA", "UK", "Canada", "Germany", "France"], k=10000)

print(f"\nSimulating parsing 10,000 CSV rows with categorical data...\n")

# Test WITHOUT cache
print("📊 Test 1: WITHOUT @lru_cache (creates duplicate objects)")
tracemalloc.start()

data_no_cache = []
for cat, country in zip(categories, countries):
    cat_obj = create_category_no_cache(cat)
    country_obj = create_category_no_cache(country)
    data_no_cache.append((cat_obj, country_obj))

current_no_cache, peak_no_cache = tracemalloc.get_traced_memory()
tracemalloc.stop()

unique_cat_objects = len(set(id(row[0]) for row in data_no_cache))
unique_country_objects = len(set(id(row[1]) for row in data_no_cache))

print(f"   Total objects: {len(data_no_cache) * 2}")
print(f"   Memory: {current_no_cache / 1024 / 1024:.2f} MB")
print(f"   Unique category objects: {unique_cat_objects} (should be 5)")
print(f"   Unique country objects: {unique_country_objects} (should be 5)")

# Test WITH cache
print("\n📊 Test 2: WITH @lru_cache (object interning)")
tracemalloc.start()

data_cached = []
for cat, country in zip(categories, countries):
    cat_obj = create_category_cached(cat)
    country_obj = create_category_cached(country)
    data_cached.append((cat_obj, country_obj))

current_cached, peak_cached = tracemalloc.get_traced_memory()
tracemalloc.stop()

unique_cat_objects_cached = len(set(id(row[0]) for row in data_cached))
unique_country_objects_cached = len(set(id(row[1]) for row in data_cached))

print(f"   Total objects: {len(data_cached) * 2}")
print(f"   Memory: {current_cached / 1024 / 1024:.2f} MB")
print(f"   Unique category objects: {unique_cat_objects_cached}")
print(f"   Unique country objects: {unique_country_objects_cached}")
print(f"   Cache info: {create_category_cached.cache_info()}")

print(f"\n🎯 MEMORY SAVINGS:")
memory_saved = (current_no_cache - current_cached) / 1024 / 1024
savings_percent = (1 - current_cached / current_no_cache) * 100
print(f"   Memory saved: {memory_saved:.2f} MB ({savings_percent:.1f}% reduction)")

print(f"""
💡 WHY THIS SAVES MEMORY (INTERNING):
   Without cache:
   - Created {len(data_no_cache) * 2} CategoryData objects
   - Even though only ~10 unique values exist!
   - Massive duplication

   With cache (@lru_cache):
   - Created only {create_category_cached.cache_info().misses} unique objects
   - Other {create_category_cached.cache_info().hits} are shared references
   - Same string = same object (interning pattern)

   This is similar to Python's string interning, but for custom objects!
""")


# ============================================================================
# Example 4: Graph/Tree Node Deduplication
# ============================================================================

print("\n" + "=" * 80)
print("Example 4: Graph Node Deduplication (Advanced)")
print("=" * 80)

print("""
Scenario: Building a graph where many nodes represent the same entity.
Without caching, duplicate nodes waste memory and break identity checks.
""")


class GraphNode:
    """Node in a graph with metadata."""

    def __init__(self, node_id: int):
        self.node_id = node_id
        self.properties = {f"prop_{i}": random.random() for i in range(100)}
        self.neighbors = []  # Will be populated later

    def __hash__(self):
        return hash(self.node_id)

    def __eq__(self, other):
        return isinstance(other, GraphNode) and self.node_id == other.node_id


@lru_cache(maxsize=10000)
def get_or_create_node(node_id: int) -> GraphNode:
    """Get cached node or create new one (ensures single instance per ID)."""
    return GraphNode(node_id)


# Build a graph with 1000 edges
# Only 100 unique nodes, but each edge references 2 nodes
edges = [(random.randint(1, 100), random.randint(1, 100)) for _ in range(1000)]

print(f"\nBuilding graph with {len(edges)} edges ({100} unique nodes)...\n")

# Without cache - creates duplicates
print("📊 Test 1: WITHOUT @lru_cache")
tracemalloc.start()

nodes_no_cache = []
for src, dst in edges:
    src_node = GraphNode(src)  # New object every time!
    dst_node = GraphNode(dst)
    nodes_no_cache.extend([src_node, dst_node])

current_no_cache, peak_no_cache = tracemalloc.get_traced_memory()
tracemalloc.stop()

unique_node_objects = len(set(id(n) for n in nodes_no_cache))
print(f"   Total node references: {len(nodes_no_cache)}")
print(f"   Unique node objects: {unique_node_objects}")
print(f"   Memory: {current_no_cache / 1024 / 1024:.2f} MB")
print(f"   Problem: Node identity broken! (node1 is node2) == False even if same ID")

# With cache - single instance per node_id
print("\n📊 Test 2: WITH @lru_cache")
tracemalloc.start()

nodes_cached = []
for src, dst in edges:
    src_node = get_or_create_node(src)  # Returns cached node if exists
    dst_node = get_or_create_node(dst)
    nodes_cached.extend([src_node, dst_node])

current_cached, peak_cached = tracemalloc.get_traced_memory()
tracemalloc.stop()

unique_node_objects_cached = len(set(id(n) for n in nodes_cached))
print(f"   Total node references: {len(nodes_cached)}")
print(f"   Unique node objects: {unique_node_objects_cached}")
print(f"   Memory: {current_cached / 1024 / 1024:.2f} MB")
print(f"   Cache info: {get_or_create_node.cache_info()}")
print(f"   ✅ Node identity preserved! (node1 is node2) == True for same ID")

# Verify identity
node_5_first = None
node_5_second = None
for i, (src, dst) in enumerate(edges):
    if src == 5:
        if node_5_first is None:
            node_5_first = nodes_cached[i * 2]
        else:
            node_5_second = nodes_cached[i * 2]
            break

if node_5_first and node_5_second:
    print(f"\n🔍 Identity check:")
    print(f"   First occurrence of node 5: {id(node_5_first)}")
    print(f"   Second occurrence of node 5: {id(node_5_second)}")
    print(f"   Same object? {node_5_first is node_5_second}")

print(f"\n🎯 MEMORY SAVINGS:")
memory_saved = (current_no_cache - current_cached) / 1024 / 1024
savings_percent = (1 - current_cached / current_no_cache) * 100
print(f"   Memory saved: {memory_saved:.2f} MB ({savings_percent:.1f}% reduction)")

print(f"""
💡 WHY THIS SAVES MEMORY (AND FIXES BUGS):
   Without cache:
   - Created {unique_node_objects} separate GraphNode objects
   - Same node_id has multiple objects in memory
   - Identity checks fail: (node1 is node2) == False even for same ID
   - Wastes memory AND breaks graph algorithms!

   With cache:
   - Created only {unique_node_objects_cached} unique GraphNode objects
   - Same node_id = same object reference
   - Identity checks work: (node1 is node2) == True for same ID
   - Saves memory AND fixes correctness!

   This pattern is ESSENTIAL for:
   ✅ Graph/tree structures
   ✅ Entity management systems
   ✅ Object pools
   ✅ Flyweight pattern
""")


# ============================================================================
# SUMMARY: When @lru_cache SAVES Memory
# ============================================================================

print("\n" + "=" * 80)
print("🎯 SUMMARY: When @lru_cache SAVES Memory")
print("=" * 80)

print("""
┌────────────────────────────────────────────────────────────────────────────┐
│ COMMON MISCONCEPTION: "@lru_cache uses more memory (stores cached results)" │
│                                                                              │
│ REALITY: @lru_cache SAVES memory by preventing duplicate object creation!  │
└────────────────────────────────────────────────────────────────────────────┘

✅ USE @lru_cache TO SAVE MEMORY WHEN:

1️⃣  REPEATED CALLS WITH SAME ARGUMENTS CREATE LARGE OBJECTS
   Example: load_user_settings(user_id) called 1000x for 50 unique users
   Without cache: 1000 UserSettings objects
   With cache: 50 UserSettings objects (950 duplicates eliminated!)

2️⃣  EXPENSIVE FEATURE COMPUTATION WITH DUPLICATES
   Example: compute_ml_features(product_id) in batch processing
   Without cache: Compute 5000 feature vectors (many duplicates)
   With cache: Compute 100 unique vectors, reuse for 4900 calls

3️⃣  OBJECT INTERNING (FLYWEIGHT PATTERN)
   Example: Parsing CSV with repeated categorical values
   Without cache: 10,000 CategoryData objects for 10 unique values
   With cache: 10 CategoryData objects, 9,990 are shared references

4️⃣  GRAPH/TREE NODE DEDUPLICATION
   Example: Building graph where nodes appear in multiple edges
   Without cache: Duplicate nodes, broken identity checks
   With cache: Single instance per node, correct identity, less memory

5️⃣  IMMUTABLE DATA STRUCTURES WITH SHARING
   Example: Configuration objects, lookup tables, enum-like classes
   Without cache: Redundant copies
   With cache: Single canonical instance


❌ DO NOT USE @lru_cache TO SAVE MEMORY WHEN:

1. Function returns small primitives (int, float, small strings)
   → Python already interns these, cache overhead > savings

2. Function is called with mostly unique arguments
   → Cache fills up with single-use entries, wastes memory

3. Return values are large AND calls have diverse arguments
   → Cache grows unbounded, uses MORE memory than no cache

4. Return values are mutable and modified by callers
   → Shared references = shared mutations = BUGS!


🎯 GOLDEN RULE:

   @lru_cache saves memory when:
   • Function creates LARGE objects (KB to MB)
   • Called with REPEATED arguments (high cache hit rate)
   • Return values are IMMUTABLE (safe to share)

   Memory saved = (object_size × duplicate_calls) - cache_overhead


📊 REAL-WORLD NUMBERS (from examples above):

   Example 1 (UserSettings):     {memory_saved:.2f} MB saved ({savings_percent:.1f}% reduction)
   Example 2 (ML Features):      Saved by avoiding duplicate computation
   Example 3 (Interning):        Massive savings by sharing categorical objects
   Example 4 (Graph Nodes):      Saved + fixed correctness bugs!


🚀 BEST PRACTICES:

1. Monitor cache hit rate with .cache_info()
   → High hit rate (>70%) = good candidate for caching

2. Choose maxsize wisely
   → maxsize = estimated unique arguments
   → Too small = evictions, too large = memory waste

3. Benchmark memory with tracemalloc
   → Measure before/after to confirm savings

4. Use for immutable return values only
   → Or return defensive copies

5. Consider alternatives for mutable objects
   → Explicit object pools, weak references, etc.
""")

print("\n" + "=" * 80)
print("✅ Run this script to see all memory measurements!")
print("   python examples/lru_cache_saves_memory.py")
print("=" * 80)
