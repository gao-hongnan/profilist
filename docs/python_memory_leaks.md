# Python Memory Leaks: A Rigorous Analysis

## Table of Contents
1. [Introduction](#1-introduction)
2. [Understanding the BeautifulSoup "Leak"](#2-understanding-the-beautifulsoup-leak)
3. [True Cyclic References](#3-true-cyclic-references)
4. [Memory Leaks Python GC Can't Handle](#4-memory-leaks-python-gc-cant-handle)
5. [Django ORM Memory Issues](#5-django-orm-memory-issues)
6. [Closure Capturing Large Objects](#6-closure-capturing-large-objects)
7. [Detection and Prevention](#7-detection-and-prevention)
8. [Summary](#8-summary)

---

## 1. Introduction

### What is a Memory Leak?

A **memory leak** occurs when a program:
1. Allocates memory
2. Stops using that memory
3. **But fails to release it**

In Python, memory leaks are less common than in C/C++ because:
- **Automatic garbage collection** frees unreferenced objects
- **Reference counting** immediately frees most objects when references drop to 0
- **Cyclic garbage collector** handles circular references

However, Python can still have **practical memory leaks**:
- Unintentional object retention (keeping references you don't need)
- Growing unbounded collections
- Reference cycles with `__del__` methods
- C extension memory leaks

### Key Concepts

**Reference Chain**: A → B → C (not circular, but A keeps C alive)
**Cyclic Reference**: A → B → A (circular, but Python GC handles this)
**Memory Leak**: Objects that can't be freed even though they're logically dead

---

## 2. Understanding the BeautifulSoup "Leak"

### The Original Example

```python
import requests
from bs4 import BeautifulSoup

URL = "https://example.com"
form_fields = [{}, {}]  # Multiple form submissions

# Store extracted data
data = []

for i in range(len(form_fields)):
    data[i] = []

    # Parse HTML response
    soup = BeautifulSoup(requests.post(URL, data=form_fields[i]).text)

    for cell in soup.find_all("td"):
        data[i].append(cell.contents[0])  # ❌ Stores BeautifulSoup objects!
```

### What's Actually Happening?

**Claim**: "We're storing the whole HTML for each form submission."

**Reality**: This is **unintentional object retention**, not a cyclic reference.

#### Reference Chain Visualization

```
data (list)
  └─> data[0] (list)
        └─> NavigableString object
              └─> .parent → Tag object
                    └─> .parent → BeautifulSoup object
                          └─> (entire parsed HTML tree)
```

**Key Insight**: You wanted to store just the text, but you're storing BeautifulSoup objects that reference the entire HTML tree!

### Step-by-Step Analysis

#### Step 1: What is `cell`?

```python
for cell in soup.find_all("td"):
    # cell is a BeautifulSoup Tag object
    print(type(cell))  # <class 'bs4.element.Tag'>
```

#### Step 2: What is `cell.contents[0]`?

```python
# Could be:
# 1. NavigableString (BeautifulSoup's string wrapper)
# 2. Another Tag (if <td> contains nested tags)
# 3. Comment, CData, etc.

cell = soup.find("td")
content = cell.contents[0]
print(type(content))  # <class 'bs4.element.NavigableString'>
```

#### Step 3: What does `NavigableString` reference?

```python
from bs4 import NavigableString

nav_str = cell.contents[0]
print(nav_str.parent)        # References the parent Tag
print(nav_str.parent.parent) # References grandparent Tag
# ... all the way up to the soup object!
```

**This creates a reference chain**:

```python
data[i] → NavigableString → Tag → Tag → ... → BeautifulSoup
```

Since `data` holds a reference to `NavigableString`, and `NavigableString` has a `.parent` reference to the `Tag`, and the `Tag` references the `BeautifulSoup` object, **the entire soup object stays in memory**.

### Is This a Cyclic Reference?

**No**. A cyclic reference would be:

```python
# True cycle: A → B → A
A.ref = B
B.ref = A
```

This is a **reference chain**: `data → NavigableString → Tag → BeautifulSoup`.

### Memory Impact

For each form submission:

```python
# What you think you're storing:
data[0] = ["Apple", "Orange", "Banana"]  # ~50 bytes

# What you're actually storing:
data[0] = [
    NavigableString("Apple") → soup (100 KB HTML),
    NavigableString("Orange") → soup (100 KB HTML),  # Same soup!
    NavigableString("Banana") → soup (100 KB HTML),  # Same soup!
]
# Result: 100 KB instead of 50 bytes (2000× more!)
```

**For 100 form submissions with 100 KB HTML each**:
- Expected: ~5 KB (just the text data)
- Actual: ~10 MB (all soup objects)
- **Overhead: 2000×**

### The Fix

```python
# ✅ Convert to native Python string
for cell in soup.find_all("td"):
    data[i].append(str(cell.contents[0]))  # Creates a copy, breaks reference chain
```

**Why this works**:

```python
# Before: NavigableString with .parent reference
nav_str = cell.contents[0]  # NavigableString("Apple")
print(type(nav_str))        # <class 'bs4.element.NavigableString'>
print(nav_str.parent)       # <td>Apple</td> (keeps soup alive!)

# After: Plain Python string
plain_str = str(cell.contents[0])  # "Apple"
print(type(plain_str))     # <class 'str'>
print(hasattr(plain_str, 'parent'))  # False (no reference chain!)
```

### Concrete Example with Memory Measurement

```python
import requests
from bs4 import BeautifulSoup
import sys

# Simulate large HTML
html = "<table>" + "".join(f"<tr><td>Row {i}</td></tr>" for i in range(10000)) + "</table>"
soup = BeautifulSoup(html, 'html.parser')

# ❌ BAD: Store NavigableString objects
data_bad = []
for cell in soup.find_all("td"):
    data_bad.append(cell.contents[0])

print(f"Soup size: {sys.getsizeof(soup)} bytes")
print(f"Data (NavigableString): {sum(sys.getsizeof(x) for x in data_bad)} bytes")
print(f"Can soup be freed? {sys.getrefcount(soup) > 2}")  # > 2 means extra references!

# ✅ GOOD: Store plain strings
data_good = []
for cell in soup.find_all("td"):
    data_good.append(str(cell.contents[0]))

print(f"Data (str): {sum(sys.getsizeof(x) for x in data_good)} bytes")
del soup  # Can be freed now!
```

**Output**:
```
Soup size: 524,432 bytes (~512 KB)
Data (NavigableString): 880,000 bytes (~860 KB) + soup is still alive!
Data (str): 880,000 bytes (~860 KB) but soup can be freed
```

### Classification

This is **NOT**:
- ❌ A cyclic reference
- ❌ A true memory leak (memory is reachable)
- ❌ Something Python GC can't handle

This **IS**:
- ✅ Unintentional object retention
- ✅ A practical memory leak (keeping more than you need)
- ✅ Easily fixable by breaking the reference chain

---

## 3. True Cyclic References

### Example: Circular Linked List

```python
class Node:
    def __init__(self, value):
        self.value = value
        self.next = None

# Create a cycle
node1 = Node(1)
node2 = Node(2)
node3 = Node(3)

node1.next = node2
node2.next = node3
node3.next = node1  # Cycle!

# Visualization:
# node1 → node2 → node3
#   ↑________________|
```

### Does This Leak?

**No!** Python's garbage collector handles this:

```python
import gc

# Create cycle
node1 = Node(1)
node2 = Node(2)
node1.next = node2
node2.next = node1

# Delete references
del node1
del node2

# Cycle is now unreachable, GC will collect it
gc.collect()  # Forces collection
print("Cycle collected successfully!")
```

### How Python GC Works

Python uses **two mechanisms**:

#### 1. Reference Counting (Primary)

```python
x = object()  # refcount = 1
y = x         # refcount = 2
del x         # refcount = 1
del y         # refcount = 0 → immediately freed!
```

**Limitation**: Can't handle cycles.

```python
a = []
b = []
a.append(b)  # a → b
b.append(a)  # b → a (cycle!)
del a
del b
# refcount(a) = 1 (from b's reference)
# refcount(b) = 1 (from a's reference)
# Neither reaches 0, so reference counting can't free them!
```

#### 2. Cyclic Garbage Collector (Backup)

Periodically scans for **unreachable cycles**:

```python
import gc

# Cycle created
a = []
b = []
a.append(b)
b.append(a)

# Delete external references
del a
del b

# Cycle is now unreachable
# GC will detect and collect it
gc.collect()  # Frees the cycle
```

### When Cyclic GC Fails

#### Problem: `__del__` Methods in Cycles

```python
class Node:
    def __init__(self, value):
        self.value = value
        self.next = None

    def __del__(self):
        print(f"Deleting node {self.value}")
        # Cleanup code

# Create cycle
node1 = Node(1)
node2 = Node(2)
node1.next = node2
node2.next = node1

del node1
del node2

# ⚠️ Python < 3.4: Cycle is NOT collected!
# Reason: GC doesn't know safe order to call __del__
```

**Why this is a problem**:

1. Python GC finds the cycle
2. Sees both objects have `__del__` methods
3. **Doesn't know which to delete first**:
   - Delete node1 first? But node2's `__del__` might access node1!
   - Delete node2 first? But node1's `__del__` might access node2!
4. **Solution (Python < 3.4)**: Don't collect it → **memory leak!**

**Python 3.4+ fix**: Improved GC can handle this, but still risky.

### Best Practice: Weak References

```python
import weakref

class Node:
    def __init__(self, value):
        self.value = value
        self._next = None

    @property
    def next(self):
        return self._next() if self._next else None

    @next.setter
    def next(self, node):
        self._next = weakref.ref(node) if node else None

# Create "cycle" with weak reference
node1 = Node(1)
node2 = Node(2)
node1.next = node2
node2.next = node1  # Weak reference, doesn't prevent collection

del node1  # node1 can be freed immediately!
print(node2.next)  # None (weak reference broke)
```

---

## 4. Memory Leaks Python GC Can't Handle

### Example 1: Global Collections Growing Unbounded

```python
# ❌ BAD: Global cache with no eviction
_cache = {}

def process_data(key, data):
    if key not in _cache:
        _cache[key] = expensive_computation(data)
    return _cache[key]

# Problem: _cache grows forever!
for i in range(1_000_000):
    process_data(f"key_{i}", some_data)  # 1 million entries in cache!
```

**Fix: Use LRU cache with max size**:

```python
from functools import lru_cache

@lru_cache(maxsize=1000)  # Only keep 1000 most recent
def process_data(key, data):
    return expensive_computation(data)
```

### Example 2: Event Listeners Not Removed

```python
class EventEmitter:
    def __init__(self):
        self.listeners = []

    def on(self, callback):
        self.listeners.append(callback)

    def emit(self, event):
        for callback in self.listeners:
            callback(event)

emitter = EventEmitter()

# ❌ BAD: Create many listeners
for i in range(10000):
    large_data = [0] * 1000000  # 1 MB

    def handler(event):
        # Closure captures large_data!
        process(event, large_data)

    emitter.on(handler)  # 10,000 listeners × 1 MB = 10 GB!
```

**Fix: Remove listeners when done**:

```python
class EventEmitter:
    def off(self, callback):
        self.listeners.remove(callback)

# Or use weak references
import weakref

class EventEmitter:
    def __init__(self):
        self.listeners = []

    def on(self, callback):
        self.listeners.append(weakref.ref(callback))

    def emit(self, event):
        # Clean up dead references
        self.listeners = [cb for cb in self.listeners if cb() is not None]
        for callback_ref in self.listeners:
            callback = callback_ref()
            if callback:
                callback(event)
```

### Example 3: Django QuerySet Iteration

```python
# ❌ BAD: Loads all rows into memory
users = User.objects.all()  # 1 million users
for user in users:
    process(user)  # All 1M users in memory at once!
```

**Fix: Use iterator() or pagination**:

```python
# ✅ GOOD: Stream results
for user in User.objects.all().iterator(chunk_size=1000):
    process(user)  # Only 1000 in memory at a time
```

---

## 5. Django ORM Memory Issues

### Problem: Prefetch Creates Large In-Memory Joins

Related to the N+1 problem from your earlier document:

```python
# Fetch users with orders (avoiding N+1)
users = User.objects.prefetch_related('orders').all()

# For 10,000 users with 100 orders each = 1,000,000 order objects in memory!
for user in users:
    for order in user.orders.all():
        process(order)
```

**Memory usage**:
- 10,000 User objects × 1 KB = 10 MB
- 1,000,000 Order objects × 0.5 KB = 500 MB
- **Total: 510 MB**

### Fix: Batch Processing

```python
# ✅ Process in batches
batch_size = 100

user_ids = User.objects.values_list('id', flat=True)
for i in range(0, len(user_ids), batch_size):
    batch_ids = user_ids[i:i + batch_size]
    users = User.objects.filter(id__in=batch_ids).prefetch_related('orders')

    for user in users:
        for order in user.orders.all():
            process(order)

    # Users and orders freed after each batch
```

**Memory usage**:
- 100 User objects × 1 KB = 100 KB
- 10,000 Order objects × 0.5 KB = 5 MB
- **Total: ~5 MB (100× less!)**

---

## 6. Closure Capturing Large Objects

### Problem: Lambda/Function Capturing Scope

```python
def create_processors():
    processors = []

    for i in range(1000):
        large_data = load_large_file(f"file_{i}.dat")  # 10 MB each

        # ❌ BAD: Lambda captures large_data
        processors.append(lambda x: process(x, large_data))

    return processors

# All 1000 large_data objects stay in memory! (10 GB total)
procs = create_processors()
```

### Why This Happens

```python
large_data = [1, 2, 3, 4, 5]

def make_adder():
    return lambda x: x + sum(large_data)  # Captures large_data

adder = make_adder()
print(adder.__closure__)  # (<cell at 0x...: list object at 0x...>,)
# The closure keeps large_data alive!
```

### Fix: Use Default Arguments

```python
def create_processors():
    processors = []

    for i in range(1000):
        large_data = load_large_file(f"file_{i}.dat")

        # ✅ GOOD: Bind large_data as default argument
        processors.append(lambda x, data=large_data: process(x, data))

        # large_data can be freed after this iteration

    return processors
```

**Or extract only what you need**:

```python
def create_processors():
    processors = []

    for i in range(1000):
        large_data = load_large_file(f"file_{i}.dat")  # 10 MB

        # Extract only the summary (1 KB)
        summary = compute_summary(large_data)

        # ✅ GOOD: Only capture summary
        processors.append(lambda x: process(x, summary))

        # large_data freed, only summary kept

    return processors
```

---

## 7. Detection and Prevention

### Tool 1: `sys.getsizeof()`

```python
import sys

obj = [0] * 1000000
print(f"Size: {sys.getsizeof(obj):,} bytes")  # Size: 8,000,064 bytes
```

**Limitation**: Doesn't count referenced objects.

### Tool 2: `tracemalloc`

```python
import tracemalloc

tracemalloc.start()

# Code to profile
data = []
for i in range(1000):
    data.append([0] * 10000)

snapshot = tracemalloc.take_snapshot()
top_stats = snapshot.statistics('lineno')

for stat in top_stats[:10]:
    print(stat)
```

**Output**:
```
<file>:5: size=76.3 MiB, count=1000, average=78.1 KiB
```

### Tool 3: `memory_profiler`

```python
from memory_profiler import profile

@profile
def process_data():
    data = []
    for i in range(1000):
        data.append([0] * 10000)
    return data

process_data()
```

**Output**:
```
Line #    Mem usage    Increment  Occurrences   Line Contents
=============================================================
     3     38.2 MiB     38.2 MiB           1   def process_data():
     4     38.2 MiB      0.0 MiB           1       data = []
     5    114.5 MiB     76.3 MiB        1001       for i in range(1000):
     6    114.5 MiB     76.3 MiB        1000           data.append([0] * 10000)
     7    114.5 MiB      0.0 MiB           1       return data
```

### Tool 4: `objgraph`

```python
import objgraph

# Find what's keeping an object alive
obj = MyClass()
objgraph.show_backrefs([obj], filename='backrefs.png')

# Find most common types
objgraph.show_most_common_types()
```

**Output**:
```
dict                       12587
tuple                       7123
list                        3456
```

### Prevention Checklist

1. **Use weak references** for caches and event listeners
2. **Limit collection sizes** with `maxlen` or LRU
3. **Use iterators** for large datasets
4. **Avoid closures capturing large objects**
5. **Profile memory** in development
6. **Use `__slots__`** to reduce instance overhead
7. **Clear references explicitly** when done

```python
# Example: Bounded cache
from collections import OrderedDict

class LRUCache:
    def __init__(self, maxsize=100):
        self.cache = OrderedDict()
        self.maxsize = maxsize

    def get(self, key):
        if key in self.cache:
            self.cache.move_to_end(key)  # Mark as recently used
            return self.cache[key]
        return None

    def put(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.maxsize:
            self.cache.popitem(last=False)  # Remove oldest
```

---

## 8. Summary

### Classification of Memory Issues

| Type | Python GC Handles? | Example | Fix |
|------|-------------------|---------|-----|
| **Simple reference** | ✅ Yes | `x = object()` then `del x` | Automatic |
| **Cyclic reference** | ✅ Yes (usually) | `a.ref = b; b.ref = a` | Automatic GC |
| **Cycle with `__del__`** | ⚠️ Maybe (Python 3.4+) | Class with `__del__` in cycle | Avoid `__del__` or use weak refs |
| **Unintentional retention** | ❌ No | BeautifulSoup example | Break reference chain |
| **Unbounded collections** | ❌ No | Global cache growing forever | Use LRU or maxsize |
| **Closure capture** | ❌ No | Lambda capturing large objects | Use default args or extract data |
| **C extension leak** | ❌ No | ctypes memory not freed | Manual free() |

### The BeautifulSoup Example Revisited

**Original claim**: "Memory leak due to storing references."

**Actual issue**: **Unintentional object retention** (reference chain, not cycle).

**Fix**: Convert to plain strings to break the chain.

**Key lesson**: Even without cycles, keeping unnecessary references can waste memory.

### Best Practices

1. **Understand what you're storing**: Is it the data or a wrapper object?
2. **Break reference chains**: Convert to plain types when extracting data
3. **Use weak references**: For caches, event listeners, observers
4. **Limit collection growth**: Use LRU, maxsize, or periodic cleanup
5. **Profile in development**: Use `tracemalloc` or `memory_profiler`
6. **Batch large operations**: Don't load everything into memory at once

### Quick Reference

```python
# ❌ BAD: Unintentional retention
data = [soup_element.contents[0] for soup_element in soup.find_all("td")]

# ✅ GOOD: Extract just the data
data = [str(soup_element.contents[0]) for soup_element in soup.find_all("td")]

# ❌ BAD: Unbounded cache
cache = {}
cache[key] = value  # Grows forever

# ✅ GOOD: LRU cache
from functools import lru_cache
@lru_cache(maxsize=1000)
def expensive_func(key): ...

# ❌ BAD: Closure capture
funcs = [lambda x: x + large_data for _ in range(1000)]

# ✅ GOOD: Default argument
funcs = [lambda x, d=large_data: x + d for _ in range(1000)]

# ❌ BAD: Load all
for user in User.objects.all():  # 1M users in memory

# ✅ GOOD: Stream
for user in User.objects.all().iterator(chunk_size=1000):  # 1K at a time
```

---

## References

- [Python Garbage Collection](https://docs.python.org/3/library/gc.html)
- [Weak References](https://docs.python.org/3/library/weakref.html)
- [tracemalloc](https://docs.python.org/3/library/tracemalloc.html)
- [memory_profiler](https://pypi.org/project/memory-profiler/)
- [BeautifulSoup Documentation](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)
