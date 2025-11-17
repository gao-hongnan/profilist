# Database Implementation Corrections

## Critical Issues Fixed

### 1. **Incorrect Type Hints** ❌ → ✅

**Wrong:**
```python
async def query(cls, sql: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
```

**Correct:**
```python
async def query(cls, sql: str, params: tuple[Any, ...] | dict[str, Any] | None = None) -> list[DictRow]:
```

**Why:**
- `cur.fetchall()` with `row_factory=dict_row` returns `list[DictRow]`, not `list[dict]`
- `DictRow` is a dict-like object but not a plain dict
- Type checkers (mypy, pyright) will catch this error
- Params can be dict for named parameters (e.g., `%(name)s`)

### 2. **No Transaction Support** ❌ → ✅

**Missing:**
```python
# No way to run multiple queries atomically
```

**Added:**
```python
@asynccontextmanager
async def transaction(cls) -> AsyncIterator[AsyncConnection]:
    """Auto-commit on success, auto-rollback on exception."""
    async with cls._pool.connection() as conn:
        async with conn.transaction():
            yield conn
```

**Why:**
- Industry standard requires ACID transaction support
- Critical for data consistency
- Prevents partial updates on errors

**Usage:**
```python
async with Database.transaction() as conn:
    async with conn.cursor() as cur:
        await cur.execute("INSERT INTO accounts ...")
        await cur.execute("UPDATE balance ...")
    # Auto-commits if no exception raised
```

### 3. **execute() Returns Nothing** ❌ → ✅

**Wrong:**
```python
async def execute(cls, sql: str, params: tuple[Any, ...] | None = None) -> None:
    async with cls._pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(sql, params or ())
```

**Correct:**
```python
async def execute(cls, sql: str, params: tuple[Any, ...] | dict[str, Any] | None = None) -> int:
    async with cls._pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(sql, params or ())
        return cur.rowcount  # Number of affected rows
```

**Why:**
- Need to know how many rows were affected (INSERT/UPDATE/DELETE)
- Standard practice: return affected row count
- Enables verification of operations

### 4. **No RETURNING Clause Support** ❌ → ✅

**Added:**
```python
async def execute_returning(
    cls, sql: str, params: tuple[Any, ...] | dict[str, Any] | None = None
) -> list[DictRow]:
    """Execute INSERT/UPDATE/DELETE with RETURNING clause."""
    async with cls._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql, params or ())
        return await cur.fetchall()
```

**Why:**
- PostgreSQL's RETURNING clause is extremely useful
- Get generated IDs, timestamps, computed values in one query
- Avoids race conditions from separate SELECT

**Usage:**
```python
result = await Database.execute_returning(
    "INSERT INTO users (name) VALUES (%s) RETURNING id, created_at",
    ("Alice",)
)
user_id = result[0]['id']
```

### 5. **No Batch Operations** ❌ → ✅

**Added:**
```python
async def executemany(
    cls, sql: str, params_seq: list[tuple[Any, ...]] | list[dict[str, Any]]
) -> int:
    """Execute batch operations efficiently."""
    async with cls._pool.connection() as conn, conn.cursor() as cur:
        await cur.executemany(sql, params_seq)
        return cur.rowcount
```

**Why:**
- Massive performance improvement for bulk inserts
- Industry standard for batch operations
- Reduces round-trips to database

**Performance:**
```python
# Slow: 1000 round-trips
for user in users:
    await Database.execute("INSERT INTO users ...", user)

# Fast: 1 round-trip
await Database.executemany("INSERT INTO users ...", users)
```

### 6. **No Single Row Query** ❌ → ✅

**Added:**
```python
async def query_one(
    cls, sql: str, params: tuple[Any, ...] | dict[str, Any] | None = None
) -> DictRow | None:
    """Fetch single row or None."""
    async with cls._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql, params or ())
        return await cur.fetchone()
```

**Why:**
- Common pattern: expect zero or one result
- More efficient than `fetchall()[0]`
- Clearer intent in code

### 7. **No Connection Context Manager** ❌ → ✅

**Added:**
```python
@asynccontextmanager
async def connection(cls) -> AsyncIterator[AsyncConnection]:
    """Get connection for complex multi-query operations."""
    async with cls._pool.connection() as conn:
        yield conn
```

**Why:**
- Some operations need multiple queries on same connection
- Prepared statements, cursors, complex logic
- More control when needed

## Industry Standard Checklist

✅ **Connection pooling** - Efficient resource management
✅ **Async/await** - Non-blocking I/O
✅ **Parameterized queries** - SQL injection prevention
✅ **Transaction support** - ACID guarantees
✅ **Batch operations** - Performance optimization
✅ **Correct type hints** - Type safety and IDE support
✅ **RETURNING clause** - PostgreSQL-specific optimization
✅ **Row count returns** - Operation verification
✅ **Context managers** - Automatic resource cleanup
✅ **Named parameters** - Dict-style parameter passing
✅ **Single-row queries** - Common pattern support

## Type Hint Correctness

### DictRow vs dict[str, Any]

```python
from psycopg.rows import DictRow

# DictRow is dict-like but NOT a dict subclass
row: DictRow = {"id": 1, "name": "Alice"}  # Type error!

# Correct usage:
rows: list[DictRow] = await Database.query("SELECT ...")
row = rows[0]
user_id: int = row["id"]  # ✅ Works like dict
name: str = row["name"]    # ✅ Dict-like access
```

### Why This Matters:

1. **Type checker compliance**: mypy/pyright will catch errors
2. **IDE autocomplete**: Better IntelliSense support
3. **Runtime behavior**: DictRow has additional methods
4. **Documentation**: Clear what type is actually returned

## Memory Tracking Use Case

For your experiment, this implementation provides:

1. **Efficient bulk inserts** for frequent memory snapshots
2. **Transactions** for consistent multi-table updates
3. **RETURNING clauses** to get snapshot IDs immediately
4. **Connection pooling** to handle concurrent measurements
5. **Type safety** to prevent bugs in data collection

Example:
```python
# Record 1000 memory snapshots efficiently
snapshots = [(exp_id, rss, vms, pct) for ...]
await Database.executemany(
    "INSERT INTO snapshots (exp_id, rss, vms, pct) VALUES (%s, %s, %s, %s)",
    snapshots
)
```

## Minimal but Correct

This implementation follows the principle: **"As simple as possible, but no simpler"**

- ✅ All essential operations
- ✅ Industry-standard patterns
- ✅ Type-safe
- ✅ Production-ready
- ❌ No ORM overhead
- ❌ No unnecessary abstractions
- ❌ No bloat

Total: ~230 lines for a complete, correct async database interface.
