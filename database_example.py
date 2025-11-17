"""Example usage patterns for the Database class."""

from __future__ import annotations

import asyncio

from database import Database, DatabaseSettings


async def example_basic_operations() -> None:
    """Demonstrate basic database operations."""
    # Initialize connection pool
    settings = DatabaseSettings(
        host="localhost",
        port=5432,
        database="profilist",
        user="postgres",
        password="postgres",
    )
    await Database.aconnect(settings)

    try:
        # 1. Execute DDL (returns row count, typically 0 for DDL)
        await Database.aexecute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
            """
        )

        # 2. Insert with row count
        row_count = await Database.execute(
            "INSERT INTO users (name, email) VALUES (%s, %s)",
            ("Alice", "alice@example.com"),
        )
        print(f"Inserted {row_count} row(s)")

        # 3. Insert with RETURNING clause (get back the inserted row)
        result = await Database.aexecute_returning(
            "INSERT INTO users (name, email) VALUES (%s, %s) RETURNING *",
            ("Bob", "bob@example.com"),
        )
        print(f"Inserted user: {result[0]}")

        # 4. Batch insert
        users_data = [
            ("Charlie", "charlie@example.com"),
            ("Diana", "diana@example.com"),
            ("Eve", "eve@example.com"),
        ]
        row_count = await Database.aexecutemany(
            "INSERT INTO users (name, email) VALUES (%s, %s)",
            users_data,
        )
        print(f"Batch inserted {row_count} row(s)")

        # 5. Query all rows (returns list[DictRow])
        users = await Database.aquery("SELECT * FROM users ORDER BY id")
        print(f"\nAll users ({len(users)}):")
        for user in users:
            # DictRow works like a dict
            print(f"  {user['id']}: {user['name']} ({user['email']})")

        # 6. Query single row
        user = await Database.aquery_one(
            "SELECT * FROM users WHERE email = %s",
            ("alice@example.com",),
        )
        if user:
            print(f"\nFound user: {user['name']}")

        # 7. Update with row count
        row_count = await Database.execute(
            "UPDATE users SET name = %s WHERE email = %s",
            ("Alice Updated", "alice@example.com"),
        )
        print(f"\nUpdated {row_count} row(s)")

        # 8. Named parameters (dict-style)
        users = await Database.aquery(
            "SELECT * FROM users WHERE name LIKE %(pattern)s",
            {"pattern": "%Updated%"},
        )
        print(f"Users matching pattern: {len(users)}")

    finally:
        await Database.adisconnect()


async def example_transactions() -> None:
    """Demonstrate transaction management."""
    settings = DatabaseSettings()
    await Database.aconnect(settings)

    try:
        # Transaction: both operations succeed or both roll back
        async with Database.atransaction() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO users (name, email) VALUES (%s, %s)",
                    ("Frank", "frank@example.com"),
                )
                await cur.execute(
                    "INSERT INTO users (name, email) VALUES (%s, %s)",
                    ("Grace", "grace@example.com"),
                )
            # Auto-commits here if no exception

        print("Transaction committed successfully")

        # Transaction with rollback on error
        try:
            async with Database.atransaction() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "INSERT INTO users (name, email) VALUES (%s, %s)",
                        ("Henry", "henry@example.com"),
                    )
                    # This will fail due to duplicate email
                    await cur.execute(
                        "INSERT INTO users (name, email) VALUES (%s, %s)",
                        ("Isabel", "frank@example.com"),  # Duplicate!
                    )
        except Exception as e:
            print(f"Transaction rolled back: {e}")

    finally:
        await Database.adisconnect()


async def example_complex_operations() -> None:
    """Demonstrate complex multi-query operations."""
    settings = DatabaseSettings()
    await Database.aconnect(settings)

    try:
        # Use connection context for complex operations
        async with Database.aconnection() as conn:
            # Prepare statement and execute multiple times efficiently
            async with conn.cursor() as cur:
                await cur.execute("PREPARE insert_user AS INSERT INTO users (name, email) VALUES ($1, $2)")
                await cur.execute("EXECUTE insert_user (%s, %s)", ("John", "john@example.com"))
                await cur.execute("EXECUTE insert_user (%s, %s)", ("Jane", "jane@example.com"))
                await cur.execute("DEALLOCATE insert_user")

            # Multiple queries with same connection
            async with conn.cursor() as cur:
                await cur.execute("SELECT COUNT(*) as total FROM users")
                count = await cur.fetchone()
                print(f"Total users: {count[0]}")

    finally:
        await Database.adisconnect()


async def example_memory_tracking() -> None:
    """Example: Memory tracking experiment database."""
    settings = DatabaseSettings(database="memory_experiment")
    await Database.aconnect(settings)

    try:
        # Create schema for memory tracking
        await Database.aexecute(
            """
            CREATE TABLE IF NOT EXISTS memory_snapshots (
                id SERIAL PRIMARY KEY,
                experiment_id TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT NOW(),
                process_name TEXT,
                rss_bytes BIGINT,
                vms_bytes BIGINT,
                percent_memory REAL,
                metadata JSONB
            )
            """
        )

        # Record memory snapshot
        snapshot = await Database.execute_returning(
            """
            INSERT INTO memory_snapshots
                (experiment_id, process_name, rss_bytes, vms_bytes, percent_memory, metadata)
            VALUES
                (%s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                "exp_001",
                "python",
                1024 * 1024 * 512,  # 512 MB
                1024 * 1024 * 1024,  # 1 GB
                5.2,
                {"tags": ["baseline", "before_load"]},
            ),
        )
        print(f"Recorded snapshot: {snapshot[0]['id']}")

        # Query snapshots for analysis
        snapshots = await Database.query(
            """
            SELECT
                timestamp,
                rss_bytes / 1024 / 1024 as rss_mb,
                percent_memory
            FROM memory_snapshots
            WHERE experiment_id = %s
            ORDER BY timestamp
            """,
            ("exp_001",),
        )

        for snap in snapshots:
            print(f"  {snap['timestamp']}: {snap['rss_mb']:.2f} MB ({snap['percent_memory']}%)")

    finally:
        await Database.adisconnect()


if __name__ == "__main__":
    # Run examples
    print("=== Basic Operations ===")
    asyncio.run(example_basic_operations())

    print("\n=== Transactions ===")
    asyncio.run(example_transactions())

    print("\n=== Memory Tracking ===")
    asyncio.run(example_memory_tracking())
