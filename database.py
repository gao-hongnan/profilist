"""Canonical industry-standard async database interface using psycopg connection pooling."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from psycopg import AsyncConnection
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool


class DatabaseSettings:
    """Database configuration settings."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "profilist",
        user: str = "postgres",
        password: str = "postgres",
        pool_min_size: int = 2,
        pool_max_size: int = 10,
        pool_timeout: float = 30.0,
    ) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.POOL_MIN_SIZE = pool_min_size
        self.POOL_MAX_SIZE = pool_max_size
        self.POOL_TIMEOUT = pool_timeout

    @property
    def conninfo(self) -> str:
        """Generate PostgreSQL connection string."""
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )


class Database:
    """Thread-safe async database connection pool manager.

    Provides industry-standard operations:
    - Connection pooling
    - Transaction management
    - Parameterized queries
    - Batch operations
    """

    _pool: AsyncConnectionPool | None = None

    @classmethod
    async def connect(cls, settings: DatabaseSettings) -> None:
        """Initialize connection pool.

        Args:
            settings: Database configuration settings
        """
        if cls._pool is not None:
            return

        cls._pool = AsyncConnectionPool(
            conninfo=settings.conninfo,
            min_size=settings.POOL_MIN_SIZE,
            max_size=settings.POOL_MAX_SIZE,
            timeout=settings.POOL_TIMEOUT,
            open=True,
        )

        await cls._pool.wait()

    @classmethod
    async def disconnect(cls) -> None:
        """Close connection pool and cleanup resources."""
        if cls._pool is not None:
            await cls._pool.close()
            cls._pool = None

    @classmethod
    async def query(
        cls,
        sql: str,
        params: tuple[Any, ...] | dict[str, Any] | None = None,
    ) -> list[DictRow]:
        """Execute SELECT query and return all rows as dictionaries.

        Args:
            sql: SQL query string
            params: Query parameters (tuple for positional, dict for named)

        Returns:
            List of rows as DictRow objects (dict-like)

        Raises:
            RuntimeError: If database not connected
        """
        if cls._pool is None:
            msg = "Database not connected. Call Database.connect() first."
            raise RuntimeError(msg)

        async with cls._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params or ())
            return await cur.fetchall()

    @classmethod
    async def query_one(
        cls,
        sql: str,
        params: tuple[Any, ...] | dict[str, Any] | None = None,
    ) -> DictRow | None:
        """Execute SELECT query and return single row or None.

        Args:
            sql: SQL query string
            params: Query parameters

        Returns:
            Single row as DictRow or None if no results

        Raises:
            RuntimeError: If database not connected
        """
        if cls._pool is None:
            msg = "Database not connected. Call Database.connect() first."
            raise RuntimeError(msg)

        async with cls._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params or ())
            return await cur.fetchone()

    @classmethod
    async def execute(
        cls,
        sql: str,
        params: tuple[Any, ...] | dict[str, Any] | None = None,
    ) -> int:
        """Execute INSERT/UPDATE/DELETE and return affected row count.

        Args:
            sql: SQL statement
            params: Query parameters

        Returns:
            Number of affected rows

        Raises:
            RuntimeError: If database not connected
        """
        if cls._pool is None:
            msg = "Database not connected. Call Database.connect() first."
            raise RuntimeError(msg)

        async with cls._pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(sql, params or ())
            return cur.rowcount

    @classmethod
    async def execute_returning(
        cls,
        sql: str,
        params: tuple[Any, ...] | dict[str, Any] | None = None,
    ) -> list[DictRow]:
        """Execute INSERT/UPDATE/DELETE with RETURNING clause.

        Args:
            sql: SQL statement with RETURNING clause
            params: Query parameters

        Returns:
            Returned rows as DictRow objects

        Raises:
            RuntimeError: If database not connected
        """
        if cls._pool is None:
            msg = "Database not connected. Call Database.connect() first."
            raise RuntimeError(msg)

        async with cls._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params or ())
            return await cur.fetchall()

    @classmethod
    async def executemany(
        cls,
        sql: str,
        params_seq: list[tuple[Any, ...]] | list[dict[str, Any]],
    ) -> int:
        """Execute batch INSERT/UPDATE/DELETE operations.

        Args:
            sql: SQL statement
            params_seq: Sequence of parameter sets

        Returns:
            Total number of affected rows

        Raises:
            RuntimeError: If database not connected
        """
        if cls._pool is None:
            msg = "Database not connected. Call Database.connect() first."
            raise RuntimeError(msg)

        async with cls._pool.connection() as conn, conn.cursor() as cur:
            await cur.executemany(sql, params_seq)
            return cur.rowcount

    @classmethod
    @asynccontextmanager
    async def transaction(cls) -> AsyncIterator[AsyncConnection]:
        """Context manager for database transactions.

        Automatically commits on success, rolls back on exception.

        Usage:
            async with Database.transaction() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("INSERT ...")
                    await cur.execute("UPDATE ...")
                # Auto-commit if no exception

        Yields:
            Database connection with transaction

        Raises:
            RuntimeError: If database not connected
        """
        if cls._pool is None:
            msg = "Database not connected. Call Database.connect() first."
            raise RuntimeError(msg)

        async with cls._pool.connection() as conn:
            async with conn.transaction():
                yield conn

    @classmethod
    @asynccontextmanager
    async def connection(cls) -> AsyncIterator[AsyncConnection]:
        """Get a connection from the pool for complex operations.

        Usage:
            async with Database.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute("SELECT ...")
                    results = await cur.fetchall()

        Yields:
            Database connection from pool

        Raises:
            RuntimeError: If database not connected
        """
        if cls._pool is None:
            msg = "Database not connected. Call Database.connect() first."
            raise RuntimeError(msg)

        async with cls._pool.connection() as conn:
            yield conn
