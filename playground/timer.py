from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any, Callable

from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from profilist.timer import Timer, timer

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True, markup=True)],
)

console = Console()


def section(title: str) -> None:
    """Print a section header."""
    console.print(f"\n[bold cyan]{'=' * 60}[/bold cyan]")
    console.print(f"[bold yellow]{title}[/bold yellow]")
    console.print(f"[bold cyan]{'=' * 60}[/bold cyan]\n")


def demo_basic_timer() -> None:
    """Demonstrate basic Timer usage."""
    section("1. Basic Timer Context Manager")

    console.print("[green]Standard usage with logging:[/green]")
    with Timer(name="Database connection"):
        time.sleep(0.15)
        console.print("  Establishing connection to PostgreSQL...")

    console.print("\n[green]Silent mode - capture time without logging:[/green]")
    with Timer(name="Cache lookup", silent=True) as t:
        time.sleep(0.02)
        console.print("  Checking Redis cache...")
    console.print(
        f"  [yellow]Cache lookup time: {t.elapsed_seconds:.4f} seconds[/yellow]"
    )


def demo_nested_timers() -> None:
    """Demonstrate nested timer usage for complex operations."""
    section("2. Nested Timers - ETL Pipeline")

    with Timer(name="ETL Pipeline"):
        console.print("Starting ETL pipeline...")

        with Timer(name="  Extract phase"):
            console.print("  Extracting data from source systems...")
            time.sleep(0.25)
            console.print("    - Connected to data warehouse")
            time.sleep(0.15)
            console.print("    - Retrieved 10,000 records")

        with Timer(name="  Transform phase"):
            console.print("  Transforming data...")
            time.sleep(0.1)
            console.print("    - Applied business rules")
            time.sleep(0.08)
            console.print("    - Validated data integrity")

        with Timer(name="  Load phase"):
            console.print("  Loading data to destination...")
            time.sleep(0.12)
            console.print("    - Batch inserted records")
            time.sleep(0.05)
            console.print("    - Updated indexes")


@timer(name="execute_database_query")
def execute_query(_query: str, timeout: float = 0.1) -> dict[str, Any]:
    """Simulate database query execution."""
    time.sleep(timeout)
    return {
        "rows_affected": random.randint(1, 1000),
        "execution_time": timeout,
        "query_plan": "Index Scan",
    }


@timer(name="process_batch", silent=True)
def process_batch(batch_size: int) -> int:
    """Process a batch of items silently."""
    processing_time = batch_size * 0.0001
    time.sleep(processing_time)
    return batch_size


def demo_sync_decorator() -> None:
    """Demonstrate @timer decorator on sync functions."""
    section("3. Database Operations with @timer")

    console.print("[green]Executing database queries:[/green]")
    result = execute_query("SELECT * FROM users WHERE active = true", 0.08)
    console.print(f"  Query result: {result['rows_affected']} rows affected")

    console.print("\n[green]Batch processing (silent mode):[/green]")
    batch_sizes = [100, 500, 1000, 5000]
    total_processed = 0
    for size in batch_sizes:
        processed = process_batch(size)
        total_processed += processed
        console.print(f"  Processed batch of {processed} items")
    console.print(f"  [yellow]Total processed: {total_processed} items[/yellow]")


@timer(name="api_request")
async def make_api_request(endpoint: str, delay: float) -> dict[str, Any]:
    """Simulate API request with retry logic."""
    await asyncio.sleep(delay)
    return {
        "endpoint": endpoint,
        "status": 200,
        "response_time": delay * 1000,
        "data": {"items": random.randint(10, 100)},
    }


@timer(name="cache_operation")
async def cache_operation(operation: str, key: str) -> tuple[str, bool]:
    """Simulate cache operations."""
    operations = {
        "get": 0.005,
        "set": 0.008,
        "delete": 0.003,
        "expire": 0.002,
    }
    await asyncio.sleep(operations.get(operation, 0.01))
    hit = random.choice([True, False]) if operation == "get" else True
    return key, hit


async def demo_async_decorator() -> None:
    """Demonstrate @timer decorator on async functions."""
    section("4. Async Operations - API & Cache")

    console.print("[green]Making API requests:[/green]")
    endpoints = [
        "/api/users",
        "/api/products",
        "/api/orders",
    ]

    for endpoint in endpoints:
        result = await make_api_request(endpoint, random.uniform(0.05, 0.15))
        console.print(
            f"  {endpoint}: {result['data']['items']} items ({result['response_time']:.0f}ms)"
        )

    console.print("\n[green]Concurrent cache operations:[/green]")
    cache_tasks = [cache_operation("get", f"user:{i}") for i in range(5)]
    results = await asyncio.gather(*cache_tasks)

    hits = sum(1 for _, hit in results if hit)
    console.print(
        f"  Cache hits: {hits}/{len(results)} ({hits / len(results) * 100:.0f}% hit rate)"
    )


def demo_performance_comparison() -> None:
    """Compare performance of different data processing approaches."""
    section("5. Performance Comparison - Data Processing")

    data_size = 50000

    approaches: dict[str, Callable[[], Any]] = {
        "List Comprehension": lambda: [x**2 + x / 2 for x in range(data_size)],
        "Generator Expression": lambda: [x**2 + x / 2 for x in range(data_size)],
        "Map with Lambda": lambda: [x**2 + x / 2 for x in range(data_size)],
        "Traditional Loop": lambda: process_with_loop(data_size),
        "Filter + Map Combo": lambda: [x**2 for x in range(data_size) if x % 2 == 0],
    }

    def process_with_loop(size: int) -> list[float]:
        return [i**2 + i / 2 for i in range(size)]

    table = Table(title="Data Processing Performance")
    table.add_column("Approach", style="cyan")
    table.add_column("Time (seconds)", style="yellow")
    table.add_column("Relative", style="green")

    times: list[tuple[str, float]] = []

    for name, func in approaches.items():
        with Timer(silent=True) as t:
            for _ in range(10):
                _ = func()
        times.append((name, t.elapsed_seconds))

    min_time = min(t for _, t in times)

    for name, elapsed in sorted(times, key=lambda x: x[1]):
        relative = f"{elapsed / min_time:.2f}x"
        table.add_row(name, f"{elapsed:.6f}", relative)

    console.print(table)


def demo_practical_example() -> None:
    """Demonstrate practical usage scenario: API Data Pipeline."""
    section("6. Real-World Example: API Data Processing Pipeline")

    @timer(name="fetch_from_api")
    def fetch_data() -> list[dict[str, Any]]:
        """Simulate fetching data from external API."""
        time.sleep(0.3)
        return [
            {
                "id": i,
                "value": random.randint(100, 1000),
                "status": random.choice(["active", "pending"]),
            }
            for i in range(1000)
        ]

    @timer(name="validate_records")
    def validate_data(
        data: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Validate and split data into valid and invalid records."""
        time.sleep(0.08)
        valid = [d for d in data if d["status"] == "active"]
        invalid = [d for d in data if d["status"] != "active"]
        return valid, invalid

    @timer(name="enrich_data")
    def enrich_data(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Enrich data with additional fields."""
        time.sleep(0.12)
        for record in data:
            record["processed_at"] = time.time()
            record["score"] = record["value"] * 1.2
        return data

    @timer(name="write_to_database", silent=True)
    def save_to_database(data: list[dict[str, Any]]) -> int:
        """Simulate database write operation."""
        time.sleep(0.2)
        return len(data)

    @timer(name="update_cache")
    def update_cache(_data: list[dict[str, Any]]) -> None:
        """Update cache with processed data."""
        time.sleep(0.05)

    with (
        Timer(name="Complete API Pipeline"),
        Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress,
    ):
        task = progress.add_task("Fetching data from API...", total=None)
        raw_data = fetch_data()

        progress.update(task, description="Validating records...")
        valid_data, invalid_data = validate_data(raw_data)

        progress.update(task, description="Enriching data...")
        enriched_data = enrich_data(valid_data)

        progress.update(task, description="Saving to database...")
        saved_count = save_to_database(enriched_data)

        progress.update(task, description="Updating cache...")
        update_cache(enriched_data)

        console.print(f"  [green]✓ Processed {saved_count} valid records[/green]")
        console.print(
            f"  [yellow]⚠ Skipped {len(invalid_data)} invalid records[/yellow]"
        )


def demo_collecting_metrics() -> None:
    """Demonstrate collecting timing metrics for monitoring."""
    section("7. Performance Monitoring - Request Latencies")

    console.print("[green]Simulating API endpoint monitoring:[/green]\n")

    endpoints = [
        ("GET /api/users", 0.02, 0.05),
        ("GET /api/products", 0.03, 0.08),
        ("POST /api/orders", 0.05, 0.15),
        ("GET /api/health", 0.001, 0.005),
    ]

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        for endpoint, min_time, max_time in endpoints:
            times: list[float] = []
            progress.add_task(f"Monitoring {endpoint}...", total=None)

            for _ in range(20):
                with Timer(silent=True) as t:
                    time.sleep(random.uniform(min_time, max_time))
                    if random.random() < 0.05:  # 5% error rate
                        time.sleep(random.uniform(0.5, 1.0))
                times.append(t.elapsed_seconds)

            p50 = sorted(times)[len(times) // 2]
            p95 = sorted(times)[int(len(times) * 0.95)]
            p99 = sorted(times)[int(len(times) * 0.99)]

            stats = Panel(
                f"[cyan]Endpoint:[/cyan] {endpoint}\n"
                f"[cyan]Requests:[/cyan] {len(times)}\n"
                f"[cyan]P50 latency:[/cyan] {p50 * 1000:.1f}ms\n"
                f"[cyan]P95 latency:[/cyan] {p95 * 1000:.1f}ms\n"
                f"[cyan]P99 latency:[/cyan] {p99 * 1000:.1f}ms\n"
                f"[cyan]Avg latency:[/cyan] {sum(times) / len(times) * 1000:.1f}ms",
                title=f"[bold yellow]{endpoint} Metrics[/bold yellow]",
                border_style="green" if p95 < 0.1 else "yellow" if p95 < 0.5 else "red",
            )
            console.print(stats)


async def demo_advanced_async() -> None:
    """Demonstrate advanced async patterns with timing."""
    section("8. Advanced Async - Concurrent Task Processing")

    @timer(name="task_worker")
    async def process_task(task_id: int, complexity: float) -> dict[str, Any]:
        """Simulate processing a task with variable complexity."""
        await asyncio.sleep(complexity)
        return {
            "task_id": task_id,
            "status": "completed",
            "processing_time": complexity * 1000,
        }

    console.print("[green]Processing task queue with worker pool:[/green]\n")

    # Simulate task queue
    tasks = [(i, random.uniform(0.01, 0.1)) for i in range(15)]

    # Process in batches (simulating worker pool)
    batch_size = 5
    for i in range(0, len(tasks), batch_size):
        batch = tasks[i : i + batch_size]
        console.print(f"  Processing batch {i // batch_size + 1}...")

        with Timer(name=f"  Batch {i // batch_size + 1}", silent=False):
            batch_tasks = [
                process_task(task_id, complexity) for task_id, complexity in batch
            ]
            results = await asyncio.gather(*batch_tasks)

        avg_time = sum(r["processing_time"] for r in results) / len(results)
        console.print(f"    Average task time: {avg_time:.1f}ms")


async def main() -> None:
    """Run all demos."""
    console.print(
        Panel(
            "[bold cyan]Professional Timer Demo Suite[/bold cyan]\n"
            "Real-world examples of performance monitoring and optimization",
            border_style="blue",
        )
    )

    demo_basic_timer()
    demo_nested_timers()
    demo_sync_decorator()
    await demo_async_decorator()
    demo_performance_comparison()
    demo_practical_example()
    demo_collecting_metrics()
    await demo_advanced_async()

    console.print(
        Panel(
            "[bold green]✓ Demo Complete![/bold green]\n"
            "The Timer class helps monitor and optimize real-world application performance.",
            border_style="green",
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
