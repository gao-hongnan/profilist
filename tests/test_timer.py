from __future__ import annotations

import asyncio
import logging
from unittest.mock import MagicMock, patch

import pytest

from profilist.timer import Timer, timer


@pytest.mark.unit
class TestTimer:
    def test_timer_measures_elapsed_time(self) -> None:
        with patch("profilist.timer.timeit.default_timer") as mock_timer:
            mock_timer.side_effect = [100.0, 100.5]

            with Timer(name="test_operation") as t:
                pass

            assert t.elapsed_seconds == 0.5

    def test_timer_silent_mode(self) -> None:
        with patch.object(logging.getLogger("profilist.timer"), "info") as mock_log:
            with Timer(name="test_operation", silent=True) as t:
                pass

            mock_log.assert_not_called()
            assert t.elapsed_seconds >= 0

    @pytest.mark.asyncio
    async def test_timer_async_context(self) -> None:
        with patch("profilist.timer.timeit.default_timer") as mock_timer:
            mock_timer.side_effect = [200.0, 200.3]

            async with Timer(name="async_operation") as t:
                await asyncio.sleep(0.001)

            assert abs(t.elapsed_seconds - 0.3) < 0.001

    def test_timer_with_exception(self) -> None:
        with patch("profilist.timer.timeit.default_timer") as mock_timer:
            mock_timer.side_effect = [300.0, 300.2]

            with pytest.raises(ValueError), Timer(name="failing_operation") as t:
                raise ValueError("Test error")

            assert abs(t.elapsed_seconds - 0.2) < 0.001


@pytest.mark.unit
class TestTimerDecorator:
    def test_decorator_sync_function(self) -> None:
        mock_func = MagicMock(return_value="result")
        mock_func.__name__ = "test_func"

        decorated = timer()(mock_func)
        result = decorated("arg1", key="value")

        mock_func.assert_called_once_with("arg1", key="value")
        assert result == "result"

    def test_decorator_sync_preserves_return_value(self) -> None:
        @timer(silent=True)
        def add_numbers(a: int, b: int) -> int:
            return a + b

        result = add_numbers(5, 3)
        assert result == 8

    @pytest.mark.asyncio
    async def test_decorator_async_function(self) -> None:
        call_args: list[tuple[int, int]] = []

        @timer(silent=True)
        async def async_multiply(x: int, y: int) -> int:
            call_args.append((x, y))
            await asyncio.sleep(0.001)
            return x * y

        result = await async_multiply(4, 7)

        assert call_args == [(4, 7)]
        assert result == 28

    @pytest.mark.asyncio
    async def test_decorator_async_preserves_return_value(self) -> None:
        @timer(name="async_concat", silent=True)
        async def concat_strings(s1: str, s2: str) -> str:
            await asyncio.sleep(0.001)
            return f"{s1}-{s2}"

        result = await concat_strings("hello", "world")
        assert result == "hello-world"

    def test_decorator_warns_on_generator_function(self) -> None:
        with pytest.warns(UserWarning) as warning_records:

            @timer(silent=True)
            def my_generator(n: int):
                for i in range(n):
                    yield i * 2

        assert len(warning_records) == 1
        warning_msg = str(warning_records[0].message)
        assert "does not properly time generator function 'my_generator'" in warning_msg
        assert "only measure generator object creation" in warning_msg

        gen = my_generator(3)
        values = list(gen)
        assert values == [0, 2, 4]

    @pytest.mark.asyncio
    async def test_decorator_warns_on_async_generator_function(self) -> None:
        with pytest.warns(UserWarning) as warning_records:

            @timer(silent=True)
            async def my_async_generator(n: int):
                for i in range(n):
                    await asyncio.sleep(0.001)
                    yield i * 3

        assert len(warning_records) == 1
        warning_msg = str(warning_records[0].message)
        assert "does not properly time generator function 'my_async_generator'" in warning_msg
        assert "only measure generator object creation" in warning_msg

        async_gen = my_async_generator(2)
        values = [val async for val in async_gen]
        assert values == [0, 3]
