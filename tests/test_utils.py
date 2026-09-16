import asyncio
from collections.abc import AsyncGenerator

import pytest

from taskiq_aio_pika.utils import merge_async_iterables


async def _gen(*items: int, delay: float = 0.0) -> AsyncGenerator[int, None]:
    for item in items:
        if delay:
            await asyncio.sleep(delay)
        yield item


async def test_when_multiple_generators_passed__then_all_items_are_yielded() -> None:
    result = [
        item
        async for item in merge_async_iterables(
            _gen(1, 2, 3),
            _gen(4, 5),
        )
    ]

    assert sorted(result) == [1, 2, 3, 4, 5]


async def test_when_single_generator_passed__then_its_items_are_yielded_in_order() -> (
    None
):
    result = [item async for item in merge_async_iterables(_gen(1, 2, 3))]

    assert result == [1, 2, 3]


async def test_when_no_generators_passed__then_nothing_is_yielded() -> None:
    result = [item async for item in merge_async_iterables()]

    assert result == []


async def test_when_one_generator_raises__then_exception_is_propagated() -> None:
    async def failing_gen() -> AsyncGenerator[int, None]:
        yield 1
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        async for _ in merge_async_iterables(failing_gen(), _gen(2, delay=1)):
            pass


async def test_when_one_generator_raises__then_other_generators_are_cancelled() -> None:
    other_was_cancelled = False

    async def slow_gen() -> AsyncGenerator[int, None]:
        nonlocal other_was_cancelled
        try:
            await asyncio.sleep(10)
            yield 1
        except asyncio.CancelledError:
            other_was_cancelled = True
            raise

    async def failing_gen() -> AsyncGenerator[int, None]:
        yield 1
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        async for _ in merge_async_iterables(failing_gen(), slow_gen()):
            pass

    assert other_was_cancelled


async def test_when_consumer_stops_early__then_remaining_generators_are_cancelled() -> (
    None
):
    other_was_cancelled = False

    async def slow_gen() -> AsyncGenerator[int, None]:
        nonlocal other_was_cancelled
        try:
            await asyncio.sleep(10)
            yield 1
        except asyncio.CancelledError:
            other_was_cancelled = True
            raise

    merged = merge_async_iterables(_gen(1), slow_gen())
    try:
        async for _ in merged:
            break
    finally:
        await merged.aclose()

    assert other_was_cancelled
