import asyncio
from collections.abc import AsyncGenerator
from typing import Any, TypeVar

from typing_extensions import Sentinel

_T = TypeVar("_T")
_SENTINEL = Sentinel("_SENTINEL")


async def merge_async_iterables(
    *iterables: AsyncGenerator[_T, None],
) -> AsyncGenerator[_T, None]:
    """
    Merge multiple async generators into a single one.

    Items are yielded as soon as they're produced by any of the source generators, in the order they arrive.

    :param iterables: async generators to merge.
    :yields: items produced by any of the source generators.
    """
    queue: asyncio.Queue[Any] = asyncio.Queue()

    async def _pump(iterable: AsyncGenerator[_T, None]) -> None:
        try:
            async for item in iterable:
                await queue.put(item)
        except BaseException as exc:
            await queue.put(exc)
        else:
            await queue.put(_SENTINEL)

    tasks = [asyncio.ensure_future(_pump(iterable)) for iterable in iterables]
    remaining = len(tasks)
    try:
        while remaining:
            item = await queue.get()
            if item is _SENTINEL:
                remaining -= 1
            elif isinstance(item, BaseException):
                raise item
            else:
                yield item
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
