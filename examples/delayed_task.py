"""
Example of delayed task execution using Taskiq with AioPika broker.

How to run:
    1. Run worker: taskiq worker examples.delayed_task:broker -w 1
    2. Run broker: uv run examples/delayed_task.py
"""

import asyncio

from taskiq_redis import RedisAsyncResultBackend

from taskiq_aio_pika import AioPikaBroker

broker = AioPikaBroker(
    "amqp://guest:guest@localhost:5672/",
).with_result_backend(RedisAsyncResultBackend("redis://localhost:6379/0"))


@broker.task
async def add_one(value: int) -> int:
    return value + 1


async def main() -> None:
    await broker.startup()
    # Send the task to the broker.
    task = await add_one.kicker().with_labels(delay=2).kiq(1)
    print("Task sent with 2 seconds delay.")
    # Wait for the result.
    result = await task.wait_result(timeout=3)
    print(f"Task execution took: {result.execution_time} seconds.")
    if not result.is_err:
        print(f"Returned value: {result.return_value}")
    else:
        print("Error found while executing task.")
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
