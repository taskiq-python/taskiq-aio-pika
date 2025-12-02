"""
Example with two queues for different workers and one topic exchange.

It can be useful when you want to have two worker

How to run:
    1. Run worker for queue_1: taskiq worker examples.topic_with_two_queues:get_broker_for_queue_1 -w 1
    2. Run worker for queue_2: taskiq worker examples.topic_with_two_queues:get_broker_for_queue_2 -w 1
    3. Run broker to send a task: uv run examples/topic_with_two_queues.py --queue 1
    4. Optionally run broker to send a task to other queue: uv run examples/topic_with_two_queues.py --queue 2
"""

import argparse
import asyncio
import uuid

from aio_pika.abc import ExchangeType
from taskiq_redis import RedisAsyncResultBackend

from taskiq_aio_pika import AioPikaBroker, Exchange, Queue, QueueType

broker = AioPikaBroker(
    "amqp://guest:guest@localhost:5672/",
    exchange=Exchange(
        name="topic_exchange",
        type=ExchangeType.TOPIC,
    ),
    delay_queue=Queue(
        name="taskiq.delay",
        routing_key="queue1",
    ),  # send delayed messages to queue1
).with_result_backend(RedisAsyncResultBackend("redis://localhost:6379/0"))


@broker.task
async def add_one(value: int) -> int:
    return value + 1


queue_1 = Queue(
    name="queue1",
    type=QueueType.CLASSIC,
    durable=False,
)
queue_2 = Queue(
    name="queue2",
    type=QueueType.CLASSIC,
    durable=False,
)


def get_broker_for_queue_1() -> AioPikaBroker:
    print("This broker will listen to queue1")
    return broker.with_queue(queue_1)


def get_broker_for_queue_2() -> AioPikaBroker:
    print("This broker will listen to queue2")
    return broker.with_queue(queue_2)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--queue",
        choices=["1", "2"],
        required=True,
        help="Queue to send the task to.",
    )
    args = parser.parse_args()

    queue_name = queue_1.name if args.queue == "1" else queue_2.name

    broker.with_queues(
        queue_1,
        queue_2,
    )  # declare both queues to know about them during publishing
    await broker.startup()

    task = (
        await add_one.kicker()
        .with_labels(queue_name=queue_name)  # or it can be routing_key from queue_1
        .with_task_id(uuid.uuid4().hex)
        .kiq(2)
    )
    result = await task.wait_result(timeout=2)
    print(f"Task execution took: {result.execution_time} seconds.")
    if not result.is_err:
        print(f"Returned value: {result.return_value}")
    else:
        print("Error found while executing task.")
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
