import aiormq
import pytest
from aio_pika import Channel
from aio_pika.exceptions import QueueEmpty
from taskiq import BrokerMessage

from taskiq_aio_pika import AioPikaBroker
from taskiq_aio_pika.exchange import Exchange
from taskiq_aio_pika.queue import Queue, QueueType
from tests.conftest import _cleanup_amqp_resources


async def test_when_delivery_limit_exceeded__message_is_dead_lettered(
    amqp_url: str,
    test_channel: Channel,
    queue_name: str,
    dead_queue_name: str,
    exchange_name: str,
) -> None:
    # given
    broker = AioPikaBroker(
        url=amqp_url,
        exchange=Exchange(name=exchange_name, declare=True),
        dead_letter_queue=Queue(name=dead_queue_name, declare=True),
        task_queues=[
            Queue(
                name=queue_name,
                declare=True,
                type=QueueType.QUORUM,
                delivery_limit=2,
            ),
        ],
    )

    try:
        await broker.startup()
        main_queue = await test_channel.get_queue(queue_name)
        dead_letter_queue = await test_channel.get_queue(dead_queue_name)

        await broker.kick(
            BrokerMessage(
                task_id="1",
                task_name="name",
                message=b"poison",
                labels={},
            ),
        )

        # when
        for _ in range(
            3,
        ):  # simulate a consumer that never acks the message, so it gets requeued
            message = await main_queue.get()
            await message.nack(requeue=True)

        # then
        with pytest.raises(QueueEmpty):
            await main_queue.get()

        dead_lettered_message = await dead_letter_queue.get()
        assert dead_lettered_message.body == b"poison"
    finally:
        await broker.shutdown()
        await _cleanup_amqp_resources(
            amqp_url,
            [exchange_name],
            [queue_name, dead_queue_name],
        )


async def test_when_delivery_limit_set_on_classic_queue__warning_is_logged(
    amqp_url: str,
    queue_name: str,
    dead_queue_name: str,
    exchange_name: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # given
    broker = AioPikaBroker(
        url=amqp_url,
        exchange=Exchange(name=exchange_name, declare=True),
        dead_letter_queue=Queue(name=dead_queue_name, declare=True),
        task_queues=[
            Queue(
                name=queue_name,
                declare=True,
                type=QueueType.CLASSIC,
                delivery_limit=2,
            ),
        ],
    )

    try:
        # when
        with (
            caplog.at_level("WARNING", logger="taskiq.aio_pika_broker"),
            pytest.raises(aiormq.exceptions.ChannelPreconditionFailed),
        ):
            await broker.startup()

        # then
        assert any(
            "delivery_limit" in record.message and queue_name in record.message
            for record in caplog.records
        )
    finally:
        await broker.shutdown()
        await _cleanup_amqp_resources(
            amqp_url,
            [exchange_name],
            [queue_name, dead_queue_name],
        )
