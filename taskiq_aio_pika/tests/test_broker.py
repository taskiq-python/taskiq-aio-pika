import asyncio
import uuid

import pytest
from aio_pika import Channel, Message
from aio_pika.abc import AbstractExchange, AbstractQueue
from aio_pika.exceptions import QueueEmpty
from mock import AsyncMock
from taskiq import BrokerMessage

from taskiq_aio_pika.broker import AioPikaBroker


@pytest.mark.anyio
async def test_kick_success(broker: AioPikaBroker) -> None:
    """
    Test that messages are published and read correctly.

    We kick the message and then try to listen to the queue,
    and check that message we got is the same as we sent.

    :param broker: current broker.
    """
    task_id = uuid.uuid4().hex
    task_name = uuid.uuid4().hex

    sent = BrokerMessage(
        task_id=task_id,
        task_name=task_name,
        message="my_msg",
        labels={
            "label1": "val1",
        },
    )

    await broker.kick(sent)

    callback = AsyncMock()

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(broker.listen(callback), timeout=0.4)
    message = callback.call_args_list[0].args[0]

    assert message == sent


@pytest.mark.anyio
async def test_startup(  # noqa: WPS211
    broker: AioPikaBroker,
    queue: AbstractQueue,
    exchange: AbstractExchange,
    test_channel: Channel,
    queue_name: str,
    exchange_name: str,
) -> None:
    """
    Test startup event.

    In this test we delete the exchange and the queue,
    call startup method, and ensure that queue and exchange
    exist.

    :param broker: current broker.
    :param queue: test queue.
    :param exchange: test exchange.
    :param test_channel: test channel.
    :param queue_name: name of the queue.
    :param exchange_name: name of the test exchange.
    """
    await queue.delete()
    await exchange.delete()
    broker._declare_exchange = True

    await broker.startup()

    await test_channel.get_queue(queue_name, ensure=True)
    await test_channel.get_exchange(exchange_name, ensure=True)


@pytest.mark.anyio
async def test_listen(broker: AioPikaBroker, exchange: AbstractExchange) -> None:
    """
    Test that message are read correctly.

    Tests that broker listens to the queue
    correctly and listen can be iterated.

    :param broker: current broker.
    :param exchange: current exchange.
    """
    await exchange.publish(
        Message(
            b"test_message",
            headers={
                "task_id": "test_id",
                "task_name": "task_name",
                "label1": "label_val",
            },
        ),
        routing_key="task_name",
    )

    callback = AsyncMock()

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(broker.listen(callback), timeout=0.4)
    message = callback.call_args_list[0].args[0]

    assert message.message == "test_message"
    assert message.labels == {"label1": "label_val"}
    assert message.task_id == "test_id"
    assert message.task_name == "task_name"


@pytest.mark.anyio
async def test_wrong_format(
    broker: AioPikaBroker,
    queue: AbstractQueue,
    test_channel: Channel,
) -> None:
    """
    Tests that messages with wrong format are ignored.

    :param broker: aio-pika broker.
    :param queue: test queue.
    :param test_channel: test channel.
    """
    await test_channel.default_exchange.publish(
        Message(b"wrong"),
        routing_key=queue.name,
    )
    callback = AsyncMock()

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            broker.listen(callback=callback),
            0.4,
        )

    with pytest.raises(QueueEmpty):
        await queue.get()
