import asyncio
import uuid

import pytest
from aio_pika import Channel, Message
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
async def test_startup(
    broker: AioPikaBroker,
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
    :param test_channel: test channel.
    :param queue_name: name of the queue.
    :param exchange_name: name of the test exchange.
    """
    queue = await test_channel.get_queue(queue_name)
    exchange = await test_channel.get_exchange(exchange_name)
    await queue.delete()
    await exchange.delete()
    broker._declare_exchange = True

    await broker.startup()

    await test_channel.get_queue(queue_name, ensure=True)
    await test_channel.get_exchange(exchange_name, ensure=True)


@pytest.mark.anyio
async def test_listen(
    broker: AioPikaBroker,
    test_channel: Channel,
    exchange_name: str,
) -> None:
    """
    Test that message are read correctly.

    Tests that broker listens to the queue
    correctly and listen can be iterated.

    :param broker: current broker.
    :param test_channel: amqp channel.
    :param exchange_name: main exchange name.
    """
    exchange = await test_channel.get_exchange(exchange_name)
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
    queue_name: str,
    test_channel: Channel,
) -> None:
    """
    Tests that messages with wrong format are ignored.

    :param broker: aio-pika broker.
    :param queue_name: test queue name.
    :param test_channel: test channel.
    """
    queue = await test_channel.get_queue(queue_name)
    await test_channel.default_exchange.publish(
        Message(b"wrong"),
        routing_key=queue_name,
    )
    callback = AsyncMock()

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            broker.listen(callback=callback),
            0.4,
        )

    with pytest.raises(QueueEmpty):
        await queue.get()


@pytest.mark.anyio
async def test_delayed_message(
    broker: AioPikaBroker,
    test_channel: Channel,
    queue_name: str,
    delay_queue_name: str,
) -> None:
    """
    Test that delayed messages are delivered correctly.

    This test send message with delay label,
    checks that this message appears in delay queue.
    After that it waits specified delay period and
    checks that message was transfered to the main queue.

    :param broker: current broker.
    :param test_channel: amqp channel for tests.
    :param queue_name: test queue name.
    :param delay_queue_name: name of the test queue for delayed messages.
    """
    delay_queue = await test_channel.get_queue(delay_queue_name)
    main_queue = await test_channel.get_queue(queue_name)
    broker_msg = BrokerMessage(
        task_id="1",
        task_name="name",
        message="message",
        labels={"delay": "2"},
    )
    await broker.kick(broker_msg)

    # We check that message appears in delay queue.
    delay_msg = await delay_queue.get()
    await delay_msg.nack(requeue=True)  # type: ignore

    # After we wait the delay message must appear in
    # the main queue.
    await asyncio.sleep(2)

    # Check that it disappear.
    with pytest.raises(QueueEmpty):
        await delay_queue.get(no_ack=True)

    # Check that we can get the message.
    await main_queue.get()
