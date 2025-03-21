import asyncio
import uuid
from typing import Any, Literal

import pytest
from aio_pika import Channel, Message
from aio_pika.exceptions import QueueEmpty
from taskiq import BrokerMessage
from taskiq.message import AckableNackableWrappedMessageWithMetadata
from taskiq.utils import maybe_awaitable

from taskiq_aio_pika.broker import AioPikaBroker, QueueType


async def get_first_task(  # type: ignore
    broker: AioPikaBroker,
) -> AckableNackableWrappedMessageWithMetadata:
    """
    Get first message from the queue.

    :param broker: async message broker.
    :return: first message from listen method
    """
    async for message in broker.listen():  # noqa: RET503
        return message


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
        message=b"my_msg",
        labels={
            "label1": "val1",
        },
    )

    await broker.kick(sent)

    message = await asyncio.wait_for(get_first_task(broker), timeout=0.4)

    assert message.message == sent.message
    await maybe_awaitable(message.ack())


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

    message = await asyncio.wait_for(get_first_task(broker), timeout=0.4)

    assert message.message == b"test_message"
    await maybe_awaitable(message.ack())


@pytest.mark.anyio
async def test_wrong_format(
    broker: AioPikaBroker,
    queue_name: str,
    test_channel: Channel,
) -> None:
    """
    Tests that messages with wrong format are still received.

    :param broker: aio-pika broker.
    :param queue_name: test queue name.
    :param test_channel: test channel.
    """
    queue = await test_channel.get_queue(queue_name)
    await test_channel.default_exchange.publish(
        Message(b"wrong"),
        routing_key=queue_name,
    )

    message = await asyncio.wait_for(get_first_task(broker), 0.4)

    assert message.message == b"wrong"
    await maybe_awaitable(message.ack())

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
        message=b"message",
        labels={"delay": "2"},
    )
    await broker.kick(broker_msg)

    # We check that message appears in delay queue.
    delay_msg = await delay_queue.get()
    await delay_msg.nack(requeue=True)

    # After we wait the delay message must appear in
    # the main queue.
    await asyncio.sleep(2)

    # Check that it disappear.
    with pytest.raises(QueueEmpty):
        await delay_queue.get(no_ack=True)

    # Check that we can get the message.
    await main_queue.get()


@pytest.mark.anyio
async def test_delayed_message_with_plugin(
    broker_with_delayed_message_plugin: AioPikaBroker,
    test_channel: Channel,
    queue_name: str,
) -> None:
    """Test that we can send delayed messages with plugin.

    :param broker_with_delayed_message_plugin: broker with
    turned on plugin integration.
    :param test_channel: amqp channel for tests.
    :param queue_name: test queue name.
    """
    main_queue = await test_channel.get_queue(queue_name)
    broker_msg = BrokerMessage(
        task_id="1",
        task_name="name",
        message=b"message",
        labels={"delay": "2"},
    )

    await broker_with_delayed_message_plugin.kick(broker_msg)
    with pytest.raises(QueueEmpty):
        await main_queue.get(no_ack=True)

    await asyncio.sleep(2)

    assert await main_queue.get()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "args, x_queue_type, x_delivery_limit, max_attempts_at_message, raised_exception",
    [
        (  # custom `max_attempts_at_message`
            {
                "queue_type": QueueType.QUORUM,
                "max_attempts_at_message": 4,
            },
            "quorum",
            5,
            4,
            False,
        ),
        (  # unlimited `max_attempts_at_message`
            {
                "queue_type": QueueType.QUORUM,
                "max_attempts_at_message": None,
            },
            "quorum",
            "-1",
            None,
            False,
        ),
        (  # default `max_attempts_at_message`
            {
                "queue_type": QueueType.QUORUM,
            },
            "quorum",
            21,
            20,
            False,
        ),
        (  # classic queue type
            {
                "queue_type": QueueType.CLASSIC,
            },
            "classic",
            False,
            None,
            False,
        ),
        (  # classic queue type with `max_attempts_at_message` at `None`
            {
                "queue_type": QueueType.CLASSIC,
                "max_attempts_at_message": None,
            },
            "classic",
            False,
            None,
            False,
        ),
        ({}, "classic", False, None, False),  # default queue type
        (  # `x-queue-type` should raise
            {"declare_queues_kwargs": {"arguments": {"x-queue-type": "classic"}}},
            "classic",
            False,
            None,
            True,
        ),
        (  # `x-delivery-limit` should raise
            {"declare_queues_kwargs": {"arguments": {"x-delivery-limit": 3}}},
            None,
            None,
            None,
            True,
        ),
        (  # classic queue type with `max_attempts_at_message` should raise
            {
                "queue_type": QueueType.CLASSIC,
                "max_attempts_at_message": 3,
            },
            "classic",
            False,
            None,
            True,
        ),
    ],
)
async def test_broker_arguments(
    amqp_url: str,
    args: dict[str, Any],
    x_delivery_limit: int | str | Literal[False],
    x_queue_type: str,
    max_attempts_at_message: int | None,
    raised_exception: bool,
) -> None:
    if raised_exception:
        with pytest.raises(ValueError):
            broker = AioPikaBroker(amqp_url, **args)
        return

    broker = AioPikaBroker(amqp_url, **args)
    await broker.startup()
    queue = await broker.declare_queues(broker.write_channel)  # type: ignore[arg-type]

    assert queue.arguments["x-dead-letter-exchange"] == ""  # type: ignore[index]
    assert queue.arguments["x-dead-letter-routing-key"] == "taskiq.dead_letter"  # type: ignore[index]
    assert queue.arguments["x-queue-type"] == x_queue_type  # type: ignore[index]
    if x_delivery_limit:
        assert queue.arguments["x-delivery-limit"] == x_delivery_limit  # type: ignore[index]
    assert broker.max_attempts_at_message == max_attempts_at_message

    await broker.shutdown()
