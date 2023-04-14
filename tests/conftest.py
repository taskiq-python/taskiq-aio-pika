import os
from typing import AsyncGenerator
from uuid import uuid4

import pytest
from aio_pika import Channel, connect_robust
from aio_pika.abc import AbstractChannel, AbstractRobustConnection

from taskiq_aio_pika.broker import AioPikaBroker


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """
    Backend for anyio pytest plugin.

    :return: backend name.
    """
    return "asyncio"


@pytest.fixture
def amqp_url() -> str:
    """
    Get custom amqp url.

    This function tries to get custom amqp URL,
    or returns default otherwise.

    :return: rabbitmq url.
    """
    return os.environ.get("TEST_AMQP_URL", "amqp://guest:guest@localhost:5672")


@pytest.fixture
def queue_name() -> str:
    """
    Generated queue name.

    :return: random queue name.
    """
    return uuid4().hex


@pytest.fixture
def routing_key() -> str:
    """
    Generated routing key.

    :return: random routing key.
    """
    return uuid4().hex


@pytest.fixture
def delay_queue_name() -> str:
    """
    Generated name for delay queue.

    :return: random exchange name.
    """
    return uuid4().hex


@pytest.fixture
def dead_queue_name() -> str:
    """
    Generated name for dead letter queue.

    :return: random exchange name.
    """
    return uuid4().hex


@pytest.fixture
def exchange_name() -> str:
    """
    Generated exchange name.

    :return: random exchange name.
    """
    return uuid4().hex


@pytest.fixture
async def test_connection(
    amqp_url: str,
) -> AsyncGenerator[AbstractRobustConnection, None]:
    """
    Create robust connection.

    :param amqp_url: url for rabbitmq.
    :yield: opened connection.
    """
    connection = await connect_robust(amqp_url)

    yield connection

    await connection.close()


@pytest.fixture
async def test_channel(
    test_connection: AbstractRobustConnection,
) -> AsyncGenerator[AbstractChannel, None]:
    """
    Opens a new channel.

    :param test_connection: current rabbitmq conneciton.
    :yield: opened channel.
    """
    async with test_connection.channel() as chan:
        yield chan


@pytest.fixture
async def broker(
    amqp_url: str,
    queue_name: str,
    delay_queue_name: str,
    dead_queue_name: str,
    exchange_name: str,
    test_channel: Channel,
) -> AsyncGenerator[AioPikaBroker, None]:
    """
    Yields new broker instance.

    This function is used to
    create broker, run startup,
    and shutdown after test.

    :param amqp_url: current rabbitmq connection string.
    :param test_channel: amqp channel for tests.
    :param queue_name: test queue name.
    :param delay_queue_name: test delay queue name.
    :param dead_queue_name: test dead letter queue name.
    :param exchange_name: test exchange name.
    :yield: broker.
    """
    broker = AioPikaBroker(
        url=amqp_url,
        declare_exchange=True,
        exchange_name=exchange_name,
        dead_letter_queue_name=dead_queue_name,
        delay_queue_name=delay_queue_name,
        queue_name=queue_name,
    )
    broker.is_worker_process = True

    await broker.startup()

    yield broker

    await broker.shutdown()

    exchange = await test_channel.get_exchange(exchange_name)
    await exchange.delete(
        timeout=1,
        if_unused=False,
    )
    for i_queue_name in (queue_name, delay_queue_name, dead_queue_name):
        queue = await test_channel.get_queue(i_queue_name, ensure=False)
        await queue.delete(
            timeout=1,
            if_empty=False,
            if_unused=False,
        )


@pytest.fixture
async def broker_with_delayed_message_plugin(
    amqp_url: str,
    queue_name: str,
    delay_queue_name: str,
    dead_queue_name: str,
    exchange_name: str,
    routing_key: str,
    test_channel: Channel,
) -> AsyncGenerator[AioPikaBroker, None]:
    """
    Yields new broker instance.

    This function is used to
    create broker, run startup,
    and shutdown after test.

    :param amqp_url: current rabbitmq connection string.
    :param test_channel: amqp channel for tests.
    :param queue_name: test queue name.
    :param delay_queue_name: test delay queue name.
    :param dead_queue_name: test dead letter queue name.
    :param exchange_name: test exchange name.
    :param routing_key: routing_key.
    :yield: broker.
    """
    broker = AioPikaBroker(
        url=amqp_url,
        declare_exchange=True,
        exchange_name=exchange_name,
        dead_letter_queue_name=dead_queue_name,
        queue_name=queue_name,
        delayed_message_exchange_plugin=True,
        routing_key=routing_key,
    )
    broker.is_worker_process = True

    await broker.startup()

    yield broker

    await broker.shutdown()

    exchange = await test_channel.get_exchange(exchange_name)
    await exchange.delete(
        timeout=1,
        if_unused=False,
    )
    plugin_exchange = await test_channel.get_exchange(
        broker._delay_plugin_exchange_name,
    )
    await plugin_exchange.delete(
        timeout=1,
        if_unused=False,
    )
    for i_queue_name in (queue_name, delay_queue_name, dead_queue_name):
        queue = await test_channel.get_queue(i_queue_name, ensure=False)
        await queue.delete(
            timeout=1,
            if_empty=False,
            if_unused=False,
        )
