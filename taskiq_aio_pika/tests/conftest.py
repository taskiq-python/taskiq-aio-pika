import os
from typing import AsyncGenerator
from uuid import uuid4

import pytest
from aio_pika import Channel, ExchangeType, connect_robust
from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractQueue,
    AbstractRobustConnection,
)
from aiormq import ChannelNotFoundEntity

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
async def exchange(
    test_channel: Channel,
    exchange_name: str,
) -> AsyncGenerator[AbstractExchange, None]:
    """
    Create test exchange.

    This fixture declares exchange,
    and deletes it after test is complete.

    :param test_channel: current channel.
    :param exchange_name: name of the test exchange.
    :yield: exchange.
    """
    exchange = await test_channel.declare_exchange(
        exchange_name,
        type=ExchangeType.TOPIC,
    )

    yield exchange

    try:
        await exchange.delete(
            timeout=1,
            if_unused=False,
        )
    except ChannelNotFoundEntity:  # pragma: no cover
        pass  # noqa: WPS420


@pytest.fixture
async def queue(
    test_channel: Channel,
    queue_name: str,
) -> AsyncGenerator[AbstractQueue, None]:
    """
    Create test queue.

    This fixture declares queue,
    and deletes it after test is complete.

    :param test_channel: current channel.
    :param queue_name: name of the test queue.
    :yield: queue.
    """
    queue = await test_channel.declare_queue(queue_name)

    yield queue

    try:
        await queue.delete(
            timeout=1,
            if_empty=False,
            if_unused=False,
        )
    except ChannelNotFoundEntity:  # pragma: no cover
        pass  # noqa: WPS420


@pytest.fixture
async def broker(
    amqp_url: str,
    queue_name: str,
    exchange_name: str,
    queue: AbstractQueue,
    exchange: AbstractExchange,
) -> AsyncGenerator[AioPikaBroker, None]:
    """
    Yields new broker instance.

    This function is used to
    create broker, run startup,
    and shutdown after test.

    :param amqp_url: current rabbitmq connection string.
    :param queue_name: test queue name.
    :param exchange_name: test exchange name.
    :param queue: declared queue.
    :param exchange: declared exchange.
    :yield: broker.
    """
    broker = AioPikaBroker(
        url=amqp_url,
        declare_exchange=False,
        exchange_name=exchange_name,
        queue_name=queue_name,
    )
    broker.is_worker_process = True

    await broker.startup()

    yield broker

    await broker.shutdown()
