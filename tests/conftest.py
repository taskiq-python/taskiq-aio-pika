import os
from collections.abc import AsyncGenerator
from contextlib import suppress
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

    :param test_connection: current rabbitmq connection.
    :yield: opened channel.
    """
    async with test_connection.channel() as chan:
        yield chan


async def _cleanup_amqp_resources(
    amqp_url: str,
    exchange_names: list[str],
    queue_names: list[str],
) -> None:
    """
    Clean up AMQP resources using a fresh connection.

    This creates a separate connection for cleanup to avoid
    issues with closed or invalid channels from test fixtures.

    :param amqp_url: AMQP connection URL.
    :param exchange_names: List of exchange names to delete.
    :param queue_names: List of queue names to delete.
    """
    cleanup_connection = await connect_robust(amqp_url)
    try:
        async with cleanup_connection.channel() as cleanup_channel:
            for exchange_name in exchange_names:
                with suppress(Exception):
                    exchange = await cleanup_channel.get_exchange(exchange_name)
                    await exchange.delete(timeout=1, if_unused=False)

            for queue_name in queue_names:
                with suppress(Exception):
                    queue = await cleanup_channel.get_queue(queue_name, ensure=False)
                    await queue.delete(timeout=1, if_empty=False, if_unused=False)
    finally:
        await cleanup_connection.close()


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

    await _cleanup_amqp_resources(
        amqp_url,
        [exchange_name],
        [queue_name, delay_queue_name, dead_queue_name],
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

    await _cleanup_amqp_resources(
        amqp_url,
        [exchange_name, broker._delay_plugin_exchange_name],
        [queue_name, delay_queue_name, dead_queue_name],
    )
