import uuid
from collections.abc import AsyncGenerator

import aiormq
import pytest
from aio_pika import Channel

from taskiq_aio_pika import AioPikaBroker
from taskiq_aio_pika.exceptions import ExchangeNotDeclaredError, QueueNotDeclaredError
from taskiq_aio_pika.exchange import Exchange
from taskiq_aio_pika.queue import Queue, QueueType
from tests.conftest import _cleanup_amqp_resources


class TestStartup:
    broker: AioPikaBroker | None = None

    @pytest.fixture(autouse=True)
    async def cleanup_class_broker(self, amqp_url: str) -> AsyncGenerator[None, None]:
        yield
        if self.broker is not None:
            await self.broker.shutdown()
            queue_names = [queue.name for queue in self.broker._task_queues] + [
                self.broker._dead_letter_queue.name,
            ]
            if self.broker._delay_queue is not None:
                queue_names.append(self.broker._delay_queue.name)
            await _cleanup_amqp_resources(
                amqp_url,
                [self.broker._exchange.name],
                queue_names,
            )

    async def test_when_declare_flag_passed_to_queue__broker_declare_queue_on_startup(
        self,
        amqp_url: str,
        test_channel: Channel,
        exchange_name: str,
    ) -> None:
        # given
        self.broker = AioPikaBroker(
            url=amqp_url,
            exchange=Exchange(
                name=exchange_name,
                declare=True,
                durable=False,
            ),
        )

        # when
        self.broker.with_queue(
            Queue(
                name="declared_queue",
                declare=True,
            ),
        )
        await self.broker.startup()

        # then
        queue = await test_channel.get_queue("declared_queue", ensure=True)
        assert queue.name == "declared_queue"
        assert not queue.durable
        assert not queue.exclusive
        assert not queue.auto_delete
        assert queue.passive
        assert queue.arguments is None

    async def test_when_declare_flag_not_passed_to_queue__broker_does_not_declare_queue_on_startup(
        self,
        amqp_url: str,
        test_channel: Channel,
        exchange_name: str,
        delay_queue_name: str,
    ) -> None:
        # given
        self.broker = AioPikaBroker(
            url=amqp_url,
            exchange=Exchange(
                name=exchange_name,
                declare=True,
                durable=False,
            ),
            delay_queue=Queue(
                name=delay_queue_name,
                type=QueueType.CLASSIC,
                declare=True,
                durable=False,
            ),
        )
        not_declared_queue_name = "not_declared_queue" + uuid.uuid4().hex

        # when & then
        self.broker.with_queues(
            Queue(
                name=not_declared_queue_name,
                declare=False,
            ),
        )
        with pytest.raises(
            QueueNotDeclaredError,
            match=f"Queue '{not_declared_queue_name}' was not declared and does not exist.",
        ):
            await self.broker.startup()

    async def test_when_exchange_declare_flag_false__broker_does_not_declare_exchange_on_startup(
        self,
        amqp_url: str,
        exchange_name: str,
    ) -> None:
        # given
        self.broker = AioPikaBroker(
            url=amqp_url,
            exchange=Exchange(
                name=exchange_name,
                declare=False,
            ),
        )

        # when & then
        with pytest.raises(
            ExchangeNotDeclaredError,
            match=f"Exchange '{exchange_name}' was not declared and does not exist.",
        ):
            await self.broker.startup()

    async def test_when_exchange_declare_flag_true__broker_declares_exchange_on_startup(
        self,
        amqp_url: str,
        test_channel: Channel,
        exchange_name: str,
    ) -> None:
        # given
        self.broker = AioPikaBroker(
            url=amqp_url,
            exchange=Exchange(
                name=exchange_name,
                declare=True,
            ),
        )

        # when
        await self.broker.startup()

        # then
        exchange = await test_channel.get_exchange(exchange_name, ensure=True)
        assert (
            exchange.name == exchange_name
        )  # should be more checks for arguments here

    async def test_when_delayed_message_exchange_plugin_enabled__broker_declares_exchange_on_startup(
        self,
        amqp_url: str,
        test_channel: Channel,
        exchange_name: str,
    ) -> None:
        # given
        self.broker = AioPikaBroker(
            url=amqp_url,
            exchange=Exchange(
                name=exchange_name,
                declare=True,
            ),
            delayed_message_exchange_plugin=True,
        )

        # when
        await self.broker.startup()

        # then
        exchange = await test_channel.get_exchange(
            f"{exchange_name}.plugin_delay",
            ensure=True,
        )
        assert (
            exchange.name == f"{exchange_name}.plugin_delay"
        )  # should be more checks for arguments here

    async def test_when_delayed_message_exchange_plugin_disabled__broker_does_not_declare_exchange_on_startup(
        self,
        amqp_url: str,
        exchange_name: str,
        test_channel: Channel,
    ) -> None:
        # given
        self.broker = AioPikaBroker(
            url=amqp_url,
            exchange=Exchange(
                name=exchange_name,
                declare=True,
            ),
            delayed_message_exchange_plugin=False,
        )

        # when
        await self.broker.startup()

        # then
        with pytest.raises(
            aiormq.exceptions.ChannelNotFoundEntity,
        ):
            await test_channel.get_exchange(
                f"{exchange_name}.plugin_delay",
                ensure=True,
            )

    async def test_when_delayed_message_exchange_plugin_enabled_and_custom_exchange_not_declared__broker_raise_error(
        self,
        amqp_url: str,
        exchange_name: str,
    ) -> None:
        # given
        delayed_message_exchange_name = "custom_delay_exchange" + uuid.uuid4().hex
        self.broker = AioPikaBroker(
            url=amqp_url,
            exchange=Exchange(
                name=exchange_name,
                declare=True,
            ),
            delayed_message_exchange_plugin=True,
            delayed_message_exchange=Exchange(
                name=delayed_message_exchange_name,
                declare=False,
            ),
        )
        # when & then
        with pytest.raises(
            ExchangeNotDeclaredError,
            match=f"Exchange '{delayed_message_exchange_name}' was not declared and does not exist.",
        ):
            await self.broker.startup()

    async def test_when_delay_queue_not_specified__broker_does_not_create_delay_queue(
        self,
        amqp_url: str,
        test_channel: Channel,
        exchange_name: str,
    ) -> None:
        # given
        self.broker = AioPikaBroker(
            url=amqp_url,
            exchange=Exchange(
                name=exchange_name,
                declare=True,
            ),
        )

        # when
        await self.broker.startup()

        # then
        assert self.broker._delay_queue is None
        with pytest.raises(aiormq.exceptions.ChannelNotFoundEntity):
            await test_channel.get_queue("taskiq.delay", ensure=True)
