import asyncio
import uuid
from collections.abc import AsyncGenerator

import aio_pika
import pytest
from aio_pika import Channel, Message
from aio_pika.abc import ExchangeType
from taskiq import BrokerMessage
from taskiq.utils import maybe_awaitable

from taskiq_aio_pika import AioPikaBroker, Queue
from taskiq_aio_pika.exchange import Exchange
from tests.conftest import _cleanup_amqp_resources
from tests.utils import get_first_task


class TestRouting:
    broker: AioPikaBroker | None = None

    @pytest.fixture(autouse=True)
    async def cleanup_class_broker(self, amqp_url: str) -> AsyncGenerator[None, None]:
        yield
        if self.broker is not None:
            await self.broker.shutdown()
            await _cleanup_amqp_resources(
                amqp_url,
                [self.broker._exchange.name],
                [queue.name for queue in self.broker._task_queues]
                + [
                    self.broker._dead_letter_queue.name,
                    self.broker._delay_queue.name,
                ],
            )

    async def test_when_message_has_wrong_format__then_message_still_can_be_received(
        self,
        broker: AioPikaBroker,
        queue_name: str,
        test_channel: Channel,
    ) -> None:
        # given & when
        await test_channel.default_exchange.publish(
            Message(b"wrong"),
            routing_key=queue_name,
        )

        # then
        message = await asyncio.wait_for(get_first_task(broker), 0.4)
        assert message.data == b"wrong"

    async def test_when_broker_has_only_default_settings__then_task_can_be_passed(
        self,
        amqp_url: str,
    ) -> None:
        # given
        self.broker = AioPikaBroker(
            url=amqp_url,
        )
        self.broker.is_worker_process = True
        await self.broker.startup()
        task_id = uuid.uuid4().hex
        message = BrokerMessage(
            task_id=task_id,
            task_name="task_name",
            message=b"my_msg",
            labels={
                "label1": "val1",
            },
        )

        # when
        await self.broker.kick(message)

        # then
        received_message = await asyncio.wait_for(
            get_first_task(self.broker),
            timeout=0.4,
        )
        assert received_message.data == message.message
        await maybe_awaitable(received_message.ack())

    async def test_when_broker_has_two_queues_and_default_exchange__then_task_should_be_put_only_to_right_queue(
        self,
        amqp_url: str,
        test_channel: Channel,
    ) -> None:
        # given
        self.broker = AioPikaBroker(
            url=amqp_url,
            task_queues=[
                Queue(
                    name="queue1",
                    declare=True,
                ),
                Queue(
                    name="queue2",
                    declare=True,
                ),
            ],
        )
        self.broker.is_worker_process = True
        await self.broker.startup()

        queue_1 = await test_channel.get_queue("queue1")
        queue_2 = await test_channel.get_queue("queue2")

        message_to_queue_1 = BrokerMessage(
            task_id=uuid.uuid4().hex,
            task_name="task_name",
            message=b"my_msg",
            labels={
                "queue_name": "queue1",
            },
        )

        # when
        await self.broker.kick(message_to_queue_1)

        # then
        received_message = await queue_1.get()
        assert received_message is not None
        await received_message.nack(requeue=True)

        with pytest.raises(aio_pika.exceptions.QueueEmpty):
            received_message = await queue_2.get()
            assert received_message is None

    async def test_when_queue_bind_with_pattern_and_exchange_type_topic__when_message_published_in_right_queue(
        self,
        amqp_url: str,
        test_channel: Channel,
    ) -> None:
        # given
        self.broker = AioPikaBroker(
            url=amqp_url,
            exchange=Exchange(
                name="test_topic_exchange",
                type=ExchangeType.TOPIC,
                declare=True,
            ),
            task_queues=[
                Queue(
                    name="service.task.queue1",
                    declare=True,
                    routing_key="service.task.*",
                ),
                Queue(
                    name="service.task.queue2",
                    declare=True,
                    routing_key="service.task.only_specific_task",
                ),
            ],
        )
        self.broker.is_worker_process = True
        await self.broker.startup()

        queue_1 = await test_channel.get_queue("service.task.queue1")
        queue_2 = await test_channel.get_queue("service.task.queue2")

        message_with_specific_task = BrokerMessage(
            task_id=uuid.uuid4().hex,
            task_name="task_name",
            message=b"my_msg",
            labels={"queue_name": "service.task.only_specific_task"},
        )

        # when
        await self.broker.kick(message_with_specific_task)

        # then
        received_message_1 = await queue_1.get()
        assert (
            received_message_1 is not None
        ), "Message was not routed to queue, but should be by pattern"
        await received_message_1.ack()

        received_message_2 = await queue_2.get()
        assert (
            received_message_2 is not None
        ), "Message was not routed to queue, but should be by specific name"
        await received_message_2.ack()

    async def test_when_exchange_fanout__when_message_published_in_all_queues(
        self,
        amqp_url: str,
        test_channel: Channel,
    ) -> None:
        # given
        self.broker = AioPikaBroker(
            url=amqp_url,
            exchange=Exchange(
                name="test_topic_exchange",
                type=ExchangeType.FANOUT,
                declare=True,
            ),
            task_queues=[
                Queue(
                    name="service.task.queue1",
                    declare=True,
                ),
                Queue(
                    name="service.task.queue2",
                    declare=True,
                ),
            ],
        )
        self.broker.is_worker_process = True
        await self.broker.startup()

        queue_1 = await test_channel.get_queue("service.task.queue1")
        queue_2 = await test_channel.get_queue("service.task.queue2")

        message_for_all_queues = BrokerMessage(
            task_id=uuid.uuid4().hex,
            task_name="task_name",
            message=b"my_msg",
            labels={"queue_name": "service.task.only_specific_task"},
        )

        # when
        await self.broker.kick(message_for_all_queues)

        # then
        received_message_1 = await queue_1.get()
        assert received_message_1 is not None
        await received_message_1.ack()

        received_message_2 = await queue_2.get()
        assert received_message_2 is not None
        await received_message_2.ack()
