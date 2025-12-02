import asyncio
from collections.abc import AsyncGenerator, Callable
from datetime import timedelta
from logging import getLogger
from typing import Any, TypeVar

import aiormq
from aio_pika import DeliveryMode, ExchangeType, Message, connect_robust
from aio_pika.abc import AbstractChannel, AbstractQueue, AbstractRobustConnection
from aiostream import stream
from pamqp.common import FieldTable
from taskiq import AckableMessage, AsyncBroker, AsyncResultBackend, BrokerMessage
from typing_extensions import Self

from taskiq_aio_pika.exceptions import (
    ExchangeNotDeclaredError,
    IncorrectRoutingKeyError,
    NoStartupError,
    QueueNotDeclaredError,
)
from taskiq_aio_pika.exchange import Exchange
from taskiq_aio_pika.queue import Queue

_T = TypeVar("_T")

logger = getLogger("taskiq.aio_pika_broker")


def parse_val(
    parse_func: Callable[[str], _T],
    target: str | None = None,
) -> _T | None:
    """
    Parse string to some value.

    :param parse_func: function to use if value is present.
    :param target: value to parse, defaults to None
    :return: Optional value.
    """
    if target is None:
        return None

    try:
        return parse_func(target)
    except ValueError:
        return None


class AioPikaBroker(AsyncBroker):
    """Broker that works with RabbitMQ."""

    def __init__(
        self,
        url: str | None = None,
        result_backend: AsyncResultBackend[_T] | None = None,
        task_id_generator: Callable[[], str] | None = None,
        qos: int = 10,
        loop: asyncio.AbstractEventLoop | None = None,
        exchange: Exchange | None = None,
        task_queues: list[Queue] | None = None,
        dead_letter_queue: Queue | None = None,
        delay_queue: Queue | None = None,
        delayed_message_exchange_plugin: bool = False,
        delayed_message_exchange: Exchange | None = None,
        label_for_routing: str = "queue_name",
        label_for_priority: str = "priority",
        **connection_kwargs: Any,
    ) -> None:
        """
        Construct a new broker.

        :param url: url to rabbitmq. If None,
            the default "amqp://guest:guest@localhost:5672" is used.
        :param result_backend: custom result backend.
        :param task_id_generator: custom task_id genertaor.
        :param qos: number of messages that worker can prefetch.
        :param loop: specific even loop.
        :param exchange: parameters of exchange that used to send messages.
        :param task_queues: parameters of queues
            that will be used to get incoming messages.
        :param dead_letter_queue: parameters of dead-letter queue.
        :param delay_queue: parameters of queue for simple delay implementation.
        :param delayed_message_exchange_plugin: turn on or disable
            delayed-message-exchange rabbitmq plugin.
        :param delayed_message_exchange: parameters of exchange
            that used to send messages with delay.
        :param label_for_routing: label name to use for routing key selection.
        :param label_for_priority: label name to use for message priority.
        :param connection_kwargs: additional keyword arguments,
            for connect_robust method of aio-pika.
        """
        super().__init__(result_backend, task_id_generator)

        self.url = url
        self._loop = loop
        self._conn_kwargs = connection_kwargs
        self._exchange = exchange or Exchange()
        self._qos = qos
        self._task_queues = task_queues or []
        self._dead_letter_queue = dead_letter_queue or Queue(name="taskiq.dead_letter")

        self._label_for_routing = label_for_routing
        self._label_for_priority = label_for_priority

        self._delay_queue = delay_queue or Queue(name="taskiq.delay")

        self._delayed_message_exchange_plugin = delayed_message_exchange_plugin
        if self._delayed_message_exchange_plugin:
            self._delayed_message_exchange = delayed_message_exchange or Exchange(
                name=f"{self._exchange.name}.plugin_delay",
                type=ExchangeType.X_DELAYED_MESSAGE,
                arguments={"x-delayed-type": "direct"},
                declare=True,
            )

        self.read_conn: AbstractRobustConnection | None = None
        self.write_conn: AbstractRobustConnection | None = None
        self.write_channel: AbstractChannel | None = None
        self.read_channel: AbstractChannel | None = None

    async def startup(self) -> None:
        """Create exchange and queue on startup."""
        await super().startup()
        self.write_conn = await connect_robust(
            self.url,
            loop=self._loop,
            **self._conn_kwargs,
        )
        self.write_channel = await self.write_conn.channel()

        if self.is_worker_process:
            self.read_conn = await connect_robust(
                self.url,
                loop=self._loop,
                **self._conn_kwargs,
            )
            self.read_channel = await self.read_conn.channel()

        await self._declare_exchanges()
        await self._declare_queues(self.write_channel)

    async def _declare_exchanges(
        self,
    ) -> None:
        """
        Declare all exchanges.

        :param channel: channel to use for declaration.
        :raises NoStartupError: if startup wasn't called.
        """
        if self.write_channel is None:
            raise NoStartupError(
                "Write channel is not initialized. Please call startup first.",
            )

        if self._exchange.declare:
            await self.write_channel.declare_exchange(
                name=self._exchange.name,
                type=self._exchange.type,
                durable=self._exchange.durable,
                auto_delete=self._exchange.auto_delete,
                internal=self._exchange.internal,
                passive=self._exchange.passive,
                arguments=self._exchange.arguments,
                timeout=self._exchange.timeout,
            )
        else:
            try:
                await self.write_channel.get_exchange(
                    name=self._exchange.name,
                    ensure=True,
                )
            except aiormq.exceptions.ChannelNotFoundEntity as error:
                raise ExchangeNotDeclaredError(
                    f"Exchange '{self._exchange.name}' "
                    f"was not declared and does not exist.",
                ) from error

        if self._delayed_message_exchange_plugin:
            if self._delayed_message_exchange.declare:
                await self.write_channel.declare_exchange(
                    name=self._delayed_message_exchange.name,
                    type=self._delayed_message_exchange.type,
                    durable=self._delayed_message_exchange.durable,
                    auto_delete=self._delayed_message_exchange.auto_delete,
                    internal=self._delayed_message_exchange.internal,
                    passive=self._delayed_message_exchange.passive,
                    arguments=self._delayed_message_exchange.arguments,
                    timeout=self._delayed_message_exchange.timeout,
                )
            else:
                try:
                    await self.write_channel.get_exchange(
                        name=self._delayed_message_exchange.name,
                        ensure=True,
                    )
                except aiormq.exceptions.ChannelNotFoundEntity as error:
                    raise ExchangeNotDeclaredError(
                        f"Exchange '{self._delayed_message_exchange.name}' "
                        f"was not declared and does not exist.",
                    ) from error

    async def _declare_dead_letter_queue(
        self,
        channel: AbstractChannel,
    ) -> None:
        if self._dead_letter_queue.declare:
            dead_letter_queue_arguments = self._dead_letter_queue.arguments.copy()
            if self._dead_letter_queue.max_priority is not None:
                dead_letter_queue_arguments["x-max-priority"] = (
                    self._dead_letter_queue.max_priority
                )
            dead_letter_queue_arguments["x-queue-type"] = (
                self._dead_letter_queue.type.value
            )
            await channel.declare_queue(
                name=self._dead_letter_queue.name,
                durable=self._dead_letter_queue.durable,
                exclusive=self._dead_letter_queue.exclusive,
                passive=self._dead_letter_queue.passive,
                auto_delete=self._dead_letter_queue.auto_delete,
                arguments=dead_letter_queue_arguments,
                timeout=self._dead_letter_queue.timeout,
            )
        else:
            try:
                await channel.get_queue(
                    name=self._dead_letter_queue.name,
                    ensure=True,
                )
            except aiormq.exceptions.ChannelNotFoundEntity as error:
                raise QueueNotDeclaredError(
                    f"Dead-letter queue '{self._dead_letter_queue.name}' "
                    f"was not declared and does not exist.",
                ) from error

    async def _declare_queues(
        self,
        channel: AbstractChannel,
    ) -> list[tuple[AbstractQueue, dict[str, Any]]]:
        """
        Declare all queues.

        It's useful since aio-pika have automatic
        recover mechanism, which works only if
        the queue, you're going to listen was
        declared by aio-pika.

        :param channel: channel to used for declaration.
        :return: main queue instance.
        """
        await self._declare_dead_letter_queue(channel)
        declared_queues = []
        queue_default_arguments: FieldTable = {
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": (
                self._dead_letter_queue.routing_key or self._dead_letter_queue.name
            ),
        }
        if not self._task_queues:  # add default queue if user didn't provide any
            self._task_queues.append(Queue())

        queues = self._task_queues.copy()
        if not self._delayed_message_exchange_plugin:
            queues.append(self._delay_queue)

        for queue in filter(lambda queue: queue.declare, queues):
            per_queue_arguments: FieldTable = queue_default_arguments.copy()
            if queue.max_priority is not None:
                per_queue_arguments["x-max-priority"] = queue.max_priority
            per_queue_arguments["x-queue-type"] = queue.type.value
            if queue.name == self._delay_queue.name:
                per_queue_arguments["x-dead-letter-exchange"] = self._exchange.name
                per_queue_arguments["x-dead-letter-routing-key"] = (
                    self._delay_queue.routing_key
                    or queues[0].routing_key
                    or queues[0].name
                )
            per_queue_arguments.update(
                queue.arguments if queue.arguments is not None else {},
            )
            declared_queue = await channel.declare_queue(
                name=queue.name,
                durable=queue.durable,
                exclusive=queue.exclusive,
                passive=queue.passive,
                auto_delete=queue.auto_delete,
                arguments=per_queue_arguments,
                timeout=queue.timeout,
            )
            logger.debug(
                "Bind queue to exchange with routing key '%s'",
                queue.routing_key or queue.name,
            )
            if queue.name != self._delay_queue.name:
                await declared_queue.bind(
                    exchange=self._exchange.name,
                    routing_key=queue.routing_key or queue.name,
                    arguments=queue.bind_arguments,
                    timeout=queue.bind_timeout,
                )
            if self._delayed_message_exchange_plugin:
                await declared_queue.bind(
                    exchange=self._delayed_message_exchange.name,
                    routing_key=queue.routing_key or queue.name,
                )
            declared_queues.append((declared_queue, queue.consumer_arguments))

        for queue in filter(lambda queue: not queue.declare, queues):
            try:
                existing_queue = await channel.get_queue(
                    name=queue.name,
                    ensure=True,
                )
            except aiormq.exceptions.ChannelNotFoundEntity as error:
                raise QueueNotDeclaredError(
                    f"Queue '{queue.name}' was not declared and does not exist.",
                ) from error
            declared_queues.append((existing_queue, queue.consumer_arguments))
        return declared_queues

    def with_queue(self, queue: Queue) -> Self:
        """
        Add new queue to the broker.

        :param queue: queue to add.
        :return: self.
        """
        self._task_queues.append(queue)
        return self

    def with_queues(self, *queues: Queue) -> Self:
        """
        Replace existing queues with new ones.

        :param queues: queues to add.
        :return: self.
        """
        self._task_queues = list(queues)
        return self

    async def kick(self, message: BrokerMessage) -> None:
        """
        Send message to the exchange.

        This function constructs rmq message
        and sends it.

        The message has task_id and task_name and labels
        in headers. And message's routing key is the same
        as the task_name.

        :raises NoStartupError: if startup wasn't called.
        :raises IncorrectRoutingKeyError: if routing key is incorrect.
        :param message: message to send.
        """
        if self.write_channel is None:
            raise NoStartupError("Please run startup before kicking.")
        priority = parse_val(int, message.labels.get(self._label_for_priority))
        rmq_message = Message(
            body=message.message,
            headers={
                "task_id": message.task_id,
                "task_name": message.task_name,
                **message.labels,
            },
            delivery_mode=DeliveryMode.PERSISTENT,
            priority=priority,
        )
        delay = parse_val(float, message.labels.get("delay"))

        if len(self._task_queues) == 1:
            routing_key_name = (
                self._task_queues[0].routing_key or self._task_queues[0].name
            )
        else:
            routing_key_name = (
                parse_val(
                    str,
                    message.labels.get(self._label_for_routing),
                )
                or ""
            )
            if self._exchange.type == ExchangeType.DIRECT and routing_key_name not in {
                queue.routing_key or queue.name for queue in self._task_queues
            }:
                raise IncorrectRoutingKeyError(
                    f"Routing key '{routing_key_name}' is not valid. "
                    f"Check routing keys and queue names in broker queues.",
                )

        if delay is None:
            exchange = await self.write_channel.get_exchange(
                self._exchange.name,
                ensure=False,
            )
            await exchange.publish(rmq_message, routing_key=routing_key_name)
        elif self._delayed_message_exchange_plugin:
            rmq_message.headers["x-delay"] = int(delay * 1000)
            exchange = await self.write_channel.get_exchange(
                self._delayed_message_exchange.name,
            )
            await exchange.publish(rmq_message, routing_key=routing_key_name)
        else:
            rmq_message.expiration = timedelta(seconds=delay)
            await self.write_channel.default_exchange.publish(
                rmq_message,
                routing_key=self._delay_queue.routing_key or self._delay_queue.name,
            )

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """
        Listen to queue.

        This function listens to queue and
        yields every new message.

        :raises NoStartupError: if startup wasn't called.
        :yields: parsed broker message.
        """
        if self.read_channel is None:
            raise NoStartupError("Call startup before starting listening.")
        await self.read_channel.set_qos(prefetch_count=self._qos)
        queue_with_consumer_args_list = await self._declare_queues(self.read_channel)

        async def body(
            queue: AbstractQueue,
            consumer_args: dict[str, Any],
        ) -> AsyncGenerator[AckableMessage, None]:
            try:
                async with queue.iterator(**consumer_args) as iterator:
                    async for message in iterator:
                        yield AckableMessage(
                            data=message.body,
                            ack=message.ack,
                        )
            except (RuntimeError, asyncio.CancelledError):
                # Suppress errors during iterator cleanup if channel is being closed
                logger.info("Queue iterator closed during shutdown")

        combine = stream.merge(
            *[
                body(queue, consumer_args)
                for queue, consumer_args in queue_with_consumer_args_list
                if queue.name != self._delay_queue.name
            ],
        )

        async with combine.stream() as streamer:
            async for message in streamer:
                yield message

    async def shutdown(self) -> None:
        """Close all connections on shutdown."""
        await super().shutdown()
        if self.write_channel:
            await self.write_channel.close()
        if self.read_channel:
            await self.read_channel.close()
        if self.write_conn:
            await self.write_conn.close()
        if self.read_conn:
            await self.read_conn.close()
