import asyncio
from datetime import timedelta
from logging import getLogger
from typing import Any, AsyncGenerator, Callable, Dict, Optional, TypeVar

from aio_pika import DeliveryMode, ExchangeType, Message, connect_robust
from aio_pika.abc import AbstractChannel, AbstractQueue, AbstractRobustConnection
from taskiq import AckableMessage, AsyncBroker, AsyncResultBackend, BrokerMessage

_T = TypeVar("_T")

logger = getLogger("taskiq.aio_pika_broker")


def parse_val(
    parse_func: Callable[[str], _T],
    target: Optional[str] = None,
) -> Optional[_T]:
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
        url: Optional[str] = None,
        result_backend: Optional[AsyncResultBackend[_T]] = None,
        task_id_generator: Optional[Callable[[], str]] = None,
        qos: int = 10,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        exchange_name: str = "taskiq",
        queue_name: str = "taskiq",
        dead_letter_queue_name: Optional[str] = None,
        delay_queue_name: Optional[str] = None,
        declare_exchange: bool = True,
        declare_queues: bool = True,
        routing_key: str = "#",
        exchange_type: ExchangeType = ExchangeType.TOPIC,
        max_priority: Optional[int] = None,
        delayed_message_exchange_plugin: bool = False,
        declare_exchange_kwargs: Optional[Dict[Any, Any]] = None,
        declare_queues_kwargs: Optional[Dict[Any, Any]] = None,
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
        :param exchange_name: name of exchange that used to send messages.
        :param queue_name: queue that used to get incoming messages.
        :param dead_letter_queue_name: custom name for dead-letter queue.
            by default it set to {queue_name}.dead_letter.
        :param delay_queue_name: custom name for queue that used to
            deliver messages with delays.
        :param declare_exchange: whether you want to declare new exchange
            if it doesn't exist.
        :param declare_queues: whether you want to declare queues even on
            client side. May be useful for message persistance.
        :param routing_key: that used to bind that queue to the exchange.
        :param exchange_type: type of the exchange.
            Used only if `declare_exchange` is True.
        :param max_priority: maximum priority value for messages.
        :param delayed_message_exchange_plugin: turn on or disable
            delayed-message-exchange rabbitmq plugin.
        :param declare_exchange_kwargs: additional from AbstractChannel.declare_exchange
        :param declare_queues_kwargs: additional from AbstractChannel.declare_queue
        :param connection_kwargs: additional keyword arguments,
            for connect_robust method of aio-pika.
        """
        super().__init__(result_backend, task_id_generator)

        self.url = url
        self._loop = loop
        self._conn_kwargs = connection_kwargs
        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
        self._qos = qos
        self._declare_exchange = declare_exchange
        self._declare_exchange_kwargs = declare_exchange_kwargs or {}
        self._declare_queues = declare_queues
        self._declare_queues_kwargs = declare_queues_kwargs or {}
        self._queue_name = queue_name
        self._routing_key = routing_key
        self._max_priority = max_priority
        self._delayed_message_exchange_plugin = delayed_message_exchange_plugin

        self._dead_letter_queue_name = f"{queue_name}.dead_letter"
        if dead_letter_queue_name:
            self._dead_letter_queue_name = dead_letter_queue_name

        self._delay_queue_name = f"{queue_name}.delay"
        if delay_queue_name:
            self._delay_queue_name = delay_queue_name

        self._delay_plugin_exchange_name = f"{exchange_name}.plugin_delay"

        self.read_conn: Optional[AbstractRobustConnection] = None
        self.write_conn: Optional[AbstractRobustConnection] = None
        self.write_channel: Optional[AbstractChannel] = None
        self.read_channel: Optional[AbstractChannel] = None

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

        if self._declare_exchange:
            await self.write_channel.declare_exchange(
                self._exchange_name,
                type=self._exchange_type,
                **self._declare_exchange_kwargs,
            )

        if self._delayed_message_exchange_plugin:
            await self.write_channel.declare_exchange(
                self._delay_plugin_exchange_name,
                type=ExchangeType.X_DELAYED_MESSAGE,
                arguments={
                    "x-delayed-type": "direct",
                },
            )

        if self._declare_queues:
            await self.declare_queues(self.write_channel)

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

    async def declare_queues(
        self,
        channel: AbstractChannel,
    ) -> AbstractQueue:
        """
        This function is used to declare queues.

        It's useful since aio-pika have automatic
        recover mechanism, which works only if
        the queue, you're going to listen was
        declared by aio-pika.

        :param channel: channel to used for declaration.
        :return: main queue instance.
        """
        await channel.declare_queue(
            self._dead_letter_queue_name,
            **self._declare_queues_kwargs,
        )
        args: "Dict[str, Any]" = {
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": self._dead_letter_queue_name,
        }
        if self._max_priority is not None:
            args["x-max-priority"] = self._max_priority
        queue = await channel.declare_queue(
            self._queue_name,
            arguments=args,
            **self._declare_queues_kwargs,
        )
        if self._delayed_message_exchange_plugin:
            await queue.bind(
                exchange=self._delay_plugin_exchange_name,
                routing_key=self._routing_key,
            )
        else:
            await channel.declare_queue(
                self._delay_queue_name,
                arguments={
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": self._queue_name,
                },
                **self._declare_queues_kwargs,
            )

        await queue.bind(
            exchange=self._exchange_name,
            routing_key=self._routing_key,
        )
        return queue

    async def kick(self, message: BrokerMessage) -> None:
        """
        Send message to the exchange.

        This function constructs rmq message
        and sends it.

        The message has task_id and task_name and labels
        in headers. And message's routing key is the same
        as the task_name.

        :raises ValueError: if startup wasn't called.
        :param message: message to send.
        """
        if self.write_channel is None:
            raise ValueError("Please run startup before kicking.")

        message_base_params: Dict[str, Any] = {
            "body": message.message,
            "headers": {
                "task_id": message.task_id,
                "task_name": message.task_name,
                **message.labels,
            },
            "delivery_mode": DeliveryMode.PERSISTENT,
            "priority": parse_val(
                int,
                message.labels.get("priority"),
            ),
        }

        delay: Optional[float] = parse_val(float, message.labels.get("delay"))
        rmq_message: Message = Message(**message_base_params)

        if delay is None:
            exchange = await self.write_channel.get_exchange(
                self._exchange_name,
                ensure=False,
            )
            await exchange.publish(rmq_message, routing_key=message.task_name)
        elif self._delayed_message_exchange_plugin:
            rmq_message.headers["x-delay"] = int(delay * 1000)
            exchange = await self.write_channel.get_exchange(
                self._delay_plugin_exchange_name,
            )
            await exchange.publish(
                rmq_message,
                routing_key=self._routing_key,
            )
        else:
            rmq_message.expiration = timedelta(seconds=delay)
            await self.write_channel.default_exchange.publish(
                rmq_message,
                routing_key=self._delay_queue_name,
            )

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """
        Listen to queue.

        This function listens to queue and
        yields every new message.

        :yields: parsed broker message.
        :raises ValueError: if startup wasn't called.
        """
        if self.read_channel is None:
            raise ValueError("Call startup before starting listening.")
        await self.read_channel.set_qos(prefetch_count=self._qos)
        queue = await self.declare_queues(self.read_channel)
        async with queue.iterator() as iterator:
            async for message in iterator:
                yield AckableMessage(
                    data=message.body,
                    ack=message.ack,
                )
