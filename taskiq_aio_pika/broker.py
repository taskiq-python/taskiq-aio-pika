import asyncio
from logging import getLogger
from typing import Any, Callable, Coroutine, Optional, TypeVar

from aio_pika import ExchangeType, Message, connect_robust
from aio_pika.abc import (
    AbstractChannel,
    AbstractIncomingMessage,
    AbstractRobustConnection,
)
from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.message import BrokerMessage

_T = TypeVar("_T")  # noqa: WPS111

logger = getLogger("taskiq.aio_pika_broker")


class AioPikaBroker(AsyncBroker):
    """Broker that works with RabbitMQ."""

    def __init__(  # noqa: WPS211
        self,
        url: Optional[str] = None,
        result_backend: Optional[AsyncResultBackend[_T]] = None,
        task_id_generator: Optional[Callable[[], str]] = None,
        qos: int = 10,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        exchange_name: str = "taskiq",
        queue_name: str = "taskiq",
        declare_exchange: bool = True,
        routing_key: str = "#",
        exchange_type: ExchangeType = ExchangeType.TOPIC,
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
        :param declare_exchange: whether you want to declare new exchange
            if it doesn't exist.
        :param routing_key: that used to bind that queue to the exchange.
        :param exchange_type: type of the exchange.
            Used only if `declare_exchange` is True.
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
        self._queue_name = queue_name
        self._routing_key = routing_key
        self.read_conn: Optional[AbstractRobustConnection] = None
        self.write_conn: Optional[AbstractRobustConnection] = None
        self.write_channel: Optional[AbstractChannel] = None
        self.read_channel: Optional[AbstractChannel] = None

    async def startup(self) -> None:  # noqa: WPS217
        """Create exchange and queue on startup."""
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
            exchange = await self.write_channel.declare_exchange(
                self._exchange_name,
                type=self._exchange_type,
            )
        else:
            exchange = await self.write_channel.get_exchange(
                self._exchange_name,
                ensure=False,
            )
        queue = await self.write_channel.declare_queue(self._queue_name)
        await queue.bind(exchange=exchange, routing_key=self._routing_key)

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
        rmq_msg = Message(
            body=message.message.encode(),
            headers={
                "task_id": message.task_id,
                "task_name": message.task_name,
                **message.labels,
            },
        )
        exchange = await self.write_channel.get_exchange(
            self._exchange_name,
            ensure=False,
        )
        await exchange.publish(rmq_msg, routing_key=message.task_name)

    async def listen(
        self,
        callback: Callable[[BrokerMessage], Coroutine[Any, Any, None]],
    ) -> None:
        """
        Listen to queue.

        This function listens to queue and calls
        callback on every new message.

        :param callback: function to call on new message.
        :raises ValueError: if startup wasn't called.
        """
        self.callback = callback
        if self.read_channel is None:
            raise ValueError("Call startup before starting listening.")
        await self.read_channel.set_qos(prefetch_count=self._qos)
        queue = await self.read_channel.get_queue(self._queue_name, ensure=False)
        await queue.consume(self.process_message)
        try:  # noqa: WPS501
            # Wait until terminate
            await asyncio.Future()
        finally:
            await self.shutdown()

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

    async def process_message(self, message: AbstractIncomingMessage) -> None:
        """
        Process received message.

        This function parses broker message and
        calls callback.

        :param message: received message.
        """
        async with message.process():
            try:
                broker_message = BrokerMessage(
                    task_id=message.headers.pop("task_id"),
                    task_name=message.headers.pop("task_name"),
                    message=message.body,
                    labels=message.headers,
                )
            except (ValueError, LookupError) as exc:
                logger.debug(
                    "Cannot read broker message %s",
                    exc,
                    exc_info=True,
                )
                return
            await self.callback(broker_message)
