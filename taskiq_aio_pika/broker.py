from asyncio import AbstractEventLoop
from logging import getLogger
from typing import Any, AsyncGenerator, Callable, Optional, TypeVar

from aio_pika import Channel, ExchangeType, Message, connect_robust
from aio_pika.abc import AbstractChannel, AbstractRobustConnection
from aio_pika.pool import Pool
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
        loop: Optional[AbstractEventLoop] = None,
        max_channel_pool_size: int = 2,
        max_connection_pool_size: int = 10,
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
        :param max_channel_pool_size: maximum number of channels for each connection.
        :param max_connection_pool_size: maximum number of connections in pool.
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

        async def _get_rmq_connection() -> AbstractRobustConnection:
            return await connect_robust(
                url,
                loop=loop,
                **connection_kwargs,
            )

        self._connection_pool: Pool[AbstractRobustConnection] = Pool(
            _get_rmq_connection,
            max_size=max_connection_pool_size,
            loop=loop,
        )

        async def get_channel() -> AbstractChannel:
            async with self._connection_pool.acquire() as connection:
                return await connection.channel()

        self._channel_pool: Pool[Channel] = Pool(
            get_channel,
            max_size=max_channel_pool_size,
            loop=loop,
        )

        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
        self._qos = qos
        self._declare_exchange = declare_exchange
        self._queue_name = queue_name
        self._routing_key = routing_key

    async def startup(self) -> None:
        """Create exchange and queue on startup."""
        async with self._channel_pool.acquire() as channel:
            if self._declare_exchange:
                exchange = await channel.declare_exchange(
                    self._exchange_name,
                    type=self._exchange_type,
                )
            else:
                exchange = await channel.get_exchange(self._exchange_name, ensure=False)
            queue = await channel.declare_queue(self._queue_name)
            await queue.bind(exchange=exchange, routing_key=self._routing_key)

    async def kick(self, message: BrokerMessage) -> None:
        """
        Send message to the exchange.

        This function constructs rmq message
        and sends it.

        The message has task_id and task_name and labels
        in headers. And message's routing key is the same
        as the task_name.

        :param message: message to send.
        """
        rmq_msg = Message(
            body=message.message.encode(),
            headers={
                "task_id": message.task_id,
                "task_name": message.task_name,
                **message.labels,
            },
        )
        async with self._channel_pool.acquire() as channel:
            exchange = await channel.get_exchange(self._exchange_name, ensure=False)
            await exchange.publish(rmq_msg, routing_key=message.task_name)

    async def listen(self) -> AsyncGenerator[BrokerMessage, None]:
        """
        Listen to queue.

        This function listens to queue and yields
        new messages.

        :yield: parsed broker messages.
        """
        async with self._channel_pool.acquire() as channel:
            await channel.set_qos(prefetch_count=self._qos)
            queue = await channel.get_queue(self._queue_name, ensure=False)
            async with queue.iterator() as queue_iter:
                async for rmq_message in queue_iter:
                    async with rmq_message.process():
                        try:
                            yield BrokerMessage(
                                task_id=rmq_message.headers.pop("task_id"),
                                task_name=rmq_message.headers.pop("task_name"),
                                message=rmq_message.body,
                                labels=rmq_message.headers,
                            )
                        except (ValueError, LookupError) as exc:
                            logger.debug(
                                "Cannot read broker message %s",
                                exc,
                                exc_info=True,
                            )

    async def shutdown(self) -> None:
        """Close all connections on shutdown."""
        await super().shutdown()
        await self._connection_pool.close()
