import asyncio
from datetime import timedelta
from logging import getLogger
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, TypeVar

from aio_pika import DeliveryMode, ExchangeType, Message, connect_robust
from aio_pika.abc import AbstractChannel, AbstractQueue, AbstractRobustConnection
from aiostream import stream
from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.message import BrokerMessage

_T = TypeVar("_T")  # noqa: WPS111

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

    def __init__(  # noqa: WPS211
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
        exchange_type: ExchangeType = ExchangeType.TOPIC,
        max_priority: Optional[int] = None,
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
        :param exchange_type: type of the exchange.
            Used only if `declare_exchange` is True.
        :param max_priority: maximum priority value for messages.
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
        self._declare_queues = declare_queues
        self._queue_name = queue_name
        self._max_priority = max_priority
        self._queue_name_list: List[str] = []

        self._dead_letter_queue_name = f"{queue_name}.dead_letter"
        if dead_letter_queue_name:
            self._dead_letter_queue_name = dead_letter_queue_name

        self._delay_queue_name = f"{queue_name}.delay"
        if delay_queue_name:
            self._delay_queue_name = delay_queue_name

        self.read_conn: Optional[AbstractRobustConnection] = None
        self.write_conn: Optional[AbstractRobustConnection] = None
        self.write_channel: Optional[AbstractChannel] = None
        self.read_channel: Optional[AbstractChannel] = None

    async def startup(self) -> None:  # noqa: WPS217
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
            )
        if self._declare_queues:
            await self.declare_queues(self.write_channel)

    def add_queue(self, queue_name: str) -> "AioPikaBroker":
        """
        This function is add queue name.

        :param queue_name: queue_name.
        :return: AioPikaBroker
        """
        self._queue_name_list.append(queue_name)
        return self

    async def declare_queues(
        self,
        channel: AbstractChannel,
    ) -> List[AbstractQueue]:
        """
        This function is used to declare queues.

        It's useful since aio-pika have automatic
        recover mechanism, which works only if
        the queue, you're going to listen was
        declared by aio-pika.

        :param channel: channel to used for declaration.
        :return: main queue instance.
        """
        queue_list = []

        async def bind_queue(queue_name: str) -> AbstractQueue:
            queue = await channel.declare_queue(
                queue_name,
                arguments=args,
            )
            await queue.bind(exchange=self._exchange_name, routing_key=queue_name)
            return queue

        await channel.declare_queue(
            self._dead_letter_queue_name,
        )
        args: "Dict[str, Any]" = {
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": self._dead_letter_queue_name,
        }
        if self._max_priority is not None:
            args["x-max-priority"] = self._max_priority
        queue_list.append(await bind_queue(self._queue_name))
        await channel.declare_queue(
            self._delay_queue_name,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": self._queue_name,
            },
        )
        if self._queue_name_list:
            for _queue_name in self._queue_name_list:
                queue_list.append(await bind_queue(_queue_name))

        return queue_list

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
        priority = parse_val(int, message.labels.get("priority"))
        rmq_msg = Message(
            body=message.message,
            headers={
                "task_id": message.task_id,
                "task_name": message.task_name,
                **message.labels,
            },
            delivery_mode=DeliveryMode.PERSISTENT,
            priority=priority,
        )
        delay = parse_val(int, message.labels.get("delay"))
        routing_key_name = message.queue_name or self._queue_name  # type: ignore # todo
        if delay is None:
            exchange = await self.write_channel.get_exchange(
                self._exchange_name,
                ensure=False,
            )
            await exchange.publish(rmq_msg, routing_key=routing_key_name)
        else:
            rmq_msg.expiration = timedelta(seconds=delay)
            await self.write_channel.default_exchange.publish(
                rmq_msg,
                routing_key=self._delay_queue_name,
            )

    async def listen(self) -> AsyncGenerator[bytes, None]:
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
        queue_list = await self.declare_queues(self.read_channel)

        async def body(queue: AbstractQueue) -> AsyncGenerator[bytes, None]:
            async with queue.iterator() as iterator:
                async for message in iterator:
                    async with message.process():
                        yield message.body

        combine = stream.merge(*[body(queue) for queue in queue_list])

        async with combine.stream() as streamer:
            async for message_body in streamer:
                yield message_body

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
