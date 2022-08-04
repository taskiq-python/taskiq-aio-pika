from asyncio import AbstractEventLoop
from logging import getLogger
from typing import Any, AsyncGenerator, Optional, TypeVar

from aio_pika import Channel, ExchangeType, Message, connect_robust
from aio_pika.abc import AbstractChannel, AbstractRobustConnection
from aio_pika.pool import Pool
from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.message import BrokerMessage

_T = TypeVar("_T")

logger = getLogger("taskiq.aio_pika_broker")


class AioPikaBroker(AsyncBroker):
    def __init__(
        self,
        result_backend: Optional[AsyncResultBackend[_T]] = None,
        qos: int = 10,
        loop: Optional[AbstractEventLoop] = None,
        max_channel_pool_size: int = 2,
        max_connection_pool_size: int = 10,
        exchange_name: str = "taskiq",
        queue_name: str = "taskiq",
        declare_exchange: bool = True,
        *connection_args: Any,
        **connection_kwargs: Any,
    ) -> None:
        super().__init__(result_backend)

        async def _get_rmq_connection() -> AbstractRobustConnection:
            return await connect_robust(*connection_args, **connection_kwargs)

        self.connection_pool: Pool[AbstractRobustConnection] = Pool(
            _get_rmq_connection,
            max_size=max_connection_pool_size,
            loop=loop,
        )

        async def get_channel() -> AbstractChannel:
            async with self.connection_pool.acquire() as connection:
                return await connection.channel()

        self.channel_pool: Pool[Channel] = Pool(
            get_channel,
            max_size=max_channel_pool_size,
            loop=loop,
        )

        self.exchange_name = exchange_name
        self.qos = qos
        self.declare_exchange = declare_exchange
        self.queue_name = queue_name

    async def startup(self) -> None:
        async with self.channel_pool.acquire() as channel:
            if self.declare_exchange:
                exchange = await channel.declare_exchange(
                    self.exchange_name,
                    type=ExchangeType.TOPIC,
                )
            else:
                exchange = await channel.get_exchange(self.exchange_name, ensure=False)
            queue = await channel.declare_queue(self.queue_name)
            await queue.bind(exchange=exchange, routing_key="*")

    async def kick(self, message: BrokerMessage) -> None:
        rmq_msg = Message(
            body=message.message.encode(),
            headers={
                "task_id": message.task_id,
                "task_name": message.task_name,
                **message.headers,
            },
        )
        async with self.channel_pool.acquire() as channel:
            exchange = await channel.get_exchange(self.exchange_name, ensure=False)
            await exchange.publish(rmq_msg, routing_key=message.task_id)

    async def listen(self) -> AsyncGenerator[BrokerMessage, None]:
        async with self.channel_pool.acquire() as channel:
            await channel.set_qos(prefetch_count=self.qos)
            queue = await channel.get_queue(self.queue_name, ensure=False)
            async with queue.iterator() as queue_iter:
                async for rmq_message in queue_iter:
                    async with rmq_message.process():
                        try:
                            yield BrokerMessage(
                                task_id=rmq_message.headers["task_id"],
                                task_name=rmq_message.headers["task_name"],
                                message=rmq_message.body,
                                headers=rmq_message.headers,
                            )
                        except (ValueError, LookupError) as exc:
                            logger.debug(
                                "Cannot read broker message %s",
                                exc,
                                exc_info=True,
                            )

    async def shutdown(self) -> None:
        await self.connection_pool.close()
