from typing import Any, AsyncGenerator, Dict, Optional, TypeVar
from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.message import TaskiqMessage
from aio_pika.abc import AbstractRobustConnection
from aio_pika.pool import Pool
from asyncio import AbstractEventLoop
from aio_pika import connect_robust, Channel, Message, ExchangeType

_T = TypeVar("_T")


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

        async def get_channel() -> Channel:
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

    async def kick(self, message: TaskiqMessage) -> None:
        rmq_msg = Message(
            body=message.json().encode(),
            content_type="application/json",
            headers={
                "task_id": message.task_id,
                "task_name": message.task_name,
            },
        )
        async with self.channel_pool.acquire() as channel:
            exchange = await channel.get_exchange(self.exchange_name, ensure=False)
            await exchange.publish(rmq_msg, routing_key=message.task_id)

    async def listen(self) -> AsyncGenerator[TaskiqMessage, None]:
        async with self.channel_pool.acquire() as channel:
            await channel.set_qos(prefetch_count=self.qos)
            queue = await channel.get_queue(self.queue_name, ensure=False)
            async with queue.iterator() as queue_iter:
                async for rmq_message in queue_iter:
                    async with rmq_message.process():
                        try:
                            yield TaskiqMessage.parse_raw(
                                rmq_message.body,
                                content_type=rmq_message.content_type,
                            )
                        except ValueError:
                            continue

    async def shutdown(self) -> None:
        await self.connection_pool.close()
