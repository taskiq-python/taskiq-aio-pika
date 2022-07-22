from typing import Any, AsyncGenerator
from taskiq.abc.broker import AsyncBroker
from taskiq.message import TaskiqMessage

class AioPikaBroker(AsyncBroker):
    async def kick(self, task_name: str, *args: Any, **kwargs: Any) -> Any:
        print("AIO PIKA IS KICKING TASK.")
        return await super().kick(task_name, *args, **kwargs)

    async def listen(self) -> AsyncGenerator[TaskiqMessage, None]:
        print("Star listening")

        yield TaskiqMessage(
            task_name="noop",
            headers={},
            meta={},
            args=tuple(),
            kwargs={}
        )
