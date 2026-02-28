from taskiq import AckableMessage

from taskiq_aio_pika.broker import AioPikaBroker


async def get_first_task(broker: AioPikaBroker) -> AckableMessage:
    """
    Get first message from the queue.

    :param broker: async message broker.
    :return: first message from listen method
    """
    async for message in broker.listen():
        return message
    return None  # type: ignore
