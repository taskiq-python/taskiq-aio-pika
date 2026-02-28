import asyncio

import pytest
from aio_pika import Channel
from aio_pika.exceptions import QueueEmpty
from taskiq import BrokerMessage

from taskiq_aio_pika import AioPikaBroker


async def test_when_delayed_message_queue_exists__then_send_with_delay_must_work(
    broker: AioPikaBroker,
    test_channel: Channel,
    queue_name: str,
    delay_queue_name: str,
) -> None:
    delay_queue = await test_channel.get_queue(delay_queue_name)
    main_queue = await test_channel.get_queue(queue_name)
    broker_msg = BrokerMessage(
        task_id="1",
        task_name="name",
        message=b"message",
        labels={"delay": "2"},
    )
    await broker.kick(broker_msg)

    # We check that message appears in delay queue.
    delay_msg = await delay_queue.get()
    await delay_msg.nack(requeue=True)

    # After we wait the delay message must appear in
    # the main queue.
    await asyncio.sleep(2)

    # Check that it disappear.
    with pytest.raises(QueueEmpty):
        await delay_queue.get(no_ack=True)

    # Check that we can get the message.
    await main_queue.get()
