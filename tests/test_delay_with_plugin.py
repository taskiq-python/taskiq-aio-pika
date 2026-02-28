import asyncio

import pytest
from aio_pika import Channel
from aio_pika.exceptions import QueueEmpty
from taskiq import BrokerMessage

from taskiq_aio_pika import AioPikaBroker


async def test_when_delayed_message_plugin_enabled__then_send_with_delay_must_work(
    broker_with_delayed_message_plugin: AioPikaBroker,
    test_channel: Channel,
    queue_name: str,
) -> None:
    # given
    main_queue = await test_channel.get_queue(queue_name)
    broker_msg = BrokerMessage(
        task_id="1",
        task_name="name",
        message=b"message",
        labels={"delay": "2"},
    )

    # when & then
    await broker_with_delayed_message_plugin.kick(broker_msg)
    with pytest.raises(QueueEmpty):
        await main_queue.get(no_ack=True)
    await asyncio.sleep(2)
    assert await main_queue.get()
