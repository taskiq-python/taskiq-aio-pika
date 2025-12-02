# AioPika broker for taskiq

[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/taskiq-aio-pika?style=for-the-badge)](https://pypi.org/project/taskiq-aio-pika/)
[![PyPI](https://img.shields.io/pypi/v/taskiq-aio-pika?style=for-the-badge)](https://pypi.org/project/taskiq-aio-pika/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/taskiq-aio-pika?style=for-the-badge)](https://pypistats.org/packages/taskiq-aio-pika)

This library provides you with aio-pika broker for taskiq.

Features:
- Supports delayed messages using dead-letter queues or RabbitMQ delayed message exchange plugin.
- Supports message priorities.
- Supports multiple queues and custom routing.

Usage example:

```python
from taskiq_aio_pika import AioPikaBroker

broker = AioPikaBroker(...)

@broker.task
async def test() -> None:
    print("nothing")

```

## Delays

### Default delays

To send delayed message, you have to specify delay label. You can do it with `task` decorator, or by using kicker.

In this type of delay we are using additional queue with `expiration` parameter. After declared time message will be deleted from `delay` queue and sent to the main queue. For example:

```python
broker = AioPikaBroker(...)

@broker.task(delay=3)
async def delayed_task() -> int:
    return 1

async def main():
    await broker.startup()
    # This message will be received by workers
    # After 3 seconds delay.
    await delayed_task.kiq()

    # This message is going to be received after the delay in 4 seconds.
    # Since we overridden the `delay` label using kicker.
    await delayed_task.kicker().with_labels(delay=4).kiq()

    # This message is going to be send immediately. Since we deleted the label.
    await delayed_task.kicker().with_labels(delay=None).kiq()

    # Of course the delay is managed by rabbitmq, so you don't
    # have to wait delay period before message is going to be sent.
```

### Delays with `rabbitmq-delayed-message-exchange` plugin

First of all please make sure that your RabbitMQ server has [rabbitmq-delayed-message-exchange plugin](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange) installed.

Also you need to configure you broker by passing `delayed_message_exchange_plugin=True` to broker.

This plugin can handle tasks with different delay times well, and the delay based on dead letter queue is suitable for tasks with the same delay time. For example:

```python
broker = AioPikaBroker(
    delayed_message_exchange_plugin=True,
)

@broker.task(delay=3)
async def delayed_task() -> int:
    return 1

async def main():
    await broker.startup()
    # This message will be received by workers
    # After 3 seconds delay.
    await delayed_task.kiq()

    # This message is going to be received after the delay in 4 seconds.
    # Since we overridden the `delay` label using kicker.
    await delayed_task.kicker().with_labels(delay=4).kiq()
```

## Priorities

You can define priorities for messages using `priority` label. Messages with higher priorities are delivered faster.

Before doing so please read the [documentation](https://www.rabbitmq.com/priority.html#behaviour) about what
downsides you get by using prioritized queues.

```python
broker = AioPikaBroker(...)

# We can define default priority for tasks.
@broker.task(priority=2)
async def prio_task() -> int:
    return 1

async def main():
    await broker.startup()
    # This message has priority = 2.
    await prio_task.kiq()

    # This message is going to have priority 4.
    await prio_task.kicker().with_labels(priority=4).kiq()

    # This message is going to have priority 0.
    await prio_task.kicker().with_labels(priority=None).kiq()
```

## Custom Queue and Exchange arguments

You can pass custom arguments to the underlying RabbitMQ queues and exchange declaration by using the `Queue`/`Exchange` classes from `taskiq_aio_pika`. If you used `faststream` before you are probably familiar with this concept.

These arguments will be merged with the default arguments used by the broker
(such as dead-lettering and priority settings). If there are any conflicts, the values you provide will take precedence over the broker's defaults. Example:

```python
from taskiq_aio_pika import AioPikaBroker, Queue, QueueType, Exchange
from aio_pika.abc import ExchangeType

broker = AioPikaBroker(
    exchange=Exchange(
        name="custom_exchange",
        type=ExchangeType.TOPIC,
        declare=True,
        durable=True,
        auto_delete=False,
    )
    task_queues=[
        Queue(
            name="custom_queue",
            type=QueueType.CLASSIC,
            declare=True,
            durable=True,
            max_priority=10,
            routing_key="custom_queue",
        )
    ]
)
```

This will ensure that the queue is created with your custom arguments, in addition to the broker's defaults.


## Multiqueue support

You can define multiple queues for your tasks. Each queue can have its own routing key and other settings. And your workers can listen to multiple queues (or specific queue) as well.

You can check [multiqueue usage example](./examples/topic_with_two_queues.py) in examples folder for more details.
