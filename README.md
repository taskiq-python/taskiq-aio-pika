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

To send delayed message, you need to specify queue for delayed messages. You can do it by passing `delay_queue` parameter to the broker. For example:

```python
from taskiq_aio_pika import AioPikaBroker, Queue, QueueType

broker = AioPikaBroker(
    ...,
    delay_queue=Queue(name="taskiq.delay_queue"),
)
```

After that you have to specify x_delay label. You can do it with `task` decorator, or by using kicker.

In this type of delay we are using additional queue with `expiration` parameter. After declared time message will be deleted from `delay` queue and sent to the main queue. For example:

```python
broker = AioPikaBroker(...)

@broker.task(x_delay=3)
async def delayed_task() -> int:
    return 1

async def main():
    await broker.startup()
    # This message will be received by workers
    # After 3 seconds delay.
    await delayed_task.kiq()

    # This message is going to be received after the delay in 4 seconds.
    # Since we overridden the `x_delay` label using kicker.
    await delayed_task.kicker().with_labels(x_delay=4).kiq()

    # This message is going to be send immediately. Since we deleted the label.
    await delayed_task.kicker().with_labels(x_delay=None).kiq()

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

@broker.task(x_delay=3)
async def delayed_task() -> int:
    return 1

async def main():
    await broker.startup()
    # This message will be received by workers
    # After 3 seconds delay.
    await delayed_task.kiq()

    # This message is going to be received after the delay in 4 seconds.
    # Since we overridden the `x_delay` label using kicker.
    await delayed_task.kicker().with_labels(x_delay=4).kiq()
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

## Poison message handling

Quorum queues (the default queue type used by this broker) keep track of how many times a message has been redelivered. This is useful for preventing "poison messages" — messages that crash the consumer (e.g. via an OOM) before it can ack, nack, or update a retry count — from being redelivered forever.

Set `delivery_limit` on a `Queue` to have RabbitMQ automatically dead-letter a message once it has been redelivered too many times, instead of requeuing it indefinitely:

```python
from taskiq_aio_pika import AioPikaBroker, Queue, QueueType

broker = AioPikaBroker(
    task_queues=[
        Queue(
            name="taskiq",
            type=QueueType.QUORUM,
            delivery_limit=5,
        ),
    ],
)
```

Once a message has been redelivered more than `delivery_limit` times, RabbitMQ dead-letters it to the broker's dead-letter queue instead of redelivering it again — no application code involved. `delivery_limit` is only supported by quorum queues. See the [RabbitMQ docs](https://www.rabbitmq.com/docs/quorum-queues#poison-message-handling) for details.

## Connection loss and message redelivery

RabbitMQ deliveries are acknowledged on the specific channel they were delivered on. If the underlying connection drops (a network blip, a broker restart, etc.), any message that was already delivered but not yet acked cannot be acked anymore, even after the connection reconnects — the delivery tag isn't valid on a new channel. `AioPikaBroker` handles this by logging a warning and letting the message go instead of crashing the worker; RabbitMQ automatically requeues the message once the old channel closes.

The practical consequence is that a task can run **more than once** whenever a connection is lost while the task is in flight — regardless of `ack_time`. This is a property of AMQP itself, not something a broker can paper over, so:

- Write tasks to be idempotent whenever you can (safe to execute twice with the same effect).
- If you can't, prefer `ack_time="when_received"` (the taskiq default) to shrink the window between delivery and ack, at the cost of losing the message outright if the worker crashes mid-task instead of duplicating it. `ack_time="when_executed"`/`"when_saved"` hold the message unacked for longer (through task execution / result saving), which widens the duplicate-delivery window but guarantees the message isn't lost if the worker itself crashes.

Publishing (`kick`) is affected too, but differently: right after a connection recovers, there's a short window (order of a second) where the write channel can still be mid-recovery. `AioPikaBroker` retries `kick` automatically in that case — configurable via the `retries` constructor argument:

```python
from taskiq_aio_pika import AioPikaBroker

broker = AioPikaBroker(
    retries={
        "kick": {
            "max_attempts": 4,  # set to 0 to disable retrying
            "backoff": 0.2,  # doubled after each retry
        },
    },
)
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
    ),
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
