# AioPika broker for taskiq

This library provides you with aio-pika broker for taskiq.

Usage:
```python
from taskiq_aio_pika import AioPikaBroker

broker = AioPikaBroker()

@broker.task
async def test() -> None:
    print("nothing")

```

## Non-obvious things

You can send delayed messages and set priorities to messages using labels.

## Delays

### **Default retries**

To send delayed message, you have to specify
delay label. You can do it with `task` decorator,
or by using kicker.
In this type of delay we are using additional queue with `expiration` parameter and after with time message will be deleted from `delay` queue and sent to the main taskiq queue.
For example:

```python
broker = AioPikaBroker()

@broker.task(delay=3)
async def delayed_task() -> int:
    return 1

async def main():
    await broker.startup()
    # This message will be received by workers
    # After 3 seconds delay.
    await delayed_task.kiq()

    # This message is going to be received after the delay in 4 seconds.
    # Since we overriden the `delay` label using kicker.
    await delayed_task.kicker().with_labels(delay=4).kiq()

    # This message is going to be send immediately. Since we deleted the label.
    await delayed_task.kicker().with_labels(delay=None).kiq()

    # Of course the delay is managed by rabbitmq, so you don't
    # have to wait delay period before message is going to be sent.
```

### **Retries with `rabbitmq-delayed-message-exchange` plugin**

To send delayed message you can install `rabbitmq-delayed-message-exchange`
plugin https://github.com/rabbitmq/rabbitmq-delayed-message-exchange.

And you need to configure you broker.
There is `delayed_message_exchange_plugin` `AioPikaBroker` parameter and it must be `True` to turn on delayed message functionality.

The delay plugin can handle tasks with different delay times well, and the delay based on dead letter queue is suitable for tasks with the same delay time.
For example:

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
    # Since we overriden the `delay` label using kicker.
    await delayed_task.kicker().with_labels(delay=4).kiq()
```

## Priorities

You can define priorities for messages using `priority` label.
Messages with higher priorities are delivered faster.
But to use priorities you need to define `max_priority` of the main queue, by passing `max_priority` parameter in broker's init.
This parameter sets maximum priority for the queue and
declares it as the prority queue.

Before doing so please read the [documentation](https://www.rabbitmq.com/priority.html#behaviour) about what
downsides you get by using prioritized queues.


```python
broker = AioPikaBroker(max_priority=10)

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

## Configuration

AioPikaBroker parameters:
* `url` - url to rabbitmq. If None, "amqp://guest:guest@localhost:5672" is used.
* `result_backend` - custom result backend.
* `task_id_generator` - custom task_id genertaor.
* `exchange_name` - name of exchange that used to send messages.
* `exchange_type` - type of the exchange. Used only if `declare_exchange` is True.
* `queue_name` - queue that used to get incoming messages.
* `routing_key` - that used to bind that queue to the exchange.
* `declare_exchange` - whether you want to declare new exchange if it doesn't exist.
* `max_priority` - maximum priority for messages.
* `delay_queue_name` - custom delay queue name.
    This queue is used to deliver messages with delays.
* `dead_letter_queue_name` - custom dead letter queue name.
    This queue is used to receive negatively acknowleged messages from the main queue.
* `qos` - number of messages that worker can prefetch.
* `declare_queues` - whether you want to declare queues even on
    client side. May be useful for message persistance.
* `declare_queues_kwargs` - see [Custom Queue Arguments](#custom-queue-arguments) for more details.

## Custom Queue Arguments

You can pass custom arguments to the underlying RabbitMQ queue declaration by using the `declare_queues_kwargs` parameter of `AioPikaBroker`. If you want to set specific queue arguments (such as RabbitMQ extensions or custom behaviors), provide them in the `arguments` dictionary inside `declare_queues_kwargs`.

These arguments will be merged with the default arguments used by the broker (such as dead-lettering and priority settings).

**Example:**

```python
broker = AioPikaBroker(
    declare_queues_kwargs={
        "arguments": {
            "x-message-ttl": 60000,  # Set message TTL to 60 seconds
            "x-queue-type": "quorum",  # Use quorum queue type
        }
    }
)
```

This will ensure that the queue is created with your custom arguments, in addition to the broker's defaults.

