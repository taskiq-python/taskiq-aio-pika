# AioPika broker for taskiq

This lirary provides you with aio-pika broker for taskiq.

Usage:
```python
from taskiq_aio_pika import AioPikaBroker

broker = AioPikaBroker()

@broker.task
async def test() -> None:
    print("nothing")

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
* `qos` - number of messages that worker can prefetch.
* `max_connection_pool_size` - maximum number of connections in pool.
* `max_channel_pool_size` - maximum number of channels for each connection.
