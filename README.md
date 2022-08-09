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