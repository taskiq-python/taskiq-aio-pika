"""Taskiq integration with aio-pika."""

from importlib.metadata import version

from taskiq_aio_pika.broker import AioPikaBroker
from taskiq_aio_pika.exchange import Exchange
from taskiq_aio_pika.queue import Queue, QueueType

__version__ = version("taskiq-aio-pika")

__all__ = [
    "AioPikaBroker",
    "Exchange",
    "Queue",
    "QueueType",
]
