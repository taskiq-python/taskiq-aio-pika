"""Taskiq integration with aio-pika."""
from importlib.metadata import version

from taskiq_aio_pika.broker import AioPikaBroker

__version__ = version("taskiq-aio-pika")

__all__ = ["AioPikaBroker"]
