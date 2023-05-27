"""Taskiq integration with aio-pika."""
from taskiq_aio_pika.broker import AioPikaBroker

try:
    # Python 3.8+
    from importlib.metadata import version  # noqa: WPS433
except ImportError:
    # Python 3.7
    from importlib_metadata import version  # noqa: WPS433, WPS440

__version__ = version("taskiq-aio-pika")

__all__ = ["AioPikaBroker"]
