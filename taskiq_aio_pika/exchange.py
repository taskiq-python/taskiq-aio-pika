from dataclasses import dataclass, field

from aio_pika import ExchangeType
from pamqp.common import FieldTable


@dataclass(frozen=True)
class Exchange:
    """
    Represents a RabbitMQ exchange configuration.

    Attributes:
        name: The name of the exchange.
        type: The type of the exchange (topic, direct, fanout, headers).
        internal: Whether the exchange is internal.
        passive: Whether to check if the exchange exists without creating it.
        auto_delete: Whether the exchange should be auto-deleted.
        declare: Whether to declare the exchange on startup.
        durable: Whether the exchange should survive broker restarts.
        arguments: Additional arguments for the exchange declaration.
        timeout: Timeout for exchange declaration.
    """

    name: str = "taskiq"
    type: ExchangeType = ExchangeType.TOPIC
    internal: bool = False
    passive: bool = False
    auto_delete: bool = False
    declare: bool = True
    durable: bool = True
    arguments: FieldTable = field(default_factory=dict)
    timeout: int | float | None = None
