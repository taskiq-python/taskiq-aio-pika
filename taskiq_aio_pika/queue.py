import enum
from dataclasses import dataclass, field

from pamqp.common import FieldTable


class QueueType(str, enum.Enum):
    """Enum representing different types of RabbitMQ queues."""

    QUORUM = "quorum"
    CLASSIC = "classic"
    STREAM = "stream"


@dataclass(frozen=True)
class Queue:
    """
    Represents a RabbitMQ queue configuration.

    Attributes:
        name: The name of the queue.
        type: The type of the queue (quorum, classic, stream).
        declare: Whether to declare the queue on startup.
        durable: Whether the queue should survive broker restarts.
        exclusive: Whether the queue is exclusive to the connection.
        passive: Whether to check if the queue exists without creating it.
        auto_delete: Whether the queue should be auto-deleted.
        max_priority: The maximum priority for the queue.
        arguments: Additional arguments for the queue declaration.
        timeout: Timeout for queue declaration.
        routing_key: The routing key for the queue.
        bind_arguments: Arguments for binding the queue.
        bind_timeout: Timeout for binding the queue.
        consumer_arguments: Arguments for the consumer.
    """

    declare: bool = True

    # will be passed as arguments
    type: QueueType = QueueType.QUORUM

    # from declare_queue arguments
    name: str = "taskiq"
    durable: bool = True
    exclusive: bool = False
    passive: bool = False
    auto_delete: bool = False
    max_priority: int | None = None
    arguments: FieldTable = field(default_factory=dict)
    timeout: int | float | None = None

    # will be used during binding to tasks exchange
    routing_key: str | None = None
    bind_arguments: FieldTable = field(default_factory=dict)
    bind_timeout: int | float | None = None

    # will be used during message consumption
    consumer_arguments: FieldTable = field(default_factory=dict)
