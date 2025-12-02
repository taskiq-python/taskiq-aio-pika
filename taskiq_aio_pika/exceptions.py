class BaseAioPikaBrokerError(Exception):
    """Base exception for AioPika broker errors."""


class NoStartupError(BaseAioPikaBrokerError):
    """Raised when an operation is attempted before the broker has started."""


class IncorrectRoutingKeyError(BaseAioPikaBrokerError):
    """Raised when a message is received with an incorrect routing key."""


class QueueNotDeclaredError(BaseAioPikaBrokerError):
    """Raised when attempting to use a queue that has not been declared."""


class ExchangeNotDeclaredError(BaseAioPikaBrokerError):
    """Raised when attempting to use an exchange that has not been declared."""
