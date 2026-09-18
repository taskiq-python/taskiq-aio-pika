from typing_extensions import TypedDict


class KickRetries(TypedDict, total=False):
    """Retry policy for `AioPikaBroker.kick`."""

    max_attempts: int
    backoff: float


class RetriesConfig(TypedDict, total=False):
    """Per-operation retry policies for `AioPikaBroker`."""

    kick: KickRetries


DEFAULT_KICK_RETRIES: KickRetries = {"max_attempts": 4, "backoff": 0.2}
