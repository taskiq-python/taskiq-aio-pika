__all__ = ("AioPikaRetryMiddleware",)

import random
from logging import getLogger
from typing import Any

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.exceptions import NoResultError
from taskiq.kicker import AsyncKicker
from taskiq.message import TaskiqMessage
from taskiq.result import TaskiqResult

_logger = getLogger("taskiq.aio_pika_retry_middleware")


class AioPikaRetryMiddleware(TaskiqMiddleware):
    """Middleware to retry tasks using AioPika delays.

    This middleware retries failed tasks with support for:
    - max retries
    - delay
    - jitter
    - exponential backoff

    Important:
        To work correctly, your AioPikaBroker **must** be initialized with
        `delayed_message_exchange_plugin=True`.
        Also ensure the plugin is enabled on RabbitMQ:
            rabbitmq-plugins enable rabbitmq_delayed_message_exchange
    """

    def __init__(
        self,
        default_retry_count: int = 3,
        default_retry_label: bool = False,
        no_result_on_retry: bool = True,
        default_delay: float = 5,
        use_jitter: bool = False,
        use_delay_exponent: bool = False,
        max_delay_exponent: float = 60,
    ) -> None:
        """
        Initialize retry middleware.

        :param default_retry_count: Default max retries if not specified.
        :param default_retry_label: Whether to retry tasks by default.
        :param no_result_on_retry: Replace result with NoResultError on retry.
        :param default_delay: Delay in seconds before retrying.
        :param use_jitter: Add random jitter to retry delay.
        :param use_delay_exponent: Apply exponential backoff to delay.
        :param max_delay_exponent: Maximum allowed delay when using backoff.
        """
        super().__init__()
        self.default_retry_count = default_retry_count
        self.default_retry_label = default_retry_label
        self.no_result_on_retry = no_result_on_retry
        self.default_delay = default_delay
        self.use_jitter = use_jitter
        self.use_delay_exponent = use_delay_exponent
        self.max_delay_exponent = max_delay_exponent

    def is_retry_on_error(self, message: TaskiqMessage) -> bool:
        """
        Check if retry is enabled for this task.

        Looks for `retry_on_error` label, falls back to default.

        :param message: Original task message.
        :return: True if should retry on error.
        """
        retry_on_error = message.labels.get("retry_on_error")
        if isinstance(retry_on_error, str):
            retry_on_error = retry_on_error.lower() == "true"
        if retry_on_error is None:
            retry_on_error = self.default_retry_label
        return retry_on_error

    def make_delay(self, message: TaskiqMessage, retries: int) -> float:
        """
        Calculate retry delay.

        Includes jitter and exponential backoff if enabled.

        :param message: Task message.
        :param retries: Current retry count.
        :return: Delay in seconds.
        """
        delay = float(message.labels.get("delay", self.default_delay))
        if self.use_delay_exponent:
            delay = min(delay * retries, self.max_delay_exponent)

        if self.use_jitter:
            delay += random.random()  # noqa: S311

        return delay

    async def on_error(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
        exception: BaseException,
    ) -> None:
        """
        Retry on error.

        If an error is raised during task execution,
        this middleware schedules the task to be retried
        after a calculated delay.

        :param message: Message that caused the error.
        :param result: Execution result.
        :param exception: Caught exception.
        """
        if isinstance(exception, NoResultError):
            return

        retry_on_error = self.is_retry_on_error(message)

        if not retry_on_error:
            return

        retries = int(message.labels.get("_retries", 0)) + 1
        max_retries = int(message.labels.get("max_retries", self.default_retry_count))

        if retries < max_retries:
            delay = self.make_delay(message, retries)

            _logger.info(
                "Task %s failed. Retrying %d/%d in %.2f seconds.",
                message.task_name,
                retries,
                max_retries,
                delay,
            )

            kicker: AsyncKicker[Any, Any] = (
                AsyncKicker(
                    task_name=message.task_name,
                    broker=self.broker,
                    labels=message.labels,
                )
                .with_task_id(message.task_id)
                .with_labels(_retries=retries, delay=delay)
            )

            await kicker.kiq(*message.args, **message.kwargs)

            if self.no_result_on_retry:
                result.error = NoResultError()

        else:
            _logger.warning(
                "Task '%s' invocation failed. Maximum retries count is reached.",
                message.task_name,
            )
