from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, patch

import aiormq
import pytest
from aio_pika.abc import AbstractIncomingMessage
from taskiq import BrokerMessage

from taskiq_aio_pika import AioPikaBroker


class TestPublishWithRetry:
    async def test_when_publish_succeeds_immediately__it_is_called_only_once(
        self,
    ) -> None:
        broker = AioPikaBroker(retries={"kick": {"max_attempts": 4, "backoff": 0.01}})
        publish = AsyncMock(return_value=None)

        await broker._publish_with_retry(publish)

        assert publish.await_count == 1

    async def test_when_publish_fails_then_recovers__it_retries_until_success(
        self,
    ) -> None:
        broker = AioPikaBroker(retries={"kick": {"max_attempts": 4, "backoff": 0.01}})
        publish = AsyncMock(
            side_effect=[
                aiormq.exceptions.ChannelInvalidStateError("simulated"),
                aiormq.exceptions.ChannelInvalidStateError("simulated"),
                None,
            ],
        )

        await broker._publish_with_retry(publish)

        assert publish.await_count == 3

    async def test_when_publish_always_fails__it_gives_up_after_max_attempts(
        self,
    ) -> None:
        broker = AioPikaBroker(retries={"kick": {"max_attempts": 2, "backoff": 0.01}})
        publish = AsyncMock(
            side_effect=aiormq.exceptions.ChannelInvalidStateError("always failing"),
        )

        with pytest.raises(aiormq.exceptions.ChannelInvalidStateError):
            await broker._publish_with_retry(publish)

        # the initial attempt plus `max_attempts` retries
        assert publish.await_count == 3

    async def test_when_max_attempts_is_zero__it_fails_on_first_error_without_retrying(
        self,
    ) -> None:
        broker = AioPikaBroker(retries={"kick": {"max_attempts": 0, "backoff": 0.01}})
        publish = AsyncMock(
            side_effect=aiormq.exceptions.ChannelInvalidStateError("simulated"),
        )

        with pytest.raises(aiormq.exceptions.ChannelInvalidStateError):
            await broker._publish_with_retry(publish)

        assert publish.await_count == 1

    async def test_when_publish_raises_other_error__it_is_not_retried(self) -> None:
        broker = AioPikaBroker(retries={"kick": {"max_attempts": 4, "backoff": 0.01}})
        publish = AsyncMock(side_effect=ValueError("not a channel-recovery error"))

        with pytest.raises(ValueError, match="not a channel-recovery error"):
            await broker._publish_with_retry(publish)

        assert publish.await_count == 1

    async def test_when_retrying__backoff_doubles_between_attempts(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        broker = AioPikaBroker(retries={"kick": {"max_attempts": 3, "backoff": 0.1}})
        publish = AsyncMock(
            side_effect=[
                aiormq.exceptions.ChannelInvalidStateError("simulated"),
                aiormq.exceptions.ChannelInvalidStateError("simulated"),
                aiormq.exceptions.ChannelInvalidStateError("simulated"),
                None,
            ],
        )
        sleep_delays: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleep_delays.append(delay)

        monkeypatch.setattr("taskiq_aio_pika.broker.asyncio.sleep", fake_sleep)

        await broker._publish_with_retry(publish)

        assert sleep_delays == [0.1, 0.2, 0.4]


class TestSafeAck:
    async def test_when_ack_succeeds__nothing_special_happens(self) -> None:
        message = SimpleNamespace(ack=AsyncMock(return_value=None))
        await AioPikaBroker._safe_ack(
            cast(AbstractIncomingMessage, message),
            "some_queue",
        )
        message.ack.assert_awaited_once()

    async def test_when_channel_invalid_state_error_raised__it_is_swallowed_and_logged(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        message = SimpleNamespace(
            ack=AsyncMock(
                side_effect=aiormq.exceptions.ChannelInvalidStateError("simulated"),
            ),
            delivery_tag=42,
            redelivered=False,
        )
        with caplog.at_level("WARNING", logger="taskiq.aio_pika_broker"):
            await AioPikaBroker._safe_ack(
                cast(AbstractIncomingMessage, message),
                "some_queue",
            )
        assert any(
            "42" in record.message and "some_queue" in record.message
            for record in caplog.records
        )

    async def test_when_other_error_raised__it_propagates(self) -> None:
        message = SimpleNamespace(ack=AsyncMock(side_effect=ValueError("boom")))
        with pytest.raises(ValueError, match="boom"):
            await AioPikaBroker._safe_ack(
                cast(AbstractIncomingMessage, message),
                "some_queue",
            )


class TestKickRetriesIntegration:
    async def test_when_write_channel_is_transiently_invalid__kick_retries_and_succeeds(
        self,
        broker: AioPikaBroker,
    ) -> None:
        publish = AsyncMock(
            side_effect=[
                aiormq.exceptions.ChannelInvalidStateError("simulated recovery race"),
                aiormq.exceptions.ChannelInvalidStateError("simulated recovery race"),
                None,
            ],
        )
        broker._retries["kick"]["backoff"] = 0.01
        stub_exchange = SimpleNamespace(publish=publish)
        with patch.object(
            broker.write_channel,
            "get_exchange",
            AsyncMock(return_value=stub_exchange),
        ):
            await broker.kick(
                BrokerMessage(
                    task_id="1",
                    task_name="t1",
                    message=b"payload",
                    labels={},
                ),
            )
        assert publish.await_count == 3

    async def test_when_write_channel_never_recovers__kick_gives_up_and_raises(
        self,
        broker: AioPikaBroker,
    ) -> None:
        publish = AsyncMock(
            side_effect=aiormq.exceptions.ChannelInvalidStateError("always failing"),
        )
        broker._retries["kick"] = {"max_attempts": 2, "backoff": 0.01}
        stub_exchange = SimpleNamespace(publish=publish)
        with (
            patch.object(
                broker.write_channel,
                "get_exchange",
                AsyncMock(return_value=stub_exchange),
            ),
            pytest.raises(aiormq.exceptions.ChannelInvalidStateError),
        ):
            await broker.kick(
                BrokerMessage(
                    task_id="1",
                    task_name="t1",
                    message=b"payload",
                    labels={},
                ),
            )
        assert publish.await_count == 3
