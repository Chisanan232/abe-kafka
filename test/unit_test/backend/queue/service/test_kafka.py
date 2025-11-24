"""Unit tests for KafkaMessageQueueBackend using kafka-python with async wrappers."""

import asyncio
import json
import os
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from abe_plugin.backends.message_queue.service.abe_kafka import KafkaMessageQueueBackend


class TestKafkaBackendFromEnv:
    def test_from_env_requires_bootstrap(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError):
                KafkaMessageQueueBackend.from_env()

    def test_from_env_defaults(self) -> None:
        with patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"}, clear=True):
            backend = KafkaMessageQueueBackend.from_env()
            assert backend._bootstrap_servers == ["localhost:9092"]
            assert backend._security_protocol == "PLAINTEXT"
            assert backend._topic_pattern == ".*"
            assert backend._consumer_group_prefix == "abe"
            assert backend._enable_auto_commit is True
            assert backend._auto_offset_reset == "latest"


class TestKafkaBackendPublish:
    @pytest.mark.asyncio
    async def test_publish_success(self) -> None:
        backend = KafkaMessageQueueBackend(bootstrap_servers=["localhost:9092"])  # values not used due to mocking

        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_future.get.side_effect = lambda timeout=None: SimpleNamespace(topic="t", partition=0, offset=1)
        mock_producer.send.return_value = mock_future

        with patch(
            "abe_plugin.backends.message_queue.service.abe_kafka.KafkaProducer",
            return_value=mock_producer,
        ) as _mock_cls:
            await backend.publish("test-topic", {"hello": "world"})

            mock_producer.send.assert_called_once()
            args, kwargs = mock_producer.send.call_args
            assert args[0] == "test-topic"
            # payload is provided pre-serialization; kafka-python applies serializer internally
            assert args[1] == {"hello": "world"}

    @pytest.mark.asyncio
    async def test_publish_serialization_error(self) -> None:
        backend = KafkaMessageQueueBackend(bootstrap_servers=["localhost:9092"])  # values not used due to mocking

        mock_producer = MagicMock()
        # Simulate serializer failure raised by send
        mock_producer.send.side_effect = TypeError("not serializable")

        with patch(
            "abe_plugin.backends.message_queue.service.abe_kafka.KafkaProducer",
            return_value=mock_producer,
        ):
            with pytest.raises(ValueError, match="JSON-serializable"):
                await backend.publish("test-topic", {"bad": object()})

    @pytest.mark.asyncio
    async def test_publish_runtime_error(self) -> None:
        backend = KafkaMessageQueueBackend(bootstrap_servers=["localhost:9092"])  # values not used due to mocking

        mock_producer = MagicMock()
        # Simulate network error in future.get
        mock_future = MagicMock()

        def raise_err(timeout: Any = None) -> Any:  # noqa: ANN401
            raise RuntimeError("broker down")

        mock_future.get.side_effect = raise_err
        mock_producer.send.return_value = mock_future

        with patch(
            "abe_plugin.backends.message_queue.service.abe_kafka.KafkaProducer",
            return_value=mock_producer,
        ):
            with pytest.raises(RuntimeError, match="Unable to publish message"):
                await backend.publish("test-topic", {"ok": True})


class TestKafkaBackendConsume:
    @pytest.mark.asyncio
    async def test_consume_success_one_message(self) -> None:
        backend = KafkaMessageQueueBackend(bootstrap_servers=["localhost:9092"], topic_pattern="^unit-.*$")

        # Build a fake consumer.poll that returns one record then empty then raises CancelledError to end
        polls = [
            {"tp1": [SimpleNamespace(value=json.dumps({"a": 1}).encode("utf-8"))]},
            {},
            "CANCEL",
        ]

        def fake_poll(timeout_ms: int = 1000):
            if polls:
                item = polls.pop(0)
                if item == "CANCEL":
                    raise asyncio.CancelledError()
                return item
            raise asyncio.CancelledError()

        mock_consumer = MagicMock()
        mock_consumer.poll.side_effect = fake_poll

        with patch(
            "abe_plugin.backends.message_queue.service.abe_kafka.KafkaConsumer",
            return_value=mock_consumer,
        ):

            async def runner():
                gen = backend.consume(group="g1")
                msgs = []
                try:
                    async for m in gen:
                        msgs.append(m)
                        if len(msgs) >= 1:
                            break
                finally:
                    await backend.close()
                return msgs

            msgs = await asyncio.wait_for(runner(), timeout=3)
            assert msgs == [{"a": 1}]

    @pytest.mark.asyncio
    async def test_consume_invalid_json_skipped(self) -> None:
        backend = KafkaMessageQueueBackend(bootstrap_servers=["localhost:9092"], topic_pattern="^unit-.*$")

        valid = json.dumps({"x": 2}).encode("utf-8")
        invalid = b"{invalid json"

        polls = [
            {"tp1": [SimpleNamespace(value=invalid)]},
            {"tp2": [SimpleNamespace(value=valid)]},
        ]

        def fake_poll(timeout_ms: int = 1000):
            if polls:
                return polls.pop(0)
            raise asyncio.CancelledError()

        mock_consumer = MagicMock()
        mock_consumer.poll.side_effect = fake_poll

        with patch(
            "abe_plugin.backends.message_queue.service.abe_kafka.KafkaConsumer",
            return_value=mock_consumer,
        ):

            async def runner():
                gen = backend.consume()
                msgs = []
                try:
                    async for m in gen:
                        msgs.append(m)
                        if len(msgs) >= 1:
                            break
                finally:
                    await backend.close()
                return msgs

            msgs = await asyncio.wait_for(runner(), timeout=3)
            assert msgs == [{"x": 2}]


class TestKafkaBackendClose:
    @pytest.mark.asyncio
    async def test_close_calls(self) -> None:
        backend = KafkaMessageQueueBackend(bootstrap_servers=["localhost:9092"])  # values not used due to mocking

        mock_producer = MagicMock()
        mock_consumer = MagicMock()

        with (
            patch(
                "abe_plugin.backends.message_queue.service.abe_kafka.KafkaProducer",
                return_value=mock_producer,
            ),
            patch(
                "abe_plugin.backends.message_queue.service.abe_kafka.KafkaConsumer",
                return_value=mock_consumer,
            ),
        ):
            # Start both
            await backend._ensure_producer()
            await backend._ensure_consumer(group=None)
            assert backend._producer_started and backend._consumer_started

            await backend.close()
            assert backend._producer_started is False
            assert backend._consumer_started is False
            mock_producer.flush.assert_called()
            mock_producer.close.assert_called()
            mock_consumer.close.assert_called()


class TestKafkaBackendAuthConfig:
    @pytest.mark.asyncio
    async def test_sasl_plain_from_env_and_init(self) -> None:
        env = {
            "KAFKA_BOOTSTRAP_SERVERS": "k1:9092,k2:9092",
            "KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
            "KAFKA_SASL_MECHANISM": "PLAIN",
            "KAFKA_SASL_USERNAME": "u",
            "KAFKA_SASL_PASSWORD": "p",
            "KAFKA_CLIENT_ID": "cid",
            "KAFKA_TOPIC_PATTERN": "^t-.*$",
            "KAFKA_AUTO_OFFSET_RESET": "earliest",
            "KAFKA_ENABLE_AUTO_COMMIT": "false",
        }
        with patch.dict(os.environ, env, clear=True):
            backend = KafkaMessageQueueBackend.from_env()

        mock_producer = MagicMock()
        mock_consumer = MagicMock()

        with (
            patch(
                "abe_plugin.backends.message_queue.service.abe_kafka.KafkaProducer",
                return_value=mock_producer,
            ) as prod_cls,
            patch(
                "abe_plugin.backends.message_queue.service.abe_kafka.KafkaConsumer",
                return_value=mock_consumer,
            ) as cons_cls,
        ):
            await backend._ensure_producer()
            await backend._ensure_consumer(group="g1")

            # Validate producer init args
            _args, kwargs = prod_cls.call_args
            assert kwargs["security_protocol"] == "SASL_PLAINTEXT"
            assert kwargs["sasl_mechanism"] == "PLAIN"
            assert kwargs["sasl_plain_username"] == "u"
            assert kwargs["sasl_plain_password"] == "p"
            assert kwargs["client_id"] == "cid"
            assert kwargs["bootstrap_servers"] == ["k1:9092", "k2:9092"]

            # Validate consumer init args
            _cargs, ckwargs = cons_cls.call_args
            assert ckwargs["security_protocol"] == "SASL_PLAINTEXT"
            assert ckwargs["sasl_mechanism"] == "PLAIN"
            assert ckwargs["sasl_plain_username"] == "u"
            assert ckwargs["sasl_plain_password"] == "p"
            assert ckwargs["group_id"] == "abe_g1"
            assert ckwargs["auto_offset_reset"] == "earliest"
            assert ckwargs["enable_auto_commit"] is False

    @pytest.mark.asyncio
    async def test_ssl_from_env_and_init(self) -> None:
        env = {
            "KAFKA_BOOTSTRAP_SERVERS": "k1:9093",
            "KAFKA_SECURITY_PROTOCOL": "SSL",
            "KAFKA_SSL_CAFILE": "/path/ca.pem",
            "KAFKA_SSL_CERTFILE": "/path/cert.pem",
            "KAFKA_SSL_KEYFILE": "/path/key.pem",
        }
        with patch.dict(os.environ, env, clear=True):
            backend = KafkaMessageQueueBackend.from_env()

        mock_producer = MagicMock()
        mock_consumer = MagicMock()

        with (
            patch(
                "abe_plugin.backends.message_queue.service.abe_kafka.KafkaProducer",
                return_value=mock_producer,
            ) as prod_cls,
            patch(
                "abe_plugin.backends.message_queue.service.abe_kafka.KafkaConsumer",
                return_value=mock_consumer,
            ) as cons_cls,
        ):
            await backend._ensure_producer()
            await backend._ensure_consumer(group=None)

            # Validate SSL args passed through
            _args, kwargs = prod_cls.call_args
            assert kwargs["security_protocol"] == "SSL"
            assert kwargs["ssl_cafile"] == "/path/ca.pem"
            assert kwargs["ssl_certfile"] == "/path/cert.pem"
            assert kwargs["ssl_keyfile"] == "/path/key.pem"

            _cargs, ckwargs = cons_cls.call_args
            assert ckwargs["security_protocol"] == "SSL"
            assert ckwargs["ssl_cafile"] == "/path/ca.pem"
            assert ckwargs["ssl_certfile"] == "/path/cert.pem"
            assert ckwargs["ssl_keyfile"] == "/path/key.pem"
