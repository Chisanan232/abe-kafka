"""Integration tests using Testcontainers with a 3-thread model (main/producer/consumer)."""

import asyncio
import json
import os
import queue
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from abe_plugin.backends.message_queue.service.abe_kafka import KafkaMessageQueueBackend


def _docker_available() -> bool:
    try:
        import docker  # type: ignore

        client = docker.from_env()
        client.ping()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _docker_available(),
    reason="Docker not available; skipping Kafka Testcontainers integration tests",
)


def test_publish_and_consume_roundtrip_with_testcontainers_threads() -> None:
    from testcontainers.kafka import KafkaContainer  # type: ignore

    topic = f"abe_kafka_it_{uuid4().hex}"
    pattern = f"^{topic}$"
    payload = {"it": True, "id": uuid4().hex}

    # Use context manager to ensure container cleanup
    with KafkaContainer() as kafka:
        bootstrap = kafka.get_bootstrap_server()
        # testcontainers may return values like "PLAINTEXT://host:port"; kafka-python expects "host:port"
        if "://" in bootstrap:
            bootstrap = bootstrap.split("://", 1)[1]

        results: "queue.Queue[dict]" = queue.Queue()
        errors: "queue.Queue[BaseException]" = queue.Queue()

        def consumer_target() -> None:
            try:
                backend = KafkaMessageQueueBackend(
                    bootstrap_servers=[bootstrap],
                    topic_pattern=pattern,
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    consumer_group_prefix="abe_it",
                )

                async def run_consume() -> None:
                    try:
                        async for msg in backend.consume(group="g1"):
                            results.put(msg)
                            break
                    finally:
                        await backend.close()

                asyncio.run(run_consume())
            except BaseException as exc:  # capture and propagate to main thread
                errors.put(exc)  # noqa: TRY400

        def producer_target() -> None:
            try:
                backend = KafkaMessageQueueBackend(bootstrap_servers=[bootstrap])

                async def run_publish() -> None:
                    # Small delay to ensure consumer subscribes first
                    await asyncio.sleep(0.8)
                    await backend.publish(topic, payload)
                    await backend.close()

                asyncio.run(run_publish())
            except BaseException as exc:  # capture and propagate to main thread
                errors.put(exc)  # noqa: TRY400

        t_cons = threading.Thread(target=consumer_target, name="kafka-consumer", daemon=True)
        t_prod = threading.Thread(target=producer_target, name="kafka-producer", daemon=True)

        # Start producer first to ensure topic is auto-created and message is published
        t_prod.start()

        # Wait for producer to finish or error
        prod_deadline = time.time() + 15
        while t_prod.is_alive() and time.time() < prod_deadline:
            if not errors.empty():
                raise errors.get()
            time.sleep(0.1)
        assert not t_prod.is_alive(), "producer thread did not finish in time"

        # Then start consumer to read from earliest
        t_cons.start()

        # Main thread: monitor for result or failure with timeouts
        deadline = time.time() + 30
        received = None
        while time.time() < deadline:
            if not errors.empty():
                raise errors.get()
            try:
                received = results.get(timeout=0.5)
                break
            except queue.Empty:
                pass

        assert received == payload

        # Graceful shutdown: threads should exit after completing work
        t_prod.join(timeout=5)
        t_cons.join(timeout=5)
        assert not t_prod.is_alive(), "producer thread did not finish in time"
        assert not t_cons.is_alive(), "consumer thread did not finish in time"


def test_publish_and_consume_roundtrip_sasl_plain_env_threads() -> None:
    bootstrap_env = os.getenv("KAFKA_IT_BOOTSTRAP_SERVERS")
    sec = os.getenv("KAFKA_IT_SECURITY_PROTOCOL")
    mech = os.getenv("KAFKA_IT_SASL_MECHANISM")
    user = os.getenv("KAFKA_IT_SASL_USERNAME")
    pwd = os.getenv("KAFKA_IT_SASL_PASSWORD")
    mock_mode = not (bootstrap_env and sec and mech and user and pwd)

    if mock_mode:
        # Provide placeholder values for mocking verification
        bootstrap_env = "localhost:9092"
        sec = "SASL_PLAINTEXT"
        mech = "PLAIN"
        user = "user_mock"
        pwd = "pass_mock"

    topic = os.getenv("KAFKA_IT_TOPIC", f"abe_kafka_it_{uuid4().hex}")
    pattern = f"^{topic}$"
    payload = {"it": True, "id": uuid4().hex}

    bootstrap = bootstrap_env.split(",")[0].strip()
    if "://" in bootstrap:
        bootstrap = bootstrap.split("://", 1)[1]

    results: "queue.Queue[dict]" = queue.Queue()
    errors: "queue.Queue[BaseException]" = queue.Queue()

    def consumer_target() -> None:
        try:
            backend = KafkaMessageQueueBackend(
                bootstrap_servers=[bootstrap],
                topic_pattern=pattern,
                auto_offset_reset="latest",
                enable_auto_commit=False,
                consumer_group_prefix="abe_it",
                security_protocol=sec,  # type: ignore[arg-type]
                sasl_mechanism=mech,  # type: ignore[arg-type]
                sasl_username=user,  # type: ignore[arg-type]
                sasl_password=pwd,  # type: ignore[arg-type]
            )

            async def run_consume() -> None:
                try:
                    async for msg in backend.consume(group="g1"):
                        if msg == payload:
                            results.put(msg)
                            break
                finally:
                    await backend.close()

            asyncio.run(run_consume())
        except BaseException as exc:
            errors.put(exc)

    def producer_target() -> None:
        try:
            backend = KafkaMessageQueueBackend(
                bootstrap_servers=[bootstrap],
                security_protocol=sec,  # type: ignore[arg-type]
                sasl_mechanism=mech,  # type: ignore[arg-type]
                sasl_username=user,  # type: ignore[arg-type]
                sasl_password=pwd,  # type: ignore[arg-type]
            )

            async def run_publish() -> None:
                await asyncio.sleep(0.8)
                await backend.publish(topic, payload)
                await backend.close()

            asyncio.run(run_publish())
        except BaseException as exc:
            errors.put(exc)

    if mock_mode:
        # Patch Kafka clients to avoid real network and assert init kwargs
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_future.get.side_effect = lambda timeout=None: SimpleNamespace(topic=topic, partition=0, offset=1)
        mock_producer.send.return_value = mock_future

        mock_consumer = MagicMock()
        poll_items = (
            {"tp": [SimpleNamespace(value=json.dumps(payload).encode("utf-8"))]},
            {},
        )
        poll_iter = iter(poll_items)

        def fake_poll(timeout_ms: int = 1000):  # noqa: ANN001
            try:
                return next(poll_iter)
            except StopIteration:
                return {}

        mock_consumer.poll.side_effect = fake_poll

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
            t_cons = threading.Thread(target=consumer_target, name="kafka-consumer", daemon=True)
            t_prod = threading.Thread(target=producer_target, name="kafka-producer", daemon=True)
            t_cons.start()
            t_prod.start()

            deadline = time.time() + 10
            received = None
            while time.time() < deadline:
                if not errors.empty():
                    raise errors.get()
                try:
                    received = results.get(timeout=0.5)
                    break
                except queue.Empty:
                    pass
            assert received == payload

            # Ensure producer/consumer threads finished to avoid race on call_args
            t_prod.join(timeout=3)
            t_cons.join(timeout=3)

            # If still not called, wait briefly
            end = time.time() + 2
            while not prod_cls.called and time.time() < end:
                time.sleep(0.05)
            while not cons_cls.called and time.time() < end:
                time.sleep(0.05)

            # Verify init kwargs include SASL params
            assert prod_cls.called, "KafkaProducer was not constructed"
            assert cons_cls.called, "KafkaConsumer was not constructed"
            _p_args, p_kwargs = prod_cls.call_args
            assert p_kwargs["security_protocol"] == sec
            assert p_kwargs["sasl_mechanism"] == mech
            assert p_kwargs["sasl_plain_username"] == user
            assert p_kwargs["sasl_plain_password"] == pwd

            _c_args, c_kwargs = cons_cls.call_args
            assert c_kwargs["security_protocol"] == sec
            assert c_kwargs["sasl_mechanism"] == mech
            assert c_kwargs["sasl_plain_username"] == user
            assert c_kwargs["sasl_plain_password"] == pwd

            return

    # Real cluster mode
    t_cons = threading.Thread(target=consumer_target, name="kafka-consumer", daemon=True)
    t_prod = threading.Thread(target=producer_target, name="kafka-producer", daemon=True)

    t_cons.start()
    t_prod.start()

    deadline = time.time() + 30
    received = None
    while time.time() < deadline:
        if not errors.empty():
            raise errors.get()
        try:
            received = results.get(timeout=0.5)
            break
        except queue.Empty:
            pass

    assert received == payload

    t_prod.join(timeout=5)
    t_cons.join(timeout=5)
    assert not t_prod.is_alive(), "producer thread did not finish in time"
    assert not t_cons.is_alive(), "consumer thread did not finish in time"


def test_publish_and_consume_roundtrip_ssl_env_threads() -> None:
    bootstrap_env = os.getenv("KAFKA_IT_BOOTSTRAP_SERVERS")
    sec = os.getenv("KAFKA_IT_SECURITY_PROTOCOL")
    cafile = os.getenv("KAFKA_IT_SSL_CAFILE")
    certfile = os.getenv("KAFKA_IT_SSL_CERTFILE")
    keyfile = os.getenv("KAFKA_IT_SSL_KEYFILE")
    mock_mode = not (bootstrap_env and sec and cafile and certfile and keyfile)

    if mock_mode:
        bootstrap_env = "localhost:9093"
        sec = "SSL"
        cafile = "/tmp/ca.pem"
        certfile = "/tmp/cert.pem"
        keyfile = "/tmp/key.pem"

    topic = os.getenv("KAFKA_IT_TOPIC", f"abe_kafka_it_{uuid4().hex}")
    pattern = f"^{topic}$"
    payload = {"it": True, "id": uuid4().hex}

    bootstrap = bootstrap_env.split(",")[0].strip()
    if "://" in bootstrap:
        bootstrap = bootstrap.split("://", 1)[1]

    results: "queue.Queue[dict]" = queue.Queue()
    errors: "queue.Queue[BaseException]" = queue.Queue()

    def consumer_target() -> None:
        try:
            backend = KafkaMessageQueueBackend(
                bootstrap_servers=[bootstrap],
                topic_pattern=pattern,
                auto_offset_reset="latest",
                enable_auto_commit=False,
                consumer_group_prefix="abe_it",
                security_protocol=sec,  # type: ignore[arg-type]
                ssl_cafile=cafile,  # type: ignore[arg-type]
                ssl_certfile=certfile,  # type: ignore[arg-type]
                ssl_keyfile=keyfile,  # type: ignore[arg-type]
            )

            async def run_consume() -> None:
                try:
                    async for msg in backend.consume(group="g1"):
                        if msg == payload:
                            results.put(msg)
                            break
                finally:
                    await backend.close()

            asyncio.run(run_consume())
        except BaseException as exc:
            errors.put(exc)

    def producer_target() -> None:
        try:
            backend = KafkaMessageQueueBackend(
                bootstrap_servers=[bootstrap],
                security_protocol=sec,  # type: ignore[arg-type]
                ssl_cafile=cafile,  # type: ignore[arg-type]
                ssl_certfile=certfile,  # type: ignore[arg-type]
                ssl_keyfile=keyfile,  # type: ignore[arg-type]
            )

            async def run_publish() -> None:
                await asyncio.sleep(0.8)
                await backend.publish(topic, payload)
                await backend.close()

            asyncio.run(run_publish())
        except BaseException as exc:
            errors.put(exc)

    if mock_mode:
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_future.get.side_effect = lambda timeout=None: SimpleNamespace(topic=topic, partition=0, offset=1)
        mock_producer.send.return_value = mock_future

        mock_consumer = MagicMock()
        poll_items = (
            {"tp": [SimpleNamespace(value=json.dumps(payload).encode("utf-8"))]},
            {},
        )
        poll_iter = iter(poll_items)

        def fake_poll(timeout_ms: int = 1000):  # noqa: ANN001
            try:
                return next(poll_iter)
            except StopIteration:
                return {}

        mock_consumer.poll.side_effect = fake_poll

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
            t_cons = threading.Thread(target=consumer_target, name="kafka-consumer", daemon=True)
            t_prod = threading.Thread(target=producer_target, name="kafka-producer", daemon=True)
            t_cons.start()
            t_prod.start()

            deadline = time.time() + 10
            received = None
            while time.time() < deadline:
                if not errors.empty():
                    raise errors.get()
                try:
                    received = results.get(timeout=0.5)
                    break
                except queue.Empty:
                    pass
            assert received == payload

            # Ensure producer/consumer threads finished to avoid race on call_args
            t_prod.join(timeout=3)
            t_cons.join(timeout=3)

            # If still not called, wait briefly
            end = time.time() + 2
            while not prod_cls.called and time.time() < end:
                time.sleep(0.05)
            while not cons_cls.called and time.time() < end:
                time.sleep(0.05)

            # Verify init kwargs include SSL params
            assert prod_cls.called, "KafkaProducer was not constructed"
            assert cons_cls.called, "KafkaConsumer was not constructed"
            _p_args, p_kwargs = prod_cls.call_args
            assert p_kwargs["security_protocol"] == sec
            assert p_kwargs["ssl_cafile"] == cafile
            assert p_kwargs["ssl_certfile"] == certfile
            assert p_kwargs["ssl_keyfile"] == keyfile

            _c_args, c_kwargs = cons_cls.call_args
            assert c_kwargs["security_protocol"] == sec
            assert c_kwargs["ssl_cafile"] == cafile
            assert c_kwargs["ssl_certfile"] == certfile
            assert c_kwargs["ssl_keyfile"] == keyfile

            return

    # Real cluster mode
    t_cons = threading.Thread(target=consumer_target, name="kafka-consumer", daemon=True)
    t_prod = threading.Thread(target=producer_target, name="kafka-producer", daemon=True)

    t_cons.start()
    t_prod.start()

    deadline = time.time() + 30
    received = None
    while time.time() < deadline:
        if not errors.empty():
            raise errors.get()
        try:
            received = results.get(timeout=0.5)
            break
        except queue.Empty:
            pass

    assert received == payload

    t_prod.join(timeout=5)
    t_cons.join(timeout=5)
    assert not t_prod.is_alive(), "producer thread did not finish in time"
    assert not t_cons.is_alive(), "consumer thread did not finish in time"
