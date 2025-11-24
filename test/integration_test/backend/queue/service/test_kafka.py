"""Integration tests using Testcontainers with a 3-thread model (main/producer/consumer)."""

import asyncio
import json
import queue
import threading
import time
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
