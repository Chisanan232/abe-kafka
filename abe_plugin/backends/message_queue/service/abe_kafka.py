"""
Kafka implementation of the MessageQueueBackend protocol.

This backend uses aiokafka to provide producer/consumer capabilities that
conform to the Abstract Backend message-queue protocol.
"""

import asyncio
import json
import logging
import os
from typing import Any, AsyncIterator, Dict, List, Optional

from abe.backends.message_queue.base.protocol import MessageQueueBackend

from kafka import KafkaConsumer, KafkaProducer


logger = logging.getLogger(__name__)


class KafkaMessageQueueBackend(MessageQueueBackend):
    """Kafka-backed implementation of MessageQueueBackend using aiokafka."""

    def __init__(
        self,
        *,
        bootstrap_servers: List[str],
        security_protocol: str = "PLAINTEXT",
        sasl_mechanism: Optional[str] = None,
        sasl_username: Optional[str] = None,
        sasl_password: Optional[str] = None,
        ssl_cafile: Optional[str] = None,
        ssl_certfile: Optional[str] = None,
        ssl_keyfile: Optional[str] = None,
        client_id: Optional[str] = None,
        topic_pattern: str = ".*",
        consumer_group_prefix: str = "abe",
        enable_auto_commit: bool = True,
        auto_offset_reset: str = "latest",
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._security_protocol = security_protocol
        self._sasl_mechanism = sasl_mechanism
        self._sasl_username = sasl_username
        self._sasl_password = sasl_password
        self._ssl_cafile = ssl_cafile
        self._ssl_certfile = ssl_certfile
        self._ssl_keyfile = ssl_keyfile
        self._client_id = client_id
        self._topic_pattern = topic_pattern
        self._consumer_group_prefix = consumer_group_prefix
        self._enable_auto_commit = enable_auto_commit
        self._auto_offset_reset = auto_offset_reset

        self._producer: Optional[KafkaProducer] = None
        self._consumer: Optional[KafkaConsumer] = None
        self._producer_started: bool = False
        self._consumer_started: bool = False

    # ---------------------- Factory ----------------------
    @classmethod
    def from_env(cls) -> "KafkaMessageQueueBackend":
        """Create a KafkaMessageQueueBackend instance from environment variables.

        Expected env vars:
        - KAFKA_BOOTSTRAP_SERVERS: Comma-separated list of host:port
        - KAFKA_SECURITY_PROTOCOL: PLAINTEXT | SASL_PLAINTEXT | SASL_SSL | SSL
        - KAFKA_SASL_MECHANISM: PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 | OAUTHBEARER
        - KAFKA_SASL_USERNAME / KAFKA_SASL_PASSWORD
        - KAFKA_SSL_CAFILE / KAFKA_SSL_CERTFILE / KAFKA_SSL_KEYFILE
        - KAFKA_CLIENT_ID
        - KAFKA_TOPIC_PATTERN: Regex for topics to consume, default ".*"
        - KAFKA_CONSUMER_GROUP_PREFIX: Prefix for consumer group id, default "abe"
        - KAFKA_ENABLE_AUTO_COMMIT: true/false (default true)
        - KAFKA_AUTO_OFFSET_RESET: latest | earliest (default latest)
        """
        servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        if not servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required, e.g. 'localhost:9092' or 'host1:9092,host2:9092'")
        bootstrap_servers = [s.strip() for s in servers.split(",") if s.strip()]
        if not bootstrap_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS contains no valid endpoints")

        security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
        sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM")
        sasl_username = os.getenv("KAFKA_SASL_USERNAME")
        sasl_password = os.getenv("KAFKA_SASL_PASSWORD")
        ssl_cafile = os.getenv("KAFKA_SSL_CAFILE")
        ssl_certfile = os.getenv("KAFKA_SSL_CERTFILE")
        ssl_keyfile = os.getenv("KAFKA_SSL_KEYFILE")
        client_id = os.getenv("KAFKA_CLIENT_ID")
        topic_pattern = os.getenv("KAFKA_TOPIC_PATTERN", ".*")
        consumer_group_prefix = os.getenv("KAFKA_CONSUMER_GROUP_PREFIX", "abe")
        enable_auto_commit = os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "true").lower() == "true"
        auto_offset_reset = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")

        return cls(
            bootstrap_servers=bootstrap_servers,
            security_protocol=security_protocol,
            sasl_mechanism=sasl_mechanism,
            sasl_username=sasl_username,
            sasl_password=sasl_password,
            ssl_cafile=ssl_cafile,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            client_id=client_id,
            topic_pattern=topic_pattern,
            consumer_group_prefix=consumer_group_prefix,
            enable_auto_commit=enable_auto_commit,
            auto_offset_reset=auto_offset_reset,
        )

    # ---------------------- Internals ----------------------
    async def _ensure_producer(self) -> None:
        if self._producer_started:
            return
        if KafkaProducer is None:
            raise RuntimeError("kafka-python is not installed; install 'kafka-python' to use Kafka backend")
        # kafka-python connects lazily on first use
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=self._bootstrap_servers,
                security_protocol=self._security_protocol,
                sasl_mechanism=self._sasl_mechanism,
                sasl_plain_username=self._sasl_username,
                sasl_plain_password=self._sasl_password,
                ssl_cafile=self._ssl_cafile,
                ssl_certfile=self._ssl_certfile,
                ssl_keyfile=self._ssl_keyfile,
                client_id=self._client_id,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            self._producer_started = True
            logger.info("Kafka producer created", extra={"bootstrap_servers": ",".join(self._bootstrap_servers)})
        except Exception as exc:  # pragma: no cover - network dependent
            logger.error("Unable to create Kafka producer", exc_info=exc)
            raise ConnectionError(f"Unable to create Kafka producer: {exc}")

    async def _ensure_consumer(self, group: Optional[str]) -> None:
        if self._consumer_started:
            return
        if KafkaConsumer is None:
            raise RuntimeError("kafka-python is not installed; install 'kafka-python' to use Kafka backend")
        group_id = f"{self._consumer_group_prefix}_{group}" if group else f"{self._consumer_group_prefix}_default"
        try:
            self._consumer = KafkaConsumer(
                bootstrap_servers=self._bootstrap_servers,
                security_protocol=self._security_protocol,
                sasl_mechanism=self._sasl_mechanism,
                sasl_plain_username=self._sasl_username,
                sasl_plain_password=self._sasl_password,
                ssl_cafile=self._ssl_cafile,
                ssl_certfile=self._ssl_certfile,
                ssl_keyfile=self._ssl_keyfile,
                client_id=self._client_id,
                group_id=group_id,
                enable_auto_commit=self._enable_auto_commit,
                auto_offset_reset=self._auto_offset_reset,
                value_deserializer=lambda v: v,
            )
            # Subscribe using regex so provider can route by key/topic
            assert self._consumer is not None
            self._consumer.subscribe(pattern=self._topic_pattern)
            self._consumer_started = True
            logger.info(
                "Kafka consumer created",
                extra={"group_id": group_id, "pattern": self._topic_pattern},
            )
        except Exception as exc:  # pragma: no cover - network dependent
            logger.error("Unable to create Kafka consumer", exc_info=exc)
            raise ConnectionError(f"Unable to create Kafka consumer: {exc}")

    async def _with_retry(self, coro_factory, *, retries: int = 3) -> Any:
        last_exc: Optional[BaseException] = None
        for attempt in range(retries + 1):
            try:
                return await coro_factory()
            except Exception as exc:  # pragma: no cover - transient paths
                last_exc = exc
                if attempt == retries:
                    break
                sleep_for = 2 ** attempt
                logger.warning(
                    "Kafka operation failed; retrying",
                    extra={"attempt": attempt + 1, "sleep_seconds": sleep_for},
                )
                await asyncio.sleep(sleep_for)
        assert last_exc is not None
        raise RuntimeError(f"Kafka operation failed after retries: {last_exc}")

    # ---------------------- Protocol methods ----------------------
    async def publish(self, key: str, payload: Dict[str, Any]) -> None:
        await self._ensure_producer()
        assert self._producer is not None

        try:
            # Validate payload is JSON-serialisable before sending. This ensures
            # we raise a ValueError instead of wrapping inside retry logic.
            json.dumps(payload)

            async def _op() -> Any:
                assert self._producer is not None, "Producer not started"
                future = self._producer.send(key, payload)
                # Wait for delivery in a thread to avoid blocking loop
                return await asyncio.to_thread(future.get, 30)

            await self._with_retry(_op)
            logger.debug("Published Kafka message", extra={"topic": key})
        except (TypeError, ValueError) as exc:
            # json serializer raised because payload is not serialisable
            logger.error("Payload is not JSON-serialisable", exc_info=exc)
            raise ValueError("Payload is not JSON-serializable")
        except Exception as exc:  # pragma: no cover - network dependent
            logger.error("Unable to publish Kafka message", exc_info=exc)
            raise RuntimeError(f"Unable to publish message: {exc}")

    async def consume(self, *, group: Optional[str] = None) -> AsyncIterator[Dict[str, Any]]:
        await self._ensure_consumer(group)
        assert self._consumer is not None

        try:
            while True:
                records = await asyncio.to_thread(self._consumer.poll, timeout_ms=1000)
                if not records:
                    continue
                for _tp, messages in records.items():
                    for msg in messages:
                        try:
                            payload = json.loads(msg.value.decode("utf-8"))
                        except json.JSONDecodeError as exc:
                            logger.error("Invalid JSON payload from Kafka", exc_info=exc)
                            continue
                        yield payload
        except asyncio.CancelledError:
            # Surface cancellation for graceful shutdown
            raise
        except Exception as exc:  # pragma: no cover - network dependent
            logger.error("Error while consuming from Kafka", exc_info=exc)
            raise RuntimeError(f"Unable to consume messages: {exc}")

    async def close(self) -> None:
        # Close producer
        if self._producer is not None and self._producer_started:
            try:
                await asyncio.to_thread(self._producer.flush)
                await asyncio.to_thread(self._producer.close)
            finally:
                self._producer_started = False
        # Close consumer
        if self._consumer is not None and self._consumer_started:
            try:
                await asyncio.to_thread(self._consumer.close)
            finally:
                self._consumer_started = False
