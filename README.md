# abe-kafka

[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/Chisanan232/abe-kafka/master.svg)](https://results.pre-commit.ci/latest/github/Chisanan232/abe-kafka/master)
[![CI](https://github.com/Chisanan232/abe-kafka/actions/workflows/ci.yaml/badge.svg)](https://github.com/Chisanan232/abe-kafka/actions/workflows/ci.yaml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=Chisanan232_abe-kafka&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=Chisanan232_abe-kafka)
[![codecov](https://codecov.io/gh/Chisanan232/abe-kafka/graph/badge.svg?token=VHdrUuASXP)](https://codecov.io/gh/Chisanan232/abe-kafka)
[![documentation](https://github.com/Chisanan232/abe-kafka/actions/workflows/documentation.yaml/badge.svg)](https://chisanan232.github.io/abe-kafka/)
[![PyPI](https://img.shields.io/pypi/v/abe-kafka.svg)](https://pypi.org/project/abe-kafka)
[![GitHub Release](https://img.shields.io/github/v/release/Chisanan232/abe-kafka)](https://github.com/Chisanan232/abe-kafka/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

[![Python Versions](https://img.shields.io/pypi/pyversions/abe-kafka.svg?logo=python&logoColor=FBE072)](https://pypi.org/project/abe-kafka)
[![Monthly Downloads](https://pepy.tech/badge/abe-kafka/month)](https://pepy.tech/project/abe-kafka)

## Overview

Kafka message queue backend implementation for the Abstract Backend ecosystem. Provides an async-friendly Kafka client with simple, environment-driven configuration and JSON message handling.

- Environment-first config via `KAFKA_*` variables
- Auth modes: PLAINTEXT, SASL/PLAIN, SSL
- Async publish/consume with JSON serialization
- Drop-in backend selected by `MESSAGE_QUEUE_BACKEND=kafka`


## Python versions support

Supports Python 3.12 and 3.13.

[![Supported Versions](https://img.shields.io/pypi/pyversions/abe-kafka.svg?logo=python&logoColor=FBE072)](https://pypi.org/project/abe-kafka)


## Quickly Start

Install and run a minimal round-trip locally.

```bash
# Install (choose one)
pip install abe-kafka
# or
uv add abe-kafka

# Minimal environment
export MESSAGE_QUEUE_BACKEND=kafka
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

```python
import asyncio
from abe_plugin.backends.message_queue.service.abe_kafka import KafkaMessageQueueBackend

async def main():
    backend = KafkaMessageQueueBackend.from_env()
    await backend.publish("demo-topic", {"hello": "world"})
    async for msg in backend.consume(group="g1"):
        print("received:", msg)
        break
    await backend.close()

asyncio.run(main())
```

For SASL/SSL examples and starting a local Kafka (Docker Compose or Testcontainers), see the Installation and How to Run docs below.

## Documentation

- Quick Start: [Installation](./docs/contents/document/quick-start/installation.mdx)
- Quick Start: [How to Run](./docs/contents/document/quick-start/how-to-run.mdx)
- Overview and configuration: [Introduction](./docs/contents/document/introduction.mdx)
- API Reference: [Kafka Backend](./docs/contents/document/api-references/kafka-backend.mdx)

For contributor guides, see Development: [index](./docs/contents/development/index.mdx), [workflow](./docs/contents/development/workflow.mdx).

## Coding style and following rules

**_abe-kafka_** follows coding styles **_black_** and **_PyLint_** to control code quality.

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![linting: pylint](https://img.shields.io/badge/linting-pylint-yellowgreen)](https://github.com/pylint-dev/pylint)


## Downloads

Package download statistics:

[![Downloads](https://pepy.tech/badge/abe-kafka)](https://pepy.tech/project/abe-kafka)
[![Downloads](https://pepy.tech/badge/abe-kafka/month)](https://pepy.tech/project/abe-kafka)


## License

[MIT License](./LICENSE)
