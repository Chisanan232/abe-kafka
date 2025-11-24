"""
In-memory implementation of the MessageQueueBackend protocol.

This implementation is intended for development and testing only.
It uses an asyncio.Queue to store messages in memory, which means:
1. Messages are lost when the process restarts
2. Messages are only visible to consumers in the same process
"""

import asyncio
import logging
import warnings
from typing import Any, AsyncIterator, Dict, Optional, Tuple

from abe.backends.message_queue.base.protocol import MessageQueueBackend

# Set up logger for the memory backend
logger = logging.getLogger(__name__)


class YourMessageQueueBackend(MessageQueueBackend):
    """In-memory implementation of MessageQueueBackend using asyncio.Queue.

    This class is intended for development and testing only.
    Messages are stored in a class-level asyncio.Queue to simulate
    a message broker, but only work within a single process.
    """

    # Class-level queue shared by all instances
    _queue: "asyncio.Queue[Tuple[str, Dict[str, Any]]]" = asyncio.Queue()

    @classmethod
    def from_env(cls) -> "YourMessageQueueBackend":
        """Create a new YourMessageQueueBackend instance from environment variables.

        For the memory backend, no environment variables are required.
        This method also prints a warning about using this backend for development only.

        Returns:
            A new YourMessageQueueBackend instance
        """
        warnings.warn(
            "⚠️  Memory backend is for development/testing only. "
            "Messages will be lost on restart and are only visible to consumers in the same process.",
            UserWarning,
        )
        return cls()

    async def publish(self, key: str, payload: Dict[str, Any]) -> None:
        """Publish a message to the in-memory queue.

        Args:
            key: The routing key for the message
            payload: The message payload as a dictionary
        """
        ...

    async def consume(self, *, group: Optional[str] = None) -> AsyncIterator[Dict[str, Any]]:
        """Consume messages from the in-memory queue.

        The group parameter is ignored in the memory backend implementation
        as it doesn't support consumer groups.

        Args:
            group: Ignored in this implementation

        Yields:
            Message payloads in the order they were published
        """
        ...
