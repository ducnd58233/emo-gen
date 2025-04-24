from abc import ABC, abstractmethod
from typing import Callable, List, Set

from infra.messaging.schema import Message
from pydantic import BaseModel, ConfigDict


class MessageInfo(BaseModel):
    """Information about a consumed Kafka message for tracking and committing"""

    topic: str
    partition: int
    offset: int

    model_config = ConfigDict(frozen=True)

    def __str__(self) -> str:
        return f"{self.topic}:{self.partition}:{self.offset}"

    def __hash__(self) -> int:
        """Custom hash implementation to ensure MessageInfo objects can be used in sets"""
        return hash((self.topic, self.partition, self.offset))

    def __eq__(self, other: object) -> bool:
        """Custom equality check for comparison and set operations"""
        if not isinstance(other, MessageInfo):
            return False
        return (
            self.topic == other.topic
            and self.partition == other.partition
            and self.offset == other.offset
        )


class Producer(ABC):
    @abstractmethod
    def produce(self, topic: str, message: Message) -> None:
        """Produce message to Kafka topic"""

    @abstractmethod
    def produce_batch(self, topic: str, messages: List[Message]) -> None:
        """Produce batch messages to Kafka topic"""


class Consumer(ABC):
    @abstractmethod
    def consume(self, topic: str) -> Message:
        """Consume message from topic"""

    @abstractmethod
    def subscribe(self, topics: List[str]) -> None:
        """Subscribe to topics"""

    @abstractmethod
    def start(self, callback: Callable[[Message, MessageInfo], bool]) -> None:
        """
        Start consuming messages and process with callback.

        The callback should return True if processing was successful and the message
        should be committed (when auto-commit is disabled).
        """

    @abstractmethod
    def stop(self) -> None:
        """Stop consuming messages"""

    @abstractmethod
    def commit(self, message_infos: Set[MessageInfo]) -> None:
        """Commit offsets for the given messages"""
