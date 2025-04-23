from abc import ABC, abstractmethod
from typing import Callable, List

from messaging.schema import Message


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
        """Consume message from Kafka topic"""

    @abstractmethod
    def subscribe(self, topics: List[str]) -> None:
        """Subscribe to Kafka topics"""

    @abstractmethod
    def start(self, callback: Callable[[Message], None]) -> None:
        """Start consuming messages"""

    @abstractmethod
    def stop(self) -> None:
        """Stop consuming messages"""
