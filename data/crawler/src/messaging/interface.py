from abc import ABC, abstractmethod
from typing import List, Callable
from messaging.schema import Message


class Producer(ABC):
    @abstractmethod
    def produce(self, topic: str, message: Message) -> None:
        """Produce message to Kafka topic"""
        pass

    @abstractmethod
    def produce_batch(self, topic: str, messages: List[Message]) -> None:
        """Produce batch messages to Kafka topic"""
        pass


class Consumer(ABC):
    @abstractmethod
    def consume(self, topic: str) -> Message:
        """Consume message from Kafka topic"""
        pass

    @abstractmethod
    def subscribe(self, topics: List[str]) -> None:
        """Subscribe to Kafka topics"""
        pass

    @abstractmethod
    def start(self, callback: Callable[[Message], None]) -> None:
        """Start consuming messages"""
        pass

    @abstractmethod
    def stop(self) -> None:
        """Stop consuming messages"""
        pass
