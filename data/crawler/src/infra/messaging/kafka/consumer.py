import json
from typing import Callable, List

from core.config import config
from core.logger import get_logger
from infra.messaging.interface import Consumer
from infra.messaging.schema import Message
from kafka import KafkaConsumer

logger = get_logger("messaging.kafka.consumer")


class KafkaMessageConsumer(Consumer):
    def __init__(self, group_id: str = config.kafka.group_id):
        self.consumer = KafkaConsumer(
            bootstrap_servers=config.kafka.bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            enable_auto_commit=True,
            consumer_timeout_ms=1000,  # 1 second timeout for polling
        )
        self._running = False
        logger.info(
            f"Kafka consumer initialized: {config.kafka.bootstrap_servers}, group: {group_id}"
        )

    def consume(self, topic: str) -> Message:
        """Consume a single message from a topic"""
        self.consumer.subscribe([topic])
        for message in self.consumer:
            return Message.model_validate(message.value)

    def subscribe(self, topics: List[str]) -> None:
        """Subscribe to a list of topics"""
        self.consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {topics}")

    def start(self, callback: Callable[[Message], None]) -> None:
        """Start consuming messages and apply callback to each"""
        logger.info("Starting consumer...")
        self._running = True
        try:
            while self._running:
                # Poll with timeout
                records = self.consumer.poll(timeout_ms=1000)
                for topic_partition, messages in records.items():
                    for kafka_message in messages:
                        try:
                            message = Message.model_validate(kafka_message.value)
                            callback(message)
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")

                # Allow interruption between polls
                if not self._running:
                    break

        except Exception as e:
            logger.error(f"Consumer error: {e}")
            raise
        finally:
            logger.info("Consumer stopped")

    def stop(self) -> None:
        """Stop the consumer"""
        logger.info("Stopping consumer...")
        self._running = False
