from datetime import datetime
import json

from typing import List
from kafka import KafkaProducer

from core.logger import get_logger
from core.config import config
from messaging.interface import Producer
from messaging.schema import Message
from core.decorator import singleton

logger = get_logger("messaging.kafka.producer")


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

@singleton
class KafkaMessageProducer(Producer):
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=config.kafka.bootstrap_servers,
            value_serializer=lambda x: json.dumps(x, cls=DateTimeEncoder).encode('utf-8')
        )
        logger.info(f"Kafka producer initialized with servers: {config.kafka.bootstrap_servers}")

    def produce(self, topic: str, message: Message) -> None:
        """Produce a single message to a Kafka topic"""
        try:
            self.producer.send(topic, value=message.model_dump())
            self.producer.flush()
            logger.debug(
                f"Message sent to topic: {topic}, type: {message.type}")
        except Exception as e:
            logger.error(f"Error sending message to Kafka: {e}")
            raise

    def produce_batch(self, topic: str, messages: List[Message]) -> None:
        """Produce multiple messages to a Kafka topic"""
        try:
            for message in messages:
                self.producer.send(topic, value=message.model_dump())
            self.producer.flush()
            logger.info(
                f"Batch of {len(messages)} messages sent to topic: {topic}")
        except Exception as e:
            logger.error(f"Error sending batch messages to Kafka: {e}")
            raise

    def __del__(self):
        self.producer.close()
