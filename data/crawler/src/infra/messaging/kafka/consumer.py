import json
import traceback
from typing import Callable, List, Set

from core.config import config
from core.decorator import retry, timer
from core.logger import get_logger
from infra.messaging.interface import Consumer, MessageInfo
from infra.messaging.schema import Message
from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition

logger = get_logger("messaging.kafka.consumer")


class KafkaMessageConsumer(Consumer):
    """Kafka consumer implementation with support for manual commits and callbacks"""

    def __init__(
        self,
        bootstrap_servers: str = config.kafka.bootstrap_servers,
        group_id: str = config.kafka.group_id,
        auto_commit: bool = False,
        consumer_timeout_ms: int = 1000,
        **kwargs,
    ):
        """
        Initialize the Kafka consumer.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            group_id: Consumer group ID
            auto_commit: Whether to auto-commit offsets
            consumer_timeout_ms: Timeout for polling messages
            **kwargs: Additional Kafka consumer configuration
        """
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.auto_commit = auto_commit
        self.consumer_timeout_ms = consumer_timeout_ms
        self.consumer_kwargs = kwargs
        self.consumer = None
        self.running = False
        self.topics = []

        logger.info(
            f"Initialized Kafka consumer: {bootstrap_servers}, group: {group_id}"
        )

    def _create_consumer(self) -> KafkaConsumer:
        """Create and return a new KafkaConsumer instance"""
        return KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            enable_auto_commit=self.auto_commit,
            consumer_timeout_ms=self.consumer_timeout_ms,
            **self.consumer_kwargs,
        )

    def get_consumer(self) -> KafkaConsumer:
        """Get or create the KafkaConsumer instance"""
        if self.consumer is None:
            self.consumer = self._create_consumer()
        return self.consumer

    def consume(self, topic: str) -> Message:
        """
        Consume a single message from the specified topic.

        Args:
            topic: Topic to consume from

        Returns:
            Parsed message

        Raises:
            ValueError: If no message is available within timeout
        """
        if topic not in self.topics:
            self.subscribe([topic])

        consumer = self.get_consumer()

        # Poll for a single message
        records = consumer.poll(timeout_ms=self.consumer_timeout_ms, max_records=1)

        if not records:
            raise ValueError(f"No message available from topic {topic} within timeout")

        # Process the first message found
        for tp, messages in records.items():
            if messages:
                return Message.model_validate(messages[0].value)

        raise ValueError(f"No message available from topic {topic}")

    def subscribe(self, topics: List[str]) -> None:
        """
        Subscribe to Kafka topics.

        Args:
            topics: List of topics to subscribe to
        """
        self.topics = topics
        self.get_consumer().subscribe(topics)
        logger.info(f"Subscribed to topics: {topics}")

    @timer(name="Consumer Processing")
    def start(self, callback: Callable[[Message, MessageInfo], bool]) -> None:
        """
        Start consuming messages and process with callback.

        The callback should return True if processing was successful and the message
        should be committed (when auto_commit is False).

        Args:
            callback: Function that processes messages and returns success status
        """
        if not self.topics:
            raise ValueError("No topics subscribed")

        consumer = self.get_consumer()
        self.running = True

        logger.info(f"Starting consumer for topics: {self.topics}")

        try:
            while self.running:
                try:
                    # Poll for messages with timeout
                    records = consumer.poll(
                        timeout_ms=self.consumer_timeout_ms, max_records=10
                    )

                    if not records:
                        continue

                    for topic_partition, messages in records.items():
                        for message in messages:
                            try:
                                # Create message info for tracking
                                message_info = MessageInfo(
                                    topic=message.topic,
                                    partition=message.partition,
                                    offset=message.offset,
                                )

                                # Parse message using Pydantic model validation
                                parsed_message = Message.model_validate(message.value)

                                # Process message through callback
                                success = callback(parsed_message, message_info)

                                # Commit if manual commit mode and processing succeeded
                                if not self.auto_commit and success:
                                    self.commit({message_info})

                            except Exception as e:
                                logger.error(f"Error processing message: {e}")
                                logger.error(traceback.format_exc())

                except Exception as e:
                    if self.running:
                        logger.error(f"Consumer poll error: {e}")
                        logger.error(traceback.format_exc())

        except Exception as e:
            logger.error(f"Fatal consumer error: {e}")
            logger.error(traceback.format_exc())

        finally:
            logger.info("Consumer loop stopped")

    def stop(self) -> None:
        """Stop the consumer loop"""
        self.running = False
        logger.info("Consumer stopping")

    @retry(max_attempts=3, delay=1, backoff=2)
    def commit(self, message_infos: Set[MessageInfo]) -> None:
        """
        Commit the offsets for the given messages.

        Args:
            message_infos: Set of message info objects to commit
        """
        if not message_infos or not self.consumer:
            return

        try:
            commit_dict = {}
            for info in message_infos:
                tp = TopicPartition(info.topic, info.partition)
                # offset + 1 indicates the next message to consume
                # leader_epoch is set to -1 (default/unknown) when not provided
                commit_dict[tp] = OffsetAndMetadata(
                    offset=info.offset + 1, metadata="", leader_epoch=-1
                )

            if commit_dict:
                self.consumer.commit(commit_dict)
                logger.debug(f"Committed offsets for {len(message_infos)} messages")
        except Exception as e:
            logger.error(f"Failed to commit offsets: {e}")
            logger.error(traceback.format_exc())
            raise

    def close(self) -> None:
        """Close the Kafka consumer and release resources"""
        if self.consumer:
            self.consumer.close()
            self.consumer = None
            logger.info("Consumer closed")

    def __del__(self) -> None:
        """Ensure resources are released when object is garbage collected"""
        self.close()
