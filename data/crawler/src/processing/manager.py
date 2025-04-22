from typing import Any, List, Optional
import threading

from pyspark.sql import DataFrame

from core.logger import get_logger
from messaging.kafka.consumer import KafkaMessageConsumer
from messaging.schema import Message
from processing.factory import PipelineFactory
from processing.interface import PipelineContext

logger = get_logger("processing.manager")


class ProcessingManager:
    """
    Manager for processing messages using pipelines

    Responsibilities:
    1. Consume messages from Kafka
    2. Create appropriate pipeline for processing
    3. Execute pipeline on messages
    4. Handle errors
    """

    def __init__(
        self,
        topic: str,
        pipeline_type: str,
        consumer: Optional[KafkaMessageConsumer] = None,
        group_id: str = "emoji-processor"
    ):
        self.topic = topic
        self.pipeline_type = pipeline_type
        self.consumer = consumer or KafkaMessageConsumer(group_id=group_id)
        self.pipeline = PipelineFactory.create_pipeline(pipeline_type)
        self.consumer_thread = None
        self.running = False

    def process_message(self, message: Message) -> None:
        """
        Process a message through the pipeline

        Args:
            message: Message to process
        """
        try:
            # Create context
            context = PipelineContext(
                message_type=message.type,
                processing_time=message.metadata.timestamp
            )

            # Execute pipeline
            logger.info(
                f"Processing message of type {message.type} through {self.pipeline_type} pipeline")
            result = self.pipeline.execute(message.model_dump(), context)

            # Handle result
            self._handle_result(result, context)

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _handle_result(self, result: Any, context: PipelineContext) -> None:
        """
        Handle pipeline result

        Args:
            result: Pipeline result
            context: Pipeline context
        """
        if isinstance(result, DataFrame):
            count = result.count()
            logger.info(
                f"Pipeline completed with {count} records in result DataFrame")
        else:
            logger.info(
                f"Pipeline completed with result of type {type(result)}")

    def _consumer_loop(self):
        """Consumer thread function"""
        try:
            self.consumer.start(self.process_message)
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}")
            self.running = False

    def start(self, non_blocking: bool = False) -> None:
        """
        Start consuming and processing messages

        Args:
            non_blocking: If True, start in non-blocking mode (return immediately)
        """
        logger.info(
            f"Starting processing manager for topic {self.topic} with {self.pipeline_type} pipeline")

        self.consumer.subscribe([self.topic])
        self.running = True

        if non_blocking:
            # Start in a new thread for non-blocking operation
            self.consumer_thread = threading.Thread(target=self._consumer_loop)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()
        else:
            # Run directly in the current thread (blocking)
            self._consumer_loop()

    def stop(self) -> None:
        """
        Stop processing
        """
        logger.info("Stopping processing manager")
        self.running = False

        # Stop the consumer if it's running
        if hasattr(self.consumer, 'stop'):
            self.consumer.stop()

        # If running in non-blocking mode, wait for thread completion
        if self.consumer_thread and self.consumer_thread.is_alive():
            logger.info("Waiting for consumer thread to finish...")
            self.consumer_thread.join(timeout=5.0)
            logger.info("Consumer thread finished")
