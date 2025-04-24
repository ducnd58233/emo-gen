import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.logger import get_logger
from infra.messaging.interface import MessageInfo
from infra.messaging.kafka.consumer import KafkaMessageConsumer
from processing.factory import PipelineFactory
from processing.interface import PipelineContext

logger = get_logger("processing.manager")

DEFAULT_MAX_WORKERS = 4


class ProcessingManager:
    """Manager for parallel processing of messages using pipelines"""

    def __init__(
        self,
        topic: str,
        pipeline_type: str,
        consumer=None,
        group_id: str = None,
        max_workers=DEFAULT_MAX_WORKERS,
    ):
        self.topic = topic
        self.pipeline_type = pipeline_type
        self.consumer = consumer or KafkaMessageConsumer(
            group_id=group_id,
            auto_commit=False,
        )
        self.pipeline = PipelineFactory.create_pipeline(pipeline_type)
        self.max_workers = max_workers
        self.running = False
        self.executor = None
        self.processing_futures = set()
        self.consumer_thread = None
        self.lock = threading.Lock()

    def process_message(self, message):
        """Process a message through the pipeline"""
        context = PipelineContext(
            message_type=message.type, processing_time=message.metadata.timestamp
        )

        logger.info(f"Processing message of type {message.type}")
        return self.pipeline.execute(message.model_dump(), context)

    def _consumer_loop(self):
        """Consumer thread that submits messages to thread pool for processing"""

        def submit_for_processing(message, message_info):
            """Submit message for processing in thread pool"""
            if not self.running:
                return False

            try:
                with self.lock:
                    future = self.executor.submit(self.process_message, message)
                    if not isinstance(message_info, MessageInfo):
                        logger.warning(
                            f"Converting message_info to MessageInfo: {message_info}"
                        )
                        message_info = MessageInfo(
                            topic=message_info.topic,
                            partition=message_info.partition,
                            offset=message_info.offset,
                        )
                    self.processing_futures.add((future, message_info))

                self._cleanup_completed_futures()
                return True
            except Exception as e:
                logger.error(f"Error submitting message: {e}")
                import traceback

                logger.error(traceback.format_exc())
                return False

        logger.info(f"Starting consumer for topic {self.topic}")
        try:
            self.consumer.start(submit_for_processing)
        except Exception as e:
            logger.error(f"Consumer loop error: {e}")
            self.running = False

    def _cleanup_completed_futures(self):
        """Clean up completed futures and commit their offsets"""
        with self.lock:
            completed = [(f, info) for f, info in self.processing_futures if f.done()]
            if not completed:
                return

            successful_messages = set()

            # Process completed futures
            for future, message_info in completed:
                try:
                    future.result()
                    successful_messages.add(message_info)
                except Exception as e:
                    logger.error(f"Task error: {e}")

            # Commit successful offsets
            if successful_messages:
                try:
                    self.consumer.commit(successful_messages)
                except Exception as e:
                    logger.error(f"Commit error: {e}")
                    import traceback

                    logger.error(traceback.format_exc())

            # Remove completed futures
            self.processing_futures = {
                (f, info) for f, info in self.processing_futures if not f.done()
            }

    def start(self):
        """Start consuming and processing messages"""
        if self.running:
            return

        logger.info(f"Starting processing manager for topic {self.topic}")
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self.processing_futures = set()
        self.consumer.subscribe([self.topic])
        self.running = True

        self.consumer_thread = threading.Thread(target=self._consumer_loop)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()
        logger.info("Processing manager started")

    def stop(self):
        """Stop processing and clean up resources"""
        if not self.running:
            return

        logger.info("Stopping processing manager")
        self.running = False
        self.consumer.stop()

        if self.executor:
            logger.info("Waiting for pending tasks...")

            with self.lock:
                futures = [f for f, _ in self.processing_futures]

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception:
                    pass

            self.executor.shutdown(wait=True)
            logger.info("All tasks completed")

        if self.consumer_thread and self.consumer_thread.is_alive():
            self.consumer_thread.join(timeout=5.0)

        self.consumer.close()
        logger.info("Processing manager stopped")
