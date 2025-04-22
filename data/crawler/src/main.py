import signal
import sys
import threading

from core.logger import get_logger
from core.config import config
from extraction.factory import CrawlerFactory
from processing.manager import ProcessingManager
from processing.spark.session import SparkSessionManager

logger = get_logger("main")


def signal_handler(sig, frame):
    """Handle Ctrl+C signal"""
    logger.info("Received shutdown signal, exiting...")
    # Stop spark session
    SparkSessionManager().stop()
    sys.exit(0)


def main():
    # Register signal handler
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Start the processing manager
        processing_manager = ProcessingManager(
            topic=config.kafka.emoji_topic,
            pipeline_type="emoji",
            group_id=config.kafka.group_id
        )

        # Start processing in a non-blocking way (will be implemented in ProcessingManager)
        processing_manager.start(non_blocking=True)

        # Initialize crawler with sensible defaults from config
        crawler = CrawlerFactory.create_crawler(
            source="discord",
            headless=config.crawler.headless,
            max_workers=4
        )

        # Start crawling
        logger.info(
            f"Starting crawler for discord at {crawler.ROOT_URL}emoji-list")
        results = crawler.crawl(f"{crawler.ROOT_URL}emoji-list")
        logger.info(f"Crawling completed. Found {len(results)} emojis.")

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
    except Exception as e:
        logger.error(f"Error in main loop: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # Clean up resources
        if 'processing_manager' in locals():
            processing_manager.stop()
        SparkSessionManager().stop()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
