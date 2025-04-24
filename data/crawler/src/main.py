import argparse
import signal
import sys
import time
from typing import Optional

from core.config import config
from core.logger import get_logger
from extraction.factory import CrawlerFactory
from infra.spark.session import SparkSessionManager
from processing.manager import ProcessingManager

logger = get_logger("main")

# Global variables for graceful shutdown
processing_manager = None
spark_manager = None


def signal_handler(sig, frame):
    """Handle signals (SIGINT, SIGTERM) for graceful shutdown"""
    logger.info("Received shutdown signal, initiating graceful shutdown...")

    # Stop processing manager if running
    global processing_manager
    if processing_manager:
        logger.info("Stopping processing manager...")
        processing_manager.stop()

    # Stop Spark session
    logger.info("Stopping Spark session...")
    SparkSessionManager().stop()

    logger.info("Shutdown complete, exiting...")
    sys.exit(0)


def setup_signal_handlers():
    """Set up signal handlers for graceful shutdown"""
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Emoji Crawler and Processor")

    # Main mode selection
    parser.add_argument(
        "--mode",
        choices=["crawler", "processor", "both"],
        default="both",
        help="Run mode: crawler, processor, or both (default)",
    )

    # Crawler options
    parser.add_argument(
        "--source", default="discord", help="Source to crawl (default: discord)"
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        default=config.crawler.headless,
        help="Run crawler in headless mode",
    )
    parser.add_argument(
        "--crawler-workers",
        type=int,
        default=4,
        help="Number of crawler worker threads (default: 4)",
    )

    # Processor options
    parser.add_argument(
        "--topic", default=config.kafka.emoji_topic, help="Kafka topic to consume from"
    )
    parser.add_argument(
        "--group-id", default=config.kafka.group_id, help="Kafka consumer group ID"
    )
    parser.add_argument(
        "--processor-workers",
        type=int,
        default=4,
        help="Number of processor worker threads (default: 4)",
    )

    return parser.parse_args()


def start_processor(args) -> Optional[ProcessingManager]:
    """Start the processing manager based on command arguments"""
    logger.info("Initializing processing manager...")

    try:
        global processing_manager
        processing_manager = ProcessingManager(
            topic=args.topic,
            pipeline_type="emoji",
            group_id=args.group_id,
            max_workers=args.processor_workers,
        )

        processing_manager.start()
        logger.info("Processing manager initialized and started")
        return processing_manager

    except Exception as e:
        logger.error(f"Failed to start processing manager: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return None


def run_crawler(args) -> int:
    """Run the crawler based on command arguments"""
    logger.info(f"Initializing crawler for source: {args.source}")

    try:
        crawler = CrawlerFactory.create_crawler(
            source=args.source, headless=args.headless, max_workers=args.crawler_workers
        )

        url = f"{crawler.ROOT_URL}emoji-list"
        logger.info(f"Starting crawler for {args.source} at {url}")

        results = crawler.crawl(url)

        logger.info(f"Crawling completed. Found {len(results)} emojis.")
        return len(results)

    except Exception as e:
        logger.error(f"Crawler error: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return 0


def main():
    """Main application entry point"""
    setup_signal_handlers()
    args = parse_arguments()

    try:
        # Initialize Spark session if needed
        if args.mode in ["processor", "both"]:
            logger.info("Initializing Spark session...")
            global spark_manager
            spark_manager = SparkSessionManager()

        # Start processor if needed
        if args.mode in ["processor", "both"]:
            start_processor(args)

        # Run crawler if needed
        if args.mode in ["crawler", "both"]:
            emoji_count = run_crawler(args)
            logger.info(f"Total emojis crawled: {emoji_count}")

        # If only running processor, keep main thread alive
        if args.mode == "processor" and processing_manager:
            logger.info("Running in processor-only mode. Press Ctrl+C to exit.")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Keyboard interrupt received, shutting down...")
                if processing_manager:
                    processing_manager.stop()

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")

    except Exception as e:
        logger.error(f"Error in main loop: {e}")
        import traceback

        logger.error(traceback.format_exc())

    finally:
        # Clean up resources
        if processing_manager:
            logger.info("Stopping processing manager...")
            processing_manager.stop()

        logger.info("Stopping Spark session...")
        SparkSessionManager().stop()

        logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
