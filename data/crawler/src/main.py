import argparse

from core.logger import logger
from extraction.factory import CrawlerFactory


def parse_args():
    parser = argparse.ArgumentParser(description='Emoji Generator Crawler')
    parser.add_argument('--source', type=str, default='discord',
                        help='Source to crawl (default: discord)')
    parser.add_argument('--url', type=str, default='https://discords.com/emoji-list',
                        help='URL to crawl (default: https://discords.com/emoji-list)')
    parser.add_argument('--headless', action='store_true', default=True,
                        help='Run browser in headless mode')
    parser.add_argument('--worker-count', type=int, default=4,
                        help='Number of worker threads for processing')

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()

    try:
        crawler = CrawlerFactory.create_crawler(
            source=args.source,
            headless=args.headless
        )

        logger.info(f"Starting crawler for {args.source} at {args.url}")
        results = crawler.crawl(args.url)
        logger.info(f"Crawling completed. Found {len(results)} emojis.")

    except Exception as e:
        logger.error(f"Error during crawling: {e}")

