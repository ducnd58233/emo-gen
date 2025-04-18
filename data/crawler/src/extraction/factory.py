from extraction.interface import ICrawler
from extraction.sources.discord import DiscordEmojiCrawler


class CrawlerFactory:
    @staticmethod
    def create_crawler(source: str, **kwargs) -> ICrawler:
        if source == 'discord':
            return DiscordEmojiCrawler(**kwargs)
        else:
            raise ValueError(f'Invalid source: {source}')
