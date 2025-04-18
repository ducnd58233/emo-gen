from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional


class SourceDiscoveryStrategy(ABC):
    """Strategy for discovering sources (topics, categories, etc.)"""

    @abstractmethod
    def discover(self, html: str, base_url: str) -> List[str]:
        """
        Discover sources to crawl from the HTML content.
        
        Args:
            html: The HTML content
            base_url: The base URL for resolving relative URLs
            
        Returns:
            List of source URLs
        """
        pass


class ItemExtractionStrategy(ABC):
    """Strategy for extracting items from a source"""

    @abstractmethod
    def extract(self, html: str, base_url: str) -> List[Dict[str, Any]]:
        """
        Extract items from the HTML content.
        
        Args:
            html: The HTML content
            base_url: The base URL for resolving relative URLs
            
        Returns:
            List of extracted items
        """
        pass


class FetchStrategy(ABC):
    """Strategy for fetching content from a URL"""

    @abstractmethod
    def fetch(self, url: str) -> str:
        """
        Fetch content from a URL.
        
        Args:
            url: The URL to fetch
            
        Returns:
            The fetched content
        """
        pass
