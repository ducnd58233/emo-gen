from typing import Dict, List, Optional, Union

from core.logger import get_logger
from sqlalchemy.orm import Session
from storage.database.client import SessionLocal
from storage.database.models.emoji import CrawlEmoji, Status
from storage.database.repositories.base import (
    FILTER_EQ,
    FILTER_IN,
    FilterCondition,
    Repository,
)

logger = get_logger("storage.database.repositories.crawl_emoji_repository")


class CrawlEmojiRepository(Repository[CrawlEmoji]):
    """Repository for CrawlEmoji model with specific methods"""

    def __init__(self, session_factory=SessionLocal):
        super().__init__(CrawlEmoji, session_factory)

    def get_by_ids(
        self, ids: List[int], session: Optional[Session] = None
    ) -> List[CrawlEmoji]:
        """
        Get multiple emojis by their IDs in a single query

        Args:
            ids: List of emoji IDs
            session: Optional database session

        Returns:
            List of emoji objects with matching IDs
        """
        if not ids:
            return []

        return list(
            self.filter_by_multiple_values(
                field="id", values=ids, session=session, as_dict=False
            )
        )

    def filter_by_source_and_status(
        self,
        source: str,
        status: Status,
        identifiers: List[str],
        as_dict: bool = False,
        match_by: str = "name",
        session: Optional[Session] = None,
    ) -> Union[Dict[str, CrawlEmoji], List[CrawlEmoji]]:
        """
        Query emojis efficiently by source, status, and a list of identifiers
        (either names or image_urls) in a single database call.

        Args:
            source: Source of the emojis to match
            status: Status to filter by
            identifiers: List of names or image_urls to match
            as_dict: Return results as a dictionary with key_attr as the key
            match_by: Field to match by ('name' or 'image_url')
            session: Optional database session

        Returns:
            Dictionary mapping identifier to emoji object
        """
        if not identifiers:
            return {}

        conditions = [
            FilterCondition("source", source, FILTER_EQ),
            FilterCondition("status", status, FILTER_EQ),
            FilterCondition(match_by, identifiers, FILTER_IN),
        ]

        result = self.get_many(
            filters=conditions,
            limit=len(identifiers) * 2,
            key_attr=match_by,
            as_dict=as_dict,
            session=session,
        )

        logger.info(
            f"Found {len(result)} emojis with {status} status for source '{source}' "
            f"(matched by {match_by}, requested {len(identifiers)})"
        )

        return result

    def get_by_status(
        self,
        status: Status,
        session: Optional[Session] = None,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "id",
    ) -> List[CrawlEmoji]:
        """
        Get emojis by status with pagination and ordering

        Args:
            status: Status to filter by
            session: Optional database session
            limit: Maximum number of records to return
            offset: Number of records to skip
            order_by: Field to order by (prefix with '-' for descending)

        Returns:
            List of emojis matching the status
        """
        filters = {"status": status}
        return self.get_many(
            limit=limit,
            offset=offset,
            filters=filters,
            order_by=order_by,
            session=session,
        )

    def count_by_status(self, status: Status, session: Optional[Session] = None) -> int:
        """
        Count emojis by status

        Args:
            status: Status to filter by
            session: Optional database session

        Returns:
            Count of emojis with the given status
        """
        filters = {"status": status}
        return self.count(filters=filters, session=session)

    def update_status(
        self, emoji_id: int, new_status: Status, session: Optional[Session] = None
    ) -> Optional[CrawlEmoji]:
        """
        Update emoji status

        Args:
            emoji_id: Emoji ID
            new_status: New status value
            session: Optional database session

        Returns:
            Updated emoji or None if not found
        """
        updates = {"status": new_status}
        return self.update(emoji_id, updates, session)

    def update_status_bulk(
        self,
        emoji_ids: List[int],
        new_status: Status,
        session: Optional[Session] = None,
    ) -> int:
        """
        Update status for multiple emojis

        Args:
            emoji_ids: List of emoji IDs
            new_status: New status value
            session: Optional database session

        Returns:
            Number of updated records
        """
        with self.get_session(session) as db:
            try:
                stmt = (
                    self.model_class.__table__.update()
                    .where(self.model_class.id.in_(emoji_ids))
                    .values(status=new_status)
                )

                result = db.execute(stmt)
                db.commit()

                count = result.rowcount
                logger.info(f"Bulk updated status for {count} emojis to {new_status}")
                return result
            except Exception as e:
                db.rollback()
                logger.error(f"Error updating emoji statuses: {e}")
                raise

    def get_by_name(
        self, name: str, session: Optional[Session] = None
    ) -> Optional[CrawlEmoji]:
        """
        Find emoji by exact name

        Args:
            name: Emoji name
            session: Optional database session

        Returns:
            Emoji object or None if not found
        """
        with self.get_session(session) as db:
            return (
                db.query(self.model_class).filter(self.model_class.name == name).first()
            )

    def get_by_name_pattern(
        self, pattern: str, session: Optional[Session] = None, limit: int = 100
    ) -> List[CrawlEmoji]:
        """
        Find emojis by name pattern

        Args:
            pattern: SQL LIKE pattern (e.g., '%happy%')
            session: Optional database session
            limit: Maximum number of records to return

        Returns:
            List of matching emoji objects
        """
        with self.get_session(session) as db:
            return (
                db.query(self.model_class)
                .filter(self.model_class.name.ilike(pattern))
                .limit(limit)
                .all()
            )
