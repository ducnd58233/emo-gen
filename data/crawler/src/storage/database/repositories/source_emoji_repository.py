from typing import Any, Dict, List, Optional

from core.logger import get_logger
from sqlalchemy.orm import Session
from storage.database.client import SessionLocal
from storage.database.models.emoji import SourceEmoji
from storage.database.repositories.base import Repository

logger = get_logger("storage.database.repositories.source_emoji_repository")


class SourceEmojiRepository(Repository[SourceEmoji]):
    """Repository for SourceEmoji model with specific methods"""

    def __init__(self, session_factory=SessionLocal):
        super().__init__(SourceEmoji, session_factory)

    def get_by_name(
        self, name: str, session: Optional[Session] = None
    ) -> Optional[SourceEmoji]:
        """
        Find source emoji by exact name

        Args:
            name: Emoji name
            session: Optional database session

        Returns:
            SourceEmoji object or None if not found
        """
        with self.get_session(session) as db:
            return (
                db.query(self.model_class).filter(
                    self.model_class.name == name).first()
            )

    def get_by_ids(
        self, ids: List[int], session: Optional[Session] = None
    ) -> List[SourceEmoji]:
        """
        Get multiple source emojis by their IDs in a single query

        Args:
            ids: List of emoji IDs
            session: Optional database session

        Returns:
            List of emoji objects with matching IDs
        """
        with self.get_session(session) as db:
            return db.query(self.model_class).filter(self.model_class.id.in_(ids)).all()

    def create_from_crawl_emoji(
        self,
        emoji_id: int,
        name: str,
        image_path: str,
        source: str = "unknown",
        session: Optional[Session] = None,
    ) -> SourceEmoji:
        """
        Create a new source emoji from crawl emoji data

        Args:
            emoji_id: ID of the crawled emoji (will be reused as source emoji ID)
            name: Emoji name
            image_path: Path to the stored image in MinIO
            source: Source of the emoji (e.g., "discord", "slack")
            session: Optional database session

        Returns:
            Created source emoji object
        """
        source_emoji = SourceEmoji(
            id=emoji_id,
            name=name,
            image_path=image_path,
            source=source
        )

        with self.get_session(session) as db:
            try:
                db.add(source_emoji)
                db.commit()
                db.refresh(source_emoji)
                logger.info(
                    f"Created source emoji: {source_emoji.id} - {source_emoji.name} from {source}"
                )
                return source_emoji
            except Exception as e:
                db.rollback()
                logger.error(f"Error creating source emoji: {e}")
                raise

    def bulk_create_from_crawl_emojis(
        self, emoji_data: List[Dict[str, Any]], session: Optional[Session] = None
    ) -> List[SourceEmoji]:
        """
        Create multiple source emojis from crawl emoji data

        Args:
            emoji_data: List of dictionaries with emoji data (id, name, image_path, source)
            session: Optional database session

        Returns:
            List of created source emoji objects
        """
        if not emoji_data:
            logger.warning(
                "No emoji data provided to bulk_create_from_crawl_emojis")
            return []

        # Validate input data
        for i, item in enumerate(emoji_data):
            if not all(key in item for key in ["id", "name", "image_path"]):
                logger.error(
                    f"Missing required keys in emoji data at index {i}: {item}"
                )
                raise ValueError(
                    f"Missing required keys in emoji data: {item}")

            if not isinstance(item["id"], int):
                try:
                    item["id"] = int(item["id"])
                except (ValueError, TypeError):
                    logger.error(
                        f"Invalid id format in emoji data at index {i}: {item['id']}"
                    )
                    raise ValueError(
                        f"Invalid id format in emoji data: {item['id']}")

            if "source" not in item:
                item["source"] = "unknown"

        logger.info(
            f"Creating {len(emoji_data)} source emojis with ids: {[item['id'] for item in emoji_data]}"
        )

        source_emojis = [
            SourceEmoji(
                id=item["id"],
                name=item["name"],
                image_path=item["image_path"],
                source=item.get("source", "unknown")
            )
            for item in emoji_data
        ]

        with self.get_session(session) as db:
            try:
                # Check for existing records to avoid IntegrityError
                existing_ids = set()
                if source_emojis:
                    ids_to_check = [emoji.id for emoji in source_emojis]
                    existing_records = (
                        db.query(self.model_class.id)
                        .filter(self.model_class.id.in_(ids_to_check))
                        .all()
                    )
                    existing_ids = set(record[0]
                                       for record in existing_records)

                    if existing_ids:
                        logger.info(
                            f"Found {len(existing_ids)} existing source emojis with ids: {existing_ids}"
                        )

                # Filter out existing records
                new_source_emojis = [
                    emoji for emoji in source_emojis if emoji.id not in existing_ids
                ]

                if not new_source_emojis:
                    logger.info(
                        "All source emojis already exist, skipping creation")
                    # Return existing records
                    return source_emojis

                logger.info(
                    f"Adding {len(new_source_emojis)} new source emojis")
                db.add_all(new_source_emojis)
                db.commit()

                for emoji in new_source_emojis:
                    db.refresh(emoji)

                logger.info(f"Created {len(new_source_emojis)} source emojis")
                return source_emojis
            except Exception as e:
                db.rollback()
                logger.error(f"Error bulk creating source emojis: {e}")
                logger.exception("Detailed error:")
                raise
