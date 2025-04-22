from enum import Enum

from sqlalchemy import Column, Integer, String, DateTime, Enum as SQLAlchemyEnum
from sqlalchemy.sql import func

from storage.database.models.base import Base


class Status(Enum):
    CRAWLED = "crawled"
    PROCESSED = "processed"
    FAILED = "failed"


class CrawlEmoji(Base):
    __tablename__ = "crawl_emojis"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False, unique=True)
    image_url = Column(String, nullable=False)
    status = Column(SQLAlchemyEnum(Status),
                    nullable=False, default=Status.CRAWLED, index=True)
    created_at = Column(DateTime, server_default=func.current_timestamp())
    updated_at = Column(DateTime, server_default=func.current_timestamp(
    ), onupdate=func.current_timestamp())

    def __repr__(self):
        return f"<CrawlEmoji(id={self.id}, name='{self.name}', image_url='{self.image_url}', status='{self.status}')>"

class SourceEmoji(Base):
    __tablename__ = "source_emojis"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    image_path = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.current_timestamp())
