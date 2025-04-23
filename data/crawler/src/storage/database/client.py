from core.config import config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from storage.database.models.emoji import Base

engine = create_engine(
    config.db.uri,
    echo=False,  # Set to True for SQL debugging
    pool_pre_ping=True,  # Verify connections before using
)
Base.metadata.create_all(bind=engine)
SessionLocal = sessionmaker(bind=engine)
