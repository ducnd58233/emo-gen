from core.config import config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from storage.database.models.emoji import Base

engine = create_engine(
    config.db.uri,
    echo=False,  # Set to True for SQL debugging
    pool_pre_ping=True,  # Verify connections before using
    pool_size=config.db.pool_size,  # Connection pool size
    max_overflow=config.db.max_overflow,  # Allowed connections beyond pool_size
    pool_timeout=config.db.pool_timeout,  # Seconds to wait for a connection
    # Recycle connections after this many seconds
    pool_recycle=config.db.pool_recycle,
)
Base.metadata.create_all(bind=engine)
SessionLocal = sessionmaker(bind=engine)
