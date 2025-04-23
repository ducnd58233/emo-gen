from contextlib import contextmanager
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar

from core.logger import get_logger
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from storage.database.client import SessionLocal
from storage.database.models.base import Base

logger = get_logger("storage.database.repositories.base")

T = TypeVar("T", bound=Base)

MAX_BATCH_SIZE = 20


class Repository(Generic[T]):
    """Base repository with common database operations"""

    def __init__(self, model_class: Type[T], session_factory=SessionLocal):
        self.model_class = model_class
        self.session_factory = session_factory

    @contextmanager
    def get_session(self, session: Optional[Session] = None) -> Session:
        """Context manager for database sessions

        Args:
            session: Optional existing session to use

        Yields:
            Active database session
        """
        # Use provided session or create new one
        should_close = False
        if session is not None:
            db_session = session
        else:
            db_session = self.session_factory()
            should_close = True

        try:
            yield db_session
        except SQLAlchemyError as e:
            db_session.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if should_close:
                db_session.close()

    def create(self, obj: T, session: Optional[Session] = None) -> T:
        """Create a new record"""
        with self.get_session(session) as db:
            try:
                db.add(obj)
                db.commit()
                db.refresh(obj)
                logger.info(
                    f"{self.model_class.__name__} created: {getattr(obj, 'id', 'unknown')}"
                )
                return obj
            except Exception as e:
                db.rollback()
                logger.error(f"Error creating {self.model_class.__name__}: {e}")
                raise

    def bulk_create(
        self, objects: List[T], session: Optional[Session] = None
    ) -> List[T]:
        """Create multiple records efficiently"""
        if not objects:
            return []

        with self.get_session(session) as db:
            try:
                for i in range(0, len(objects), MAX_BATCH_SIZE):
                    batch = objects[i : i + MAX_BATCH_SIZE]
                    db.bulk_save_objects(batch)
                    db.commit()
                    db.flush()
                    logger.info(
                        f"Bulk created {len(batch)} {self.model_class.__name__} records (batch {i//MAX_BATCH_SIZE + 1})"
                    )
                logger.info(
                    f"Bulk created {len(objects)} {self.model_class.__name__} records"
                )
                return objects
            except Exception as e:
                db.rollback()
                logger.error(f"Error bulk creating {self.model_class.__name__}: {e}")
                raise

    def _object_to_dict(self, obj: T) -> Dict[str, Any]:
        """
        Helper method to convert an object to a dictionary suitable for SQL operations

        Args:
            obj: The model object to convert

        Returns:
            Dictionary representation of the object
        """
        obj_dict = {}
        for c in obj.__table__.columns:
            if c.primary_key or c.name in ("created_at", "updated_at"):
                continue
            value = getattr(obj, c.name)

            # Add the value
            obj_dict[c.name] = value
        return obj_dict

    def bulk_upsert(
        self,
        objects: List[T],
        unique_fields: Optional[List[str]] = None,
        session: Optional[Session] = None,
    ) -> List[T]:
        """
        Perform batch upsert with a generic approach that works across all SQL dialects

        Args:
            objects: List of model objects to insert/update
            unique_fields: Fields that determine uniqueness (for conflict resolution)
            session: Optional database session

        Returns:
            List of created/updated objects
        """
        if not objects:
            return []

        if not unique_fields:
            return self.bulk_create(objects, session)

        # Deduplicate objects based on unique fields
        deduplicated_objects = []
        seen_keys = set()

        for obj in objects:
            key_parts = []
            for field in unique_fields:
                value = getattr(obj, field)
                key_parts.append(str(value))

            key = ":".join(key_parts)

            if key not in seen_keys:
                seen_keys.add(key)
                deduplicated_objects.append(obj)  # This line was missing

        all_results = []

        # Process in batches
        for i in range(0, len(deduplicated_objects), MAX_BATCH_SIZE):
            batch = deduplicated_objects[i : i + MAX_BATCH_SIZE]

            with self.get_session(session) as db:
                try:
                    object_dicts = [self._object_to_dict(obj) for obj in batch]

                    # Create insert statement
                    insert_stmt = insert(self.model_class.__table__).values(
                        object_dicts
                    )

                    # Create upsert statement with returning clause
                    upsert_stmt = insert_stmt.on_conflict_do_update(
                        index_elements=unique_fields,
                        set_={
                            **{
                                col.name: insert_stmt.excluded[col.name]
                                for col in self.model_class.__table__.columns
                                if col.name
                                not in unique_fields + ["created_at", "updated_at"]
                            },
                            "updated_at": func.current_timestamp(),
                        },
                    ).returning(self.model_class.__table__)

                    # Execute and get results
                    result = db.execute(upsert_stmt)
                    db.commit()

                    # Convert result rows to model objects
                    result_rows = result.mappings().all()
                    batch_results = [self.model_class(**row) for row in result_rows]
                    all_results.extend(batch_results)

                    logger.info(
                        f"Upserted {len(batch_results)} {self.model_class.__name__} records (batch {i//MAX_BATCH_SIZE + 1})"
                    )

                except Exception as e:
                    db.rollback()
                    logger.error(
                        f"Error batch upserting {self.model_class.__name__}: {e}"
                    )
                    import traceback

                    logger.error(f"Error details: {traceback.format_exc()}")
                    raise

        return all_results

    def get_by_id(self, id: int, session: Optional[Session] = None) -> Optional[T]:
        """Get a record by id"""
        with self.get_session(session) as db:
            return db.query(self.model_class).filter(self.model_class.id == id).first()

    def get_many(
        self,
        limit: int = 100,
        offset: int = 0,
        filters: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        session: Optional[Session] = None,
    ) -> List[T]:
        """
        Get multiple records with pagination, filtering and ordering

        Args:
            limit: Maximum number of records to return
            offset: Number of records to skip
            filters: Dictionary of field:value pairs to filter by
            order_by: Field name to order by (prefix with '-' for descending)
            session: Optional database session

        Returns:
            List of model objects
        """
        with self.get_session(session) as db:
            query = db.query(self.model_class)

            # Apply filters
            if filters:
                for field, value in filters.items():
                    query = query.filter(getattr(self.model_class, field) == value)

            # Apply ordering
            if order_by:
                if order_by.startswith("-"):
                    field = order_by[1:]
                    query = query.order_by(getattr(self.model_class, field).desc())
                else:
                    query = query.order_by(getattr(self.model_class, field))

            # Apply pagination
            return query.offset(offset).limit(limit).all()

    def count(
        self,
        filters: Optional[Dict[str, Any]] = None,
        session: Optional[Session] = None,
    ) -> int:
        """
        Count records with optional filtering

        Args:
            filters: Dictionary of field:value pairs to filter by
            session: Optional database session

        Returns:
            Count of matching records
        """
        with self.get_session(session) as db:
            query = select(func.count()).select_from(self.model_class)

            # Apply filters
            if filters:
                for field, value in filters.items():
                    query = query.where(getattr(self.model_class, field) == value)

            return db.scalar(query)

    def update(
        self, id: int, updates: Dict[str, Any], session: Optional[Session] = None
    ) -> Optional[T]:
        """
        Update a record by id

        Args:
            id: Record ID
            updates: Dictionary of field:value pairs to update
            session: Optional database session

        Returns:
            Updated model object or None if not found
        """
        with self.get_session(session) as db:
            obj = db.query(self.model_class).filter(self.model_class.id == id).first()
            if obj:
                for key, value in updates.items():
                    setattr(obj, key, value)
                db.commit()
                logger.info(f"Updated {self.model_class.__name__} with id {id}")
                return obj
            return None

    def delete(self, id: int, session: Optional[Session] = None) -> bool:
        """
        Delete a record by id

        Args:
            id: Record ID
            session: Optional database session

        Returns:
            True if deleted, False if not found
        """
        with self.get_session(session) as db:
            obj = db.query(self.model_class).filter(self.model_class.id == id).first()
            if obj:
                db.delete(obj)
                db.commit()
                logger.info(f"Deleted {self.model_class.__name__} with id {id}")
                return True
            return False
