from pydantic import BaseModel, Field
from typing import Any, Dict
from datetime import datetime
import uuid


class MessageMetadata(BaseModel):
    message_id: str = Field(default_factory=lambda: str(uuid.uuid5(uuid.NAMESPACE_URL, "crawler")))
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    source: str = "crawler"
    version: str = "1.0"


class Message(BaseModel):
    """Generic message structure"""
    type: str
    payload: Dict[str, Any]
    metadata: MessageMetadata = Field(default_factory=MessageMetadata)

    class Config:
        arbitrary_types_allowed = True
