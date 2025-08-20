from pydantic import BaseModel, AnyHttpUrl, Field
from typing import List, Optional
from datetime import datetime

class LinkIn(BaseModel):
    url: AnyHttpUrl
    title: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    notes: Optional[str] = None

class LinkUpdate(BaseModel):
    url: Optional[AnyHttpUrl] = None
    title: Optional[str] = None
    tags: Optional[List[str]] = None
    notes: Optional[str] = None

class Link(LinkIn):
    id: str
    created_at: datetime
    updated_at: datetime
