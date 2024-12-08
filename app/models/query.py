from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class QueryResult(BaseModel):
    query_id: int
    url: str
    warc_id: str
    text: Optional[str] = None
    crawled_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None 