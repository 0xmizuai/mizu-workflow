from pydantic import BaseModel
from datetime import datetime
from typing import List

class QueryResult(BaseModel):
    query_id: int
    url: str
    warc_id: str
    text: str
    crawled_at: datetime
    processed_at: datetime

class PaginatedQueryResults(BaseModel):
    results: List[QueryResult]
    total: int
    page: int
    page_size: int
    has_more: bool

class QueryDetail(BaseModel):
    id: int
    dataset: str
    query: str
    publisher: str
    created_at: datetime