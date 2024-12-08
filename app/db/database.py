from datetime import datetime
import psycopg2
from contextlib import contextmanager
from fastapi import HTTPException
import os
from typing import Optional

class Connection:
    def __init__(self):
        self.POSTGRES_URL = os.environ["POSTGRES_URL"]

    @contextmanager
    def get_pg_connection(self):
        conn = None
        cur = None
        try:
            conn = psycopg2.connect(self.POSTGRES_URL)
            cur = conn.cursor()
            yield cur
            conn.commit()
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

def save_query(
    cur, 
    dataset_id: int, 
    query: str, 
    publisher: str, 
    language: str = 'en', 
    progress: int = 0
) -> int:
    cur.execute(
        """INSERT INTO queries 
           (dataset_id, query_text, publisher, language, progress) 
           VALUES (%s, %s, %s, %s, %s) 
           RETURNING id""",
        (dataset_id, query, publisher, language, progress)
    )
    return cur.fetchone()[0]


def update_query_progress(cur, query_id: int, progress: int) -> None:
    cur.execute(
        """UPDATE queries 
           SET progress = %s 
           WHERE id = %s""",
        (progress, query_id)
    )


def save_query_result(
    cur,
    query_id: int,
    url: str,
    warc_id: str,
    text: Optional[str] = None,
    crawled_at: Optional[datetime] = None,
    processed_at: Optional[datetime] = None
) -> int:
    """
    Save a query result to the database.
    
    Args:
        cur: Database cursor
        query_id: ID of the parent query
        url: URL of the content
        warc_id: WARC record ID
        text: Extracted text content (optional)
        crawled_at: When the content was crawled (optional)
        processed_at: When the content was processed (optional)
    
    Returns:
        int: ID of the newly created query result
    """
    cur.execute(
        """INSERT INTO query_results 
           (query_id, url, warc_id, text, crawled_at, processed_at) 
           VALUES (%s, %s, %s, %s, %s, %s) 
           RETURNING id""",
        (query_id, url, warc_id, text, crawled_at, processed_at)
    )
    return cur.fetchone()[0]


def save_data_record(
    cur,
    name: str,
    data_type: str,
    r2_key: str,
    byte_size: int,
    md5: str,
    language: str = 'en',
    num_of_records: int = None,
    decompressed_byte_size: int = None,
    processed_at: datetime = None
) -> int:
    cur.execute(
        """INSERT INTO datasets 
           (name, language, data_type, r2_key, md5, byte_size, 
            num_of_records, decompressed_byte_size, processed_at) 
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
           RETURNING id""",
        (name, language, data_type, r2_key, md5, byte_size,
         num_of_records, decompressed_byte_size, processed_at)
    )
    return cur.fetchone()[0]
