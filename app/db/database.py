from datetime import datetime
import psycopg2
from contextlib import contextmanager
from fastapi import HTTPException
import os
from typing import Optional


@contextmanager
def get_pg_connection():
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(os.environ["POSTGRES_URL"])
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


def save_query_result(
    cur,
    query_id: int,
    url: str,
    warc_id: str,
    text: Optional[str] = None,
    crawled_at: Optional[datetime] = None,
    processed_at: Optional[datetime] = None
) -> int:
    cur.execute(
        """WITH inserted_result AS (
               INSERT INTO query_results 
               (query_id, url, warc_id, text, crawled_at, processed_at) 
               VALUES (%s, %s, %s, %s, %s, %s) 
               RETURNING id
           )
           UPDATE queries 
           SET total_processed = total_processed + 1 
           WHERE id = %s;
           SELECT id FROM inserted_result;""",
        (query_id, url, warc_id, text, crawled_at, processed_at, query_id)
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

def get_query_results(cur, query_id: int, page: int = 1, page_size: int = 1000) -> tuple[list, int]:
    # Get total count
    cur.execute(
        "SELECT COUNT(*) FROM query_results WHERE query_id = %s",
        (query_id,)
    )
    total = cur.fetchone()[0]
    
    # Get paginated results
    offset = (page - 1) * page_size
    cur.execute(
        """SELECT query_id, url, warc_id, text, crawled_at, processed_at 
           FROM query_results 
           WHERE query_id = %s 
           ORDER BY id 
           LIMIT %s OFFSET %s""",
        (query_id, page_size, offset)
    )
    results = cur.fetchall()
    
    return results, total

def get_query_status(cur, query_id: int) -> dict:
    # 1. Get query details
    cur.execute(
        """
        SELECT 
            id,
            dataset,
            progress,
            created_at,
            language
        FROM queries
        WHERE id = %s
        """,
        (query_id,)
    )
    result = cur.fetchone()
    if not result:
        return None

    # 2. Get total dataset size for this dataset+language combination
    cur.execute(
        """
        SELECT COUNT(*) 
        FROM datasets
        WHERE name = %s AND language = %s
        """,
        (result[1], result[4])  # dataset_id and language
    )
    dataset_size = cur.fetchone()[0]

    # 3. Get total results for this specific query
    cur.execute(
        """
        SELECT COUNT(*) 
        FROM query_results
        WHERE query_id = %s
        """,
        (query_id,)
    )
    query_results_count = cur.fetchone()[0]

    return {
        "query_id": result[0],
        "dataset_id": result[1],
        "processed_records": result[2],
        "created_at": result[3],
        "language": result[4],
        "dataset_size": dataset_size,
        "query_results_count": query_results_count
    }


def get_query_detail(cur, query_id: int) -> dict:
    cur.execute(
        """
        SELECT 
            id,
            dataset_id,
            query_text,
            publisher,
            language,
            progress,
            created_at
        FROM queries
        WHERE id = %s
        """,
        (query_id,)
    )
    result = cur.fetchone()
    if not result:
        return None
        
    return {
        "id": result[0],
        "dataset_id": result[1],
        "query_text": result[2],
        "publisher": result[3],
        "language": result[4],
        "progress": result[5],
        "created_at": result[6]
    }
