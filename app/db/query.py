import psycopg2
from contextlib import contextmanager
from fastapi import HTTPException
import os

from psycopg2 import connection

class Connection:
    def __init__(self):
        self.DATABASE_URL = os.getenv("POSTGRES_URL", "postgresql://user:password@localhost:5432/dbname")

    @contextmanager
    def get_pg_connection(self):
        conn = None
        cur = None
        try:
            conn = psycopg2.connect(self.DATABASE_URL)
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

def save_query(dataset: str, query: str, publisher: str) -> int:
    db = Connection()
    with db.get_pg_connection() as cur:
        cur.execute(
            """INSERT INTO queries 
               (dataset, query_text, publisher, progress) 
               VALUES (%s, %s, %s, %s) 
               RETURNING id""",
            (dataset, query, publisher, "pending")
        )
        return cur.fetchone()[0]
