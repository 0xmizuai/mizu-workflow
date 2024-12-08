import os
import aiohttp
from typing import List
from datetime import datetime
from app.db.database import get_pg_connection

class BatchClassify:
    def __init__(self, query_id: int, texts: List[str], urls: List[str]):
        self.query_id = query_id
        self.texts = texts
        self.urls = urls
        self.created_at = datetime.utcnow()

async def publish_jobs(query_id: int, batch_size: int = 1000) -> int:
    published_count = 0

    with get_pg_connection() as cur:
        # Get query details to know what dataset to read from
        cur.execute(
            """SELECT d.name, q.total_published, q.language 
               FROM queries q 
               JOIN datasets d ON q.dataset_id = d.id 
               WHERE q.id = %s""",
            (query_id,)
        )
        result = cur.fetchone()
        if not result:
            return 0
            
        dataset_name, total_published, language = result
        
        # Get next batch of unprocessed records
        cur.execute(
            """SELECT id, text, url 
               FROM datasets 
               WHERE name = %s AND language = %s
               ORDER BY id 
               LIMIT %s OFFSET %s""",
            (dataset_name, language, batch_size, total_published)
        )
        records = cur.fetchall()
        
        if not records:
            return 0

        # Prepare batch
        texts = [r[1] for r in records]
        urls = [r[2] for r in records]
        batch = BatchClassify(query_id, texts, urls)

        # Publish to MIZU node
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{os.environ['MIZU_NODE_URL']}/classify",
                json=batch.__dict__
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(f"Failed to publish batch: {error_text}")
        
        # Update total_published
        published_count = len(records)
        cur.execute(
            """UPDATE queries 
               SET total_published = total_published + %s 
               WHERE id = %s""",
            (published_count, query_id)
        )
        
    return published_count 

async def publish_all_queries(batch_size: int = 100) -> dict:
    db = Connection()
    results = {}
    
    with db.get_pg_connection() as cur:
        # Get processing queries that haven't reached target count
        cur.execute(
            """SELECT id 
               FROM queries 
               WHERE total_published < target_count 
               AND status = 'publishing'"""
        )
        query_ids = [row[0] for row in cur.fetchall()]
        
    # Process each query
    for query_id in query_ids:
        try:
            published = await publish_jobs(query_id)
            results[query_id] = {
                'status': 'success',
                'published': published
            }
        except Exception as e:
            results[query_id] = {
                'status': 'error',
                'error': str(e)
            }
    
    return results 