from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Annotated
from app.db.database import Connection, save_query, save_query_result, get_query_results, update_query_progress
from app.auth import get_user, verify_internal_service
from contextlib import asynccontextmanager
import uvicorn
from app.models.query import PaginatedQueryResults, QueryResult, QueryDetail

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = Connection()
    yield

app = FastAPI(lifespan=lifespan)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"status": "ok"}

@app.get("/health")
async def health_check():
    return {"status": "ok"}

@app.get("/submit_query")
async def submit_query(
    dataset: str, 
    query: str, 
    publisher: Annotated[str, Depends(get_user)]
):
    with app.state.db.get_pg_connection() as cur:
        query_id = save_query(cur, dataset, query, publisher)
        return {
            "query_id": query_id,
            "dataset": dataset,
            "query": query,
            "publisher": publisher,
            "progress": "pending",
            "status": "saved"
        }


@app.post("/save_query_result")
async def save_query_result_callback(
    result: QueryResult,
    _: Annotated[bool, Depends(verify_internal_service)]
):
    try:
        with app.state.db.get_pg_connection() as cur:
            result_id = save_query_result(
                cur,
                result.query_id,
                result.url,
                result.warc_id,
                result.text,
                result.crawled_at,
                result.processed_at
            )
            return {
                "result_id": result_id,
                "status": "saved"
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/queries/{query_id}/results", response_model=PaginatedQueryResults)
async def get_query_results(
    query_id: int,
    publisher: Annotated[str, Depends(get_user)],
    page: int = Query(default=1, ge=1)
):
    try:
        with app.state.db.get_pg_connection() as cur:
            # Verify query belongs to publisher
            cur.execute(
                "SELECT publisher FROM queries WHERE id = %s",
                (query_id,)
            )
            query = cur.fetchone()
            if not query or query[0] != publisher:
                raise HTTPException(status_code=404, detail="Query not found")

            # Get paginated results
            results, total = get_query_results(cur, query_id)

            # Convert to Pydantic models
            query_results = [
                QueryResult(
                    query_id=query_id,
                    url=r[1],
                    warc_id=r[2],
                    text=r[3],
                    crawled_at=r[4],
                    processed_at=r[5]
                ) for r in results
            ]

            page_size = 1000
            return PaginatedQueryResults(
                results=query_results,
                total=total,
                page=page,
                page_size=page_size,
                has_more=total > page * page_size
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/queries/{query_id}", response_model=QueryDetail)
async def get_query_detail(
    query_id: int,
    publisher: Annotated[str, Depends(get_user)]
):
    try:
        with app.state.db.get_pg_connection() as cur:
            query = get_query_detail(cur, query_id)
            
            if not query:
                raise HTTPException(status_code=404, detail="Query not found")
                
            # Verify publisher ownership
            if query["publisher"] != publisher:
                raise HTTPException(status_code=404, detail="Query not found")
                
            return QueryDetail(**query)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def start():
    """Start production server"""
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        proxy_headers=True,
        forwarded_allow_ips="*",
    )

def start_dev():
    """Start development server with hot reload"""
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=["app"],
    )

if __name__ == "__main__":
    start_dev()
