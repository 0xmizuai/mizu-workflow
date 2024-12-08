from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from typing import Annotated
from app.db.database import Connection, save_query, save_query_result
from app.auth import get_user, verify_internal_service
from contextlib import asynccontextmanager
import uvicorn

from app.models.query import QueryResult

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
