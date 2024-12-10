from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Annotated
from app.db.database import (
    get_db_session,
    save_query,
    save_query_result,
    get_query_results,
    get_query_detail,
)
from app.auth import get_user, verify_internal_service
from contextlib import asynccontextmanager
import uvicorn
from app.models.query import PaginatedQueryResults, QueryResult, QueryDetail
from app.models.service import JobResult


@asynccontextmanager
async def lifespan(app: FastAPI):
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


@app.get("/register_query")
async def register_query(
    dataset: str, query: str, publisher: Annotated[str, Depends(get_user)]
):
    with get_db_session() as session:
        query_id = save_query(
            session, dataset_id=dataset, query=query, publisher=publisher
        )
        return {
            "query_id": query_id,
            "dataset": dataset,
            "query": query,
            "publisher": publisher,
            "progress": "pending",
            "status": "saved",
        }


@app.post("/save_query_result")
async def save_query_result_callback(
    result: JobResult, _: Annotated[bool, Depends(verify_internal_service)]
):
    try:
        with get_db_session() as session:
            result_id = save_query_result(session, result)
            return {"result_id": result_id, "status": "saved"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/queries/{query_id}/results", response_model=PaginatedQueryResults)
async def get_query_results_endpoint(
    query_id: int,
    publisher: Annotated[str, Depends(get_user)],
    page: int = Query(default=1, ge=1),
):
    try:
        with get_db_session() as session:
            # Verify query belongs to publisher
            query = (
                session.query(Query)
                .filter(Query.id == query_id, Query.publisher == publisher)
                .first()
            )

            if not query:
                raise HTTPException(status_code=404, detail="Query not found")

            # Get paginated results
            results, total = get_query_results(session, query_id, page)

            page_size = 1000
            return PaginatedQueryResults(
                results=[
                    QueryResult(
                        query_id=query_id,
                        results=r.results,
                    )
                    for r in results
                ],
                total=total,
                page=page,
                page_size=page_size,
                has_more=total > page * page_size,
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/queries/{query_id}", response_model=QueryDetail)
async def get_query_detail_endpoint(
    query_id: int, publisher: Annotated[str, Depends(get_user)]
):
    try:
        with get_db_session() as session:
            query = get_query_detail(session, query_id)

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
