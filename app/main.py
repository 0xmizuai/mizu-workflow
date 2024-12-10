from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from typing import Annotated
from app.db.database import (
    get_db_session,
    get_owned_queries,
    save_new_query,
    save_query_result,
    get_query_results,
    get_query_detail,
)
from app.auth import get_user, verify_internal_service
from contextlib import asynccontextmanager
import uvicorn
from app.models.service import (
    PaginatedQueryResults,
    QueryContext,
    QueryDetails,
    QueryList,
    QueryResult,
    JobResult,
    RegisterQueryRequest,
    RegisterQueryResponse,
)
from app.models.query import Query
from app.response import build_ok_response, error_handler


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


@app.post("/register_query")
@error_handler
async def register_query(
    query: RegisterQueryRequest, owner: Annotated[str, Depends(get_user)]
):
    with get_db_session() as session:
        query_id = save_new_query(
            session,
            dataset=query.dataset,
            language=query.language,
            query_text=query.query_text,
            model=query.model,
            owner=owner,
        )
        return build_ok_response(RegisterQueryResponse(query_id=query_id))


@app.post("/save_query_result")
@error_handler
async def save_query_result_callback(
    result: JobResult, _: Annotated[bool, Depends(verify_internal_service)]
):
    with get_db_session() as session:
        save_query_result(session, result)
        return build_ok_response()


@app.get("/queries/{query_id}/results", response_model=PaginatedQueryResults)
@error_handler
async def get_query_results_endpoint(
    query_id: int,
    user: Annotated[str, Depends(get_user)],
    page: int = Query(default=1, ge=1),
):
    with get_db_session() as session:
        # Verify query belongs to publisher
        query = (
            session.query(Query)
            .filter(
                Query.id == query_id,
                Query.owner == user,
                Query.status != "pending",
                Query.results.any(),
            )
            .first()
        )

        if not query:
            raise HTTPException(status_code=404, detail="Query not found")

        # Get paginated results
        results, total = get_query_results(session, query_id, page)

        page_size = 1000
        return build_ok_response(
            PaginatedQueryResults(
                results=[
                    QueryResult(
                        results=r.results,
                    )
                    for r in results
                ],
                total=total,
                page=page,
                page_size=page_size,
                has_more=total > page * page_size,
            )
        )


@app.get("/queries/{query_id}", response_model=QueryContext)
async def get_query_context(query_id: int, user: Annotated[str, Depends(get_user)]):
    with get_db_session() as session:
        query = get_query_detail(session, query_id)
        if not query:
            raise HTTPException(status_code=404, detail="Query not found")
        return build_ok_response(
            QueryContext(query_text=query.query_text, model=query.model)
        )


@app.get("/queries", response_model=QueryContext)
async def get_all_queries(user: Annotated[str, Depends(get_user)]):
    with get_db_session() as session:
        queries = get_owned_queries(session, owner=user)
        return build_ok_response(
            QueryList(
                queries=[
                    QueryDetails(
                        query_id=q.id,
                        dataset=q.dataset,
                        language=q.language,
                        query_text=q.query_text,
                        model=q.model,
                        created_at=q.created_at,
                    )
                    for q in queries
                ]
            )
        )


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
