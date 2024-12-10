from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
from fastapi import HTTPException
import os
from datetime import datetime, timezone

from app.models import Base, Dataset, Query, QueryResult
from app.models.service import JobResult

# Create engine and session factory
engine = create_engine(os.environ["POSTGRES_URL"])
SessionLocal = sessionmaker(bind=engine)


@contextmanager
def get_db_session():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        session.close()


def save_new_query(
    session: Session,
    dataset: str,
    language: str,
    query_text: str,
    model: str,
    owner: str,
    status: str = "pending",
) -> int:
    query_obj = Query(
        dataset=dataset,
        language=language,
        query_text=query_text,
        model=model,
        owner=owner,
        status=status,
    )
    session.add(query_obj)
    session.flush()  # To get the ID
    return query_obj.id


def add_query_result(
    session: Session,
    query_id: int,
    data_id: int,
    job_id: str,
) -> int:
    query_result = QueryResult(
        query_id=query_id,
        data_id=data_id,
        job_id=job_id,
        status="pending",
    )
    session.add(query_result)
    session.flush()
    return query_result.id


def save_query_result(
    session: Session,
    result: JobResult,
) -> int:
    # Try to find existing QueryResult
    query_result = (
        session.query(QueryResult).filter(QueryResult.job_id == result.job_id).first()
    )

    if query_result:
        # Update existing record
        query_result.data_id = result.data_id
        query_result.result = result.classify_result or result.error_result
        query_result.finished_at = datetime.now(timezone.utc)
        query_result.status = "error" if result.error_result else "processed"
    else:
        raise HTTPException(status_code=404, detail="QueryResult not found")

    # Update the query's total_processed
    query = session.query(Query).filter(Query.id == query_result.query_id).first()
    if query:
        query.total_processed += 1

    session.flush()
    return query_result.id


def save_data_record(
    session: Session,
    name: str,
    data_type: str,
    r2_key: str,
    byte_size: int,
    md5: str,
    language: str = "en",
    num_of_records: int = None,
    decompressed_byte_size: int = None,
    source: str = None,
) -> int:
    dataset = Dataset(
        name=name,
        language=language,
        data_type=data_type,
        r2_key=r2_key,
        md5=md5,
        byte_size=byte_size,
        num_of_records=num_of_records,
        decompressed_byte_size=decompressed_byte_size,
        source=source,
    )
    session.add(dataset)
    session.flush()
    return dataset.id


def get_query_results(
    session: Session, query_id: int, page: int = 1, page_size: int = 1000
) -> tuple[list, int]:
    # Get total count
    total = session.query(QueryResult).filter(QueryResult.query_id == query_id).count()

    # Get paginated results
    results = (
        session.query(QueryResult)
        .filter(QueryResult.query_id == query_id)
        .filter(QueryResult.status == "processed")
        .filter(QueryResult.result.isnot(None))
        .order_by(QueryResult.id)
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return results, total


def get_query_status(session: Session, query_id: int) -> dict:
    query = session.query(Query).filter(Query.id == query_id).first()
    if not query:
        return None

    dataset_size = (
        session.query(Dataset)
        .filter(Dataset.name == query.dataset.name, Dataset.language == query.language)
        .count()
    )

    query_results_count = (
        session.query(QueryResult).filter(QueryResult.query_id == query_id).count()
    )

    return {
        "query_id": query.id,
        "dataset_id": query.dataset_id,
        "processed_records": query.progress,
        "created_at": query.created_at,
        "language": query.language,
        "dataset_size": dataset_size,
        "query_results_count": query_results_count,
    }


def get_query_detail(session: Session, query_id: int) -> Query:
    return session.query(Query).filter(Query.id == query_id).first()


def get_owned_queries(session: Session, owner: str) -> list[dict]:
    return session.query(Query).filter(Query.publisher == owner).all()
