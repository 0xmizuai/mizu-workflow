import os
from typing import Any, AsyncGenerator
import aiohttp
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.query_result import QueryResult
from app.models.service import PublishBatchClassifyJobRequest, BatchClassifyContext
from app.models.dataset import Dataset
from app.models.service import Query
from datetime import datetime

BATCH_SIZE = 1000

async def create_batch_classify_requests(session: AsyncSession, query: Query) -> AsyncGenerator[PublishBatchClassifyJobRequest, None]:
    """Creates batch classify requests for dataset records in batches"""
    # Get total count for offset calculation
    count_stmt = select(func.count()).select_from(Dataset).where(
        Dataset.language == query.language,
        Dataset.name == query.dataset
    )
    total_count = await session.scalar(count_stmt)

    if not total_count:
        raise ValueError(f"No dataset found for query: {query.query_text}")

    # Process datasets in batches using offset/limit
    for offset in range(0, total_count, BATCH_SIZE):
        stmt = select(Dataset).where(
            Dataset.language == query.language,
            Dataset.name == query.dataset
        ).offset(offset).limit(BATCH_SIZE)

        result = await session.execute(stmt)
        batch_datasets = result.scalars().all()

        # Create contexts for this batch
        batch_contexts = []
        for dataset in batch_datasets:
            context = BatchClassifyContext(
                dataUrl=dataset.r2_key,
                batchSize=0,
                bytesize=dataset.byte_size,
                decompressedByteSize=dataset.decompressed_byte_size,
                checksumMd5=dataset.md5,
                classifierId=query.id
            )
            batch_contexts.append(context)

        if batch_contexts:
            yield PublishBatchClassifyJobRequest(data=batch_contexts)

async def save_batch_query_results(session: AsyncSession, query: Query, batch_response: dict, batch_contexts: list[BatchClassifyContext]):
    """Creates QueryResults after getting job IDs from the response"""
    job_ids = batch_response.get('ids')
    if not job_ids:
        raise ValueError("No job_ids in response")

    # Create QueryResults by zipping contexts with job_ids
    for job_id, context in zip(job_ids, batch_contexts):
        query_result = QueryResult(
            query_id=query.id,
            job_id=job_id,
            data_id=context.data_id,
            status="pending",
            created_at=datetime.utcnow()
        )
        session.add(query_result)
    
    # Update query status
    query.status = "published"
    query.total_published += len(batch_contexts)
    
    await session.flush()

async def process_query(session: AsyncSession, query: Query):
    """Main function to process query and create jobs"""
    try:
        async for batch_request in create_batch_classify_requests(session, query):
            # First publish the batch
            response = await publish_batch_classify_jobs(batch_request)

            # Then create QueryResults with the returned job ID
            await save_batch_query_results(session, query, response, batch_request.data)
            
        await session.commit()
    except Exception as e:
        await session.rollback()
        raise

async def publish_batch_classify_jobs(request: PublishBatchClassifyJobRequest) -> dict[str, Any]:
    """Publishes batch classify jobs to the Mizu node service"""
    mizu_url = os.environ["MIZU_NODE_SERVICE_URL"]
    endpoint = f"{mizu_url}/publish_batch_classify_job"
    
    async with aiohttp.ClientSession() as session:
        async with session.post(
            endpoint,
            json=request.model_dump(by_alias=True)
        ) as response:
            response.raise_for_status()
            return await response.json()
