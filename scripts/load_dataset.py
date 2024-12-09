import logging
import os
import asyncio
import aioboto3
from botocore.config import Config
from typing import AsyncGenerator

from app.db.database import get_pg_connection

R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")

DATASET_BUCKET = "mizu-cmc"

# Set up logging at the top of the file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


async def get_object_metadata(s3_client, obj: dict) -> dict:
    """Get metadata for a single object"""
    try:
        head = await s3_client.head_object(Bucket=DATASET_BUCKET, Key=obj["Key"])

        # Parse key components
        key_parts = obj["Key"].split("/")
        if len(key_parts) >= 4:
            dataset = key_parts[0]
            data_type = key_parts[1]
            language = key_parts[2]
            md5 = key_parts[3].replace(".zz", "")
        else:
            raise Exception(f"Invalid key format: {obj['Key']}")

        metadata = head.get("Metadata", {})
        return {
            "name": dataset,
            "language": language,
            "data_type": data_type,
            "r2_key": obj["Key"],
            "md5": md5,
            "num_of_records": int(metadata.get("num_of_records", 0)),
            "decompressed_bytesize": int(metadata.get("decompressed_bytesize", 0)),
            "byte_size": int(obj["Size"]),
            "source": metadata.get("source", ""),
        }
    except Exception as e:
        logger.error(f"Error getting metadata for {obj['Key']}: {str(e)}")
        return None


async def list_r2_objects(prefix: str = "") -> AsyncGenerator[list[dict], None]:
    """Lists objects from R2 bucket and gets their metadata in batches"""
    logger.info(f"Starting to list objects with prefix: {prefix}")
    processed = 0
    errors = 0
    batch = []

    session = aioboto3.Session()
    async with session.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        config=Config(retries=dict(max_attempts=3)),
    ) as s3_client:
        try:
            paginator = s3_client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=DATASET_BUCKET, Prefix=prefix):
                if "Contents" not in page:
                    logger.warning(f"No contents found for prefix: {prefix}")
                    continue

                for obj in page["Contents"]:
                    batch.append(obj)

                    if len(batch) >= 1000:
                        # Process batch asynchronously
                        tasks = [get_object_metadata(s3_client, obj) for obj in batch]
                        results = await asyncio.gather(*tasks)

                        # Filter out None results (failed requests)
                        valid_results = [r for r in results if r is not None]
                        processed += len(valid_results)
                        errors += len(results) - len(valid_results)

                        logger.info(
                            f"Processed batch of {len(valid_results)} objects. Total: {processed}, Errors: {errors}"
                        )
                        yield valid_results
                        batch = []

                    break

            # Process remaining items
            if batch:
                tasks = [get_object_metadata(s3_client, obj) for obj in batch]
                results = await asyncio.gather(*tasks)
                valid_results = [r for r in results if r is not None]
                processed += len(valid_results)
                errors += len(results) - len(valid_results)
                yield valid_results

            logger.info(
                f"Completed listing objects. Total processed: {processed}, Errors: {errors}"
            )

        except Exception as e:
            logger.error(f"Error listing objects from R2: {str(e)}")
            return


def insert_batch_to_db(objects: list[dict]):
    """
    Insert a batch of objects into the dataset table
    """
    try:
        logger.info(f"Inserting batch of {len(objects)} records to database")
        with get_pg_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(
                """
                INSERT INTO datasets (
                    name, language, data_type, r2_key, md5,
                    num_of_records, decompressed_bytesize, byte_size, source
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9
                ) ON CONFLICT (md5) DO NOTHING
                """,
                [
                    (
                        obj["name"],
                        obj["language"],
                        obj["data_type"],
                        obj["r2_key"],
                        obj["md5"],
                        obj["num_of_records"],
                        obj["decompressed_bytesize"],
                        obj["byte_size"],
                        obj["source"],
                    )
                    for obj in objects
                ],
            )
            conn.commit()  # Add commit to save changes
        logger.info("Successfully inserted batch to database")
    except Exception as e:
        logger.error(f"Error inserting batch into database: {str(e)}")


async def load_dataset(dataset: str, data_type: str):
    try:
        prefix = f"{dataset}/{data_type}"
        total_processed = 0

        logger.info(f"Starting dataset load for {prefix}")

        async for batch_metadata in list_r2_objects(prefix):
            if batch_metadata:
                insert_batch_to_db(batch_metadata)
                total_processed += len(batch_metadata)
                logger.info(f"Total processed: {total_processed}")

        logger.info(
            f"Completed loading dataset {prefix}. Total processed: {total_processed}"
        )

    except KeyboardInterrupt:
        logger.info("Received interrupt signal. Exiting...")
        raise


def start():
    import asyncio

    asyncio.run(load_dataset("CC-MAIN-2024-46", "text"))
