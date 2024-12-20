import logging
import os
import asyncio
import aioboto3
from botocore.config import Config
from typing import AsyncGenerator
from sqlalchemy.sql import text
from datetime import date

from app.db.database import get_db_session

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
        #       head = await s3_client.head_object(Bucket=DATASET_BUCKET, Key=obj["Key"])

        # Parse key components
        key_parts = obj["Key"].split("/")
        if len(key_parts) >= 4:
            dataset = key_parts[0]
            data_type = key_parts[1]
            language = key_parts[2]
            md5 = key_parts[3].replace(".zz", "")
        else:
            raise Exception(f"Invalid key format: {obj['Key']}")

        #         metadata = head.get("Metadata", {})
        return {
            "name": dataset,
            "language": language,
            "data_type": data_type,
            "md5": md5,
            "num_of_records": 0,
            "decompressed_byte_size": 0,
            "byte_size": int(obj.get("Size", 0)),
            "source": "",
        }
    except Exception as e:
        logger.error(f"Error getting metadata for {obj['Key']}: {str(e)}")
        return None


async def list_r2_objects(
    prefix: str = "", offset: str = ""
) -> AsyncGenerator[list[dict], None]:
    """Lists objects from R2 bucket and gets their metadata in batches"""
    logger.info(f"Starting to list objects with prefix: {prefix}")
    processed = 0
    errors = 0

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
            async for page in paginator.paginate(
                Bucket=DATASET_BUCKET,
                Prefix=prefix,
                StartAfter=offset,
            ):
                if "Contents" not in page:
                    logger.warning(f"No contents found for prefix: {prefix}")
                    continue

                # Process the entire page directly
                tasks = [
                    get_object_metadata(s3_client, obj) for obj in page["Contents"]
                ]
                results = await asyncio.gather(*tasks)

                # Filter out None results (failed requests)
                valid_results = [r for r in results if r is not None]
                processed += len(valid_results)
                errors += len(results) - len(valid_results)

                logger.info(
                    f"Processed batch of {len(valid_results)} objects. Total: {processed}, Errors: {errors}"
                )
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
        with get_db_session() as session:
            session.execute(
                text(
                    """
                INSERT INTO datasets (
                    name, language, data_type, md5,
                    num_of_records, decompressed_byte_size, byte_size, source
                ) VALUES (
                    :name, :language, :data_type, :md5,
                    :num_of_records, :decompressed_byte_size, :byte_size, :source
                ) ON CONFLICT (md5) DO NOTHING
                """
                ),
                objects,
            )
        logger.info("Successfully inserted batch to database")
    except Exception as e:
        logger.error(f"Error inserting batch into database: {str(e)}")


async def load_dataset(dataset: str, data_type: str, offset: str = ""):
    logger.info(
        f"Loading dataset {dataset} with data type {data_type} with offset {offset}"
    )
    try:
        prefix = f"{dataset}/{data_type}"
        total_processed = 0

        logger.info(f"Starting dataset load for {prefix}")

        async for batch_metadata in list_r2_objects(prefix, offset):
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


def get_last_processed_key() -> str:
    """Get the r2_key of the last processed item from the database"""
    try:
        with get_db_session() as session:
            result = session.execute(
                text("SELECT name, data_type, language, md5 FROM datasets ORDER BY id DESC LIMIT 1")
            ).fetchone()

            if result:
                last_key = f"{result[0]}/{result[1]}/{result[2]}/{result[3]}.zz"
                logger.info(f"Resuming from last processed key: {last_key}")
                return last_key

            logger.info("No previous progress found, starting from beginning")
            return ""
    except Exception as e:
        logger.error(f"Error getting last processed key: {str(e)}")
        return ""


def update_dataset_stats():
    """
    Calculate and store dataset statistics in the dataset_stats table
    """
    logger.info("Starting dataset statistics calculation")
    try:
        with get_db_session() as session:
            # Get statistics grouped by language and r2_prefix
            stats = session.execute(
                text(
                    """
                    WITH prefix_extract AS (
                        SELECT 
                            language,
                            data_type,
                            name,
                            md5,
                        FROM datasets
                    )
                    SELECT 
                        language,
                        data_type,
                        name,
                        COUNT(*) as total_objects,
                    FROM prefix_extract
                    GROUP BY name, language, data_type
                """
                )
            ).fetchall()

            # Insert or update statistics
            current_date = date.today()
            for stat in stats:
                session.execute(
                    text(
                        """
                        INSERT INTO dataset_stats 
                            (language, data_type, name, total_objects)
                        VALUES 
                            (:language, :data_type, :name, :total_objects)
                        ON CONFLICT (language, data_type, name) 
                        DO UPDATE SET
                            total_objects = EXCLUDED.total_objects,
                            created_at = CURRENT_TIMESTAMP
                    """
                    ),
                    {
                        "language": stat.language,
                        "data_type": stat.data_type,
                        "name": stat.name,
                        "total_objects": stat.total_objects,
                    },
                )

            session.commit()
            logger.info(
                f"Successfully updated dataset statistics for {len(stats)} combinations"
            )

    except Exception as e:
        logger.error(f"Error updating dataset statistics: {str(e)}")


def start():
    import asyncio
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--resume", action="store_true", help="Resume from last processed item"
    )
    parser.add_argument(
        "--stats", action="store_true", help="Update dataset statistics"
    )
    args = parser.parse_args()

    if args.stats:
        update_dataset_stats()
        return

    offset = get_last_processed_key() if args.resume else ""
    asyncio.run(load_dataset("CC-MAIN-2024-46", "text", offset))
