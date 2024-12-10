from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, ConfigDict, Field

from pydantic import BaseModel, ConfigDict, Field


class ClassifyResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    warc_id: str = Field(alias="warcId")
    uri: str
    text: str


class ErrorResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    code: int
    message: Optional[str] = Field(default=None)


class BatchClassifyContext(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    data_url: str = Field(alias="dataUrl")
    batch_size: int = Field(alias="batchSize")
    bytesize: int = Field(alias="bytesize")
    decompressed_byte_size: int = Field(alias="decompressedByteSize")
    checksum_md5: str = Field(alias="checksumMd5")
    classifier_id: int = Field(alias="classifierId")


class PublishBatchClassifyJobRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    data: list[BatchClassifyContext]


class JobResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    job_id: str = Field(alias="jobId")
    error_result: Optional[ErrorResult] = None
    classify_result: list[ClassifyResult] = []


class QueryResult(BaseModel):
    query_id: int
    results: list[ClassifyResult]


class PaginatedQueryResults(BaseModel):
    results: List[QueryResult]
    total: int
    page: int
    page_size: int
    has_more: bool


class QueryDetail(BaseModel):
    dataset: str
    query: str
    created_at: datetime
