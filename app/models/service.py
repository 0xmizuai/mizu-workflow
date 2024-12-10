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
    error_result: Optional[ErrorResult] = Field(alias="errorResult", default=None)
    classify_result: list[ClassifyResult] = Field(alias="classifyResult", default=[])


class QueryResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    results: list[ClassifyResult] = Field(alias="results", default=[])


class PaginatedQueryResults(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    results: List[QueryResult]
    total: int
    page: int
    page_size: int = Field(alias="pageSize")
    has_more: bool


class QueryContext(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    query_text: str = Field(alias="queryText")
    model: str


class RegisterQueryRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    dataset: str
    language: str
    query_text: str = Field(alias="queryText")
    model: str


class RegisterQueryResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    query_id: int = Field(alias="queryId")


class QueryDetails(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    query_id: int = Field(alias="queryId")
    dataset: str = Field(alias="dataset")
    query_text: str = Field(alias="queryText")
    model: str = Field(alias="model")
    language: str = Field(alias="language")
    created_at: datetime = Field(alias="createdAt")


class QueryList(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    queries: list[QueryDetails] = Field(alias="queries", default=[])
