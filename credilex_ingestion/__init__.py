"""Credilex Ingestion Python SDK.

Public API:
    from credilex_ingestion import IngestClient, AsyncIngestClient
    from credilex_ingestion import BatchSplitter, BatchUploadResult
    from credilex_ingestion import upload_document, upload_documents_batch
    from credilex_ingestion import IngestError, ValidationError
"""
from credilex_ingestion._version import __version__
from credilex_ingestion.batch import BatchSplitter, BatchUploadResult
from credilex_ingestion.client import AsyncIngestClient, IngestClient
from credilex_ingestion.documents import upload_document, upload_documents_batch
from credilex_ingestion.errors import (
    AuthError,
    IngestConfigError,
    IngestError,
    NotFoundError,
    QuotaError,
    ServerError,
    ValidationError as IngestValidationError,
)

__all__ = [
    "__version__",
    "IngestClient",
    "AsyncIngestClient",
    "BatchSplitter",
    "BatchUploadResult",
    "upload_document",
    "upload_documents_batch",
    "IngestError",
    "AuthError",
    "IngestValidationError",
    "QuotaError",
    "NotFoundError",
    "ServerError",
    "IngestConfigError",
]
