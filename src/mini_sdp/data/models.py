"""Data model for the Data Controller."""
from datetime import datetime
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel


class Status(StrEnum):
    """Status of an observing or processing block."""
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    FINISHED = "finished"
    FAILED = "failed"


class QAStatus(StrEnum):
    """Outcome of manual quality assessment of a processed dataset."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class DatasetStatus(StrEnum):
    """Storage lifecycle of a dataset."""
    ALLOCATED = "allocated"
    STORED = "stored"
    RELEASING = "releasing"
    ARCHIVED = "archived"
    DELETED = "deleted"


class RawDataset(BaseModel):
    """Raw visibilities measurement set."""
    id: int | None = None
    path: Path
    size: int  # bytes
    status: DatasetStatus = DatasetStatus.ALLOCATED


class ProcessedDataset(BaseModel):
    """Processed image products (FITS) from a measurement set."""
    id: int | None = None
    path: Path
    size: int  # bytes
    status: DatasetStatus = DatasetStatus.ALLOCATED


class ObservingBlock(BaseModel):
    id: int | None = None
    status: Status = Status.NOT_STARTED
    raw_dataset_id: int | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    error: str | None = None


class ProcessingBlock(BaseModel):
    id: int | None = None
    observing_block_id: int
    status: Status = Status.NOT_STARTED
    processed_dataset_id: int | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    error: str | None = None


class QAAssessment(BaseModel):
    """Manual assessment of one processed dataset."""
    id: int | None = None
    processed_dataset_id: int
    status: QAStatus = QAStatus.PENDING
    notes: str | None = None
    assessed_at: datetime | None = None
