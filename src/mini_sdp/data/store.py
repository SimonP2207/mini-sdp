"""SQLite-backed State Store for the Data Controller.

The State Store is the single source of truth shared by the three processes
(orchestrator, observation generator, QA web app). It maps the Pydantic models in
``models.py`` onto SQLite rows: ``model_dump(mode="json")`` on the way in (enums become
their string value, ``datetime`` becomes ISO text, ``Path`` becomes text) and
``Model(**row)`` on the way out (Pydantic parses the strings back).

WAL journal mode lets the separate processes read/write the one database file
concurrently (many readers, one writer).
"""
import sqlite3
import threading
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import TypeVar

from pydantic import BaseModel

from .models import (
    DatasetStatus,
    ObservingBlock,
    ProcessedDataset,
    ProcessingBlock,
    QAAssessment,
    QAStatus,
    RawDataset,
    Status,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS observing_blocks (
    id             INTEGER PRIMARY KEY,
    status         TEXT NOT NULL,
    raw_dataset_id INTEGER,
    start_time     TEXT,
    end_time       TEXT,
    error          TEXT
);
CREATE TABLE IF NOT EXISTS processing_blocks (
    id                  INTEGER PRIMARY KEY,
    observing_block_id  INTEGER NOT NULL,
    status              TEXT NOT NULL,
    processed_dataset_id INTEGER,
    start_time          TEXT,
    end_time            TEXT,
    error               TEXT
);
CREATE TABLE IF NOT EXISTS raw_datasets (
    id     INTEGER PRIMARY KEY,
    path   TEXT NOT NULL,
    size   INTEGER NOT NULL,
    status TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS processed_datasets (
    id     INTEGER PRIMARY KEY,
    path   TEXT NOT NULL,
    size   INTEGER NOT NULL,
    status TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS qa_assessments (
    id                   INTEGER PRIMARY KEY,
    processed_dataset_id INTEGER NOT NULL,
    status               TEXT NOT NULL,
    notes                TEXT,
    assessed_at          TEXT
);
"""

# Column order per table (excluding the auto-assigned id), used for inserts.
_COLUMNS = {
    "observing_blocks": ["status", "raw_dataset_id", "start_time", "end_time", "error"],
    "processing_blocks": [
        "observing_block_id", "status", "processed_dataset_id",
        "start_time", "end_time", "error",
    ],
    "raw_datasets": ["path", "size", "status"],
    "processed_datasets": ["path", "size", "status"],
    "qa_assessments": ["processed_dataset_id", "status", "notes", "assessed_at"],
}


ControllerModel = TypeVar("ControllerModel", bound=BaseModel)


def _now() -> str:
    """Current UTC time"""
    return datetime.now(UTC).isoformat()


class StateStore:
    """
    Governs DB modifications/access and facilitates conversion (in both 
    directions) between SQL and pydantic models
    """

    def __init__(self, path: Path | str):
        """
        Parameters
        ----------
        path : Path | str
           Full path to SQLite DB
        """
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._lock = threading.Lock()

    def init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # ################ GENERIC INSERT, UPDATE, GET METHODS ################### #
    def _fetchone(self, sql: str, params: tuple = ()) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(sql, params).fetchone()

    def _fetchall(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def _insert(self, table: str, model: ControllerModel) -> ControllerModel:
        cols = _COLUMNS[table]
        data = model.model_dump(mode="json")
        placeholders = ", ".join("?" for _ in cols)
        with self._lock:
            cur = self._conn.execute(
                f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})",
                [data[c] for c in cols],
            )
            self._conn.commit()
            new_id = cur.lastrowid
        return model.model_copy(update={"id": new_id})

    def _get(
        self, table: str, model_cls: type[ControllerModel], id: int
    ) -> ControllerModel | None:
        row = self._fetchone(f"SELECT * FROM {table} WHERE id = ?", (id,))
        return model_cls(**dict(row)) if row is not None else None

    def _set_status(self, table: str, id: int, status: StrEnum) -> None:
        with self._lock:
            self._conn.execute(
                f"UPDATE {table} SET status = ? WHERE id = ?",
                (str(status), id),
            )
            self._conn.commit()

    # ################## OBSERVING BLOCKS-RELATED METHODS #################### #
    def insert_observing_block(self, block: ObservingBlock) -> ObservingBlock:
        return self._insert("observing_blocks", block)

    def get_observing_block(self, id: int) -> ObservingBlock | None:
        return self._get("observing_blocks", ObservingBlock, id)

    def update_observing_block(self, block: ObservingBlock) -> None:
        data = block.model_dump(mode="json")
        with self._lock:
            self._conn.execute(
                "UPDATE observing_blocks SET status=?, raw_dataset_id=?, "
                "start_time=?, end_time=?, error=? WHERE id=?",
                (data["status"], data["raw_dataset_id"], data["start_time"],
                 data["end_time"], data["error"], data["id"]),
            )
            self._conn.commit()

    def list_observing_blocks_by_status(self, status: Status) -> list[ObservingBlock]:
        rows = self._fetchall(
            "SELECT * FROM observing_blocks WHERE status = ? ORDER BY id",
            (str(status),),
        )
        return [ObservingBlock(**dict(r)) for r in rows]

    def count_not_started_observing_blocks(self) -> int:
        row = self._fetchone(
            "SELECT COUNT(*) AS n FROM observing_blocks WHERE status = ?",
            (str(Status.NOT_STARTED),),
        )
        return int(row["n"])

    def claim_next_observing_block(self) -> ObservingBlock | None:
        """Claim the oldest NOT_STARTED block, by flipping it to IN_PROGRESS"""
        with self._lock:
            row = self._conn.execute(
                "SELECT id FROM observing_blocks WHERE status = ? ORDER BY id LIMIT 1",
                (str(Status.NOT_STARTED),),
            ).fetchone()
            if row is None:
                return None
            block_id = row["id"]
            cur = self._conn.execute(
                "UPDATE observing_blocks SET status = ?, start_time = ? "
                "WHERE id = ? AND status = ?",
                (str(Status.IN_PROGRESS), _now(), block_id, str(Status.NOT_STARTED)),
            )
            self._conn.commit()
            if cur.rowcount == 0:
                return None
            claimed = self._conn.execute(
                "SELECT * FROM observing_blocks WHERE id = ?", (block_id,)
            ).fetchone()
        return ObservingBlock(**dict(claimed))

    # ################# PROCESSING BLOCKS-RELATED METHODS #################### #
    def insert_processing_block(self, block: ProcessingBlock) -> ProcessingBlock:
        return self._insert("processing_blocks", block)

    def get_processing_block(self, id: int) -> ProcessingBlock | None:
        return self._get("processing_blocks", ProcessingBlock, id)

    def list_processing_blocks_by_status(self, status: Status) -> list[ProcessingBlock]:
        rows = self._fetchall(
            "SELECT * FROM processing_blocks WHERE status = ? ORDER BY id",
            (str(status),),
        )
        return [ProcessingBlock(**dict(r)) for r in rows]

    def list_processing_blocks_for_observing_block(
        self, observing_block_id: int
    ) -> list[ProcessingBlock]:
        rows = self._fetchall(
            "SELECT * FROM processing_blocks WHERE observing_block_id = ? ORDER BY id",
            (observing_block_id,),
        )
        return [ProcessingBlock(**dict(r)) for r in rows]

    def get_processing_block_by_processed_dataset(
        self, processed_dataset_id: int
    ) -> ProcessingBlock | None:
        row = self._fetchone(
            "SELECT * FROM processing_blocks WHERE processed_dataset_id = ? "
            "ORDER BY id DESC LIMIT 1",
            (processed_dataset_id,),
        )
        return ProcessingBlock(**dict(row)) if row is not None else None

    def update_processing_block(self, block: ProcessingBlock) -> None:
        data = block.model_dump(mode="json")
        with self._lock:
            self._conn.execute(
                "UPDATE processing_blocks SET status=?, processed_dataset_id=?, "
                "start_time=?, end_time=?, error=? WHERE id=?",
                (data["status"], data["processed_dataset_id"], data["start_time"],
                 data["end_time"], data["error"], data["id"]),
            )
            self._conn.commit()

    # ###################### RAW DATASETS-RELATED METHODS #################### #
    def insert_raw_dataset(self, dataset: RawDataset) -> RawDataset:
        return self._insert("raw_datasets", dataset)

    def get_raw_dataset(self, id: int) -> RawDataset | None:
        return self._get("raw_datasets", RawDataset, id)

    def set_raw_dataset_status(self, id: int, status: DatasetStatus) -> None:
        self._set_status("raw_datasets", id, status)

    def set_raw_dataset_size(self, id: int, size: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE raw_datasets SET size = ? WHERE id = ?", (size, id)
            )
            self._conn.commit()

    def total_stored_raw_bytes(self) -> int:
        """Bytes of raw visibilities occupying (or reserved on) disk.

        Counts ALLOCATED (being written) and STORED datasets; ARCHIVED/DELETED
        datasets have been released and no longer count against the threshold.
        """
        row = self._fetchone(
            "SELECT COALESCE(SUM(size), 0) AS total FROM raw_datasets "
            "WHERE status IN (?, ?)",
            (str(DatasetStatus.ALLOCATED), str(DatasetStatus.STORED)),
        )
        return int(row["total"])

    # ################# PROCESSED DATASETS-RELATED METHODS ################### #
    def insert_processed_dataset(self, dataset: ProcessedDataset) -> ProcessedDataset:
        return self._insert("processed_datasets", dataset)

    def get_processed_dataset(self, id: int) -> ProcessedDataset | None:
        return self._get("processed_datasets", ProcessedDataset, id)

    def set_processed_dataset_status(self, id: int, status: DatasetStatus) -> None:
        self._set_status("processed_datasets", id, status)

    def set_processed_dataset_size(self, id: int, size: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE processed_datasets SET size = ? WHERE id = ?", (size, id)
            )
            self._conn.commit()

    # ################### QA ASSESSMENT-RELATED METHODS ###################### #
    def insert_qa_assessment(self, assessment: QAAssessment) -> QAAssessment:
        return self._insert("qa_assessments", assessment)

    def get_qa_assessment(self, id: int) -> QAAssessment | None:
        return self._get("qa_assessments", QAAssessment, id)

    def set_qa_status(
        self, id: int, status: QAStatus, notes: str | None = None
    ) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE qa_assessments SET status = ?, notes = ?, assessed_at = ? "
                "WHERE id = ?",
                (str(status), notes, _now(), id),
            )
            self._conn.commit()

    def list_pending_qa(self) -> list[QAAssessment]:
        """Get QA tasks that still need to be assessed i.e. PENDING"""
        rows = self._fetchall(
            "SELECT * FROM qa_assessments WHERE status = ? ORDER BY id",
            (str(QAStatus.PENDING),),
        )
        return [QAAssessment(**dict(r)) for r in rows]

    def list_actionable_qa(self) -> list[QAAssessment]:
        """
        QA outcomes the orchestrator has not acted on yet i.e. APPROVED/REJECTED
        and its processed dataset is still STORED.
        """
        rows = self._fetchall(
            "SELECT q.* FROM qa_assessments q "
            "JOIN processed_datasets p ON p.id = q.processed_dataset_id "
            "WHERE q.status IN (?, ?) AND p.status = ? ORDER BY q.id",
            (str(QAStatus.APPROVED), str(QAStatus.REJECTED), str(DatasetStatus.STORED)),
        )
        return [QAAssessment(**dict(r)) for r in rows]
