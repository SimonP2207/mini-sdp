"""Tests for the SQLite State Store: model <-> row round-trips and queries."""
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

import pytest

from mini_sdp.data.models import (
    DatasetStatus,
    ObservingBlock,
    ProcessedDataset,
    ProcessingBlock,
    QAAssessment,
    QAStatus,
    RawDataset,
    Status,
)
from mini_sdp.data.store import StateStore


@pytest.fixture
def store(tmp_path) -> StateStore:
    s = StateStore(tmp_path / "state.db")
    s.init_schema()
    yield s
    s.close()


def test_insert_assigns_id_and_roundtrips(store):
    block = store.insert_observing_block(ObservingBlock())
    assert block.id is not None
    assert block.status is Status.NOT_STARTED

    fetched = store.get_observing_block(block.id)
    assert fetched == block


def test_raw_dataset_roundtrip_preserves_types(store):
    ds = store.insert_raw_dataset(
        RawDataset(path=Path("data/obs-1/out.ms"), size=1234, status=DatasetStatus.STORED)
    )
    fetched = store.get_raw_dataset(ds.id)
    assert isinstance(fetched.path, Path)
    assert fetched.path == Path("data/obs-1/out.ms")
    assert fetched.size == 1234
    assert fetched.status is DatasetStatus.STORED


def test_datetime_roundtrip(store):
    now = datetime.now(UTC)
    block = store.insert_observing_block(ObservingBlock(start_time=now))
    fetched = store.get_observing_block(block.id)
    assert fetched.start_time == now


def test_claim_next_is_atomic_and_serial(store):
    a = store.insert_observing_block(ObservingBlock())
    b = store.insert_observing_block(ObservingBlock())

    first = store.claim_next_observing_block()
    assert first.id == a.id                      # oldest first
    assert first.status is Status.IN_PROGRESS
    assert first.start_time is not None

    second = store.claim_next_observing_block()
    assert second.id == b.id

    # queue now empty -> None, and previously claimed blocks are not re-claimed
    assert store.claim_next_observing_block() is None


def test_count_not_started(store):
    assert store.count_not_started_observing_blocks() == 0
    store.insert_observing_block(ObservingBlock())
    store.insert_observing_block(ObservingBlock())
    assert store.count_not_started_observing_blocks() == 2
    store.claim_next_observing_block()
    assert store.count_not_started_observing_blocks() == 1


def test_total_stored_raw_bytes_counts_allocated_and_stored(store):
    store.insert_raw_dataset(RawDataset(path=Path("a"), size=100, status=DatasetStatus.ALLOCATED))
    store.insert_raw_dataset(RawDataset(path=Path("b"), size=200, status=DatasetStatus.STORED))
    released = store.insert_raw_dataset(
        RawDataset(path=Path("c"), size=999, status=DatasetStatus.STORED)
    )
    assert store.total_stored_raw_bytes() == 1299  # 100 + 200 + 999

    store.set_raw_dataset_status(released.id, DatasetStatus.DELETED)
    assert store.total_stored_raw_bytes() == 300  # deleted no longer counts (100+200)


def test_list_actionable_qa_filters_on_processed_status(store):
    stored_pd = store.insert_processed_dataset(
        ProcessedDataset(path=Path("p1"), size=10, status=DatasetStatus.STORED)
    )
    archived_pd = store.insert_processed_dataset(
        ProcessedDataset(path=Path("p2"), size=10, status=DatasetStatus.ARCHIVED)
    )
    qa_actionable = store.insert_qa_assessment(
        QAAssessment(processed_dataset_id=stored_pd.id)
    )
    qa_done = store.insert_qa_assessment(
        QAAssessment(processed_dataset_id=archived_pd.id)
    )
    store.insert_qa_assessment(
        QAAssessment(processed_dataset_id=stored_pd.id)
    )

    store.set_qa_status(qa_actionable.id, QAStatus.APPROVED)
    store.set_qa_status(qa_done.id, QAStatus.APPROVED)   # processed already ARCHIVED
    # qa_pending stays PENDING

    actionable = store.list_actionable_qa()
    ids = {q.id for q in actionable}
    assert ids == {qa_actionable.id}


def test_concurrent_access_is_serialized(store):
    """The single connection is shared across threads (as under the FastAPI QA app);
    concurrent reads and writes must not raise sqlite3.InterfaceError (SQLITE_MISUSE)."""
    for _ in range(20):
        store.insert_observing_block(ObservingBlock())

    def worker(i: int):
        # mix reads and a write, as concurrent HTTP requests would
        store.count_not_started_observing_blocks()
        store.get_observing_block((i % 20) + 1)
        store.list_observing_blocks_by_status(Status.NOT_STARTED)
        store.insert_raw_dataset(RawDataset(path=Path(f"r{i}"), size=1))

    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = [pool.submit(worker, i) for i in range(200)]
        for f in futures:
            f.result()  # re-raises any exception from the worker thread


def test_processing_block_lookup_by_processed_dataset(store):
    obs = store.insert_observing_block(ObservingBlock())
    pd = store.insert_processed_dataset(ProcessedDataset(path=Path("p"), size=5))
    pb = store.insert_processing_block(
        ProcessingBlock(observing_block_id=obs.id, processed_dataset_id=pd.id)
    )
    found = store.get_processing_block_by_processed_dataset(pd.id)
    assert found.id == pb.id
    assert found.observing_block_id == obs.id
