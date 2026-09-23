"""End-to-end tests of the control logic, driven on the fake backend (no Docker)."""
from pathlib import Path

from mini_sdp.archiver import DataArchiver, LocalArchive
from mini_sdp.data.models import (
    DatasetStatus,
    ObservingBlock,
    ProcessedDataset,
    ProcessingBlock,
    QAStatus,
    RawDataset,
    Status,
)
from mini_sdp.data.store import StateStore
from mini_sdp.orchestrator import DataOrchestrator
from mini_sdp.storage import LocalStorage, StorageManager

from .fakes import FakeObserver, FakeProcessor

RAW_SIZE = 1000
PRODUCT_SIZE = 500  # 5 files * 100 bytes


def build(tmp_path, monkeypatch, *, capacity, max_parallel=2, processor=None):
    # The orchestrator uses the module-level size estimators; patch them so the fake
    # workers' tiny files line up with the admission accounting.
    monkeypatch.setattr(
        "mini_sdp.orchestrator.estimate_obs_dataset_size_bytes", lambda: RAW_SIZE
    )
    monkeypatch.setattr(
        "mini_sdp.orchestrator.estimate_processed_dataset_size_bytes", lambda: PRODUCT_SIZE
    )
    store = StateStore(tmp_path / "state.db")
    store.init_schema()
    storage = LocalStorage(tmp_path / "data", capacity_bytes=capacity)
    manager = StorageManager(storage, store)
    archive = LocalArchive(tmp_path / "archive")
    observer = FakeObserver(raw_size=RAW_SIZE)
    processor = processor or FakeProcessor()
    orch = DataOrchestrator(
        store, manager, observer, processor, DataArchiver(archive),
        max_parallel=max_parallel,
    )
    return orch, store, manager, observer, processor, tmp_path / "archive"


async def test_observing_stops_at_threshold(tmp_path, monkeypatch):
    orch, store, manager, observer, _, _ = build(tmp_path, monkeypatch, capacity=3 * RAW_SIZE)
    for _ in range(5):
        store.insert_observing_block(ObservingBlock())

    ran = [await orch._observe_once() for _ in range(5)]

    assert ran == [True, True, True, False, False]   # threshold stops the 4th
    assert observer.calls == [1, 2, 3]
    assert manager.used_bytes() == 3 * RAW_SIZE
    await orch._drain_processing()


async def test_processing_pool_is_bounded(tmp_path, monkeypatch):
    processor = FakeProcessor(delay=0.05)
    orch, store, *_ = build(
        tmp_path, monkeypatch, capacity=10 * RAW_SIZE, max_parallel=2, processor=processor
    )
    for _ in range(5):
        store.insert_observing_block(ObservingBlock())

    for _ in range(5):
        await orch._observe_once()
    await orch._drain_processing()

    assert len(processor.calls) == 5
    assert processor.max_active <= 2       # never more than max_parallel at once
    assert processor.max_active == 2       # and it did actually run in parallel


async def test_qa_approve_archives_releases_and_resumes(tmp_path, monkeypatch):
    # capacity for exactly one observation, so we can prove approval frees space
    orch, store, manager, _, _, archive_root = build(tmp_path, monkeypatch, capacity=RAW_SIZE)
    store.insert_observing_block(ObservingBlock())
    store.insert_observing_block(ObservingBlock())

    assert await orch._observe_once() is True
    await orch._drain_processing()
    assert await orch._observe_once() is False       # full: second obs blocked

    qa = store.list_pending_qa()[0]
    pd = store.get_processed_dataset(qa.processed_dataset_id)
    raw = orch._raw_for_processed(pd)
    obs_dir = Path(raw.path).parent
    assert obs_dir.exists()
    store.set_qa_status(qa.id, QAStatus.APPROVED)

    await orch._reconcile_once()

    # product copied into the archive
    archived = list(Path(archive_root).rglob("*.fits"))
    assert len(archived) == 5
    # raw released from disk + storage freed
    assert not Path(raw.path).exists()
    assert store.get_raw_dataset(raw.id).status is DatasetStatus.DELETED
    assert store.get_processed_dataset(pd.id).status is DatasetStatus.ARCHIVED
    assert manager.used_bytes() == 0
    assert not obs_dir.exists()          # empty observation directory cleaned up

    # observing can now resume
    assert await orch._observe_once() is True
    await orch._drain_processing()


async def test_qa_reprocess_discards_and_reruns_on_same_raw(tmp_path, monkeypatch):
    orch, store, *_ , = build(tmp_path, monkeypatch, capacity=10 * RAW_SIZE)
    store.insert_observing_block(ObservingBlock())

    assert await orch._observe_once() is True
    await orch._drain_processing()

    qa = store.list_pending_qa()[0]
    old_pd = store.get_processed_dataset(qa.processed_dataset_id)
    raw = orch._raw_for_processed(old_pd)
    store.set_qa_status(qa.id, QAStatus.REJECTED)

    await orch._reconcile_once()
    await orch._drain_processing()

    # old product discarded, raw still present
    assert store.get_processed_dataset(old_pd.id).status is DatasetStatus.DELETED
    assert Path(raw.path).exists()
    assert store.get_raw_dataset(raw.id).status is DatasetStatus.STORED

    # a fresh processed dataset + pending QA now exist for the same raw
    pending = store.list_pending_qa()
    assert len(pending) == 1
    new_pd = store.get_processed_dataset(pending[0].processed_dataset_id)
    assert new_pd.id != old_pd.id
    assert new_pd.status is DatasetStatus.STORED
    assert orch._raw_for_processed(new_pd).id == raw.id


# CRASH RECOVERY BELOW!
async def test_recover_requeues_interrupted_observation(tmp_path, monkeypatch):
    orch, store, manager, *_ = build(tmp_path, monkeypatch, capacity=RAW_SIZE)
    # Simulate a crash mid-observation: block IN_PROGRESS with a partial ALLOCATED raw.
    block = store.insert_observing_block(ObservingBlock(status=Status.IN_PROGRESS))
    obs_dir = manager.create_observation_dir(block.id)
    ms = obs_dir / "out.ms"
    ms.mkdir()
    (ms / "partial").write_bytes(b"x" * RAW_SIZE)
    raw = store.insert_raw_dataset(
        RawDataset(path=ms, size=RAW_SIZE, status=DatasetStatus.ALLOCATED)
    )
    block.raw_dataset_id = raw.id
    store.update_observing_block(block)
    assert manager.used_bytes() == RAW_SIZE       # leaked allocation before recovery

    await orch._recover()

    recovered = store.get_observing_block(block.id)
    assert recovered.status is Status.NOT_STARTED
    assert recovered.raw_dataset_id is None
    assert not ms.exists()                         # partial data cleaned up
    assert store.get_raw_dataset(raw.id).status is DatasetStatus.DELETED
    assert manager.used_bytes() == 0               # threshold accounting freed


async def test_recover_reprocesses_interrupted_processing(tmp_path, monkeypatch):
    orch, store, manager, *_ = build(tmp_path, monkeypatch, capacity=10 * RAW_SIZE)
    # Observation finished, raw stored; processing was interrupted (partial product).
    block = store.insert_observing_block(ObservingBlock(status=Status.FINISHED))
    obs_dir = manager.create_observation_dir(block.id)
    ms = obs_dir / "out.ms"
    ms.mkdir()
    raw = store.insert_raw_dataset(
        RawDataset(path=ms, size=RAW_SIZE, status=DatasetStatus.STORED)
    )
    block.raw_dataset_id = raw.id
    store.update_observing_block(block)

    run_dir = obs_dir / "run-0001"
    run_dir.mkdir()
    (run_dir / "partial.fits").write_bytes(b"y" * 10)
    pd = store.insert_processed_dataset(
        ProcessedDataset(path=run_dir, size=PRODUCT_SIZE, status=DatasetStatus.ALLOCATED)
    )
    pb = store.insert_processing_block(
        ProcessingBlock(observing_block_id=block.id, processed_dataset_id=pd.id,
                        status=Status.IN_PROGRESS)
    )

    await orch._recover()
    await orch._drain_processing()

    # interrupted run failed, its partial product discarded
    assert store.get_processing_block(pb.id).status is Status.FAILED
    assert store.get_processed_dataset(pd.id).status is DatasetStatus.DELETED
    # a fresh processing run produced a stored product + pending QA on the same raw
    pending = store.list_pending_qa()
    assert len(pending) == 1
    new_pd = store.get_processed_dataset(pending[0].processed_dataset_id)
    assert new_pd.id != pd.id
    assert new_pd.status is DatasetStatus.STORED
    assert orch._raw_for_processed(new_pd).id == raw.id


async def test_recover_processes_observation_that_never_started_processing(tmp_path, monkeypatch):
    orch, store, manager, *_ = build(tmp_path, monkeypatch, capacity=10 * RAW_SIZE)
    # Crash after observing but before any processing block was recorded.
    block = store.insert_observing_block(ObservingBlock(status=Status.FINISHED))
    obs_dir = manager.create_observation_dir(block.id)
    ms = obs_dir / "out.ms"
    ms.mkdir()
    raw = store.insert_raw_dataset(
        RawDataset(path=ms, size=RAW_SIZE, status=DatasetStatus.STORED)
    )
    block.raw_dataset_id = raw.id
    store.update_observing_block(block)

    await orch._recover()
    await orch._drain_processing()

    pending = store.list_pending_qa()
    assert len(pending) == 1
    assert orch._raw_for_processed(
        store.get_processed_dataset(pending[0].processed_dataset_id)
    ).id == raw.id


async def test_recover_does_not_reprocess_live_product(tmp_path, monkeypatch):
    orch, store, manager, *_ = build(tmp_path, monkeypatch, capacity=10 * RAW_SIZE)
    # A completed observation with a stored product already awaiting QA must be left alone.
    block = store.insert_observing_block(ObservingBlock(status=Status.FINISHED))
    obs_dir = manager.create_observation_dir(block.id)
    ms = obs_dir / "out.ms"
    ms.mkdir()
    raw = store.insert_raw_dataset(
        RawDataset(path=ms, size=RAW_SIZE, status=DatasetStatus.STORED)
    )
    block.raw_dataset_id = raw.id
    store.update_observing_block(block)
    pd = store.insert_processed_dataset(
        ProcessedDataset(path=obs_dir / "run-0001", size=PRODUCT_SIZE,
                         status=DatasetStatus.STORED)
    )
    store.insert_processing_block(
        ProcessingBlock(observing_block_id=block.id, processed_dataset_id=pd.id,
                        status=Status.FINISHED)
    )

    await orch._recover()
    await orch._drain_processing()

    # no new processing block was created
    assert len(store.list_processing_blocks_for_observing_block(block.id)) == 1
