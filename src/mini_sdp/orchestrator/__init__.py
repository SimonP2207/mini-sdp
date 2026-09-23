"""DataOrchestrator: the control logic that ties the other components together.
(see docs/images/components.svg for an overview)
"""
import asyncio
import logging
from datetime import UTC, datetime
from pathlib import Path

from ..archiver import DataArchiver
from ..data.models import (
    DatasetStatus,
    ObservingBlock,
    ProcessedDataset,
    ProcessingBlock,
    QAAssessment,
    QAStatus,
    RawDataset,
    Status,
)
from ..data.store import StateStore
from ..observer import estimate_obs_dataset_size_bytes
from ..processor import estimate_processed_dataset_size_bytes
from ..storage import StorageManager, _delete_path

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(UTC)


def _dir_size(path: Path) -> int:
    """Total size in bytes of every file under ``path`` (a file or a directory)."""
    path = Path(path)
    if path.is_file():
        return path.stat().st_size
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


class DataOrchestrator:
    """Coordinate the observe, process and quality-assessment pipeline. Runs 3
    three cooperating pieces on a single asyncio event loop:

    * an **observing loop** that is strictly serial (one observation at a time), gated by the
    Storage Manager's threshold, consuming NOT_STARTED blocks from the State Store queue;
    * a **bounded processing pool** (``asyncio.Semaphore``) so several processing runs proceed
    in parallel but never more than ``max_parallel`` at once;
    * a **QA reconcile loop** that polls the State Store for approved/rejected products and
    archives+releases or re-processes accordingly.

    The observing/processing/file-system work (Docker via the Observer/Processor,
    and large filesystem deletes/copies) is delegated to worker threads with 
    ``asyncio.to_thread`` so it never stalls the event loop.
    """

    def __init__(
        self,
        state_store: StateStore,
        storage_manager: StorageManager,
        observer,
        processor,
        archiver: DataArchiver,
        *,
        max_parallel: int = 2,
        poll_interval: float = 1.0,
    ):
        """Wire the orchestrator to its collaborators.

        Parameters
        ----------
        state_store : StateStore
            Shared source of truth for observing and processing blocks, datasets
            and QA assessments
        storage_manager : StorageManager
            Admits observations against the storage threshold and releases raw
            visibilities once their products are approved
        observer : DataObserver
            Conducts an observation block, producing RawDatasets
        processor : DataProcessor
            Processes raw visibilities into ProcessedDatasets by running the
            visibility-processing container
        archiver : DataArchiver
            Copies QA-approved data products to the product archive.
        max_parallel : int, optional
            Maximum number of processing runs allowed to execute concurrently,
            bounding the processing pool via an ``asyncio.Semaphore``
            (default ``2``)
        poll_interval : float, optional
            Seconds to wait between DB polls when the observing loop is blocked
            (storage full or queue empty) and between QA reconcile passes
            (default ``1.0``)
        """
        self._store = state_store
        self._storage = storage_manager
        self._observer = observer
        self._processor = processor
        self._archiver = archiver
        self._poll = poll_interval
        self._sem = asyncio.Semaphore(max_parallel)
        self._processing_tasks: set[asyncio.Task] = set()
        self._stopping = asyncio.Event()

    # ######################### MAIN EVENT LOOP (AND STOP) ################### #
    async def run(self) -> None:
        """Run the observing and QA-reconcile loops until :meth:`stop` is called."""
        await self._recover()
        try:
            await asyncio.gather(self._observing_loop(), self._qa_reconcile_loop())
        finally:
            await self._drain_processing()

    def stop(self) -> None:
        logger.warning("Stopping DataOrchestrator gracefully")
        self._stopping.set()

    # ######################### RECOVERY E.G. AFTER CRASH  ################### #
    async def _recover(self) -> None:
        """
        Reconcile IN_PROGRESS work left behind by e.g. a crash, before the
        loops start. Interrupted observations are re-queued, interrupted
        processing runs are failed and their partial products discarded, and any
        finished observation left without a live product is re-dispatched for 
        processing.
        """
        await self._recover_interrupted_observations()
        await self._recover_interrupted_processing()
        self._redispatch_unprocessed_observations()

    async def _recover_interrupted_observations(self) -> None:
        for block in self._store.list_observing_blocks_by_status(Status.IN_PROGRESS):
            if block.raw_dataset_id is not None:
                raw = self._store.get_raw_dataset(block.raw_dataset_id)
                if raw is not None and raw.status is not DatasetStatus.DELETED:
                    await asyncio.to_thread(self._storage.delete_raw_dataset, raw)
            block.status = Status.NOT_STARTED
            block.raw_dataset_id = None
            block.start_time = None
            block.end_time = None
            block.error = None
            self._store.update_observing_block(block)
            logger.info("Recovered interrupted observation %s; re-queued", block.id)

    async def _recover_interrupted_processing(self) -> None:
        for pb in self._store.list_processing_blocks_by_status(Status.IN_PROGRESS):
            if pb.processed_dataset_id is not None:
                pd = self._store.get_processed_dataset(pb.processed_dataset_id)
                if pd is not None and pd.status in (
                    DatasetStatus.ALLOCATED, DatasetStatus.STORED
                ):
                    await asyncio.to_thread(_delete_path, Path(pd.path))
                    self._store.set_processed_dataset_status(pd.id, DatasetStatus.DELETED)
            pb.status = Status.FAILED
            pb.error = "interrupted by restart"
            pb.end_time = _now()
            self._store.update_processing_block(pb)
            logger.info("Recovered interrupted processing %s; will re-process", pb.id)

    def _redispatch_unprocessed_observations(self) -> None:
        """Re-process FINISHED observations whose raw is still STORED but has no
        processing product.
        """
        for block in self._store.list_observing_blocks_by_status(Status.FINISHED):
            if block.raw_dataset_id is None:
                continue
            raw = self._store.get_raw_dataset(block.raw_dataset_id)
            if raw is None or raw.status is not DatasetStatus.STORED:
                continue
            if self._has_completed_product(block.id):
                continue
            logger.info("Re-dispatching processing for observation %s after restart",
                        block.id)
            self._dispatch_processing(block, raw)

    def _has_completed_product(self, observing_block_id: int) -> bool:
        """True if the observation already has a processed dataset that is STORED/ARCHIVED."""
        for pb in self._store.list_processing_blocks_for_observing_block(observing_block_id):
            if pb.processed_dataset_id is None:
                continue
            pd = self._store.get_processed_dataset(pb.processed_dataset_id)
            if pd is not None and pd.status in (
                DatasetStatus.STORED, DatasetStatus.ARCHIVED
            ):
                return True
        return False

    # ########################### OBSERVATIONS ############################### #
    async def _observing_loop(self) -> None:
        while not self._stopping.is_set():
            observed = await self._observe_once()
            if not observed:
                await asyncio.sleep(self._poll)

    async def _observe_once(self) -> bool:
        """Attempt one observation. Returns True if one ran."""
        estimated = estimate_obs_dataset_size_bytes()
        if not self._storage.can_allocate(estimated):
            logger.warning("Could not allocate space for new observation")
            return False

        block = self._store.claim_next_observing_block()
        if block is None:
            return False

        obs_dir = self._storage.create_observation_dir(block.id)
        raw = self._store.insert_raw_dataset(
            RawDataset(path=obs_dir / "out.ms", size=estimated,
                       status=DatasetStatus.ALLOCATED)
        )
        block.raw_dataset_id = raw.id
        self._store.update_observing_block(block)

        try:
            await asyncio.to_thread(self._observer.observe, block, raw)
        except Exception as exc:
            logger.exception("Observation %s failed", block.id)
            block.status = Status.FAILED
            block.error = str(exc)
            block.end_time = _now()
            self._store.update_observing_block(block)
            self._store.set_raw_dataset_status(raw.id, DatasetStatus.DELETED)
            return True

        raw.size = _dir_size(raw.path)
        self._store.set_raw_dataset_size(raw.id, raw.size)
        self._store.set_raw_dataset_status(raw.id, DatasetStatus.STORED)
        block.status = Status.FINISHED
        block.end_time = _now()
        self._store.update_observing_block(block)

        self._dispatch_processing(block, raw)
        return True

    # ########################## DATASET PROCESSING ########################## #
    def _dispatch_processing(
        self, observing_block: ObservingBlock, raw: RawDataset
    ) -> asyncio.Task:
        task = asyncio.create_task(self._run_processing(observing_block, raw))
        self._processing_tasks.add(task)
        task.add_done_callback(self._processing_tasks.discard)
        return task

    async def _run_processing(
        self, observing_block: ObservingBlock, raw: RawDataset
    ) -> None:
        async with self._sem:
            pb = self._store.insert_processing_block(
                ProcessingBlock(
                    observing_block_id=observing_block.id,
                    status=Status.IN_PROGRESS,
                    start_time=_now(),
                )
            )
            processed_dir = Path(raw.path).parent / f"run-{pb.id:04d}"
            processed_dir.mkdir(parents=True, exist_ok=True)
            prefix = Path(raw.path).stem
            pd = self._store.insert_processed_dataset(
                ProcessedDataset(
                    path=processed_dir,
                    size=estimate_processed_dataset_size_bytes(),
                    status=DatasetStatus.ALLOCATED,
                )
            )
            pb.processed_dataset_id = pd.id
            self._store.update_processing_block(pb)

            try:
                await asyncio.to_thread(self._processor.process, pb, raw, pd, prefix)
            except Exception as exc:
                logger.exception("Processing %s failed", pb.id)
                pb.status = Status.FAILED
                pb.error = str(exc)
                pb.end_time = _now()
                self._store.update_processing_block(pb)
                self._store.set_processed_dataset_status(pd.id, DatasetStatus.DELETED)
                return

            self._store.set_processed_dataset_size(pd.id, _dir_size(processed_dir))
            self._store.set_processed_dataset_status(pd.id, DatasetStatus.STORED)
            pb.status = Status.FINISHED
            pb.end_time = _now()
            self._store.update_processing_block(pb)

            self._store.insert_qa_assessment(
                QAAssessment(processed_dataset_id=pd.id, status=QAStatus.PENDING)
            )

    # ######################### QUALITY ASSESSMENT ########################### #
    async def _qa_reconcile_loop(self) -> None:
        while not self._stopping.is_set():
            await self._reconcile_once()
            await asyncio.sleep(self._poll)

    async def _reconcile_once(self) -> None:
        """Deal with datasets that have been quality assessed"""
        for qa in self._store.list_actionable_qa():
            pd = self._store.get_processed_dataset(qa.processed_dataset_id)
            if pd is None or pd.status is not DatasetStatus.STORED:
                continue
            if qa.status is QAStatus.APPROVED:
                await self._apply_approval(pd)
            elif qa.status is QAStatus.REJECTED:
                await self._apply_rejection(pd)

    async def _apply_approval(self, pd: ProcessedDataset) -> None:
        """Archive the product, and release the raw dataset to free storage."""
        await asyncio.to_thread(self._archiver.archive, pd)

        raw = self._raw_for_processed(pd)
        observing_block = self._observing_for_processed(pd)

        await asyncio.to_thread(_delete_path, Path(pd.path))
        if raw is not None:
            await asyncio.to_thread(self._storage.delete_raw_dataset, raw)
        if observing_block is not None:
            await asyncio.to_thread(
                self._storage.remove_observation_dir_if_empty, observing_block.id
            )

        self._store.set_processed_dataset_status(pd.id, DatasetStatus.ARCHIVED)
        logger.info(
            f"Approved product {pd.id} archived; raw visibilities released"
        )

    async def _apply_rejection(self, pd: ProcessedDataset) -> None:
        """Discard the rejected product and re-process the same raw visibilities."""
        raw = self._raw_for_processed(pd)
        await asyncio.to_thread(_delete_path, Path(pd.path))
        self._store.set_processed_dataset_status(pd.id, DatasetStatus.DELETED)

        if raw is not None:
            observing_block = self._observing_for_processed(pd)
            self._dispatch_processing(observing_block, raw)
            logger.info("Rejected product %s discarded; re-processing raw %s",
                        pd.id, raw.id)

    # ######################### REUSED STATE LOOKUPS ######################### #
    def _observing_for_processed(self, pd: ProcessedDataset) -> ObservingBlock | None:
        pb = self._store.get_processing_block_by_processed_dataset(pd.id)
        if pb is not None:
            return self._store.get_observing_block(pb.observing_block_id)

    def _raw_for_processed(self, pd: ProcessedDataset) -> RawDataset | None:
        observing_block = self._observing_for_processed(pd)
        if observing_block and observing_block.raw_dataset_id:
            return self._store.get_raw_dataset(observing_block.raw_dataset_id)

    async def _drain_processing(self) -> None:
        """Await all in-flight processing tasks (used by tests and shutdown)."""
        while self._processing_tasks:
            await asyncio.gather(*list(self._processing_tasks))
