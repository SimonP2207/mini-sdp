"""Fake Observer/Processor that mimic the Docker workers without any Docker.

They write dummy files of the requested size so the Storage Manager, the archiver, and the
size accounting all behave as they would with real data. Injected into the orchestrator in
place of ``DataObserver`` / ``DataProcessor``.
"""
import threading
import time
from pathlib import Path

from ..data.models import ObservingBlock, ProcessedDataset, ProcessingBlock, RawDataset

# Small sizes so tests stay fast; the control logic is size-agnostic.
FAKE_RAW_SIZE = 1000
FAKE_PRODUCT_FILES = ("image.fits", "dirty.fits", "psf.fits", "residual.fits", "model.fits")


class FakeObserver:
    """Writes a dummy measurement-set directory of ``FAKE_RAW_SIZE`` bytes."""

    def __init__(self, raw_size: int = FAKE_RAW_SIZE):
        self.raw_size = raw_size
        self.calls: list[int] = []

    def observe(self, obs_block: ObservingBlock, out_dataset: RawDataset) -> None:
        ms = Path(out_dataset.path)
        ms.mkdir(parents=True, exist_ok=True)
        (ms / "table.dat").write_bytes(b"x" * self.raw_size)
        self.calls.append(obs_block.id)


class FakeProcessor:
    """Writes dummy FITS products under the processed dataset directory.

    Tracks the maximum number of concurrent ``process`` calls (``max_active``) so the
    orchestrator's bounded pool can be asserted on; an optional ``delay`` forces overlap.
    """

    def __init__(self, per_file_size: int = 100, fail: bool = False, delay: float = 0.0):
        self.per_file_size = per_file_size
        self.fail = fail
        self.delay = delay
        self.calls: list[int] = []
        self._lock = threading.Lock()
        self._active = 0
        self.max_active = 0

    def process(
        self,
        processing_block: ProcessingBlock,
        raw_dataset: RawDataset,
        processed_dataset: ProcessedDataset,
        dataset_prefix: str | None = None,
    ) -> None:
        with self._lock:
            self._active += 1
            self.max_active = max(self.max_active, self._active)
        try:
            if self.delay:
                time.sleep(self.delay)
            if self.fail:
                raise RuntimeError("fake processing failure")
            out_dir = Path(processed_dataset.path)
            out_dir.mkdir(parents=True, exist_ok=True)
            prefix = dataset_prefix or Path(raw_dataset.path).stem
            for name in FAKE_PRODUCT_FILES:
                (out_dir / f"{prefix}-{name}").write_bytes(b"y" * self.per_file_size)
            self.calls.append(processed_dataset.id)
        finally:
            with self._lock:
                self._active -= 1
