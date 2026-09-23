"""
Considers observations' feasibility against a storage threshold i.e. in support 
of "stop observing once the storage size of all observed visibilities on disk 
reaches a configured threshold"
"""
import logging
import shutil
from abc import ABC, abstractmethod
from pathlib import Path

from ..data.models import DatasetStatus, RawDataset
from ..data.store import StateStore


class Storage(ABC):
    """A storage volume with a configured capacity (the observing threshold)."""

    def __init__(self, capacity_bytes: int):
        self.capacity_bytes = capacity_bytes

    @abstractmethod
    def free_disk_bytes(self) -> int:
        """Real free space on the underlying device, in bytes."""


class LocalStorage(Storage):
    """Storage backed by a local filesystem directory."""

    def __init__(self, root: Path, capacity_bytes: int):
        super().__init__(capacity_bytes)
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def free_disk_bytes(self) -> int:
        return shutil.disk_usage(self.root).free


def _delete_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def _obs_dir_name_from_obs_id(obs_id: int) -> str:
    return f"obs-{obs_id:04d}"


class StorageManager:
    """Admits allocations against the threshold + disk margin, and releases them."""

    def __init__(
        self,
        storage: Storage,
        state_store: StateStore,
        disk_margin_bytes: int = 0,
        logger: logging.Logger | None = None,
    ):
        self._storage = storage
        self._store = state_store
        self._disk_margin = disk_margin_bytes
        self._logger = logger if logger else logging.getLogger()

    def can_allocate(self, n_bytes: int) -> bool:
        """Returns True if ``n_bytes`` can be written to disk"""
        space_is_remaining = self.used_bytes() + n_bytes <= self._storage.capacity_bytes
        outside_disk_margin = self._storage.free_disk_bytes() - n_bytes >= self._disk_margin
        return space_is_remaining and outside_disk_margin

    def create_observation_dir(self, obs_id: int) -> Path:
        obs_dir = self._storage.root / _obs_dir_name_from_obs_id(obs_id)
        obs_dir.mkdir(parents=True, exist_ok=True)
        self._logger.debug(f"Created directory: {obs_dir.resolve()!s}")
        return obs_dir

    def remove_observation_dir_if_empty(self, obs_id: int):
        obs_dir = self._storage.root / _obs_dir_name_from_obs_id(obs_id)
        try:
            obs_dir.rmdir()
            self._logger.debug(
                f"Removed directory: {obs_dir.resolve()!s}"
            )
        except OSError as exc:
            self._logger.warning(
                f"Could not remove directory : {obs_dir.resolve()!s}; {exc}"
            )

    def delete_raw_dataset(self, dataset: RawDataset) -> None:
        """Delete raw visibilities from disk and mark the dataset DELETED"""
        _delete_path(Path(dataset.path))
        if dataset.id is not None:
            self._store.set_raw_dataset_status(dataset.id, DatasetStatus.DELETED)

    def used_bytes(self) -> int:
        return self._store.total_stored_raw_bytes()

    def free_headroom(self) -> int:
        """Bytes remaining under the threshold"""
        return self._storage.capacity_bytes - self.used_bytes()
