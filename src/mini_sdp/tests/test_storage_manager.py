"""Tests for the Storage Manager admission and release logic."""
from collections.abc import Iterator
from pathlib import Path

import pytest

from mini_sdp.data.models import DatasetStatus, RawDataset
from mini_sdp.data.store import StateStore
from mini_sdp.storage import LocalStorage, StorageManager


@pytest.fixture
def store(tmp_path) -> Iterator[StateStore]:
    s = StateStore(tmp_path / "state.db")
    s.init_schema()
    yield s
    s.close()


def make_manager(tmp_path, store, capacity, disk_margin=0):
    storage = LocalStorage(tmp_path / "data", capacity_bytes=capacity)
    return StorageManager(storage, store, disk_margin_bytes=disk_margin)


def test_admits_below_threshold(tmp_path, store):
    mgr = make_manager(tmp_path, store, capacity=1000)
    assert mgr.can_allocate(600) is True


def test_denies_at_or_over_threshold(tmp_path, store):
    mgr = make_manager(tmp_path, store, capacity=1000)
    store.insert_raw_dataset(RawDataset(path=Path("a"), size=600, status=DatasetStatus.STORED))
    assert mgr.can_allocate(400) is True     # 600 + 400 == 1000, still admissible
    assert mgr.can_allocate(401) is False    # would exceed the threshold


def test_disk_margin_guard(tmp_path, store):
    # capacity is huge, but a disk margin larger than the whole device denies everything
    storage = LocalStorage(tmp_path / "data", capacity_bytes=10**18)
    mgr = StorageManager(storage, store, disk_margin_bytes=10**18)
    assert mgr.can_allocate(1) is False


def test_remove_frees_space_and_marks_deleted(tmp_path, store):
    mgr = make_manager(tmp_path, store, capacity=1000)
    obs_dir = mgr.create_observation_dir(1)
    ms = obs_dir / "out.ms"
    ms.write_bytes(b"x" * 10)

    ds = store.insert_raw_dataset(
        RawDataset(path=ms, size=800, status=DatasetStatus.STORED)
    )
    assert mgr.used_bytes() == 800
    assert mgr.can_allocate(300) is False

    mgr.delete_raw_dataset(ds)
    assert not ms.exists()
    assert store.get_raw_dataset(ds.id).status is DatasetStatus.DELETED
    assert mgr.used_bytes() == 0
    assert mgr.can_allocate(300) is True      # space freed -> observing can resume
