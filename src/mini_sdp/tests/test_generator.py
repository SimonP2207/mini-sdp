"""Tests for the observation generator's backpressure."""
import pytest

from mini_sdp.data.store import StateStore
from mini_sdp.generator import top_up


@pytest.fixture
def store(tmp_path) -> StateStore:
    s = StateStore(tmp_path / "state.db")
    s.init_schema()
    yield s
    s.close()


def test_top_up_inserts_only_when_queue_empty(store):
    assert top_up(store) is True                          # empty -> insert one
    assert store.count_not_started_observing_blocks() == 1

    assert top_up(store) is False                         # one queued -> no-op
    assert store.count_not_started_observing_blocks() == 1


def test_top_up_refills_after_claim(store):
    top_up(store)
    store.claim_next_observing_block()                    # queue drained
    assert store.count_not_started_observing_blocks() == 0

    assert top_up(store) is True                          # refills the single slot
    assert store.count_not_started_observing_blocks() == 1
