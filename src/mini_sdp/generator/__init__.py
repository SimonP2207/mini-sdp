"""Observation generator making sure only one observation is conducted at a time
"""
import argparse
import time
from pathlib import Path

from ..data.models import ObservingBlock
from ..data.store import StateStore


def top_up(store: StateStore) -> bool:
    """Insert one NOT_STARTED block if none is queued. Returns True if one was added."""
    if store.count_not_started_observing_blocks() == 0:
        store.insert_observing_block(ObservingBlock())
        return True
    return False


def run(store: StateStore, interval: float) -> None:
    while True:
        if top_up(store):
            print("Queued a new observation request")
        time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="Mini-SDP observation generator")
    parser.add_argument("--db", type=Path, default=Path("sdp.db"),
                        help="Path to the shared State Store SQLite database")
    parser.add_argument("--interval", type=float, default=2.0,
                        help="Seconds between queue checks")
    args = parser.parse_args()

    store = StateStore(args.db)
    store.init_schema()
    try:
        run(store, args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        store.close()
