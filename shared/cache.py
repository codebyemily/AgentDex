import time
from typing import Optional

_store: dict[str, dict] = {}


def get_warm(topic: str) -> Optional[dict]:
    return _store.get(topic.lower().strip())


def set_warm(topic: str, data: dict) -> None:
    _store[topic.lower().strip()] = {**data, "cached_at": time.time()}


def all_topics() -> list[str]:
    return list(_store.keys())
