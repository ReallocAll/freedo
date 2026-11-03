"""Process-local, memory-pressure-driven LRU for HTTP results.

Follows Flarith's RSS-based admission and short read leases. No TTL or
persistence: results may disappear after eviction or a process restart.
"""
import gc
import os
import secrets
import threading
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable, Iterator

MIB = 1024 * 1024
MAX_STORED_BYTES = 128 * MIB
MAX_PROCESS_RSS = 896 * MIB


class ResultNotFoundError(KeyError):
    pass


class ResourceLimitError(ValueError):
    pass


def process_rss_bytes() -> int:
    try:
        with open("/proc/self/statm", encoding="ascii") as statm:
            pages = int(statm.read().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError) as exc:
        raise ResourceLimitError("process RSS unavailable") from exc


@dataclass(slots=True)
class CachedResult:
    data: bytes
    body_format: str
    status_code: int
    size_bytes: int
    headers: dict[str, str] | None
    active: int = 0


class ResultCache:
    def __init__(self, *, rss_reader: Callable[[], int] = process_rss_bytes,
                 rss_limit: int = MAX_PROCESS_RSS):
        self._rss_reader = rss_reader
        self._rss_limit = rss_limit
        self._entries: OrderedDict[str, CachedResult] = OrderedDict()
        self._lock = threading.RLock()

    def _evict_one(self) -> bool:
        for ref, entry in self._entries.items():
            if entry.active:
                continue
            del self._entries[ref]
            del entry
            gc.collect()
            return True
        return False

    def reserve(self, additional: int) -> None:
        with self._lock:
            while self._rss_reader() + additional > self._rss_limit:
                if not self._evict_one():
                    raise ResourceLimitError("insufficient RSS headroom")

    def put(self, data: bytes, body_format: str, status_code: int, size_bytes: int,
            headers: dict[str, str] | None) -> str:
        if len(data) > MAX_STORED_BYTES:
            raise ResourceLimitError("response exceeds cache admission limit")
        with self._lock:
            # The data is already allocated and counted in RSS.
            self.reserve(0)
            ref = secrets.token_urlsafe(18)
            self._entries[ref] = CachedResult(
                data, body_format, status_code, size_bytes, headers,
            )
            return ref

    @contextmanager
    def borrow(self, ref: str) -> Iterator[CachedResult]:
        with self._lock:
            item = self._entries.get(ref)
            if item is None:
                raise ResultNotFoundError(ref)
            item.active += 1
            self._entries.move_to_end(ref)
        try:
            yield item
        finally:
            with self._lock:
                item.active -= 1

    def release(self, ref: str) -> bool:
        with self._lock:
            item = self._entries.get(ref)
            if item is None:
                return False
            if item.active:
                raise ResourceLimitError("result is currently being read")
            del self._entries[ref]
            return True
