from __future__ import annotations

import threading

from app.services.gtfs_loader import GTFSStore


class GTFSStoreProvider:
    def __init__(self, initial: GTFSStore):
        self._store = initial
        self._lock = threading.Lock()

    def get(self) -> GTFSStore:
        with self._lock:
            return self._store

    def replace(self, candidate: GTFSStore) -> GTFSStore:
        if not candidate.loaded:
            raise ValueError(candidate.error or "Cannot activate an unloaded GTFSStore")
        with self._lock:
            previous = self._store
            self._store = candidate
            return previous


class GTFSStoreProxy:
    """Compatibility view that resolves every access against the active store."""

    def __init__(self, provider: GTFSStoreProvider):
        object.__setattr__(self, "_provider", provider)

    def __getattr__(self, name: str):
        return getattr(self._provider.get(), name)
