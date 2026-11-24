"""Tiny tenant-scoped TTL cache for report responses."""

from __future__ import annotations

import time
from typing import Any

TTL_SECONDS = 30.0
_cache: dict[str, tuple[float, Any]] = {}


def get(key: str) -> Any | None:
    entry = _cache.get(key)
    if entry is None:
        return None
    expires, value = entry
    if expires <= time.monotonic():
        _cache.pop(key, None)
        return None
    return value


def put(key: str, value: Any) -> None:
    _cache[key] = (time.monotonic() + TTL_SECONDS, value)


def invalidate_tenant(tenant_id: str) -> None:
    prefix = f"{tenant_id}:"
    for key in [key for key in _cache if key.startswith(prefix)]:
        _cache.pop(key, None)


def clear() -> None:
    _cache.clear()
