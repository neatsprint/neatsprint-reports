from __future__ import annotations

import json
from datetime import UTC, date, datetime
from uuid import uuid4

from neatsprint_reports.api.router import _range
from neatsprint_reports.db.models import ReportProcessedEvent, ReportTimeFact
from neatsprint_reports.services import cache
from neatsprint_reports.services.projector import handle_event


class _FakeSession:
    def __init__(self, processed: object | None = None) -> None:
        self.processed = processed
        self.merged: list[object] = []
        self.added: list[object] = []
        self.committed = False

    async def get(self, model: object, key: object) -> object | None:
        return self.processed

    async def merge(self, instance: object) -> object:
        self.merged.append(instance)
        return instance

    def add(self, instance: object) -> None:
        self.added.append(instance)

    async def commit(self) -> None:
        self.committed = True

    async def delete(self, instance: object) -> None:
        return None


def _event(event_type: str = "time_entry.recorded", **overrides: object) -> dict[str, object]:
    tenant = uuid4()
    values: dict[str, object] = {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "tenant_id": str(tenant),
        "actor_id": str(uuid4()),
        "occurred_at": datetime(2026, 11, 24, 9, 0, tzinfo=UTC).isoformat(),
        "data": json.dumps(
            {
                "entry_id": str(uuid4()),
                "user_id": str(uuid4()),
                "project_id": str(uuid4()),
                "item_id": str(uuid4()),
                "duration_seconds": 1800,
                "source": "MANUAL",
                "started_at": "2026-11-24T08:30:00+00:00",
            }
        ),
    }
    values.update(overrides)
    return values


async def test_time_event_projects_fact_and_marks_processed() -> None:
    session = _FakeSession()

    created = await handle_event(session, _event())

    assert created is True
    assert len(session.merged) == 1
    assert isinstance(session.merged[0], ReportTimeFact)
    assert session.merged[0].duration_seconds == 1800
    assert any(isinstance(item, ReportProcessedEvent) for item in session.added)
    assert session.committed is True


async def test_duplicate_event_is_ignored() -> None:
    session = _FakeSession(processed=object())

    assert await handle_event(session, _event()) is False
    assert session.merged == []


def test_cache_is_tenant_scoped() -> None:
    cache.clear()
    cache.put("tenant-a:time:x=1", {"ok": True})
    cache.put("tenant-b:time:x=1", {"ok": False})

    assert cache.get("tenant-a:time:x=1") == {"ok": True}
    cache.invalidate_tenant("tenant-a")
    assert cache.get("tenant-a:time:x=1") is None
    assert cache.get("tenant-b:time:x=1") == {"ok": False}
    cache.clear()


def test_range_defaults_and_validation() -> None:
    start, end = _range(None, None)
    assert (end - start).days == 30

    start, end = _range(date(2026, 11, 1), date(2026, 11, 2))
    assert start == datetime(2026, 11, 1, tzinfo=UTC)
    assert end == datetime(2026, 11, 3, tzinfo=UTC)
