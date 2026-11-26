"""Event consumer and projectors for the report read models."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import ResponseError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from neatsprint_reports.db.models import (
    ReportItemFact,
    ReportMonitorFact,
    ReportProcessedEvent,
    ReportSessionFact,
    ReportSprintFact,
    ReportTimeFact,
)
from neatsprint_reports.services import cache

logger = logging.getLogger(__name__)


def _uuid(value: object) -> UUID | None:
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def _when(value: object) -> datetime:
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        except ValueError:
            pass
    return datetime.now(tz=UTC)


def _data(fields: dict[str, Any]) -> dict[str, Any]:
    try:
        payload = json.loads(fields.get("data") or "{}")
        return payload if isinstance(payload, dict) else {}
    except (TypeError, ValueError):
        return {}


async def handle_event(session: AsyncSession, fields: dict[str, Any]) -> bool:
    event_id = _uuid(fields.get("event_id"))
    event_type = str(fields.get("event_type") or "")
    tenant_id = _uuid(fields.get("tenant_id"))
    if event_id is None or tenant_id is None:
        return False
    if await session.get(ReportProcessedEvent, event_id) is not None:
        return False
    payload = _data(fields)
    occurred = _when(fields.get("occurred_at"))

    if event_type in ("time_entry.recorded", "time_entry.updated"):
        entry_id = _uuid(payload.get("entry_id"))
        user_id = _uuid(payload.get("user_id")) or _uuid(fields.get("actor_id"))
        if entry_id and user_id:
            await session.merge(
                ReportTimeFact(
                    entry_id=entry_id,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    project_id=_uuid(payload.get("project_id")),
                    item_id=_uuid(payload.get("item_id")),
                    started_at=_when(payload.get("started_at") or occurred),
                    duration_seconds=int(payload.get("duration_seconds") or 0),
                    source=str(payload.get("source") or "TIMER"),
                    updated_at=occurred,
                )
            )
    elif event_type == "time_entry.deleted":
        entry_id = _uuid(payload.get("entry_id"))
        if entry_id:
            existing = await session.get(ReportTimeFact, entry_id)
            if existing is not None:
                await session.delete(existing)
    elif event_type in ("item.created", "item.updated"):
        item_id = _uuid(payload.get("item_id"))
        project_id = _uuid(payload.get("project_id"))
        if item_id and project_id:
            existing_item = await session.get(ReportItemFact, item_id)
            status = str(
                payload.get("status") or (existing_item.status if existing_item else "TODO")
            )
            completed_at = (
                occurred
                if status == "DONE"
                else (existing_item.completed_at if existing_item else None)
            )
            await session.merge(
                ReportItemFact(
                    item_id=item_id,
                    tenant_id=tenant_id,
                    project_id=project_id,
                    title=str(
                        payload.get("title") or (existing_item.title if existing_item else "")
                    ),
                    status=status,
                    sprint_id=_uuid(payload.get("sprint_id"))
                    or (existing_item.sprint_id if existing_item else None),
                    assignee_id=_uuid(payload.get("assignee_id"))
                    or (existing_item.assignee_id if existing_item else None),
                    estimate_points=payload.get("estimate_points")
                    if payload.get("estimate_points") is not None
                    else (existing_item.estimate_points if existing_item else None),
                    created_at=existing_item.created_at if existing_item else occurred,
                    completed_at=completed_at,
                    updated_at=occurred,
                )
            )
    elif event_type in ("item.deleted",):
        item_id = _uuid(payload.get("item_id"))
        if item_id:
            existing_item = await session.get(ReportItemFact, item_id)
            if existing_item is not None and existing_item.tenant_id == tenant_id:
                await session.delete(existing_item)
    elif event_type == "item.transitioned":
        item_id = _uuid(payload.get("item_id"))
        status = str(payload.get("to") or "")
        if item_id and status:
            existing_item = await session.get(ReportItemFact, item_id)
            if existing_item is not None:
                existing_item.status = status
                existing_item.completed_at = occurred if status == "DONE" else None
                existing_item.updated_at = occurred
            else:
                project_id = _uuid(payload.get("project_id"))
                if project_id:
                    await session.merge(
                        ReportItemFact(
                            item_id=item_id,
                            tenant_id=tenant_id,
                            project_id=project_id,
                            title="",
                            status=status,
                            created_at=occurred,
                            completed_at=occurred if status == "DONE" else None,
                            updated_at=occurred,
                        )
                    )
    elif event_type in ("work.session_started", "work.session_stopped"):
        session_id = _uuid(payload.get("session_id"))
        user_id = _uuid(fields.get("actor_id"))
        if session_id and user_id:
            existing_session = await session.get(ReportSessionFact, session_id)
            stopped = event_type == "work.session_stopped"
            await session.merge(
                ReportSessionFact(
                    session_id=session_id,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    item_id=_uuid(payload.get("item_id"))
                    or (existing_session.item_id if existing_session else None),
                    project_id=_uuid(payload.get("project_id"))
                    or (existing_session.project_id if existing_session else None),
                    started_at=existing_session.started_at if existing_session else occurred,
                    ended_at=occurred if stopped else None,
                    duration_seconds=int(payload.get("duration_seconds") or 0)
                    if stopped
                    else (existing_session.duration_seconds if existing_session else 0),
                    state="STOPPED" if stopped else "RUNNING",
                    updated_at=occurred,
                )
            )
    elif event_type in ("screenshot.available", "screenshot.deleted", "screenshot.expired"):
        screenshot_id = _uuid(payload.get("screenshot_id"))
        user_id = _uuid(payload.get("user_id"))
        if screenshot_id and user_id:
            state = {
                "screenshot.available": "VISIBLE",
                "screenshot.deleted": "DELETED",
                "screenshot.expired": "EXPIRED",
            }[event_type]
            await session.merge(
                ReportMonitorFact(
                    screenshot_id=screenshot_id,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    captured_at=_when(payload.get("captured_at") or occurred),
                    state=state,
                    deleted_at=occurred if state in ("DELETED", "EXPIRED") else None,
                    updated_at=occurred,
                )
            )
    elif event_type in ("sprint.started", "sprint.completed"):
        sprint_id = _uuid(payload.get("sprint_id"))
        project_id = _uuid(payload.get("project_id"))
        if sprint_id and project_id:
            completed = event_type == "sprint.completed"
            await session.merge(
                ReportSprintFact(
                    sprint_id=sprint_id,
                    tenant_id=tenant_id,
                    project_id=project_id,
                    state="COMPLETED" if completed else "ACTIVE",
                    committed_points=payload.get("committed_points"),
                    carried_over=int(payload.get("carried_over") or 0),
                    started_at=occurred if not completed else None,
                    completed_at=occurred if completed else None,
                    updated_at=occurred,
                )
            )

    session.add(
        ReportProcessedEvent(
            event_id=event_id,
            event_type=event_type,
            tenant_id=tenant_id,
            processed_at=datetime.now(tz=UTC),
        )
    )
    await session.commit()
    cache.invalidate_tenant(str(tenant_id))
    return True


async def _ensure_group(redis: Redis, stream: str, group: str) -> None:
    try:
        await redis.xgroup_create(stream, group, id="0", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def run_consumer(
    session_factory: async_sessionmaker[AsyncSession],
    redis_url: str,
    stream: str,
    group: str,
    stop: asyncio.Event,
    *,
    block_ms: int = 2000,
) -> None:
    redis = Redis.from_url(redis_url, decode_responses=True)
    name = f"reports-{uuid.uuid4().hex[:8]}"
    try:
        while not stop.is_set():
            try:
                await _ensure_group(redis, stream, group)
                break
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - Redis may not be ready yet
                logger.exception("reports_group_failed")
                await asyncio.sleep(2)
        while not stop.is_set():
            try:
                batches = await redis.xreadgroup(
                    group, name, {stream: ">"}, count=100, block=block_ms
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - survive transient failures
                logger.exception("reports_read_failed")
                await asyncio.sleep(2)
                continue
            for _, messages in batches or []:
                for message_id, fields in messages:
                    try:
                        async with session_factory() as session:
                            await handle_event(session, fields)
                    except Exception:  # noqa: BLE001 - keep the projection loop alive
                        logger.exception("reports_event_failed")
                    await redis.xack(stream, group, message_id)
    finally:
        await redis.aclose()
