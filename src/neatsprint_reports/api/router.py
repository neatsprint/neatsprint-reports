"""Reports API: time, completion, velocity, burndown, monitoring and transparency."""

from __future__ import annotations

import asyncio
import csv
import io
from collections import defaultdict
from datetime import UTC, date, datetime, time, timedelta
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse, Response
from neatsprint_lib.auth import Principal, get_principal, require_permission
from neatsprint_lib.db import get_tenant_session
from neatsprint_lib.errors import ApiError
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from neatsprint_reports.db.models import (
    ReportExport,
    ReportItemFact,
    ReportMonitorFact,
    ReportProcessedEvent,
    ReportSessionFact,
    ReportSprintFact,
    ReportTimeFact,
)
from neatsprint_reports.services import cache, storage
from neatsprint_reports.services.export import generate_export

router = APIRouter(tags=["reports"])

SessionDep = Annotated[AsyncSession, Depends(get_tenant_session)]
CurrentPrincipal = Annotated[Principal, Depends(get_principal)]
ProductivityView = Annotated[Principal, Depends(require_permission("report:productivity:view"))]
AgileView = Annotated[Principal, Depends(require_permission("report:agile:view"))]
MonitoringView = Annotated[Principal, Depends(require_permission("report:monitoring:view"))]
AuditView = Annotated[Principal, Depends(require_permission("report:audit:view"))]
ExportPerm = Annotated[Principal, Depends(require_permission("report:export"))]

_tasks: set[asyncio.Task[None]] = set()


class ExportCreate(BaseModel):
    report: str = Field(pattern="^(time|monitoring)$")
    format: str = Field(pattern="^(csv|pdf)$")
    from_date: date
    to_date: date


def _export_payload(job: ReportExport) -> dict[str, object]:
    return {
        "id": str(job.id),
        "report": job.report,
        "format": job.format,
        "status": job.status,
        "error": job.error,
        "created_at": job.created_at.isoformat(),
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
    }


def envelope(request: Request, data: object, **meta: object) -> dict[str, object]:
    return {
        "data": data,
        "meta": {"request_id": getattr(request.state, "request_id", None), **meta},
    }


def _range(from_date: date | None, to_date: date | None) -> tuple[datetime, datetime]:
    end_day = to_date or datetime.now(tz=UTC).date()
    start_day = from_date or (end_day - timedelta(days=29))
    if start_day > end_day:
        raise ApiError(422, "validation_failed", "The start date must precede the end date.")
    return (
        datetime.combine(start_day, time.min, tzinfo=UTC),
        datetime.combine(end_day + timedelta(days=1), time.min, tzinfo=UTC),
    )


def _cache_key(tenant_id: UUID, name: str, **params: object) -> str:
    parts = [f"{key}={value}" for key, value in sorted(params.items())]
    return f"{tenant_id}:{name}:{'&'.join(parts)}"


@router.get("/ping", tags=["platform"], summary="Service liveness echo")
async def ping() -> dict[str, str]:
    return {"ping": "pong", "service": "reports"}


@router.get("/time", summary="Time by day, project or user")
async def time_report(
    principal: ProductivityView,
    request: Request,
    session: SessionDep,
    from_date: date | None = Query(default=None, alias="from"),
    to_date: date | None = Query(default=None, alias="to"),
    group_by: str = Query(default="day", pattern="^(day|project|user)$"),
    project_id: UUID | None = Query(default=None),
    user_id: UUID | None = Query(default=None),
) -> JSONResponse:
    start, end = _range(from_date, to_date)
    key = _cache_key(
        principal.tenant_id,
        "time",
        start=start.date(),
        end=end.date(),
        group_by=group_by,
        project_id=project_id,
        user_id=user_id,
    )
    cached = cache.get(key)
    if cached is not None:
        return JSONResponse(envelope(request, cached, cached=True))

    query = select(ReportTimeFact).where(
        ReportTimeFact.tenant_id == principal.tenant_id,
        ReportTimeFact.started_at >= start,
        ReportTimeFact.started_at < end,
    )
    if project_id is not None:
        query = query.where(ReportTimeFact.project_id == project_id)
    if user_id is not None:
        query = query.where(ReportTimeFact.user_id == user_id)
    rows = list((await session.execute(query)).scalars())
    buckets: dict[str, int] = defaultdict(int)
    for row in rows:
        if group_by == "day":
            bucket = row.started_at.date().isoformat()
        elif group_by == "project":
            bucket = str(row.project_id) if row.project_id else "unassigned"
        else:
            bucket = str(row.user_id)
        buckets[bucket] += row.duration_seconds
    payload = {
        "from": start.date().isoformat(),
        "to": (end - timedelta(days=1)).date().isoformat(),
        "group_by": group_by,
        "total_seconds": sum(buckets.values()),
        "rows": [
            {"key": bucket, "seconds": seconds}
            for bucket, seconds in sorted(buckets.items())
        ],
    }
    cache.put(key, payload)
    return JSONResponse(envelope(request, payload))


@router.get("/completion", summary="Created versus completed items")
async def completion_report(
    principal: AgileView,
    request: Request,
    session: SessionDep,
    from_date: date | None = Query(default=None, alias="from"),
    to_date: date | None = Query(default=None, alias="to"),
    project_id: UUID | None = Query(default=None),
) -> JSONResponse:
    start, end = _range(from_date, to_date)
    query = select(ReportItemFact).where(
        ReportItemFact.tenant_id == principal.tenant_id,
        ReportItemFact.created_at < end,
    )
    if project_id is not None:
        query = query.where(ReportItemFact.project_id == project_id)
    rows = list((await session.execute(query)).scalars())
    created: dict[str, int] = defaultdict(int)
    completed: dict[str, int] = defaultdict(int)
    for row in rows:
        if start <= row.created_at < end:
            created[row.created_at.date().isoformat()] += 1
        if row.completed_at is not None and start <= row.completed_at < end:
            completed[row.completed_at.date().isoformat()] += 1
    days = sorted(set(created) | set(completed))
    return JSONResponse(
        envelope(
            request,
            {
                "from": start.date().isoformat(),
                "to": (end - timedelta(days=1)).date().isoformat(),
                "created": sum(created.values()),
                "completed": sum(completed.values()),
                "days": [
                    {
                        "date": day,
                        "created": created.get(day, 0),
                        "completed": completed.get(day, 0),
                    }
                    for day in days
                ],
            },
        )
    )


@router.get("/velocity", summary="Velocity for the last sprints")
async def velocity_report(
    principal: AgileView,
    request: Request,
    session: SessionDep,
    project_id: UUID = Query(...),
    limit: int = Query(default=6, ge=1, le=20),
) -> JSONResponse:
    rows = list(
        (
            await session.execute(
                select(ReportItemFact).where(
                    ReportItemFact.tenant_id == principal.tenant_id,
                    ReportItemFact.project_id == project_id,
                    ReportItemFact.sprint_id.is_not(None),
                )
            )
        ).scalars()
    )
    sprints: dict[str, dict[str, float]] = defaultdict(lambda: {"committed": 0, "completed": 0})
    for row in rows:
        if row.sprint_id is None:
            continue
        bucket = sprints[str(row.sprint_id)]
        points = float(row.estimate_points or 0)
        bucket["committed"] += points
        if row.status == "DONE":
            bucket["completed"] += points
    snapshots = {
        str(row.sprint_id): row
        for row in (
            await session.execute(
                select(ReportSprintFact).where(
                    ReportSprintFact.tenant_id == principal.tenant_id,
                    ReportSprintFact.project_id == project_id,
                )
            )
        ).scalars()
    }
    ordered = list(sprints.items())[-limit:]
    series = []
    for sprint_id, series_points in ordered:
        snapshot = snapshots.get(sprint_id)
        series.append(
            {
                "sprint_id": sprint_id,
                "state": snapshot.state if snapshot else "PLANNED",
                "committed": series_points["committed"],
                "completed": series_points["completed"],
                "completed_at": (
                    snapshot.completed_at.isoformat()
                    if snapshot and snapshot.completed_at
                    else None
                ),
            }
        )
    return JSONResponse(envelope(request, {"project_id": str(project_id), "sprints": series}))


@router.get("/burndown", summary="Burndown for a sprint")
async def burndown_report(
    principal: AgileView,
    request: Request,
    session: SessionDep,
    sprint_id: UUID = Query(...),
) -> JSONResponse:
    snapshot = await session.get(ReportSprintFact, sprint_id)
    items = list(
        (
            await session.execute(
                select(ReportItemFact).where(
                    ReportItemFact.tenant_id == principal.tenant_id,
                    ReportItemFact.sprint_id == sprint_id,
                )
            )
        ).scalars()
    )
    total = sum(float(item.estimate_points or 0) for item in items)
    start_day = (
        snapshot.started_at.date()
        if snapshot and snapshot.started_at
        else min((item.created_at.date() for item in items), default=datetime.now(tz=UTC).date())
    )
    end_day = (
        snapshot.completed_at.date()
        if snapshot and snapshot.completed_at
        else datetime.now(tz=UTC).date()
    )
    if end_day < start_day:
        end_day = start_day
    done_by_day: dict[date, float] = defaultdict(float)
    for item in items:
        if item.completed_at is not None:
            done_by_day[item.completed_at.date()] += float(item.estimate_points or 0)
    series = []
    remaining = total
    day = start_day
    while day <= end_day:
        remaining -= done_by_day.get(day, 0)
        series.append({"date": day.isoformat(), "remaining": max(0.0, remaining)})
        day += timedelta(days=1)
    return JSONResponse(
        envelope(
            request,
            {
                "sprint_id": str(sprint_id),
                "total_points": total,
                "series": series,
            },
        )
    )


@router.get("/monitoring", summary="Monitoring summary")
async def monitoring_report(
    principal: MonitoringView,
    request: Request,
    session: SessionDep,
    from_date: date | None = Query(default=None, alias="from"),
    to_date: date | None = Query(default=None, alias="to"),
    aggregate: bool = Query(default=True),
) -> JSONResponse:
    start, end = _range(from_date, to_date)
    captures = list(
        (
            await session.execute(
                select(ReportMonitorFact).where(
                    ReportMonitorFact.tenant_id == principal.tenant_id,
                    ReportMonitorFact.captured_at >= start,
                    ReportMonitorFact.captured_at < end,
                )
            )
        ).scalars()
    )
    sessions = list(
        (
            await session.execute(
                select(ReportSessionFact).where(
                    ReportSessionFact.tenant_id == principal.tenant_id,
                    ReportSessionFact.started_at >= start,
                    ReportSessionFact.started_at < end,
                )
            )
        ).scalars()
    )
    distinct_users = len({row.user_id for row in captures} | {row.user_id for row in sessions})
    threshold = request.app.state.settings.aggregation_min_users
    suppressed = aggregate and 0 < distinct_users < threshold
    per_day: dict[str, dict[str, int]] = defaultdict(lambda: {"captures": 0, "deletions": 0})
    per_user: dict[str, dict[str, int]] = defaultdict(
        lambda: {"captures": 0, "deletions": 0, "active_seconds": 0}
    )
    deleted = 0
    for capture in captures:
        day = capture.captured_at.date().isoformat()
        per_day[day]["captures"] += 1
        per_user[str(capture.user_id)]["captures"] += 1
        if capture.state == "DELETED":
            deleted += 1
            per_day[day]["deletions"] += 1
            per_user[str(capture.user_id)]["deletions"] += 1
    for session_row in sessions:
        per_user[str(session_row.user_id)]["active_seconds"] += session_row.duration_seconds
    payload: dict[str, Any] = {
        "from": start.date().isoformat(),
        "to": (end - timedelta(days=1)).date().isoformat(),
        "totals": {
            "sessions": len(sessions),
            "captures": len(captures),
            "deletions": deleted,
            "deletion_rate": round(deleted / len(captures), 4) if captures else 0.0,
            "active_seconds": sum(row.duration_seconds for row in sessions),
        },
        "days": [
            {"date": day, **values} for day, values in sorted(per_day.items())
        ],
        "suppressed": suppressed,
        "aggregation_threshold": threshold,
    }
    if not suppressed:
        payload["users"] = [
            {"user_id": user, **values} for user, values in sorted(per_user.items())
        ]
    else:
        payload["users"] = []
    return JSONResponse(envelope(request, payload))


@router.get("/transparency", summary="Personal report log")
async def transparency_report(
    principal: CurrentPrincipal,
    request: Request,
    session: SessionDep,
    from_date: date | None = Query(default=None, alias="from"),
    to_date: date | None = Query(default=None, alias="to"),
) -> JSONResponse:
    start, end = _range(from_date, to_date)
    time_rows = list(
        (
            await session.execute(
                select(ReportTimeFact).where(
                    ReportTimeFact.tenant_id == principal.tenant_id,
                    ReportTimeFact.user_id == principal.user_id,
                    ReportTimeFact.started_at >= start,
                    ReportTimeFact.started_at < end,
                )
            )
        ).scalars()
    )
    monitor_rows = list(
        (
            await session.execute(
                select(ReportMonitorFact).where(
                    ReportMonitorFact.tenant_id == principal.tenant_id,
                    ReportMonitorFact.user_id == principal.user_id,
                    ReportMonitorFact.captured_at >= start,
                    ReportMonitorFact.captured_at < end,
                )
            )
        ).scalars()
    )
    return JSONResponse(
        envelope(
            request,
            {
                "time_seconds": sum(row.duration_seconds for row in time_rows),
                "captures": len(monitor_rows),
                "deletions": sum(1 for row in monitor_rows if row.state == "DELETED"),
                "entries": [
                    {
                        "entry_id": str(row.entry_id),
                        "started_at": row.started_at.isoformat(),
                        "duration_seconds": row.duration_seconds,
                    }
                    for row in time_rows
                ],
            },
        )
    )


@router.get("/export", summary="Immediate CSV export")
async def export_report(
    principal: ProductivityView,
    request: Request,
    session: SessionDep,
    report: str = Query(default="time", pattern="^(time|monitoring)$"),
    from_date: date | None = Query(default=None, alias="from"),
    to_date: date | None = Query(default=None, alias="to"),
) -> Response:
    start, end = _range(from_date, to_date)
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer)
    if report == "time":
        time_rows = list(
            (
                await session.execute(
                    select(ReportTimeFact).where(
                        ReportTimeFact.tenant_id == principal.tenant_id,
                        ReportTimeFact.started_at >= start,
                        ReportTimeFact.started_at < end,
                    )
                )
            ).scalars()
        )
        writer.writerow(
            ["entry_id", "user_id", "project_id", "item_id", "started_at", "seconds", "source"]
        )
        for time_row in time_rows:
            writer.writerow(
                [
                    time_row.entry_id,
                    time_row.user_id,
                    time_row.project_id or "",
                    time_row.item_id or "",
                    time_row.started_at.isoformat(),
                    time_row.duration_seconds,
                    time_row.source,
                ]
            )
    else:
        monitor_rows = list(
            (
                await session.execute(
                    select(ReportMonitorFact).where(
                        ReportMonitorFact.tenant_id == principal.tenant_id,
                        ReportMonitorFact.captured_at >= start,
                        ReportMonitorFact.captured_at < end,
                    )
                )
            ).scalars()
        )
        writer.writerow(["screenshot_id", "user_id", "captured_at", "state", "deleted_at"])
        for monitor_row in monitor_rows:
            writer.writerow(
                [
                    monitor_row.screenshot_id,
                    monitor_row.user_id,
                    monitor_row.captured_at.isoformat(),
                    monitor_row.state,
                    monitor_row.deleted_at.isoformat() if monitor_row.deleted_at else "",
                ]
            )
    return Response(
        content=buffer.getvalue().encode("utf-8"),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{report}-report.csv"'},
    )


@router.post("/exports", status_code=202, summary="Queue a report export")
async def create_export(
    payload: ExportCreate,
    principal: ExportPerm,
    request: Request,
    session: SessionDep,
) -> JSONResponse:
    if payload.from_date > payload.to_date:
        raise ApiError(422, "validation_failed", "The start date must precede the end date.")
    job = ReportExport(
        tenant_id=principal.tenant_id,
        user_id=principal.user_id,
        report=payload.report,
        format=payload.format,
        status="PENDING",
        from_date=datetime.combine(payload.from_date, time.min, tzinfo=UTC),
        to_date=datetime.combine(
            payload.to_date + timedelta(days=1), time.min, tzinfo=UTC
        ),
    )
    session.add(job)
    await session.commit()
    task = asyncio.create_task(
        generate_export(job.id, request.app.state.session_factory, request.app.state.settings)
    )
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)
    return JSONResponse(envelope(request, _export_payload(job)), status_code=202)


@router.get("/exports", summary="List my exports")
async def list_exports(
    principal: ExportPerm, request: Request, session: SessionDep
) -> JSONResponse:
    rows = (
        await session.execute(
            select(ReportExport)
            .where(
                ReportExport.tenant_id == principal.tenant_id,
                ReportExport.user_id == principal.user_id,
            )
            .order_by(ReportExport.created_at.desc())
            .limit(25)
        )
    ).scalars()
    return JSONResponse(envelope(request, [_export_payload(row) for row in rows]))


@router.get("/exports/{export_id}/download", summary="Download a completed export")
async def download_export(
    export_id: UUID,
    principal: ExportPerm,
    request: Request,
    session: SessionDep,
) -> JSONResponse:
    job = await session.get(ReportExport, export_id)
    if (
        job is None
        or job.tenant_id != principal.tenant_id
        or job.user_id != principal.user_id
    ):
        raise ApiError(404, "not_found", "Export not found.")
    if job.status != "COMPLETED" or not job.storage_key:
        raise ApiError(409, "not_ready", "That export is not ready yet.")
    try:
        url = storage.presign(request.app.state.settings, job.storage_key)
    except RuntimeError as exc:
        raise ApiError(503, "dependency_unavailable", "Export storage is unavailable.") from exc
    return JSONResponse(
        envelope(request, {"download_url": url, "expires_in": 300, "format": job.format})
    )


@router.get("/projection", summary="Projection health and lag")
async def projection_status(
    principal: AuditView, request: Request, session: SessionDep
) -> JSONResponse:
    count = (
        await session.execute(
            select(func.count()).select_from(ReportProcessedEvent)
        )
    ).scalar_one()
    latest = (
        await session.execute(select(func.max(ReportProcessedEvent.processed_at)))
    ).scalar_one()
    lag = (datetime.now(tz=UTC) - latest).total_seconds() if latest else None
    return JSONResponse(
        envelope(
            request,
            {
                "processed": count,
                "last_processed_at": latest.isoformat() if latest else None,
                "lag_seconds": lag,
            },
        )
    )
