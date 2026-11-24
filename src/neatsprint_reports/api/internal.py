"""Internal, resumable backfill from source services."""

from __future__ import annotations

import secrets
from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import UUID

import httpx
from fastapi import APIRouter, Depends, Header, Request
from fastapi.responses import JSONResponse
from neatsprint_lib.db import get_session
from neatsprint_lib.errors import ApiError
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from neatsprint_reports.db.models import (
    ReportItemFact,
    ReportMonitorFact,
    ReportTimeFact,
)
from neatsprint_reports.services import cache

router = APIRouter(prefix="/internal", tags=["internal"])
AdminSession = Annotated[AsyncSession, Depends(get_session)]


class BackfillRequest(BaseModel):
    tenant_id: UUID
    source: str = Field(pattern="^(task|worklog|monitoring)$")
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=100, ge=1, le=500)


async def _bootstrap_guard(
    request: Request, x_bootstrap_token: Annotated[str | None, Header()] = None
) -> None:
    expected = request.app.state.settings.bootstrap_token
    if not expected or not x_bootstrap_token or not secrets.compare_digest(
        x_bootstrap_token, expected
    ):
        raise ApiError(403, "permission_denied", "Invalid bootstrap token.")


BootstrapDep = Annotated[None, Depends(_bootstrap_guard)]


def _when(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


async def _fetch(request: Request, source: str, body: BackfillRequest) -> list[dict[str, Any]]:
    settings = request.app.state.settings
    token = settings.service_token or settings.bootstrap_token
    if not token:
        raise ApiError(503, "dependency_unavailable", "Source services are not configured.")
    base = {
        "task": settings.task_internal_url,
        "worklog": settings.worklog_internal_url,
        "monitoring": settings.monitoring_internal_url,
    }[source]
    path = {
        "task": "/internal/items",
        "worklog": "/internal/entries",
        "monitoring": "/internal/screenshots",
    }[source]
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(
                f"{base}{path}",
                params={
                    "tenant_id": str(body.tenant_id),
                    "offset": body.offset,
                    "limit": body.limit,
                },
                headers={"X-Service-Token": token},
            )
    except httpx.HTTPError as exc:
        raise ApiError(
            503, "dependency_unavailable", "A source service is unavailable."
        ) from exc
    if response.status_code != 200:
        raise ApiError(503, "dependency_unavailable", "A source service rejected the backfill.")
    payload = response.json().get("data")
    return payload if isinstance(payload, list) else []


@router.post("/backfill", summary="Backfill one page of report facts")
async def backfill(
    body: BackfillRequest,
    _: BootstrapDep,
    request: Request,
    session: AdminSession,
) -> JSONResponse:
    rows = await _fetch(request, body.source, body)
    for row in rows:
        if body.source == "task":
            await session.merge(
                ReportItemFact(
                    item_id=UUID(str(row["item_id"])),
                    tenant_id=body.tenant_id,
                    project_id=UUID(str(row["project_id"])),
                    title=str(row.get("title") or ""),
                    status=str(row.get("status") or "TODO"),
                    sprint_id=UUID(str(row["sprint_id"])) if row.get("sprint_id") else None,
                    assignee_id=(
                        UUID(str(row["assignee_id"])) if row.get("assignee_id") else None
                    ),
                    estimate_points=row.get("estimate_points"),
                    created_at=_when(row["created_at"]),
                    completed_at=(
                        _when(row["completed_at"]) if row.get("completed_at") else None
                    ),
                    updated_at=datetime.now(tz=UTC),
                )
            )
        elif body.source == "worklog":
            await session.merge(
                ReportTimeFact(
                    entry_id=UUID(str(row["entry_id"])),
                    tenant_id=body.tenant_id,
                    user_id=UUID(str(row["user_id"])),
                    project_id=UUID(str(row["project_id"])) if row.get("project_id") else None,
                    item_id=UUID(str(row["item_id"])) if row.get("item_id") else None,
                    started_at=_when(row["started_at"]),
                    duration_seconds=int(row["duration_seconds"]),
                    source=str(row.get("source") or "TIMER"),
                    updated_at=datetime.now(tz=UTC),
                )
            )
        else:
            await session.merge(
                ReportMonitorFact(
                    screenshot_id=UUID(str(row["screenshot_id"])),
                    tenant_id=body.tenant_id,
                    user_id=UUID(str(row["user_id"])),
                    captured_at=_when(row["captured_at"]),
                    state=str(row.get("state") or "PENDING"),
                    deleted_at=(
                        _when(row["deleted_at"]) if row.get("deleted_at") else None
                    ),
                    updated_at=datetime.now(tz=UTC),
                )
            )
    await session.commit()
    cache.invalidate_tenant(str(body.tenant_id))
    next_offset = body.offset + len(rows) if len(rows) == body.limit else None
    return JSONResponse(
        {"data": {"processed": len(rows), "next_offset": next_offset}}
    )
