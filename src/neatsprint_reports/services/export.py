"""Background export generation for reports."""

from __future__ import annotations

import csv
import io
import logging
from datetime import UTC, datetime
from uuid import UUID

from reportlab.lib import colors  # type: ignore[import-untyped]
from reportlab.lib.pagesizes import A4  # type: ignore[import-untyped]
from reportlab.lib.styles import getSampleStyleSheet  # type: ignore[import-untyped]
from reportlab.platypus import (  # type: ignore[import-untyped]
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from neatsprint_reports.db.models import ReportExport, ReportMonitorFact, ReportTimeFact
from neatsprint_reports.services import storage
from neatsprint_reports.settings import Settings

logger = logging.getLogger(__name__)


def _time_rows(facts: list[ReportTimeFact]) -> tuple[list[str], list[list[str]]]:
    header = ["started_at", "user_id", "project_id", "item_id", "seconds", "source"]
    rows = [
        [
            fact.started_at.isoformat(),
            str(fact.user_id),
            str(fact.project_id or ""),
            str(fact.item_id or ""),
            str(fact.duration_seconds),
            fact.source,
        ]
        for fact in facts
    ]
    return header, rows


def _monitor_rows(facts: list[ReportMonitorFact]) -> tuple[list[str], list[list[str]]]:
    header = ["captured_at", "user_id", "state", "deleted_at"]
    rows = [
        [
            fact.captured_at.isoformat(),
            str(fact.user_id),
            fact.state,
            fact.deleted_at.isoformat() if fact.deleted_at else "",
        ]
        for fact in facts
    ]
    return header, rows


def _csv(header: list[str], rows: list[list[str]]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer)
    writer.writerow(header)
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


def _pdf(title: str, header: list[str], rows: list[list[str]]) -> bytes:
    buffer = io.BytesIO()
    document = SimpleDocTemplate(buffer, pagesize=A4, title=title)
    styles = getSampleStyleSheet()
    story = [Paragraph(title, styles["Title"]), Spacer(1, 12)]
    data = [header, *rows]
    table = Table(data, repeatRows=1)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e0e7ff")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 7),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#cbd5e1")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    story.append(table)
    document.build(story)
    return buffer.getvalue()


async def generate_export(
    job_id: UUID,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> None:
    async with session_factory() as session:
        job = await session.get(ReportExport, job_id)
        if job is None:
            return
        job.status = "RUNNING"
        await session.commit()
        try:
            if job.report == "time":
                time_facts = list(
                    (
                        await session.execute(
                            select(ReportTimeFact).where(
                                ReportTimeFact.tenant_id == job.tenant_id,
                                ReportTimeFact.started_at >= job.from_date,
                                ReportTimeFact.started_at < job.to_date,
                            )
                        )
                    ).scalars()
                )
                header, rows = _time_rows(time_facts)
            else:
                monitor_facts = list(
                    (
                        await session.execute(
                            select(ReportMonitorFact).where(
                                ReportMonitorFact.tenant_id == job.tenant_id,
                                ReportMonitorFact.captured_at >= job.from_date,
                                ReportMonitorFact.captured_at < job.to_date,
                            )
                        )
                    ).scalars()
                )
                header, rows = _monitor_rows(monitor_facts)
            if job.format == "pdf":
                body = _pdf(f"{job.report.title()} report", header, rows)
                content_type = "application/pdf"
            else:
                body = _csv(header, rows)
                content_type = "text/csv"
            key = f"tenant/{job.tenant_id}/exports/{job.id}.{job.format}"
            storage.upload(settings, key, body, content_type)
            job.storage_key = key
            job.status = "COMPLETED"
            job.completed_at = datetime.now(tz=UTC)
            job.error = None
        except Exception as exc:  # noqa: BLE001 - surfaced to the job status
            logger.exception("report_export_failed", extra={"job_id": str(job.id)})
            job.status = "FAILED"
            job.error = str(exc)[:500]
        await session.commit()
