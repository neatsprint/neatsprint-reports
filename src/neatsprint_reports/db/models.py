"""Rebuildable report read models."""

from __future__ import annotations

import uuid
from datetime import datetime

from neatsprint_lib.db import Base, TenantMixin, TimestampMixin
from neatsprint_lib.events import OutboxEvent as OutboxEvent  # noqa: F401 - register table
from neatsprint_lib.ids import new_id
from sqlalchemy import DateTime, Index, Integer, Numeric, String, text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column


class ReportTimeFact(Base, TenantMixin):
    __tablename__ = "report_time_facts"
    __table_args__ = (
        Index("ix_report_time_started", "tenant_id", "started_at"),
        Index("ix_report_time_project", "tenant_id", "project_id", "started_at"),
        Index("ix_report_time_user", "tenant_id", "user_id", "started_at"),
    )

    entry_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    user_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    project_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True))
    item_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True))
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    duration_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    source: Mapped[str] = mapped_column(String(16), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class ReportItemFact(Base, TenantMixin):
    __tablename__ = "report_item_facts"
    __table_args__ = (
        Index("ix_report_items_project", "tenant_id", "project_id", "created_at"),
        Index("ix_report_items_sprint", "tenant_id", "sprint_id"),
    )

    item_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    project_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    title: Mapped[str] = mapped_column(String(240), nullable=False, default="")
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="TODO")
    sprint_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True))
    assignee_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True))
    estimate_points: Mapped[float | None] = mapped_column(Numeric(6, 2))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class ReportMonitorFact(Base, TenantMixin):
    __tablename__ = "report_monitor_facts"
    __table_args__ = (
        Index("ix_report_monitor_captured", "tenant_id", "captured_at"),
        Index("ix_report_monitor_user", "tenant_id", "user_id", "captured_at"),
    )

    screenshot_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    user_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False, default="PENDING")
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class ReportSessionFact(Base, TenantMixin):
    __tablename__ = "report_session_facts"
    __table_args__ = (Index("ix_report_sessions_started", "tenant_id", "started_at"),)

    session_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    user_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    item_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True))
    project_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True))
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    duration_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    state: Mapped[str] = mapped_column(String(16), nullable=False, default="RUNNING")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class ReportSprintFact(Base, TenantMixin):
    __tablename__ = "report_sprint_snapshots"
    __table_args__ = (Index("ix_report_sprints_project", "tenant_id", "project_id"),)

    sprint_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    project_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False, default="PLANNED")
    committed_points: Mapped[float | None] = mapped_column(Numeric(8, 2))
    carried_over: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class ReportProcessedEvent(Base):
    __tablename__ = "report_processed_events"

    event_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    tenant_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True))
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class ReportExport(Base, TenantMixin, TimestampMixin):
    __tablename__ = "report_exports"
    __table_args__ = (Index("ix_report_exports_user", "tenant_id", "user_id", "created_at"),)

    id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=new_id
    )
    user_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    report: Mapped[str] = mapped_column(String(32), nullable=False)
    format: Mapped[str] = mapped_column(String(8), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="PENDING")
    from_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    to_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    storage_key: Mapped[str | None] = mapped_column(String(512))
    error: Mapped[str | None] = mapped_column(String(500))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
