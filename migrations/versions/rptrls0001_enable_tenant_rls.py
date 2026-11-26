"""Add RLS policies to all tenant-owned report tables.

Revision ID: rptrls0001
Revises: 9f092edb13c1
"""

from __future__ import annotations

from alembic import op

revision = "rptrls0001"
down_revision = "9f092edb13c1"
branch_labels = None
depends_on = None

TABLES = (
    "report_item_facts",
    "report_monitor_facts",
    "report_session_facts",
    "report_sprint_snapshots",
    "report_time_facts",
    "report_exports",
)

POLICY = (
    "(nullif(current_setting('app.tenant_id', true), '') IS NULL OR "
    "tenant_id = nullif(current_setting('app.tenant_id', true), '')::uuid)"
)


def upgrade() -> None:
    for table in TABLES:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
        op.execute(
            f"CREATE POLICY tenant_isolation ON {table} "
            f"USING ({POLICY}) WITH CHECK ({POLICY})"
        )


def downgrade() -> None:
    for table in TABLES:
        op.execute(f"DROP POLICY IF EXISTS tenant_isolation ON {table}")
        op.execute(f"ALTER TABLE {table} NO FORCE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY")
