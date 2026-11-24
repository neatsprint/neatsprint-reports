"""Resumable report backfill CLI.

Usage:
    python -m neatsprint_reports.backfill --tenant <uuid> [--source task] [--source worklog]
"""

from __future__ import annotations

import argparse
import os
from typing import Any

import httpx


def run(tenant_id: str, sources: list[str], base_url: str, token: str) -> None:
    for source in sources:
        offset: int | None = 0
        total = 0
        while offset is not None:
            response = httpx.post(
                f"{base_url}/internal/backfill",
                json={
                    "tenant_id": tenant_id,
                    "source": source,
                    "offset": offset,
                    "limit": 200,
                },
                headers={"X-Bootstrap-Token": token},
                timeout=30,
            )
            response.raise_for_status()
            payload: dict[str, Any] = response.json()["data"]
            total += int(payload["processed"])
            offset = payload["next_offset"]
        print(f"{source}: {total} rows")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)
    parser.add_argument(
        "--source",
        action="append",
        choices=["task", "worklog", "monitoring"],
        dest="sources",
    )
    args = parser.parse_args()
    run(
        args.tenant,
        args.sources or ["task", "worklog", "monitoring"],
        os.environ.get("REPORTS_INTERNAL_URL", "http://localhost:8008"),
        os.environ.get("BOOTSTRAP_TOKEN", "dev-bootstrap-token-change-me"),
    )


if __name__ == "__main__":
    main()
