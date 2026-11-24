"""S3 storage for report exports."""

from __future__ import annotations

from typing import Any

import boto3  # type: ignore[import-untyped]
from botocore.client import Config  # type: ignore[import-untyped]
from botocore.exceptions import BotoCoreError, ClientError  # type: ignore[import-untyped]

from neatsprint_reports.settings import Settings


def _client(settings: Settings, *, public: bool = False) -> Any:
    if not settings.s3_endpoint or not settings.s3_access_key:
        raise RuntimeError("export storage is not configured")
    endpoint = (
        settings.s3_public_endpoint
        if public and settings.s3_public_endpoint
        else settings.s3_endpoint
    )
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
        config=Config(signature_version="s3v4"),
    )


def upload(settings: Settings, key: str, body: bytes, content_type: str) -> None:
    try:
        _client(settings).put_object(
            Bucket=settings.s3_bucket,
            Key=key,
            Body=body,
            ContentType=content_type,
        )
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError("could not store export") from exc


def presign(settings: Settings, key: str) -> str:
    try:
        return str(
            _client(settings, public=True).generate_presigned_url(
                "get_object",
                Params={"Bucket": settings.s3_bucket, "Key": key},
                ExpiresIn=300,
            )
        )
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError("could not sign export") from exc
