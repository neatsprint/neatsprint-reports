from neatsprint_lib.settings import ServiceSettings


class Settings(ServiceSettings):
    service_name: str = "reports"
    api_prefix: str = "reports"
    outbox_stream: str = "neatsprint.events"
    reports_consumer_group: str = "reports-projector"
    aggregation_min_users: int = 3
    s3_endpoint: str | None = None
    s3_public_endpoint: str | None = None
    s3_bucket: str = "neatsprint"
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    s3_region: str = "us-east-1"


settings = Settings()
