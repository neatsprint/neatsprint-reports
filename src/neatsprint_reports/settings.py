from neatsprint_lib.settings import ServiceSettings


class Settings(ServiceSettings):
    service_name: str = "reports"
    api_prefix: str = "reports"
    outbox_stream: str = "neatsprint.events"
    reports_consumer_group: str = "reports-projector"
    aggregation_min_users: int = 3


settings = Settings()
