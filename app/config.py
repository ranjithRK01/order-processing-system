"""Environment-driven application settings."""

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    mongodb_uri: str | None
    mongodb_database: str
    aws_region: str
    eventbridge_event_bus_name: str
    sqs_queue_name: str
    s3_invoice_bucket: str
    running_in_lambda: bool


def load_settings() -> Settings:
    return Settings(
        mongodb_uri=os.getenv("MONGODB_URI"),
        mongodb_database=os.getenv("MONGODB_DATABASE", "order_processing"),
        aws_region=os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "ap-south-2")),
        eventbridge_event_bus_name=os.getenv("EVENTBRIDGE_EVENT_BUS_NAME", "order-processing-bus"),
        sqs_queue_name=os.getenv("SQS_QUEUE_NAME", "order-events-queue"),
        s3_invoice_bucket=os.getenv("S3_INVOICE_BUCKET", "order-processing-invoices"),
        running_in_lambda=bool(os.getenv("AWS_LAMBDA_FUNCTION_NAME")),
    )


settings = load_settings()
