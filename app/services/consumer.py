"""SQS event processing for local polling and Lambda batch invocation."""

import asyncio
import json
import logging
from datetime import UTC, datetime

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException
from pymongo.errors import DuplicateKeyError, PyMongoError

from app.config import settings
from app.constants import AUDIT_COLLECTION
from app.services.invoices import ensure_invoice_uploaded

logger = logging.getLogger("orders")


async def process_order_event_body(app: FastAPI, body: str) -> None:
    envelope = json.loads(body)
    detail = envelope.get("detail")
    if isinstance(detail, str):
        detail = json.loads(detail)
    if not isinstance(detail, dict) or not detail.get("event_id") or not detail.get("order_id"):
        raise ValueError("SQS message does not contain a valid order event")
    if envelope.get("detail-type") == "OrderCreated":
        await ensure_invoice_uploaded(app, detail["order_id"], detail["event_id"])
    audit_record = {"event_id": detail["event_id"], "event_type": envelope.get("detail-type", "Unknown"), "order_id": detail["order_id"], "status": detail.get("status"), "occurred_at": datetime.fromisoformat(detail["occurred_at"]), "processed_at": datetime.now(UTC), "eventbridge_event_id": envelope.get("id"), "correlation_id": detail.get("correlation_id")}
    try:
        await app.state.database[AUDIT_COLLECTION].insert_one(audit_record)
        logger.info("Processed SQS event %s", audit_record["event_id"])
    except DuplicateKeyError:
        logger.info("Ignored duplicate SQS event %s", audit_record["event_id"])


async def process_sqs_message(app: FastAPI, message: dict) -> None:
    await process_order_event_body(app, message["Body"])
    await asyncio.to_thread(app.state.sqs_client.delete_message, QueueUrl=app.state.sqs_queue_url, ReceiptHandle=message["ReceiptHandle"])


async def sqs_consumer_loop(app: FastAPI) -> None:
    logger.info("SQS consumer started for queue '%s'", settings.sqs_queue_name)
    while True:
        try:
            response = await asyncio.to_thread(app.state.sqs_client.receive_message, QueueUrl=app.state.sqs_queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=20, VisibilityTimeout=60, AttributeNames=["All"])
            for message in response.get("Messages", []):
                try:
                    await process_sqs_message(app, message)
                except (HTTPException, json.JSONDecodeError, KeyError, TypeError, ValueError, PyMongoError, BotoCoreError, ClientError):
                    logger.exception("SQS message processing failed; it will be retried")
        except asyncio.CancelledError:
            logger.info("SQS consumer stopped")
            raise
        except (BotoCoreError, ClientError):
            logger.exception("SQS receive failed; retrying in five seconds")
            await asyncio.sleep(5)


async def run_sqs_lambda_batch(app: FastAPI, event: dict) -> dict:
    failures = []
    for record in event.get("Records", []):
        try:
            await process_order_event_body(app, record["body"])
        except (HTTPException, json.JSONDecodeError, KeyError, TypeError, ValueError, PyMongoError, BotoCoreError, ClientError):
            logger.exception("Lambda worker failed SQS message %s", record.get("messageId"))
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}
