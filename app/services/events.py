"""Transactional outbox and EventBridge publishing."""

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI
from pymongo import ASCENDING, ReturnDocument

from app.config import settings
from app.constants import OUTBOX_COLLECTION

logger = logging.getLogger("orders")


def build_order_event(detail_type: str, order: dict, correlation_id: str) -> dict:
    event_id = str(uuid4())
    occurred_at = datetime.now(UTC)
    return {
        "event_id": event_id,
        "source": "order.processing",
        "detail_type": detail_type,
        "detail": {
            "event_id": event_id,
            "order_id": str(order["_id"]),
            "customer_id": order["customer_id"],
            "status": order["status"],
            "total_amount": str(order["total_amount"]),
            "occurred_at": occurred_at.isoformat(),
            "correlation_id": correlation_id,
        },
        "created_at": occurred_at,
        "published_at": None,
        "publish_lease_id": None,
        "publish_lease_until": None,
        "publish_attempts": 0,
    }


async def publish_outbox_event(app: FastAPI, event: dict) -> bool:
    now = datetime.now(UTC)
    lease_id = str(uuid4())
    claimed_event = await app.state.database[OUTBOX_COLLECTION].find_one_and_update(
        {
            "event_id": event["event_id"], "published_at": None,
            "$or": [
                {"publish_lease_until": {"$exists": False}},
                {"publish_lease_until": None},
                {"publish_lease_until": {"$lte": now}},
            ],
        },
        {"$set": {"publish_lease_id": lease_id, "publish_lease_until": now + timedelta(minutes=2)}},
        return_document=ReturnDocument.AFTER,
    )
    if not claimed_event:
        return True
    try:
        response = await asyncio.to_thread(
            app.state.eventbridge_client.put_events,
            Entries=[{
                "Source": claimed_event["source"],
                "DetailType": claimed_event["detail_type"],
                "Detail": json.dumps(claimed_event["detail"], separators=(",", ":")),
                "EventBusName": settings.eventbridge_event_bus_name,
            }],
        )
        entry = response.get("Entries", [{}])[0]
        if response.get("FailedEntryCount", 0) or entry.get("ErrorCode"):
            raise RuntimeError(f"EventBridge rejected event: {entry}")
        await app.state.database[OUTBOX_COLLECTION].update_one(
            {"event_id": claimed_event["event_id"], "published_at": None, "publish_lease_id": lease_id},
            {"$set": {"published_at": datetime.now(UTC), "eventbridge_event_id": entry.get("EventId")}, "$unset": {"publish_lease_id": "", "publish_lease_until": ""}, "$inc": {"publish_attempts": 1}},
        )
        logger.info("Published %s event %s", claimed_event["detail_type"], claimed_event["event_id"])
        return True
    except (BotoCoreError, ClientError, RuntimeError) as exc:
        await app.state.database[OUTBOX_COLLECTION].update_one(
            {"event_id": claimed_event["event_id"], "published_at": None, "publish_lease_id": lease_id},
            {"$set": {"last_publish_error": str(exc), "last_publish_attempt_at": datetime.now(UTC)}, "$unset": {"publish_lease_id": "", "publish_lease_until": ""}, "$inc": {"publish_attempts": 1}},
        )
        logger.exception("Event %s was saved for retry: %s", claimed_event["event_id"], exc)
        return False


async def retry_unpublished_events(app: FastAPI) -> None:
    events = await app.state.database[OUTBOX_COLLECTION].find({"published_at": None}).sort("created_at", ASCENDING).to_list(50)
    for event in events:
        await publish_outbox_event(app, event)
