"""Pending-order processing used locally and by the EventBridge schedule Lambda."""

import logging
from datetime import UTC, datetime
from uuid import uuid4

from fastapi import FastAPI
from pymongo import ReturnDocument

from app.constants import ORDER_COLLECTION, OUTBOX_COLLECTION
from app.models import OrderStatus
from app.services.events import build_order_event, publish_outbox_event

logger = logging.getLogger("orders")


async def transition_pending_order(app: FastAPI, order_id) -> bool:
    now = datetime.now(UTC)
    async with await app.state.mongo_client.start_session() as session:
        async with session.start_transaction():
            order = await app.state.database[ORDER_COLLECTION].find_one_and_update({"_id": order_id, "status": OrderStatus.PENDING.value}, {"$set": {"status": OrderStatus.PROCESSING.value, "updated_at": now}}, return_document=ReturnDocument.AFTER, session=session)
            if not order:
                return False
            event = build_order_event("OrderStatusChanged", order, f"scheduler-{uuid4()}")
            await app.state.database[OUTBOX_COLLECTION].insert_one(event, session=session)
    await publish_outbox_event(app, event)
    return True


async def advance_pending_orders(app: FastAPI) -> None:
    orders = await app.state.database[ORDER_COLLECTION].find({"status": OrderStatus.PENDING.value}, {"_id": 1}).to_list(1_000)
    updated_count = sum([await transition_pending_order(app, order["_id"]) for order in orders])
    if updated_count:
        logger.info("Scheduled processor moved %s orders to PROCESSING", updated_count)
