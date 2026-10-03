"""Order and invoice HTTP endpoints."""

import asyncio
import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal

from fastapi import APIRouter, Header, HTTPException, Query, Request, Response, status
from pymongo import ASCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

from app.constants import AUDIT_COLLECTION, ORDER_COLLECTION, OUTBOX_COLLECTION
from app.dependencies import get_database, get_request_id, parse_order_id
from app.models import CreateOrderRequest, InvoiceDownloadResponse, OrderEventResponse, OrderPageResponse, OrderResponse, OrderStatus, UpdateStatusRequest
from app.services.events import build_order_event, publish_outbox_event

router = APIRouter(prefix="/orders", tags=["Orders"])


def serialize_order(document: dict) -> OrderResponse:
    decimal_value = document["total_amount"].to_decimal() if hasattr(document["total_amount"], "to_decimal") else Decimal(str(document["total_amount"]))
    return OrderResponse(id=str(document["_id"]), customer_id=document["customer_id"], items=document["items"], total_amount=decimal_value, status=document["status"], created_at=document["created_at"], updated_at=document["updated_at"], cancelled_at=document.get("cancelled_at"))


def payload_fingerprint(payload: CreateOrderRequest) -> str:
    canonical_payload = json.dumps(payload.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()


@router.post("", response_model=OrderResponse, status_code=status.HTTP_201_CREATED)
async def create_order(payload: CreateOrderRequest, request: Request, response: Response, idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=128)) -> OrderResponse:
    database = get_database(request)
    key_hash = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest() if idempotency_key else None
    request_hash = payload_fingerprint(payload)
    if key_hash:
        existing = await database[ORDER_COLLECTION].find_one({"idempotency_key_hash": key_hash})
        if existing:
            if existing["idempotency_request_hash"] != request_hash:
                raise HTTPException(status_code=409, detail="Idempotency-Key was already used for a different order payload")
            response.status_code = status.HTTP_200_OK
            return serialize_order(existing)
    now = datetime.now(UTC)
    items, total = [], Decimal("0")
    for item in payload.items:
        line_total = item.unit_price * item.quantity
        total += line_total
        items.append({**item.model_dump(), "unit_price": str(item.unit_price), "line_total": str(line_total)})
    order = {"customer_id": payload.customer_id, "items": items, "total_amount": str(total), "status": OrderStatus.PENDING.value, "created_at": now, "updated_at": now}
    if key_hash:
        order.update({"idempotency_key_hash": key_hash, "idempotency_request_hash": request_hash})
    try:
        async with await request.app.state.mongo_client.start_session() as session:
            async with session.start_transaction():
                result = await database[ORDER_COLLECTION].insert_one(order, session=session)
                order["_id"] = result.inserted_id
                event = build_order_event("OrderCreated", order, get_request_id(request))
                await database[OUTBOX_COLLECTION].insert_one(event, session=session)
    except DuplicateKeyError:
        existing = await database[ORDER_COLLECTION].find_one({"idempotency_key_hash": key_hash})
        if existing and existing["idempotency_request_hash"] == request_hash:
            response.status_code = status.HTTP_200_OK
            return serialize_order(existing)
        raise HTTPException(status_code=409, detail="Idempotency-Key was already used for a different order payload")
    await publish_outbox_event(request.app, event)
    return serialize_order(order)


@router.get("/{order_id}/events", response_model=list[OrderEventResponse])
async def get_order_events(order_id: str, request: Request) -> list[OrderEventResponse]:
    parse_order_id(order_id)
    events = await get_database(request)[AUDIT_COLLECTION].find({"order_id": order_id}).sort("processed_at", ASCENDING).to_list(1_000)
    return [OrderEventResponse(event_id=item["event_id"], event_type=item["event_type"], order_id=item["order_id"], status=item["status"], occurred_at=item["occurred_at"], processed_at=item["processed_at"], correlation_id=item.get("correlation_id")) for item in events]


@router.get("/{order_id}/invoice", response_model=InvoiceDownloadResponse)
async def get_invoice_download_url(order_id: str, request: Request) -> InvoiceDownloadResponse:
    order = await get_database(request)[ORDER_COLLECTION].find_one({"_id": parse_order_id(order_id)})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    invoice = order.get("invoice")
    if not invoice:
        raise HTTPException(status_code=404, detail="Invoice is not available yet")
    expires_in_seconds = 300
    url = await asyncio.to_thread(request.app.state.s3_client.generate_presigned_url, "get_object", Params={"Bucket": invoice["s3_bucket"], "Key": invoice["s3_key"]}, ExpiresIn=expires_in_seconds)
    return InvoiceDownloadResponse(order_id=order_id, s3_key=invoice["s3_key"], download_url=url, expires_in_seconds=expires_in_seconds)


@router.get("/{order_id}", response_model=OrderResponse)
async def get_order(order_id: str, request: Request) -> OrderResponse:
    order = await get_database(request)[ORDER_COLLECTION].find_one({"_id": parse_order_id(order_id)})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return serialize_order(order)


@router.get("", response_model=OrderPageResponse)
async def list_orders(request: Request, order_status: OrderStatus | None = Query(default=None, alias="status"), limit: int = Query(default=20, ge=1, le=100), cursor: str | None = Query(default=None)) -> OrderPageResponse:
    query = {"status": order_status.value} if order_status else {}
    if cursor:
        query["_id"] = {"$lt": parse_order_id(cursor)}
    orders = await get_database(request)[ORDER_COLLECTION].find(query).sort("_id", -1).to_list(length=limit + 1)
    next_cursor = str(orders[limit - 1]["_id"]) if len(orders) > limit else None
    return OrderPageResponse(items=[serialize_order(order) for order in orders[:limit]], next_cursor=next_cursor)


@router.patch("/{order_id}/status", response_model=OrderResponse)
async def update_order_status(order_id: str, payload: UpdateStatusRequest, request: Request) -> OrderResponse:
    transitions = {OrderStatus.PENDING: OrderStatus.PROCESSING, OrderStatus.PROCESSING: OrderStatus.SHIPPED, OrderStatus.SHIPPED: OrderStatus.DELIVERED}
    expected = next((current for current, target in transitions.items() if target == payload.status), None)
    if expected is None:
        raise HTTPException(status_code=422, detail="Status must follow the order lifecycle")
    object_id = parse_order_id(order_id)
    async with await request.app.state.mongo_client.start_session() as session:
        async with session.start_transaction():
            order = await get_database(request)[ORDER_COLLECTION].find_one_and_update({"_id": object_id, "status": expected.value}, {"$set": {"status": payload.status.value, "updated_at": datetime.now(UTC)}}, return_document=ReturnDocument.AFTER, session=session)
            if order:
                event = build_order_event("OrderStatusChanged", order, get_request_id(request))
                await get_database(request)[OUTBOX_COLLECTION].insert_one(event, session=session)
    if order:
        await publish_outbox_event(request.app, event)
        return serialize_order(order)
    if not await get_database(request)[ORDER_COLLECTION].find_one({"_id": object_id}):
        raise HTTPException(status_code=404, detail="Order not found")
    raise HTTPException(status_code=409, detail=f"Order must be {expected.value} before becoming {payload.status.value}")


@router.post("/{order_id}/cancel", response_model=OrderResponse)
async def cancel_order(order_id: str, request: Request) -> OrderResponse:
    object_id, now = parse_order_id(order_id), datetime.now(UTC)
    async with await request.app.state.mongo_client.start_session() as session:
        async with session.start_transaction():
            order = await get_database(request)[ORDER_COLLECTION].find_one_and_update({"_id": object_id, "status": OrderStatus.PENDING.value}, {"$set": {"status": OrderStatus.CANCELLED.value, "cancelled_at": now, "updated_at": now}}, return_document=ReturnDocument.AFTER, session=session)
            if order:
                event = build_order_event("OrderCancelled", order, get_request_id(request))
                await get_database(request)[OUTBOX_COLLECTION].insert_one(event, session=session)
    if order:
        await publish_outbox_event(request.app, event)
        return serialize_order(order)
    if not await get_database(request)[ORDER_COLLECTION].find_one({"_id": object_id}):
        raise HTTPException(status_code=404, detail="Order not found")
    raise HTTPException(status_code=409, detail="Only PENDING orders can be cancelled")
