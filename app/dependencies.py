"""FastAPI dependencies and shared request helpers."""

from fastapi import HTTPException, Request
from motor.motor_asyncio import AsyncIOMotorDatabase


def get_database(request: Request) -> AsyncIOMotorDatabase:
    return request.app.state.database


def get_request_id(request: Request) -> str:
    return request.state.request_id


def parse_order_id(order_id: str):
    from bson import ObjectId

    if not ObjectId.is_valid(order_id):
        raise HTTPException(status_code=404, detail="Order not found")
    return ObjectId(order_id)
