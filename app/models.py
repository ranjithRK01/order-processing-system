"""Request and response contracts for the Order Processing API."""

from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator


class OrderStatus(StrEnum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"


class OrderItemInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    product_id: str = Field(min_length=1, max_length=100, examples=["SKU-RED-42"])
    name: str = Field(min_length=1, max_length=250, examples=["Red T-shirt"])
    quantity: int = Field(gt=0, le=1_000, examples=[2])
    unit_price: Decimal = Field(gt=0, max_digits=12, decimal_places=2, examples=[19.99])


class CreateOrderRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    customer_id: str = Field(min_length=1, max_length=100, examples=["customer-123"])
    items: list[OrderItemInput] = Field(min_length=1)

    @field_validator("items")
    @classmethod
    def unique_products(cls, items: list[OrderItemInput]) -> list[OrderItemInput]:
        product_ids = [item.product_id for item in items]
        if len(product_ids) != len(set(product_ids)):
            raise ValueError("each product_id may appear only once per order")
        return items


class UpdateStatusRequest(BaseModel):
    status: OrderStatus


class OrderItemResponse(OrderItemInput):
    line_total: Decimal


class OrderResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    customer_id: str
    items: list[OrderItemResponse]
    total_amount: Decimal
    status: OrderStatus
    created_at: datetime
    updated_at: datetime
    cancelled_at: datetime | None = None


class OrderEventResponse(BaseModel):
    event_id: str
    event_type: str
    order_id: str
    status: OrderStatus
    occurred_at: datetime
    processed_at: datetime
    correlation_id: str | None = None


class InvoiceDownloadResponse(BaseModel):
    order_id: str
    s3_key: str
    download_url: str
    expires_in_seconds: int


class OrderPageResponse(BaseModel):
    items: list[OrderResponse]
    next_cursor: str | None = None
