"""
Pydantic v2 event schemas for the e-commerce pipeline.
"""
from __future__ import annotations

from datetime import datetime
from typing import List, Literal

from pydantic import BaseModel, Field, field_validator


class OrderItem(BaseModel):
    product_id: str
    quantity: int = Field(gt=0)
    unit_price: float = Field(gt=0)


class OrderEvent(BaseModel):
    event_id: str
    event_type: Literal["order_placed", "order_cancelled", "order_refunded"]
    order_id: str
    user_id: str
    session_id: str
    items: List[OrderItem]
    currency: str = Field(min_length=3, max_length=3)
    total_amount: float = Field(ge=0)
    discount_amount: float = Field(ge=0)
    payment_method: str
    country_code: str = Field(min_length=2, max_length=2)
    timestamp: str

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        datetime.fromisoformat(v)
        return v


class SessionEvent(BaseModel):
    event_id: str
    event_type: Literal["session_start", "page_view", "session_end"]
    session_id: str
    user_id: str
    page_url: str
    referrer: str
    device_type: Literal["desktop", "mobile", "tablet"]
    country_code: str = Field(min_length=2, max_length=2)
    timestamp: str

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        datetime.fromisoformat(v)
        return v


class CartEvent(BaseModel):
    event_id: str
    event_type: Literal["add_to_cart", "remove_from_cart", "cart_abandoned"]
    cart_id: str
    session_id: str
    user_id: str
    product_id: str
    quantity: int = Field(gt=0)
    unit_price: float = Field(gt=0)
    timestamp: str

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        datetime.fromisoformat(v)
        return v
