"""Canonical ClickPulse event schema.

Keep this schema aligned with docs/DATA_CONTRACT.md.
Do not make nullable fields mandatory without an approved contract change.
"""

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class ClickEvent:
    event_id: str
    user_id: str
    session_id: str
    event_type: str
    product_id: str | None
    category: str | None
    price: Decimal | None
    quantity: int | None
    device: str | None
    event_time: str
    updated_at: str
