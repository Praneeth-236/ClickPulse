"""Record validation entry point.

Implement the complete V01–V08 validation rules from docs/DATA_CONTRACT.md.
Return all applicable rejection reasons for each invalid occurrence.
"""

from typing import Any


def validate_event(record: Any) -> list[str]:
    """Return rejection reason codes; an empty list means valid.

    TODO:
      - Reject non-object JSON values.
      - Validate required identifiers and event type.
      - Validate timestamps.
      - Reject negative price and quantity.
      - Validate numeric/integer types without silently coercing invalid values.
      - Preserve legitimate nulls for nullable fields.
    """
    raise NotImplementedError("Implement validation from DATA_CONTRACT.md")
