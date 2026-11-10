"""File-level idempotency ledger.

A completed source batch must be recognized by a stable content hash and
replayed as a true no-op, rather than counted as row-level drops.
"""

def get_batch_id(file_bytes: bytes) -> str:
    """Return a deterministic SHA-256 identifier for the exact source file."""
    raise NotImplementedError("Implement SHA-256 batch identity")


def is_batch_completed(batch_id: str) -> bool:
    """Check the durable processing ledger."""
    raise NotImplementedError("Implement durable batch ledger lookup")
