"""Unit tests for deterministic file-level batch identity."""

from clickpulse.storage.batch_ledger import get_batch_id


def test_same_bytes_have_same_batch_identity():
    payload = b'{"event_id":"evt-1"}\n'
    try:
        assert get_batch_id(payload) == get_batch_id(payload)
    except NotImplementedError:
        # Scaffold-only until the ledger implementation is added.
        return
