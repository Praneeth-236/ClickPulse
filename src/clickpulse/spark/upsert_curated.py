"""Apply validated batch winners to the Iceberg current-state table."""

def run_upsert() -> None:
    """Apply insert/update/drop/reject rules from DATA_CONTRACT.md.

    Absent event_id -> INSERT.
    Incoming event_time later than stored -> UPDATE.
    Equal or earlier event_time -> DROP.
    Invalid incoming record -> REJECT and preserve current valid state.
    """
    raise NotImplementedError("Iceberg upsert is not implemented yet")
