"""Export the full current curated state for Druid ingestion."""

def export_current_state() -> None:
    """Export the current Iceberg table to Parquet for Druid.

    TODO: prove the selected Druid ingestion/replacement strategy does not
    leave stale versions or duplicate records after updates.
    """
    raise NotImplementedError("Druid export is not implemented yet")
