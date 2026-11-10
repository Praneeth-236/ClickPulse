"""Kafka consumer and raw HDFS ingestion entry point."""

def consume_to_hdfs() -> None:
    """Consume Kafka records and persist original bytes to HDFS.

    TODO:
      - Persist every occurrence, including duplicates and malformed lines.
      - Preserve the exact original message value.
      - Persist source metadata separately.
      - Make writes recoverable and safe on retry.
    """
    raise NotImplementedError("Kafka-to-HDFS ingestion is not implemented yet")
