"""Kafka event producer entry point."""

def main() -> None:
    """Publish source events to the agreed Kafka topic.

    TODO: preserve the original input line as the Kafka message value.
    Carry source file and line number as metadata, not by rewriting the value.
    """
    raise NotImplementedError("Kafka producer is not implemented yet")


if __name__ == "__main__":
    main()
