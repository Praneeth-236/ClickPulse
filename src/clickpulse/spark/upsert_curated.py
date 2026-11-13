"""Pure cross-batch current-state upsert decisions for ClickPulse records."""

import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any, Literal

from clickpulse.common.validation import validate_event
from clickpulse.spark.validate_and_deduplicate import DeduplicationResult

Operation = Literal["INSERT", "UPDATE", "DROP", "INVALID"]


@dataclass(frozen=True)
class UpsertDecision:
    """Auditable decision for one incoming event ID."""

    event_id: str
    operation: Operation
    previous_record: Mapping[str, Any] | None
    resulting_record: Mapping[str, Any] | None
    incoming_record: Mapping[str, Any]
    incoming_occurrences: tuple[Any, ...]
    reason: str


@dataclass(frozen=True)
class UpsertMetrics:
    insert_count: int
    update_count: int
    drop_count: int


@dataclass(frozen=True)
class UpsertResult:
    current_state: tuple[Mapping[str, Any], ...]
    decisions: tuple[UpsertDecision, ...]
    metrics: UpsertMetrics
    unresolved_event_ids: tuple[str, ...]
    invalid_event_ids: tuple[str, ...]


def _parse_event_time(record: Mapping[str, Any]) -> datetime:
    value = record["event_time"]
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("event_time must include a timezone")
    return parsed.astimezone(timezone.utc)  # noqa: UP017 - supports the Python 3.10 target


def _with_event_date(record: Mapping[str, Any]) -> dict[str, Any]:
    output = dict(record)
    output["event_date"] = _parse_event_time(record).date().isoformat()
    return output


def apply_upsert(
    existing_records: Iterable[Mapping[str, Any]],
    incoming: DeduplicationResult,
) -> UpsertResult:
    """Apply valid deduplication winners using strictly later ``event_time``.

    Records in unresolved latest-``updated_at`` ties are excluded from decisions.
    Invalid winners are surfaced as ``INVALID`` defensive decisions and never alter
    the state; normal pipeline validation should reject them before this function.
    """
    state: dict[str, dict[str, Any]] = {}
    for record in existing_records:
        event_id = record["event_id"]
        if event_id in state:
            raise ValueError(f"Existing curated state contains duplicate event_id: {event_id}")
        if validate_event(record):
            raise ValueError(f"Existing curated state contains invalid event_id: {event_id}")
        state[event_id] = _with_event_date(record)

    tie_ids = {tie.event_id for tie in incoming.unresolved_ties}
    winners_by_id: dict[str, Any] = {}
    for winner in incoming.winners:
        event_id = winner.record["event_id"]
        if event_id in tie_ids:
            raise ValueError(f"Unresolved event_id also appears among winners: {event_id}")
        if event_id in winners_by_id:
            raise ValueError(f"Incoming winners contain duplicate event_id: {event_id}")
        winners_by_id[event_id] = winner

    decisions: list[UpsertDecision] = []
    insert_count = update_count = drop_count = 0
    invalid_ids: list[str] = []

    for event_id in sorted(winners_by_id):
        winner = winners_by_id[event_id]
        record = winner.record
        previous = state.get(event_id)
        errors = validate_event(record)
        if errors:
            invalid_ids.append(event_id)
            decisions.append(
                UpsertDecision(
                    event_id=event_id,
                    operation="INVALID",
                    previous_record=previous,
                    resulting_record=previous,
                    incoming_record=dict(record),
                    incoming_occurrences=winner.occurrences,
                    reason="invalid incoming record: " + ", ".join(errors),
                )
            )
            continue

        incoming_with_date = _with_event_date(record)
        if previous is None:
            state[event_id] = incoming_with_date
            insert_count += 1
            operation: Operation = "INSERT"
            reason = "event_id absent from current state"
            resulting = incoming_with_date
        elif _parse_event_time(record) > _parse_event_time(previous):
            state[event_id] = incoming_with_date
            update_count += 1
            operation = "UPDATE"
            reason = "incoming event_time is later"
            resulting = incoming_with_date
        else:
            drop_count += 1
            operation = "DROP"
            reason = (
                "incoming event_time equals current event_time"
                if _parse_event_time(record) == _parse_event_time(previous)
                else "incoming event_time is earlier"
            )
            resulting = previous

        decisions.append(
            UpsertDecision(
                event_id=event_id,
                operation=operation,
                previous_record=previous,
                resulting_record=resulting,
                incoming_record=dict(record),
                incoming_occurrences=winner.occurrences,
                reason=reason,
            )
        )

    return UpsertResult(
        current_state=tuple(state[event_id] for event_id in sorted(state)),
        decisions=tuple(decisions),
        metrics=UpsertMetrics(insert_count, update_count, drop_count),
        unresolved_event_ids=tuple(sorted(tie_ids)),
        invalid_event_ids=tuple(sorted(invalid_ids)),
    )


@dataclass(frozen=True)
class SparkUpsertMetrics:
    insert_count: int
    update_count: int
    drop_count: int


@dataclass(frozen=True)
class SparkUpsertResult:
    """Upsert output; callers must call ``release`` when done with operation details.

    ``operation_details`` is eagerly locally checkpointed before the Iceberg
    MERGE, truncating its dependency on the mutable target table. The checkpoint
    uses Spark's cache subsystem, is caller-owned, and must be released to avoid
    retaining data across repeated batches.
    """

    current_state: Any
    operation_details: Any
    metrics: SparkUpsertMetrics
    iceberg_snapshot_id: str | None
    iceberg_committed_at: datetime | None

    def release(self, *, blocking: bool = False) -> None:
        """Release the pre-MERGE local checkpoint after its final use."""
        self.operation_details.unpersist(blocking=blocking)


def _event_spark_schema():
    """Explicit input schema for canonical records stored in raw_line."""
    from pyspark.sql.types import (
        DecimalType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    fields = [
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DecimalType(38, 18), True),
        StructField("quantity", IntegerType(), True),
        StructField("device", StringType(), True),
        StructField("event_time", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
    return StructType(fields)


def _quote_identifier(identifier: str) -> str:
    import re

    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier) is None:
        raise ValueError(f"Invalid SQL identifier: {identifier!r}")
    return f"`{identifier}`"


def _table_identifier(catalog: str, namespace: str, table: str) -> str:
    return ".".join(_quote_identifier(part) for part in (catalog, namespace, table))


def _ensure_iceberg_table(spark: Any, table_identifier: str) -> None:
    columns = """
        event_id STRING NOT NULL,
        user_id STRING NOT NULL,
        session_id STRING NOT NULL,
        event_type STRING NOT NULL,
        product_id STRING,
        category STRING,
        price DECIMAL(38, 18),
        quantity INT,
        device STRING,
        event_time TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL,
        event_date DATE NOT NULL
    """
    namespace = table_identifier.rsplit(".", maxsplit=1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table_identifier} ({columns}) "
        "USING iceberg PARTITIONED BY (event_date)"
    )


def _validate_iceberg_session(spark: Any, catalog: str) -> None:
    extension = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    configured_extensions = spark.conf.get("spark.sql.extensions", "")
    if extension not in configured_extensions.split(","):
        raise RuntimeError(
            "Iceberg SQL extensions are missing; create Spark with "
            "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
    catalog_key = f"spark.sql.catalog.{catalog}"
    if not spark.conf.get(catalog_key, None):
        raise RuntimeError(f"Iceberg catalog {catalog!r} is not configured in this SparkSession")


def _parse_incoming_records(spark: Any, incoming_records: Any):
    from pyspark.sql import functions as F
    from pyspark.sql.functions import udf
    from pyspark.sql.types import BooleanType, StructField, StructType

    required_columns = {"raw_line", "event_id"}
    missing = required_columns.difference(incoming_records.columns)
    if missing:
        raise ValueError(f"Incoming Spark DataFrame is missing columns: {sorted(missing)}")
    parsed = incoming_records.select(
        "raw_line",
        F.col("event_id").alias("_winner_event_id"),
        F.from_json(
            "raw_line",
            _event_spark_schema(),
            {"mode": "FAILFAST", "timeZone": "UTC"},
        ).alias("_event"),
    )
    event = parsed.select(
        "raw_line",
        "_winner_event_id",
        F.col("_event.event_id").alias("event_id"),
        F.col("_event.user_id").alias("user_id"),
        F.col("_event.session_id").alias("session_id"),
        F.col("_event.event_type").alias("event_type"),
        F.col("_event.product_id").alias("product_id"),
        F.col("_event.category").alias("category"),
        F.col("_event.price").alias("price"),
        F.col("_event.quantity").alias("quantity"),
        F.col("_event.device").alias("device"),
        F.col("_event.event_time").alias("event_time"),
        F.col("_event.updated_at").alias("updated_at"),
    )
    decimal_fits_schema = udf(_price_fits_decimal_38_18, BooleanType())
    raw_price = F.get_json_object("raw_line", "$.price")
    identity_mismatch = event.where(
        F.col("_winner_event_id").isNull()
        | (F.col("_winner_event_id") != F.col("event_id"))
    )
    if identity_mismatch.limit(1).count():
        raise ValueError("Incoming winner event_id does not match the event_id in raw_line")

    invalid_conversion = event.where(
        F.col("event_id").isNull()
        | F.col("user_id").isNull()
        | F.col("session_id").isNull()
        | F.col("event_type").isNull()
        | F.col("event_time").isNull()
        | F.col("updated_at").isNull()
        | (
            F.col("price").isNull()
            & F.get_json_object("raw_line", "$.price").isNotNull()
            & (F.get_json_object("raw_line", "$.price") != "null")
        )
        | (
            F.col("quantity").isNull()
            & F.get_json_object("raw_line", "$.quantity").isNotNull()
            & (F.get_json_object("raw_line", "$.quantity") != "null")
        )
        | (raw_price.isNotNull() & ~decimal_fits_schema(raw_price))
    )
    if invalid_conversion.limit(1).count():
        raise ValueError(
            "A validated winner could not be represented exactly by the canonical Spark schema"
        )

    projected = event.select(
        "event_id",
        "user_id",
        "session_id",
        "event_type",
        "product_id",
        "category",
        "price",
        "quantity",
        "device",
        "event_time",
        "updated_at",
        F.to_date("event_time").alias("event_date"),
    )
    required_fields = {
        "event_id",
        "user_id",
        "session_id",
        "event_type",
        "event_time",
        "updated_at",
        "event_date",
    }
    target_source_schema = StructType(
        [
            StructField(
                field.name,
                field.dataType,
                nullable=field.name not in required_fields,
                metadata=field.metadata,
            )
            for field in projected.schema.fields
        ]
    )
    # Spark filters do not refine nullable metadata. Re-encoding the distributed
    # rows against this target-derived schema both advertises the required
    # fields as NOT NULL to MERGE and verifies actual values on workers. This
    # does not collect records on the driver or discard null-bearing rows.
    return spark.createDataFrame(
        projected.rdd,
        schema=target_source_schema,
        verifySchema=True,
    )


def _price_fits_decimal_38_18(raw_value: str | None) -> bool:
    """Reject Spark decimal narrowing that would round or overflow a valid price."""
    if raw_value is None:
        return True
    try:
        value = Decimal(raw_value)
        quantum = Decimal("0.000000000000000001")
        with localcontext() as context:
            context.prec = 50
            return value.is_finite() and value == value.quantize(quantum) and abs(value) < 10**20
    except (InvalidOperation, ValueError):
        return False


def _execute_sql_with_snapshot_properties(
    spark: Any, statement: str, properties: Mapping[str, str]
) -> None:
    """Run a SQL write inside Iceberg's JVM-side commit metadata scope.

    Iceberg 1.8.1 exposes custom metadata for SQL writes through
    ``CommitMetadata.withCommitProperties``. Its properties are thread-local,
    so a Python ``Callable`` proxy would run the SQL on a different JVM thread.
    Java 11's built-in Nashorn engine provides a JVM-local ``Callable`` adapter
    and keeps the MERGE on the thread where Iceberg installs those properties.
    """
    jvm = spark._jvm
    engine = jvm.javax.script.ScriptEngineManager().getEngineByName("nashorn")
    if engine is None:
        raise RuntimeError(
            "Iceberg SQL snapshot markers require the Java 11 Nashorn scripting engine"
        )

    java_properties = jvm.java.util.HashMap()
    for key, value in properties.items():
        java_properties.put(key, value)

    engine.put("clickpulseSpark", spark._jsparkSession)
    engine.put("clickpulseStatement", statement)
    engine.put("clickpulseProperties", java_properties)
    engine.eval(
        "var Callable = Java.type('java.util.concurrent.Callable');\n"
        "var CommitMetadata = Java.type('org.apache.iceberg.spark.CommitMetadata');\n"
        "var RuntimeException = Java.type('java.lang.RuntimeException');\n"
        "var merge = new Callable({ call: function() {\n"
        "  clickpulseSpark.sql(clickpulseStatement);\n"
        "  return null;\n"
        "} });\n"
        "CommitMetadata.withCommitProperties("
        "clickpulseProperties, merge, RuntimeException.class);"
    )


def _require_batch_snapshot_marker(summary: Mapping[str, str], batch_id: str) -> None:
    if summary.get("clickpulse.batch_id") != batch_id:
        raise RuntimeError(
            "Iceberg committed a new snapshot without the expected ClickPulse batch marker"
        )


def _execute_materialized_upsert(
    session: Any,
    source: Any,
    details: Any,
    *,
    table_id: str,
    batch_id: str | None,
) -> SparkUpsertResult:
    """Commit source rows using metrics from the already-materialized decisions."""
    from pyspark.sql import functions as F

    counts = details.agg(
        F.sum(F.when(F.col("operation") == "INSERT", 1).otherwise(0)).alias("insert_count"),
        F.sum(F.when(F.col("operation") == "UPDATE", 1).otherwise(0)).alias("update_count"),
        F.sum(F.when(F.col("operation") == "DROP", 1).otherwise(0)).alias("drop_count"),
    ).first()
    metrics = SparkUpsertMetrics(
        insert_count=int(counts.insert_count or 0),
        update_count=int(counts.update_count or 0),
        drop_count=int(counts.drop_count or 0),
    )

    source_view = f"clickpulse_incoming_{uuid.uuid4().hex}"
    source.createOrReplaceTempView(source_view)
    columns = (
        "event_id",
        "user_id",
        "session_id",
        "event_type",
        "product_id",
        "category",
        "price",
        "quantity",
        "device",
        "event_time",
        "updated_at",
        "event_date",
    )
    assignments = ", ".join(f"t.{column} = s.{column}" for column in columns)
    insert_columns = ", ".join(columns)
    insert_values = ", ".join(f"s.{column}" for column in columns)
    previous_snapshot = (
        session.sql(
            f"SELECT snapshot_id FROM {table_id}.snapshots "
            "ORDER BY committed_at DESC LIMIT 1"
        )
        .first()
    )
    previous_snapshot_id = str(previous_snapshot.snapshot_id) if previous_snapshot else None
    marker_values = {
        "clickpulse.batch_id": batch_id,
        "clickpulse.insert_count": str(metrics.insert_count),
        "clickpulse.update_count": str(metrics.update_count),
        "clickpulse.drop_count": str(metrics.drop_count),
    }
    merge_statement = (
        f"MERGE INTO {table_id} AS t USING {source_view} AS s "
        "ON t.event_id = s.event_id "
        f"WHEN MATCHED AND s.event_time > t.event_time THEN UPDATE SET {assignments} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
    )
    try:
        if batch_id is None:
            session.sql(merge_statement)
        else:
            _execute_sql_with_snapshot_properties(session, merge_statement, marker_values)
    finally:
        session.catalog.dropTempView(source_view)

    current_state = session.table(table_id)
    duplicate_after_merge = (
        current_state.groupBy("event_id")
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate_after_merge:
        raise RuntimeError("Iceberg MERGE completed but duplicate event IDs were detected")

    snapshot_id = None
    committed_at = None
    snapshots = session.table(f"{table_id}.snapshots")
    latest_snapshot = snapshots.orderBy(F.col("committed_at").desc()).first()
    batch_snapshot = None
    if batch_id is not None:
        batch_snapshot = (
            snapshots.where(
                F.col("summary").getItem("clickpulse.batch_id") == batch_id
            )
            .orderBy(F.col("committed_at").desc())
            .first()
        )
    if batch_snapshot is not None:
        snapshot_id = str(batch_snapshot.snapshot_id)
        committed_at = batch_snapshot.committed_at.replace(tzinfo=timezone.utc)
    elif latest_snapshot and str(latest_snapshot.snapshot_id) != previous_snapshot_id:
        if batch_id is not None:
            _require_batch_snapshot_marker(latest_snapshot.summary, batch_id)
        snapshot_id = str(latest_snapshot.snapshot_id)
        committed_at = latest_snapshot.committed_at.replace(tzinfo=timezone.utc)
    return SparkUpsertResult(
        current_state=current_state,
        operation_details=details,
        metrics=metrics,
        iceberg_snapshot_id=snapshot_id,
        iceberg_committed_at=committed_at,
    )


def run_upsert(
    incoming_records: Any,
    *,
    spark: Any | None = None,
    unresolved_ties: Any | None = None,
    batch_id: str | None = None,
) -> SparkUpsertResult:
    """Merge Spark winners into the configured Iceberg current-state table.

    This is the distributed execution path. It never calls ``apply_upsert`` or
    collects event rows into Python. ``incoming_records`` is the winners
    DataFrame returned by Spark deduplication. Supplying unresolved tie IDs is
    supported defensively; those IDs are excluded before any table operation.
    """
    from pyspark.sql import functions as F

    from clickpulse.spark.runtime import create_iceberg_spark_session, runtime_config

    session = spark if spark is not None else create_iceberg_spark_session("clickpulse-upsert")
    config = runtime_config()
    _validate_iceberg_session(session, config.catalog)
    table_id = _table_identifier(config.catalog, config.namespace, config.table)
    _ensure_iceberg_table(session, table_id)

    winners = incoming_records
    if hasattr(incoming_records, "winners"):
        if unresolved_ties is None:
            unresolved_ties = incoming_records.unresolved_ties
        winners = incoming_records.winners
    if unresolved_ties is not None:
        winners = winners.join(
            unresolved_ties.select("event_id").dropDuplicates(["event_id"]),
            on="event_id",
            how="left_anti",
        )

    source = _parse_incoming_records(session, winners)
    duplicate_incoming = (
        source.groupBy("event_id")
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate_incoming:
        raise ValueError("Incoming Spark winners contain duplicate event IDs")

    existing = session.table(table_id)
    duplicate_existing = (
        existing.groupBy("event_id")
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate_existing:
        raise ValueError("Iceberg curated table already contains duplicate event IDs")

    source_alias = source.alias("s")
    target_alias = existing.select("event_id", "event_time").alias("t")
    comparison = source_alias.join(
        target_alias,
        F.col("s.event_id") == F.col("t.event_id"),
        "left",
    )
    operation = (
        F.when(F.col("t.event_id").isNull(), F.lit("INSERT"))
        .when(F.col("s.event_time") > F.col("t.event_time"), F.lit("UPDATE"))
        .otherwise(F.lit("DROP"))
    )
    details = comparison.select(
        F.col("s.event_id").alias("event_id"),
        operation.alias("operation"),
        F.when(F.col("t.event_id").isNull(), F.lit("event_id absent from current state"))
        .when(F.col("s.event_time") > F.col("t.event_time"), F.lit("incoming event_time is later"))
        .when(
            F.col("s.event_time") == F.col("t.event_time"),
            F.lit("incoming event_time equals current event_time"),
        )
        .otherwise(F.lit("incoming event_time is earlier"))
        .alias("reason"),
    ).orderBy("event_id").localCheckpoint(eager=True)
    try:
        # Eager localCheckpoint materializes the pre-MERGE join and truncates
        # its lineage, so later reads cannot resolve the Iceberg table again.
        # Both metrics and the returned audit consume this checkpoint.
        return _execute_materialized_upsert(
            session,
            source,
            details,
            table_id=table_id,
            batch_id=batch_id,
        )
    except BaseException:
        details.unpersist(blocking=True)
        raise
