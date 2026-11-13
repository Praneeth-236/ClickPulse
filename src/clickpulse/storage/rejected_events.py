"""Idempotent persistence of rejected ClickPulse input occurrences to Iceberg."""

import re
import uuid
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RejectedEventWriteResult:
    input_occurrence_count: int
    inserted_occurrence_count: int


def rejected_occurrence_id_dataframe(rejected_occurrences: Any, batch_id: str):
    """Add a deterministic SHA-256 ID derived from structured occurrence identity."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        ArrayType,
        LongType,
        StringType,
        TimestampType,
    )

    if not isinstance(batch_id, str) or re.fullmatch(r"[0-9a-f]{64}", batch_id) is None:
        raise ValueError("batch_id must be a lowercase SHA-256 digest")

    expected_types = {
        "raw_line": StringType(),
        "source_file": StringType(),
        "source_line_number": LongType(),
        "processing_timestamp": TimestampType(),
        "rejection_reasons": ArrayType(StringType(), containsNull=False),
    }
    fields = {field.name: field for field in rejected_occurrences.schema.fields}
    missing = set(expected_types).difference(fields)
    if missing:
        raise ValueError(f"Rejected occurrence DataFrame is missing columns: {sorted(missing)}")
    wrong_types = {
        name: (fields[name].dataType, expected_type)
        for name, expected_type in expected_types.items()
        if fields[name].dataType != expected_type
    }
    if wrong_types:
        raise TypeError(
            "Rejected occurrence DataFrame has incompatible field types: "
            f"{wrong_types}"
        )

    identity_json = F.to_json(
        F.struct(
            F.lit(batch_id).alias("batch_id"),
            F.col("source_file").alias("source_file"),
            F.col("source_line_number").alias("source_line_number"),
        ),
        {"ignoreNullFields": "false"},
    )
    return rejected_occurrences.select(
        F.sha2(identity_json, 256).alias("occurrence_id"),
        F.lit(batch_id).alias("batch_id"),
        F.col("raw_line"),
        F.col("source_file"),
        F.col("source_line_number"),
        F.col("processing_timestamp"),
        F.col("processing_timestamp").alias("rejected_at"),
        F.col("rejection_reasons"),
    )


class IcebergRejectedEventsStore:
    """Persist rejected occurrences with insert-only, deterministic MERGE semantics.

    Writes are idempotent for sequential retries. The component does not
    serialize concurrent writers and makes no cross-table atomicity guarantee.
    """

    def __init__(self, spark: Any, catalog: str, namespace: str, table: str) -> None:
        from clickpulse.spark.upsert_curated import _table_identifier

        self.spark = spark
        self.identifier = _table_identifier(catalog, namespace, table)

    def _ensure_table(self) -> None:
        namespace_id = self.identifier.rsplit(".", maxsplit=1)[0]
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace_id}")
        self.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {self.identifier} ("
            "occurrence_id STRING NOT NULL, "
            "batch_id STRING NOT NULL, "
            "raw_line STRING NOT NULL, "
            "source_file STRING NOT NULL, "
            "source_line_number BIGINT NOT NULL, "
            "processing_timestamp TIMESTAMP NOT NULL, "
            "rejected_at TIMESTAMP NOT NULL, "
            "rejection_reasons ARRAY<STRING> NOT NULL"
            ") USING iceberg"
        )

    def persist(self, rejected_occurrences: Any, batch_id: str) -> RejectedEventWriteResult:
        """Insert each occurrence once, leaving previously stored rows unchanged."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            ArrayType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        self._ensure_table()
        projected = rejected_occurrence_id_dataframe(rejected_occurrences, batch_id)

        missing_values = projected.where(
            F.col("occurrence_id").isNull()
            | F.col("batch_id").isNull()
            | F.col("raw_line").isNull()
            | F.col("source_file").isNull()
            | F.col("source_line_number").isNull()
            | F.col("processing_timestamp").isNull()
            | F.col("rejected_at").isNull()
            | F.col("rejection_reasons").isNull()
            | (F.size("rejection_reasons") == 0)
        )
        if missing_values.limit(1).count():
            raise ValueError("Rejected occurrences contain null or empty required fields")

        required_schema = StructType(
            [
                StructField("occurrence_id", StringType(), nullable=False),
                StructField("batch_id", StringType(), nullable=False),
                StructField("raw_line", StringType(), nullable=False),
                StructField("source_file", StringType(), nullable=False),
                StructField("source_line_number", LongType(), nullable=False),
                StructField("processing_timestamp", TimestampType(), nullable=False),
                StructField("rejected_at", TimestampType(), nullable=False),
                StructField(
                    "rejection_reasons",
                    ArrayType(StringType(), containsNull=False),
                    nullable=False,
                ),
            ]
        )
        # Spark expressions widen nullability. Re-encode the distributed rows
        # to the Iceberg required schema and verify actual worker values.
        source = self.spark.createDataFrame(
            projected.rdd,
            schema=required_schema,
            verifySchema=True,
        )

        source_count = source.count()
        duplicate_source_ids = (
            source.groupBy("occurrence_id")
            .count()
            .where(F.col("count") > 1)
            .limit(1)
            .count()
        )
        if duplicate_source_ids:
            raise ValueError("Rejected occurrence input contains duplicate occurrence IDs")

        target = self.spark.table(self.identifier)
        duplicate_target_ids = (
            target.groupBy("occurrence_id")
            .count()
            .where(F.col("count") > 1)
            .limit(1)
            .count()
        )
        if duplicate_target_ids:
            raise RuntimeError("Rejected events table already contains duplicate occurrence IDs")

        existing_ids = target.select("occurrence_id")
        previously_stored = source.select("occurrence_id").join(
            existing_ids, on="occurrence_id", how="inner"
        ).count()
        source_view = f"clickpulse_rejected_{uuid.uuid4().hex}"
        source.createOrReplaceTempView(source_view)
        columns = (
            "occurrence_id",
            "batch_id",
            "raw_line",
            "source_file",
            "source_line_number",
            "processing_timestamp",
            "rejected_at",
            "rejection_reasons",
        )
        insert_columns = ", ".join(columns)
        insert_values = ", ".join(f"s.{column}" for column in columns)
        try:
            self.spark.sql(
                f"MERGE INTO {self.identifier} AS t USING {source_view} AS s "
                "ON t.occurrence_id = s.occurrence_id "
                f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) "
                f"VALUES ({insert_values})"
            )
        finally:
            self.spark.catalog.dropTempView(source_view)

        duplicate_ids_after_write = (
            self.spark.table(self.identifier)
            .groupBy("occurrence_id")
            .count()
            .where(F.col("count") > 1)
            .limit(1)
            .count()
        )
        if duplicate_ids_after_write:
            raise RuntimeError("Rejected events write produced duplicate occurrence IDs")

        return RejectedEventWriteResult(
            input_occurrence_count=source_count,
            inserted_occurrence_count=source_count - previously_stored,
        )
