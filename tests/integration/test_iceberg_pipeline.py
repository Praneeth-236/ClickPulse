"""VM3 integration tests for the configured Spark/Iceberg runtime.

Run with CLICKPULSE_RUN_ICEBERG_INTEGRATION=1 and ICEBERG_RUNTIME_JAR set to the
Spark 3.3.4 Iceberg runtime jar. Without those settings the tests are skipped.
"""

import os
import shutil
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("CLICKPULSE_RUN_ICEBERG_INTEGRATION") != "1"
    or not os.environ.get("ICEBERG_RUNTIME_JAR"),
    reason="requires explicitly configured Spark 3.3.4 and Iceberg runtime jar",
)


def test_day1_day2_iceberg_upsert_replay_and_commit_recovery(tmp_path, monkeypatch):
    from pyspark.sql import functions as F

    from clickpulse.spark.pipeline import (
        _configured_catalog_table_exists,
        _find_curated_commit,
        process_batch,
    )
    from clickpulse.spark.runtime import create_iceberg_spark_session
    from clickpulse.spark.upsert_curated import _parse_incoming_records
    from clickpulse.storage.rejected_events import (
        IcebergRejectedEventsStore,
        rejected_occurrence_id_dataframe,
    )

    repo = Path(__file__).resolve().parents[2]
    day1_source = repo / "data" / "sample" / "clicks_day1.jsonl"
    day2_source = repo / "data" / "sample" / "clicks_day2.jsonl"
    assert day1_source.is_file() and day2_source.is_file()

    day1 = tmp_path / day1_source.name
    day2 = tmp_path / day2_source.name
    shutil.copyfile(day1_source, day1)
    shutil.copyfile(day2_source, day2)

    catalog = "clickpulse_test_catalog"
    namespace = "acceptance"
    table = "clickstream_events"
    rejected_table = "rejected_events"
    warehouse = (tmp_path / "warehouse").resolve().as_uri()
    monkeypatch.setenv("SPARK_MASTER", "local[2]")
    monkeypatch.setenv("ICEBERG_CATALOG", catalog)
    monkeypatch.setenv("ICEBERG_CATALOG_TYPE", "hadoop")
    monkeypatch.setenv("ICEBERG_DATABASE", namespace)
    monkeypatch.setenv("ICEBERG_TABLE", table)
    monkeypatch.setenv("ICEBERG_REJECTED_TABLE", rejected_table)
    monkeypatch.setenv("ICEBERG_LEDGER_TABLE", "batch_ledger")
    monkeypatch.setenv("ICEBERG_WAREHOUSE", warehouse)
    monkeypatch.setenv("SPARK_DRIVER_HOST", "127.0.0.1")
    monkeypatch.setenv("SPARK_DRIVER_BIND_ADDRESS", "127.0.0.1")

    spark = create_iceberg_spark_session("clickpulse-iceberg-acceptance")
    previous_dates = None
    try:
        assert spark.version == "3.3.4"
        table_id = f"{catalog}.{namespace}.{table}"
        rejected_table_id = f"{catalog}.{namespace}.{rejected_table}"
        assert not _configured_catalog_table_exists(
            spark, f"`{catalog}`.`{namespace}`.`{table}`"
        )
        assert not _configured_catalog_table_exists(
            spark, f"`{catalog}`.`{namespace}`.`{rejected_table}`"
        )
        assert not _configured_catalog_table_exists(
            spark, f"`{catalog}`.`{namespace}`.`batch_ledger`"
        )

        day1_batch_ids = []

        def fail_after_commit(_batch_id):
            day1_batch_ids.append(_batch_id)
            raise RuntimeError("simulated process loss after Iceberg commit")

        original_rejected_persist = IcebergRejectedEventsStore.persist

        def fail_rejected_persist(_store, _rejected, _batch_id):
            raise RuntimeError("simulated rejected-event persistence failure")

        monkeypatch.setattr(IcebergRejectedEventsStore, "persist", fail_rejected_persist)
        with pytest.raises(RuntimeError, match="rejected-event persistence failure"):
            process_batch(str(day1), spark=spark)
        assert not _configured_catalog_table_exists(
            spark, f"`{catalog}`.`{namespace}`.`{table}`"
        )
        monkeypatch.setattr(IcebergRejectedEventsStore, "persist", original_rejected_persist)

        with pytest.raises(RuntimeError, match="simulated process loss"):
            process_batch(str(day1), spark=spark, after_curated_commit=fail_after_commit)

        assert len(day1_batch_ids) == 1
        day1_batch_id = day1_batch_ids[0]
        day1_commit_snapshot = (
            spark.table(f"{table_id}.snapshots")
            .where(F.col("summary").getItem("clickpulse.batch_id") == day1_batch_id)
            .orderBy(F.col("committed_at").desc())
            .first()
        )
        assert day1_commit_snapshot is not None
        assert day1_commit_snapshot.summary["clickpulse.batch_id"] == day1_batch_id
        assert day1_commit_snapshot.summary["clickpulse.insert_count"] == "116"
        assert day1_commit_snapshot.summary["clickpulse.update_count"] == "0"
        assert day1_commit_snapshot.summary["clickpulse.drop_count"] == "0"
        assert day1_commit_snapshot.operation
        recovered_marker = _find_curated_commit(spark, table_id, day1_batch_id)
        assert recovered_marker is not None
        assert recovered_marker[0].snapshot_id == str(day1_commit_snapshot.snapshot_id)
        assert recovered_marker[1] == {"insert_count": 116, "update_count": 0, "drop_count": 0}
        rejected_state = spark.table(rejected_table_id)
        assert rejected_state.count() == 16
        assert rejected_state.where(F.col("batch_id") == day1_batch_id).count() == 16

        # The durable ledger still says CURATED_COMMITTED is in progress. The
        # batch marker in the curated snapshot lets the next process recover it.
        recovered = process_batch(str(day1), spark=spark)
        assert recovered.replayed is True
        day1_state = spark.table(table_id).cache()
        assert day1_state.count() == 116
        assert day1_state.select("event_id").distinct().count() == 116
        # Detach the Day 1 dates from the Iceberg table before Day 2 MERGE can
        # invalidate caches or cause a later action to resolve the new snapshot.
        previous_dates = day1_state.select(
            "event_id", F.col("event_date").alias("previous_event_date")
        ).localCheckpoint(eager=True)

        day2_result = process_batch(str(day2), spark=spark)
        assert day2_result.upsert is not None
        assert day2_result.upsert.metrics.insert_count == 101
        assert day2_result.upsert.metrics.update_count == 12
        assert day2_result.upsert.metrics.drop_count == 12

        target = spark.table(table_id)
        assert target.count() == 217
        assert target.select("event_id").distinct().count() == 217

        operation_details = day2_result.upsert.operation_details
        operation_counts = {
            row.operation: row["count"]
            for row in operation_details.groupBy("operation").count().collect()
        }
        assert operation_counts == {"INSERT": 101, "UPDATE": 12, "DROP": 12}
        assert operation_counts == {
            "INSERT": day2_result.upsert.metrics.insert_count,
            "UPDATE": day2_result.upsert.metrics.update_count,
            "DROP": day2_result.upsert.metrics.drop_count,
        }
        update_ids = operation_details.where(F.col("operation") == "UPDATE").select(
            "event_id"
        )
        assert update_ids.count() == 12

        incoming = _parse_incoming_records(spark, day2_result.deduplication.winners)
        update_pairs = (
            incoming.join(update_ids, "event_id")
            .alias("i")
            .join(target.alias("t"), "event_id")
        )
        compared_fields = (
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
        mismatch = F.lit(False)
        for field in compared_fields:
            mismatch = mismatch | ~F.col(f"i.{field}").eqNullSafe(F.col(f"t.{field}"))
        assert update_pairs.where(mismatch).count() == 0
        assert (
            update_pairs.where(F.col("i.event_date") != F.col("t.event_date")).count()
            == 0
        )

        rejected_ids = (
            day2_result.validation.rejected_occurrences.select(
                F.get_json_object("raw_line", "$.event_id").alias("event_id")
            )
            .where(F.col("event_id").isNotNull())
            .join(day1_state.select("event_id"), "event_id", "inner")
            .select("event_id")
            .distinct()
        )
        assert rejected_ids.count() == 2
        before_invalid = (
            day1_state.join(rejected_ids, "event_id").orderBy("event_id").collect()
        )
        after_invalid = target.join(rejected_ids, "event_id").orderBy("event_id").collect()
        assert [row.asDict() for row in before_invalid] == [
            row.asDict() for row in after_invalid
        ]

        assert rejected_state.count() == 31
        assert rejected_state.where(F.col("batch_id") == day1_batch_id).count() == 16
        assert rejected_state.where(F.col("batch_id") == day2_result.batch_id).count() == 15
        day1_rejected_ids = rejected_state.where(
            F.col("batch_id") == day1_batch_id
        ).select("occurrence_id")
        day2_rejected_ids = rejected_state.where(
            F.col("batch_id") == day2_result.batch_id
        ).select("occurrence_id")
        assert day1_rejected_ids.intersect(day2_rejected_ids).count() == 0
        assert (
            rejected_state.groupBy("occurrence_id")
            .count()
            .where(F.col("count") > 1)
            .count()
            == 0
        )
        rejected_schema = {field.name: field for field in rejected_state.schema.fields}
        assert set(rejected_schema) == {
            "occurrence_id",
            "batch_id",
            "raw_line",
            "source_file",
            "source_line_number",
            "processing_timestamp",
            "rejected_at",
            "rejection_reasons",
        }
        assert all(
            not rejected_schema[name].nullable
            for name in (
                "occurrence_id",
                "batch_id",
                "raw_line",
                "source_file",
                "source_line_number",
                "processing_timestamp",
                "rejected_at",
                "rejection_reasons",
            )
        )

        expected_rejections = day2_result.validation.rejected_occurrences.select(
            "raw_line", "source_file", "source_line_number", "rejection_reasons"
        )
        stored_rejections = rejected_state.where(
            F.col("batch_id") == day2_result.batch_id
        ).select("raw_line", "source_file", "source_line_number", "rejection_reasons")
        assert expected_rejections.exceptAll(stored_rejections).count() == 0
        assert stored_rejections.exceptAll(expected_rejections).count() == 0

        ids_first_pass = [
            row.occurrence_id
            for row in rejected_occurrence_id_dataframe(
                day2_result.validation.rejected_occurrences, day2_result.batch_id
            )
            .select("occurrence_id")
            .orderBy("occurrence_id")
            .collect()
        ]
        ids_second_pass = [
            row.occurrence_id
            for row in rejected_occurrence_id_dataframe(
                day2_result.validation.rejected_occurrences, day2_result.batch_id
            )
            .select("occurrence_id")
            .orderBy("occurrence_id")
            .collect()
        ]
        assert ids_first_pass == ids_second_pass

        # The acceptance dataset moves each updated row into a different UTC date.
        assert update_pairs.where(F.col("i.event_date") != F.col("t.event_date")).count() == 0
        moved_updates = (
            target.join(update_ids, "event_id")
            .join(previous_dates, "event_id")
            .where(F.col("event_date") != F.col("previous_event_date"))
        )
        assert moved_updates.count() == 12
        previous_dates.unpersist()
        previous_dates = None
        day2_result.release()

        snapshot_count = spark.table(f"{table_id}.snapshots").count()
        replay = process_batch(str(day2), spark=spark)
        assert replay.replayed is True
        assert spark.table(f"{table_id}.snapshots").count() == snapshot_count
        assert spark.table(table_id).count() == 217
        assert spark.table(table_id).groupBy("event_id").count().where("count > 1").count() == 0

        from clickpulse.storage.batch_ledger import BatchStage
        from clickpulse.storage.iceberg_batch_ledger import batch_record_from_json
        from scripts.backfill_rejected_events import backfill_rejected_events

        ledger_id = f"{catalog}.{namespace}.batch_ledger"
        ledger_before = [
            row.asDict()
            for row in spark.table(ledger_id)
            .select("batch_id", "record_json")
            .orderBy("batch_id")
            .collect()
        ]
        assert len(ledger_before) == 2
        assert all(
            batch_record_from_json(row["record_json"]).last_completed_stage
            is BatchStage.COMPLETED
            for row in ledger_before
        )
        curated_count_before_backfill = spark.table(table_id).count()
        curated_snapshot_count_before_backfill = spark.table(f"{table_id}.snapshots").count()
        rejected_snapshot_count_before_backfill = spark.table(
            f"{rejected_table_id}.snapshots"
        ).count()

        first_backfill = backfill_rejected_events([str(day1), str(day2)], spark=spark)
        second_backfill = backfill_rejected_events([str(day1), str(day2)], spark=spark)
        assert [item.rejected_occurrence_count for item in first_backfill] == [16, 15]
        assert [item.inserted_occurrence_count for item in first_backfill] == [0, 0]
        assert [item.rejected_occurrence_count for item in second_backfill] == [16, 15]
        assert [item.inserted_occurrence_count for item in second_backfill] == [0, 0]
        assert spark.table(rejected_table_id).count() == 31
        assert spark.table(table_id).count() == curated_count_before_backfill == 217
        assert (
            spark.table(f"{table_id}.snapshots").count()
            == curated_snapshot_count_before_backfill
        )
        assert (
            spark.table(f"{rejected_table_id}.snapshots").count()
            == rejected_snapshot_count_before_backfill
        )
        ledger_after = [
            row.asDict()
            for row in spark.table(ledger_id)
            .select("batch_id", "record_json")
            .orderBy("batch_id")
            .collect()
        ]
        assert ledger_after == ledger_before

        # A later commit marked with a different batch identity must not be
        # mistaken for the recovered Day 1 commit.
        (
            target.limit(1)
            .withColumn("event_id", F.concat(F.lit("marker-probe-"), F.col("event_id")))
            .writeTo(table_id)
            .option("snapshot-property.clickpulse.batch_id", "unrelated-batch")
            .append()
        )
        unrelated_snapshot = (
            spark.table(f"{table_id}.snapshots")
            .where(F.col("summary").getItem("clickpulse.batch_id") == "unrelated-batch")
            .orderBy(F.col("committed_at").desc())
            .first()
        )
        assert unrelated_snapshot is not None
        assert str(unrelated_snapshot.snapshot_id) != str(day1_commit_snapshot.snapshot_id)
        day1_commit_after_unrelated = _find_curated_commit(spark, table_id, day1_batch_id)
        assert day1_commit_after_unrelated is not None
        assert day1_commit_after_unrelated[0].snapshot_id == str(day1_commit_snapshot.snapshot_id)
        day1_state.unpersist()
    finally:
        if previous_dates is not None:
            previous_dates.unpersist()
        spark.stop()
