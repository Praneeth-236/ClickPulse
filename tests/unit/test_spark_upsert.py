"""Tests for backend-independent Spark upsert guards."""

from clickpulse.spark.pipeline import _source_identity
from clickpulse.spark.upsert_curated import (
    SparkUpsertMetrics,
    SparkUpsertResult,
    _execute_sql_with_snapshot_properties,
    _price_fits_decimal_38_18,
    _require_batch_snapshot_marker,
)


def test_source_identity_handles_windows_paths_and_uris():
    windows_path = _source_identity(r"C:\clickpulse\raw\clicks_day1.jsonl")
    hdfs_path = _source_identity(
        "hdfs://172.22.184.125:9000/clickpulse/raw/clicks_day1.jsonl"
    )
    assert windows_path.source_name == "clicks_day1.jsonl"
    assert hdfs_path.source_name == "clicks_day1.jsonl"
    assert windows_path.source_uri == r"C:\clickpulse\raw\clicks_day1.jsonl"


def test_price_representation_guard_rejects_rounding_and_overflow():
    assert _price_fits_decimal_38_18("0")
    assert _price_fits_decimal_38_18("123.450000000000000000")
    assert _price_fits_decimal_38_18(None)
    assert not _price_fits_decimal_38_18("0.0000000000000000001")
    assert not _price_fits_decimal_38_18("100000000000000000000")
    assert not _price_fits_decimal_38_18("not-a-number")


def test_sql_snapshot_properties_use_iceberg_commit_metadata_on_jvm_thread():
    from unittest.mock import MagicMock

    spark = MagicMock()
    engine = spark._jvm.javax.script.ScriptEngineManager().getEngineByName.return_value
    java_properties = spark._jvm.java.util.HashMap.return_value

    _execute_sql_with_snapshot_properties(
        spark,
        "MERGE INTO catalog.ns.events USING incoming",
        {"clickpulse.batch_id": "abc123"},
    )

    engine.put.assert_any_call("clickpulseSpark", spark._jsparkSession)
    engine.put.assert_any_call("clickpulseStatement", "MERGE INTO catalog.ns.events USING incoming")
    java_properties.put.assert_called_once_with("clickpulse.batch_id", "abc123")
    script = engine.eval.call_args.args[0]
    assert "CommitMetadata.withCommitProperties" in script
    assert "clickpulseSpark.sql(clickpulseStatement)" in script
    assert "spark.sql.iceberg.snapshot-property" not in script


def test_snapshot_marker_mismatch_fails_clearly():
    import pytest

    with pytest.raises(RuntimeError, match="without the expected ClickPulse batch marker"):
        _require_batch_snapshot_marker({"clickpulse.batch_id": "another-batch"}, "expected")


def test_missing_java11_nashorn_fails_before_running_sql():
    from unittest.mock import MagicMock

    import pytest

    spark = MagicMock()
    spark._jvm.javax.script.ScriptEngineManager().getEngineByName.return_value = None

    with pytest.raises(RuntimeError, match="Java 11 Nashorn"):
        _execute_sql_with_snapshot_properties(
            spark, "MERGE INTO catalog.ns.events USING incoming", {"clickpulse.batch_id": "abc"}
        )

    spark._jsparkSession.sql.assert_not_called()


def test_spark_upsert_result_release_unpersists_operation_details():
    from unittest.mock import MagicMock

    details = MagicMock()
    result = SparkUpsertResult(
        current_state=MagicMock(),
        operation_details=details,
        metrics=SparkUpsertMetrics(insert_count=1, update_count=2, drop_count=3),
        iceberg_snapshot_id=None,
        iceberg_committed_at=None,
    )

    result.release(blocking=True)

    details.unpersist.assert_called_once_with(blocking=True)
