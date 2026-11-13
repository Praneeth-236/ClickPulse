"""Tests for backend-independent Spark upsert guards."""

from clickpulse.spark.pipeline import _source_identity
from clickpulse.spark.upsert_curated import _price_fits_decimal_38_18


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
