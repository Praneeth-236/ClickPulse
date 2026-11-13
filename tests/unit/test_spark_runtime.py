from clickpulse.common.constants import ICEBERG_REJECTED_TABLE
from clickpulse.spark.runtime import runtime_config


def test_rejected_events_table_is_configurable_with_shared_default(monkeypatch):
    monkeypatch.delenv("ICEBERG_REJECTED_TABLE", raising=False)
    assert runtime_config().rejected_table == ICEBERG_REJECTED_TABLE

    monkeypatch.setenv("ICEBERG_REJECTED_TABLE", "rejected_archive")
    assert runtime_config().rejected_table == "rejected_archive"
