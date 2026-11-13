from clickpulse.common.constants import ICEBERG_REJECTED_TABLE
from clickpulse.spark.runtime import runtime_config


def test_rejected_events_table_is_configurable_with_shared_default(monkeypatch):
    monkeypatch.delenv("ICEBERG_REJECTED_TABLE", raising=False)
    assert runtime_config().rejected_table == ICEBERG_REJECTED_TABLE

    monkeypatch.setenv("ICEBERG_REJECTED_TABLE", "rejected_archive")
    assert runtime_config().rejected_table == "rejected_archive"


def test_current_state_export_path_uses_hdfs_uri_by_default(monkeypatch):
    monkeypatch.delenv("CLICKPULSE_EXPORT_PATH", raising=False)
    monkeypatch.setenv("HDFS_NAMENODE_URI", "hdfs://namenode.example:9000")
    assert (
        runtime_config().export_path
        == "hdfs://namenode.example:9000/clickpulse/exports/current-state"
    )


def test_current_state_export_path_can_be_overridden(monkeypatch):
    monkeypatch.setenv("CLICKPULSE_EXPORT_PATH", "hdfs://other:9000/exports/current")
    assert runtime_config().export_path == "hdfs://other:9000/exports/current"
