"""Regression tests for Iceberg commit lookup during batch recovery."""

from unittest.mock import MagicMock

import pytest

from clickpulse.spark.pipeline import (
    _configured_catalog_table_exists,
    _find_curated_commit,
)


def spark_with_catalog_table_exists(result=False):
    spark = MagicMock()
    spark._jvm.java.lang.reflect.Array.newInstance.return_value = "namespace-array"
    catalog = spark._jsparkSession.sessionState().catalogManager().catalog(
        "clickpulse_test_catalog"
    )
    catalog.tableExists.return_value = result
    return spark, catalog


def test_missing_curated_table_has_no_recovered_commit_and_does_not_read_metadata():
    spark, catalog = spark_with_catalog_table_exists(False)

    result = _find_curated_commit(
        spark,
        "`clickpulse_test_catalog`.`acceptance`.`clickstream_events`",
        "batch-sha256",
    )

    assert result is None
    catalog.tableExists.assert_called_once()
    spark.table.assert_not_called()


def test_table_existence_uses_configured_v2_catalog_identifier():
    spark, catalog = spark_with_catalog_table_exists(True)

    assert _configured_catalog_table_exists(
        spark,
        "`clickpulse_test_catalog`.`acceptance`.`clickstream_events`",
    )

    jvm = spark._jvm
    jvm.java.lang.reflect.Array.newInstance.assert_called_once_with(
        jvm.java.lang.String, 1
    )
    jvm.java.lang.reflect.Array.set.assert_called_once_with(
        "namespace-array", 0, "acceptance"
    )
    identifier = jvm.org.apache.spark.sql.connector.catalog.Identifier.of.return_value
    jvm.org.apache.spark.sql.connector.catalog.Identifier.of.assert_called_once_with(
        "namespace-array", "clickstream_events"
    )
    catalog.tableExists.assert_called_once_with(identifier)


def test_catalog_and_permission_errors_are_not_treated_as_missing_tables():
    spark, catalog = spark_with_catalog_table_exists()
    catalog.tableExists.side_effect = PermissionError("catalog access denied")

    with pytest.raises(PermissionError, match="catalog access denied"):
        _find_curated_commit(
            spark,
            "`clickpulse_test_catalog`.`acceptance`.`clickstream_events`",
            "batch-sha256",
        )
    spark.table.assert_not_called()


def test_table_identifier_must_be_fully_qualified():
    spark = MagicMock()
    with pytest.raises(ValueError, match="catalog.namespace.table"):
        _configured_catalog_table_exists(spark, "acceptance.clickstream_events")
    spark._jsparkSession.assert_not_called()
