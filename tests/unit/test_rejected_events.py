from types import SimpleNamespace

import pytest
from pyspark.sql.types import (
    ArrayType,
    StringType,
    TimestampType,
)

from clickpulse.storage.rejected_events import rejected_occurrence_id_dataframe


def test_rejected_occurrence_id_requires_canonical_batch_sha256():
    for batch_id in ("not-a-digest", "A" * 64, "a" * 63, "a" * 65):
        with pytest.raises(ValueError, match="lowercase SHA-256"):
            rejected_occurrence_id_dataframe(None, batch_id)


def test_rejected_occurrence_id_requires_existing_validation_schema():
    frame = SimpleNamespace(schema=SimpleNamespace(fields=[]))

    with pytest.raises(ValueError, match="missing columns"):
        rejected_occurrence_id_dataframe(frame, "a" * 64)


def test_rejected_occurrence_id_rejects_type_mismatch_without_coercion():
    expected_fields = [
        SimpleNamespace(name="raw_line", dataType=StringType()),
        SimpleNamespace(name="source_file", dataType=StringType()),
        SimpleNamespace(name="source_line_number", dataType=StringType()),
        SimpleNamespace(name="processing_timestamp", dataType=TimestampType()),
        SimpleNamespace(
            name="rejection_reasons",
            dataType=ArrayType(StringType(), containsNull=False),
        ),
    ]
    frame = SimpleNamespace(schema=SimpleNamespace(fields=expected_fields))

    with pytest.raises(TypeError, match="incompatible field types"):
        rejected_occurrence_id_dataframe(frame, "a" * 64)
