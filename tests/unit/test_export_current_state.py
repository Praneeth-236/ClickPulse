import pytest

from clickpulse.druid.export_current_state import (
    _promote_staging_export,
    _validate_export_counts,
)


@pytest.mark.parametrize(
    ("record_count", "null_count", "duplicate_count", "message"),
    [
        (0, 0, 0, "empty"),
        (4, 1, 0, "null event_id"),
        (4, 0, 2, "duplicate event_id"),
    ],
)
def test_current_state_export_rejects_invalid_source_counts(
    record_count, null_count, duplicate_count, message
):
    with pytest.raises(ValueError, match=message):
        _validate_export_counts(record_count, null_count, duplicate_count)


def test_current_state_export_accepts_nonempty_unique_source():
    _validate_export_counts(3, 0, 0)


class FakeFileSystem:
    def __init__(self, contents, fail_rename_source=None):
        self.contents = contents
        self.fail_rename_source = fail_rename_source

    def exists(self, path):
        return path in self.contents

    def rename(self, source, destination):
        if source == self.fail_rename_source or source not in self.contents:
            return False
        self.contents[destination] = self.contents.pop(source)
        return True

    def delete(self, path, recursive):
        del recursive
        self.contents.pop(path, None)
        return True


def test_successful_export_promotion_replaces_previous_directory_and_removes_backup():
    fs = FakeFileSystem({"destination": "old", "staging": "new"})

    _promote_staging_export(fs, "staging", "destination", "backup")

    assert fs.contents == {"destination": "new"}


def test_failed_export_promotion_restores_previous_directory():
    fs = FakeFileSystem(
        {"destination": "old", "staging": "new"}, fail_rename_source="staging"
    )

    with pytest.raises(RuntimeError, match="Could not promote"):
        _promote_staging_export(fs, "staging", "destination", "backup")

    assert fs.contents == {"destination": "old", "staging": "new"}
