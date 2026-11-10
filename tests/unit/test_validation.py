"""Unit tests for event validation."""

from clickpulse.common.validation import validate_event


def test_validation_is_not_silently_assumed_valid():
    # Replace this placeholder with contract-based valid/invalid test cases.
    try:
        validate_event({})
    except NotImplementedError:
        return
    raise AssertionError("Validation implementation should be added with its tests")
