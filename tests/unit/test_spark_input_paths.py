"""Path normalization and occurrence provenance tests for Spark JSONL reads."""

from clickpulse.spark.validate_and_deduplicate import (
    _normalize_spark_input_path,
    read_jsonl_occurrences,
)


def test_linux_absolute_local_path_uses_file_uri_with_empty_authority():
    assert _normalize_spark_input_path("/tmp/example.jsonl") == "file:///tmp/example.jsonl"


def test_existing_file_uri_is_preserved_without_reencoding():
    path = "file:///tmp/example%20file.jsonl"
    assert _normalize_spark_input_path(path) == path


def test_hdfs_uri_is_preserved_exactly():
    path = "hdfs://host:9000/path/example.jsonl"
    assert _normalize_spark_input_path(path) == path


def test_windows_absolute_path_becomes_a_valid_file_uri():
    assert (
        _normalize_spark_input_path(r"C:\clickpulse\raw\example file.jsonl")
        == "file:///C:/clickpulse/raw/example%20file.jsonl"
    )


class FakeRDD(list):
    def zipWithIndex(self):
        return FakeRDD((line, index) for index, line in enumerate(self))

    def map(self, function):
        return FakeRDD(function(item) for item in self)


class FakeSparkContext:
    def __init__(self):
        self.read_paths = []

    def textFile(self, path):
        self.read_paths.append(path)
        return FakeRDD(["first", "second"])

    @staticmethod
    def union(rdds):
        return FakeRDD(row for rdd in rdds for row in rdd)


class FakeSpark:
    def __init__(self):
        self.sparkContext = FakeSparkContext()

    @staticmethod
    def createDataFrame(rows, schema):
        return list(rows), schema


def test_read_preserves_source_labels_and_per_file_line_numbers():
    spark = FakeSpark()
    paths = [
        "/tmp/example.jsonl",
        "file:///tmp/other.jsonl",
        "hdfs://host:9000/path/example.jsonl",
        r"C:\clickpulse\raw\windows.jsonl",
    ]

    rows, _schema = read_jsonl_occurrences(spark, paths)

    assert spark.sparkContext.read_paths == [
        "file:///tmp/example.jsonl",
        "file:///tmp/other.jsonl",
        "hdfs://host:9000/path/example.jsonl",
        "file:///C:/clickpulse/raw/windows.jsonl",
    ]
    assert rows == [
        (line, source, line_number)
        for source in paths
        for line_number, line in enumerate(("first", "second"), start=1)
    ]
