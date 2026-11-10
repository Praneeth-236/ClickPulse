"""Shared names used across ClickPulse components."""

KAFKA_TOPIC = "retailmart_clickstream"

HDFS_RAW_PATH = "/clickpulse/raw/"
HDFS_REJECTED_PATH = "/clickpulse/rejected/"

ICEBERG_DATABASE = "retailmart"
ICEBERG_CURATED_TABLE = "clickstream_events"
ICEBERG_REJECTED_TABLE = "rejected_events"

DRUID_DATASOURCE = "retailmart_clickstream"
AIRFLOW_DAG_ID = "clickpulse_pipeline"

VALID_EVENT_TYPES = {
    "page_view",
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "purchase",
}
