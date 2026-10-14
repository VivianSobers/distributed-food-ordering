import pytest
from pyspark.sql.types import DoubleType, StringType, StructType

from foodpipe.schema import (
    CITY_SUMMARY_COLUMNS,
    COMPUTED_FIELDS,
    METRICS_FIELDS,
    METRICS_SCHEMA,
    NUMERIC_FIELDS,
    ORDER_FIELDS,
    ORDER_SCHEMA,
    RESTAURANT_SUMMARY_COLUMNS,
    STRING_FIELDS,
    TIMESTAMP_FIELDS,
    TIMESTAMP_FORMAT,
)


def test_order_fields_match_brief_order():
    assert ORDER_FIELDS == [
        "order_id", "customer_id", "restaurant_id", "delivery_partner_id",
        "city", "order_timestamp", "accepted_timestamp", "picked_up_timestamp",
        "expected_delivery_timestamp", "delivered_timestamp", "distance_km",
        "order_amount", "delivery_fee", "status",
    ]


def test_timestamp_fields_and_format():
    assert TIMESTAMP_FIELDS == [
        "order_timestamp", "accepted_timestamp", "picked_up_timestamp",
        "expected_delivery_timestamp", "delivered_timestamp",
    ]
    assert TIMESTAMP_FORMAT == "yyyy-MM-dd HH:mm:ss"


def test_order_field_groups():
    assert NUMERIC_FIELDS == ["distance_km", "order_amount", "delivery_fee"]
    assert STRING_FIELDS == [
        field for field in ORDER_FIELDS if field not in NUMERIC_FIELDS
    ]


def test_order_schema_names_and_types():
    assert isinstance(ORDER_SCHEMA, StructType)
    assert ORDER_SCHEMA.fieldNames() == ORDER_FIELDS + ["_corrupt_record"]
    for field in NUMERIC_FIELDS:
        assert isinstance(ORDER_SCHEMA[field].dataType, DoubleType)
    for field in STRING_FIELDS:
        assert isinstance(ORDER_SCHEMA[field].dataType, StringType)


@pytest.mark.parametrize("schema", [ORDER_SCHEMA, METRICS_SCHEMA])
def test_schema_ends_with_corrupt_record_string(schema):
    assert schema.fields[-1].name == "_corrupt_record"
    assert isinstance(schema.fields[-1].dataType, StringType)


def test_metrics_fields_extend_order_fields():
    assert COMPUTED_FIELDS == [
        "preparation_time_min", "delivery_time_min", "fulfillment_time_min",
        "delivery_speed_kmph", "delay_min", "delivery_status", "delay_category",
    ]
    assert METRICS_FIELDS == ORDER_FIELDS + COMPUTED_FIELDS


def test_metrics_schema_names_and_types():
    assert isinstance(METRICS_SCHEMA, StructType)
    assert METRICS_SCHEMA.fieldNames()[:-1] == METRICS_FIELDS
    for field in ORDER_FIELDS:
        assert METRICS_SCHEMA[field].dataType == ORDER_SCHEMA[field].dataType
    for field in COMPUTED_FIELDS[:5]:
        assert isinstance(METRICS_SCHEMA[field].dataType, DoubleType)
    for field in ["delivery_status", "delay_category"]:
        assert isinstance(METRICS_SCHEMA[field].dataType, StringType)


def test_restaurant_summary_columns():
    assert RESTAURANT_SUMMARY_COLUMNS == [
        "restaurant_id", "order_count", "avg_order_amount",
        "avg_preparation_time_min", "avg_delivery_time_min",
        "delayed_order_count", "delay_rate_pct",
    ]


def test_city_summary_columns():
    assert CITY_SUMMARY_COLUMNS == [
        "city", "order_count", "avg_order_amount", "avg_delivery_time_min",
        "delayed_order_count", "delay_rate_pct",
    ]
