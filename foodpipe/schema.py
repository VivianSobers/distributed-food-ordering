from pyspark.sql.types import DoubleType, StringType, StructField, StructType


ORDER_FIELDS: list[str] = [
    "order_id",
    "customer_id",
    "restaurant_id",
    "delivery_partner_id",
    "city",
    "order_timestamp",
    "accepted_timestamp",
    "picked_up_timestamp",
    "expected_delivery_timestamp",
    "delivered_timestamp",
    "distance_km",
    "order_amount",
    "delivery_fee",
    "status",
]

TIMESTAMP_FIELDS: list[str] = [
    "order_timestamp",
    "accepted_timestamp",
    "picked_up_timestamp",
    "expected_delivery_timestamp",
    "delivered_timestamp",
]

NUMERIC_FIELDS: list[str] = ["distance_km", "order_amount", "delivery_fee"]
STRING_FIELDS: list[str] = [
    field for field in ORDER_FIELDS if field not in NUMERIC_FIELDS
]

ORDER_SCHEMA: StructType = StructType(
    [
        StructField(field, DoubleType() if field in NUMERIC_FIELDS else StringType())
        for field in ORDER_FIELDS
    ]
    + [StructField("_corrupt_record", StringType())]
)

COMPUTED_FIELDS: list[str] = [
    "preparation_time_min",
    "delivery_time_min",
    "fulfillment_time_min",
    "delivery_speed_kmph",
    "delay_min",
    "delivery_status",
    "delay_category",
]

METRICS_FIELDS: list[str] = ORDER_FIELDS + COMPUTED_FIELDS

METRICS_SCHEMA: StructType = StructType(
    [
        StructField(field.name, field.dataType, field.nullable)
        for field in ORDER_SCHEMA.fields[:-1]
    ]
    + [
        StructField(field, DoubleType() if field in COMPUTED_FIELDS[:5] else StringType())
        for field in COMPUTED_FIELDS
    ]
    + [StructField("_corrupt_record", StringType())]
)

TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"

RESTAURANT_SUMMARY_COLUMNS: list[str] = [
    "restaurant_id",
    "order_count",
    "avg_order_amount",
    "avg_preparation_time_min",
    "avg_delivery_time_min",
    "delayed_order_count",
    "delay_rate_pct",
]

CITY_SUMMARY_COLUMNS: list[str] = [
    "city",
    "order_count",
    "avg_order_amount",
    "avg_delivery_time_min",
    "delayed_order_count",
    "delay_rate_pct",
]
