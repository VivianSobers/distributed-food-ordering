import json
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

from foodpipe.schema import METRICS_FIELDS, NUMERIC_FIELDS, ORDER_FIELDS, TIMESTAMP_FIELDS


def _round_metric(value):
    return float(Decimal(repr(value)).quantize(Decimal("0.01"), ROUND_HALF_UP))


def reference_order_metrics(path) -> list[dict]:
    """Build cleaned order metrics independently from a JSONL input file."""
    results = []
    seen = set()
    timestamp_format = "%Y-%m-%d %H:%M:%S"
    with open(path, encoding="utf-8") as source:
        for line in source:
            try:
                record = json.loads(line)
            except (ValueError, TypeError):
                continue
            if not isinstance(record, dict):
                continue
            if any(field not in record or record[field] is None for field in ORDER_FIELDS):
                continue
            if any(
                not isinstance(record[field], str) or not record[field].strip()
                for field in ORDER_FIELDS if field not in NUMERIC_FIELDS
            ):
                continue
            if any(type(record[field]) not in (int, float) for field in NUMERIC_FIELDS):
                continue
            try:
                numbers = {field: float(record[field]) for field in NUMERIC_FIELDS}
            except (ValueError, OverflowError):
                continue
            if any(not Decimal(repr(value)).is_finite() for value in numbers.values()):
                continue
            if numbers["distance_km"] <= 0 or numbers["order_amount"] <= 0:
                continue
            if record["status"] != "DELIVERED":
                continue
            try:
                timestamps = {
                    field: datetime.strptime(record[field], timestamp_format).replace(tzinfo=timezone.utc)
                    for field in TIMESTAMP_FIELDS
                }
            except ValueError:
                continue
            if any(timestamps[field].strftime(timestamp_format) != record[field]
                   for field in TIMESTAMP_FIELDS):
                continue
            ordered = timestamps["order_timestamp"]
            accepted = timestamps["accepted_timestamp"]
            picked_up = timestamps["picked_up_timestamp"]
            delivered = timestamps["delivered_timestamp"]
            if not ordered <= accepted <= picked_up < delivered:
                continue

            cleaned = {field: numbers[field] if field in NUMERIC_FIELDS else record[field]
                       for field in ORDER_FIELDS}
            identity = tuple(cleaned[field] for field in ORDER_FIELDS)
            if identity in seen:
                continue
            seen.add(identity)

            preparation = (picked_up - accepted).total_seconds() / 60.0
            delivery = (delivered - picked_up).total_seconds() / 60.0
            fulfillment = (delivered - ordered).total_seconds() / 60.0
            speed = numbers["distance_km"] / (delivery / 60.0)
            delay = max(0.0, (delivered - timestamps["expected_delivery_timestamp"]).total_seconds() / 60.0)
            if delay == 0:
                category = "ON_TIME"
            elif delay <= 15:
                category = "SLIGHT_DELAY"
            elif delay <= 30:
                category = "MODERATE_DELAY"
            else:
                category = "SEVERE_DELAY"
            cleaned.update({
                "preparation_time_min": _round_metric(preparation),
                "delivery_time_min": _round_metric(delivery),
                "fulfillment_time_min": _round_metric(fulfillment),
                "delivery_speed_kmph": _round_metric(speed),
                "delay_min": _round_metric(delay),
                "delivery_status": "ON_TIME" if delay == 0 else "DELAYED",
                "delay_category": category,
            })
            results.append({field: cleaned[field] for field in METRICS_FIELDS})
    return results
