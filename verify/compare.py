"""Compare pipeline output files with the independent dataset reference."""

import argparse
from collections import Counter
import csv
from decimal import Decimal, ROUND_HALF_UP
from itertools import zip_longest
import json
from pathlib import Path
import sys

from verify.reference import reference_order_metrics, reference_summaries


def _normalise(row):
    result = {}
    for key, value in row.items():
        if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
            number = Decimal(str(value))
            if not number.is_finite():
                raise ValueError(f"Non-finite number in {key}: {value}")
            value = float(number.quantize(Decimal("0.01"), ROUND_HALF_UP))
            if value == 0:
                value = 0.0
        result[key] = value
    return result


def _report(path, differences):
    for difference in differences[:10]:
        print(f"{path}: {difference}")
    if len(differences) > 10:
        print(f"{path}: {len(differences) - 10} further differences omitted")
    return not differences


def _compare_metrics(path, reference):
    actual = []
    with Path(path).open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, start=1):
            if not line.strip():
                continue
            row = json.loads(line)
            if not isinstance(row, dict):
                raise ValueError(f"Metrics line {line_number} must be a JSON object")
            actual.append(row)

    def serialise(rows):
        return sorted(json.dumps(_normalise(row), sort_keys=True, allow_nan=False) for row in rows)

    expected_rows, actual_rows = serialise(reference), serialise(actual)
    if expected_rows == actual_rows:
        return True
    expected_counts, actual_counts = Counter(expected_rows), Counter(actual_rows)
    differences = []
    for label, counts in (("missing", expected_counts - actual_counts),
                          ("extra", actual_counts - expected_counts)):
        for row, count in sorted(counts.items()):
            differences.append(f"{label} row (copies={count}): {row}")
    return _report(path, differences)


def _compare_csv(path, reference):
    columns = list(reference.columns)
    with Path(path).open(encoding="utf-8", newline="") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames != columns:
            return _report(path, [f"different columns: expected {columns}, actual {reader.fieldnames}"])
        actual = []
        for row_number, row in enumerate(reader, start=1):
            if None in row or any(value is None for value in row.values()):
                raise ValueError(f"CSV row {row_number} has the wrong number of fields")
            actual.append(_normalise({
                column: row[column] if column == columns[0] else Decimal(row[column])
                for column in columns
            }))
    expected = [_normalise(row) for row in reference.to_dict(orient="records")]
    differences = []
    missing = object()
    for number, (wanted, found) in enumerate(zip_longest(expected, actual, fillvalue=missing), start=1):
        if wanted is missing:
            differences.append(f"extra row {number}: {json.dumps(found, sort_keys=True)}")
        elif found is missing:
            differences.append(f"missing row {number}: {json.dumps(wanted, sort_keys=True)}")
        elif wanted != found:
            changes = {column: {"expected": wanted[column], "actual": found[column]}
                       for column in columns if wanted[column] != found[column]}
            differences.append(f"different row {number}: {json.dumps(changes, sort_keys=True)}")
    return _report(path, differences)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Compare pipeline outputs with the dataset reference.")
    for name in ("dataset", "metrics", "restaurant", "city"):
        parser.add_argument(f"--{name}", required=True)
    args = parser.parse_args(argv)
    try:
        metrics = reference_order_metrics(args.dataset)
        restaurants, cities = reference_summaries(metrics)
    except (OSError, ValueError, TypeError, ArithmeticError) as error:
        print(f"{args.dataset}: reference error: {error}")
        return 1

    matched = True
    for compare, path, reference in (
        (_compare_metrics, args.metrics, metrics),
        (_compare_csv, args.restaurant, restaurants),
        (_compare_csv, args.city, cities),
    ):
        try:
            file_matched = compare(path, reference)
        except (OSError, ValueError, TypeError, ArithmeticError) as error:
            print(f"{path}: comparison error: {error}")
            file_matched = False
        matched = file_matched and matched
    if matched:
        print("MATCH")
    return 0 if matched else 1


if __name__ == "__main__":
    sys.exit(main())
