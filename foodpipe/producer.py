"""Publish non-blank file lines to a Kafka topic as raw bytes."""

import argparse
import sys

from kafka import KafkaProducer

from foodpipe.config import bootstrap_servers, load_env


def produce_file(producer, topic, path) -> dict[int, int]:
    """Send file records and count acknowledged messages by partition."""
    counts = {}
    futures = []
    with open(path, "rb") as source:
        for line_number, line in enumerate(source, start=1):
            if not line.strip():
                continue
            value = line.removesuffix(b"\r\n") if line.endswith(b"\r\n") else (
                line.removesuffix(b"\n")
            )
            future = producer.send(topic, value=value, key=str(line_number).encode())
            futures.append(future)
    producer.flush()
    for future in futures:
        partition = future.get(timeout=60).partition
        counts[partition] = counts.get(partition, 0) + 1
    return counts


def main(argv=None):
    parser = argparse.ArgumentParser(description="Publish file records to Kafka.")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--file", required=True)
    parser.add_argument("--bootstrap")
    args = parser.parse_args(argv)
    try:
        bootstrap = args.bootstrap or bootstrap_servers(load_env())
        producer = KafkaProducer(
            bootstrap_servers=bootstrap, acks="all", retries=5, linger_ms=10,
        )
        try:
            counts = produce_file(producer, args.topic, args.file)
        finally:
            producer.close()
    except Exception as error:
        print(f"Production failed: {error}", file=sys.stderr)
        return 1

    print(f"Produced {sum(counts.values())} records to {args.topic}")
    for partition, count in sorted(counts.items()):
        print(f"partition {partition}: {count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
