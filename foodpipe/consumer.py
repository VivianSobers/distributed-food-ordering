"""Land Kafka message values in a local file for batch processing."""

import argparse
from pathlib import Path
import sys
import time

from kafka import KafkaConsumer

from foodpipe.config import bootstrap_servers, load_env


def consume_to_file(consumer, out_path, idle_timeout, max_wait, clock=time.monotonic) -> dict[int, int]:
    """Write messages until the idle timeout or the initial wait expires."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    counts = {}
    started = clock()
    last_message = None
    with out_path.open("wb") as output:
        while True:
            batches = consumer.poll(timeout_ms=1000)
            received = False
            for messages in batches.values():
                for message in messages:
                    output.write(message.value + b"\n")
                    output.flush()
                    counts[message.partition] = counts.get(message.partition, 0) + 1
                    last_message = clock()
                    received = True
            if not received:
                if last_message is None:
                    if clock() - started >= max_wait:
                        break
                elif clock() - last_message >= idle_timeout:
                    break
    return counts


def main(argv=None):
    parser = argparse.ArgumentParser(description="Land Kafka records in a local file.")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--group", required=True)
    parser.add_argument("--bootstrap")
    parser.add_argument("--idle-timeout", type=float, default=15)
    parser.add_argument("--max-wait", type=float, default=300)
    args = parser.parse_args(argv)
    try:
        bootstrap = args.bootstrap or bootstrap_servers(load_env())
        consumer = KafkaConsumer(
            args.topic, group_id=args.group, auto_offset_reset="earliest",
            enable_auto_commit=True, bootstrap_servers=bootstrap,
        )
        try:
            counts = consume_to_file(consumer, args.out, args.idle_timeout, args.max_wait)
        finally:
            consumer.close()
    except Exception as error:
        print(f"Consumption failed: {error}", file=sys.stderr)
        return 1

    total = sum(counts.values())
    print(f"Consumed {total} records from {args.topic} -> {args.out}")
    for partition, count in sorted(counts.items()):
        print(f"partition {partition}: {count}")
    return 0 if total else 1


if __name__ == "__main__":
    sys.exit(main())
