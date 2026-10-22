<div align="center">

# Distributed Food Ordering & Analytics Pipeline

<p>
A four-machine pipeline for the Big Data 2026 assignment <em>Distributed Food Ordering System</em>. A two-broker <strong>Apache Kafka</strong> cluster in KRaft mode carries food-order records between the machines, which talk over a private <strong>ZeroTier</strong> network. <strong>Apache Spark</strong> batch jobs clean the orders, compute delivery metrics, and summarise performance by restaurant and by city.
</p>

![Course](https://img.shields.io/badge/Course-Big%20Data%202026%20%7C%20UE24CS343AB2-1565C0?style=for-the-badge&labelColor=42A5F5)
![Streaming](https://img.shields.io/badge/Streaming-Apache%20Kafka%203.9.2%20KRaft-6A1B9A?style=for-the-badge&labelColor=AB47BC)
![Processing](https://img.shields.io/badge/Processing-Apache%20Spark%203.3.4-E65100?style=for-the-badge&labelColor=FF9800)
![Network](https://img.shields.io/badge/Network-ZeroTier%20Private%20LAN-B8860B?style=for-the-badge&labelColor=FFD54F)
![Replication](https://img.shields.io/badge/Replication-RF%202%20%C2%B7%202%20Partitions-2E7D32?style=for-the-badge&labelColor=66BB6A)
![Failure Test](https://img.shields.io/badge/Failure%20Test-Broker%202%20Shutdown-B71C1C?style=for-the-badge&labelColor=EF5350)
![Cluster](https://img.shields.io/badge/Cluster-4%20Machines-512DA8?style=for-the-badge&labelColor=9575CD)
![Language](https://img.shields.io/badge/Language-Python%203.10-1C1C1C?style=for-the-badge&logo=python&logoColor=FFD700&labelColor=424242)
![Runtime](https://img.shields.io/badge/Runtime-Java%2011-00897B?style=for-the-badge&labelColor=26A69A)

<p>
  <a href="#overview">Overview</a> &bull;
  <a href="#system-architecture">Architecture</a> &bull;
  <a href="#cluster-topology">Topology</a> &bull;
  <a href="#data-pipeline">Pipeline</a> &bull;
  <a href="#analytics-preview">Outputs</a> &bull;
  <a href="#fault-tolerance">Failure Test</a> &bull;
  <a href="#repository-layout">Repository</a> &bull;
  <a href="#getting-started">Getting Started</a> &bull;
  <a href="#testing-verification">Testing</a> &bull;
  <a href="#engineering-highlights">Decisions</a> &bull;
  <a href="#team-members">Team</a>
</p>

</div>

> [!NOTE]
> **Status.** Every stage is built and tested. The whole pipeline, including the broker-failure test, has passed a full rehearsal on one laptop with two local brokers (`bash scripts/local_e2e.sh --down`). The four-machine run over ZeroTier is next; [RUNBOOK.md](RUNBOOK.md) lists the demo commands in order.

---

## Table of Contents

<div align="center">

| Architecture & Dataflow | Operations & Reference |
| :--- | :--- |
| • [Overview](#overview)<br>• [System Architecture](#system-architecture)<br>• [Cluster Topology & Node Roles](#cluster-topology)<br>• [Data Pipeline & Business Logic](#data-pipeline)<br>• [Analytics Output Preview](#analytics-preview) | • [Broker Failure Test](#fault-tolerance)<br>• [Repository Layout](#repository-layout)<br>• [Getting Started](#getting-started)<br>• [Testing](#testing-verification)<br>• [Design Decisions](#engineering-highlights)<br>• [Team Members](#team-members) |

</div>

---

<a id="overview"></a>
## Overview

Every order in the dataset carries timestamps for when it was placed, accepted, picked up, promised and delivered, plus distance, amount, fee and status. Some records are deliberately dirty: malformed JSON, missing fields, impossible timestamps, zero or negative amounts, non-delivered statuses and exact duplicates. The pipeline drops those, then works out how long each order took and how late it was.

> [!IMPORTANT]
> **The assignment fixes a few rules that shape the design:**
> - Spark never reads Kafka directly. A consumer first writes each topic to a local JSONL file, and Spark reads that file.
> - Both topics, `orders` and `order_metrics`, have 2 partitions and replication factor 2 on the same two-broker cluster.
> - There is no HDFS, and no file is copied from VM4 to VM1. The metrics reach VM1 only through Kafka.
> - The failure test stops broker 2 on VM3. VM2 keeps running, because controller handover is out of scope.

A separate reference implementation in plain Python (`verify/reference.py`) recomputes the Job 1 metrics without Spark, so the team has a second calculation to check Spark against.

---

<a id="system-architecture"></a>
## System Architecture

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/images/architecture-dark.svg">
    <img src="docs/images/architecture-light.svg" width="100%" alt="Orders flow from VM1 through the orders topic to VM4, where Spark Job 1 enriches them; the metrics flow back through the order_metrics topic to VM1 for Spark Job 2. Both topics have two partitions, each led by one broker and replicated on the other.">
  </picture>
</p>

<p align="center"><sub>Numbers give the pipeline order. Each topic partition has a leader on one broker and a follower copy on the other, which is why the data survives VM3 being stopped. The diagram is drawn by <code>docs/images/architecture.py</code>.</sub></p>

---

<a id="cluster-topology"></a>
## Cluster Topology & Node Roles

<div align="center">

| Node | Teammate | Services | Role in the failure test |
| :--- | :--- | :--- | :--- |
| **VM1** | Dhriti ([@DHRITIRAJESH](https://github.com/DHRITIRAJESH)) | Producer 1 publishes `data/order.jsonl` to `orders`.<br>Consumer 2 lands `order_metrics` into `order_metrics_landed.jsonl`.<br>Spark Job 2 writes the two summary CSVs. | Client; keeps consuming through the surviving broker. |
| **VM2** | Vivian ([@VivianSobers](https://github.com/VivianSobers)) | KRaft controller and Kafka broker 1 (`node.id=1`, port `9092`, controller port `9093`). | Must stay up for the whole demo. |
| **VM3** | Dhanya ([@DhanyaSPrasad](https://github.com/DhanyaSPrasad)) | Kafka broker 2 (`node.id=2`, port `9092`). | The broker that gets stopped. |
| **VM4** | Navya ([@nav-yeah](https://github.com/nav-yeah)) | Consumer 1 lands `orders` into `orders_landed.jsonl`.<br>Spark Job 1 writes `order_metrics.jsonl`.<br>Producer 2 publishes that file to `order_metrics`. | Client. |

</div>

> [!NOTE]
> A spare standby machine on the same ZeroTier network is set up to take over VM1, VM3 or VM4 if a laptop fails. It cannot replace VM2 once the demo has started, since that would need a new controller.

---

<a id="data-pipeline"></a>
## Data Pipeline & Business Logic

### Stage 1: Ingestion

1. Producer 1 on VM1 sends each line of `data/order.jsonl` to the `orders` topic as raw bytes, without parsing it, so malformed records reach Spark and get dropped there.
2. Consumer 1 on VM4 reads `orders` and writes every message to `orders_landed.jsonl`. It stops once the topic has been quiet for a set time.

### Stage 2: Spark Job 1, cleaning and enrichment

Spark Job 1 reads `orders_landed.jsonl`.

#### Cleaning rules

<details>
<summary><b>Expand data cleaning and validation rules (7 filters)</b></summary>
<br>

- Malformed JSON lines are dropped.
- All 14 fields are required. A missing field, `null`, an empty string or a whitespace-only string makes the record invalid. Extra fields are ignored.
- `distance_km`, `order_amount` and `delivery_fee` must be JSON numbers. A quoted number such as `"5.6"` is invalid.
- All five timestamps must parse as `yyyy-MM-dd HH:mm:ss` and must be in order:
  `order_timestamp <= accepted_timestamp <= picked_up_timestamp < delivered_timestamp`.
  Delivery has to come strictly after pickup, or the speed would divide by zero.
- `distance_km > 0` and `order_amount > 0`.
- Only `status == "DELIVERED"` (exact, case-sensitive) is kept.
- Exact duplicates, meaning identical values in all 14 fields regardless of key order or extra fields, are counted once. Two records with the same `order_id` but any differing field are both kept, because the brief only removes *exact* duplicates.

Spark runs with `spark.sql.ansi.enabled=false` and `spark.sql.legacy.timeParserPolicy=CORRECTED`, so a bad timestamp becomes null and the record is dropped instead of crashing the job. Durations are computed in UTC, so the machines' time zones don't change the results.

</details>

#### Derived metrics

<div align="center">

| Metric column | Definition | Meaning |
| :--- | :--- | :--- |
| `preparation_time_min` | `picked_up_timestamp - accepted_timestamp` | Kitchen time, in minutes. |
| `delivery_time_min` | `delivered_timestamp - picked_up_timestamp` | Time on the road, in minutes. |
| `fulfillment_time_min` | `delivered_timestamp - order_timestamp` | Order placed to order delivered, in minutes. |
| `delivery_speed_kmph` | `distance_km / (delivery_time_min / 60)` | Average delivery speed in km/h. |
| `delay_min` | `max(0, delivered_timestamp - expected_delivery_timestamp)` | Minutes past the promised time, floored at 0. |
| `delivery_status` | `ON_TIME` if the delay is 0, else `DELAYED` | |
| `delay_category` | see below | |

</div>

<br>

<div align="center">

| `delay_min` | `delay_category` |
| :--- | :--- |
| `0` | `ON_TIME` |
| more than 0, up to 15 | `SLIGHT_DELAY` |
| more than 15, up to 30 | `MODERATE_DELAY` |
| more than 30 | `SEVERE_DELAY` |

</div>

<br>

Status and category use the unrounded delay, and speed uses the unrounded delivery time. The computed numbers are then rounded half-up to 2 decimal places. Job 1 writes a single file, `order_metrics.jsonl`: one JSON object per line holding the 14 input fields plus the 7 computed ones, sorted so a rerun produces the same file.

### Stage 3: Relaying the metrics

1. Producer 2 on VM4 publishes `order_metrics.jsonl` to the `order_metrics` topic.
2. Consumer 2 on VM1 writes that topic to `order_metrics_landed.jsonl`.

### Stage 4: Spark Job 2, analytics

Spark Job 2 reads `order_metrics_landed.jsonl`, drops malformed lines and exact duplicates, and writes two CSVs:

1. `restaurant_summary.csv`: `restaurant_id`, `order_count`, `avg_order_amount`, `avg_preparation_time_min`, `avg_delivery_time_min`, `delayed_order_count`, `delay_rate_pct`
2. `city_summary.csv`: `city`, `order_count`, `avg_order_amount`, `avg_delivery_time_min`, `delayed_order_count`, `delay_rate_pct`

`delay_rate_pct = round(delayed_order_count / order_count * 100, 2)`

All averages and rates are rounded to 2 decimal places.

---

<a id="analytics-preview"></a>
## Analytics Output Preview

These are the actual outputs from running Job 1 and then Job 2 directly on `data/order.jsonl`, on one machine without Kafka. Of the 62 input lines, 44 orders survive cleaning. The CSVs store plain numbers; the `%` signs are added here for readability.

### City summary (`city_summary.csv`)

<div align="center">

| city | order_count | avg_order_amount | avg_delivery_time_min | delayed_order_count | delay_rate_pct |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Bengaluru** | 16 | 634.38 | 22.50 | 12 | 75.00% |
| **Hyderabad** | 7 | 590.14 | 45.00 | 7 | 100.00% |
| **Mumbai** | 14 | 590.50 | 32.50 | 11 | 78.57% |
| **Pune** | 7 | 614.57 | 40.00 | 3 | 42.86% |

</div>

### Restaurant summary (`restaurant_summary.csv`)

<details>
<summary><b>Expand restaurant performance summary table (6 restaurants)</b></summary>
<br>

<div align="center">

| restaurant_id | order_count | avg_order_amount | avg_preparation_time_min | avg_delivery_time_min | delayed_order_count | delay_rate_pct |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **R01** | 8 | 639.00 | 12.50 | 20.00 | 4 | 50.00% |
| **R02** | 8 | 629.75 | 12.75 | 25.00 | 8 | 100.00% |
| **R03** | 7 | 542.00 | 13.43 | 30.00 | 4 | 57.14% |
| **R04** | 7 | 639.00 | 13.29 | 35.00 | 7 | 100.00% |
| **R05** | 7 | 614.57 | 13.14 | 40.00 | 3 | 42.86% |
| **R06** | 7 | 590.14 | 13.00 | 45.00 | 7 | 100.00% |

</div>

</details>

---

<a id="fault-tolerance"></a>
## Broker Failure Test

With replication factor 2, every partition has a copy on both brokers. When VM3 stops, the controller on VM2 moves leadership of VM3's partitions to broker 1, and clients carry on through VM2. `scripts/broker_failure_check.sh` runs the test: it counts both topics, waits for VM3 to be stopped and for broker 2 to drop out of the in-sync set, then counts again. In the one-laptop rehearsal all 62 orders and 44 metrics were still readable with broker 2 stopped. After VM3 restarts, `scripts/broker_rejoin_check.sh` waits until every partition is back in sync on both brokers and reads both topics again. In rehearsal, running the whole pipeline again with VM3 stopped, and once more after it rejoined, gave outputs byte-identical to the first run.

```mermaid
sequenceDiagram
    autonumber
    participant P as VM4 [Producer 2]
    participant B1 as VM2 [Broker 1 - Leader P0]
    participant B2 as VM3 [Broker 2 - Leader P1]
    participant C as VM1 [Consumer 2]

    Note over B1,B2: Normal operation: RF=2, min.insync.replicas=1
    P->>B1: Send batch (partition 0)
    P->>B2: Send batch (partition 1)
    B1-->>B2: Replicate
    C->>B1: Fetch partition 0
    C->>B2: Fetch partition 1

    Note over B2: Failure test: VM3 stopped
    P->>B2: Connection lost to broker 2
    Note over B1: Controller moves leadership of P1 to broker 1
    P->>B1: Send batch (partition 1)
    C->>B1: Fetch partitions 0 and 1
```

> [!IMPORTANT]
> - Topics are created with `--partitions 2 --replication-factor 2`.
> - The brokers set `offsets.topic.replication.factor=2`, so consumer offsets also survive losing VM3, and `min.insync.replicas=1`, so writes still succeed with one broker left.
> - The KRaft controller runs on VM2 and stays up throughout the test.

---

<a id="repository-layout"></a>
## Repository Layout

<details>
<summary><b>Expand repository tree</b></summary>
<br>

```text
.
├── README.md                    # This file
├── RUNBOOK.md                   # Live demo commands, in order
├── config/
│   ├── cluster.env              # ZeroTier IPs, ports, cluster id, topic settings (real cluster)
│   └── cluster.local.env        # Same keys for the one-laptop rehearsal (ports 19092-19094)
├── data/
│   └── order.jsonl              # Assigned order dataset
├── docs/images/
│   ├── architecture.py          # Draws the architecture diagram
│   └── architecture-*.svg       # Light and dark versions of the diagram
├── foodpipe/                    # Python package
│   ├── config.py                # Reads config/*.env, builds the bootstrap server list
│   ├── producer.py              # File -> topic (Producer 1 and Producer 2)
│   ├── consumer.py              # Topic -> local file, stops when the topic goes quiet (Consumer 1 and 2)
│   ├── job1_enrichment.py       # Spark Job 1: cleaning and order metrics
│   ├── job2_analytics.py        # Spark Job 2: restaurant and city summaries
│   ├── io_utils.py              # Writes one named JSONL or CSV file instead of a part-file folder
│   ├── schema.py                # Field lists, Spark schemas, output column orders
│   └── spark_session.py         # Spark session settings (UTC, parser policy)
├── kafka/                       # Broker tooling (bash)
│   ├── common.sh                # Shared settings and helpers sourced by every script
│   ├── install_kafka.sh         # Downloads and verifies Kafka 3.9.2
│   ├── server.properties.template
│   ├── render_config.sh         # Template -> per-machine broker config
│   ├── format_storage.sh        # Formats KRaft storage with the cluster id
│   ├── start_broker.sh          # Starts vm2 or vm3 and waits for its port
│   ├── stop_broker.sh           # Stops exactly one broker
│   ├── create_topics.sh         # orders + order_metrics, 2 partitions, RF 2
│   ├── reset_topics.sh          # Deletes and recreates both topics
│   ├── describe_topics.sh       # Leaders, replicas and in-sync sets per partition
│   └── local_cluster.sh         # Two brokers on one laptop for rehearsal
├── scripts/                     # One script per machine and stage
│   ├── vm1_stage1_produce.sh    # VM1: produce the dataset to orders
│   ├── vm4_stage1.sh            # VM4: consume orders, Spark Job 1, produce order_metrics
│   ├── vm1_stage2.sh            # VM1: consume order_metrics, Spark Job 2, print the tables
│   ├── broker_failure_check.sh  # Counts both topics before and after VM3 stops
│   ├── broker_rejoin_check.sh   # Waits for VM3 back in the ISR, then reads both topics
│   ├── demo_env.sh              # Prints KAFKA_HOME and BS for the demo terminal
│   └── local_e2e.sh             # Full pipeline rehearsal on one laptop
├── tests/                       # pytest suite (Spark jobs, Kafka clients, shell scripts)
├── verify/
│   ├── reference.py             # Plain-Python reference for the metrics and both summaries
│   └── compare.py               # Checks Spark outputs against the reference
├── pytest.ini
└── requirements.txt
```

`output/` holds generated pipeline files and is ignored by Git.

</details>

---

<a id="getting-started"></a>
## Getting Started

### Prerequisites

- Python 3.10
- Java 11. If your default `java` is a different version, point `JAVA_HOME` at a Java 11 install before running anything.
- Kafka 3.9.2 (`kafka_2.12-3.9.2`) on the two broker machines only. PySpark 3.3.4 comes from `requirements.txt`, so you don't need a separate Spark install.

### Quick Start

#### Step 1: Set up the environment

```bash
git clone https://github.com/VivianSobers/distributed-food-ordering.git
cd distributed-food-ordering

python3.10 -m venv .venv
.venv/bin/pip install -r requirements.txt

# only if `java -version` is not 11
export JAVA_HOME=/path/to/jdk-11
```

#### Step 2: Run the Spark jobs

In the pipeline, Job 1 runs on VM4 against the landed `orders` file and Job 2 runs on VM1 against the landed `order_metrics` file:

```bash
.venv/bin/python -m foodpipe.job1_enrichment \
    --input output/orders_landed.jsonl \
    --output output/order_metrics.jsonl

.venv/bin/python -m foodpipe.job2_analytics \
    --input output/order_metrics_landed.jsonl \
    --restaurant-out output/restaurant_summary.csv \
    --city-out output/city_summary.csv
```

To try both jobs on one machine without Kafka, give Job 1 the raw dataset (`--input data/order.jsonl`) and give Job 2 Job 1's output (`--input output/order_metrics.jsonl`). That is how the preview tables above were produced. Each job writes exactly one named file, not a Spark folder of `part-*` files.

---

<a id="testing-verification"></a>
## Testing

```bash
.venv/bin/python -m pytest -q
```

The tests cover every cleaning rule, including each required field, bad and misordered timestamps, quoted numbers and duplicate handling. They also check each metric formula against hand-worked values, including the delay-category boundaries at 15 and 30 minutes, the summary calculations, and the single-file output. `verify/reference.py` recomputes the metrics and both summaries in plain Python, and `tests/test_reference_agrees.py` requires Spark's output on the real dataset to match it exactly. Another test feeds the dataset in twice, as happens when it is produced to the topic twice, and requires byte-identical outputs.

To check outputs that came through Kafka against the reference:

```bash
.venv/bin/python -m verify.compare --dataset data/order.jsonl \
    --metrics output/order_metrics.jsonl \
    --restaurant output/restaurant_summary.csv \
    --city output/city_summary.csv
```

It prints `MATCH`, or the rows that differ, and exits non-zero on any difference.

---

<a id="engineering-highlights"></a>
## Design Decisions

> [!TIP]
> 1. **KRaft instead of ZooKeeper.** Kafka stores its cluster metadata in its own Raft log, so there's no separate ZooKeeper ensemble to run. One controller on VM2 is enough for this assignment.
>
> 2. **Landing files between Kafka and Spark.** The brief requires it, and it also gives each Spark run a fixed input file that can be inspected and rerun.
>
> 3. **Single-file writer (`write_single_file`).** Spark normally writes a folder of `part-*` files plus `_SUCCESS`. `foodpipe/io_utils.py` writes through one partition, moves the single part file to the exact name the brief asks for, and removes the temporary folder. An empty result still produces a file (a header-only CSV).
>
> 4. **Fixed time zone and parser policy.** UTC sessions and `timeParserPolicy=CORRECTED` mean every teammate's machine parses the same timestamps the same way.

---

<a id="team-members"></a>
## Team Members

<div align="center">

| Member | GitHub | Role | Node |
| :--- | :---: | :--- | :---: |
| **Dhriti** | [@DHRITIRAJESH](https://github.com/DHRITIRAJESH) | Producer 1, Consumer 2 & Spark Job 2 | **VM1** |
| **Vivian** | [@VivianSobers](https://github.com/VivianSobers) | KRaft controller & Kafka broker 1 | **VM2** |
| **Dhanya** | [@DhanyaSPrasad](https://github.com/DhanyaSPrasad) | Kafka broker 2 & failure test | **VM3** |
| **Navya** | [@nav-yeah](https://github.com/nav-yeah) | Consumer 1, Spark Job 1 & Producer 2 | **VM4** |

<br>

<sub>Big Data 2026 (UE24CS343AB2)</sub>

</div>
